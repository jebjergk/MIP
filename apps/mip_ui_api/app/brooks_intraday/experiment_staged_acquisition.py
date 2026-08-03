"""Phase 9 staged acquisition logging and week pipeline (one week at a time)."""

from __future__ import annotations

import json
import logging
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable

from .acquisition import acquire_historical_bars, refresh_bar_statuses, validate_stored_session
from .calendar import resolve_week_sessions
from .experiment_ib_preflight import (
    compare_phase2b_phase9_config,
    probe_session_readonly,
    run_ib_connection_preflight,
    verify_persisted_session,
)
from .experiment_pipeline import (
    _require_bar_completeness,
    bootstrap_validation_run,
    run_analysis_chain,
)
from .historical_bar_repository import load_bars_from_store, persist_bars
from .ib_historical_provider import (
    IbHistoricalProviderError,
    fetch_session_bars_ib,
    ib_payload_to_historical_bars,
)
from .bars import validate_bars
from app.services.ibkr_live_bars import project_root

logger = logging.getLogger(__name__)

LOG_PATH = project_root() / "cursorfiles" / "brooks_phase9_batch_run.log"

PROBE_SYMBOL = "AAPL"
PROBE_DATE = date(2026, 7, 13)
WEEK_START = date(2026, 7, 13)
IB_PACE_SEC = 12
MAX_SESSION_RETRIES = 4


def _log(stage: str, message: str, **fields: Any) -> None:
    rec = {
        "ts_utc": datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec="seconds"),
        "stage": stage,
        "message": message,
        **fields,
    }
    line = json.dumps(rec, default=str)
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LOG_PATH.open("a", encoding="utf-8") as fh:
        fh.write(line + "\n")
    logger.info("%s %s", stage, message)


def paced_fetch(symbol: str, trading_date: date):
    if not hasattr(paced_fetch, "_last"):
        paced_fetch._last = 0.0  # type: ignore
    wait = IB_PACE_SEC - (time.time() - paced_fetch._last)  # type: ignore
    if wait > 0:
        time.sleep(wait)
    paced_fetch._last = time.time()  # type: ignore
    return fetch_session_bars_ib(symbol, trading_date)


def persist_single_session(symbol: str, trading_date: date, run_id: str | None = None) -> dict[str, Any]:
    from . import store

    payload = fetch_session_bars_ib(symbol, trading_date)
    bars = ib_payload_to_historical_bars(payload, trading_date)
    week = resolve_week_sessions(trading_date)
    session = next(s for s in week.sessions if s.trading_date == trading_date)
    val = validate_bars(bars, session, source="IBKR_HISTORICAL_5M_RTH_V0_1")
    if val.status != "COMPLETE":
        return {"ok": False, "validation_status": val.status, "messages": val.messages}
    persist_bars(
        bars,
        source_request_id=payload.get("source_request_id"),
        source_metadata={"probe": "phase9_persistence", "run_id": run_id},
        data_quality_status=val.status,
    )
    verify = verify_persisted_session(symbol, trading_date)
    return {"ok": verify.get("exactly_78_unique"), "verify": verify, "dataset_hash": verify.get("dataset_hash")}


def find_next_incomplete_session(state: dict[str, Any]):
    from .acquisition import list_sessions_for_run

    for sym, td, session in list_sessions_for_run(state):
        existing = validate_stored_session(sym, td, session)
        if existing.get("status") != "COMPLETE":
            return sym, td, session
    return None


def acquire_one_session(
    state: dict[str, Any],
    *,
    ib_fetch: Callable | None = None,
    max_retries: int = MAX_SESSION_RETRIES,
) -> dict[str, Any]:
    """Acquire at most one missing session; persist immediately on success."""
    fetch_fn = ib_fetch or paced_fetch
    nxt = find_next_incomplete_session(state)
    if not nxt:
        refresh_bar_statuses(state)
        counts = (state.get("preparation") or {}).get("counts") or {}
        return {
            "done": True,
            "skipped": False,
            "complete_sessions": int(counts.get("bar_sessions_complete") or 0),
            "expected_sessions": int(counts.get("bar_sessions_expected") or 20),
        }
    sym, td, session = nxt
    existing = validate_stored_session(sym, td, session)
    if existing.get("status") == "COMPLETE":
        refresh_bar_statuses(state)
        return {"done": False, "skipped": True, "symbol": sym, "trading_date": td.isoformat()}

    last_err: dict | None = None
    for attempt in range(1, max_retries + 1):
        try:
            payload = fetch_fn(sym, td)
            bars = ib_payload_to_historical_bars(payload, td)
            val = validate_bars(bars, session, source="IBKR_HISTORICAL_5M_RTH_V0_1")
            if val.status != "COMPLETE":
                last_err = {"status": val.status, "messages": val.messages}
                if attempt < max_retries:
                    time.sleep(min(15 * attempt, 45))
                continue
            persist_bars(
                bars,
                source_request_id=payload.get("source_request_id"),
                source_metadata={"week_acquisition": True, "phase9a": True},
                data_quality_status=val.status,
            )
            verify = verify_persisted_session(sym, td)
            refresh_bar_statuses(state)
            counts = (state.get("preparation") or {}).get("counts") or {}
            return {
                "done": False,
                "acquired": True,
                "symbol": sym,
                "trading_date": td.isoformat(),
                "session_hash": verify.get("dataset_hash"),
                "complete_sessions": int(counts.get("bar_sessions_complete") or 0),
                "expected_sessions": int(counts.get("bar_sessions_expected") or 20),
            }
        except IbHistoricalProviderError as exc:
            last_err = {"code": exc.code, "message": exc.message}
            if "PACING" in exc.code or "pacing" in (exc.message or "").lower():
                time.sleep(min(20 * attempt, 60))
            elif attempt < max_retries:
                time.sleep(5 * attempt)
    return {
        "done": False,
        "acquired": False,
        "symbol": sym,
        "trading_date": td.isoformat(),
        "error": last_err,
    }


def acquire_week_resumable(state: dict[str, Any], *, ib_fetch: Callable | None = None) -> dict[str, Any]:
    """Acquire missing sessions only; commit each session; bounded retry on pacing."""
    from .acquisition import list_sessions_for_run

    fetch_fn = ib_fetch or paced_fetch
    totals = {"acquired": 0, "skipped_complete": 0, "failed": []}
    sessions = list_sessions_for_run(state)
    total = len(sessions)

    for idx, (sym, td, session) in enumerate(sessions, start=1):
        existing = validate_stored_session(sym, td, session)
        if existing.get("status") == "COMPLETE":
            totals["skipped_complete"] += 1
            _log(
                "WEEK_ACQUISITION",
                "skip complete session",
                run_id=state.get("run_id"),
                week=str(state.get("selected_week_start")),
                symbol=sym,
                trading_date=td.isoformat(),
                progress=f"{idx}/{total}",
            )
            continue
        success = False
        last_err: dict | None = None
        for attempt in range(1, MAX_SESSION_RETRIES + 1):
            try:
                payload = fetch_fn(sym, td)
                bars = ib_payload_to_historical_bars(payload, td)
                val = validate_bars(bars, session, source="IBKR_HISTORICAL_5M_RTH_V0_1")
                if val.status != "COMPLETE":
                    last_err = {"status": val.status, "messages": val.messages}
                    if attempt < MAX_SESSION_RETRIES:
                        time.sleep(min(15 * attempt, 45))
                    continue
                persist_bars(
                    bars,
                    source_request_id=payload.get("source_request_id"),
                    source_metadata={"week_acquisition": True},
                    data_quality_status=val.status,
                )
                totals["acquired"] += 1
                success = True
                _log(
                    "WEEK_ACQUISITION",
                    "session acquired",
                    run_id=state.get("run_id"),
                    symbol=sym,
                    trading_date=td.isoformat(),
                    progress=f"{idx}/{total}",
                    completed_sessions=(state.get("preparation") or {}).get("counts", {}).get(
                        "bar_sessions_complete"
                    ),
                )
                break
            except IbHistoricalProviderError as exc:
                last_err = {"code": exc.code, "message": exc.message}
                if "PACING" in exc.code or "pacing" in exc.message.lower():
                    time.sleep(min(20 * attempt, 60))
                elif attempt < MAX_SESSION_RETRIES:
                    time.sleep(5 * attempt)
        if not success:
            totals["failed"].append({"symbol": sym, "trading_date": td.isoformat(), "error": last_err})
            _log(
                "WEEK_ACQUISITION",
                "session failed",
                run_id=state.get("run_id"),
                symbol=sym,
                trading_date=td.isoformat(),
                error=last_err,
                progress=f"{idx}/{total}",
            )

    refresh_bar_statuses(state)
    counts = (state.get("preparation") or {}).get("counts") or {}
    _log(
        "WEEK_ACQUISITION",
        "week pass complete",
        run_id=state.get("run_id"),
        acquired=totals["acquired"],
        skipped=totals["skipped_complete"],
        failed=len(totals["failed"]),
        complete_sessions=counts.get("bar_sessions_complete"),
        expected=counts.get("bar_sessions_expected"),
    )
    totals["counts"] = counts
    return totals


def acquire_next_incomplete_session(
    state: dict[str, Any],
    *,
    ib_fetch: Callable | None = None,
    max_retries: int = MAX_SESSION_RETRIES,
) -> dict[str, Any]:
    """Acquire at most one missing session; persist immediately. Safe checkpoint after return."""
    from .acquisition import list_sessions_for_run

    fetch_fn = ib_fetch or paced_fetch
    sessions = list_sessions_for_run(state)
    total = len(sessions)
    complete_before = int((state.get("preparation") or {}).get("counts", {}).get("bar_sessions_complete") or 0)

    for idx, (sym, td, session) in enumerate(sessions, start=1):
        existing = validate_stored_session(sym, td, session)
        if existing.get("status") == "COMPLETE":
            continue
        last_err: dict | None = None
        for attempt in range(1, max_retries + 1):
            try:
                payload = fetch_fn(sym, td)
                bars = ib_payload_to_historical_bars(payload, td)
                val = validate_bars(bars, session, source="IBKR_HISTORICAL_5M_RTH_V0_1")
                if val.status != "COMPLETE":
                    last_err = {"status": val.status, "messages": val.messages}
                    if attempt < max_retries:
                        time.sleep(min(15 * attempt, 45))
                    continue
                persist_bars(
                    bars,
                    source_request_id=payload.get("source_request_id"),
                    source_metadata={"week_acquisition": True, "phase9a": True},
                    data_quality_status=val.status,
                )
                refresh_bar_statuses(state)
                verify = verify_persisted_session(sym, td)
                counts = (state.get("preparation") or {}).get("counts") or {}
                return {
                    "action": "acquired",
                    "symbol": sym,
                    "trading_date": td.isoformat(),
                    "session_index": idx,
                    "total_sessions": total,
                    "bar_sessions_complete": int(counts.get("bar_sessions_complete") or 0),
                    "dataset_hash": verify.get("dataset_hash"),
                    "retry_attempt": attempt,
                    "ib_status": payload.get("status"),
                }
            except IbHistoricalProviderError as exc:
                last_err = {"code": exc.code, "message": exc.message}
                if "PACING" in exc.code or "pacing" in (exc.message or "").lower():
                    time.sleep(min(20 * attempt, 60))
                elif attempt < max_retries:
                    time.sleep(5 * attempt)
        refresh_bar_statuses(state)
        return {
            "action": "failed",
            "symbol": sym,
            "trading_date": td.isoformat(),
            "session_index": idx,
            "total_sessions": total,
            "error": last_err,
            "recoverable": True,
        }

    refresh_bar_statuses(state)
    counts = (state.get("preparation") or {}).get("counts") or {}
    complete_after = int(counts.get("bar_sessions_complete") or 0)
    return {
        "action": "all_complete",
        "bar_sessions_complete": complete_after,
        "total_sessions": total,
        "skipped_already_complete": complete_after - complete_before,
    }


def find_first_incomplete_session(state: dict[str, Any]) -> tuple[str, date] | None:
    from .acquisition import list_sessions_for_run

    for sym, td, session in list_sessions_for_run(state):
        existing = validate_stored_session(sym, td, session)
        if existing.get("status") != "COMPLETE":
            return sym, td
    return None


def _find_validation_run_id(week_start: date) -> str | None:
    from .experiment_analytics import list_validation_run_ids

    target = week_start.isoformat()
    for rec in list_validation_run_ids():
        if str(rec.get("selected_week_start") or "")[:10] == target:
            return str(rec.get("run_id"))
    return None


def run_staged_preflight_and_week(
    *,
    week_start: date | None = None,
    run_full_pipeline_if_ready: bool = True,
) -> dict[str, Any]:
    from .experiment_week_selection import LOCKED_VALIDATION_WEEKS

    ws = week_start or WEEK_START
    week = resolve_week_sessions(ws)
    probe_date = week.sessions[0].trading_date
    report: dict[str, Any] = {"week_start": ws.isoformat()}

    _log("IB_PREFLIGHT", "starting connection preflight")
    config_cmp = compare_phase2b_phase9_config()
    report["config_comparison"] = config_cmp
    preflight = run_ib_connection_preflight()
    report["ib_preflight"] = preflight
    _log("IB_PREFLIGHT", "complete", connected=preflight.get("connected"), port=preflight.get("port"))

    if not preflight.get("connected"):
        report["stopped_at"] = "IB_PREFLIGHT"
        return report

    _log(
        "SESSION_PROBE",
        "read-only AAPL probe",
        symbol=PROBE_SYMBOL,
        trading_date=probe_date.isoformat(),
        week=ws.isoformat(),
    )
    probe = probe_session_readonly(PROBE_SYMBOL, probe_date)
    report["session_probe"] = probe
    analysis = probe.get("analysis") or {}
    if not probe.get("fetch_ok") or not analysis.get("exactly_78"):
        report["stopped_at"] = "SESSION_PROBE"
        _log("SESSION_PROBE", "failed — not 78 bars", analysis=analysis, ib_error=probe.get("ib_error"))
        return report
    _log("SESSION_PROBE", "success 78 bars", duration_sec=probe.get("duration_sec"))

    from . import store

    existing_run = _find_validation_run_id(ws)
    if existing_run:
        run_id = existing_run
        store.get_run(run_id)
        _log("SESSION_PROBE", "reuse validation run", run_id=run_id)
    else:
        boot = bootstrap_validation_run(ws)
        run_id = boot["run_id"]
        store.get_run(run_id)
        with store._lock:
            state = store._runs[run_id]
            if state.get("preparation") is None:
                state["preparation"] = {}
        store.run_prepare(run_id)
        store.run_reconstruct_dossiers(run_id)
    report["validation_run_id"] = run_id
    _log("PERSISTENCE_PROBE", "starting", run_id=run_id, symbol=PROBE_SYMBOL)
    persist_result = persist_single_session(PROBE_SYMBOL, probe_date, run_id=run_id)
    report["persistence_probe"] = persist_result
    if not persist_result.get("ok"):
        report["stopped_at"] = "PERSISTENCE_PROBE"
        _log("PERSISTENCE_PROBE", "failed", result=persist_result)
        return report
    _log("PERSISTENCE_PROBE", "ok", hash=persist_result.get("dataset_hash"))

    with store._lock:
        state = store._runs[run_id]
    _log("WEEK_ACQUISITION", "starting resumable week acquire", run_id=run_id)
    week_acq = acquire_week_resumable(state, ib_fetch=paced_fetch)
    report["week_acquisition"] = week_acq
    with store._lock:
        store._runs[run_id] = state
        store._persist_run_header(state)

    store.run_prepare(run_id)
    with store._lock:
        state = store._runs[run_id]
    try:
        _require_bar_completeness(state)
        report["bar_validation"] = {"ok": True, "sessions": "20/20"}
        _log("BAR_VALIDATION", "20/20 complete", run_id=run_id)
    except Exception as exc:
        report["bar_validation"] = {"ok": False, "error": str(exc)}
        report["stopped_at"] = "BAR_VALIDATION"
        _log("BAR_VALIDATION", "incomplete", error=str(exc))
        return report

    if not run_full_pipeline_if_ready:
        report["stopped_at"] = "BAR_VALIDATION_OK_PIPELINE_SKIPPED"
        return report

    _log("DOSSIER_RECONSTRUCTION", "already frozen for validation run", run_id=run_id)
    _log("OBJECTIVE_REPLAY", "starting bulk", run_id=run_id)
    attempts = run_analysis_chain(state)
    report["pipeline_attempts"] = attempts
    _log("OBJECTIVE_REPLAY", "done", objective=attempts.get("objective_attempt_id"))
    _log("PATTERN_REPLAY", "done", pattern=attempts.get("pattern_attempt_id"))
    _log("CONTEXT_REPLAY", "done", context=attempts.get("context_attempt_id"))
    _log("SIMULATION_REPLAY", "done", simulation=attempts.get("simulation_attempt_id"))
    with store._lock:
        store._runs[run_id] = state
        store._persist_run_header(state)

    from .experiment_analytics import build_week_report

    report["week_report"] = build_week_report(run_id)
    _log("WEEK_COMPLETE", "week pipeline finished", run_id=run_id, week=ws.isoformat())
    report["stopped_at"] = "WEEK_COMPLETE"
    try:
        idx = LOCKED_VALIDATION_WEEKS.index(ws)
        next_week = LOCKED_VALIDATION_WEEKS[idx + 1] if idx + 1 < len(LOCKED_VALIDATION_WEEKS) else None
    except ValueError:
        next_week = None
    if next_week:
        report["next_command"] = f"Use Brooks Intraday Lab: Run next validation week ({next_week.isoformat()})."
    else:
        report["next_command"] = "All locked validation weeks complete — open comparison and aggregate report in the Lab."
    return report
