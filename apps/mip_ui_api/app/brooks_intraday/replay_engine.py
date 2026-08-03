from __future__ import annotations

import hashlib
import json
import logging
import threading
from datetime import date, datetime
from typing import Any

from .bars import HistoricalBar
from .errors import BrooksIntradayError
from .historical_bar_repository import load_bars_from_store
from .observation_engine import observe_bar, reset_session_state_for_date
from .observation_repository import (
    count_completed_steps_from_observations,
    count_observations,
    insert_objective_observation,
    load_baseline_observation_index,
    load_observation_at_bar,
    observation_sequence_hash,
    slice_observation_count,
)
from .objective_ruleset_v01 import DEFAULT_PARAMETERS, RULESET_VERSION
from .pattern_engine import advance_patterns_for_bar, reset_pattern_session
from .pattern_repository import (
    count_completed_pattern_slices,
    count_pattern_instances,
    insert_bar_pattern_link,
    pattern_sequence_hash,
    slice_pattern_link_count,
    upsert_pattern_instance,
)
from app.db import get_connection
from .pattern_engine_v02 import advance_patterns_v02_for_bar
from .pattern_engine_v03 import advance_patterns_v03_for_bar
from .pattern_ruleset_v01 import DEFAULT_PARAMETERS as PATTERN_DEFAULT_PARAMETERS
from .pattern_ruleset_v01 import RULESET_VERSION as PATTERN_RULESET_VERSION
from .pattern_ruleset_v02 import DEFAULT_PARAMETERS as PATTERN_V02_DEFAULT_PARAMETERS
from .pattern_ruleset_v02 import RULESET_VERSION as PATTERN_V02
from .pattern_ruleset_v03 import DEFAULT_PARAMETERS as PATTERN_V03_DEFAULT_PARAMETERS
from .pattern_ruleset_v03 import RULESET_VERSION as PATTERN_V03
from .replay_attempt_repository import complete_replay_attempt, create_replay_attempt
from .replay_bar_view import ReplayBarView
from .replay_persistence import load_replay_cursor_table, persist_replay_cursor
from .replay_schedule import ReplayStep, build_replay_schedule, total_steps_for_week
from .repository import get_dossier

logger = logging.getLogger(__name__)

_replay_locks: dict[str, threading.Lock] = {}
_engine_lock = threading.Lock()

REPLAY_ENGINE_VERSION = "BROOKS_REPLAY_ENGINE_V0_3"


def is_pattern_ruleset(ruleset_version: str | None) -> bool:
    return bool(ruleset_version and ruleset_version.startswith("BROOKS_PATTERN_"))


def objective_baseline_attempt_id(state: dict[str, Any]) -> str:
    baseline = (
        state.get("phase4_review_baseline_attempt_id")
        or state.get("configuration", {}).get("phase4_review_baseline_attempt_id")
    )
    if not baseline:
        from .replay_attempt_repository import get_review_baseline_attempt_id

        baseline = get_review_baseline_attempt_id(state["run_id"])
    if not baseline:
        raise BrooksIntradayError(
            "PATTERN_REPLAY_NO_BASELINE",
            "Phase 4 objective baseline attempt is required for pattern replay.",
            run_id=state.get("run_id"),
        )
    return str(baseline)


def ensure_pattern_replay_attempt(state: dict[str, Any], *, force_new: bool = False) -> str:
    if not force_new and state.get("active_replay_attempt_id"):
        rs = state.get("ruleset_version") or state.get("configuration", {}).get("ruleset_version")
        if is_pattern_ruleset(rs):
            return str(state["active_replay_attempt_id"])
    baseline = objective_baseline_attempt_id(state)
    cursor = get_cursor(state)
    target_ruleset = (
        state.get("pattern_ruleset_version")
        or state.get("configuration", {}).get("pattern_ruleset_version")
        or PATTERN_RULESET_VERSION
    )
    if target_ruleset == PATTERN_V03:
        param_base = PATTERN_V03_DEFAULT_PARAMETERS
    elif target_ruleset == PATTERN_V02:
        param_base = PATTERN_V02_DEFAULT_PARAMETERS
    else:
        param_base = PATTERN_DEFAULT_PARAMETERS
    params = {
        **param_base,
        "objective_baseline_attempt_id": baseline,
        "objective_ruleset_version": RULESET_VERSION,
        "parent_run_id": state["run_id"],
        "phase5_v01_reference_attempt_id": state.get("phase5_pattern_v01_attempt_id")
        or state.get("configuration", {}).get("phase5_pattern_v01_attempt_id"),
    }
    notes = (
        "Phase 5C Brooks pattern attempt (V0.3)."
        if target_ruleset == PATTERN_V03
        else "Phase 5B Brooks pattern attempt (V0.2)."
        if target_ruleset == PATTERN_V02
        else "Phase 5 stateful Brooks pattern attempt."
    )
    attempt_id = create_replay_attempt(
        run_id=state["run_id"],
        ruleset_version=target_ruleset,
        starting_cursor=cursor,
        notes=notes,
        parameters=params,
    )
    state["active_replay_attempt_id"] = attempt_id
    state.setdefault("configuration", {})["active_replay_attempt_id"] = attempt_id
    state["ruleset_version"] = target_ruleset
    state.setdefault("configuration", {})["ruleset_version"] = target_ruleset
    state["replay_mode"] = "PATTERN"
    state.setdefault("configuration", {})["replay_mode"] = "PATTERN"
    state.pop("_pattern_session_state", None)
    return attempt_id


def get_active_replay_attempt_id(state: dict[str, Any]) -> str:
    attempt = state.get("active_replay_attempt_id")
    if not attempt:
        attempt = state.get("configuration", {}).get("active_replay_attempt_id")
    if not attempt:
        raise BrooksIntradayError(
            "REPLAY_NOT_READY",
            "No active replay analysis attempt. Reset or start replay to create one.",
            run_id=state.get("run_id"),
        )
    return str(attempt)


def ensure_replay_attempt(state: dict[str, Any], *, force_new: bool = False) -> str:
    if not force_new and state.get("active_replay_attempt_id"):
        return str(state["active_replay_attempt_id"])
    cursor = get_cursor(state)
    attempt_id = create_replay_attempt(
        run_id=state["run_id"],
        ruleset_version=RULESET_VERSION,
        starting_cursor=cursor,
        notes="Phase 4 objective Brooks observation attempt.",
        parameters=DEFAULT_PARAMETERS,
    )
    state["active_replay_attempt_id"] = attempt_id
    state.setdefault("configuration", {})["active_replay_attempt_id"] = attempt_id
    state["ruleset_version"] = RULESET_VERSION
    state.setdefault("configuration", {})["ruleset_version"] = RULESET_VERSION
    state.pop("_obs_session_state", None)
    return attempt_id


def _normalize_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is not None:
        from zoneinfo import ZoneInfo

        return dt.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)
    return dt


def _run_lock(run_id: str) -> threading.Lock:
    with _engine_lock:
        if run_id not in _replay_locks:
            _replay_locks[run_id] = threading.Lock()
        return _replay_locks[run_id]


def _schedule_cache(state: dict[str, Any]) -> list[ReplayStep]:
    key = "_replay_schedule"
    if key not in state:
        week = state["selected_week_start"]
        if isinstance(week, str):
            week = date.fromisoformat(week[:10])
        state[key] = build_replay_schedule(week)
    return state[key]


def _bars_index(state: dict[str, Any]) -> dict[tuple[str, date, datetime], HistoricalBar]:
    key = "_frozen_bars_index"
    if key in state:
        return state[key]
    index: dict[tuple[str, date, datetime], HistoricalBar] = {}
    symbols = state["symbols"]
    schedule = _schedule_cache(state)
    dates = sorted({s.trading_date for s in schedule})
    for sym in symbols:
        for td in dates:
            for bar in load_bars_from_store(sym, td):
                ts = _normalize_utc(bar.ts_utc)
                index[(sym, td, ts)] = bar
    state[key] = index
    state["_frozen_bars_by_symbol"] = _group_by_symbol(index, symbols)
    return index


def _group_by_symbol(
    index: dict[tuple[str, date, datetime], HistoricalBar],
    symbols: list[str],
) -> dict[str, list[HistoricalBar]]:
    by_sym: dict[str, list[HistoricalBar]] = {s: [] for s in symbols}
    for (sym, _td, _ts), bar in sorted(index.items(), key=lambda x: (x[0][0], x[0][2])):
        by_sym[sym].append(bar)
    return by_sym


def default_cursor(state: dict[str, Any]) -> dict[str, Any]:
    schedule = _schedule_cache(state)
    first = schedule[0]
    return {
        "run_id": state["run_id"],
        "trading_date": first.trading_date.isoformat(),
        "bar_timestamp_utc": None,
        "bar_timestamp_ny": None,
        "session_index": 0,
        "bar_index_in_session": -1,
        "global_step_index": 0,
        "completed_steps": 0,
        "total_steps": len(schedule),
        "status": "READY",
        "engine_version": REPLAY_ENGINE_VERSION,
        "active_dossiers": {},
        "last_request_token": None,
    }


def get_cursor(state: dict[str, Any]) -> dict[str, Any]:
    cur = state.get("replay_cursor")
    if not cur:
        cur = state.get("configuration", {}).get("replay_cursor")
    if not cur:
        table = load_replay_cursor_table(state["run_id"])
        cur = table
    if not cur:
        cur = default_cursor(state)
    schedule = _schedule_cache(state)
    cur["total_steps"] = len(schedule)
    state["replay_cursor"] = cur
    state.setdefault("configuration", {})["replay_cursor"] = cur
    return cur


def save_cursor(state: dict[str, Any], cursor: dict[str, Any]) -> None:
    state["replay_cursor"] = cursor
    state.setdefault("configuration", {})["replay_cursor"] = cursor
    persist_replay_cursor(state["run_id"], cursor)


def verify_replay_ready(state: dict[str, Any]) -> None:
    readiness = state.get("readiness") or state.get("configuration", {}).get("readiness") or {}
    if readiness.get("replay_readiness") != "READY":
        raise BrooksIntradayError("REPLAY_NOT_READY", "Replay is not ready.", run_id=state["run_id"])
    if not state.get("bars_frozen") and not state.get("configuration", {}).get("bars_frozen"):
        raise BrooksIntradayError("REPLAY_NOT_READY", "Historical bars are not frozen.", run_id=state["run_id"])
    if not state.get("dossiers_frozen"):
        readiness = state.get("readiness") or state.get("configuration", {}).get("readiness") or {}
        if not (
            readiness.get("dossier_readiness") == "READY"
            and readiness.get("replay_readiness") == "READY"
        ):
            raise BrooksIntradayError("REPLAY_NOT_READY", "Dossiers are not frozen.", run_id=state["run_id"])
    if len(state.get("symbols") or []) != 4:
        raise BrooksIntradayError("REPLAY_NOT_READY", "Exactly four symbols required.", run_id=state["run_id"])


def verify_schedule_bars(state: dict[str, Any]) -> None:
    schedule = _schedule_cache(state)
    index = _bars_index(state)
    symbols = state["symbols"]
    missing: list[str] = []
    for step in schedule:
        for sym in symbols:
            key = (sym, step.trading_date, _normalize_utc(step.bar_timestamp_utc))
            if key not in index:
                missing.append(f"{sym}|{step.trading_date}|{step.bar_timestamp_utc}")
    if missing:
        raise BrooksIntradayError(
            "FROZEN_DATASET_MISMATCH",
            "Missing frozen bars for scheduled replay timestamps.",
            run_id=state["run_id"],
            details={"missing_sample": missing[:10], "missing_count": len(missing)},
        )


def recover_run_on_load(state: dict[str, Any]) -> None:
    if state.get("status") == "RUNNING":
        state["status"] = "PAUSED"
    try:
        get_active_replay_attempt_id(state)
    except BrooksIntradayError:
        return
    cursor = get_cursor(state)
    attempt_id = get_active_replay_attempt_id(state)
    rs = state.get("ruleset_version") or state.get("configuration", {}).get("ruleset_version")
    if is_pattern_ruleset(rs):
        obs_steps = count_completed_pattern_slices(
            state["run_id"], len(state["symbols"]), replay_attempt_id=attempt_id
        )
    else:
        obs_steps = count_completed_steps_from_observations(
            state["run_id"], len(state["symbols"]), replay_attempt_id=attempt_id
        )
    completed = int(cursor.get("completed_steps") or 0)
    if obs_steps != completed:
        raise BrooksIntradayError(
            "REPLAY_CURSOR_OBSERVATION_MISMATCH",
            f"Cursor completed_steps={completed} but replay records imply {obs_steps} slices.",
            run_id=state["run_id"],
            status_code=409,
        )
    verify_cursor_obs_consistency(state, cursor)


def verify_cursor_obs_consistency(state: dict[str, Any], cursor: dict[str, Any]) -> None:
    attempt_id = get_active_replay_attempt_id(state)
    rs = state.get("ruleset_version") or state.get("configuration", {}).get("ruleset_version")
    if is_pattern_ruleset(rs):
        obs_steps = count_completed_pattern_slices(
            state["run_id"], len(state["symbols"]), replay_attempt_id=attempt_id
        )
    else:
        obs_steps = count_completed_steps_from_observations(
            state["run_id"], len(state["symbols"]), replay_attempt_id=attempt_id
        )
    completed = int(cursor.get("completed_steps") or 0)
    if obs_steps != completed:
        raise BrooksIntradayError(
            "REPLAY_CURSOR_OBSERVATION_MISMATCH",
            f"Cursor/observation mismatch: cursor={completed} observations={obs_steps}.",
            run_id=state["run_id"],
            status_code=409,
        )


def _load_session_dossiers(state: dict[str, Any], trading_date: date) -> dict[str, Any]:
    run_id = state["run_id"]
    out: dict[str, Any] = {}
    for sym in state["symbols"]:
        row = get_dossier(run_id, sym, trading_date)
        if not row or not row.get("frozen_dossier_json"):
            raise BrooksIntradayError(
                "DOSSIER_NOT_FOUND_FOR_SESSION",
                f"No frozen dossier for {sym} on {trading_date.isoformat()}.",
                run_id=run_id,
                symbol=sym,
                trading_date=trading_date.isoformat(),
            )
        out[sym] = row["frozen_dossier_json"]
    return out


def replay_start(state: dict[str, Any]) -> None:
    verify_replay_ready(state)
    verify_schedule_bars(state)
    if state.get("status") == "STOPPED":
        raise BrooksIntradayError(
            "REPLAY_NOT_READY",
            "Run is STOPPED. Reset before starting again.",
            run_id=state["run_id"],
        )
    rs = state.get("ruleset_version") or state.get("configuration", {}).get("ruleset_version")
    if is_pattern_ruleset(rs) or state.get("replay_mode") == "PATTERN":
        ensure_pattern_replay_attempt(state, force_new=False)
    else:
        ensure_replay_attempt(state, force_new=False)
    if state.get("status") == "COMPLETED":
        raise BrooksIntradayError(
            "REPLAY_ALREADY_COMPLETED",
            "Replay already completed. Reset to replay again.",
            run_id=state["run_id"],
        )
    cursor = get_cursor(state)
    if int(cursor.get("completed_steps") or 0) == 0:
        first = _schedule_cache(state)[0]
        cursor.update(
            {
                "trading_date": first.trading_date.isoformat(),
                "session_index": 0,
                "bar_index_in_session": -1,
                "global_step_index": 0,
                "completed_steps": 0,
                "bar_timestamp_utc": None,
                "bar_timestamp_ny": None,
            }
        )
        cursor["active_dossiers"] = _load_session_dossiers(state, first.trading_date)
    state["status"] = "RUNNING"
    speed = (state.get("playback_speed") or "manual").lower()
    if speed in {"manual", "manual".lower()}:
        state["status"] = "PAUSED"
    save_cursor(state, cursor)


def replay_pause(state: dict[str, Any]) -> None:
    if state.get("status") == "RUNNING":
        state["status"] = "PAUSED"


def replay_resume(state: dict[str, Any]) -> None:
    if state.get("status") == "STOPPED":
        raise BrooksIntradayError(
            "REPLAY_NOT_READY",
            "Cannot resume a STOPPED run. Reset first.",
            run_id=state["run_id"],
        )
    if state.get("status") == "COMPLETED":
        raise BrooksIntradayError("REPLAY_ALREADY_COMPLETED", "Replay completed.", run_id=state["run_id"])
    if state.get("status") != "PAUSED":
        return
    state["status"] = "RUNNING"
    speed = (state.get("playback_speed") or "manual").lower()
    if speed == "manual":
        state["status"] = "PAUSED"


def replay_stop(state: dict[str, Any]) -> None:
    if state.get("status") in ("RUNNING", "PAUSED"):
        state["status"] = "STOPPED"


def replay_reset_pattern(state: dict[str, Any], *, ruleset_version: str | None = None) -> None:
    """Reset cursor for a new Phase 5 pattern replay (Phase 4 baseline observations unchanged)."""
    cursor = default_cursor(state)
    first = _schedule_cache(state)[0]
    cursor["active_dossiers"] = _load_session_dossiers(state, first.trading_date)
    save_cursor(state, cursor)
    state["replay_timestamp"] = None
    state["active_trading_date"] = None
    state["status"] = "READY"
    state.pop("_pattern_session_state", None)
    if ruleset_version:
        state["pattern_ruleset_version"] = ruleset_version
        state["ruleset_version"] = ruleset_version
        state.setdefault("configuration", {})["pattern_ruleset_version"] = ruleset_version
        state.setdefault("configuration", {})["ruleset_version"] = ruleset_version
    ensure_pattern_replay_attempt(state, force_new=True)


def run_pattern_replay_bulk(
    state: dict[str, Any],
    *,
    progress_every: int = 50,
    commit_every: int = 20,  # noqa: ARG001 — kept for API compatibility
) -> str:
    """
    Run a full pattern ruleset replay with batched Snowflake writes.
    Intended for agent validation scripts (not interactive step API).
    """
    verify_replay_ready(state)
    verify_schedule_bars(state)
    rs = state.get("ruleset_version") or state.get("configuration", {}).get("ruleset_version")
    if not is_pattern_ruleset(rs):
        raise BrooksIntradayError("BULK_REPLAY_NOT_PATTERN", "Bulk replay requires a pattern ruleset.", run_id=state["run_id"])

    state.pop("_pattern_session_state", None)
    cursor = default_cursor(state)
    save_cursor(state, cursor)
    attempt_id = ensure_pattern_replay_attempt(state, force_new=True)
    baseline_id = objective_baseline_attempt_id(state)
    obs_index = load_baseline_observation_index(state["run_id"], baseline_id)

    schedule = _schedule_cache(state)
    index = _bars_index(state)
    symbols = state["symbols"]
    pending_patterns: dict[str, tuple[str, Any]] = {}
    pending_links: list[dict[str, Any]] = []
    completed = 0
    for step in schedule:
        prev_session = cursor.get("trading_date")
        if prev_session != step.trading_date.isoformat() and completed > 0:
            cursor["active_dossiers"] = _load_session_dossiers(state, step.trading_date)
            for sym in symbols:
                reset_pattern_session(state, sym, step.trading_date)

        for sym in symbols:
            key = (sym, step.trading_date, _normalize_utc(step.bar_timestamp_utc))
            bar = index.get(key)
            if not bar:
                raise BrooksIntradayError(
                    "FROZEN_DATASET_MISMATCH",
                    f"Missing bar for {sym} at {step.bar_timestamp_utc}.",
                    run_id=state["run_id"],
                    symbol=sym,
                )
            obs_key = (sym.upper(), str(_normalize_utc(bar.ts_utc) or bar.ts_utc)[:19])
            objective_obs = obs_index.get(obs_key)
            if not objective_obs:
                raise BrooksIntradayError(
                    "PATTERN_MISSING_OBJECTIVE_OBS",
                    f"No Phase 4 baseline observation for {sym} at {bar.ts_utc}.",
                    run_id=state["run_id"],
                    symbol=sym,
                )
            if rs == PATTERN_V03:
                pat_result = advance_patterns_v03_for_bar(
                    state=state,
                    run_id=state["run_id"],
                    objective_baseline_attempt_id=baseline_id,
                    symbol=sym,
                    trading_date=step.trading_date,
                    bar=bar,
                    objective_obs=objective_obs,
                )
            elif rs == PATTERN_V02:
                pat_result = advance_patterns_v02_for_bar(
                    state=state,
                    run_id=state["run_id"],
                    objective_baseline_attempt_id=baseline_id,
                    symbol=sym,
                    trading_date=step.trading_date,
                    bar=bar,
                    objective_obs=objective_obs,
                )
            else:
                pat_result = advance_patterns_for_bar(
                    state=state,
                    run_id=state["run_id"],
                    objective_baseline_attempt_id=baseline_id,
                    symbol=sym,
                    trading_date=step.trading_date,
                    bar=bar,
                    objective_obs=objective_obs,
                )
            for pat in pat_result["pattern_instances_created_or_updated"]:
                pending_patterns[pat.pattern_instance_id] = (sym, pat)
            pending_links.append(
                {
                    "symbol": sym,
                    "trading_date": step.trading_date,
                    "bar_ts_utc": _normalize_utc(bar.ts_utc) or bar.ts_utc,
                    "active_pattern_ids": pat_result["active_pattern_instance_ids"],
                    "pattern_snapshot": pat_result["pattern_snapshot"],
                    "action": pat_result["action"],
                    "explanation": pat_result["explanation"],
                }
            )

        completed += 1
        if progress_every and completed % progress_every == 0:
            logger.info("bulk pattern replay step %s/%s attempt=%s", completed, len(schedule), attempt_id)
            print(f"step {completed}", flush=True)

    conn = get_connection()
    try:
        for i, (sym, pat) in enumerate(pending_patterns.values(), start=1):
            upsert_pattern_instance(
                run_id=state["run_id"],
                replay_attempt_id=attempt_id,
                symbol=sym,
                pat=pat,
                pattern_ruleset_version=rs,
                conn=conn,
                commit=False,
            )
            if i % 200 == 0:
                conn.commit()
                print(f"flushed patterns {i}/{len(pending_patterns)}", flush=True)
        for i, link in enumerate(pending_links, start=1):
            insert_bar_pattern_link(
                run_id=state["run_id"],
                replay_attempt_id=attempt_id,
                symbol=link["symbol"],
                trading_date=link["trading_date"],
                bar_ts_utc=link["bar_ts_utc"],
                active_pattern_ids=link["active_pattern_ids"],
                pattern_snapshot=link["pattern_snapshot"],
                action=link["action"],
                explanation=link["explanation"],
                conn=conn,
                commit=False,
            )
            if i % 400 == 0:
                conn.commit()
                print(f"flushed links {i}/{len(pending_links)}", flush=True)
        conn.commit()
        print(f"persisted {len(pending_patterns)} patterns and {len(pending_links)} bar links", flush=True)
    finally:
        conn.close()

    cursor.update(
        {
            "completed_steps": len(schedule),
            "global_step_index": len(schedule),
            "trading_date": schedule[-1].trading_date.isoformat(),
            "bar_timestamp_utc": schedule[-1].bar_timestamp_utc.isoformat() if schedule[-1].bar_timestamp_utc else None,
            "bar_timestamp_ny": schedule[-1].bar_timestamp_ny.isoformat(),
            "session_index": schedule[-1].session_index,
            "bar_index_in_session": schedule[-1].bar_index_in_session,
            "status": "COMPLETED",
        }
    )
    state["status"] = "COMPLETED"
    save_cursor(state, cursor)
    seq_hash = pattern_sequence_hash(state["run_id"], replay_attempt_id=attempt_id)
    complete_replay_attempt(
        attempt_id=attempt_id,
        observation_count=count_pattern_instances(state["run_id"], replay_attempt_id=attempt_id),
        sequence_hash=seq_hash,
    )
    state["active_replay_attempt_id"] = attempt_id
    state["phase5_pattern_attempt_id"] = attempt_id
    state.setdefault("configuration", {})["phase5_pattern_attempt_id"] = attempt_id
    if rs == PATTERN_V02:
        state["phase5_pattern_v02_attempt_id"] = attempt_id
        state.setdefault("configuration", {})["phase5_pattern_v02_attempt_id"] = attempt_id
    elif rs == PATTERN_V03:
        state["phase5_pattern_v03_attempt_id"] = attempt_id
        state.setdefault("configuration", {})["phase5_pattern_v03_attempt_id"] = attempt_id
    return attempt_id


def run_objective_replay_bulk(
    state: dict[str, Any],
    *,
    progress_every: int = 50,
    commit_every: int = 400,
) -> str:
    """Full-week objective observation replay with batched persistence."""
    verify_replay_ready(state)
    verify_schedule_bars(state)
    state.pop("_obs_session_state", None)
    cursor = default_cursor(state)
    save_cursor(state, cursor)
    attempt_id = ensure_replay_attempt(state, force_new=True)
    schedule = _schedule_cache(state)
    index = _bars_index(state)
    symbols = state["symbols"]
    by_symbol = _group_by_symbol(index, symbols)
    pending: list[tuple] = []
    prev_td: date | None = None

    for step_idx, step in enumerate(schedule):
        if prev_td != step.trading_date:
            for sym in symbols:
                reset_session_state_for_date(state, sym, step.trading_date)
            prev_td = step.trading_date
        view = ReplayBarView(
            bars_by_symbol=by_symbol,
            cursor_timestamp_utc=step.bar_timestamp_utc,
            symbols=symbols,
        )
        for sym_index, sym in enumerate(symbols):
            key = (sym, step.trading_date, _normalize_utc(step.bar_timestamp_utc))
            bar = index.get(key)
            if not bar:
                raise BrooksIntradayError(
                    "FROZEN_DATASET_MISMATCH",
                    f"Missing bar for {sym} at {step.bar_timestamp_utc}.",
                    run_id=state["run_id"],
                    symbol=sym,
                )
            view.current_slice([bar])
            visible_session = [b for b in view.visible_bars(sym) if b.trading_date == step.trading_date]
            obs_payload = observe_bar(
                state=state,
                symbol=sym,
                trading_date=step.trading_date,
                bar=bar,
                session_bar_index=step.bar_index_in_session,
                visible_same_session=visible_session,
            )
            ohlcv = {
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": bar.volume,
            }
            seq = step.global_step_index * len(symbols) + sym_index
            pending.append(
                (
                    sym,
                    step.trading_date,
                    _normalize_utc(bar.ts_utc) or bar.ts_utc,
                    seq,
                    ohlcv,
                    obs_payload,
                )
            )
        if progress_every and (step_idx + 1) % progress_every == 0:
            print(f"objective step {step_idx + 1}/{len(schedule)}", flush=True)

    conn = get_connection()
    try:
        for i, (sym, td, ts, seq, ohlcv, obs_payload) in enumerate(pending, start=1):
            insert_objective_observation(
                run_id=state["run_id"],
                replay_attempt_id=attempt_id,
                symbol=sym,
                trading_date=td,
                bar_ts_utc=ts,
                sequence_num=seq,
                ohlcv=ohlcv,
                payload=obs_payload,
                conn=conn,
                commit=False,
            )
            if commit_every and i % commit_every == 0:
                conn.commit()
                print(f"flushed objective rows {i}/{len(pending)}", flush=True)
        conn.commit()
    finally:
        conn.close()

    cursor.update(
        {
            "completed_steps": len(schedule),
            "global_step_index": len(schedule),
            "trading_date": schedule[-1].trading_date.isoformat(),
            "bar_timestamp_utc": schedule[-1].bar_timestamp_utc.isoformat()
            if schedule[-1].bar_timestamp_utc
            else None,
            "bar_timestamp_ny": schedule[-1].bar_timestamp_ny.isoformat(),
            "session_index": schedule[-1].session_index,
            "bar_index_in_session": schedule[-1].bar_index_in_session,
            "status": "COMPLETED",
        }
    )
    state["status"] = "COMPLETED"
    save_cursor(state, cursor)
    seq_hash = observation_sequence_hash(state["run_id"], replay_attempt_id=attempt_id)
    complete_replay_attempt(
        attempt_id=attempt_id,
        observation_count=count_observations(state["run_id"], replay_attempt_id=attempt_id),
        sequence_hash=seq_hash,
    )
    state["active_replay_attempt_id"] = attempt_id
    state["phase4_review_baseline_attempt_id"] = attempt_id
    state.setdefault("configuration", {})["phase4_review_baseline_attempt_id"] = attempt_id
    state["ruleset_version"] = RULESET_VERSION
    state.setdefault("configuration", {})["ruleset_version"] = RULESET_VERSION
    return attempt_id


def replay_reset(state: dict[str, Any]) -> None:
    """Reset cursor and start a new immutable replay-analysis attempt (prior attempts retained)."""
    cursor = default_cursor(state)
    first = _schedule_cache(state)[0]
    cursor["active_dossiers"] = _load_session_dossiers(state, first.trading_date)
    save_cursor(state, cursor)
    state["replay_timestamp"] = None
    state["active_trading_date"] = None
    state["status"] = "READY"
    state.pop("_replay_schedule", None)
    state.pop("_frozen_bars_index", None)
    state.pop("_frozen_bars_by_symbol", None)
    state.pop("_obs_session_state", None)
    for sym in state["symbols"]:
        reset_session_state_for_date(state, sym, first.trading_date)
    ensure_replay_attempt(state, force_new=True)


def _step_response(
    state: dict[str, Any],
    cursor: dict[str, Any],
    step: ReplayStep,
    bars_payload: list[dict[str, Any]],
) -> dict[str, Any]:
    schedule = _schedule_cache(state)
    completed = int(cursor["completed_steps"])
    next_step = schedule[completed] if completed < len(schedule) else None
    return {
        "run_id": state["run_id"],
        "global_step_index": completed,
        "total_steps": len(schedule),
        "trading_date": step.trading_date.isoformat(),
        "timestamp_ny": step.bar_timestamp_ny.isoformat(),
        "timestamp_utc": step.bar_timestamp_utc.isoformat() if step.bar_timestamp_utc else None,
        "bars": bars_payload,
        "run_status": state.get("status"),
        "next_timestamp_ny": next_step.bar_timestamp_ny.isoformat() if next_step else None,
        "observations_total": _observation_total(state),
        "observations_expected": len(schedule) * len(state["symbols"]),
        "replay_attempt_id": state.get("active_replay_attempt_id"),
        "ruleset_version": state.get("ruleset_version", RULESET_VERSION),
        "replay_cursor": cursor,
    }


def _observation_total(state: dict[str, Any]) -> int:
    try:
        attempt_id = get_active_replay_attempt_id(state)
        return count_observations(state["run_id"], replay_attempt_id=attempt_id)
    except BrooksIntradayError:
        return count_observations(state["run_id"])


def process_next_bar(state: dict[str, Any], *, request_token: str | None = None) -> dict[str, Any]:
    verify_replay_ready(state)
    status = state.get("status")
    if status == "COMPLETED":
        raise BrooksIntradayError("REPLAY_ALREADY_COMPLETED", "All steps processed.", run_id=state["run_id"])
    if status == "STOPPED":
        raise BrooksIntradayError("REPLAY_NOT_PAUSED", "Run is STOPPED.", run_id=state["run_id"])
    if status == "RUNNING":
        speed = (state.get("playback_speed") or "manual").lower()
        if speed not in ("manual",):
            raise BrooksIntradayError(
                "REPLAY_NOT_PAUSED",
                "Automatic playback in progress; pause before manual Next Bar.",
                run_id=state["run_id"],
            )
    if status not in ("PAUSED", "RUNNING", "READY"):
        raise BrooksIntradayError("REPLAY_NOT_PAUSED", f"Cannot advance from status {status}.", run_id=state["run_id"])

    lock = _run_lock(state["run_id"])
    if not lock.acquire(blocking=False):
        raise BrooksIntradayError(
            "REPLAY_CURSOR_CONFLICT",
            "Another replay step is in progress.",
            run_id=state["run_id"],
            status_code=409,
        )
    try:
        cursor = get_cursor(state)
        schedule = _schedule_cache(state)
        rs = state.get("ruleset_version") or state.get("configuration", {}).get("ruleset_version")
        if is_pattern_ruleset(rs) or state.get("replay_mode") == "PATTERN":
            attempt_id = ensure_pattern_replay_attempt(state, force_new=False)
        else:
            attempt_id = ensure_replay_attempt(state, force_new=False)
        completed = int(cursor.get("completed_steps") or 0)
        if completed >= len(schedule):
            state["status"] = "COMPLETED"
            raise BrooksIntradayError("REPLAY_ALREADY_COMPLETED", "All steps processed.", run_id=state["run_id"])

        if request_token and cursor.get("last_request_token") == request_token:
            step = schedule[completed - 1] if completed > 0 else schedule[0]
            return _cached_last_response(state, cursor, schedule, completed)

        step = schedule[completed]
        if (
            step.bar_timestamp_utc
            and (
                slice_pattern_link_count(
                    state["run_id"], step.bar_timestamp_utc, replay_attempt_id=attempt_id
                )
                if is_pattern_ruleset(rs)
                else slice_observation_count(
                    state["run_id"], step.bar_timestamp_utc, replay_attempt_id=attempt_id
                )
            )
            >= len(state["symbols"])
        ):
            completed += 1
            cursor["completed_steps"] = completed
            cursor["global_step_index"] = completed
            save_cursor(state, cursor)
            if completed >= len(schedule):
                state["status"] = "COMPLETED"
            return _step_response(state, cursor, step, [])

        prev_session = cursor.get("trading_date")
        if prev_session != step.trading_date.isoformat() and completed > 0:
            cursor["active_dossiers"] = _load_session_dossiers(state, step.trading_date)
            for sym in state["symbols"]:
                if is_pattern_ruleset(rs):
                    reset_pattern_session(state, sym, step.trading_date)
                else:
                    reset_session_state_for_date(state, sym, step.trading_date)

        index = _bars_index(state)
        by_symbol = state.get("_frozen_bars_by_symbol") or _group_by_symbol(index, state["symbols"])
        view = ReplayBarView(
            bars_by_symbol=by_symbol,
            cursor_timestamp_utc=step.bar_timestamp_utc,
            symbols=state["symbols"],
        )

        bars_payload: list[dict[str, Any]] = []
        baseline_id = objective_baseline_attempt_id(state) if is_pattern_ruleset(rs) else None
        for sym_index, sym in enumerate(state["symbols"]):
            key = (sym, step.trading_date, _normalize_utc(step.bar_timestamp_utc))
            bar = index.get(key)
            if not bar:
                raise BrooksIntradayError(
                    "FROZEN_DATASET_MISMATCH",
                    f"Missing bar for {sym} at {step.bar_timestamp_utc}.",
                    run_id=state["run_id"],
                    symbol=sym,
                )
            view.current_slice([bar])
            visible_session = [
                b
                for b in view.visible_bars(sym)
                if b.trading_date == step.trading_date
            ]
            ohlcv = {
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": bar.volume,
            }
            if is_pattern_ruleset(rs):
                assert baseline_id
                objective_obs = load_observation_at_bar(
                    state["run_id"],
                    replay_attempt_id=baseline_id,
                    symbol=sym,
                    bar_ts_utc=_normalize_utc(bar.ts_utc) or bar.ts_utc,
                )
                if not objective_obs:
                    raise BrooksIntradayError(
                        "PATTERN_MISSING_OBJECTIVE_OBS",
                        f"No Phase 4 baseline observation for {sym} at {bar.ts_utc}.",
                        run_id=state["run_id"],
                        symbol=sym,
                    )
                if rs == PATTERN_V03:
                    pat_result = advance_patterns_v03_for_bar(
                        state=state,
                        run_id=state["run_id"],
                        objective_baseline_attempt_id=baseline_id,
                        symbol=sym,
                        trading_date=step.trading_date,
                        bar=bar,
                        objective_obs=objective_obs,
                    )
                elif rs == PATTERN_V02:
                    pat_result = advance_patterns_v02_for_bar(
                        state=state,
                        run_id=state["run_id"],
                        objective_baseline_attempt_id=baseline_id,
                        symbol=sym,
                        trading_date=step.trading_date,
                        bar=bar,
                        objective_obs=objective_obs,
                    )
                else:
                    pat_result = advance_patterns_for_bar(
                        state=state,
                        run_id=state["run_id"],
                        objective_baseline_attempt_id=baseline_id,
                        symbol=sym,
                        trading_date=step.trading_date,
                        bar=bar,
                        objective_obs=objective_obs,
                    )
                for pat in pat_result["pattern_instances_created_or_updated"]:
                    upsert_pattern_instance(
                        run_id=state["run_id"],
                        replay_attempt_id=attempt_id,
                        symbol=sym,
                        pat=pat,
                        pattern_ruleset_version=rs,
                    )
                insert_bar_pattern_link(
                    run_id=state["run_id"],
                    replay_attempt_id=attempt_id,
                    symbol=sym,
                    trading_date=step.trading_date,
                    bar_ts_utc=_normalize_utc(bar.ts_utc) or bar.ts_utc,
                    active_pattern_ids=pat_result["active_pattern_instance_ids"],
                    pattern_snapshot=pat_result["pattern_snapshot"],
                    action=pat_result["action"],
                    explanation=pat_result["explanation"],
                )
                bars_payload.append(
                    {
                        "symbol": sym,
                        **ohlcv,
                        "patterns": pat_result["pattern_snapshot"],
                        "action": pat_result["action"],
                    }
                )
                continue

            obs_payload = observe_bar(
                state=state,
                symbol=sym,
                trading_date=step.trading_date,
                bar=bar,
                session_bar_index=step.bar_index_in_session,
                visible_same_session=visible_session,
            )
            ohlcv = {
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": bar.volume,
            }
            seq = step.global_step_index * len(state["symbols"]) + sym_index
            insert_objective_observation(
                run_id=state["run_id"],
                replay_attempt_id=attempt_id,
                symbol=sym,
                trading_date=step.trading_date,
                bar_ts_utc=_normalize_utc(bar.ts_utc) or bar.ts_utc,
                sequence_num=seq,
                ohlcv=ohlcv,
                payload=obs_payload,
            )
            bars_payload.append(
                {
                    "symbol": sym,
                    **ohlcv,
                    "brooks_terms": obs_payload.get("brooks_obs_json"),
                    "observation_id": f"{attempt_id}:{sym}:{bar.ts_utc}",
                }
            )

        completed += 1
        cursor.update(
            {
                "completed_steps": completed,
                "global_step_index": completed,
                "trading_date": step.trading_date.isoformat(),
                "bar_timestamp_utc": step.bar_timestamp_utc.isoformat() if step.bar_timestamp_utc else None,
                "bar_timestamp_ny": step.bar_timestamp_ny.isoformat(),
                "session_index": step.session_index,
                "bar_index_in_session": step.bar_index_in_session,
                "last_request_token": request_token,
            }
        )
        state["replay_timestamp"] = cursor["bar_timestamp_ny"]
        state["active_trading_date"] = step.trading_date

        if completed >= len(schedule):
            state["status"] = "COMPLETED"
            cursor["status"] = "COMPLETED"
            if is_pattern_ruleset(rs):
                seq_hash = pattern_sequence_hash(state["run_id"], replay_attempt_id=attempt_id)
                complete_replay_attempt(
                    attempt_id=attempt_id,
                    observation_count=count_pattern_instances(state["run_id"], replay_attempt_id=attempt_id),
                    sequence_hash=seq_hash,
                )
                state["phase5_pattern_attempt_id"] = attempt_id
                state.setdefault("configuration", {})["phase5_pattern_attempt_id"] = attempt_id
                if rs == PATTERN_V02:
                    state["phase5_pattern_v02_attempt_id"] = attempt_id
                    state.setdefault("configuration", {})["phase5_pattern_v02_attempt_id"] = attempt_id
                elif rs == PATTERN_V03:
                    state["phase5_pattern_v03_attempt_id"] = attempt_id
                    state.setdefault("configuration", {})["phase5_pattern_v03_attempt_id"] = attempt_id
            else:
                seq_hash = observation_sequence_hash(state["run_id"], replay_attempt_id=attempt_id)
                complete_replay_attempt(
                    attempt_id=attempt_id,
                    observation_count=count_observations(state["run_id"], replay_attempt_id=attempt_id),
                    sequence_hash=seq_hash,
                )
        else:
            state["status"] = "PAUSED"
            cursor["status"] = "PAUSED"

        save_cursor(state, cursor)
        verify_cursor_obs_consistency(state, cursor)
        return _step_response(state, cursor, step, bars_payload)
    finally:
        lock.release()


def _cached_last_response(
    state: dict[str, Any],
    cursor: dict[str, Any],
    schedule: list[ReplayStep],
    completed: int,
) -> dict[str, Any]:
    step = schedule[completed - 1]
    return _step_response(state, cursor, step, [])


def get_replay_state(state: dict[str, Any]) -> dict[str, Any]:
    cursor = get_cursor(state)
    schedule = _schedule_cache(state)
    completed = int(cursor.get("completed_steps") or 0)
    return {
        "run_id": state["run_id"],
        "run_status": state.get("status"),
        "replay_cursor": cursor,
        "total_steps": len(schedule),
        "completed_steps": completed,
        "observations_total": _observation_total(state),
        "observations_expected": len(schedule) * len(state["symbols"]),
        "playback_speed": state.get("playback_speed"),
        "replay_attempt_id": state.get("active_replay_attempt_id"),
        "phase4_review_baseline_attempt_id": state.get("phase4_review_baseline_attempt_id"),
        "phase5_pattern_attempt_id": state.get("phase5_pattern_attempt_id")
        or state.get("configuration", {}).get("phase5_pattern_attempt_id"),
        "ruleset_version": state.get("ruleset_version", RULESET_VERSION),
        "replay_mode": state.get("replay_mode") or state.get("configuration", {}).get("replay_mode"),
    }


def get_visible_bars(state: dict[str, Any], symbol: str | None = None) -> dict[str, Any]:
    cursor = get_cursor(state)
    ts_raw = cursor.get("bar_timestamp_utc")
    cursor_ts = datetime.fromisoformat(str(ts_raw)[:19]) if ts_raw else None
    index = _bars_index(state)
    by_symbol = state.get("_frozen_bars_by_symbol") or _group_by_symbol(index, state["symbols"])
    view = ReplayBarView(
        bars_by_symbol=by_symbol,
        cursor_timestamp_utc=cursor_ts,
        symbols=state["symbols"],
    )
    if symbol:
        sym = symbol.upper()
        bars = view.visible_bars(sym)
        return {
            "run_id": state["run_id"],
            "symbol": sym,
            "bars": [_bar_dict(b) for b in bars],
            "cursor_timestamp_utc": ts_raw,
        }
    out: dict[str, list] = {}
    for sym in state["symbols"]:
        out[sym] = [_bar_dict(b) for b in view.visible_bars(sym)]
    return {"run_id": state["run_id"], "symbols": out, "cursor_timestamp_utc": ts_raw}


def assert_future_bar_denied(state: dict[str, Any], symbol: str, ts_utc: datetime) -> None:
    cursor = get_cursor(state)
    ts_raw = cursor.get("bar_timestamp_utc")
    cursor_ts = datetime.fromisoformat(str(ts_raw)[:19]) if ts_raw else None
    index = _bars_index(state)
    by_symbol = _group_by_symbol(index, state["symbols"])
    view = ReplayBarView(bars_by_symbol=by_symbol, cursor_timestamp_utc=cursor_ts, symbols=state["symbols"])
    view.assert_timestamp_allowed(ts_utc)


def replay_sequence_hash(state: dict[str, Any]) -> str:
    schedule = _schedule_cache(state)
    index = _bars_index(state)
    parts: list[str] = []
    for step in schedule:
        for sym in state["symbols"]:
            bar = index[(sym, step.trading_date, _normalize_utc(step.bar_timestamp_utc))]
            parts.append(
                f"{step.trading_date}|{step.bar_timestamp_ny.isoformat()}|{sym}|"
                f"{bar.open}|{bar.high}|{bar.low}|{bar.close}|{bar.volume}"
            )
    return hashlib.sha256("\n".join(parts).encode()).hexdigest()


def _bar_dict(bar: HistoricalBar) -> dict[str, Any]:
    return {
        "symbol": bar.symbol,
        "trading_date": bar.trading_date.isoformat(),
        "ts_utc": bar.ts_utc.isoformat(),
        "ts_ny": bar.ts_ny.isoformat(),
        "open": bar.open,
        "high": bar.high,
        "low": bar.low,
        "close": bar.close,
        "volume": bar.volume,
    }
