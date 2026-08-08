"""Phase 9 — unseen-week validation pipeline orchestration."""

from __future__ import annotations

import logging
from datetime import date
from typing import Any, Callable

from .acquisition import acquire_historical_bars
from .constants import EXPECTED_RTH_5M_BARS_FULL_SESSION
from .errors import BrooksIntradayError
from .experiment_analytics import build_week_report
from .experiment_freeze import EXPERIMENT_ROLE, FREEZE_ID, build_freeze_record
from .experiment_week_selection import LOCKED_VALIDATION_WEEKS
from .models import CreateRunRequest
from .pattern_ruleset_v03 import RULESET_VERSION as PATTERN_V03
from .replay_engine import run_objective_replay_bulk, run_pattern_replay_bulk

logger = logging.getLogger(__name__)


def _require_bar_completeness(state: dict[str, Any]) -> None:
    prep = state.get("preparation") or {}
    counts = prep.get("counts") or {}
    complete = int(counts.get("bar_sessions_complete") or 0)
    expected = int(counts.get("bar_sessions_expected") or 20)
    if complete != expected or expected != 20:
        hist = state.get("historical_data") or {}
        raise BrooksIntradayError(
            "VALIDATION_BAR_INCOMPLETE",
            f"Expected 20/20 complete sessions, got {complete}/{expected}. "
            f"Replay readiness={ (state.get('readiness') or {}).get('replay_readiness') }. "
            f"Ensure IBKR historical fetch is available (see brooks_phase2b_acquire_week.py). "
            f"Historical summary keys={list(hist.keys())[:5]}",
            run_id=state.get("run_id"),
        )
    matrix = prep.get("matrix") or []
    for row in matrix:
        if int(row.get("bar_count") or 0) != EXPECTED_RTH_5M_BARS_FULL_SESSION:
            raise BrooksIntradayError(
                "VALIDATION_BAR_SESSION_INVALID",
                f"Session {row.get('symbol')} {row.get('trading_date')} bar_count={row.get('bar_count')}.",
                run_id=state.get("run_id"),
            )


def run_analysis_chain(state: dict[str, Any]) -> dict[str, str]:
    """Objective → pattern V0.3 → context V0.2 → simulation V0.1 (bulk)."""
    from . import store
    from .lab_execution_policy import legacy_pipeline_env_enabled

    allow = legacy_pipeline_env_enabled()
    run_id = state["run_id"]
    obj_id = run_objective_replay_bulk(state, progress_every=50, allow_diagnostic_legacy=allow)
    state["phase4_review_baseline_attempt_id"] = obj_id
    state.setdefault("configuration", {})["phase4_review_baseline_attempt_id"] = obj_id

    state["pattern_ruleset_version"] = PATTERN_V03
    state["ruleset_version"] = PATTERN_V03
    state.setdefault("configuration", {})["pattern_ruleset_version"] = PATTERN_V03
    state.setdefault("configuration", {})["ruleset_version"] = PATTERN_V03
    state.pop("_pattern_session_state", None)
    pat_id = run_pattern_replay_bulk(state, progress_every=50, allow_diagnostic_legacy=allow)
    state["phase5_pattern_v03_attempt_id"] = pat_id
    state.setdefault("configuration", {})["phase5_pattern_v03_attempt_id"] = pat_id

    ctx_out = store.run_context_bulk_v02(run_id, allow_diagnostic_legacy=allow)
    ctx_id = str(ctx_out["context_attempt_id"])

    with store._lock:
        state = store._runs[run_id]

    sim_out = store.run_simulation_bulk(run_id, context_attempt_id=ctx_id, allow_diagnostic_legacy=allow)
    sim_id = str(sim_out.get("simulation_attempt_id") or "")
    return {
        "objective_attempt_id": obj_id,
        "pattern_attempt_id": pat_id,
        "context_attempt_id": ctx_id,
        "simulation_attempt_id": sim_id,
    }


def bootstrap_validation_run(
    week_start: date,
    *,
    created_by: str = "phase9_agent",
) -> dict[str, Any]:
    from . import store

    freeze = build_freeze_record()
    req = CreateRunRequest(
        week_start=week_start,
        experiment_role=EXPERIMENT_ROLE,
        experiment_freeze_id=FREEZE_ID,
        created_by=created_by,
    )
    detail = store.create_run(req)
    run_id = detail.run_id
    with store._lock:
        state = store._runs[run_id]
        state["configuration"]["experiment_freeze"] = freeze
        state["configuration"]["baseline_run_id"] = freeze["baseline_run_id"]
        state["configuration"]["selected_week_start"] = week_start.isoformat()
    store._persist_run_header(state)
    return {"run_id": run_id, "week_start": week_start.isoformat(), "freeze_id": FREEZE_ID}


def execute_validation_week(
    week_start: date,
    *,
    ib_fetch: Callable[[str, date], Any] | None = None,
    max_acquire_passes: int = 25,
) -> dict[str, Any]:
    """
    Full pipeline for one unseen week: reconstruct → acquire → prepare → frozen analysis chain.
    """
    from . import store

    boot = bootstrap_validation_run(week_start)
    run_id = boot["run_id"]
    store.get_run(run_id)

    with store._lock:
        state = store._runs[run_id]
        if state.get("preparation") is None:
            state["preparation"] = {}
        store._runs[run_id] = state

    store.run_prepare(run_id)
    store.run_reconstruct_dossiers(run_id)
    with store._lock:
        state = store._runs[run_id]

    for pass_num in range(1, max_acquire_passes + 1):
        with store._lock:
            state = store._runs[run_id]
        acquire_historical_bars(state, ib_fetch=ib_fetch, retry_failed_only=(pass_num > 1))
        with store._lock:
            store._runs[run_id] = state
            store._persist_run_header(state)
        readiness = state.get("readiness") or {}
        counts = (state.get("preparation") or {}).get("counts") or {}
        logger.info(
            "acquire pass %s/%s complete=%s/%s replay=%s frozen=%s",
            pass_num,
            max_acquire_passes,
            counts.get("bar_sessions_complete"),
            counts.get("bar_sessions_expected"),
            readiness.get("replay_readiness"),
            state.get("bars_frozen"),
        )
        if readiness.get("replay_readiness") == "READY" and state.get("bars_frozen"):
            break

    prep = store.run_prepare(run_id)
    with store._lock:
        state = store._runs[run_id]
    _require_bar_completeness(state)

    attempts = run_analysis_chain(state)
    with store._lock:
        store._runs[run_id] = state
        store._persist_run_header(state)

    report = build_week_report(run_id)
    return {
        "run_id": run_id,
        "week_start": week_start.isoformat(),
        "preparation_status": prep.get("preparation_status"),
        "attempts": attempts,
        "week_report": report,
    }


def execute_locked_validation_batch(
    *,
    ib_fetch: Callable[[str, date], Any] | None = None,
    weeks: tuple[date, ...] | None = None,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for ws in weeks or LOCKED_VALIDATION_WEEKS:
        logger.info("Phase 9 validation week %s", ws.isoformat())
        results.append(execute_validation_week(ws, ib_fetch=ib_fetch))
    return results
