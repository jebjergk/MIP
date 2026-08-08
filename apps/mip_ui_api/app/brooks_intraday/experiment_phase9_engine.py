"""Phase 9A — one safe work unit per tick (historical data + replay only)."""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Any

from .calendar import resolve_week_sessions
from .experiment_analytics import build_week_report, list_validation_run_ids
from .experiment_execution_repository import (
    append_event,
    get_active_execution,
    list_stages,
    renew_lease,
    update_execution,
    upsert_stage,
)
from .experiment_freeze import EXPERIMENT_ROLE
from .experiment_phase9_constants import (
    ACTION_CANCEL,
    COMPLETED_WEEK_RUN_ID,
    COMPLETED_WEEK_START,
    COHORT_WEEKS,
    FREEZE,
    LEASE_SEC,
    OVERALL_COMPLETED,
    OVERALL_FAILED_RECOVERABLE,
    OVERALL_FAILED_TERMINAL,
    OVERALL_PAUSED,
    OVERALL_READY,
    OVERALL_RUNNING,
    OVERALL_WAITING_TWS,
    PIPELINE_STAGES,
    STAGE_BAR_VALIDATION,
    STAGE_CONTEXT_REPLAY,
    STAGE_DOSSIER_RECONSTRUCTION,
    STAGE_DOSSIER_VALIDATION,
    STAGE_IB_PREFLIGHT,
    STAGE_OBJECTIVE_REPLAY,
    STAGE_PATTERN_REPLAY,
    STAGE_SESSION_PROBE,
    STAGE_SIMULATION_REPLAY,
    STAGE_WEEK_ACQUISITION,
    STAGE_WEEK_COMPLETE,
    STAGE_WEEK_REPORT,
    TWS_CONNECTED,
)
from .experiment_phase9_tws import check_tws_connection
from .experiment_pipeline import _require_bar_completeness, bootstrap_validation_run
from .experiment_staged_acquisition import (
    _find_validation_run_id,
    acquire_next_incomplete_session,
    paced_fetch,
    persist_single_session,
    probe_session_readonly,
)
from .ib_historical_provider import IbHistoricalProviderError

logger = logging.getLogger(__name__)

PROBE_SYMBOL = "AAPL"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _week_key(d: date) -> str:
    return d.isoformat()


def _progress_weeks(progress: dict | None) -> dict[str, Any]:
    p = dict(progress or {})
    p.setdefault("weeks", {})
    return p


def _is_week_complete_in_db(week_start: date) -> tuple[bool, str | None]:
    ws = week_start.isoformat()
    if ws == COMPLETED_WEEK_START:
        return True, COMPLETED_WEEK_RUN_ID
    for rec in list_validation_run_ids():
        if str(rec.get("selected_week_start") or "")[:10] != ws:
            continue
        cfg = rec.get("config_json") or {}
        sim = cfg.get("simulation_summary") or {}
        if cfg.get("phase6b_context_attempt_id") and (
            cfg.get("phase4_review_baseline_attempt_id") or cfg.get("phase5_pattern_v03_attempt_id")
        ):
            if sim.get("simulation_attempt_id") or cfg.get("simulation_attempt_id"):
                return True, str(rec.get("run_id"))
        rid = str(rec.get("run_id"))
        try:
            rep = build_week_report(rid)
            if (rep.get("simulation_layer") or {}).get("simulation_attempt_id") or rep.get("attempt_ids", {}).get(
                "simulation"
            ):
                return True, rid
        except Exception:
            pass
    return False, None


def _next_pending_week(progress: dict) -> date | None:
    for ws in COHORT_WEEKS:
        wk = _week_key(ws)
        wrec = (progress.get("weeks") or {}).get(wk) or {}
        if wrec.get("status") == STAGE_WEEK_COMPLETE:
            continue
        done, _ = _is_week_complete_in_db(ws)
        if done:
            continue
        return ws
    return None


def _run_state(run_id: str) -> dict[str, Any]:
    from . import store

    store.get_run(run_id)
    with store._lock:
        return dict(store._runs[run_id])


def _stage_done(stages: list[dict], week: date, key: str) -> bool:
    wk = week.isoformat()
    for s in stages:
        ws = str(s.get("week_start") or "")[:10]
        if ws == wk and s.get("stage_key") == key and s.get("stage_status") == "COMPLETE":
            return True
    return False


def _apply_pause_if_requested(execution: dict[str, Any], owner: str) -> bool:
    """Honor PAUSE_REQUESTED from Snowflake (never trust a stale in-memory execution dict)."""
    eid = execution["execution_id"]
    fresh = get_active_execution()
    if fresh and str(fresh.get("execution_id")) == str(eid):
        execution = fresh
    if not execution.get("pause_requested"):
        return False
    update_execution(
        execution["execution_id"],
        overall_status=OVERALL_PAUSED,
        pause_requested=False,
        requested_action=None,
        current_stage=execution.get("current_stage"),
        next_automatic_action="Resume current week when ready",
    )
    append_event(
        execution["execution_id"],
        "Paused at safe checkpoint",
        stage=execution.get("current_stage"),
        recoverable=True,
    )
    return True


def _set_waiting_tws(execution_id: str, tws: dict, err: dict | None = None) -> None:
    update_execution(
        execution_id,
        overall_status=OVERALL_WAITING_TWS,
        tws_state_json=tws,
        latest_error_json=err,
        next_automatic_action="Open and log in to TWS, then use Retry connection or Resume.",
    )


def _run_pipeline_stage(
    state: dict[str, Any],
    stage_key: str,
    *,
    execution_id: str | None = None,
) -> dict[str, Any]:
    from . import store
    from .experiment_progress import make_progress_callback, patch_stage_progress
    from .lab_execution_policy import legacy_pipeline_env_enabled
    from .replay_engine import run_objective_replay_bulk, run_pattern_replay_bulk
    from .pattern_ruleset_v03 import RULESET_VERSION as PATTERN_V03

    allow = legacy_pipeline_env_enabled()
    run_id = state["run_id"]
    if stage_key == STAGE_OBJECTIVE_REPLAY:
        if state.get("phase4_review_baseline_attempt_id"):
            return {"attempt_id": state["phase4_review_baseline_attempt_id"], "skipped": True}
        cb = make_progress_callback(execution_id, stage=stage_key, total=1560, unit="observations", every=100)
        obj_id = run_objective_replay_bulk(
            state, progress_every=50, on_progress=cb, allow_diagnostic_legacy=allow
        )
        state["phase4_review_baseline_attempt_id"] = obj_id
        state.setdefault("configuration", {})["phase4_review_baseline_attempt_id"] = obj_id
        with store._lock:
            store._runs[run_id] = state
            store._persist_run_header(state)
        if execution_id:
            patch_stage_progress(execution_id, stage=stage_key, completed=1560, total=1560, clear=True)
        return {"attempt_id": obj_id, "row_count": 1560}

    if stage_key == STAGE_PATTERN_REPLAY:
        if state.get("phase5_pattern_v03_attempt_id"):
            return {"attempt_id": state["phase5_pattern_v03_attempt_id"], "skipped": True}
        state["pattern_ruleset_version"] = PATTERN_V03
        state["ruleset_version"] = PATTERN_V03
        state.setdefault("configuration", {})["pattern_ruleset_version"] = PATTERN_V03
        state.pop("_pattern_session_state", None)
        # Pattern schedule length equals bar-steps; progress unit is schedule steps.
        sched_total = 390
        cb = make_progress_callback(execution_id, stage=stage_key, total=sched_total, unit="schedule_steps", every=50)
        pat_id = run_pattern_replay_bulk(
            state, progress_every=50, on_progress=cb, allow_diagnostic_legacy=allow
        )
        state["phase5_pattern_v03_attempt_id"] = pat_id
        state.setdefault("configuration", {})["phase5_pattern_v03_attempt_id"] = pat_id
        with store._lock:
            store._runs[run_id] = state
            store._persist_run_header(state)
        if execution_id:
            patch_stage_progress(execution_id, stage=stage_key, completed=sched_total, total=sched_total, clear=True)
        return {"attempt_id": pat_id, "row_count": 544}

    if stage_key == STAGE_CONTEXT_REPLAY:
        cfg = state.get("configuration") or {}
        if cfg.get("phase6b_context_attempt_id"):
            return {"attempt_id": cfg["phase6b_context_attempt_id"], "skipped": True}
        cb = make_progress_callback(execution_id, stage=stage_key, total=1560, unit="observations", every=100)
        ctx_out = store.run_context_bulk_v02(run_id, on_progress=cb, allow_diagnostic_legacy=allow)
        ctx_id = str(ctx_out["context_attempt_id"])
        with store._lock:
            state = store._runs[run_id]
        if execution_id:
            patch_stage_progress(execution_id, stage=stage_key, completed=1560, total=1560, clear=True)
        return {"attempt_id": ctx_id, "row_count": 1560}

    if stage_key == STAGE_SIMULATION_REPLAY:
        cfg = state.get("configuration") or {}
        ctx_id = cfg.get("phase6b_context_attempt_id")
        if cfg.get("simulation_attempt_id"):
            return {"attempt_id": cfg["simulation_attempt_id"], "skipped": True}
        sim_out = store.run_simulation_bulk(
            run_id, context_attempt_id=str(ctx_id), allow_diagnostic_legacy=allow
        )
        sim_id = str(sim_out.get("simulation_attempt_id") or "")
        with store._lock:
            state = store._runs[run_id]
            store._persist_run_header(state)
        return {"attempt_id": sim_id, "row_count": 390}

    raise ValueError(f"Unknown pipeline stage {stage_key}")


def execute_work_unit(execution: dict[str, Any], owner: str) -> None:
    """Process exactly one durable checkpoint."""
    eid = execution["execution_id"]
    renew_lease(eid, owner, LEASE_SEC)

    if execution.get("requested_action") == ACTION_CANCEL:
        update_execution(
            eid,
            requested_action=None,
            overall_status=OVERALL_READY,
            next_automatic_action="Cancelled before start — use Run next validation week",
        )
        append_event(eid, "Pending work cancelled", severity="INFO")
        return

    if _apply_pause_if_requested(execution, owner):
        return

    progress = _progress_weeks(execution.get("progress_json"))
    stages = list_stages(eid)

    # All weeks done?
    pending = _next_pending_week(progress)
    if pending is None and not execution.get("run_all_remaining"):
        if execution.get("overall_status") != OVERALL_COMPLETED:
            from .experiment_comparison import build_comparison, get_aggregate_report

            agg = get_aggregate_report()
            update_execution(
                eid,
                overall_status=OVERALL_COMPLETED,
                current_stage=STAGE_WEEK_COMPLETE,
                progress_json={**progress, "aggregate_ready": True},
                next_automatic_action="Phase 9 complete — review reports in the Lab",
                last_success_action="All validation weeks complete",
            )
            append_event(eid, "Phase 9 complete — 3/3 unseen weeks processed", stage=STAGE_WEEK_COMPLETE)
        return

    week_start = execution.get("current_week_start")
    if week_start is not None and not isinstance(week_start, date):
        week_start = date.fromisoformat(str(week_start)[:10])
    if week_start is None or (progress.get("weeks") or {}).get(_week_key(week_start), {}).get("status") == STAGE_WEEK_COMPLETE:
        week_start = pending
        if week_start is None:
            return
        update_execution(
            eid,
            current_week_start=week_start,
            validation_run_id=_find_validation_run_id(week_start),
            current_stage=STAGE_IB_PREFLIGHT,
        )
        execution = {**execution, "current_week_start": week_start}

    ws = week_start
    wk_str = _week_key(ws)
    week_meta = progress["weeks"].setdefault(wk_str, {})

    run_id = execution.get("validation_run_id") or _find_validation_run_id(ws)
    if not run_id:
        run_id = None

    # Sync already-complete week from DB without re-running
    done_db, rid_db = _is_week_complete_in_db(ws)
    if done_db and rid_db and week_meta.get("status") != STAGE_WEEK_COMPLETE:
        week_meta.update({"status": STAGE_WEEK_COMPLETE, "run_id": rid_db})
        progress["weeks"][wk_str] = week_meta
        update_execution(
            eid,
            progress_json=progress,
            validation_run_id=rid_db,
            last_success_action=f"Week {wk_str} already complete",
            current_stage=STAGE_WEEK_COMPLETE,
        )
        append_event(eid, f"Week {wk_str} marked complete (existing run)", week_start=ws, stage=STAGE_WEEK_COMPLETE)
        if execution.get("run_all_remaining"):
            update_execution(eid, current_week_start=None, validation_run_id=None, current_stage=None)
        return

    stage = execution.get("current_stage") or STAGE_IB_PREFLIGHT

    # Non-IB pipeline stages after bars — skip TWS if past acquisition
    ib_stages = {STAGE_IB_PREFLIGHT, STAGE_SESSION_PROBE, STAGE_WEEK_ACQUISITION}
    if stage in ib_stages or stage is None:
        tws = check_tws_connection()
        update_execution(eid, tws_state_json=tws)
        if tws.get("readiness") != TWS_CONNECTED:
            _set_waiting_tws(
                eid,
                tws,
                {"message": "TWS unavailable", "readiness": tws.get("readiness"), "recoverable": True},
            )
            append_event(
                eid,
                f"TWS not ready ({tws.get('readiness')})",
                severity="WARN",
                stage=stage,
                recoverable=True,
            )
            return

    if stage == STAGE_IB_PREFLIGHT:
        upsert_stage(eid, ws, STAGE_IB_PREFLIGHT, stage_status="COMPLETE", finished_at=_utc_now())
        update_execution(
            eid,
            current_stage=STAGE_SESSION_PROBE,
            last_success_action="TWS connection successful",
        )
        append_event(eid, "TWS connection successful", stage=STAGE_IB_PREFLIGHT, week_start=ws)
        return

    if stage == STAGE_SESSION_PROBE:
        week_cal = resolve_week_sessions(ws)
        probe_date = week_cal.sessions[0].trading_date
        if not run_id:
            from . import store

            boot = bootstrap_validation_run(ws, created_by="phase9a_ui")
            run_id = boot["run_id"]
            store.get_run(run_id)
            with store._lock:
                st = store._runs[run_id]
                if st.get("preparation") is None:
                    st["preparation"] = {}
            store.run_prepare(run_id)
            store.run_reconstruct_dossiers(run_id)
            update_execution(eid, validation_run_id=run_id)
        state = _run_state(run_id)
        counts = (state.get("preparation") or {}).get("counts") or {}
        if int(counts.get("bar_sessions_complete") or 0) >= 20:
            update_execution(eid, current_stage=STAGE_BAR_VALIDATION, validation_run_id=run_id)
            return
        need_probe = week_meta.get("session_probe_ok") is not True
        if need_probe:
            probe = probe_session_readonly(PROBE_SYMBOL, probe_date)
            if not probe.get("fetch_ok") or not (probe.get("analysis") or {}).get("exactly_78"):
                err = probe.get("ib_error") or {"message": "Session probe failed"}
                if isinstance(err, dict) and "PACING" in str(err.get("code", "")):
                    _set_waiting_tws(eid, check_tws_connection(), err)
                    return
                update_execution(
                    eid,
                    overall_status=OVERALL_FAILED_RECOVERABLE,
                    latest_error_json={"stage": STAGE_SESSION_PROBE, **err},
                )
                append_event(eid, "Session probe failed", severity="ERROR", stage=STAGE_SESSION_PROBE, recoverable=True)
                return
            persist_result = persist_single_session(PROBE_SYMBOL, probe_date, run_id=run_id)
            if not persist_result.get("ok"):
                update_execution(
                    eid,
                    overall_status=OVERALL_FAILED_RECOVERABLE,
                    latest_error_json={"stage": STAGE_SESSION_PROBE, "result": persist_result},
                )
                return
            week_meta["session_probe_ok"] = True
            progress["weeks"][wk_str] = week_meta
            update_execution(eid, progress_json=progress, validation_run_id=run_id)
            append_event(
                eid,
                f"Session probe OK — stored 78 bars for {PROBE_SYMBOL} {probe_date}",
                stage=STAGE_SESSION_PROBE,
                week_start=ws,
                symbol=PROBE_SYMBOL,
                trading_date=probe_date,
            )
        update_execution(eid, current_stage=STAGE_WEEK_ACQUISITION, validation_run_id=run_id)
        return

    if stage == STAGE_WEEK_ACQUISITION:
        if not run_id:
            update_execution(eid, current_stage=STAGE_SESSION_PROBE)
            return
        state = _run_state(run_id)
        from . import store

        try:
            result = acquire_next_incomplete_session(state, ib_fetch=paced_fetch)
        except IbHistoricalProviderError as exc:
            _set_waiting_tws(
                eid,
                check_tws_connection(),
                {"code": exc.code, "message": exc.message, "recoverable": True},
            )
            append_event(
                eid,
                f"IB error: {exc.message}",
                severity="WARN",
                stage=STAGE_WEEK_ACQUISITION,
                recoverable=True,
            )
            return
        with store._lock:
            store._runs[run_id] = state
            store._persist_run_header(state)
        progress["bars"] = {
            "complete": result.get("bar_sessions_complete"),
            "total": result.get("total_sessions", 20),
            "current_symbol": result.get("symbol"),
            "current_trading_date": result.get("trading_date"),
            "retry_count": result.get("retry_attempt"),
        }
        progress["weeks"][wk_str] = week_meta
        update_execution(eid, progress_json=progress, validation_run_id=run_id)

        if result.get("action") == "acquired":
            append_event(
                eid,
                f"Stored 78 bars — {result.get('symbol')} {result.get('trading_date')}",
                stage=STAGE_WEEK_ACQUISITION,
                week_start=ws,
                symbol=result.get("symbol"),
                trading_date=date.fromisoformat(str(result.get("trading_date"))[:10])
                if result.get("trading_date")
                else None,
            )
            append_event(
                eid,
                f"Progress {result.get('bar_sessions_complete')}/20",
                stage=STAGE_WEEK_ACQUISITION,
                week_start=ws,
            )
            update_execution(
                eid,
                last_success_action=f"Acquired {result.get('symbol')} {result.get('trading_date')}",
            )
            if _apply_pause_if_requested(execution, owner):
                return
            return

        if result.get("action") == "failed":
            update_execution(
                eid,
                overall_status=OVERALL_FAILED_RECOVERABLE,
                latest_error_json={"stage": STAGE_WEEK_ACQUISITION, "error": result.get("error")},
            )
            append_event(
                eid,
                f"Failed {result.get('symbol')} {result.get('trading_date')}",
                severity="ERROR",
                stage=STAGE_WEEK_ACQUISITION,
                recoverable=True,
            )
            return

        update_execution(eid, current_stage=STAGE_BAR_VALIDATION)
        return

    # Pipeline stages (one per tick; bulk runs inside stage)
    if stage == STAGE_BAR_VALIDATION:
        from . import store

        state = _run_state(run_id)
        store.run_prepare(run_id)
        state = _run_state(run_id)
        try:
            _require_bar_completeness(state)
        except Exception as exc:
            update_execution(
                eid,
                overall_status=OVERALL_FAILED_RECOVERABLE,
                latest_error_json={"stage": STAGE_BAR_VALIDATION, "message": str(exc)},
            )
            return
        upsert_stage(eid, ws, STAGE_BAR_VALIDATION, stage_status="COMPLETE", finished_at=_utc_now(), row_count=20)
        update_execution(eid, current_stage=STAGE_DOSSIER_RECONSTRUCTION, last_success_action="Bars 20/20 validated")
        append_event(eid, "Bars 20/20 complete", stage=STAGE_BAR_VALIDATION, week_start=ws)
        return

    if stage == STAGE_DOSSIER_RECONSTRUCTION:
        from . import store

        if not _stage_done(stages, ws, STAGE_DOSSIER_RECONSTRUCTION):
            store.run_reconstruct_dossiers(run_id)
            upsert_stage(
                eid,
                ws,
                STAGE_DOSSIER_RECONSTRUCTION,
                stage_status="COMPLETE",
                finished_at=_utc_now(),
            )
        update_execution(eid, current_stage=STAGE_DOSSIER_VALIDATION)
        return

    if stage == STAGE_DOSSIER_VALIDATION:
        state = _run_state(run_id)
        counts = (state.get("preparation") or {}).get("counts") or {}
        if int(counts.get("dossiers_compiled") or 0) < 20:
            from . import store

            store.run_reconstruct_dossiers(run_id)
            state = _run_state(run_id)
            counts = (state.get("preparation") or {}).get("counts") or {}
        upsert_stage(
            eid,
            ws,
            STAGE_DOSSIER_VALIDATION,
            stage_status="COMPLETE",
            finished_at=_utc_now(),
            row_count=int(counts.get("dossiers_compiled") or 0),
        )
        update_execution(eid, current_stage=STAGE_OBJECTIVE_REPLAY)
        return

    if stage in (STAGE_OBJECTIVE_REPLAY, STAGE_PATTERN_REPLAY, STAGE_CONTEXT_REPLAY, STAGE_SIMULATION_REPLAY):
        if _stage_done(stages, ws, stage):
            idx = PIPELINE_STAGES.index(stage)
            nxt = PIPELINE_STAGES[idx + 1] if idx + 1 < len(PIPELINE_STAGES) else STAGE_WEEK_COMPLETE
            update_execution(eid, current_stage=nxt)
            return
        state = _run_state(run_id)
        upsert_stage(eid, ws, stage, stage_status="RUNNING", started_at=_utc_now())
        try:
            out = _run_pipeline_stage(state, stage, execution_id=eid)
        except Exception as exc:
            upsert_stage(
                eid,
                ws,
                stage,
                stage_status="FAILED",
                error_json={"message": str(exc)},
                finished_at=_utc_now(),
            )
            update_execution(
                eid,
                overall_status=OVERALL_FAILED_RECOVERABLE,
                latest_error_json={"stage": stage, "message": str(exc), "recoverable": True},
            )
            append_event(eid, str(exc), severity="ERROR", stage=stage, recoverable=True)
            return
        upsert_stage(
            eid,
            ws,
            stage,
            stage_status="COMPLETE",
            attempt_id=out.get("attempt_id"),
            row_count=out.get("row_count"),
            finished_at=_utc_now(),
        )
        idx = PIPELINE_STAGES.index(stage)
        nxt = PIPELINE_STAGES[idx + 1] if idx + 1 < len(PIPELINE_STAGES) else STAGE_WEEK_REPORT
        update_execution(
            eid,
            current_stage=nxt,
            last_success_action=f"{stage} complete",
            latest_error_json=None,
        )
        append_event(eid, f"{stage} complete", stage=stage, week_start=ws)
        if _apply_pause_if_requested(execution, owner):
            return
        return

    if stage == STAGE_WEEK_REPORT:
        rep = build_week_report(run_id)
        upsert_stage(eid, ws, STAGE_WEEK_REPORT, stage_status="COMPLETE", finished_at=_utc_now())
        week_meta.update(
            {
                "status": STAGE_WEEK_COMPLETE,
                "run_id": run_id,
                "report": {
                    "trades": (rep.get("simulation_layer") or {}).get("trades"),
                    "ending_cash": (rep.get("simulation_layer") or {}).get("ending_cash"),
                },
            }
        )
        progress["weeks"][wk_str] = week_meta
        upsert_stage(eid, ws, STAGE_WEEK_COMPLETE, stage_status="COMPLETE", finished_at=_utc_now())
        append_event(eid, f"Week {wk_str} complete", stage=STAGE_WEEK_COMPLETE, week_start=ws)
        update_execution(
            eid,
            progress_json=progress,
            current_stage=None,
            current_week_start=None,
            validation_run_id=None,
            last_success_action=f"Week {wk_str} complete",
            overall_status=OVERALL_RUNNING if execution.get("run_all_remaining") else OVERALL_READY,
        )
        if execution.get("run_all_remaining"):
            nxt = _next_pending_week(progress)
            if nxt is None:
                update_execution(eid, overall_status=OVERALL_COMPLETED, next_automatic_action="Phase 9 complete")
            else:
                update_execution(
                    eid,
                    current_week_start=nxt,
                    current_stage=STAGE_IB_PREFLIGHT,
                    next_automatic_action=f"Continuing with week {nxt.isoformat()}",
                )
        else:
            update_execution(eid, overall_status=OVERALL_READY, next_automatic_action="Run next validation week or review results")
        return

    logger.warning("phase9 unknown stage %s", stage)
