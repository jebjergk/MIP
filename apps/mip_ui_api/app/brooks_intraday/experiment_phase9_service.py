"""Phase 9A — API-facing experiment control (UI orchestration)."""

from __future__ import annotations

import socket
import os
from datetime import date, datetime, timezone
from typing import Any

from .calendar import resolve_week_sessions
from .experiment_analytics import build_week_report, list_validation_run_ids
from .experiment_comparison import build_comparison, get_aggregate_report
from .experiment_execution_repository import (
    append_event,
    get_active_execution,
    insert_execution,
    list_events,
    list_stages,
    recover_stale_running_executions,
    update_execution,
)
from .experiment_freeze import EXPERIMENT_ROLE, FREEZE_ID
from .experiment_phase9_constants import (
    ACTION_CANCEL,
    ACTION_PAUSE,
    ACTION_RESUME,
    ACTION_RETRY,
    ACTION_RUN_NEXT,
    ACTION_RUN_REMAINING,
    COHORT_WEEKS,
    COMPLETED_WEEK_RUN_ID,
    COMPLETED_WEEK_START,
    OVERALL_COMPLETED,
    OVERALL_FAILED_RECOVERABLE,
    OVERALL_PAUSED,
    OVERALL_READY,
    OVERALL_RUNNING,
    OVERALL_WAITING_TWS,
    STAGE_WEEK_COMPLETE,
)
from .experiment_phase9_engine import _is_week_complete_in_db, _next_pending_week, _week_key
from .experiment_phase9_tws import check_tws_connection


def worker_owner_id() -> str:
    return f"{socket.gethostname()}:{os.getpid()}"


def ensure_execution_record() -> dict[str, Any]:
    ex = get_active_execution()
    if ex:
        return ex
    progress: dict[str, Any] = {"weeks": {}}
    progress["weeks"][COMPLETED_WEEK_START] = {
        "status": STAGE_WEEK_COMPLETE,
        "run_id": COMPLETED_WEEK_RUN_ID,
        "locked": True,
    }
    completed_count = 1
    for ws in COHORT_WEEKS:
        if ws.isoformat() == COMPLETED_WEEK_START:
            continue
        done, rid = _is_week_complete_in_db(ws)
        if done and rid:
            progress["weeks"][ws.isoformat()] = {"status": STAGE_WEEK_COMPLETE, "run_id": rid}
            completed_count += 1
    status = OVERALL_COMPLETED if completed_count >= len(COHORT_WEEKS) else OVERALL_READY
    eid = insert_execution(
        freeze_id=FREEZE_ID,
        cohort=[w.isoformat() for w in COHORT_WEEKS],
        overall_status=status,
        progress=progress,
    )
    append_event(eid, "Phase 9A execution record initialized", severity="INFO")
    if completed_count >= 1:
        append_event(
            eid,
            f"Week {COMPLETED_WEEK_START} already complete (existing run preserved)",
            stage=STAGE_WEEK_COMPLETE,
        )
    ex = get_active_execution()
    assert ex
    return ex


def _week_card(execution_id: str, week_start: date, progress: dict) -> dict[str, Any]:
    wk = week_start.isoformat()
    week_cal = resolve_week_sessions(week_start)
    week_end = week_cal.trading_dates[-1].isoformat() if week_cal.trading_dates else wk
    meta = (progress.get("weeks") or {}).get(wk) or {}
    run_id = meta.get("run_id")
    done, rid = _is_week_complete_in_db(week_start)
    if rid and not run_id:
        run_id = rid
    stages = list_stages(execution_id, week_start)
    stage_map = {s.get("stage_key"): s for s in stages}

    bar_complete = 0
    dossier_complete = 0
    obj_id = pat_id = ctx_id = sim_id = None
    trades = None
    ending_cash = None
    week_status = "NOT_CREATED"
    latest_error = None

    if meta.get("status") == STAGE_WEEK_COMPLETE or done:
        week_status = STAGE_WEEK_COMPLETE
    elif run_id:
        week_status = meta.get("status") or "CREATED"
        try:
            from . import store

            store.get_run(run_id)
            with store._lock:
                st = store._runs[run_id]
            counts = (st.get("preparation") or {}).get("counts") or {}
            bar_complete = int(counts.get("bar_sessions_complete") or 0)
            dossier_complete = int(counts.get("dossiers_compiled") or 0)
            cfg = st.get("configuration") or {}
            obj_id = st.get("phase4_review_baseline_attempt_id") or cfg.get("phase4_review_baseline_attempt_id")
            pat_id = st.get("phase5_pattern_v03_attempt_id") or cfg.get("phase5_pattern_v03_attempt_id")
            ctx_id = cfg.get("phase6b_context_attempt_id")
            sim_id = cfg.get("simulation_attempt_id") or (cfg.get("simulation_summary") or {}).get(
                "simulation_attempt_id"
            )
            if bar_complete < 20:
                week_status = "ACQUIRING_BARS" if bar_complete else "CREATED"
            elif bar_complete >= 20:
                week_status = "BARS_READY"
            if obj_id:
                week_status = "OBJECTIVE_COMPLETE"
            if pat_id:
                week_status = "PATTERN_COMPLETE"
            if ctx_id:
                week_status = "CONTEXT_COMPLETE"
            if sim_id:
                week_status = "SIMULATION_COMPLETE"
        except Exception:
            week_status = "CREATED"

    if done or meta.get("status") == STAGE_WEEK_COMPLETE:
        week_status = STAGE_WEEK_COMPLETE
        if run_id:
            try:
                rep = build_week_report(run_id)
                sim = rep.get("simulation_layer") or {}
                trades = sim.get("trades")
                ending_cash = sim.get("ending_cash")
                aids = rep.get("attempt_ids") or {}
                obj_id = obj_id or aids.get("objective")
                pat_id = pat_id or aids.get("pattern")
                ctx_id = ctx_id or aids.get("context")
                sim_id = sim_id or aids.get("simulation")
                bar_complete = 20
                dossier_complete = 20
            except Exception:
                pass

    current_stage = None
    for sk in (
        "WEEK_ACQUISITION",
        "OBJECTIVE_REPLAY",
        "PATTERN_REPLAY",
        "CONTEXT_REPLAY",
        "SIMULATION_REPLAY",
    ):
        if stage_map.get(sk, {}).get("stage_status") == "RUNNING":
            current_stage = sk

    err_stage = next((s for s in stages if s.get("stage_status") == "FAILED"), None)
    if err_stage:
        latest_error = err_stage.get("error_json")

    return {
        "week_start": wk,
        "week_end": week_end,
        "experiment_role": EXPERIMENT_ROLE,
        "run_id": run_id,
        "week_status": week_status,
        "bar_sessions_complete": bar_complete,
        "bar_sessions_expected": 20,
        "dossiers_complete": dossier_complete,
        "dossiers_expected": 20,
        "objective_attempt_id": obj_id,
        "pattern_attempt_id": pat_id,
        "context_attempt_id": ctx_id,
        "simulation_attempt_id": sim_id,
        "current_stage": current_stage,
        "trades": trades,
        "ending_cash": ending_cash,
        "review_link": f"/research/brooks-intraday?run={run_id}" if run_id else None,
        "latest_error": latest_error,
        "stages": [
            {
                "stage_key": s.get("stage_key"),
                "status": s.get("stage_status"),
                "attempt_id": s.get("attempt_id"),
                "row_count": s.get("row_count"),
                "hash": s.get("content_hash"),
                "retry_count": s.get("retry_count"),
                "started_at": str(s.get("started_at") or "")[:19] or None,
                "finished_at": str(s.get("finished_at") or "")[:19] or None,
                "error": s.get("error_json"),
            }
            for s in stages
        ],
    }


def build_phase9_status() -> dict[str, Any]:
    ex = ensure_execution_record()
    progress = ex.get("progress_json") or {}
    weeks_complete = sum(
        1
        for ws in COHORT_WEEKS
        if (progress.get("weeks") or {}).get(ws.isoformat(), {}).get("status") == STAGE_WEEK_COMPLETE
        or _is_week_complete_in_db(ws)[0]
    )
    pending = _next_pending_week(progress)
    tws = ex.get("tws_state_json") or {}
    bars_prog = progress.get("bars") or {}
    bars_label = None
    if bars_prog.get("complete") is not None:
        sym = bars_prog.get("current_symbol")
        td = bars_prog.get("current_trading_date")
        if sym and td:
            bars_label = f"Bars: {bars_prog.get('complete')}/20 — acquiring {sym} {td}"
        else:
            bars_label = f"Bars: {bars_prog.get('complete')}/20 sessions complete"

    events_payload = get_phase9_events(limit=80)

    week_cards = [_week_card(ex["execution_id"], ws, progress) for ws in COHORT_WEEKS]

    aggregate = None
    comparison = None
    if weeks_complete >= len(COHORT_WEEKS):
        try:
            aggregate = get_aggregate_report()
            comparison = build_comparison(
                run_ids=[w["run_id"] for w in week_cards if w.get("run_id")],
                include_baseline=True,
            )
        except Exception:
            pass

    return {
        "execution_id": ex["execution_id"],
        "freeze_id": ex.get("freeze_id") or FREEZE_ID,
        "locked_weeks": [w.isoformat() for w in COHORT_WEEKS],
        "overall_status": ex.get("overall_status"),
        "completed_weeks_count": weeks_complete,
        "weeks_completed_count": weeks_complete,
        "total_weeks": len(COHORT_WEEKS),
        "weeks_total": len(COHORT_WEEKS),
        "current_active_week": str(ex.get("current_week_start") or "")[:10] or None,
        "current_week_start": str(ex.get("current_week_start") or "")[:10] or None,
        "current_stage": ex.get("current_stage"),
        "progress": {
            "bars_complete": bars_prog.get("complete"),
            "bars_total": bars_prog.get("total") or 20,
            "current_symbol": bars_prog.get("current_symbol"),
            "current_trading_date": bars_prog.get("current_trading_date"),
            "retry_count": bars_prog.get("retry_count"),
            "label": bars_label or (ex.get("current_stage") or "").replace("_", " ").title() or None,
        },
        "tws": tws,
        "last_successful_action": ex.get("last_success_action"),
        "last_success_action": ex.get("last_success_action"),
        "last_error": (
            {
                "message": (ex.get("latest_error_json") or {}).get("message")
                or str(ex.get("latest_error_json") or ""),
                "recoverable": (ex.get("latest_error_json") or {}).get("recoverable", True),
                "detail": ex.get("latest_error_json"),
            }
            if ex.get("latest_error_json")
            else None
        ),
        "next_automatic_action": ex.get("next_automatic_action"),
        "next_pending_week": pending.isoformat() if pending else None,
        "run_all_remaining": bool(ex.get("run_all_remaining")),
        "pause_requested": bool(ex.get("pause_requested")),
        "interrupted_prior_run": ex.get("overall_status") == OVERALL_PAUSED
        and "interrupted" in str(ex.get("last_success_action") or "").lower(),
        "weeks": week_cards,
        "activity": events_payload.get("events") or [],
        "aggregate_report": aggregate,
        "comparison": comparison,
        "comparison_summary": comparison,
        "phase9_complete": weeks_complete >= len(COHORT_WEEKS),
        "ui_links": {
            "comparison": "/research/brooks-intraday#phase9-comparison",
            "aggregate": "/research/brooks-intraday#phase9-aggregate",
        },
    }


def _queue_action(action: str, *, run_all: bool = False) -> dict[str, Any]:
    ex = ensure_execution_record()
    eid = ex["execution_id"]
    status = ex.get("overall_status")
    if action == ACTION_CANCEL:
        if status not in (OVERALL_READY, OVERALL_PAUSED):
            return {"ok": False, "message": "Cannot cancel — work already in progress or complete"}
        update_execution(eid, requested_action=ACTION_CANCEL, overall_status=OVERALL_READY)
        return {"ok": True, "execution_id": eid}

    if action == ACTION_PAUSE:
        update_execution(eid, pause_requested=True, requested_action=ACTION_PAUSE)
        append_event(eid, "Pause requested — will stop at next safe checkpoint", severity="INFO")
        return {"ok": True, "execution_id": eid}

    if action in (ACTION_RUN_NEXT, ACTION_RUN_REMAINING, ACTION_RESUME, ACTION_RETRY):
        pending = _next_pending_week(ex.get("progress_json") or {})
        if action == ACTION_RUN_NEXT and not pending:
            return {"ok": False, "message": "No pending validation weeks"}
        fields: dict[str, Any] = {
            "requested_action": action,
            "overall_status": OVERALL_RUNNING,
            "pause_requested": False,
            "latest_error_json": None,
        }
        if action == ACTION_RUN_REMAINING:
            fields["run_all_remaining"] = True
        if action == ACTION_RETRY and ex.get("overall_status") == OVERALL_WAITING_FOR_TWS:
            fields["overall_status"] = OVERALL_RUNNING
        if pending and not ex.get("current_week_start"):
            fields["current_week_start"] = pending
        update_execution(eid, **fields)
        append_event(eid, f"Queued {action}", severity="INFO")
        return {"ok": True, "execution_id": eid, "next_week": pending.isoformat() if pending else None}

    return {"ok": False, "message": "Unknown action"}


def startup_recovery() -> dict[str, Any]:
    n = recover_stale_running_executions(180)
    ex = ensure_execution_record()
    if n:
        append_event(
            ex["execution_id"],
            f"Recovered {n} stale RUNNING execution(s) — status PAUSED, safe to resume",
            severity="WARN",
            recoverable=True,
        )
        update_execution(
            ex["execution_id"],
            overall_status=OVERALL_PAUSED,
            last_success_action="Prior run interrupted — safe to resume",
        )
    return {"recovered": n, "execution_id": ex["execution_id"]}


def post_check_tws() -> dict[str, Any]:
    ex = ensure_execution_record()
    tws = check_tws_connection()
    update_execution(ex["execution_id"], tws_state_json=tws)
    if tws.get("readiness") == "CONNECTED_AND_READY":
        append_event(ex["execution_id"], "TWS connection successful", severity="INFO")
        if ex.get("overall_status") == OVERALL_WAITING_FOR_TWS:
            update_execution(
                ex["execution_id"],
                overall_status=OVERALL_PAUSED,
                next_automatic_action="Use Resume current week to continue",
            )
    return {"tws": tws, "status": build_phase9_status()}


def request_run_next() -> dict[str, Any]:
    return _queue_action(ACTION_RUN_NEXT)


def request_run_remaining() -> dict[str, Any]:
    return _queue_action(ACTION_RUN_REMAINING)


def request_pause() -> dict[str, Any]:
    return _queue_action(ACTION_PAUSE)


def request_resume() -> dict[str, Any]:
    return _queue_action(ACTION_RESUME)


def request_retry() -> dict[str, Any]:
    return _queue_action(ACTION_RETRY)


def request_cancel_pending() -> dict[str, Any]:
    return _queue_action(ACTION_CANCEL)


def check_tws_and_store() -> dict[str, Any]:
    return post_check_tws()["tws"]


def ensure_execution() -> dict[str, Any]:
    return ensure_execution_record()


def get_phase9_events(limit: int = 80) -> dict[str, Any]:
    ex = ensure_execution_record()
    events = list_events(ex["execution_id"], limit=limit)
    formatted = []
    for ev in events:
        ts = ev.get("event_ts")
        if isinstance(ts, datetime):
            ts_s = ts.strftime("%H:%M")
        else:
            ts_s = str(ts)[11:16] if ts else ""
        formatted.append(
            {
                "time": ts_s,
                "severity": ev.get("severity"),
                "stage": ev.get("stage"),
                "message": ev.get("message"),
                "symbol": ev.get("symbol"),
                "trading_date": str(ev.get("trading_date") or "")[:10] or None,
                "detail": ev.get("detail_json"),
                "recoverable": ev.get("recoverable"),
            }
        )
    return {"execution_id": ex["execution_id"], "events": formatted}
