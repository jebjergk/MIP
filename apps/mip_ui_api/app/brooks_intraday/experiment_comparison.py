"""Phase 9 — cross-run comparison API payloads."""

from __future__ import annotations

from typing import Any

from .experiment_analytics import build_aggregate_report, build_week_report, list_validation_run_ids
from .experiment_freeze import BASELINE_RUN_ID, FREEZE_ID, build_freeze_record
from .learning_constants import PILOT_RUN_ID


def get_ruleset_freeze() -> dict[str, Any]:
    return build_freeze_record()


def get_validation_overview() -> dict[str, Any]:
    rows = list_validation_run_ids()
    overview = []
    for rec in rows:
        cfg = rec.get("config_json") or {}
        sim = cfg.get("simulation_summary") or {}
        ctx_id = cfg.get("phase6b_context_attempt_id")
        overview.append(
            {
                "run_id": rec.get("run_id"),
                "week_start": str(rec.get("selected_week_start") or "")[:10],
                "status": rec.get("status"),
                "experiment_freeze_id": cfg.get("experiment_freeze_id"),
                "ending_cash": rec.get("ending_cash") or sim.get("ending_cash"),
                "trades": sim.get("trade_count"),
                "realized_pnl": rec.get("realized_pnl") or sim.get("realized_pnl"),
                "context_attempt_id": ctx_id,
                "review_link": f"/research/brooks-intraday?run={rec.get('run_id')}",
            }
        )
    return {
        "freeze_id": FREEZE_ID,
        "baseline_run_id": BASELINE_RUN_ID,
        "validation_runs": overview,
        "count": len(overview),
    }


def build_comparison(*, run_ids: list[str], include_baseline: bool = True) -> dict[str, Any]:
    ids = list(run_ids)
    if include_baseline and PILOT_RUN_ID not in ids:
        ids.insert(0, PILOT_RUN_ID)
    weeks = []
    for rid in ids:
        try:
            weeks.append(build_week_report(rid))
        except Exception as exc:
            weeks.append({"run_id": rid, "error": str(exc)})
    conv = _conversion_metrics(weeks)
    return {
        "freeze_id": FREEZE_ID,
        "runs_compared": ids,
        "weeks": weeks,
        "conversion_metrics": conv,
        "account_by_week": [
            {
                "run_id": w.get("run_id"),
                "week_start": w.get("week_start"),
                "starting_cash": (w.get("simulation_layer") or {}).get("starting_cash"),
                "ending_cash": (w.get("simulation_layer") or {}).get("ending_cash"),
                "trades": (w.get("simulation_layer") or {}).get("trades"),
                "realized_pnl": (w.get("simulation_layer") or {}).get("realized_pnl"),
            }
            for w in weeks
            if "error" not in w
        ],
    }


def _conversion_metrics(weeks: list[dict[str, Any]]) -> dict[str, Any]:
    total = len([w for w in weeks if "error" not in w])
    no_trade = sum(1 for w in weeks if (w.get("simulation_layer") or {}).get("trades") == 0)
    entry_armed = sum((w.get("context_layer") or {}).get("entry_armed_events") or 0 for w in weeks)
    consider = sum((w.get("context_layer") or {}).get("consider_entry_events") or 0 for w in weeks)
    trades = sum((w.get("simulation_layer") or {}).get("trades") or 0 for w in weeks)
    return {
        "no_trade_week_pct": round(no_trade / total, 4) if total else None,
        "entry_armed_events_total": entry_armed,
        "consider_entry_events_total": consider,
        "trades_total": trades,
        "consider_entry_to_trade_conversion": round(trades / consider, 4) if consider else None,
        "entry_armed_to_consider_conversion": round(consider / entry_armed, 4) if entry_armed else None,
    }


def get_aggregate_report(*, run_ids: list[str] | None = None) -> dict[str, Any]:
    if run_ids is None:
        run_ids = [r["run_id"] for r in list_validation_run_ids()]
    if not run_ids:
        return build_aggregate_report([])
    return build_aggregate_report(run_ids)


def list_trades_filtered(
    *,
    run_id: str | None = None,
    symbol: str | None = None,
    win_only: bool | None = None,
    exit_reason: str | None = None,
    context_attempt_id: str | None = None,
    simulation_attempt_id: str | None = None,
) -> dict[str, Any]:
    from .experiment_analytics import build_trade_reviews, list_validation_run_ids, _run_config
    from .learning_view import validate_review_attempt_override

    if context_attempt_id or simulation_attempt_id:
        if not run_id:
            raise ValueError("run_id is required when reviewing an alternate attempt chain")
        if not context_attempt_id or not simulation_attempt_id:
            raise ValueError("Both context_attempt_id and simulation_attempt_id are required")
        validate_review_attempt_override(
            run_id=run_id,
            context_attempt_id=context_attempt_id,
            simulation_attempt_id=simulation_attempt_id,
        )

    from . import store as _store

    run_ids = [run_id] if run_id else [r["run_id"] for r in list_validation_run_ids()]
    trades: list[dict] = []
    for rid in run_ids:
        cfg = _run_config(rid)
        run_detail = _store.get_run(rid)
        cfg_week = ""
        if run_detail and run_detail.selected_week_start:
            cfg_week = str(run_detail.selected_week_start)[:10]
        else:
            cfg_week = str(cfg.get("selected_week_start") or "")[:10]
        trades.extend(
            build_trade_reviews(
                rid,
                cfg_week,
                context_attempt_id=context_attempt_id if rid == run_id else None,
                simulation_attempt_id=simulation_attempt_id if rid == run_id else None,
            )
        )
    if symbol:
        trades = [t for t in trades if str(t.get("symbol", "")).upper() == symbol.upper()]
    if win_only is True:
        trades = [t for t in trades if float(t.get("realized_pnl") or 0) > 0]
    elif win_only is False:
        trades = [t for t in trades if float(t.get("realized_pnl") or 0) < 0]
    if exit_reason:
        trades = [t for t in trades if exit_reason.lower() in str(t.get("exit_reason") or "").lower()]
    return {
        "count": len(trades),
        "trades": trades,
        "review_override": bool(context_attempt_id and simulation_attempt_id),
        "context_attempt_id": context_attempt_id,
        "simulation_attempt_id": simulation_attempt_id,
    }
