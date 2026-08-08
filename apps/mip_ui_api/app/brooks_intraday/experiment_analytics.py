"""Phase 9 — per-week and aggregate experiment analytics (read-only)."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime
from typing import Any

from app.db import get_connection

from .context_phase6b_audit import action_event_and_duration, ui_locator
from .context_repository import load_context_observations
from .context_ruleset_v01 import ACTION_CONSIDER_ENTRY, ACTION_ENTRY_ARMED
from .dossier_access import unwrap_dossier
from .experiment_freeze import FREEZE_ID
from .historical_bar_repository import load_bars_from_store
from .observation_repository import count_observations, review_summary
from .pattern_repository import load_patterns
from .repository import load_dossiers_for_run
from .simulation_repository import load_blocked_signals, load_sim_trades, load_simulation_attempt


def _mfe_mae(trade: dict[str, Any]) -> tuple[float | None, float | None]:
    sym = trade.get("symbol")
    entry_ts = trade.get("entry_ts")
    exit_ts = trade.get("exit_ts")
    entry_price = float(trade.get("entry_price") or 0)
    if not sym or not entry_ts or not exit_ts or not entry_price:
        return None, None
    td = str(entry_ts)[:10]
    from datetime import date

    bars = load_bars_from_store(str(sym), date.fromisoformat(td))
    ek = str(entry_ts)[:19]
    xk = str(exit_ts)[:19]
    path = [b for b in bars if ek <= str(b.ts_utc)[:19] <= xk]
    if not path:
        return None, None
    max_h = max(b.high for b in path)
    min_l = min(b.low for b in path)
    mfe = max_h - entry_price
    mae = entry_price - min_l
    return round(mfe, 4), round(mae, 4)


def classify_no_trade_week(context_rows: list[dict[str, Any]], dossiers: list[dict[str, Any]]) -> dict[str, Any]:
    actions = [str(r.get("selected_action") or "") for r in context_rows]
    entry_armed = sum(1 for a in actions if a == ACTION_ENTRY_ARMED)
    consider = sum(1 for a in actions if a == ACTION_CONSIDER_ENTRY)
    reasons: list[str] = []
    if consider == 0 and entry_armed == 0:
        reasons.append("no_ENTRY_ARMED_events")
    elif entry_armed > 0 and consider == 0:
        reasons.append("entry_armed_but_no_CONSIDER_ENTRY")
    if any(a == "DO_NOT_CHASE" for a in actions):
        reasons.append("do_not_chase")
    if any(a == "THESIS_INVALIDATED" for a in actions):
        reasons.append("thesis_invalidation")
    unsuitable = sum(
        1
        for d in dossiers
        if not d.get("trade_simulation_ready") and d.get("dossier_origin") == "HISTORICAL_RECONSTRUCTION"
    )
    if unsuitable >= len(dossiers) // 2:
        reasons.append("reconstructed_paa_dossiers_unsuitable")
    elif not reasons:
        reasons.append("resistance_or_room_blockers")
    near = []
    for r in context_rows:
        pj = r.get("payload_json") or {}
        layers = pj.get("layers_json") or {}
        diag = layers.get("diagnostics") or {}
        score = diag.get("entry_score")
        if score is not None:
            near.append(
                {
                    "symbol": r.get("symbol"),
                    "bar_ts": r.get("bar_ts"),
                    "selected_action": r.get("selected_action"),
                    "entry_score": score,
                    "blockers": pj.get("blockers_json") or [],
                }
            )
    near.sort(key=lambda x: float(x.get("entry_score") or 0), reverse=True)
    return {
        "entry_armed_count": entry_armed,
        "consider_entry_count": consider,
        "primary_reasons": reasons,
        "nearest_entry_events_top10": near[:10],
    }


def build_trade_reviews(
    run_id: str,
    week_start: str,
    *,
    simulation_attempt_id: str | None = None,
    context_attempt_id: str | None = None,
) -> list[dict[str, Any]]:
    """Build trade review rows. Optional attempt ids are review-only overrides (no pin writes)."""
    cfg = _run_config(run_id)
    sim_id = simulation_attempt_id or cfg.get("phase7_simulation_attempt_id")
    trades = load_sim_trades(run_id, simulation_attempt_id=sim_id)
    dossiers = {f"{d.get('symbol')}|{d.get('trading_date')}": d for d in load_dossiers_for_run(run_id)}
    ctx_id = context_attempt_id or _context_attempt_from_run(run_id)
    ctx_rows = (
        load_context_observations(run_id, context_attempt_id=str(ctx_id), limit=6000) if ctx_id else []
    )
    ctx_by = {(str(r.get("symbol")), str(r.get("bar_ts"))[:19]): r for r in ctx_rows}
    pat_id = _pattern_attempt_from_run(run_id)
    patterns = load_patterns(run_id, replay_attempt_id=str(pat_id), limit=5000) if pat_id else []
    out: list[dict[str, Any]] = []
    for t in trades:
        sym = t.get("symbol")
        signal_key = (str(sym), str(t.get("signal_ts") or t.get("entry_ts"))[:19])
        ctx = ctx_by.get(signal_key, {})
        td = str(t.get("entry_ts") or "")[:10]
        doss = unwrap_dossier(dossiers.get(f"{sym}|{td}", {}))
        mfe, mae = _mfe_mae(t)
        pnl = float(t.get("realized_pnl") or 0)
        risk = float(t.get("entry_price") or 1) * 0.01
        r_mult = round(pnl / risk, 2) if risk else None
        active_pats = [
            p
            for p in patterns
            if str(p.get("symbol")) == str(sym)
            and str(p.get("start_ts", ""))[:10] <= td <= str(p.get("latest_ts", ""))[:10]
        ]
        out.append(
            {
                "trade_id": t.get("trade_id"),
                "week": week_start,
                "symbol": sym,
                "trading_date": td,
                "entry_timestamp": t.get("entry_ts"),
                "entry_price": t.get("entry_price"),
                "quantity": t.get("quantity"),
                "remaining_cash": t.get("remaining_cash"),
                "paa_verdict": doss.get("paa_verdict"),
                "support_resistance": {
                    "support_zones": doss.get("support_zones"),
                    "resistance_zones": doss.get("resistance_zones"),
                },
                "active_brooks_patterns": active_pats[:5],
                "advisory_at_entry": {
                    "state_after": ctx.get("state_after"),
                    "selected_action": ctx.get("selected_action"),
                    "explanation": ctx.get("explanation"),
                },
                "entry_reason": "CONSIDER_ENTRY simulation gate",
                "exit_decision_ts": t.get("exit_decision_ts"),
                "exit_fill_ts": t.get("exit_ts"),
                "exit_price": t.get("exit_price"),
                "exit_reason": t.get("exit_reason"),
                "realized_pnl": pnl,
                "result_r": r_mult,
                "mfe": mfe,
                "mae": mae,
                "context_attempt_id": ctx_id,
                "simulation_attempt_id": sim_id,
                "ui_locator": ui_locator(
                    run_id,
                    str(sym),
                    t.get("entry_ts"),
                    context_attempt_id=str(ctx_id) if ctx_id else None,
                    simulation_attempt_id=str(sim_id) if sim_id else None,
                ),
            }
        )
    return out


def _context_attempt_from_run(run_id: str) -> str | None:
    cfg = _run_config(run_id)
    return cfg.get("phase6b_context_attempt_id")


def _pattern_attempt_from_run(run_id: str) -> str | None:
    cfg = _run_config(run_id)
    return cfg.get("phase5_pattern_v03_attempt_id") or cfg.get("phase5_pattern_attempt_id")


def _objective_attempt_from_run(run_id: str) -> str | None:
    cfg = _run_config(run_id)
    return cfg.get("phase4_review_baseline_attempt_id")


def _run_config(run_id: str) -> dict[str, Any]:
    from . import store

    run = store.get_run(run_id)
    if run:
        return dict(run.configuration or {})
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT CONFIG_JSON FROM MIP.APP.BROOKS_INTRADAY_RUN WHERE RUN_ID = %s",
            (run_id,),
        )
        row = cur.fetchone()
        if not row:
            return {}
        raw = row[0]
        if isinstance(raw, str):
            import json

            return json.loads(raw)
        return dict(raw or {})
    finally:
        conn.close()


def build_week_report(run_id: str) -> dict[str, Any]:
    cfg = _run_config(run_id)
    obj_id = _objective_attempt_from_run(run_id)
    pat_id = _pattern_attempt_from_run(run_id)
    ctx_id = _context_attempt_from_run(run_id)
    sim_id = cfg.get("phase7_simulation_attempt_id")
    week = ""
    from . import store as _store

    run_detail = _store.get_run(run_id)
    if run_detail and run_detail.selected_week_start:
        week = str(run_detail.selected_week_start)[:10]
    else:
        week = str(cfg.get("selected_week_start") or "")[:10]

    obj_count = count_observations(run_id, replay_attempt_id=obj_id) if obj_id else 0
    obj_summary = review_summary(run_id, replay_attempt_id=obj_id) if obj_id else {}

    patterns = load_patterns(run_id, replay_attempt_id=str(pat_id), limit=8000) if pat_id else []
    pat_by_family: Counter[str] = Counter()
    lifecycle: Counter[str] = Counter()
    for p in patterns:
        pat_by_family[str(p.get("pattern_family") or "?")] += 1
        lifecycle[str(p.get("lifecycle_status") or "?")] += 1

    ctx_rows = (
        load_context_observations(run_id, context_attempt_id=str(ctx_id), limit=6000) if ctx_id else []
    )
    ctx_audit = action_event_and_duration(ctx_rows)
    action_dur = ctx_audit.get("action_duration_counts") or {}
    action_trans = ctx_audit.get("action_transition_event_counts") or {}
    state_counts: Counter[str] = Counter(ctx_audit.get("state_duration_counts") or {})

    trades = load_sim_trades(run_id, simulation_attempt_id=sim_id)
    blocked = load_blocked_signals(run_id, simulation_attempt_id=sim_id)
    sim_attempt = load_simulation_attempt(sim_id) if sim_id else None
    wins = [t for t in trades if float(t.get("realized_pnl") or 0) > 0]
    losses = [t for t in trades if float(t.get("realized_pnl") or 0) < 0]
    ending = float(sim_attempt.get("ending_cash") or 1000) if sim_attempt else 1000.0
    starting = float(sim_attempt.get("starting_cash") or 1000) if sim_attempt else 1000.0

    dossiers = load_dossiers_for_run(run_id)
    no_trade = classify_no_trade_week(ctx_rows, dossiers) if not trades else None

    return {
        "run_id": run_id,
        "week_start": week,
        "experiment_role": cfg.get("experiment_role"),
        "experiment_freeze_id": cfg.get("experiment_freeze_id"),
        "attempt_ids": {
            "objective": obj_id,
            "pattern": pat_id,
            "context": ctx_id,
            "simulation": sim_id,
        },
        "objective_layer": {
            "expected_rows": 1560,
            "observation_rows": obj_count,
            "term_frequencies": obj_summary.get("term_counts"),
            "by_symbol": obj_summary.get("by_symbol"),
        },
        "pattern_layer": {
            "total_instances": len(patterns),
            "by_family": dict(pat_by_family),
            "lifecycle_counts": dict(lifecycle),
        },
        "context_layer": {
            "state_counts": dict(state_counts),
            "action_duration": action_dur,
            "action_transitions": action_trans,
            "entry_armed_events": action_dur.get(ACTION_ENTRY_ARMED, 0),
            "consider_entry_events": action_dur.get(ACTION_CONSIDER_ENTRY, 0),
        },
        "simulation_layer": {
            "starting_cash": starting,
            "ending_cash": ending,
            "trades": len(trades),
            "wins": len(wins),
            "losses": len(losses),
            "realized_pnl": float(sim_attempt.get("realized_pnl") or 0) if sim_attempt else 0,
            "average_winner": _avg([float(t.get("realized_pnl") or 0) for t in wins]),
            "average_loser": _avg([float(t.get("realized_pnl") or 0) for t in losses]),
            "blocked_signals": len(blocked),
            "blocked_by_reason": dict(Counter(b.get("block_reason") for b in blocked)),
            "symbols_traded": sorted({t.get("symbol") for t in trades}),
            "eod_exits": sum(1 for t in trades if "EOD" in str(t.get("exit_reason") or "").upper()),
        },
        "no_trade_analysis": no_trade,
        "trade_reviews": build_trade_reviews(run_id, week) if trades else [],
        "bar_dataset_freeze_hash": _bar_freeze_hash(cfg),
        "exploratory_label": "Small-sample exploratory validation — not production performance.",
    }


def _avg(vals: list[float]) -> float | None:
    return round(sum(vals) / len(vals), 4) if vals else None


def _bar_freeze_hash(cfg: dict[str, Any]) -> str | None:
    import hashlib
    import json

    freeze = cfg.get("bar_dataset_freeze") or {}
    if not freeze:
        return None
    parts = sorted(json.dumps({k: v}, sort_keys=True, default=str) for k, v in freeze.items())
    return hashlib.sha256("\n".join(parts).encode()).hexdigest()


def build_aggregate_report(run_ids: list[str]) -> dict[str, Any]:
    weeks = [build_week_report(rid) for rid in run_ids]
    all_trades: list[dict] = []
    for w in weeks:
        all_trades.extend(w.get("trade_reviews") or [])
    pnls = [float(t.get("realized_pnl") or 0) for t in all_trades]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]
    gross = sum(pnls)
    no_trade_weeks = sum(1 for w in weeks if (w.get("simulation_layer") or {}).get("trades") == 0)
    by_week_pnl = {w["week_start"]: (w.get("simulation_layer") or {}).get("realized_pnl", 0) for w in weeks}
    best = max(by_week_pnl, key=by_week_pnl.get) if by_week_pnl else None
    worst = min(by_week_pnl, key=by_week_pnl.get) if by_week_pnl else None
    gross_wins = sum(wins) if wins else 0
    gross_losses = abs(sum(losses)) if losses else 0
    profit_factor = round(gross_wins / gross_losses, 4) if gross_losses else None

    sym_stats: dict[str, dict] = defaultdict(lambda: {"trades": 0, "wins": 0, "losses": 0, "pnl": 0.0})
    for t in all_trades:
        s = str(t.get("symbol"))
        sym_stats[s]["trades"] += 1
        p = float(t.get("realized_pnl") or 0)
        sym_stats[s]["pnl"] += p
        if p > 0:
            sym_stats[s]["wins"] += 1
        elif p < 0:
            sym_stats[s]["losses"] += 1

    return {
        "freeze_id": FREEZE_ID,
        "weeks_tested": len(weeks),
        "trading_days_tested": len(weeks) * 5,
        "symbol_sessions_tested": len(weeks) * 20,
        "starting_capital_per_run": 1000.0,
        "total_simulated_trades": len(all_trades),
        "win_rate": round(len(wins) / len(pnls), 4) if pnls else None,
        "average_winner": _avg(wins),
        "average_loser": _avg(losses),
        "gross_pnl": round(gross, 4),
        "average_pnl_per_trade": round(gross / len(pnls), 4) if pnls else None,
        "profit_factor": profit_factor,
        "maximum_run_drawdown_note": "Per-run independent $1,000 accounts — not compounded.",
        "best_week": best,
        "worst_week": worst,
        "no_trade_weeks": no_trade_weeks,
        "forced_eod_exits": sum((w.get("simulation_layer") or {}).get("eod_exits", 0) for w in weeks),
        "blocked_valid_signals": sum((w.get("simulation_layer") or {}).get("blocked_signals", 0) for w in weeks),
        "by_symbol": dict(sym_stats),
        "per_week": weeks,
        "exploratory_label": "Exploratory unseen-week batch — rules frozen for entire batch.",
    }


def list_validation_run_ids() -> list[dict[str, Any]]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT RUN_ID, SELECTED_WEEK_START, STATUS, CONFIG_JSON, STARTING_CASH, ENDING_CASH, REALIZED_PNL
            FROM MIP.APP.BROOKS_INTRADAY_RUN
            WHERE CONFIG_JSON:experiment_role::STRING = 'UNSEEN_VALIDATION'
            ORDER BY SELECTED_WEEK_START
            """
        )
        cols = [d[0].lower() for d in cur.description]
        out = []
        for raw in cur.fetchall():
            rec = dict(zip(cols, raw))
            cfg = rec.get("config_json")
            if isinstance(cfg, str):
                import json

                cfg = json.loads(cfg)
            rec["config_json"] = cfg
            out.append(rec)
        return out
    finally:
        conn.close()
