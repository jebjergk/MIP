"""Review-only trade management facts for BROOKS_SIMULATION_RULESET_V0_1 (no sim changes)."""

from __future__ import annotations

from datetime import date
from typing import Any

from .context_repository import load_context_observations
from .historical_bar_repository import load_bars_from_store
from .position_management_ledger_store import load_management_ledger
from .position_management_ruleset_v01 import RULESET_VERSION as PM_RULESET_VERSION
from .simulation_repository import load_sim_trades, load_simulation_attempt
from .simulation_ruleset_v01 import RULESET_VERSION, resolve_params


def simulation_exit_rules_for_attempt(simulation_attempt_id: str | None) -> dict[str, Any]:
    """Human-readable exit rules active for V0_1 (parameters are fixed defaults today)."""
    attempt = load_simulation_attempt(simulation_attempt_id) if simulation_attempt_id else None
    version = str((attempt or {}).get("simulation_ruleset_version") or RULESET_VERSION)
    if version == PM_RULESET_VERSION:
        from .position_management_ruleset_v01 import resolve_params as pm_params

        p = pm_params(None)
        return {
            "simulation_ruleset_version": version,
            "simulation_attempt_id": simulation_attempt_id,
            "inactive_review_only": True,
            "parameters": p,
            "exit_rules": [
                "PM01 initial structural stop (activates bar after entry).",
                "PM02 stop gap-through at open or protective stop on bar low.",
                "PM03 trail on CONFIRMED STRUCTURAL_SWING_LOW (active next bar).",
                "PM04 CONTEXT_FAILED_BREAKOUT at next bar open.",
                "PM05 THESIS_INVALIDATED at next bar open.",
                "PM06 FORCED_END_OF_DAY_EXIT at final RTH bar close.",
                "Precedence: stop/gap → scheduled context open → intrabar stop → schedule signals → EOD.",
            ],
            "entry_rules": [
                "Entries unchanged from BROOKS_SIMULATION_RULESET_V0_1 (CONSIDER_ENTRY @ bar close).",
            ],
        }
    params = resolve_params(None)
    return {
        "simulation_ruleset_version": version,
        "simulation_attempt_id": simulation_attempt_id,
        "parameters": params,
        "exit_rules": [
            "While a position is open, each bar calls _should_schedule_exit on that symbol's context row.",
            f"THESIS_INVALIDATED context action ({params.get('exit_fill') or 'NEXT_BAR_OPEN'} fill on the bar after the signal).",
            f"FORCED_END_OF_DAY_EXIT on the session's last bar ({params.get('eod_exit_fill') or 'FINAL_BAR_CLOSE'} fill).",
            "No price stop, trailing stop, break-even, or profit-protection exits in V0_1.",
            "stop_reference is copied from context at entry but is never read during hold.",
        ],
        "entry_rules": [
            f"Entry when context selected_action equals {params.get('entry_signal_action')!r} and dossier is simulation-ready.",
            f"Fill: {params.get('entry_fill') or 'BAR_CLOSE'}; whole shares; max {params.get('max_concurrent_positions')} position.",
        ],
    }


def _invalidation_levels(pj: dict[str, Any]) -> tuple[float | None, float | None]:
    intra = pj.get("intraday_setup_invalidation")
    daily = pj.get("daily_thesis_invalidation")
    try:
        intra_f = float(intra) if intra is not None else None
    except (TypeError, ValueError):
        intra_f = None
    try:
        daily_f = float(daily) if daily is not None else None
    except (TypeError, ValueError):
        daily_f = None
    return intra_f, daily_f


def _ts_key(ts: Any) -> str:
    return str(ts).replace("T", " ")[:19]


def _ts_iso(ts: Any) -> str:
    return _ts_key(ts).replace(" ", "T")


def _excursion_stats(trade: dict[str, Any]) -> dict[str, Any]:
    sym = trade.get("symbol")
    entry_ts = trade.get("entry_ts")
    exit_ts = trade.get("exit_ts")
    entry_price = float(trade.get("entry_price") or 0)
    qty = int(trade.get("quantity") or 0)
    if not sym or not entry_ts or not exit_ts or not entry_price or qty < 1:
        return {
            "mfe": None,
            "mae": None,
            "max_unrealized_pnl": None,
            "max_unrealized_pnl_ts": None,
            "max_unrealized_pnl_price": None,
        }
    td = str(entry_ts)[:10]
    bars = load_bars_from_store(str(sym), date.fromisoformat(td))
    ek = _ts_key(entry_ts)
    xk = _ts_key(exit_ts)
    path = [b for b in bars if ek <= _ts_key(b.ts_utc) <= xk]
    if not path:
        return {
            "mfe": None,
            "mae": None,
            "max_unrealized_pnl": None,
            "max_unrealized_pnl_ts": None,
            "max_unrealized_pnl_price": None,
        }
    max_h = max(float(b.high) for b in path)
    min_l = min(float(b.low) for b in path)
    mfe = round(max_h - entry_price, 4)
    mae = round(entry_price - min_l, 4)
    best_pnl = None
    best_ts = None
    best_px = None
    for b in path:
        upnl = (float(b.high) - entry_price) * qty
        if best_pnl is None or upnl > best_pnl:
            best_pnl = upnl
            best_ts = _ts_iso(b.ts_utc)
            best_px = float(b.high)
    return {
        "mfe": mfe,
        "mae": mae,
        "max_unrealized_pnl": round(best_pnl, 4) if best_pnl is not None else None,
        "max_unrealized_pnl_ts": best_ts,
        "max_unrealized_pnl_price": best_px,
    }


def build_trade_management_review(
    run_id: str,
    trade_id: str,
    *,
    context_attempt_id: str,
    simulation_attempt_id: str,
    workspace_symbol: str | None = None,
) -> dict[str, Any]:
    trades = load_sim_trades(run_id, simulation_attempt_id=simulation_attempt_id)
    trade = next((t for t in trades if str(t.get("trade_id")) == str(trade_id)), None)
    if not trade:
        raise ValueError(f"Trade not found for simulation attempt: {trade_id}")

    trade_sim = str(trade.get("simulation_attempt_id") or simulation_attempt_id)
    if str(trade_sim) != str(simulation_attempt_id):
        raise ValueError(
            "Trade does not belong to the selected simulation attempt; management review withheld."
        )
    trade_sym = str(trade.get("symbol") or "").upper()
    if workspace_symbol and trade_sym != str(workspace_symbol).upper():
        raise ValueError(
            f"Trade symbol {trade_sym} does not match workspace symbol {workspace_symbol.upper()}; "
            "management review withheld."
        )
    ctx_rows = load_context_observations(run_id, context_attempt_id=context_attempt_id, limit=6000)
    entry_key = (str(trade.get("symbol")), str(trade.get("entry_ts") or trade.get("signal_ts"))[:19])
    ctx_at_entry = next(
        (
            r
            for r in ctx_rows
            if (str(r.get("symbol")), str(r.get("bar_ts"))[:19]) == entry_key
        ),
        {},
    )
    pj = ctx_at_entry.get("payload_json") or {}
    intra_inv, daily_inv = _invalidation_levels(pj)

    exc = _excursion_stats(trade)
    rules = simulation_exit_rules_for_attempt(simulation_attempt_id)
    ledger = load_management_ledger(simulation_attempt_id)
    if ledger and str(ledger.get("simulation_attempt_id") or simulation_attempt_id) != str(simulation_attempt_id):
        ledger = None
    pm_initial = None
    pm_trail_events: list[dict[str, Any]] = []
    ledger_events: list[dict[str, Any]] = []
    if ledger:
        for ev in ledger.get("events") or []:
            ev_sym = str(ev.get("symbol") or trade_sym).upper()
            if ev_sym != trade_sym:
                continue
            ledger_events.append(ev)
            if ev.get("event") == "INITIAL_STOP_CALCULATED":
                pm_initial = ev
            if ev.get("event") in ("STOP_TRAIL_SCHEDULED", "STOP_TRAIL_UPDATE", "STOP_ACTIVATED"):
                pm_trail_events.append(ev)

    stop_at_entry = pm_initial.get("stop_price") if pm_initial else (
        intra_inv if intra_inv is not None else (daily_inv if daily_inv is not None else None)
    )
    stop_source = pm_initial.get("stop_source") if pm_initial else (
        "intraday_setup_invalidation"
        if intra_inv is not None
        else ("daily_thesis_invalidation" if daily_inv is not None else None)
    )

    return {
        "trade_id": trade_id,
        "run_id": run_id,
        "context_attempt_id": context_attempt_id,
        "simulation_attempt_id": simulation_attempt_id,
        "symbol": trade.get("symbol"),
        "entry_ts": trade.get("entry_ts"),
        "entry_price": trade.get("entry_price"),
        "quantity": trade.get("quantity"),
        "initial_stop": {
            "configured": stop_at_entry is not None,
            "price": stop_at_entry,
            "source_field": stop_source,
            "simulator_enforcement": "pm_v01_active" if pm_initial else "stored_only_not_checked",
            "display": stop_at_entry if stop_at_entry is not None else "Not configured",
        },
        "current_stop": {
            "updated": bool(pm_trail_events),
            "price": pm_trail_events[-1].get("new_stop") if pm_trail_events else stop_at_entry,
            "display": (
                f"PM trail ({len(pm_trail_events)} updates)"
                if pm_trail_events
                else ("Not updated by simulator" if stop_at_entry is not None else "Not configured")
            ),
        },
        "management_ledger": ledger_events if ledger_events else None,
        "management_ledger_chronological": ledger_events if ledger_events else None,
        "mfe_mae_review_only": {
            "mfe": exc["mfe"],
            "mae": exc["mae"],
            "max_unrealized_pnl": exc["max_unrealized_pnl"],
            "disclaimer": "Post-trade analytics only — never used as rule inputs.",
        },
        "mfe": exc["mfe"],
        "mae": exc["mae"],
        "max_unrealized_pnl": exc["max_unrealized_pnl"],
        "max_unrealized_pnl_ts": exc["max_unrealized_pnl_ts"],
        "max_unrealized_pnl_price": exc["max_unrealized_pnl_price"],
        "exit_ts": trade.get("exit_ts"),
        "exit_price": trade.get("exit_price"),
        "exit_reason": trade.get("exit_reason"),
        "realized_pnl": trade.get("realized_pnl"),
        "active_exit_rules": rules,
        "review_note": "Read-only management summary; does not change simulation behavior.",
    }
