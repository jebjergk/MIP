"""BROOKS_POSITION_MANAGEMENT_RULESET_V0_1 — deterministic long-position management."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

from .bars import HistoricalBar
from .context_ruleset_v01 import ACTION_THESIS_INVALIDATED
from .position_management_ruleset_v01 import resolve_params
from .simulation_engine import PortfolioSimState, execute_entry, entry_permitted, record_blocked_entry


def _ts_key(ts: Any) -> str:
    return str(ts).replace("T", " ")[:19]


def _ctx_payload(ctx: dict[str, Any]) -> dict[str, Any]:
    return ctx.get("payload_json") or {}


def _interpretation(ctx: dict[str, Any]) -> dict[str, Any]:
    pj = _ctx_payload(ctx)
    layers = pj.get("layers_json") or {}
    return layers.get("contextual_interpretation") or {}


def _broken_resistance(ctx: dict[str, Any]) -> float | None:
    interp = _interpretation(ctx)
    br = interp.get("broken_resistance")
    if br is None:
        pj = _ctx_payload(ctx)
        active = pj.get("active_levels") or {}
        br = active.get("broken_resistance")
    try:
        return float(br) if br is not None else None
    except (TypeError, ValueError):
        return None


def _resistance_lifecycle(ctx: dict[str, Any]) -> str:
    return str(_interpretation(ctx).get("resistance_lifecycle") or "")


def pm01_initial_stop(
    *,
    ctx: dict[str, Any],
    entry_bar: HistoricalBar,
    params: dict[str, Any],
) -> tuple[float, str]:
    tick = float(params.get("price_tick") or 0.01)
    pj = _ctx_payload(ctx)
    setup = pj.get("intraday_setup_invalidation")
    if setup is not None:
        try:
            return float(setup), "intraday_setup_invalidation"
        except (TypeError, ValueError):
            pass
    br = _broken_resistance(ctx)
    if br is not None:
        return br, "broken_resistance"
    low_stop = round(float(entry_bar.low) - tick, 4)
    return low_stop, "entry_bar_low_minus_tick"


@dataclass
class PMPosition:
    trade_id: str
    symbol: str
    quantity: int
    entry_price: float
    entry_ts: datetime
    signal_ts: datetime
    initial_cash: float
    active_stop: float | None = None
    stop_source: str = ""
    stop_calc_bar_ts: datetime | None = None
    stop_activation_ts: datetime | None = None
    pending_stop: float | None = None
    pending_stop_meta: dict[str, Any] | None = None
    stop_active: bool = False
    pending_context_exit: dict[str, Any] | None = None


@dataclass
class PMPortfolio(PortfolioSimState):
    pm_position: PMPosition | None = None
    management_ledger: list[dict[str, Any]] = field(default_factory=list)


def _ledger(pm: PMPortfolio, event: str, **fields: Any) -> None:
    pm.management_ledger.append({"event": event, **fields})


def _close_trade(
    pm: PMPortfolio,
    *,
    exit_ts: datetime,
    exit_price: float,
    exit_reason: str,
    exit_decision_ts: datetime | None = None,
    precedence: str,
    bar_index_in_session: int | None = None,
    trading_date: date | None = None,
) -> None:
    pos = pm.pm_position
    if not pos:
        return
    pnl = (exit_price - pos.entry_price) * pos.quantity
    pm.cash += exit_price * pos.quantity
    pm.realized_pnl += pnl
    trade_rec = {
        "trade_id": pos.trade_id,
        "symbol": pos.symbol,
        "direction": "LONG",
        "quantity": pos.quantity,
        "signal_ts": pos.signal_ts,
        "entry_ts": pos.entry_ts,
        "entry_price": pos.entry_price,
        "exit_decision_ts": exit_decision_ts or exit_ts,
        "exit_ts": exit_ts,
        "exit_price": round(exit_price, 4),
        "exit_reason": exit_reason,
        "initial_cash": pos.initial_cash,
        "remaining_cash": pm.cash,
        "realized_pnl": round(pnl, 4),
        "exit_precedence": precedence,
    }
    pm.closed_trades.append(trade_rec)
    _ledger(
        pm,
        "EXIT",
        symbol=pos.symbol,
        ts=_ts_key(exit_ts),
        exit_price=trade_rec["exit_price"],
        exit_reason=exit_reason,
        precedence=precedence,
        realized_pnl=trade_rec["realized_pnl"],
    )
    pm.pm_position = None
    pm.open_position = None
    from .reentry_policy_v01 import note_reentry_exit

    td = trading_date
    if td is None and hasattr(exit_ts, "date"):
        td = exit_ts.date()
    note_reentry_exit(
        pm,
        symbol=pos.symbol,
        trading_date=td or pos.entry_ts.date(),
        bar_index_in_session=bar_index_in_session if bar_index_in_session is not None else 0,
        exit_reason=exit_reason,
    )


def on_entry_filled(
    pm: PMPortfolio,
    *,
    ctx: dict[str, Any],
    entry_bar: HistoricalBar,
    quantity: int,
    params: dict[str, Any] | None = None,
) -> None:
    params = resolve_params(params)
    stop_px, source = pm01_initial_stop(ctx=ctx, entry_bar=entry_bar, params=params)
    trade_id = str(uuid.uuid4())
    pos = PMPosition(
        trade_id=trade_id,
        symbol=entry_bar.symbol.upper(),
        quantity=quantity,
        entry_price=float(entry_bar.close),
        entry_ts=entry_bar.ts_utc,
        signal_ts=entry_bar.ts_utc,
        initial_cash=pm.cash + quantity * float(entry_bar.close),
        active_stop=None,
        pending_stop=stop_px,
        stop_source=source,
        stop_calc_bar_ts=entry_bar.ts_utc,
        stop_active=False,
    )
    pm.pm_position = pos
    from .simulation_engine import OpenSimPosition

    pm.open_position = OpenSimPosition(
        trade_id=trade_id,
        symbol=pos.symbol,
        quantity=quantity,
        entry_price=pos.entry_price,
        entry_ts=pos.entry_ts,
        signal_ts=pos.signal_ts,
        initial_cash=pos.initial_cash,
    )
    _ledger(
        pm,
        "ENTRY",
        symbol=pos.symbol,
        ts=_ts_key(entry_bar.ts_utc),
        entry_price=pos.entry_price,
        quantity=quantity,
    )
    _ledger(
        pm,
        "INITIAL_STOP_CALCULATED",
        symbol=pos.symbol,
        ts=_ts_key(entry_bar.ts_utc),
        stop_price=stop_px,
        stop_source=source,
        activates_next_bar=True,
    )


def _apply_pending_stop(pm: PMPortfolio, bar: HistoricalBar) -> None:
    pos = pm.pm_position
    if not pos or pos.pending_stop is None:
        return
    pending = float(pos.pending_stop)
    meta = dict(pos.pending_stop_meta or {})
    pos.pending_stop = None
    pos.pending_stop_meta = None
    if not pos.stop_active:
        pos.active_stop = pending
        pos.stop_active = True
        pos.stop_activation_ts = bar.ts_utc
        _ledger(
            pm,
            "STOP_ACTIVATED",
            symbol=pos.symbol,
            ts=_ts_key(bar.ts_utc),
            active_stop=pending,
            stop_source=pos.stop_source,
        )
    elif pending > float(pos.active_stop or 0):
        prev = pos.active_stop
        pos.active_stop = pending
        pos.stop_activation_ts = bar.ts_utc
        _ledger(
            pm,
            "STOP_TRAIL_UPDATE",
            symbol=pos.symbol,
            ts=_ts_key(bar.ts_utc),
            previous_stop=prev,
            new_stop=pending,
            pattern_instance_id=meta.get("pattern_instance_id"),
            calculation_bar=meta.get("calculation_bar"),
            activation_bar=_ts_key(bar.ts_utc),
        )


def _pm02_stop_check(
    pm: PMPortfolio,
    bar: HistoricalBar,
    params: dict[str, Any],
    *,
    bar_index_in_session: int | None = None,
) -> bool:
    pos = pm.pm_position
    if not pos or not pos.stop_active or pos.active_stop is None:
        return False
    stop = float(pos.active_stop)
    reasons = params.get("exit_reasons") or {}
    _ledger(
        pm,
        "STOP_EVAL",
        symbol=pos.symbol,
        ts=_ts_key(bar.ts_utc),
        active_stop=stop,
        bar_open=float(bar.open),
        bar_low=float(bar.low),
    )
    if float(bar.open) <= stop:
        _close_trade(
            pm,
            exit_ts=bar.ts_utc,
            exit_price=float(bar.open),
            exit_reason=str(reasons.get("stop_gap") or "STOP_GAP_THROUGH"),
            exit_decision_ts=bar.ts_utc,
            precedence="PM02_stop_gap_at_open",
            bar_index_in_session=bar_index_in_session,
            trading_date=bar.trading_date,
        )
        return True
    if float(bar.low) <= stop:
        _close_trade(
            pm,
            exit_ts=bar.ts_utc,
            exit_price=stop,
            exit_reason=str(reasons.get("stop_hit") or "PROTECTIVE_STOP_HIT"),
            exit_decision_ts=bar.ts_utc,
            precedence="PM02_protective_stop_intrabar",
            bar_index_in_session=bar_index_in_session,
            trading_date=bar.trading_date,
        )
        return True
    return False


def _fill_pending_context_at_open(
    pm: PMPortfolio,
    bar: HistoricalBar,
    params: dict[str, Any],
    *,
    bar_index_in_session: int | None = None,
) -> bool:
    pos = pm.pm_position
    if not pos or not pos.pending_context_exit:
        return False
    pending = pos.pending_context_exit
    if str(pending.get("symbol", "")).upper() != bar.symbol.upper():
        return False
    reason = str(pending.get("reason") or "CONTEXT_EXIT")
    _ledger(
        pm,
        "CONTEXT_EXIT_FILL",
        symbol=pos.symbol,
        ts=_ts_key(bar.ts_utc),
        fill_price=float(bar.open),
        reason=reason,
        signal_bar=pending.get("signal_bar"),
        precedence="PM04_PM05_scheduled_context_at_open",
    )
    _close_trade(
        pm,
        exit_ts=bar.ts_utc,
        exit_price=float(bar.open),
        exit_reason=reason,
        exit_decision_ts=pending.get("decision_ts"),
        precedence="PM04_PM05_scheduled_context_at_open",
        bar_index_in_session=bar_index_in_session,
        trading_date=bar.trading_date,
    )
    pos.pending_context_exit = None
    return True


def process_pm_bar_open(
    pm: PMPortfolio,
    *,
    bar: HistoricalBar,
    params: dict[str, Any] | None = None,
    bar_index_in_session: int | None = None,
) -> None:
    """Bar open: activate stops, precedence stop then scheduled context."""
    params = resolve_params(params)
    pos = pm.pm_position
    if not pos or pos.symbol != bar.symbol.upper():
        return
    _apply_pending_stop(pm, bar)
    if _pm02_stop_check(pm, bar, params, bar_index_in_session=bar_index_in_session):
        return
    if _fill_pending_context_at_open(pm, bar, params, bar_index_in_session=bar_index_in_session):
        return


def process_pm_bar_close(
    pm: PMPortfolio,
    *,
    ctx: dict[str, Any],
    bar: HistoricalBar,
    is_last_bar_in_session: bool,
    swing_low_confirms: list[dict[str, Any]],
    params: dict[str, Any] | None = None,
    bar_index_in_session: int | None = None,
) -> None:
    params = resolve_params(params)
    pos = pm.pm_position
    if not pos or pos.symbol != bar.symbol.upper():
        return

    if is_last_bar_in_session:
        reasons = params.get("exit_reasons") or {}
        _close_trade(
            pm,
            exit_ts=bar.ts_utc,
            exit_price=float(bar.close),
            exit_reason=str(reasons.get("eod") or "FORCED_END_OF_DAY_EXIT"),
            exit_decision_ts=bar.ts_utc,
            precedence="PM06_eod_final_bar_close",
            bar_index_in_session=bar_index_in_session,
            trading_date=bar.trading_date,
        )
        return

    act = str(ctx.get("selected_action") or "")
    state_after = str(ctx.get("state_after") or "")
    rl = _resistance_lifecycle(ctx)
    reasons = params.get("exit_reasons") or {}

    failed = rl == "FAILED_BREAKOUT" or (
        state_after == "FAILED_BREAKOUT" and act == "DO_NOT_ENTER"
    )
    if failed and not pos.pending_context_exit:
        pos.pending_context_exit = {
            "symbol": pos.symbol,
            "reason": str(reasons.get("failed_breakout") or "CONTEXT_FAILED_BREAKOUT"),
            "decision_ts": bar.ts_utc,
            "signal_bar": _ts_key(bar.ts_utc),
        }
        _ledger(
            pm,
            "CONTEXT_EXIT_SIGNAL",
            symbol=pos.symbol,
            ts=_ts_key(bar.ts_utc),
            reason=pos.pending_context_exit["reason"],
            fills_next_bar_open=True,
        )

    if act == ACTION_THESIS_INVALIDATED and not pos.pending_context_exit:
        pos.pending_context_exit = {
            "symbol": pos.symbol,
            "reason": str(reasons.get("thesis") or "THESIS_INVALIDATED"),
            "decision_ts": bar.ts_utc,
            "signal_bar": _ts_key(bar.ts_utc),
        }
        _ledger(
            pm,
            "CONTEXT_EXIT_SIGNAL",
            symbol=pos.symbol,
            ts=_ts_key(bar.ts_utc),
            reason=pos.pending_context_exit["reason"],
            fills_next_bar_open=True,
        )

    tick = float(params.get("price_tick") or 0.01)
    for pat in swing_low_confirms:
        fam = str(pat.get("pattern_family") or "").upper()
        lc = str(pat.get("lifecycle") or pat.get("lifecycle_status") or "").upper()
        if fam != "STRUCTURAL_SWING_LOW" or lc != "CONFIRMED":
            continue
        rp = pat.get("relevant_prices_json") or pat.get("relevant_prices") or {}
        swing_ref = rp.get("swing_low") if isinstance(rp, dict) else None
        ref_low = float(swing_ref) if swing_ref is not None else float(bar.low)
        candidate = round(ref_low - tick, 4)
        meta = {
            "pattern_instance_id": pat.get("pattern_instance_id"),
            "calculation_bar": _ts_key(bar.ts_utc),
            "swing_reference_low": ref_low,
            "swing_reference_ts": _ts_key(pat.get("start_ts") or ""),
        }
        if not pos.stop_active:
            if pos.pending_stop is None or candidate > float(pos.pending_stop):
                pos.pending_stop = candidate
                pos.pending_stop_meta = meta
        else:
            prev = pos.active_stop
            if candidate > float(pos.active_stop or 0):
                pos.pending_stop = candidate
                pos.pending_stop_meta = {**meta, "previous_stop": prev}
                _ledger(
                    pm,
                    "STOP_TRAIL_SCHEDULED",
                    symbol=pos.symbol,
                    ts=_ts_key(bar.ts_utc),
                    previous_stop=prev,
                    candidate_stop=candidate,
                    pattern_instance_id=pat.get("pattern_instance_id"),
                    activates_next_bar=True,
                )


def schedule_swing_confirmations_for_bar(
    patterns: list[dict[str, Any]],
    *,
    symbol: str,
    bar_ts: str,
) -> list[dict[str, Any]]:
    """Patterns whose confirmation bar (latest_ts) equals this bar."""
    out: list[dict[str, Any]] = []
    sym = symbol.upper()
    bk = _ts_key(bar_ts)
    for p in patterns:
        if str(p.get("symbol", "")).upper() != sym:
            continue
        fam = str(p.get("pattern_family") or "").upper()
        lc = str(p.get("lifecycle") or p.get("lifecycle_status") or "").upper()
        if fam != "STRUCTURAL_SWING_LOW" or lc != "CONFIRMED":
            continue
        latest = _ts_key(p.get("latest_ts") or p.get("confirmation_ts") or p.get("start_ts"))
        if latest == bk:
            out.append(p)
    return out
