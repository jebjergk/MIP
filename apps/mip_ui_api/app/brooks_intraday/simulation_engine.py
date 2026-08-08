"""Phase 7 — deterministic simulated portfolio driven by persisted context (V0.2 gating)."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from .bars import HistoricalBar
from .context_ruleset_v01 import ACTION_CONSIDER_ENTRY, ACTION_THESIS_INVALIDATED
from .simulation_ruleset_v01 import resolve_params
from .reentry_policy_v01 import (
    note_reentry_entry_consumed,
    note_reentry_exit,
    observe_reentry_context_bar,
    setup_cycle_id_from_context,
)

BLOCK_REASON_POSITION_OPEN = "VALID_SIGNAL_POSITION_ALREADY_OPEN"
BLOCK_REASON_TIE_BREAK = "TIE_BREAK_LOSER"
BLOCK_REASON_NOT_ENTRY_SIGNAL = "CONTEXT_NOT_ENTRY_SIGNAL"
BLOCK_REASON_OPENING = "OPENING_WINDOW"
BLOCK_REASON_EOD_CUTOFF = "END_OF_DAY_ENTRY_CUTOFF"
BLOCK_REASON_NOT_SIM_READY = "DOSSIER_NOT_SIMULATION_READY"
BLOCK_REASON_INSUFFICIENT_CASH = "INSUFFICIENT_CASH_FOR_WHOLE_SHARE"


@dataclass
class OpenSimPosition:
    trade_id: str
    symbol: str
    quantity: int
    entry_price: float
    entry_ts: datetime
    signal_ts: datetime
    initial_cash: float
    context_observation_id: str | None = None
    stop_reference: float | None = None


@dataclass
class PendingExit:
    trade_id: str
    symbol: str
    reason: str
    decision_ts: datetime


@dataclass
class PortfolioSimState:
    cash: float
    realized_pnl: float = 0.0
    open_position: OpenSimPosition | None = None
    pending_exit: PendingExit | None = None
    closed_trades: list[dict[str, Any]] = field(default_factory=list)
    blocked_signals: list[dict[str, Any]] = field(default_factory=list)
    bars_processed: int = 0


@dataclass
class SimulationStepResult:
    symbol: str
    bar_ts: datetime
    portfolio: PortfolioSimState
    entry_candidate: bool = False
    entry_executed: bool = False
    exit_scheduled: bool = False
    exit_filled: bool = False
    notes: list[str] = field(default_factory=list)


def _paa_rank(verdict: str, params: dict[str, Any]) -> int:
    order = list(params.get("paa_rank") or [])
    v = str(verdict or "").upper()
    try:
        return order.index(v)
    except ValueError:
        return len(order) + 1


def _entry_score(ctx: dict[str, Any]) -> float:
    pj = ctx.get("payload_json") or {}
    diag = (pj.get("layers_json") or {}).get("diagnostics") or pj.get("diagnostics") or {}
    return float(diag.get("entry_score") or 0)


def _room_class(ctx: dict[str, Any]) -> str:
    pj = ctx.get("payload_json") or {}
    interp = (pj.get("layers_json") or {}).get("contextual_interpretation") or {}
    return str(interp.get("room_class") or pj.get("room_class") or "")


def entry_permitted(
    *,
    ctx: dict[str, Any],
    dossier: dict[str, Any],
    bar_index_in_session: int,
    params: dict[str, Any],
) -> tuple[bool, str | None]:
    signal = str(params.get("entry_signal_action") or ACTION_CONSIDER_ENTRY)
    if str(ctx.get("selected_action")) != signal:
        return False, BLOCK_REASON_NOT_ENTRY_SIGNAL
    if not dossier.get("trade_simulation_ready"):
        return False, BLOCK_REASON_NOT_SIM_READY
    if bar_index_in_session < int(params.get("min_bars_before_entry") or 3):
        return False, BLOCK_REASON_OPENING
    if bar_index_in_session > int(params.get("last_entry_bar_index_in_session") or 72):
        return False, BLOCK_REASON_EOD_CUTOFF
    return True, None


def _should_schedule_exit(ctx: dict[str, Any], *, last_bar: bool) -> str | None:
    act = str(ctx.get("selected_action") or "")
    if act == ACTION_THESIS_INVALIDATED:
        return "THESIS_INVALIDATED"
    if last_bar:
        return "FORCED_END_OF_DAY_EXIT"
    return None


def tie_break_pick(
    candidates: list[dict[str, Any]],
    *,
    params: dict[str, Any],
    symbol_order: list[str],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Return winner and losers (for blocked-signal logging)."""

    def sort_key(c: dict[str, Any]) -> tuple:
        ctx = c["context"]
        dossier = c["dossier"]
        sym = c["symbol"]
        room = _room_class(ctx)
        room_score = {"AMPLE_ROOM": 3, "ACCEPTABLE_ROOM": 2, "LIMITED_ROOM": 1}.get(room, 0)
        return (
            -_entry_score(ctx),
            -room_score,
            _paa_rank(str(dossier.get("paa_verdict") or ""), params),
            symbol_order.index(sym) if sym in symbol_order else 999,
        )

    ranked = sorted(candidates, key=sort_key)
    return ranked[0], ranked[1:]


def fill_pending_exit(
    portfolio: PortfolioSimState,
    *,
    bar: HistoricalBar,
    bar_index_in_session: int | None = None,
) -> SimulationStepResult | None:
    if not portfolio.pending_exit or portfolio.pending_exit.symbol != bar.symbol.upper():
        return None
    pos = portfolio.open_position
    if not pos or pos.trade_id != portfolio.pending_exit.trade_id:
        portfolio.pending_exit = None
        return None
    exit_price = float(bar.open)
    proceeds = exit_price * pos.quantity
    pnl = (exit_price - pos.entry_price) * pos.quantity
    portfolio.cash += proceeds
    portfolio.realized_pnl += pnl
    trade_rec = {
        "trade_id": pos.trade_id,
        "symbol": pos.symbol,
        "direction": "LONG",
        "quantity": pos.quantity,
        "signal_ts": pos.signal_ts,
        "entry_ts": pos.entry_ts,
        "entry_price": pos.entry_price,
        "exit_decision_ts": portfolio.pending_exit.decision_ts,
        "exit_ts": bar.ts_utc,
        "exit_price": exit_price,
        "exit_reason": portfolio.pending_exit.reason,
        "initial_cash": pos.initial_cash,
        "remaining_cash": portfolio.cash,
        "realized_pnl": round(pnl, 4),
    }
    portfolio.closed_trades.append(trade_rec)
    portfolio.open_position = None
    reason = portfolio.pending_exit.reason
    portfolio.pending_exit = None
    note_reentry_exit(
        portfolio,
        symbol=bar.symbol.upper(),
        trading_date=bar.trading_date,
        bar_index_in_session=bar_index_in_session if bar_index_in_session is not None else 0,
        exit_reason=reason,
    )
    return SimulationStepResult(
        symbol=bar.symbol,
        bar_ts=bar.ts_utc,
        portfolio=portfolio,
        exit_filled=True,
        notes=[f"exit_filled:{reason}@{exit_price}"],
    )


def process_symbol_bar(
    *,
    portfolio: PortfolioSimState,
    ctx: dict[str, Any],
    bar: HistoricalBar,
    dossier: dict[str, Any],
    bar_index_in_session: int,
    is_last_bar_in_session: bool,
    params: dict[str, Any] | None = None,
) -> SimulationStepResult:
    params = resolve_params(params)
    sym = bar.symbol.upper()
    result = SimulationStepResult(symbol=sym, bar_ts=bar.ts_utc, portfolio=portfolio)

    if portfolio.open_position and portfolio.open_position.symbol == sym:
        exit_reason = _should_schedule_exit(ctx, last_bar=is_last_bar_in_session)
        if exit_reason and not portfolio.pending_exit:
            fill_price = float(bar.close) if is_last_bar_in_session and exit_reason == "FORCED_END_OF_DAY_EXIT" else None
            if fill_price is not None and exit_reason == "FORCED_END_OF_DAY_EXIT":
                pos = portfolio.open_position
                pnl = (fill_price - pos.entry_price) * pos.quantity
                portfolio.cash += fill_price * pos.quantity
                portfolio.realized_pnl += pnl
                portfolio.closed_trades.append(
                    {
                        "trade_id": pos.trade_id,
                        "symbol": pos.symbol,
                        "direction": "LONG",
                        "quantity": pos.quantity,
                        "signal_ts": pos.signal_ts,
                        "entry_ts": pos.entry_ts,
                        "entry_price": pos.entry_price,
                        "exit_decision_ts": bar.ts_utc,
                        "exit_ts": bar.ts_utc,
                        "exit_price": fill_price,
                        "exit_reason": exit_reason,
                        "initial_cash": pos.initial_cash,
                        "remaining_cash": portfolio.cash,
                        "realized_pnl": round(pnl, 4),
                    }
                )
                portfolio.open_position = None
                result.exit_filled = True
                result.notes.append(f"eod_exit@{fill_price}")
                note_reentry_exit(
                    portfolio,
                    symbol=sym,
                    trading_date=bar.trading_date,
                    bar_index_in_session=bar_index_in_session,
                    exit_reason=exit_reason,
                )
            else:
                portfolio.pending_exit = PendingExit(
                    trade_id=portfolio.open_position.trade_id,
                    symbol=sym,
                    reason=exit_reason,
                    decision_ts=bar.ts_utc,
                )
                result.exit_scheduled = True
                result.notes.append(f"exit_scheduled:{exit_reason}")
        return result

    ok, block = entry_permitted(
        ctx=ctx, dossier=dossier, bar_index_in_session=bar_index_in_session, params=params
    )
    if ok:
        result.entry_candidate = True
    elif block and block != BLOCK_REASON_NOT_ENTRY_SIGNAL:
        result.notes.append(f"entry_blocked:{block}")
    return result


def execute_entry(
    portfolio: PortfolioSimState,
    *,
    candidate: dict[str, Any],
    bar: HistoricalBar,
    params: dict[str, Any] | None = None,
) -> bool:
    params = resolve_params(params)
    if portfolio.open_position is not None:
        return False
    price = float(bar.close)
    qty = int(portfolio.cash // price) if price > 0 else 0
    if qty < 1:
        portfolio.blocked_signals.append(
            {
                "symbol": bar.symbol.upper(),
                "signal_ts": bar.ts_utc,
                "candidate_action": str(params.get("entry_signal_action")),
                "block_reason": BLOCK_REASON_INSUFFICIENT_CASH,
            }
        )
        return False
    cost = qty * price
    trade_id = str(uuid.uuid4())
    ctx = candidate["context"]
    pj = ctx.get("payload_json") or {}
    pos = OpenSimPosition(
        trade_id=trade_id,
        symbol=bar.symbol.upper(),
        quantity=qty,
        entry_price=price,
        entry_ts=bar.ts_utc,
        signal_ts=bar.ts_utc,
        initial_cash=portfolio.cash,
        context_observation_id=ctx.get("context_observation_id"),
        stop_reference=pj.get("intraday_setup_invalidation") or pj.get("daily_thesis_invalidation"),
    )
    portfolio.cash -= cost
    portfolio.open_position = pos
    note_reentry_entry_consumed(
        portfolio,
        symbol=bar.symbol.upper(),
        trading_date=bar.trading_date,
        setup_cycle_id=setup_cycle_id_from_context(ctx),
    )
    return True


def record_blocked_entry(
    portfolio: PortfolioSimState,
    *,
    symbol: str,
    signal_ts: datetime,
    reason: str,
    candidate_action: str,
    tie_break_json: dict | None = None,
    active_position_symbol: str | None = None,
) -> None:
    portfolio.blocked_signals.append(
        {
            "symbol": symbol.upper(),
            "signal_ts": signal_ts,
            "candidate_action": candidate_action,
            "block_reason": reason,
            "tie_break_json": tie_break_json,
            "active_position_symbol": active_position_symbol
            or (portfolio.open_position.symbol if portfolio.open_position else None),
        }
    )
