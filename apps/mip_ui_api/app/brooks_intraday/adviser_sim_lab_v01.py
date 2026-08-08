"""Adviser POC lab simulation — $1k cash, whole shares, one position."""

from __future__ import annotations

import math
import uuid
from dataclasses import dataclass, field
from typing import Any

from .constants import DEFAULT_STARTING_CASH

LAB_STARTING_CASH = float(DEFAULT_STARTING_CASH)


def whole_share_qty(*, available_cash: float, entry_price: float) -> int:
    if entry_price <= 0 or available_cash <= 0:
        return 0
    return int(math.floor(available_cash / entry_price))


@dataclass
class LabSimState:
    cash: float = LAB_STARTING_CASH
    position_qty: int = 0
    position_avg: float | None = None
    stop_price: float | None = None
    open_trade_id: str | None = None
    entries: int = 0
    exits: int = 0
    entry_blocked: list[dict[str, Any]] = field(default_factory=list)


def try_open_long(
    state: LabSimState,
    *,
    entry_price: float,
    stop_price: float,
    bar_ts: Any,
    block_reason: str | None = None,
) -> bool:
    if block_reason:
        state.entry_blocked.append({"reason": block_reason, "ts": str(bar_ts), "price": entry_price})
        return False
    if state.position_qty > 0:
        state.entry_blocked.append({"reason": "ONE_POSITION_ONLY", "ts": str(bar_ts)})
        return False
    qty = whole_share_qty(available_cash=state.cash, entry_price=entry_price)
    if qty < 1:
        state.entry_blocked.append({"reason": "INSUFFICIENT_CASH", "ts": str(bar_ts), "cash": state.cash})
        return False
    cost = qty * entry_price
    state.cash -= cost
    state.position_qty = qty
    state.position_avg = entry_price
    state.stop_price = stop_price
    state.open_trade_id = str(uuid.uuid4())
    state.entries += 1
    return True


def try_close_long(
    state: LabSimState,
    *,
    exit_price: float,
    reason: str,
) -> tuple[float, int] | None:
    if state.position_qty <= 0:
        return None
    qty = state.position_qty
    avg = float(state.position_avg or exit_price)
    pnl = (exit_price - avg) * qty
    state.cash += exit_price * qty
    state.position_qty = 0
    state.position_avg = None
    state.stop_price = None
    state.exits += 1
    return pnl, qty


def summarize_sim_trade_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Summarize BROOKS_INTRADAY_ADVISER_SIM_TRADE rows (may include entry-only + exit rows)."""
    entries = 0
    completed = 0
    open_positions = 0
    exits = 0
    realized = 0.0
    seen_exit: set[str] = set()
    for r in rows:
        tid = str(r.get("trade_id") or r.get("TRADE_ID") or "")
        exit_ts = r.get("exit_ts_ny") or r.get("EXIT_TS_NY")
        exit_px = r.get("exit_price") if "exit_price" in r else r.get("EXIT_PRICE")
        pnl = r.get("realized_pnl") if "realized_pnl" in r else r.get("REALIZED_PNL")
        if exit_ts is None and exit_px is None:
            entries += 1
            open_positions += 1
        if exit_ts is not None or exit_px is not None:
            if tid and tid in seen_exit:
                continue
            if tid:
                seen_exit.add(tid)
            exits += 1
            if pnl is not None:
                realized += float(pnl)
                completed += 1
            open_positions = max(0, open_positions - 1)
    return {
        "entries": entries,
        "open_positions": open_positions,
        "completed_round_trips": completed,
        "exits": exits,
        "realized_pnl": round(realized, 4),
    }
