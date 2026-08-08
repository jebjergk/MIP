"""Re-entry Policy V0.1 — same-day re-entry after a fresh setup cycle (simulator-side)."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any

from .context_ruleset_v01 import ACTION_CONSIDER_ENTRY, ACTION_ENTRY_ARMED, STATE_ENTRY_ARMED

REENTRY_COOLDOWN_BARS = 2

BLOCK_REENTRY_EOD = "REENTRY_BLOCKED_AFTER_EOD_EXIT"
BLOCK_REENTRY_COOLDOWN = "REENTRY_COOLDOWN_ACTIVE"
BLOCK_REENTRY_SETUP_CONSUMED = "REENTRY_SETUP_ALREADY_USED"
BLOCK_REENTRY_FRESH_ARM_REQUIRED = "REENTRY_FRESH_ARM_REQUIRED"
BLOCK_REENTRY_WAIT_NEW_SETUP = "REENTRY_WAIT_FOR_NEW_SETUP"

PLAIN_MESSAGES: dict[str, str] = {
    BLOCK_REENTRY_EOD: "No re-entry after end-of-day exit on this symbol today.",
    BLOCK_REENTRY_COOLDOWN: "Re-entry cooldown: wait for more bars after the previous exit.",
    BLOCK_REENTRY_SETUP_CONSUMED: "Previous setup already used.",
    BLOCK_REENTRY_FRESH_ARM_REQUIRED: "Waiting for a new setup after the previous trade.",
    BLOCK_REENTRY_WAIT_NEW_SETUP: "Waiting for a new setup after the previous trade.",
}


def plain_language_for_block(reason: str | None, *, bars_since_exit: int | None = None) -> str | None:
    if not reason:
        return None
    if reason == BLOCK_REENTRY_COOLDOWN and bars_since_exit is not None:
        remaining = max(0, REENTRY_COOLDOWN_BARS - bars_since_exit)
        if remaining > 0:
            suffix = "bar" if remaining == 1 else "bars"
            return f"Re-entry cooldown: {remaining} {suffix} remaining."
    return PLAIN_MESSAGES.get(reason)


def _day_key(symbol: str, trading_date: date | str) -> str:
    td = trading_date.isoformat() if isinstance(trading_date, date) else str(trading_date)[:10]
    return f"{symbol.upper()}|{td}"


def setup_cycle_id_from_context(ctx: dict[str, Any]) -> str | None:
    pj = ctx.get("payload_json") or {}
    layers = pj.get("layers_json") or pj
    diag = layers.get("diagnostics") or pj.get("diagnostics") or {}
    cycle = diag.get("setup_cycle_id")
    if cycle is not None and str(cycle).strip():
        return str(cycle)
    armed_bar = diag.get("entry_armed_bar")
    if armed_bar is not None:
        return f"legacy_arm:{armed_bar}"
    return None


@dataclass
class SymbolDayReentryState:
    trading_date: str
    last_exit_bar_index: int | None = None
    last_exit_reason: str | None = None
    consumed_setup_cycle_id: str | None = None
    fresh_arm_cycle_id: str | None = None
    fresh_arm_bar_index: int | None = None
    trades_today: int = 0
    eod_blocked: bool = False
    left_entry_state_after_exit: bool = False


@dataclass
class ReentrySessionTracker:
    by_day: dict[str, SymbolDayReentryState] = field(default_factory=dict)

    def state_for(self, symbol: str, trading_date: date | str) -> SymbolDayReentryState:
        key = _day_key(symbol, trading_date)
        td = trading_date.isoformat() if isinstance(trading_date, date) else str(trading_date)[:10]
        if key not in self.by_day:
            self.by_day[key] = SymbolDayReentryState(trading_date=td)
        return self.by_day[key]


def get_reentry_tracker(portfolio: Any) -> ReentrySessionTracker:
    tr = getattr(portfolio, "reentry_tracker", None)
    if tr is None:
        tr = ReentrySessionTracker()
        portfolio.reentry_tracker = tr
    return tr


def note_reentry_exit(
    portfolio: Any,
    *,
    symbol: str,
    trading_date: date | str,
    bar_index_in_session: int,
    exit_reason: str,
) -> None:
    st = get_reentry_tracker(portfolio).state_for(symbol, trading_date)
    st.last_exit_bar_index = bar_index_in_session
    st.last_exit_reason = str(exit_reason or "")
    st.fresh_arm_cycle_id = None
    st.fresh_arm_bar_index = None
    st.left_entry_state_after_exit = False
    if st.last_exit_reason in ("FORCED_END_OF_DAY_EXIT", "FORCED_WEEK_END_FLATTEN"):
        st.eod_blocked = True


def note_reentry_entry_consumed(
    portfolio: Any,
    *,
    symbol: str,
    trading_date: date | str,
    setup_cycle_id: str | None,
) -> None:
    st = get_reentry_tracker(portfolio).state_for(symbol, trading_date)
    st.trades_today += 1
    if setup_cycle_id:
        st.consumed_setup_cycle_id = setup_cycle_id
    st.fresh_arm_cycle_id = None
    st.fresh_arm_bar_index = None


def observe_reentry_context_bar(
    portfolio: Any,
    *,
    symbol: str,
    trading_date: date | str,
    bar_index_in_session: int,
    ctx: dict[str, Any],
) -> None:
    """Track ENTRY_ARMED cycles and leaving armed state after an exit."""
    st = get_reentry_tracker(portfolio).state_for(symbol, trading_date)
    action = str(ctx.get("selected_action") or "")
    state_before = str(ctx.get("state_before") or "")

    if st.last_exit_bar_index is not None and not st.eod_blocked:
        if action not in (ACTION_ENTRY_ARMED, ACTION_CONSIDER_ENTRY, STATE_ENTRY_ARMED):
            st.left_entry_state_after_exit = True

    if st.eod_blocked or st.last_exit_bar_index is None:
        return

    bars_since = bar_index_in_session - st.last_exit_bar_index
    if bars_since < REENTRY_COOLDOWN_BARS:
        return

    if action == ACTION_ENTRY_ARMED:
        cycle = setup_cycle_id_from_context(ctx)
        if not cycle:
            return
        if cycle == st.consumed_setup_cycle_id:
            return
        st.fresh_arm_cycle_id = cycle
        st.fresh_arm_bar_index = bar_index_in_session


def evaluate_reentry_for_consider_entry(
    portfolio: Any,
    *,
    symbol: str,
    trading_date: date | str,
    bar_index_in_session: int,
    ctx: dict[str, Any],
) -> tuple[bool, str | None]:
    """
    Return (allowed, block_reason_code).
    First entry of the symbol-day skips re-entry gates.
    """
    st = get_reentry_tracker(portfolio).state_for(symbol, trading_date)
    if st.trades_today == 0 and st.last_exit_bar_index is None:
        return True, None

    if st.eod_blocked or st.last_exit_reason in ("FORCED_END_OF_DAY_EXIT", "FORCED_WEEK_END_FLATTEN"):
        return False, BLOCK_REENTRY_EOD

    if st.last_exit_bar_index is None:
        return True, None

    bars_since = bar_index_in_session - st.last_exit_bar_index
    if bars_since < REENTRY_COOLDOWN_BARS:
        return False, BLOCK_REENTRY_COOLDOWN

    cycle = setup_cycle_id_from_context(ctx)
    if not cycle:
        return False, BLOCK_REENTRY_WAIT_NEW_SETUP

    if cycle == st.consumed_setup_cycle_id:
        return False, BLOCK_REENTRY_SETUP_CONSUMED

    if st.fresh_arm_cycle_id != cycle:
        return False, BLOCK_REENTRY_FRESH_ARM_REQUIRED

    if st.fresh_arm_bar_index is not None and bar_index_in_session <= st.fresh_arm_bar_index:
        return False, BLOCK_REENTRY_FRESH_ARM_REQUIRED

    state_before = str(ctx.get("state_before") or "")
    if state_before != STATE_ENTRY_ARMED and str(ctx.get("selected_action") or "") == ACTION_CONSIDER_ENTRY:
        return False, BLOCK_REENTRY_FRESH_ARM_REQUIRED

    return True, None


def format_setup_cycle_id(symbol: str, trading_date: date, seq: int) -> str:
    return f"{symbol.upper()}|{trading_date.isoformat()}|{seq}"
