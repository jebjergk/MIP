"""Phase D — BROOKS_CONTEXT_RULESET_V0_3 context engine (isolated from V0.2)."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any

from .bars import HistoricalBar
from .context_engine_v01 import (
    ContextResult,
    _bar_range,
    _dossier_levels,
    _pattern_families,
    _select_action,
    _terms,
)
from .context_engine_v02 import _meaningful_long_patterns_v02, _simulation_ready
from .context_ruleset_v01 import (
    EFFECT_INVALIDATES,
    EFFECT_NEUTRAL,
    RECLAIM_BELOW,
    RECLAIM_CLOSED_ABOVE,
    RECLAIM_CONFIRMED,
    SUPPORT_ABOVE,
    SUPPORT_HELD,
    SUPPORT_TESTING,
)
from .context_ruleset_v03 import (
    ACTION_CONSIDER_ENTRY,
    ACTION_DO_NOT_CHASE,
    ACTION_DO_NOT_ENTER,
    ACTION_ENTRY_ARMED,
    ACTION_OBSERVE,
    ACTION_THESIS_INVALIDATED,
    ACTION_WAIT,
    ACTION_WAIT_FOR_FOLLOW_THROUGH,
    ACTION_WAIT_FOR_PULLBACK,
    ACTION_WAIT_FOR_RECLAIM,
    BLOCKER_AT_UNBROKEN_RESISTANCE,
    BLOCKER_BEARISH_CANCELLATION,
    BLOCKER_BREAKOUT_CANDIDATE_NOT_CONFIRMED,
    BLOCKER_CANDIDATE_EXPIRED,
    BLOCKER_DAILY_THESIS_INVALIDATED,
    BLOCKER_DEFERRED_BY_DAILY_THESIS,
    BLOCKER_DO_NOT_CHASE_EXTENSION,
    BLOCKER_ENTRY_RISK_TOO_LARGE,
    BLOCKER_FAILED_BREAKOUT,
    BLOCKER_GAP_ACCEPTANCE_REQUIRED,
    BLOCKER_INSUFFICIENT_TIME,
    BLOCKER_LIMITED_ROOM_TO_NEXT,
    BLOCKER_LIMITED_ROOM_TO_UNBROKEN,
    BLOCKER_NO_DAILY_LONG_AUTHORIZATION,
    BLOCKER_NO_VALID_NEXT_RESISTANCE,
    BLOCKER_ONE_POSITION_TIEBREAK_SKIP,
    BLOCKER_WAITING_FOR_BREAKOUT_FOLLOW_THROUGH,
    BLOCKER_WAITING_FOR_BREAKOUT_PULLBACK,
    BLOCKER_WAITING_FOR_INTRADAY_UPGRADE,
    LAST_ENTRY_BAR_INDEX_IN_SESSION,
    RL_BROKEN_CONFIRMED,
    RL_BROKEN_PENDING,
    RL_FAILED_BREAKOUT,
    RL_UNBROKEN,
    ROOM_ACCEPTABLE,
    ROOM_AMPLE,
    ROOM_AT_RESISTANCE,
    ROOM_LIMITED,
    ROOM_UNKNOWN_NO_NEXT,
    RULESET_VERSION,
    STATE_DO_NOT_CHASE,
    STATE_ENTRY_ARMED,
    STATE_ENTRY_BLOCKED,
    STATE_FAILED_BREAKOUT,
    STATE_INTRADAY_UPGRADE_CANDIDATE,
    STATE_OBSERVATION_ONLY,
    STATE_THESIS_INVALIDATED,
    STATE_WAITING_FOR_BREAKOUT_CONFIRMATION,
    STATE_WAITING_FOR_BREAKOUT_PULLBACK,
    STATE_WAITING_FOR_PULLBACK,
    STATE_WAITING_FOR_RECLAIM,
    VERDICT_CLASS_AUTHORIZED_DAILY,
    VERDICT_CLASS_DEFER,
    VERDICT_CLASS_NO_CLEAR_LONG,
    normalize_verdict,
    resolve_params,
    verdict_class,
)


def _session_key(symbol: str, trading_date: date) -> str:
    return f"{symbol.upper()}|{trading_date.isoformat()}"


@dataclass
class V03SessionState:
    symbol: str
    trading_date: date
    advisory_state: str = STATE_OBSERVATION_ONLY
    resistance_lifecycle: str = RL_UNBROKEN
    path_class: str = "NONE"  # DAILY | UPGRADE | NONE
    session_bars: int = 0
    session_high: float = -1e18
    session_low: float = 1e18
    initialized: bool = False
    primary_resistance: float | None = None
    broken_resistance: float | None = None
    candidate_bar_index: int | None = None
    confirm_deadline: int | None = None
    state_entered_bar: int = 0
    bars_in_state: int = 0
    consecutive_closes_above: int = 0
    consecutive_closes_below: int = 0
    gap_above_open: bool = False
    gap_acceptance_closes: int = 0
    confirmation_score: int = 0
    entry_armed_bar: int | None = None
    setup_cycle_seq: int = 0
    current_setup_cycle_id: str | None = None
    dnc_active: bool = False
    post_break_high: float | None = None
    post_break_low: float | None = None
    bars_since_confirm: int = 0
    consecutive_large_bulls: int = 0
    pullback_tested: bool = False
    prior_strong_bear: bool = False
    closes_history: list[float] = field(default_factory=list)
    highs_history: list[float] = field(default_factory=list)
    lows_history: list[float] = field(default_factory=list)
    swing_highs: list[float] = field(default_factory=list)
    reclaim_bars_above: int = 0
    reclaim_stage: str = RECLAIM_BELOW
    support_status: str = SUPPORT_ABOVE
    setup_invalidation_level: float | None = None


def get_v03_session(state: dict[str, Any], symbol: str, trading_date: date) -> V03SessionState:
    bucket = state.setdefault("_context_v03_session_state", {})
    key = _session_key(symbol, trading_date)
    if key not in bucket:
        bucket[key] = V03SessionState(symbol=symbol.upper(), trading_date=trading_date)
    return bucket[key]


def reset_v03_session(state: dict[str, Any], symbol: str, trading_date: date) -> None:
    bucket = state.setdefault("_context_v03_session_state", {})
    bucket[_session_key(symbol, trading_date)] = V03SessionState(
        symbol=symbol.upper(), trading_date=trading_date
    )


def session_range(sess: V03SessionState, bar: HistoricalBar) -> float:
    return max(sess.session_high - sess.session_low, _bar_range(bar), 0.01)


def is_bullish_bar(bar: HistoricalBar) -> bool:
    return bar.close > bar.open


def is_bearish_bar(bar: HistoricalBar) -> bool:
    return bar.close < bar.open


def close_near_high(bar: HistoricalBar, params: dict[str, Any]) -> bool:
    br = _bar_range(bar)
    return (bar.high - bar.close) / br <= float(params["close_near_high_top_fraction"])


def close_near_low(bar: HistoricalBar, params: dict[str, Any]) -> bool:
    br = _bar_range(bar)
    return (bar.close - bar.low) / br <= float(params["bear_reversal_near_low_fraction"])


def is_strong_bear_bar(bar: HistoricalBar, params: dict[str, Any]) -> bool:
    return is_bearish_bar(bar) and close_near_low(bar, params)


def has_structural_breakout_confirmed(patterns: list[dict[str, Any]]) -> bool:
    for p in patterns:
        if str(p.get("pattern_family", "")).upper() == "STRUCTURAL_BREAKOUT" and str(
            p.get("lifecycle", "")
        ).upper() == "CONFIRMED":
            return True
    return False


def has_strong_bear_reversal_pattern(patterns: list[dict[str, Any]]) -> bool:
    for p in patterns:
        fam = str(p.get("pattern_family", "")).upper()
        lc = str(p.get("lifecycle", "")).upper()
        if fam in ("POSSIBLE_SELL_CLIMAX", "BEAR_MICRO_CHANNEL") and lc in (
            "CONFIRMED",
            "DEVELOPING",
        ):
            return True
    return False


def has_bear_micro_channel(patterns: list[dict[str, Any]]) -> bool:
    for p in patterns:
        if str(p.get("pattern_family", "")).upper() == "BEAR_MICRO_CHANNEL" and str(
            p.get("lifecycle", "")
        ).upper() == "CONFIRMED":
            return True
    return False


def objective_breakout_combo(
    bar: HistoricalBar, prior: HistoricalBar | None, params: dict[str, Any]
) -> bool:
    if prior is None:
        return False
    return (
        is_bullish_bar(bar)
        and bar.close > prior.high
        and close_near_high(bar, params)
        and bar.high > prior.high
    )


def is_breakout_candidate(
    *,
    bar: HistoricalBar,
    prior: HistoricalBar | None,
    resistance: float | None,
    patterns: list[dict[str, Any]],
    thesis_effect: str,
    bar_index: int,
    params: dict[str, Any],
) -> bool:
    if resistance is None:
        return False
    if bar.close <= resistance:
        return False
    if thesis_effect == EFFECT_INVALIDATES:
        return False
    if has_strong_bear_reversal_pattern(patterns):
        return False
    if bar_index > int(params["last_entry_bar_index_in_session"]):
        return False
    struct = has_structural_breakout_confirmed(patterns)
    combo = objective_breakout_combo(bar, prior, params)
    return struct or combo


def material_close_below(close: float, level: float, sr: float, params: dict[str, Any]) -> bool:
    tol = float(params["breakout_fail_tolerance_fraction"]) * sr
    return close < level - tol


def is_failed_breakout(
    *,
    sess: V03SessionState,
    bar: HistoricalBar,
    prior: HistoricalBar | None,
    patterns: list[dict[str, Any]],
    params: dict[str, Any],
) -> bool:
    if sess.resistance_lifecycle not in (RL_BROKEN_PENDING, RL_BROKEN_CONFIRMED):
        return False
    level = sess.broken_resistance
    if level is None:
        return False
    sr = session_range(sess, bar)
    if material_close_below(bar.close, level, sr, params):
        return True
    if sess.resistance_lifecycle == RL_BROKEN_PENDING and sess.consecutive_closes_below >= 2:
        return True
    if has_bear_micro_channel(patterns):
        return True
    if (
        sess.prior_strong_bear
        and prior is not None
        and is_strong_bear_bar(bar, params)
        and sess.resistance_lifecycle == RL_BROKEN_PENDING
    ):
        return True
    if (
        sess.candidate_bar_index is not None
        and sess.session_bars == sess.candidate_bar_index + 2
        and material_close_below(bar.close, level, sr, params)
        and is_bearish_bar(bar)
    ):
        return True
    return False


def is_breakout_confirmed_route_a(
    *,
    sess: V03SessionState,
    bar: HistoricalBar,
    bars_since_candidate: int,
    params: dict[str, Any],
) -> bool:
    if sess.candidate_bar_index is None or sess.broken_resistance is None:
        return False
    n = int(params["breakout_confirm_window_bars"])
    if bars_since_candidate < 1 or bars_since_candidate > n:
        return False
    level = sess.broken_resistance
    sr = session_range(sess, bar)
    tol = float(params["breakout_fail_tolerance_fraction"]) * sr
    if any(c < level - tol for c in sess.closes_history[sess.candidate_bar_index :]):
        return False
    # Need a bullish follow-through bar after the candidate
    if bars_since_candidate >= 1 and is_bullish_bar(bar) and bar.close >= level - tol:
        return True
    return False


def is_breakout_confirmed_route_b(
    *,
    sess: V03SessionState,
    bar: HistoricalBar,
    params: dict[str, Any],
) -> bool:
    if sess.broken_resistance is None:
        return False
    m = int(params["breakout_acceptance_closes"])
    return sess.consecutive_closes_above >= m and bar.close > sess.broken_resistance


def is_breakout_confirmed(
    *,
    sess: V03SessionState,
    bar: HistoricalBar,
    params: dict[str, Any],
) -> bool:
    if sess.candidate_bar_index is None:
        return False
    bars_since = sess.session_bars - 1 - sess.candidate_bar_index
    return is_breakout_confirmed_route_a(
        sess=sess, bar=bar, bars_since_candidate=bars_since, params=params
    ) or is_breakout_confirmed_route_b(sess=sess, bar=bar, params=params)


def entry_risk_fraction(
    *,
    close: float,
    stop_ref: float | None,
    sr: float,
) -> float | None:
    if stop_ref is None:
        return None
    return max(close - stop_ref, 0.0) / sr


def is_valid_breakout_pullback(
    *,
    sess: V03SessionState,
    bar: HistoricalBar,
    patterns: list[dict[str, Any]],
    meaningful: list[dict[str, Any]],
    params: dict[str, Any],
    stop_ref: float | None,
) -> bool:
    if sess.resistance_lifecycle != RL_BROKEN_CONFIRMED or sess.broken_resistance is None:
        return False
    level = sess.broken_resistance
    sr = session_range(sess, bar)
    retest_tol = float(params["pullback_retest_tolerance_fraction"]) * sr
    hold_tol = float(params["pullback_hold_tolerance_fraction"]) * sr
    approached = min(bar.low, bar.close) <= level + retest_tol
    if approached:
        sess.pullback_tested = True
    if not sess.pullback_tested:
        return False
    if bar.close < level - hold_tol:
        return False
    # Bearish momentum not dominant
    if len(sess.closes_history) >= 2:
        recent_bears = 0
        for i in range(max(0, len(sess.closes_history) - 2), len(sess.closes_history)):
            # approximate using close vs prior
            pass
    if sess.consecutive_closes_below >= 2:
        return False
    signal = bool(meaningful) or (is_bullish_bar(bar) and close_near_high(bar, params))
    if not signal:
        return False
    risk = entry_risk_fraction(close=bar.close, stop_ref=stop_ref or level, sr=sr)
    if risk is not None and risk > float(params["max_entry_risk_fraction"]):
        return False
    return True


def is_consolidation_above_resistance(
    *,
    sess: V03SessionState,
    bar: HistoricalBar,
    patterns: list[dict[str, Any]],
    params: dict[str, Any],
) -> bool:
    if sess.resistance_lifecycle != RL_BROKEN_CONFIRMED or sess.broken_resistance is None:
        return False
    if sess.dnc_active:
        return False
    if has_strong_bear_reversal_pattern(patterns):
        return False
    if sess.bars_since_confirm > int(params["consolidation_expiry_bars"]):
        return False
    min_bars = int(params["consolidation_min_bars"])
    if sess.consecutive_closes_above < min_bars:
        return False
    level = sess.broken_resistance
    sr = session_range(sess, bar)
    dip_tol = float(params["consolidation_dip_tolerance_fraction"]) * sr
    if bar.close < level - dip_tol:
        return False
    if sess.post_break_high is None or sess.post_break_low is None:
        return False
    swing = max(sess.post_break_high - sess.post_break_low, 0.01)
    # Current consolidation width vs post-break swing
    width = (sess.session_high - sess.session_low)  # fallback
    if len(sess.highs_history) >= min_bars:
        recent_h = max(sess.highs_history[-min_bars:])
        recent_l = min(sess.lows_history[-min_bars:])
        width = recent_h - recent_l
    if width / swing > float(params["consolidation_max_range_fraction"]):
        # If swing itself is tiny, allow
        if swing >= sr * 0.15:
            return False
    return True


def is_do_not_chase_trigger(
    *,
    sess: V03SessionState,
    bar: HistoricalBar,
    dnc_level: float | None,
    params: dict[str, Any],
    stop_ref: float | None,
) -> bool:
    if dnc_level is not None and bar.close >= dnc_level:
        return True
    sr = session_range(sess, bar)
    active_break = sess.resistance_lifecycle in (RL_BROKEN_PENDING, RL_BROKEN_CONFIRMED)
    if active_break and sess.broken_resistance is not None:
        ext = (bar.close - sess.broken_resistance) / sr
        if ext >= float(params["dnc_extension_from_break_fraction"]):
            return True
    # Consecutive large bulls only matter once a breakout path is live
    if active_break and sess.consecutive_large_bulls >= int(params["dnc_consecutive_bull_bars"]):
        return True
    # Entry-risk DNC only when evaluating an active upgrade/entry path (not bare observation)
    if active_break or sess.advisory_state in (
        STATE_ENTRY_ARMED,
        STATE_WAITING_FOR_BREAKOUT_PULLBACK,
        STATE_WAITING_FOR_BREAKOUT_CONFIRMATION,
    ):
        risk = entry_risk_fraction(close=bar.close, stop_ref=stop_ref, sr=sr)
        if risk is not None and risk > float(params["max_entry_risk_fraction"]):
            return True
    return False


def may_reset_do_not_chase(
    *,
    sess: V03SessionState,
    bar: HistoricalBar,
    patterns: list[dict[str, Any]],
    meaningful: list[dict[str, Any]],
    params: dict[str, Any],
    stop_ref: float | None,
    bar_index: int,
) -> bool:
    if not sess.dnc_active:
        return False
    if bar_index > int(params["last_entry_bar_index_in_session"]):
        return False
    if sess.resistance_lifecycle == RL_FAILED_BREAKOUT:
        return False
    pullback_ok = is_valid_breakout_pullback(
        sess=sess,
        bar=bar,
        patterns=patterns,
        meaningful=meaningful,
        params=params,
        stop_ref=stop_ref,
    )
    consol_ok = is_consolidation_above_resistance(
        sess=sess, bar=bar, patterns=patterns, params=params
    )
    if not (pullback_ok or consol_ok):
        return False
    sr = session_range(sess, bar)
    risk = entry_risk_fraction(close=bar.close, stop_ref=stop_ref or sess.broken_resistance, sr=sr)
    if risk is not None and risk > float(params["max_entry_risk_fraction"]):
        return False
    # Must no longer be extended
    if sess.broken_resistance is not None:
        ext = (bar.close - sess.broken_resistance) / sr
        if ext >= float(params["dnc_extension_from_break_fraction"]):
            return False
    return True


def is_bearish_cancellation(
    *,
    sess: V03SessionState,
    bar: HistoricalBar,
    patterns: list[dict[str, Any]],
    thesis_effect: str,
    params: dict[str, Any],
    prior: HistoricalBar | None,
) -> bool:
    if thesis_effect == EFFECT_INVALIDATES:
        return True
    if is_failed_breakout(sess=sess, bar=bar, prior=prior, patterns=patterns, params=params):
        return True
    if has_bear_micro_channel(patterns):
        return True
    if sess.prior_strong_bear and is_strong_bear_bar(bar, params):
        return True
    if (
        sess.broken_resistance is not None
        and len(sess.highs_history) >= 3
        and len(sess.lows_history) >= 3
    ):
        h1, h2 = sess.highs_history[-2], sess.highs_history[-1]
        l1, l2 = sess.lows_history[-2], sess.lows_history[-1]
        if h2 < h1 and l2 < l1 and bar.close < sess.broken_resistance:
            return True
    if sess.broken_resistance is not None:
        sr = session_range(sess, bar)
        if material_close_below(bar.close, sess.broken_resistance, sr, params):
            return True
    return False


def select_next_resistance(
    *,
    dossier: dict[str, Any],
    broken_level: float,
    close: float,
    swing_highs: list[float],
) -> float | None:
    zones = dossier.get("resistance_zones") or []
    candidates: list[float] = []
    for z in zones:
        if not isinstance(z, dict):
            continue
        lo = z.get("lower", z.get("low"))
        hi = z.get("upper", z.get("high"))
        if lo is not None and hi is not None:
            mid = (float(lo) + float(hi)) / 2
        elif z.get("price") is not None:
            mid = float(z["price"])
        else:
            continue
        if mid > broken_level + 1e-9:
            candidates.append(mid)
    if candidates:
        return min(candidates)
    swings = [s for s in swing_highs if s > close and s > broken_level]
    if swings:
        return min(swings)
    return None


def room_to_level(
    *,
    close: float,
    resistance: float | None,
    sr: float,
    params: dict[str, Any],
) -> tuple[str, list[str]]:
    if resistance is None:
        return ROOM_UNKNOWN_NO_NEXT, []
    dist = resistance - close
    if dist <= 0:
        return ROOM_AT_RESISTANCE, []
    frac = dist / sr
    if frac >= float(params["min_room_ample_fraction"]):
        return ROOM_AMPLE, []
    if frac >= float(params["min_room_acceptable_fraction"]):
        return ROOM_ACCEPTABLE, []
    return ROOM_LIMITED, []


def time_ok(bar_index: int, params: dict[str, Any]) -> bool:
    return bar_index <= int(params["last_entry_bar_index_in_session"])


def tie_break_v03(
    candidates: list[dict[str, Any]],
    *,
    params: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Deterministic multi-symbol ranking for simultaneous CONSIDER_ENTRY."""
    params = resolve_params(params)
    priority = list(params["symbol_priority"])  # type: ignore[arg-type]

    def sort_key(c: dict[str, Any]) -> tuple:
        path = 0 if c.get("path_class") == "DAILY" else 1
        room = str(c.get("room_class") or "")
        room_rank = {ROOM_AMPLE: 0, ROOM_ACCEPTABLE: 1, ROOM_UNKNOWN_NO_NEXT: 2}.get(room, 9)
        risk = float(c.get("entry_risk") if c.get("entry_risk") is not None else 9e9)
        conf = -int(c.get("confirmation_score") or 0)
        ts = str(c.get("signal_ts") or "")
        sym = str(c.get("symbol") or "").upper()
        sym_i = priority.index(sym) if sym in priority else 999
        return (path, room_rank, risk, conf, ts, sym_i)

    ranked = sorted(candidates, key=sort_key)
    winner = ranked[0]
    losers = []
    for c in ranked[1:]:
        losers.append({**c, "skip_reason": BLOCKER_ONE_POSITION_TIEBREAK_SKIP})
    return winner, losers


def _init_v03(sess: V03SessionState, dossier: dict[str, Any], params: dict[str, Any]) -> None:
    if sess.initialized:
        return
    levels = _dossier_levels(dossier)
    sess.primary_resistance = levels.get("primary_resistance")
    v = normalize_verdict(str(dossier.get("paa_verdict") or ""))
    vmap = params["verdict_initial_state"]  # type: ignore[index]
    sess.advisory_state = str(vmap.get(v) or vmap.get("DEFAULT") or STATE_OBSERVATION_ONLY)
    vc = verdict_class(v)
    if vc == VERDICT_CLASS_AUTHORIZED_DAILY:
        sess.path_class = "DAILY"
    elif vc == VERDICT_CLASS_NO_CLEAR_LONG:
        sess.path_class = "UPGRADE"
        sess.advisory_state = STATE_OBSERVATION_ONLY
    else:
        sess.path_class = "NONE"
        sess.advisory_state = STATE_OBSERVATION_ONLY
    sess.initialized = True


def _update_histories(sess: V03SessionState, bar: HistoricalBar, params: dict[str, Any]) -> None:
    sess.closes_history.append(bar.close)
    sess.highs_history.append(bar.high)
    sess.lows_history.append(bar.low)
    if len(sess.highs_history) >= 3:
        i = len(sess.highs_history) - 2
        if (
            sess.highs_history[i] > sess.highs_history[i - 1]
            and sess.highs_history[i] > sess.highs_history[i + 1]
        ):
            sess.swing_highs.append(sess.highs_history[i])
    sr = session_range(sess, bar)
    large = _bar_range(bar) >= float(params["dnc_large_bar_fraction"]) * sr
    if is_bullish_bar(bar) and large:
        sess.consecutive_large_bulls += 1
    else:
        sess.consecutive_large_bulls = 0
    if sess.broken_resistance is not None:
        if bar.close > sess.broken_resistance:
            sess.consecutive_closes_above += 1
            sess.consecutive_closes_below = 0
        elif bar.close < sess.broken_resistance:
            sess.consecutive_closes_below += 1
            sess.consecutive_closes_above = 0
        if sess.resistance_lifecycle in (RL_BROKEN_PENDING, RL_BROKEN_CONFIRMED):
            sess.post_break_high = (
                bar.high
                if sess.post_break_high is None
                else max(sess.post_break_high, bar.high)
            )
            sess.post_break_low = (
                bar.low if sess.post_break_low is None else min(sess.post_break_low, bar.low)
            )
    if sess.resistance_lifecycle == RL_BROKEN_CONFIRMED:
        sess.bars_since_confirm += 1


def _set_state(sess: V03SessionState, new_state: str, bar_index: int) -> None:
    if new_state != sess.advisory_state:
        sess.advisory_state = new_state
        sess.state_entered_bar = bar_index
        sess.bars_in_state = 0
    else:
        sess.bars_in_state += 1


def _consider_entry_signal(bar: HistoricalBar, meaningful: list, params: dict[str, Any]) -> bool:
    return bool(meaningful) or (is_bullish_bar(bar) and close_near_high(bar, params))


def may_emit_consider_entry_v03(
    *,
    state_before: str,
    bar_index_in_session: int,
    sess: V03SessionState,
    bar: HistoricalBar,
    meaningful: list,
    params: dict[str, Any],
    opening: bool,
) -> bool:
    """Prior-bar arming gate: CONSIDER_ENTRY only when ENTRY_ARMED on the prior bar."""
    if opening or not time_ok(bar_index_in_session, params):
        return False
    if state_before != STATE_ENTRY_ARMED:
        return False
    if sess.entry_armed_bar is None:
        return False
    if bar_index_in_session <= sess.entry_armed_bar:
        return False
    return _consider_entry_signal(bar, meaningful, params)


def _arm_entry_on_bar(sess: V03SessionState, bar_index_in_session: int, state_before: str) -> None:
    from .reentry_policy_v01 import format_setup_cycle_id

    _set_state(sess, STATE_ENTRY_ARMED, bar_index_in_session)
    if state_before != STATE_ENTRY_ARMED:
        sess.entry_armed_bar = bar_index_in_session
        sess.setup_cycle_seq += 1
        sess.current_setup_cycle_id = format_setup_cycle_id(
            sess.symbol, sess.trading_date, sess.setup_cycle_seq
        )
    elif sess.entry_armed_bar is None:
        sess.entry_armed_bar = bar_index_in_session
        if not sess.current_setup_cycle_id:
            sess.setup_cycle_seq += 1
            sess.current_setup_cycle_id = format_setup_cycle_id(
                sess.symbol, sess.trading_date, sess.setup_cycle_seq
            )


def advance_context_v03_for_bar(
    *,
    state: dict[str, Any],
    dossier: dict[str, Any],
    symbol: str,
    trading_date: date,
    bar: HistoricalBar,
    bar_index_in_session: int,
    objective_obs: dict[str, Any],
    patterns: list[dict[str, Any]],
    params: dict[str, Any] | None = None,
    prior_bar: HistoricalBar | None = None,
    open_position_symbol: str | None = None,
) -> ContextResult:
    params = resolve_params(params)
    sess = get_v03_session(state, symbol, trading_date)
    _init_v03(sess, dossier, params)
    state_before = sess.advisory_state
    sess.session_bars += 1
    sess.session_high = max(sess.session_high, bar.high)
    sess.session_low = min(sess.session_low, bar.low)
    if sess.session_bars == 1 and sess.bars_in_state == 0:
        sess.state_entered_bar = bar_index_in_session

    def _out(
        sess_: V03SessionState,
        state_before_: str,
        candidates_: dict[str, str],
        blockers_: list[str],
        thesis_effect_: str,
        levels_: dict[str, Any],
        room_: str,
        markers_: list[str],
        rule_ids_: list[str],
        params_: dict[str, Any],
        meaningful_: list,
        next_res_: float | None,
    ) -> ContextResult:
        return _result(
            sess_,
            state_before_,
            candidates_,
            blockers_,
            thesis_effect_,
            levels_,
            room_,
            markers_,
            rule_ids_,
            params_,
            meaningful_,
            next_res_,
            symbol=symbol,
            open_position_symbol=open_position_symbol,
        )

    levels = _dossier_levels(dossier)
    if sess.primary_resistance is None:
        sess.primary_resistance = levels.get("primary_resistance")
    terms = _terms(objective_obs)
    pat_list = _pattern_families(patterns, params=params)
    meaningful = _meaningful_long_patterns_v02(pat_list, params)
    sr = session_range(sess, bar)
    opening = bar_index_in_session < int(params["opening_observation_bars"])
    v_raw = normalize_verdict(str(dossier.get("paa_verdict") or ""))
    vclass = verdict_class(v_raw)

    # Gap detection on first bar
    if bar_index_in_session == 0 and sess.primary_resistance is not None:
        if bar.open > sess.primary_resistance:
            sess.gap_above_open = True

    thesis_effect = EFFECT_NEUTRAL
    daily_inv = levels.get("daily_thesis_invalidation")
    if daily_inv is not None and bar.close < daily_inv:
        thesis_effect = EFFECT_INVALIDATES

    stop_ref = daily_inv if daily_inv is not None else levels.get("primary_support")
    if sess.broken_resistance is not None and sess.resistance_lifecycle == RL_BROKEN_CONFIRMED:
        stop_ref = sess.broken_resistance

    prior = prior_bar
    if prior is None and len(sess.closes_history) >= 1:
        # Reconstruct minimal prior from history for combo checks
        prior = HistoricalBar(
            symbol=bar.symbol,
            ts_utc=bar.ts_utc,
            ts_ny=bar.ts_ny,
            trading_date=bar.trading_date,
            open=sess.closes_history[-1],
            high=sess.highs_history[-1],
            low=sess.lows_history[-1],
            close=sess.closes_history[-1],
            volume=None,
            source="PRIOR_SYNTH",
            bar_size_minutes=5,
            rth=True,
        )

    _update_histories(sess, bar, params)
    sess.bars_in_state += 1

    blockers: list[str] = []
    markers: list[str] = []
    rule_ids: list[str] = ["CTX_V0_3"]
    candidates: dict[str, str] = {"OBSERVE": ACTION_OBSERVE, "WAIT": ACTION_WAIT}
    room = ROOM_ACCEPTABLE
    next_res: float | None = None
    sim_ready = _simulation_ready(dossier)

    # ----- DEFER hard block -----
    if vclass == VERDICT_CLASS_DEFER:
        _set_state(sess, STATE_OBSERVATION_ONLY, bar_index_in_session)
        candidates["DO_NOT_ENTER"] = ACTION_DO_NOT_ENTER
        blockers.append(BLOCKER_DEFERRED_BY_DAILY_THESIS)
        return _out(
            sess,
            state_before,
            candidates,
            blockers,
            thesis_effect,
            levels,
            room,
            markers,
            rule_ids,
            params,
            meaningful,
            next_res,
        )

    # ----- Daily invalidation -----
    if thesis_effect == EFFECT_INVALIDATES:
        _set_state(sess, STATE_THESIS_INVALIDATED, bar_index_in_session)
        sess.resistance_lifecycle = (
            RL_FAILED_BREAKOUT
            if sess.resistance_lifecycle in (RL_BROKEN_PENDING, RL_BROKEN_CONFIRMED)
            else sess.resistance_lifecycle
        )
        candidates["THESIS_INVALIDATED"] = ACTION_THESIS_INVALIDATED
        blockers.append(BLOCKER_DAILY_THESIS_INVALIDATED)
        return _out(
            sess,
            state_before,
            candidates,
            blockers,
            thesis_effect,
            levels,
            room,
            markers,
            rule_ids,
            params,
            meaningful,
            next_res,
        )

    # ----- Gap acceptance gate -----
    if sess.gap_above_open and sess.resistance_lifecycle == RL_UNBROKEN:
        if sess.primary_resistance is not None and bar.close > sess.primary_resistance:
            sess.gap_acceptance_closes += 1
        else:
            sess.gap_acceptance_closes = 0
        need = int(params["gap_acceptance_closes"])
        if sess.gap_acceptance_closes < need and sess.advisory_state == STATE_OBSERVATION_ONLY:
            blockers.append(BLOCKER_GAP_ACCEPTANCE_REQUIRED)
            candidates["WAIT"] = ACTION_WAIT
            # Still allow candidate evaluation after acceptance threshold; until then no upgrade
            if not (
                is_breakout_candidate(
                    bar=bar,
                    prior=prior,
                    resistance=sess.primary_resistance,
                    patterns=pat_list,
                    thesis_effect=thesis_effect,
                    bar_index=bar_index_in_session,
                    params=params,
                )
                and sess.gap_acceptance_closes >= need
            ):
                return _out(
                    sess,
                    state_before,
                    candidates,
                    blockers,
                    thesis_effect,
                    levels,
                    room,
                    markers,
                    rule_ids,
                    params,
                    meaningful,
                    next_res,
                )

    # ----- Failed / bearish cancel on upgrade path -----
    if sess.resistance_lifecycle in (RL_BROKEN_PENDING, RL_BROKEN_CONFIRMED):
        if is_bearish_cancellation(
            sess=sess,
            bar=bar,
            patterns=pat_list,
            thesis_effect=thesis_effect,
            params=params,
            prior=prior,
        ) or is_failed_breakout(
            sess=sess, bar=bar, prior=prior, patterns=pat_list, params=params
        ):
            sess.resistance_lifecycle = RL_FAILED_BREAKOUT
            _set_state(sess, STATE_FAILED_BREAKOUT, bar_index_in_session)
            candidates["DO_NOT_ENTER"] = ACTION_DO_NOT_ENTER
            candidates["WAIT_FOR_RECLAIM"] = ACTION_WAIT_FOR_RECLAIM
            blockers.append(BLOCKER_FAILED_BREAKOUT)
            if is_bearish_cancellation(
                sess=sess,
                bar=bar,
                patterns=pat_list,
                thesis_effect=thesis_effect,
                params=params,
                prior=prior,
            ):
                blockers.append(BLOCKER_BEARISH_CANCELLATION)
            sess.prior_strong_bear = is_strong_bear_bar(bar, params)
            return _out(
                sess,
                state_before,
                candidates,
                blockers,
                thesis_effect,
                levels,
                room,
                markers,
                rule_ids,
                params,
                meaningful,
                next_res,
            )

    # ----- Resistance lifecycle: detect candidate -----
    if (
        sess.resistance_lifecycle == RL_UNBROKEN
        and vclass == VERDICT_CLASS_NO_CLEAR_LONG
        and is_breakout_candidate(
            bar=bar,
            prior=prior,
            resistance=sess.primary_resistance,
            patterns=pat_list,
            thesis_effect=thesis_effect,
            bar_index=bar_index_in_session,
            params=params,
        )
    ):
        if sess.gap_above_open and sess.gap_acceptance_closes < int(params["gap_acceptance_closes"]):
            blockers.append(BLOCKER_GAP_ACCEPTANCE_REQUIRED)
        else:
            sess.resistance_lifecycle = RL_BROKEN_PENDING
            sess.broken_resistance = sess.primary_resistance
            sess.candidate_bar_index = bar_index_in_session
            sess.confirm_deadline = bar_index_in_session + int(params["breakout_confirm_window_bars"])
            sess.consecutive_closes_above = 1
            sess.post_break_high = bar.high
            sess.post_break_low = bar.low
            _set_state(sess, STATE_INTRADAY_UPGRADE_CANDIDATE, bar_index_in_session)
            markers.append("breakout_candidate")
            # Same-bar move into confirmation wait
            _set_state(sess, STATE_WAITING_FOR_BREAKOUT_CONFIRMATION, bar_index_in_session)
            blockers.append(BLOCKER_WAITING_FOR_BREAKOUT_FOLLOW_THROUGH)
            blockers.append(BLOCKER_BREAKOUT_CANDIDATE_NOT_CONFIRMED)
            candidates["WAIT_FOR_FOLLOW_THROUGH"] = ACTION_WAIT_FOR_FOLLOW_THROUGH
            sess.prior_strong_bear = is_strong_bear_bar(bar, params)
            return _out(
                sess,
                state_before,
                candidates,
                blockers,
                thesis_effect,
                levels,
                room,
                markers,
                rule_ids,
                params,
                meaningful,
                next_res,
            )

    # Daily path may also break resistance
    if (
        sess.resistance_lifecycle == RL_UNBROKEN
        and vclass == VERDICT_CLASS_AUTHORIZED_DAILY
        and is_breakout_candidate(
            bar=bar,
            prior=prior,
            resistance=sess.primary_resistance,
            patterns=pat_list,
            thesis_effect=thesis_effect,
            bar_index=bar_index_in_session,
            params=params,
        )
    ):
        sess.resistance_lifecycle = RL_BROKEN_PENDING
        sess.broken_resistance = sess.primary_resistance
        sess.candidate_bar_index = bar_index_in_session
        sess.confirm_deadline = bar_index_in_session + int(params["breakout_confirm_window_bars"])
        sess.consecutive_closes_above = 1
        markers.append("daily_path_breakout_pending")

    # ----- Confirmation -----
    if sess.resistance_lifecycle == RL_BROKEN_PENDING:
        if is_breakout_confirmed(sess=sess, bar=bar, params=params):
            sess.resistance_lifecycle = RL_BROKEN_CONFIRMED
            sess.bars_since_confirm = 0
            sess.confirmation_score = 2 if has_structural_breakout_confirmed(pat_list) else 1
            if is_breakout_confirmed_route_a(
                sess=sess,
                bar=bar,
                bars_since_candidate=(
                    bar_index_in_session - (sess.candidate_bar_index or bar_index_in_session)
                ),
                params=params,
            ):
                sess.confirmation_score += 1
            _set_state(sess, STATE_WAITING_FOR_BREAKOUT_PULLBACK, bar_index_in_session)
            markers.append("breakout_confirmed")
            blockers.append(BLOCKER_WAITING_FOR_BREAKOUT_PULLBACK)
            candidates["WAIT"] = ACTION_WAIT
        else:
            # expiry of confirmation window
            if (
                sess.candidate_bar_index is not None
                and bar_index_in_session - sess.candidate_bar_index
                >= int(params["expiry_breakout_confirmation_bars"])
            ):
                _set_state(sess, STATE_ENTRY_BLOCKED, bar_index_in_session)
                blockers.append(BLOCKER_BREAKOUT_CANDIDATE_NOT_CONFIRMED)
                blockers.append(BLOCKER_CANDIDATE_EXPIRED)
                candidates["WAIT"] = ACTION_WAIT
                # Reset pending so we don't confirm late
                sess.resistance_lifecycle = RL_UNBROKEN
                sess.broken_resistance = None
                sess.candidate_bar_index = None
            else:
                _set_state(sess, STATE_WAITING_FOR_BREAKOUT_CONFIRMATION, bar_index_in_session)
                blockers.append(BLOCKER_WAITING_FOR_BREAKOUT_FOLLOW_THROUGH)
                blockers.append(BLOCKER_BREAKOUT_CANDIDATE_NOT_CONFIRMED)
                candidates["WAIT_FOR_FOLLOW_THROUGH"] = ACTION_WAIT_FOR_FOLLOW_THROUGH
            sess.prior_strong_bear = is_strong_bear_bar(bar, params)
            return _out(
                sess,
                state_before,
                candidates,
                blockers,
                thesis_effect,
                levels,
                room,
                markers,
                rule_ids,
                params,
                meaningful,
                next_res,
            )

    # ----- Room after confirmed break -----
    if sess.resistance_lifecycle == RL_BROKEN_CONFIRMED and sess.broken_resistance is not None:
        next_res = select_next_resistance(
            dossier=dossier,
            broken_level=sess.broken_resistance,
            close=bar.close,
            swing_highs=sess.swing_highs,
        )
        room, _ = room_to_level(close=bar.close, resistance=next_res, sr=sr, params=params)
    elif sess.resistance_lifecycle == RL_UNBROKEN:
        res = sess.primary_resistance
        if res is not None:
            dist = res - bar.close
            prox = float(params["resistance_proximity_fraction"]) * sr
            if dist <= 0:
                room = ROOM_AT_RESISTANCE
            else:
                room, _ = room_to_level(close=bar.close, resistance=res, sr=sr, params=params)
                if 0 < dist <= prox and room == ROOM_LIMITED:
                    pass

    # ----- Late-session entry cutoff (both paths) -----
    # Index 72 opens 15:30 ET (~30 minutes before forced flatten).
    past_entry_cutoff = not time_ok(bar_index_in_session, params)
    if past_entry_cutoff and (
        sess.advisory_state
        in (
            STATE_ENTRY_ARMED,
            STATE_WAITING_FOR_BREAKOUT_PULLBACK,
            STATE_WAITING_FOR_BREAKOUT_CONFIRMATION,
            STATE_INTRADAY_UPGRADE_CANDIDATE,
            STATE_DO_NOT_CHASE,
            STATE_ENTRY_BLOCKED,
        )
        or sess.resistance_lifecycle == RL_BROKEN_CONFIRMED
        or sess.path_class == "DAILY"
    ):
        blockers.append(BLOCKER_INSUFFICIENT_TIME)

    # ----- DNC -----
    if is_do_not_chase_trigger(
        sess=sess, bar=bar, dnc_level=levels.get("do_not_chase"), params=params, stop_ref=stop_ref
    ):
        sess.dnc_active = True
    if sess.dnc_active:
        if may_reset_do_not_chase(
            sess=sess,
            bar=bar,
            patterns=pat_list,
            meaningful=meaningful,
            params=params,
            stop_ref=stop_ref,
            bar_index=bar_index_in_session,
        ):
            sess.dnc_active = False
            markers.append("dnc_reset")
            if sess.resistance_lifecycle == RL_BROKEN_CONFIRMED:
                _set_state(sess, STATE_WAITING_FOR_BREAKOUT_PULLBACK, bar_index_in_session)
        else:
            _set_state(sess, STATE_DO_NOT_CHASE, bar_index_in_session)
            candidates["DO_NOT_CHASE"] = ACTION_DO_NOT_CHASE
            blockers.append(BLOCKER_DO_NOT_CHASE_EXTENSION)
            sess.prior_strong_bear = is_strong_bear_bar(bar, params)
            return _out(
                sess,
                state_before,
                candidates,
                blockers,
                thesis_effect,
                levels,
                room,
                markers,
                rule_ids,
                params,
                meaningful,
                next_res,
            )

    # ----- FAILED sticky -----
    if sess.advisory_state == STATE_FAILED_BREAKOUT or sess.resistance_lifecycle == RL_FAILED_BREAKOUT:
        _set_state(sess, STATE_FAILED_BREAKOUT, bar_index_in_session)
        candidates["DO_NOT_ENTER"] = ACTION_DO_NOT_ENTER
        candidates["WAIT_FOR_RECLAIM"] = ACTION_WAIT_FOR_RECLAIM
        blockers.append(BLOCKER_FAILED_BREAKOUT)
        return _out(
            sess,
            state_before,
            candidates,
            blockers,
            thesis_effect,
            levels,
            room,
            markers,
            rule_ids,
            params,
            meaningful,
            next_res,
        )

    # ----- Upgrade path: pullback / arm -----
    if vclass == VERDICT_CLASS_NO_CLEAR_LONG:
        if sess.resistance_lifecycle == RL_UNBROKEN:
            # At unbroken resistance blockers
            res = sess.primary_resistance
            if res is not None:
                dist = res - bar.close
                prox = float(params["resistance_proximity_fraction"]) * sr
                if dist <= 0:
                    blockers.append(BLOCKER_AT_UNBROKEN_RESISTANCE)
                    _set_state(sess, STATE_ENTRY_BLOCKED, bar_index_in_session)
                    candidates["WAIT"] = ACTION_WAIT
                elif dist <= prox or room == ROOM_LIMITED:
                    blockers.append(BLOCKER_LIMITED_ROOM_TO_UNBROKEN)
                    _set_state(sess, STATE_OBSERVATION_ONLY, bar_index_in_session)
                    candidates["OBSERVE"] = ACTION_OBSERVE
                else:
                    _set_state(sess, STATE_OBSERVATION_ONLY, bar_index_in_session)
                    candidates["OBSERVE"] = ACTION_OBSERVE
                    blockers.append(BLOCKER_NO_DAILY_LONG_AUTHORIZATION)
                    blockers.append(BLOCKER_WAITING_FOR_INTRADAY_UPGRADE)
            else:
                _set_state(sess, STATE_OBSERVATION_ONLY, bar_index_in_session)
                blockers.append(BLOCKER_NO_DAILY_LONG_AUTHORIZATION)
            sess.prior_strong_bear = is_strong_bear_bar(bar, params)
            return _out(
                sess,
                state_before,
                candidates,
                blockers,
                thesis_effect,
                levels,
                room,
                markers,
                rule_ids,
                params,
                meaningful,
                next_res,
            )

        if sess.resistance_lifecycle == RL_BROKEN_CONFIRMED:
            # Expiry pullback wait
            if sess.bars_in_state >= int(params["expiry_breakout_pullback_bars"]) and sess.advisory_state == STATE_WAITING_FOR_BREAKOUT_PULLBACK:
                _set_state(sess, STATE_ENTRY_BLOCKED, bar_index_in_session)
                blockers.append(BLOCKER_CANDIDATE_EXPIRED)
                candidates["WAIT"] = ACTION_WAIT
                return _out(
                    sess,
                    state_before,
                    candidates,
                    blockers,
                    thesis_effect,
                    levels,
                    room,
                    markers,
                    rule_ids,
                    params,
                    meaningful,
                    next_res,
                )

            if past_entry_cutoff:
                blockers.append(BLOCKER_INSUFFICIENT_TIME)
                _set_state(sess, STATE_ENTRY_BLOCKED, bar_index_in_session)
                candidates["WAIT"] = ACTION_WAIT
                return _out(
                    sess,
                    state_before,
                    candidates,
                    blockers,
                    thesis_effect,
                    levels,
                    room,
                    markers,
                    rule_ids,
                    params,
                    meaningful,
                    next_res,
                )

            if room == ROOM_LIMITED and next_res is not None:
                _set_state(sess, STATE_ENTRY_BLOCKED, bar_index_in_session)
                blockers.append(BLOCKER_LIMITED_ROOM_TO_NEXT)
                candidates["WAIT"] = ACTION_WAIT
                return _out(
                    sess,
                    state_before,
                    candidates,
                    blockers,
                    thesis_effect,
                    levels,
                    room,
                    markers,
                    rule_ids,
                    params,
                    meaningful,
                    next_res,
                )

            pullback_ok = is_valid_breakout_pullback(
                sess=sess,
                bar=bar,
                patterns=pat_list,
                meaningful=meaningful,
                params=params,
                stop_ref=stop_ref,
            )
            consol_ok = is_consolidation_above_resistance(
                sess=sess, bar=bar, patterns=pat_list, params=params
            )
            unknown_safe = room == ROOM_UNKNOWN_NO_NEXT and pullback_ok
            room_ok = room in (ROOM_AMPLE, ROOM_ACCEPTABLE) or unknown_safe

            if room == ROOM_UNKNOWN_NO_NEXT and not unknown_safe:
                _set_state(sess, STATE_WAITING_FOR_BREAKOUT_PULLBACK, bar_index_in_session)
                blockers.append(BLOCKER_NO_VALID_NEXT_RESISTANCE)
                candidates["WAIT"] = ACTION_WAIT
                # fall through without arming
            elif (pullback_ok or consol_ok) and room_ok and time_ok(bar_index_in_session, params):
                risk = entry_risk_fraction(
                    close=bar.close, stop_ref=stop_ref or sess.broken_resistance, sr=sr
                )
                if risk is not None and risk > float(params["max_entry_risk_fraction"]):
                    blockers.append(BLOCKER_ENTRY_RISK_TOO_LARGE)
                    _set_state(sess, STATE_ENTRY_BLOCKED, bar_index_in_session)
                    candidates["WAIT"] = ACTION_WAIT
                else:
                    _arm_entry_on_bar(sess, bar_index_in_session, state_before)
                    candidates["ENTRY_ARMED"] = ACTION_ENTRY_ARMED
                    markers.append("entry_armed")
                    if may_emit_consider_entry_v03(
                        state_before=state_before,
                        bar_index_in_session=bar_index_in_session,
                        sess=sess,
                        bar=bar,
                        meaningful=meaningful,
                        params=params,
                        opening=opening,
                    ):
                        candidates["CONSIDER_ENTRY"] = ACTION_CONSIDER_ENTRY
                        markers.append("consider_entry")
            else:
                if not time_ok(bar_index_in_session, params):
                    blockers.append(BLOCKER_INSUFFICIENT_TIME)
                    _set_state(sess, STATE_ENTRY_BLOCKED, bar_index_in_session)
                else:
                    _set_state(sess, STATE_WAITING_FOR_BREAKOUT_PULLBACK, bar_index_in_session)
                    blockers.append(BLOCKER_WAITING_FOR_BREAKOUT_PULLBACK)
                candidates["WAIT"] = ACTION_WAIT

            sess.prior_strong_bear = is_strong_bear_bar(bar, params)
            return _out(
                sess,
                state_before,
                candidates,
                blockers,
                thesis_effect,
                levels,
                room,
                markers,
                rule_ids,
                params,
                meaningful,
                next_res,
            )

    # ----- AUTHORIZED_DAILY path (V0.2-style + V0.3 resistance/time) -----
    if vclass == VERDICT_CLASS_AUTHORIZED_DAILY:
        if not time_ok(bar_index_in_session, params):
            blockers.append(BLOCKER_INSUFFICIENT_TIME)
            _set_state(sess, STATE_ENTRY_BLOCKED, bar_index_in_session)
            candidates["WAIT"] = ACTION_WAIT
            return _out(
                sess,
                state_before,
                candidates,
                blockers,
                thesis_effect,
                levels,
                room,
                markers,
                rule_ids,
                params,
                meaningful,
                next_res,
            )

        # Unbroken resistance blocking
        if sess.resistance_lifecycle == RL_UNBROKEN and sess.primary_resistance is not None:
            dist = sess.primary_resistance - bar.close
            if dist <= 0:
                blockers.append(BLOCKER_AT_UNBROKEN_RESISTANCE)
                _set_state(sess, STATE_ENTRY_BLOCKED, bar_index_in_session)
                candidates["WAIT"] = ACTION_WAIT
                return _out(
                    sess,
                    state_before,
                    candidates,
                    blockers,
                    thesis_effect,
                    levels,
                    room,
                    markers,
                    rule_ids,
                    params,
                    meaningful,
                    next_res,
                )
            room, _ = room_to_level(
                close=bar.close, resistance=sess.primary_resistance, sr=sr, params=params
            )
            if room == ROOM_LIMITED:
                blockers.append(BLOCKER_LIMITED_ROOM_TO_UNBROKEN)
                _set_state(sess, STATE_ENTRY_BLOCKED, bar_index_in_session)
                candidates["WAIT"] = ACTION_WAIT
                return _out(
                    sess,
                    state_before,
                    candidates,
                    blockers,
                    thesis_effect,
                    levels,
                    room,
                    markers,
                    rule_ids,
                    params,
                    meaningful,
                    next_res,
                )

        if sess.resistance_lifecycle == RL_BROKEN_CONFIRMED:
            next_res = select_next_resistance(
                dossier=dossier,
                broken_level=float(sess.broken_resistance or 0),
                close=bar.close,
                swing_highs=sess.swing_highs,
            )
            room, _ = room_to_level(close=bar.close, resistance=next_res, sr=sr, params=params)
            if room == ROOM_LIMITED:
                blockers.append(BLOCKER_LIMITED_ROOM_TO_NEXT)
                _set_state(sess, STATE_ENTRY_BLOCKED, bar_index_in_session)
                candidates["WAIT"] = ACTION_WAIT
                return _out(
                    sess,
                    state_before,
                    candidates,
                    blockers,
                    thesis_effect,
                    levels,
                    room,
                    markers,
                    rule_ids,
                    params,
                    meaningful,
                    next_res,
                )

        # Reclaim-style daily arming (mirrors V0.2 intent)
        reclaim = levels.get("reclaim")
        support = levels.get("primary_support")
        if reclaim is not None:
            if bar.close >= reclaim:
                sess.reclaim_bars_above += 1
                if sess.reclaim_bars_above >= int(params.get("reclaim_confirm_bars") or 2):
                    sess.reclaim_stage = RECLAIM_CONFIRMED
                else:
                    sess.reclaim_stage = RECLAIM_CLOSED_ABOVE
            else:
                sess.reclaim_bars_above = 0
                sess.reclaim_stage = RECLAIM_BELOW
        if support is not None and bar.low <= support * 1.002 and bar.close >= support:
            sess.support_status = SUPPORT_TESTING if bar.close < support + sr * 0.05 else SUPPORT_HELD
        elif support is not None and bar.close > support:
            sess.support_status = SUPPORT_ABOVE

        support_ok = sess.support_status in (SUPPORT_HELD, SUPPORT_TESTING, SUPPORT_ABOVE)
        reclaim_ok = sess.reclaim_stage in (RECLAIM_CLOSED_ABOVE, RECLAIM_CONFIRMED) or reclaim is None
        room_ok = room in (ROOM_AMPLE, ROOM_ACCEPTABLE) or (
            sess.resistance_lifecycle == RL_BROKEN_CONFIRMED and room == ROOM_UNKNOWN_NO_NEXT
        )
        if (
            sim_ready
            and bool(meaningful)
            and support_ok
            and reclaim_ok
            and room_ok
            and not opening
        ):
            _arm_entry_on_bar(sess, bar_index_in_session, state_before)
            candidates["ENTRY_ARMED"] = ACTION_ENTRY_ARMED
            if may_emit_consider_entry_v03(
                state_before=state_before,
                bar_index_in_session=bar_index_in_session,
                sess=sess,
                bar=bar,
                meaningful=meaningful,
                params=params,
                opening=opening,
            ):
                if sess.reclaim_stage == RECLAIM_CONFIRMED or reclaim is None:
                    candidates["CONSIDER_ENTRY"] = ACTION_CONSIDER_ENTRY
                    markers.append("consider_entry")
        elif v_raw == "WAIT_PULLBACK":
            _set_state(sess, STATE_WAITING_FOR_PULLBACK, bar_index_in_session)
            candidates["WAIT_FOR_PULLBACK"] = ACTION_WAIT_FOR_PULLBACK
        elif v_raw == "WAIT_RECLAIM":
            _set_state(sess, STATE_WAITING_FOR_RECLAIM, bar_index_in_session)
            candidates["WAIT_FOR_RECLAIM"] = ACTION_WAIT_FOR_RECLAIM
        else:
            candidates["WAIT"] = ACTION_WAIT

        sess.prior_strong_bear = is_strong_bear_bar(bar, params)
        return _out(
            sess,
            state_before,
            candidates,
            blockers,
            thesis_effect,
            levels,
            room,
            markers,
            rule_ids,
            params,
            meaningful,
            next_res,
        )

    # OTHER verdicts
    _set_state(sess, STATE_OBSERVATION_ONLY, bar_index_in_session)
    blockers.append(BLOCKER_NO_DAILY_LONG_AUTHORIZATION)
    candidates["OBSERVE"] = ACTION_OBSERVE
    sess.prior_strong_bear = is_strong_bear_bar(bar, params)
    return _out(
        sess,
        state_before,
        candidates,
        blockers,
        thesis_effect,
        levels,
        room,
        markers,
        rule_ids,
        params,
        meaningful,
        next_res,
    )


def _result(
    sess: V03SessionState,
    state_before: str,
    candidates: dict[str, str],
    blockers: list[str],
    thesis_effect: str,
    levels: dict[str, Any],
    room: str,
    markers: list[str],
    rule_ids: list[str],
    params: dict[str, Any],
    meaningful: list,
    next_res: float | None,
    *,
    symbol: str | None = None,
    open_position_symbol: str | None = None,
) -> ContextResult:
    from .context_ruleset_v03 import ACTION_HOLD_POSITION

    selected, blocked_candidates = _select_action(candidates, params)
    if (
        open_position_symbol
        and symbol
        and open_position_symbol.upper() == symbol.upper()
        and selected == ACTION_CONSIDER_ENTRY
    ):
        selected = ACTION_HOLD_POSITION
        if "position_open" not in markers:
            markers = list(markers)
            markers.append("position_open")
    layers = {
        "objective_fact": {"layer": "OBJECTIVE_FACT"},
        "pattern_instance": {"layer": "PATTERN_INSTANCE", "patterns": meaningful},
        "contextual_interpretation": {
            "layer": "CONTEXTUAL_INTERPRETATION",
            "room_class": room,
            "resistance_lifecycle": sess.resistance_lifecycle,
            "path_class": sess.path_class,
            "next_resistance": next_res,
            "broken_resistance": sess.broken_resistance,
            "confirmation_score": sess.confirmation_score,
            "ruleset_version": RULESET_VERSION,
        },
        "advisory_state": {"layer": "ADVISORY_STATE", "state": sess.advisory_state},
        "advisory_action": {"layer": "ADVISORY_ACTION", "action": selected},
        "diagnostics": {
            "blockers": list(blockers),
            "dnc_active": sess.dnc_active,
            "entry_armed_bar": sess.entry_armed_bar,
            "setup_cycle_id": sess.current_setup_cycle_id,
            "setup_cycle_seq": sess.setup_cycle_seq,
            "gap_above_open": sess.gap_above_open,
            "last_entry_bar_index_in_session": LAST_ENTRY_BAR_INDEX_IN_SESSION,
        },
    }
    return ContextResult(
        layers=layers,
        thesis_effect=thesis_effect,
        state_before=state_before,
        state_after=sess.advisory_state,
        selected_action=selected,
        candidate_actions=list(candidates.values()),
        blocked_candidates=blocked_candidates,
        blockers=blockers,
        supporting_evidence=[],
        opposing_evidence=[],
        active_levels={
            "reclaim": levels.get("reclaim"),
            "do_not_chase": levels.get("do_not_chase"),
            "primary_support": levels.get("primary_support"),
            "primary_resistance": levels.get("primary_resistance"),
            "broken_resistance": sess.broken_resistance,
            "next_resistance": next_res,
        },
        daily_thesis_invalidation=levels.get("daily_thesis_invalidation"),
        intraday_setup_invalidation=sess.setup_invalidation_level,
        reclaim_stage=sess.reclaim_stage,
        support_status=sess.support_status,
        room_class=room,
        explanation=(
            f"V0.3 state={sess.advisory_state}; RL={sess.resistance_lifecycle}; "
            f"room={room}; action={selected}"
        ),
        context_classifications=[sess.resistance_lifecycle, room, sess.path_class],
        marker_flags=markers,
        rule_ids=rule_ids,
    )
