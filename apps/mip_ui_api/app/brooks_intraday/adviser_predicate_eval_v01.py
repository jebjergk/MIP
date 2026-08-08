"""Canonical Brooks predicate truth evaluation (shared by confirmation + thesis wake).

Wake contract patch 1: watch and confirmation use identical predicate *state* semantics.
Confirmation applies validity via tick_mandatory_confirmations; watch applies via infer_watch_validity_window.
"""

from __future__ import annotations

from typing import Any

from .adviser_invalidation_v01 import LEVEL_TOL
from .objective_ruleset_v01 import BarGeometry, compute_geometry, price_above, price_below


def _level_break(bar: Any, g: BarGeometry, pred: dict[str, Any]) -> bool:
    level = float(pred["level"])
    direction = str(pred.get("direction", "ABOVE")).upper()
    use_close = pred.get("use") != "HIGH_LOW"
    if direction == "ABOVE":
        val = bar.close if use_close else bar.high
        return val >= level - LEVEL_TOL
    val = bar.close if use_close else bar.low
    return val <= level + LEVEL_TOL


def _support_failure(bar: Any, pred: dict[str, Any], prev_bar: Any | None = None) -> bool:
    level = float(pred["level"])
    now_below = price_below(bar.close, level, tol=LEVEL_TOL)
    if prev_bar is None:
        return now_below
    was_above = prev_bar.close >= level - LEVEL_TOL
    return was_above and now_below


def _bull_follow_through(recent: list[Any], pred: dict[str, Any]) -> bool:
    n = int(pred.get("minimum_bars", 1))
    if len(recent) < n:
        return False
    for b in recent[-n:]:
        gb = compute_geometry(open_=b.open, high=b.high, low=b.low, close=b.close, volume=b.volume)
        if gb.direction != "BULLISH":
            return False
        if (gb.close_location or 0) < 0.45:
            return False
    return True


def _bar_direction_flip(g: BarGeometry, prev: dict | None, pred: dict[str, Any]) -> bool:
    if not prev:
        return False
    want = str(pred.get("direction", "")).upper()
    if prev.get("direction") == g.direction:
        return False
    if want and want not in g.direction:
        return False
    return True


def _range_expansion(g: BarGeometry, prev: dict | None, pred: dict[str, Any]) -> bool:
    if not prev or not prev.get("range"):
        return False
    mult = float(pred.get("multiplier", 1.5))
    return bool(g.total_range and g.total_range >= mult * prev["range"])


def _swing_break(bar: Any, prev: dict | None, pred: dict[str, Any]) -> bool:
    if not prev:
        return False
    direction = str(pred.get("direction", "ABOVE")).upper()
    margin = float(pred.get("margin", 0.05))
    if direction == "ABOVE":
        return bar.close > float(prev.get("high", bar.close)) + margin
    return bar.close < float(prev.get("low", bar.close)) - margin


BROOKS_PREDICATE_STATE_TYPES = frozenset(
    {
        "LEVEL_BREAK",
        "CLOSE_ABOVE",
        "BREAK_ABOVE_LEVEL",
        "BULL_FOLLOW_THROUGH_AFTER_SIGNAL",
        "BULL_FOLLOW_THROUGH",
        "BREAKOUT_PULLBACK_HOLD_CONFIRMED",
        "FAILED_BEAR_BREAKOUT_CONFIRMED",
        "FAILED_BREAKOUT_CONFIRMED",
        "SIGNAL_BAR_CONFIRMED",
        "PULLBACK_LEG_COMPLETED",
        "MIN_BODY_FRACTION",
        "BULL_BAR",
        "SUPPORT_FAILURE",
        "BAR_DIRECTION_FLIP",
        "RANGE_EXPANSION",
        "SWING_BREAK",
        "SECOND_ENTRY_CONFIRMED",
        "FAILED_BREAKOUT_CONFIRMED",
    }
)


def _pullback_legs_completed(recent: list[Any], pred: dict[str, Any]) -> bool:
    minimum = int(pred.get("minimum_legs", 2))
    if len(recent) < minimum + 1:
        return False
    legs = 0
    prev_dir = None
    for b in recent[-(minimum + 3) :]:
        g = compute_geometry(open_=b.open, high=b.high, low=b.low, close=b.close, volume=b.volume)
        if prev_dir and g.direction != prev_dir and g.direction in ("BULLISH", "BEARISH"):
            legs += 1
        if g.direction in ("BULLISH", "BEARISH"):
            prev_dir = g.direction
    return legs >= minimum


def _failed_bear_breakout_confirmed(bar: Any, prev_bar: Any | None, pred: dict[str, Any]) -> bool:
    if prev_bar is None:
        return False
    level = float(pred.get("level", pred.get("support_level", 0)))
    low_probe = bar.low < level - LEVEL_TOL
    now_above = price_above(bar.close, level, tol=LEVEL_TOL)
    g = compute_geometry(open_=bar.open, high=bar.high, low=bar.low, close=bar.close, volume=bar.volume)
    return low_probe and now_above and g.direction == "BULLISH"


def evaluate_brooks_predicate_state(
    pred: dict[str, Any],
    *,
    bar: Any,
    bar_index: int,
    recent: list[Any],
    prev_bar: Any | None,
    prev_geom: dict | None,
    g: BarGeometry | None = None,
) -> bool:
    """Whether predicate state holds on this bar (not edge-triggered)."""
    if g is None:
        g = compute_geometry(open_=bar.open, high=bar.high, low=bar.low, close=bar.close, volume=bar.volume)
    ptype = str(pred.get("type", "")).upper()
    min_body = float(pred.get("minimum_body_fraction", pred.get("min_body_fraction", 0.5)))
    min_close_loc = float(pred.get("minimum_close_location", pred.get("min_close_location", 0.5)))

    if ptype in ("LEVEL_BREAK", "CLOSE_ABOVE", "BREAK_ABOVE_LEVEL") and "level" in pred:
        if ptype == "LEVEL_BREAK":
            return _level_break(bar, g, pred)
        if not price_above(bar.close, float(pred["level"]), tol=LEVEL_TOL):
            return False
        if pred.get("require_bullish"):
            return g.direction == "BULLISH" and (g.close_location or 0) >= min_close_loc
        return True

    if ptype == "BULL_FOLLOW_THROUGH_AFTER_SIGNAL":
        if "level" in pred and not price_above(bar.close, float(pred["level"]), tol=LEVEL_TOL):
            return False
        return (
            g.direction == "BULLISH"
            and (g.body_fraction or 0) >= min_body
            and (g.close_location or 0) >= min_close_loc
        )

    if ptype == "BULL_FOLLOW_THROUGH":
        return _bull_follow_through(recent, pred)

    if ptype == "BREAKOUT_PULLBACK_HOLD_CONFIRMED":
        if "level" in pred:
            hold = price_above(bar.low, float(pred["level"]), tol=LEVEL_TOL) or price_above(
                bar.close, float(pred["level"]), tol=LEVEL_TOL
            )
            if not hold:
                return False
        return g.direction == "BULLISH" and (g.close_location or 0) >= min_close_loc

    if ptype in ("FAILED_BEAR_BREAKOUT_CONFIRMED", "FAILED_BREAKOUT_CONFIRMED"):
        return _failed_bear_breakout_confirmed(bar, prev_bar, pred)

    if ptype == "SIGNAL_BAR_CONFIRMED":
        need_body = float(pred.get("minimum_body_fraction", 0.55))
        return (g.body_fraction or 0) >= need_body and (g.close_location or 0) >= 0.6

    if ptype == "PULLBACK_LEG_COMPLETED":
        return _pullback_legs_completed(recent, pred)

    if ptype == "MIN_BODY_FRACTION":
        return (g.body_fraction or 0) >= float(pred.get("minimum", 0.7))

    if ptype == "BULL_BAR":
        return g.direction == "BULLISH" and (g.close_location or 0) >= 0.5

    if ptype == "SUPPORT_FAILURE":
        return _support_failure(bar, pred, prev_bar)

    if ptype == "BAR_DIRECTION_FLIP":
        return _bar_direction_flip(g, prev_geom, pred)

    if ptype == "RANGE_EXPANSION":
        return _range_expansion(g, prev_geom, pred)

    if ptype == "SWING_BREAK":
        return _swing_break(bar, prev_geom, pred)

    return False


def infer_watch_validity_window(pred: dict[str, Any], *, watch_issued_at_bar: int, safety_quiet_bars: int) -> tuple[int, int]:
    """Watch-only validity: honor valid_from; extend valid_until through quiet period from issuance.

    Confirmation ticks use infer_validity_window() unchanged (narrow LLM offsets preserved).
    """
    from .adviser_setup_contract_v01 import infer_validity_window

    if pred.get("valid_from_bar") is not None and pred.get("valid_until_bar") is not None:
        return int(pred["valid_from_bar"]), int(pred["valid_until_bar"])

    vf, vu = infer_validity_window(pred, signal_bar_index=watch_issued_at_bar)
    quiet_cap = watch_issued_at_bar + int(safety_quiet_bars) - 1
    return vf, max(vu, quiet_cap)
