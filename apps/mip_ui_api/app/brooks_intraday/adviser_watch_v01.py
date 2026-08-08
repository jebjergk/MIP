"""Strict watch predicate processing and edge evaluation (Adviser V0.1 POC 4 prep)."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any

from .adviser_invalidation_v01 import LEVEL_TOL, _ensure_predicate_id
from .objective_ruleset_v01 import compute_geometry, price_above, price_below

MAX_WATCH_PREDICATES = 3

BROOKS_WATCH_TYPES = frozenset(
    {
        "SECOND_ENTRY_CONFIRMED",
        "PULLBACK_LEG_COMPLETED",
        "FAILED_BREAKOUT_CONFIRMED",
        "SIGNAL_BAR_CONFIRMED",
        "BULL_FOLLOW_THROUGH_AFTER_SIGNAL",
        "BREAKOUT_PULLBACK_HOLD_CONFIRMED",
        "LEVEL_BREAK",
        "BULL_FOLLOW_THROUGH",
        "BAR_DIRECTION_FLIP",
        "RANGE_EXPANSION",
        "SWING_BREAK",
    }
)

GENERIC_STATE_TYPES = frozenset({"LEVEL_BREAK", "BULL_FOLLOW_THROUGH", "BAR_DIRECTION_FLIP"})


@dataclass
class WatchProcessStats:
    generated: int = 0
    rejected_already_true: int = 0
    rejected_cap: int = 0
    stored: int = 0


def normalize_watch_type(pred: dict[str, Any]) -> dict[str, Any]:
    out = dict(pred)
    ptype = str(out.get("type", "")).upper()
    if ptype == "BULL_FOLLOW_THROUGH" and out.get("after_signal"):
        out["type"] = "BULL_FOLLOW_THROUGH_AFTER_SIGNAL"
    return out


def watch_logical_key(pred: dict[str, Any]) -> str:
    p = normalize_watch_type(pred)
    ptype = str(p.get("type", "")).upper()
    direction = str(p.get("direction", "") or "").upper()
    if "level" in p:
        setup = str(p.get("setup_ref") or p.get("thesis_level") or "")[:48]
        return f"{ptype}:{float(p['level']):.2f}:{direction}:{setup}"
    setup = str(p.get("setup_ref") or p.get("description") or "")[:64]
    return f"{ptype}:{setup}"


def watch_predicate_is_future_actionable(bar: Any, pred: dict[str, Any]) -> bool:
    """Watch must describe a transition not already satisfied at current close."""
    p = normalize_watch_type(pred)
    ptype = str(p.get("type", "")).upper()
    close = float(bar.close)
    if ptype in ("LEVEL_BREAK", "BREAKOUT_PULLBACK_HOLD_CONFIRMED") and "level" in p:
        direction = str(p.get("direction", "ABOVE")).upper()
        level = float(p["level"])
        if direction in ("ABOVE", "UP"):
            return close <= level + LEVEL_TOL
        return close >= level - LEVEL_TOL
    if ptype in ("BREAK_BELOW_LEVEL", "SUPPORT_FAILURE") and "level" in p:
        return close >= float(p["level"]) - LEVEL_TOL
    if ptype in ("BREAK_ABOVE_LEVEL",) and "level" in p:
        return close <= float(p["level"]) + LEVEL_TOL
    return True


def _watch_state_true(
    pred: dict[str, Any],
    *,
    bar: Any,
    bar_index: int,
    g,
    prev_geom: dict | None,
    recent: list[Any],
    prev_bar: Any | None,
) -> bool:
    from .adviser_predicate_eval_v01 import evaluate_brooks_predicate_state

    return evaluate_brooks_predicate_state(
        pred,
        bar=bar,
        bar_index=bar_index,
        recent=recent,
        prev_bar=prev_bar,
        prev_geom=prev_geom,
        g=g,
    )


def evaluate_watch_edge(
    pred: dict[str, Any],
    *,
    bar: Any,
    bar_index: int,
    prev_bar: Any | None,
    prev_geom: dict | None,
    recent: list[Any],
) -> bool:
    if prev_bar is None:
        return False
    p = normalize_watch_type(pred)
    ptype = str(p.get("type", "")).upper()
    g = compute_geometry(open_=bar.open, high=bar.high, low=bar.low, close=bar.close, volume=bar.volume)
    gp = compute_geometry(
        open_=prev_bar.open, high=prev_bar.high, low=prev_bar.low, close=prev_bar.close, volume=prev_bar.volume
    )
    prev_geom_prev = {
        "direction": gp.direction,
        "range": gp.total_range,
        "high": prev_bar.high,
        "low": prev_bar.low,
    }

    if ptype in ("LEVEL_BREAK", "BREAKOUT_PULLBACK_HOLD_CONFIRMED", "BREAK_ABOVE_LEVEL") and "level" in p:
        level = float(p["level"])
        direction = str(p.get("direction", "ABOVE")).upper()
        if direction in ("ABOVE", "UP"):
            was_below = prev_bar.close <= level + LEVEL_TOL
            now_above = price_above(bar.close, level, tol=LEVEL_TOL)
            return was_below and now_above
        was_above = prev_bar.close >= level - LEVEL_TOL
        now_below = price_below(bar.close, level, tol=LEVEL_TOL)
        return was_above and now_below

    if ptype in ("BREAK_BELOW_LEVEL", "SUPPORT_FAILURE") and "level" in p:
        level = float(p["level"])
        was_above = prev_bar.close >= level - LEVEL_TOL
        now_below = price_below(bar.close, level, tol=LEVEL_TOL)
        return was_above and now_below

    now = _watch_state_true(
        p, bar=bar, bar_index=bar_index, g=g, prev_geom=prev_geom, recent=recent, prev_bar=prev_bar
    )
    was = _watch_state_true(
        p,
        bar=prev_bar,
        bar_index=bar_index - 1,
        g=gp,
        prev_geom=prev_geom_prev,
        recent=recent[:-1] if len(recent) > 1 else [prev_bar],
        prev_bar=None,
    )
    return now and not was


def normalize_operational_watch_conditions(
    watch_conditions: list[str],
    watch_predicates: list[dict[str, Any]],
) -> tuple[list[str], list[str]]:
    """Pair operational watch prose 1:1 with stored predicates; excess prose → context notes."""
    preds = list(watch_predicates or [])
    lines = [str(x).strip() for x in (watch_conditions or []) if str(x).strip()]
    operational: list[str] = []
    for i, pred in enumerate(preds):
        if i < len(lines):
            operational.append(lines[i])
        else:
            operational.append(str(pred.get("description") or pred.get("type") or "watch predicate"))
    context_notes = lines[len(preds) :] if len(lines) > len(preds) else []
    return operational, context_notes


def process_watch_predicates(
    raw: list[dict[str, Any]],
    bar: Any,
    *,
    max_count: int = MAX_WATCH_PREDICATES,
) -> tuple[list[dict[str, Any]], WatchProcessStats]:
    stats = WatchProcessStats(generated=len(raw))
    kept: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        norm = normalize_watch_type(item)
        ptype = str(norm.get("type", "")).upper()
        if ptype and ptype not in BROOKS_WATCH_TYPES:
            continue
        if not watch_predicate_is_future_actionable(bar, norm):
            stats.rejected_already_true += 1
            continue
        norm["predicate_is_future_actionable"] = True
        kept.append(norm)
    if len(kept) > max_count:
        stats.rejected_cap = len(kept) - max_count
        kept = kept[:max_count]
    stats.stored = len(kept)
    return [_ensure_predicate_id(p) for p in kept], stats


def action_phase(action: str) -> str:
    a = (action or "OBSERVE").upper()
    if a in ("OBSERVE", "WAIT"):
        return "OBSERVE"
    if a == "WATCH_LONG":
        return "WATCH"
    if a == "ARM_LONG":
        return "ARM"
    if a == "CONSIDER_ENTRY":
        return "ENTRY_READY"
    if a == "HOLD":
        return "IN_TRADE"
    if a in ("EXIT", "INVALIDATE"):
        return "FLAT_RESET"
    return a


def should_reset_watch_fired_state(*, prior_action: str, new_action: str, thesis_invalidated: bool) -> bool:
    if thesis_invalidated or new_action.upper() == "INVALIDATE":
        return True
    return action_phase(prior_action) != action_phase(new_action)
