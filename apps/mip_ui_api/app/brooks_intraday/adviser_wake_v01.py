"""Thesis-driven Adviser wake evaluation and structured watch predicates (V0.1)."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from typing import Any

from .adviser_invalidation_v01 import (
    InvalidationProcessStats,
    evaluate_invalidation_edge,
    format_invalidation_wake_detail,
    invalidation_logical_key,
    predicate_is_future_actionable,
    process_invalidation_predicates,
)
from .adviser_watch_v01 import (
    evaluate_watch_edge,
    should_reset_watch_fired_state,
    watch_logical_key,
)
from .objective_ruleset_v01 import BarGeometry, compute_geometry, price_below

SAFETY_QUIET_BARS = 10
LEVEL_TOL = 0.03

WAKE_SESSION_OPEN = "SESSION_OPEN"
WAKE_WATCH = "THESIS_WATCH_CONDITION_MET"
WAKE_INVALIDATED = "THESIS_INVALIDATED"
WAKE_POSITION = "POSITION_EVENT"
WAKE_SAFETY = "SAFETY_REFRESH"
WAKE_CONFIRMATION_COMPLETE = "CONFIRMATION_COMPLETE"


@dataclass
class WakeDecision:
    reason: str
    detail: str
    matched_predicate_ids: list[str] = field(default_factory=list)
    coalesced_reasons: list[str] = field(default_factory=list)


@dataclass
class ThesisWakeState:
    watch_predicates: list[dict[str, Any]] = field(default_factory=list)
    invalidation_predicates: list[dict[str, Any]] = field(default_factory=list)
    watch_text: list[str] = field(default_factory=list)
    fired_watch_ids: set[str] = field(default_factory=set)
    fired_watch_logical: set[str] = field(default_factory=set)
    watch_blocked_already_fired: int = 0
    fired_invalidation_ids: set[str] = field(default_factory=set)
    fired_invalidation_logical: set[str] = field(default_factory=set)
    invalidation_blocked_already_fired: int = 0
    bars_since_meaningful_wake: int = 999
    pending_position_event: str | None = None
    watch_issued_at_bar: int = 0

    def replace_predicates_from_adviser(
        self,
        *,
        watch_predicates: list[dict[str, Any]],
        invalidation_predicates: list[dict[str, Any]],
        watch_text: list[str],
        prior_action: str = "",
        new_action: str = "",
        thesis_invalidated: bool = False,
        reset_watch_fired: bool | None = None,
        watch_issued_at_bar: int | None = None,
    ) -> None:
        if reset_watch_fired is None:
            reset_watch_fired = should_reset_watch_fired_state(
                prior_action=prior_action,
                new_action=new_action,
                thesis_invalidated=thesis_invalidated,
            )
        if reset_watch_fired:
            self.fired_watch_ids.clear()
            self.fired_watch_logical.clear()
        self.watch_predicates = [_ensure_predicate_id(p) for p in watch_predicates]
        self.invalidation_predicates = [_ensure_predicate_id(p) for p in invalidation_predicates]
        self.watch_text = watch_text
        if watch_issued_at_bar is not None:
            self.watch_issued_at_bar = int(watch_issued_at_bar)
        self.fired_invalidation_ids.clear()
        self.fired_invalidation_logical.clear()


def ensure_predicate_id(p: dict[str, Any]) -> dict[str, Any]:
    return _ensure_predicate_id(p)


def _ensure_predicate_id(p: dict[str, Any]) -> dict[str, Any]:
    out = dict(p)
    if not out.get("id"):
        blob = json.dumps({k: out[k] for k in sorted(out) if k != "id"}, sort_keys=True)
        out["id"] = hashlib.sha256(blob.encode()).hexdigest()[:16]
    return out


def legacy_material_change(prev: dict | None, bar: Any, g: BarGeometry) -> str | None:
    """POC V0.1 geometry wake (used only for suppressed-call metrics)."""
    if prev is None:
        return "session_open"
    if g.direction != prev.get("direction"):
        return f"bar direction flipped to {g.direction}"
    if g.total_range and prev.get("range") and g.total_range >= 1.5 * prev["range"]:
        return "range expansion"
    if bar.close > prev.get("high", bar.close) + 0.05:
        return "broke recent swing high"
    if bar.close < prev.get("low", bar.close) - 0.05:
        return "broke recent swing low"
    return None


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
    tail = recent[-n:]
    for b in tail:
        g = compute_geometry(open_=b.open, high=b.high, low=b.low, close=b.close, volume=b.volume)
        if g.direction != "BULLISH":
            return False
        if (g.close_location or 0) < 0.45:
            return False
    return True


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


def evaluate_predicate(
    pred: dict[str, Any],
    *,
    bar: Any,
    g: BarGeometry,
    prev_geom: dict | None,
    recent: list[Any],
    prev_bar: Any | None = None,
    bar_index: int = 0,
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


def evaluate_wake(
    state: ThesisWakeState,
    *,
    bar_index: int,
    bar: Any,
    g: BarGeometry,
    prev_geom: dict | None,
    recent: list[Any],
    prev_bar: Any | None = None,
) -> WakeDecision | None:
    coalesced: list[str] = []

    if state.pending_position_event:
        detail = state.pending_position_event
        state.pending_position_event = None
        state.bars_since_meaningful_wake = 0
        return WakeDecision(reason=WAKE_POSITION, detail=detail, coalesced_reasons=[WAKE_POSITION])

    inv_hits: list[str] = []
    for raw in state.invalidation_predicates:
        pred = _ensure_predicate_id(raw)
        pid = str(pred["id"])
        lkey = invalidation_logical_key(pred)
        if lkey in state.fired_invalidation_logical or pid in state.fired_invalidation_ids:
            if not predicate_is_future_actionable(bar, pred):
                state.invalidation_blocked_already_fired += 1
            continue
        if evaluate_invalidation_edge(pred, bar=bar, prev_bar=prev_bar):
            inv_hits.append(pid)
    if inv_hits:
        for raw in state.invalidation_predicates:
            pred = _ensure_predicate_id(raw)
            if str(pred["id"]) in inv_hits:
                state.fired_invalidation_logical.add(invalidation_logical_key(pred))
        state.fired_invalidation_ids.update(inv_hits)
        state.bars_since_meaningful_wake = 0
        detail = format_invalidation_wake_detail(
            state.invalidation_predicates,
            inv_hits,
            bar=bar,
            prev_bar=prev_bar,
        )
        return WakeDecision(
            reason=WAKE_INVALIDATED,
            detail=detail,
            matched_predicate_ids=inv_hits,
            coalesced_reasons=[WAKE_INVALIDATED],
        )

    watch_hits: list[str] = []
    from .adviser_predicate_eval_v01 import infer_watch_validity_window

    for raw in state.watch_predicates:
        pred = _ensure_predicate_id(raw)
        vf, vu = infer_watch_validity_window(
            pred, watch_issued_at_bar=state.watch_issued_at_bar, safety_quiet_bars=SAFETY_QUIET_BARS
        )
        if bar_index < vf or bar_index > vu:
            continue
        lkey = watch_logical_key(pred)
        if lkey in state.fired_watch_logical:
            if prev_bar and evaluate_watch_edge(
                pred,
                bar=bar,
                bar_index=bar_index,
                prev_bar=prev_bar,
                prev_geom=prev_geom,
                recent=recent,
            ):
                state.watch_blocked_already_fired += 1
            continue
        if evaluate_watch_edge(
            pred,
            bar=bar,
            bar_index=bar_index,
            prev_bar=prev_bar,
            prev_geom=prev_geom,
            recent=recent,
        ):
            watch_hits.append(lkey)
    if watch_hits:
        state.fired_watch_logical.update(watch_hits)
        state.bars_since_meaningful_wake = 0
        return WakeDecision(
            reason=WAKE_WATCH,
            detail=f"watch predicates: {','.join(watch_hits)}",
            matched_predicate_ids=watch_hits,
            coalesced_reasons=[WAKE_WATCH],
        )

    if state.bars_since_meaningful_wake >= SAFETY_QUIET_BARS:
        state.bars_since_meaningful_wake = 0
        return WakeDecision(
            reason=WAKE_SAFETY,
            detail=f"quiet period {SAFETY_QUIET_BARS} bars",
            coalesced_reasons=[WAKE_SAFETY],
        )

    return None


def parse_predicates_from_llm(
    parsed: dict[str, Any],
    *,
    infer_invalidation: bool = True,
) -> tuple[list[dict], list[dict], list[str]]:
    watch_p = list(parsed.get("watch_predicates") or [])
    inv_p = list(parsed.get("invalidation_predicates") or [])
    watch_text = [str(x) for x in (parsed.get("watch_conditions") or []) if str(x).strip()]
    if not inv_p and infer_invalidation:
        inv_text = [str(x) for x in (parsed.get("invalidation_conditions") or []) if str(x).strip()]
        inv_p = infer_predicates_from_text(inv_text, invalidation=True)[:1]
    watch_p = watch_p[:4]
    inv_p = [p for p in inv_p if isinstance(p, dict)]
    return [_ensure_predicate_id(p) for p in watch_p if isinstance(p, dict)], inv_p, watch_text


_PRICE_RE = re.compile(r"\b(\d{3}\.\d{2})\b")


def infer_predicates_from_text(lines: list[str], *, invalidation: bool = False) -> list[dict[str, Any]]:
    """Best-effort struct from prose when LLM omits watch_predicates (strict cap)."""
    blob = " ".join(lines)
    blob_l = blob.lower()
    out: list[dict[str, Any]] = []

    m = re.search(r"(hold|stay|remains?)\s+above\s+(\d{3}\.\d{2})", blob_l)
    if m and not invalidation:
        out.append({"type": "LEVEL_BREAK", "level": float(m.group(2)), "direction": "ABOVE"})
    m = re.search(r"break\s+(above|below)\s+(\d{3}\.\d{2})", blob_l)
    if m:
        direction = "ABOVE" if m.group(1) == "above" else "BELOW"
        ptype = "LEVEL_BREAK" if direction == "ABOVE" else "SUPPORT_FAILURE"
        if invalidation or direction == "BELOW":
            out.append(
                {
                    "type": "BREAK_BELOW_LEVEL",
                    "level": float(m.group(2)),
                }
            )
        elif not invalidation:
            out.append({"type": "LEVEL_BREAK", "level": float(m.group(2)), "direction": direction})

    if not invalidation and ("follow-through" in blob_l or "follow through" in blob_l):
        out.append({"type": "BULL_FOLLOW_THROUGH", "minimum_bars": 1})

    return out[:3]
