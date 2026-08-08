"""Strict invalidation predicate validation and normalization (Adviser V0.1 POC 3+)."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any

from .objective_ruleset_v01 import price_above, price_below


def _ensure_predicate_id(p: dict[str, Any]) -> dict[str, Any]:
    out = dict(p)
    if not out.get("id"):
        blob = json.dumps({k: out[k] for k in sorted(out) if k != "id"}, sort_keys=True)
        out["id"] = hashlib.sha256(blob.encode()).hexdigest()[:16]
    return out

LEVEL_TOL = 0.03
MAX_INVALIDATION_PREDICATES = 2
LEVEL_CLUSTER_TOL = 0.75

LEVEL_INVALIDATION_TYPES = frozenset(
    {
        "SUPPORT_FAILURE",
        "BREAK_BELOW_LEVEL",
        "BREAK_ABOVE_LEVEL",
    }
)


@dataclass
class InvalidationProcessStats:
    generated: int = 0
    rejected_already_true: int = 0
    rejected_dedupe: int = 0
    rejected_cap: int = 0
    stored: int = 0

    def merge(self, other: "InvalidationProcessStats") -> None:
        self.generated += other.generated
        self.rejected_already_true += other.rejected_already_true
        self.rejected_dedupe += other.rejected_dedupe
        self.rejected_cap += other.rejected_cap
        self.stored += other.stored


def normalize_invalidation_type(pred: dict[str, Any]) -> dict[str, Any]:
    out = dict(pred)
    ptype = str(out.get("type", "")).upper()
    direction = str(out.get("direction", "")).upper()
    if ptype == "SUPPORT_FAILURE":
        out["type"] = "BREAK_BELOW_LEVEL"
    elif ptype == "LEVEL_BREAK":
        if direction == "BELOW" or direction == "DOWN":
            out["type"] = "BREAK_BELOW_LEVEL"
        elif direction == "ABOVE" or direction == "UP":
            out["type"] = "BREAK_ABOVE_LEVEL"
    return out


def invalidation_logical_key(pred: dict[str, Any]) -> str:
    ptype = str(pred.get("type", "")).upper()
    if "level" in pred:
        return f"{ptype}:{float(pred['level']):.2f}"
    return f"{ptype}:{pred.get('id', '')}"


def predicate_is_future_actionable(bar: Any, pred: dict[str, Any]) -> bool:
    """True if the predicate describes a transition that has not already occurred."""
    p = normalize_invalidation_type(pred)
    ptype = str(p.get("type", "")).upper()
    close = float(bar.close)
    if ptype in ("BREAK_BELOW_LEVEL", "SUPPORT_FAILURE"):
        level = float(p["level"])
        # Already below — not a future break from current state.
        return close >= level - LEVEL_TOL
    if ptype == "BREAK_ABOVE_LEVEL":
        level = float(p["level"])
        return close <= level + LEVEL_TOL
    if ptype == "BEAR_FOLLOW_THROUGH":
        return True
    return True


def _dedupe_level_predicates(preds: list[dict[str, Any]], *, below: bool) -> tuple[list[dict[str, Any]], int]:
    """Cluster nearby levels; keep one per cluster (tightest / most relevant)."""
    if not preds:
        return [], 0
    keyed = [(float(p["level"]), p) for p in preds if "level" in p]
    if not keyed:
        return preds[:MAX_INVALIDATION_PREDICATES], 0
    keyed.sort(key=lambda x: x[0], reverse=below)
    kept: list[dict[str, Any]] = []
    removed = 0
    for level, pred in keyed:
        if any(abs(level - float(k["level"])) <= LEVEL_CLUSTER_TOL for k in kept):
            removed += 1
            continue
        kept.append(pred)
    return kept, removed


def process_invalidation_predicates(
    raw: list[dict[str, Any]],
    bar: Any,
    *,
    max_count: int = MAX_INVALIDATION_PREDICATES,
) -> tuple[list[dict[str, Any]], InvalidationProcessStats]:
    stats = InvalidationProcessStats(generated=len(raw))
    actionable: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        norm = normalize_invalidation_type(item)
        if not predicate_is_future_actionable(bar, norm):
            stats.rejected_already_true += 1
            continue
        norm["predicate_is_future_actionable"] = True
        actionable.append(norm)

    below = [p for p in actionable if str(p.get("type", "")).upper() in ("BREAK_BELOW_LEVEL", "SUPPORT_FAILURE")]
    above = [p for p in actionable if str(p.get("type", "")).upper() == "BREAK_ABOVE_LEVEL"]
    other = [p for p in actionable if p not in below and p not in above]

    below_d, rem_b = _dedupe_level_predicates(below, below=True)
    above_d, rem_a = _dedupe_level_predicates(above, below=False)
    stats.rejected_dedupe += rem_b + rem_a

    merged = below_d + above_d + other
    if len(merged) > max_count:
        stats.rejected_cap += len(merged) - max_count
        merged = merged[:max_count]
    stats.stored = len(merged)
    merged = [_ensure_predicate_id(p) for p in merged]
    return merged, stats


def format_invalidation_wake_detail(
    invalidation_predicates: list[dict[str, Any]],
    matched_predicate_ids: list[str],
    *,
    bar: Any,
    prev_bar: Any | None,
) -> str:
    """Objective facts for THESIS_INVALIDATED wake — not a failed-breakout diagnosis."""
    if not matched_predicate_ids:
        return "Invalidation predicate edge triggered."
    facts: list[str] = []
    id_set = set(matched_predicate_ids)
    for raw in invalidation_predicates:
        pred = normalize_invalidation_type(_ensure_predicate_id(raw))
        if str(pred.get("id")) not in id_set:
            continue
        ptype = str(pred.get("type", "")).upper()
        level = pred.get("level")
        desc = str(pred.get("description") or "").strip()
        if level is not None and ptype in ("BREAK_BELOW_LEVEL", "SUPPORT_FAILURE"):
            prev_c = float(prev_bar.close) if prev_bar is not None else None
            fact = (
                f"Breakout reference {float(level):.2f} was closed below "
                f"(close {float(bar.close):.2f}"
            )
            if prev_c is not None:
                fact += f", prior close {prev_c:.2f}"
            fact += ")."
            facts.append(fact)
        elif level is not None and ptype == "BREAK_ABOVE_LEVEL":
            facts.append(
                f"Reference {float(level):.2f} was closed above (close {float(bar.close):.2f})."
            )
        elif ptype == "BEAR_FOLLOW_THROUGH_AFTER_SIGNAL":
            facts.append(
                f"Bear follow-through invalidation referenced (close {float(bar.close):.2f})."
            )
        elif desc:
            facts.append(desc.rstrip(".") + ".")
    if not facts:
        return f"Invalidation edge on predicate(s): {','.join(matched_predicate_ids)}."
    return " ".join(facts)


def evaluate_invalidation_edge(
    pred: dict[str, Any],
    *,
    bar: Any,
    prev_bar: Any | None,
) -> bool:
    """Edge-triggered invalidation only (one shot per logical condition)."""
    p = normalize_invalidation_type(pred)
    ptype = str(p.get("type", "")).upper()
    if prev_bar is None:
        return False
    level = float(p["level"]) if "level" in p else None
    if ptype in ("BREAK_BELOW_LEVEL", "SUPPORT_FAILURE") and level is not None:
        was_above = prev_bar.close >= level - LEVEL_TOL
        now_below = price_below(bar.close, level, tol=LEVEL_TOL)
        return was_above and now_below
    if ptype == "BREAK_ABOVE_LEVEL" and level is not None:
        was_below = prev_bar.close <= level + LEVEL_TOL
        now_above = price_above(bar.close, level, tol=LEVEL_TOL)
        return was_below and now_above
    return False
