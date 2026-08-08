"""Daily PAA vs intraday confirmation policy for Adviser V0.1."""

from __future__ import annotations

import json
import re
from typing import Any

# Intraday-impossible confirmation patterns (frozen daily pack cannot change).
_FROZEN_DAILY_CONFIRM_RE = re.compile(
    r"|".join(
        [
            r"paa\s*(must|becomes?|shifts?|changes?)\b",
            r"paa\s+must\s+become\s+(clear_long|long_approve|wait_pullback)",
            r"\bclear_long\b",
            r"\blong_approve\b",
            r"wait_pullback\s+becomes?",
            r"daily\s+(paa|verdict|thesis)\s+.*(clear_long|long_approve|becomes?|changes?)",
            r"daily\s+verdict\s+changes?",
            r"daily\s+thesis\s+changes?",
            r"no_clear_long\s+(must|clears?|resolves?)",
        ]
    ),
    re.IGNORECASE,
)

_SHORT_ACTION_RE = re.compile(r"\b(SHORT|SELL_SHORT|ARM_SHORT|CONSIDER_SHORT)\b", re.IGNORECASE)

VALID_LONG_ACTIONS = frozenset(
    {
        "OBSERVE",
        "WAIT",
        "WATCH_LONG",
        "ARM_LONG",
        "CONSIDER_ENTRY",
        "HOLD",
        "EXIT",
        "INVALIDATE",
    }
)


def is_frozen_daily_confirmation(text: str) -> bool:
    return bool(_FROZEN_DAILY_CONFIRM_RE.search(text or ""))


def sanitize_confirmation_strings(items: list[Any]) -> tuple[list[str], list[str]]:
    """Return (kept intraday strings, removed frozen-daily strings)."""
    kept: list[str] = []
    removed: list[str] = []
    for item in items or []:
        s = str(item).strip()
        if not s:
            continue
        if is_frozen_daily_confirmation(s):
            removed.append(s)
        else:
            kept.append(s)
    return kept, removed


def sanitize_confirmation_predicates(items: list[Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    kept: list[dict[str, Any]] = []
    removed: list[dict[str, Any]] = []
    for raw in items or []:
        if not isinstance(raw, dict):
            continue
        if raw.get("type") == "DAILY_PAA_SHIFT":
            removed.append(raw)
            continue
        desc = str(raw.get("description") or raw.get("text") or "")
        blob = json.dumps(raw)
        if is_frozen_daily_confirmation(desc) or is_frozen_daily_confirmation(blob):
            removed.append(raw)
            continue
        kept.append(raw)
    return kept, removed


def daily_bias_note(paa_verdict: str | None) -> dict[str, Any]:
    v = (paa_verdict or "UNKNOWN").upper()
    cautious = v in ("NO_CLEAR_LONG", "NO_LONG", "BEAR_BIAS", "CAUTIOUS")
    return {
        "daily_paa_verdict": v,
        "daily_bias_role": "higher_timeframe_context_only",
        "daily_is_cautious_for_longs": cautious,
        "intraday_requirement_hint": (
            "Daily context is cautious; a long requires stronger intraday evidence "
            "(signal bar, follow-through, failed bear breakout, breakout pullback hold, acceptable room)."
            if cautious
            else "Daily context is not blocking; intraday structure and confirmation bars govern entry."
        ),
    }


def assert_no_short_action(action: str, reasoning: str = "") -> str:
    act = (action or "OBSERVE").upper()
    if act not in VALID_LONG_ACTIONS:
        return "OBSERVE"
    if _SHORT_ACTION_RE.search(reasoning or ""):
        return "OBSERVE"
    return act
