"""Adviser retrieval query construction — reduce thesis echo, regime-aware."""

from __future__ import annotations

import re
from typing import Any

from .adviser_regime_v01 import STRONG_BULL_REGIMES

_ECHO_PATTERNS = (
    re.compile(r"\bfailed\s+bull\s+breakout\b", re.I),
    re.compile(r"\bbull\s+trap\b", re.I),
    re.compile(r"\bbear\s+pause\b", re.I),
    re.compile(r"\bfailed\s+breakout\b", re.I),
    re.compile(r"\bposition\s+exited\b", re.I),
    re.compile(r"\binvalidated\b", re.I),
)

_CONTINUATION_QUERY = (
    "Brooks intraday long-only: bull trend continuation, normal pullback, "
    "breakout test, buy the pullback, trend from the open, always-in long context"
)

_FAILURE_QUERY = (
    "Brooks intraday long-only: failed bull breakout exit risk, failed bear reversal, "
    "breakout invalidation when follow-through fails"
)


def sanitize_thesis_for_retrieval(thesis: str | None, *, max_len: int = 280) -> str:
    if not thesis:
        return ""
    t = thesis
    for pat in _ECHO_PATTERNS:
        t = pat.sub("", t)
    t = re.sub(r"\s+", " ", t).strip()
    if len(t) > max_len:
        t = t[: max_len - 3] + "..."
    return t


def build_retrieval_query(
    *,
    wake_reason: str,
    bar_direction: str,
    bar_close: float,
    position_state: str,
    intraday_regime: str,
    wake_detail: str,
    setup_id: str | None = None,
    thesis_context: str | None = None,
) -> str:
    """Dominant terms from current bar/wake/regime — not prior failure labels."""
    parts = [
        "Brooks intraday long-only",
        f"wake={wake_reason}",
        f"regime={intraday_regime}",
        f"position={position_state}",
        f"bar {bar_direction} close {bar_close:.2f}",
    ]
    if setup_id:
        parts.append(f"setup={setup_id}")
    if wake_detail:
        parts.append(f"event: {wake_detail[:200]}")
    ctx = sanitize_thesis_for_retrieval(thesis_context)
    if ctx:
        parts.append(f"prior context (sanitized): {ctx}")
    return ". ".join(parts) + "."


def regime_continuation_supplement_query(intraday_regime: str) -> str | None:
    if intraday_regime in STRONG_BULL_REGIMES:
        return _CONTINUATION_QUERY
    return None


def regime_failure_companion_query(intraday_regime: str) -> str:
    if intraday_regime in STRONG_BULL_REGIMES:
        return _FAILURE_QUERY
    return _FAILURE_QUERY


def merge_balanced_hits(
    primary: list[dict[str, Any]],
    supplement: list[dict[str, Any]],
    *,
    limit: int,
) -> list[dict[str, Any]]:
    """Interleave primary and supplement by CARD_ID dedupe — both sides reachable."""
    seen: set[str] = set()
    out: list[dict[str, Any]] = []

    def _cid(h: dict[str, Any]) -> str:
        return str(h.get("CARD_ID") or h.get("card_id") or "")

    a, b = list(primary), list(supplement)
    ia = ib = 0
    while len(out) < limit and (ia < len(a) or ib < len(b)):
        if ia < len(a):
            h = a[ia]
            ia += 1
            c = _cid(h)
            if c and c not in seen:
                seen.add(c)
                out.append(h)
        if len(out) >= limit:
            break
        if ib < len(b):
            h = b[ib]
            ib += 1
            c = _cid(h)
            if c and c not in seen:
                seen.add(c)
                out.append(h)
    return out
