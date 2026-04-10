"""Deterministic trade side inference (Phase 1)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

Side = Literal["buy", "sell", "unknown"]
Conf = Literal["high", "medium", "low"]
Source = Literal["feed", "inferred", "tick_rule"]


@dataclass
class SideResult:
    side: Side
    confidence: Conf
    source: Source


def infer_trade_side(
    price: float,
    bid: float | None,
    ask: float | None,
    *,
    aggressor_from_feed: Side | None = None,
) -> SideResult:
    if aggressor_from_feed in ("buy", "sell"):
        return SideResult(side=aggressor_from_feed, confidence="high", source="feed")

    if bid is None or ask is None or ask < bid:
        return SideResult(side="unknown", confidence="low", source="inferred")

    spread = ask - bid
    tick = spread / 4 if spread > 0 else 1e-9

    if price >= ask - tick * 0.25:
        conf: Conf = "high" if spread <= tick * 2 else "medium"
        return SideResult(side="buy", confidence=conf, source="inferred")
    if price <= bid + tick * 0.25:
        conf = "high" if spread <= tick * 2 else "medium"
        return SideResult(side="sell", confidence=conf, source="inferred")

    return SideResult(side="unknown", confidence="low", source="tick_rule")
