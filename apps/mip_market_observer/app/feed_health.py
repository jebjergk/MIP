"""Trade/quote staleness tiers — locked thresholds."""

from __future__ import annotations

from app.threshold_profile import STALE_DELAYED_MAX, STALE_LIVE_MAX


def tier_from_age_sec(age_sec: float | None) -> str | None:
    if age_sec is None:
        return None
    if age_sec <= STALE_LIVE_MAX:
        return "live"
    if age_sec <= STALE_DELAYED_MAX:
        return "delayed"
    return "stale"


def combine_worst(trade_tier: str | None, quote_tier: str | None, quotes_expected: bool) -> str:
    order = {"live": 0, "delayed": 1, "stale": 2}

    def rank(t: str | None) -> int:
        if t is None:
            return -1
        return order.get(t, 2)

    if not quotes_expected:
        return trade_tier or "stale"

    r_t = rank(trade_tier)
    r_q = rank(quote_tier)
    worst = max(r_t, r_q, 0)
    for name, v in order.items():
        if v == worst:
            return name
    return "stale"
