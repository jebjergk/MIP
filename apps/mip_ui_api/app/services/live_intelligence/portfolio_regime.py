"""Portfolio-level hidden factor hints from co-movement and simultaneous stress."""

from __future__ import annotations

from statistics import mean
from typing import Any


def detect_portfolio_regime(
    positions: list[dict[str, Any]],
    *,
    pairwise_correlation: dict[str, dict[str, float | None]] | None = None,
) -> dict[str, Any]:
    """
    Heuristic: if many positions show negative PnL move vs prior + high pairwise correlation,
    flag a latent risk-off / book shock hypothesis.
    """
    if not positions:
        return {
            "active": False,
            "hypothesis": "NONE",
            "confidence": 0.0,
            "affected_symbols": [],
            "scope": "NONE",
            "banner_text": "",
        }

    symbols = [str(p.get("symbol") or "").upper() for p in positions if p.get("symbol")]
    underwater = sum(1 for p in positions if (p.get("unrealized_pnl") or 0) < 0)
    high_corr_pairs = 0
    strong_corr = 0.0
    if pairwise_correlation:
        for a, row in pairwise_correlation.items():
            for b, v in (row or {}).items():
                if v is not None and v > 0.55:
                    high_corr_pairs += 1
                    strong_corr = max(strong_corr, v)

    frac_bad = underwater / max(len(positions), 1)
    active = frac_bad >= 0.5 and (high_corr_pairs >= 1 or len(positions) >= 3)
    confidence = min(0.95, 0.35 + 0.4 * frac_bad + 0.15 * min(high_corr_pairs / 3, 1.0))

    hypothesis = "RISK_OFF_BOOK_SHOCK" if active else "IDIOSYNCRATIC_MIX"
    scope = "PORTFOLIO_WIDE" if active and frac_bad >= 0.5 else "SYMBOL_SPECIFIC"

    banner = ""
    if active:
        banner = (
            f"Portfolio stress: {underwater}/{len(positions)} positions underwater; "
            f"elevated pairwise correlation (max ~{strong_corr:.2f}). "
            "Possible shared factor (risk-off, sector, USD, rates)."
        )

    return {
        "active": active,
        "hypothesis": hypothesis,
        "confidence": round(confidence, 3),
        "affected_symbols": symbols if active else [],
        "scope": scope,
        "banner_text": banner,
        "metrics": {
            "underwater_count": underwater,
            "position_count": len(positions),
            "high_correlation_pairs": high_corr_pairs,
            "max_correlation": strong_corr,
        },
    }


def merge_pairwise_from_bootstrap(portfolio_context: dict[str, Any] | None) -> dict[str, dict[str, float | None]] | None:
    if not portfolio_context:
        return None
    raw = portfolio_context.get("pairwise_return_correlation")
    return raw if isinstance(raw, dict) else None
