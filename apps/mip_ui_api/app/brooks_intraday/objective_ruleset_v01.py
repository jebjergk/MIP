"""Brooks objective observation ruleset V0.1 — experiment operationalizations, deterministic only."""

from __future__ import annotations

from dataclasses import dataclass
from statistics import median
from typing import Any

RULESET_VERSION = "BROOKS_OBJECTIVE_RULESET_V0_1"
PHASE3_LEGACY_ATTEMPT_ID = "p3:"  # suffix with run_id: f"{PHASE3_LEGACY_ATTEMPT_ID}{run_id}"


def legacy_attempt_id(run_id: str) -> str:
    return f"{PHASE3_LEGACY_ATTEMPT_ID}{run_id}"

# Central thresholds (recorded on each attempt / observation)
DEFAULT_PARAMETERS: dict[str, Any] = {
    "lookback_bars": 20,
    "doji_body_max_fraction": 0.20,
    "close_near_high_top_fraction": 0.25,
    "close_near_low_bottom_fraction": 0.25,
    "large_range_median_multiple": 1.5,
    "very_large_range_median_multiple": 2.0,
    "small_range_median_multiple": 0.75,
    "price_equality_tolerance": 0.01,
    "label": "experiment_operationalization_v0_1",
}


def price_equal(a: float, b: float, *, tol: float) -> bool:
    return abs(a - b) <= tol


def price_above(a: float, b: float, *, tol: float) -> bool:
    return a > b + tol


def price_below(a: float, b: float, *, tol: float) -> bool:
    return a < b - tol


@dataclass
class BarGeometry:
    open: float
    high: float
    low: float
    close: float
    volume: float | None
    total_range: float
    body_size: float
    body_fraction: float | None
    upper_tail: float
    lower_tail: float
    upper_tail_fraction: float | None
    lower_tail_fraction: float | None
    direction: str
    close_location: float | None
    open_location: float | None
    midpoint: float
    data_quality: str


def compute_geometry(
    *,
    open_: float,
    high: float,
    low: float,
    close: float,
    volume: float | None,
) -> BarGeometry:
    total_range = high - low
    body_size = abs(close - open_)
    if total_range <= 0:
        return BarGeometry(
            open=open_,
            high=high,
            low=low,
            close=close,
            volume=volume,
            total_range=0.0,
            body_size=body_size,
            body_fraction=None,
            upper_tail=0.0,
            lower_tail=0.0,
            upper_tail_fraction=None,
            lower_tail_fraction=None,
            direction="NEUTRAL",
            close_location=None,
            open_location=None,
            midpoint=(high + low) / 2.0,
            data_quality="INVALID_ZERO_RANGE",
        )
    upper_tail = high - max(open_, close)
    lower_tail = min(open_, close) - low
    body_fraction = body_size / total_range
    if close > open_ + DEFAULT_PARAMETERS["price_equality_tolerance"]:
        direction = "BULLISH"
    elif close < open_ - DEFAULT_PARAMETERS["price_equality_tolerance"]:
        direction = "BEARISH"
    else:
        direction = "NEUTRAL"
    close_location = (close - low) / total_range
    open_location = (open_ - low) / total_range
    return BarGeometry(
        open=open_,
        high=high,
        low=low,
        close=close,
        volume=volume,
        total_range=total_range,
        body_size=body_size,
        body_fraction=body_fraction,
        upper_tail=upper_tail,
        lower_tail=lower_tail,
        upper_tail_fraction=upper_tail / total_range,
        lower_tail_fraction=lower_tail / total_range,
        direction=direction,
        close_location=close_location,
        open_location=open_location,
        midpoint=(high + low) / 2.0,
        data_quality="COMPLETE",
    )


def relative_metrics(
    geometry: BarGeometry,
    prior_geometries: list[BarGeometry],
    params: dict[str, Any],
) -> dict[str, Any]:
    lookback = int(params.get("lookback_bars", 20))
    usable = [g for g in prior_geometries if g.data_quality == "COMPLETE"][-lookback:]
    out: dict[str, Any] = {
        "lookback_requested": lookback,
        "lookback_available": len(usable),
        "range_vs_median": None,
        "body_vs_median": None,
        "volume_vs_median": None,
        "relative_range_class": "NOT_ENOUGH_HISTORY",
        "relative_body_class": "NOT_ENOUGH_HISTORY",
        "relative_volume_class": "NOT_ENOUGH_HISTORY",
    }
    if len(usable) < 3:
        return out
    med_range = median([g.total_range for g in usable])
    med_body = median([g.body_size for g in usable])
    vols = [g.volume for g in usable if g.volume is not None]
    med_vol = median(vols) if len(vols) >= 3 else None

    if med_range > 0:
        ratio = geometry.total_range / med_range
        out["range_vs_median"] = ratio
        vlarge = float(params.get("very_large_range_median_multiple", 2.0))
        large = float(params.get("large_range_median_multiple", 1.5))
        small = float(params.get("small_range_median_multiple", 0.75))
        if ratio >= vlarge:
            out["relative_range_class"] = "VERY_LARGE"
        elif ratio >= large:
            out["relative_range_class"] = "LARGE"
        elif ratio <= small:
            out["relative_range_class"] = "SMALL"
        else:
            out["relative_range_class"] = "AVERAGE"

    if med_body > 0:
        out["body_vs_median"] = geometry.body_size / med_body
        out["relative_body_class"] = "COMPUTED"

    if med_vol and med_vol > 0 and geometry.volume is not None:
        out["volume_vs_median"] = geometry.volume / med_vol
        out["relative_volume_class"] = "COMPUTED"
    return out


def prior_bar_relationship(
    geometry: BarGeometry,
    prior: BarGeometry | None,
    *,
    params: dict[str, Any],
) -> dict[str, Any]:
    tol = float(params.get("price_equality_tolerance", 0.01))
    if prior is None or prior.data_quality != "COMPLETE" or geometry.data_quality != "COMPLETE":
        return {
            "has_prior_intraday_bar": False,
            "high_vs_prior": "NOT_ASSESSED",
            "low_vs_prior": "NOT_ASSESSED",
            "close_vs_prior_close": "NOT_ASSESSED",
            "inside_bar": "NOT_ASSESSED",
            "outside_bar": "NOT_ASSESSED",
            "overlap_fraction": None,
            "gap": None,
        }

    def cmp(a: float, b: float) -> str:
        if price_equal(a, b, tol=tol):
            return "EQUAL"
        if price_above(a, b, tol=tol):
            return "ABOVE"
        return "BELOW"

    high_vs = cmp(geometry.high, prior.high)
    low_vs = cmp(geometry.low, prior.low)
    close_vs = cmp(geometry.close, prior.close)
    inside = geometry.high <= prior.high + tol and geometry.low >= prior.low - tol
    outside = geometry.high >= prior.high - tol and geometry.low <= prior.low + tol
    overlap_low = max(geometry.low, prior.low)
    overlap_high = min(geometry.high, prior.high)
    overlap = max(0.0, overlap_high - overlap_low)
    union = max(geometry.high, prior.high) - min(geometry.low, prior.low)
    overlap_fraction = overlap / union if union > 0 else None
    gap = None
    if price_below(geometry.low, prior.high, tol=tol):
        gap = prior.high - geometry.low
    elif price_above(geometry.low, prior.high, tol=tol):
        gap = geometry.low - prior.high

    return {
        "has_prior_intraday_bar": True,
        "high_vs_prior": high_vs,
        "low_vs_prior": low_vs,
        "close_vs_prior_close": close_vs,
        "inside_bar": inside,
        "outside_bar": outside and not inside,
        "overlap_fraction": overlap_fraction,
        "gap": gap,
    }


def _rule(
    rule_id: str,
    term: str,
    inputs: list[str],
    parameters: dict[str, Any],
    result: bool,
    certainty: str = "CONFIRMED",
) -> dict[str, Any]:
    return {
        "rule_id": rule_id,
        "term": term,
        "inputs": inputs,
        "parameters": parameters,
        "result": result,
        "certainty": certainty if result else "NOT_ASSESSED",
    }


def evaluate_brooks_terms(
    *,
    geometry: BarGeometry,
    relative: dict[str, Any],
    relationship: dict[str, Any],
    session_state: dict[str, Any],
    params: dict[str, Any],
) -> tuple[list[str], list[dict[str, Any]], list[dict[str, Any]]]:
    """Returns (fact_codes, brooks_terms, rule_evaluations)."""
    facts: list[str] = []
    terms: list[dict[str, Any]] = []
    rules: list[dict[str, Any]] = []

    if geometry.data_quality != "COMPLETE":
        return facts, terms, rules

    doji_thresh = float(params.get("doji_body_max_fraction", 0.20))
    top_frac = float(params.get("close_near_high_top_fraction", 0.25))
    bot_frac = float(params.get("close_near_low_bottom_fraction", 0.25))

    is_bull = geometry.direction == "BULLISH"
    is_bear = geometry.direction == "BEARISH"
    is_doji = geometry.body_fraction is not None and geometry.body_fraction <= doji_thresh

    rules.append(
        _rule(
            "BROOKS_BAR_BULL_V0_1",
            "BULL_BAR",
            ["open", "close"],
            {},
            is_bull,
        )
    )
    rules.append(
        _rule(
            "BROOKS_BAR_BEAR_V0_1",
            "BEAR_BAR",
            ["open", "close"],
            {},
            is_bear,
        )
    )
    rules.append(
        _rule(
            "BROOKS_BAR_DOJI_V0_1",
            "DOJI",
            ["body_fraction"],
            {"max_fraction": doji_thresh},
            is_doji,
        )
    )

    if is_bull:
        terms.append({"term": "BULL_BAR", "certainty": "CONFIRMED"})
        facts.append("BULL_BAR")
    if is_bear:
        terms.append({"term": "BEAR_BAR", "certainty": "CONFIRMED"})
        facts.append("BEAR_BAR")
    if is_doji:
        terms.append({"term": "DOJI", "certainty": "CONFIRMED"})
        facts.append("DOJI")

    if geometry.close_location is not None and geometry.close_location >= (1.0 - top_frac):
        rules.append(
            _rule(
                "BROOKS_BAR_CLOSE_NEAR_HIGH_V0_1",
                "CLOSE_NEAR_HIGH",
                ["high", "low", "close"],
                {"top_fraction": top_frac},
                True,
            )
        )
        terms.append({"term": "CLOSE_NEAR_HIGH", "certainty": "CONFIRMED"})
        facts.append("CLOSE_NEAR_HIGH")
    if geometry.close_location is not None and geometry.close_location <= bot_frac:
        rules.append(
            _rule(
                "BROOKS_BAR_CLOSE_NEAR_LOW_V0_1",
                "CLOSE_NEAR_LOW",
                ["high", "low", "close"],
                {"bottom_fraction": bot_frac},
                True,
            )
        )
        terms.append({"term": "CLOSE_NEAR_LOW", "certainty": "CONFIRMED"})
        facts.append("CLOSE_NEAR_LOW")

    if geometry.upper_tail_fraction and geometry.upper_tail_fraction > 0.15:
        terms.append({"term": "UPPER_TAIL", "certainty": "CONFIRMED"})
        facts.append("UPPER_TAIL")
    if geometry.lower_tail_fraction and geometry.lower_tail_fraction > 0.15:
        terms.append({"term": "LOWER_TAIL", "certainty": "CONFIRMED"})
        facts.append("LOWER_TAIL")

    rr = relative.get("relative_range_class")
    if rr == "LARGE" or rr == "VERY_LARGE":
        if is_bull:
            terms.append({"term": "BIG_BULL_BAR", "certainty": "CONFIRMED"})
            facts.append("BIG_BULL_BAR")
        if is_bear:
            terms.append({"term": "BIG_BEAR_BAR", "certainty": "CONFIRMED"})
            facts.append("BIG_BEAR_BAR")
    if rr == "SMALL":
        terms.append({"term": "SMALL_BAR", "certainty": "CONFIRMED"})
        facts.append("SMALL_BAR")

    if relationship.get("has_prior_intraday_bar"):
        if relationship.get("high_vs_prior") == "ABOVE":
            facts.append("HIGH_ABOVE_PRIOR_HIGH")
            terms.append({"term": "HIGHER_HIGH", "certainty": "CONFIRMED"})
        elif relationship.get("high_vs_prior") == "BELOW":
            facts.append("HIGH_BELOW_PRIOR_HIGH")
            terms.append({"term": "LOWER_HIGH", "certainty": "CONFIRMED"})
        if relationship.get("low_vs_prior") == "ABOVE":
            facts.append("LOW_ABOVE_PRIOR_LOW")
            terms.append({"term": "HIGHER_LOW", "certainty": "CONFIRMED"})
        elif relationship.get("low_vs_prior") == "BELOW":
            facts.append("LOW_BELOW_PRIOR_LOW")
            terms.append({"term": "LOWER_LOW", "certainty": "CONFIRMED"})
        if relationship.get("inside_bar") is True:
            terms.append({"term": "INSIDE_BAR", "certainty": "CONFIRMED"})
            facts.append("INSIDE_BAR")
        if relationship.get("outside_bar") is True:
            terms.append({"term": "OUTSIDE_BAR", "certainty": "CONFIRMED"})
            facts.append("OUTSIDE_BAR")

        prior_high = session_state.get("_prior_high")
        prior_low = session_state.get("_prior_low")
        tol = float(params.get("price_equality_tolerance", 0.01))
        if prior_high is not None and prior_low is not None:
            if geometry.high > prior_high + tol:
                rules.append(
                    _rule(
                        "BROOKS_BREAK_ABOVE_PRIOR_BAR_V0_1",
                        "BREAK_ABOVE_PRIOR_BAR",
                        ["high", "prior_high"],
                        {},
                        True,
                    )
                )
                facts.append("BREAK_ABOVE_PRIOR_BAR")
            if geometry.low < prior_low - tol:
                rules.append(
                    _rule(
                        "BROOKS_BREAK_BELOW_PRIOR_BAR_V0_1",
                        "BREAK_BELOW_PRIOR_BAR",
                        ["low", "prior_low"],
                        {},
                        True,
                    )
                )
                facts.append("BREAK_BELOW_PRIOR_BAR")
            if geometry.close > prior_high + tol:
                facts.append("CLOSE_ABOVE_PRIOR_HIGH")
                terms.append({"term": "CLOSE_ABOVE_PRIOR_HIGH", "certainty": "CONFIRMED"})
            if geometry.close < prior_low - tol:
                facts.append("CLOSE_BELOW_PRIOR_LOW")
                terms.append({"term": "CLOSE_BELOW_PRIOR_LOW", "certainty": "CONFIRMED"})

        prev_flags = session_state.get("prior_bar_flags") or {}
        if prev_flags.get("possible_breakout") and is_bull and geometry.close_location and geometry.close_location >= (1 - top_frac):
            terms.append({"term": "POSSIBLE_FOLLOW_THROUGH_BAR", "certainty": "POSSIBLE"})
            facts.append("POSSIBLE_FOLLOW_THROUGH")
        elif prior_high is not None and geometry.high > prior_high + tol and not relationship.get("inside_bar"):
            terms.append({"term": "POSSIBLE_BREAKOUT_BAR", "certainty": "POSSIBLE"})
            facts.append("POSSIBLE_BREAKOUT")
        if prev_flags.get("possible_breakout") and relationship.get("inside_bar"):
            terms.append({"term": "FAILED_ONE_BAR_BREAKOUT", "certainty": "CONFIRMED"})
            facts.append("FAILED_ONE_BAR_BREAKOUT")

    # Consecutive bars
    if is_bull:
        session_state["consecutive_bull"] = int(session_state.get("consecutive_bull", 0)) + 1
        session_state["consecutive_bear"] = 0
        if session_state["consecutive_bull"] >= 2:
            terms.append({"term": "CONSECUTIVE_BULL_BARS", "certainty": "CONFIRMED"})
            facts.append("CONSECUTIVE_BULL_BARS")
    elif is_bear:
        session_state["consecutive_bear"] = int(session_state.get("consecutive_bear", 0)) + 1
        session_state["consecutive_bull"] = 0
        if session_state["consecutive_bear"] >= 2:
            terms.append({"term": "CONSECUTIVE_BEAR_BARS", "certainty": "CONFIRMED"})
            facts.append("CONSECUTIVE_BEAR_BARS")
    else:
        session_state["consecutive_bull"] = 0
        session_state["consecutive_bear"] = 0

    session_state["prior_bar_flags"] = {
        "possible_breakout": "POSSIBLE_BREAKOUT" in facts,
    }
    session_state["_prior_high"] = geometry.high
    session_state["_prior_low"] = geometry.low

    return facts, terms, rules


def build_explanation(
    geometry: BarGeometry,
    relative: dict[str, Any],
    relationship: dict[str, Any],
    facts: list[str],
) -> str:
    parts: list[str] = []
    if geometry.data_quality != "COMPLETE":
        return "Bar has zero or invalid range; geometry classifications are limited."
    if geometry.direction == "BULLISH":
        parts.append(
            f"Bull bar with a body covering {geometry.body_fraction * 100:.0f}% of the range."
        )
    elif geometry.direction == "BEARISH":
        parts.append(
            f"Bear bar with a body covering {geometry.body_fraction * 100:.0f}% of the range."
        )
    else:
        parts.append("Neutral close relative to the open.")
    if geometry.close_location is not None:
        pct_from_bottom = geometry.close_location * 100
        parts.append(f"The close is {pct_from_bottom:.0f}% up from the bar low (within the range).")
    if relationship.get("has_prior_intraday_bar"):
        hh = "HIGHER_HIGH" in str(facts) or any("HIGH_ABOVE" in f for f in facts)
        hl = any("LOW_ABOVE" in f for f in facts)
        if hh and hl:
            parts.append("The bar made both a higher high and a higher low than the previous bar.")
        elif "INSIDE_BAR" in facts:
            parts.append("This bar is entirely inside the prior bar's range.")
        elif "OUTSIDE_BAR" in facts:
            parts.append("This bar engulfs the prior bar's range (outside bar).")
    else:
        parts.append("First bar of the session — prior-bar comparisons are not assessed.")
    rr = relative.get("relative_range_class")
    if rr == "NOT_ENOUGH_HISTORY":
        parts.append("Not enough session history for relative size classification.")
    elif relative.get("range_vs_median") is not None:
        parts.append(
            f"Range is {relative['range_vs_median']:.1f}× the recent median ({rr.lower()} classification)."
        )
    return " ".join(parts)


def geometry_to_derived_metrics(
    geometry: BarGeometry,
    relative: dict[str, Any],
    relationship: dict[str, Any],
) -> dict[str, Any]:
    return {
        "total_range": geometry.total_range,
        "body_size": geometry.body_size,
        "body_fraction": geometry.body_fraction,
        "upper_tail": geometry.upper_tail,
        "lower_tail": geometry.lower_tail,
        "upper_tail_fraction": geometry.upper_tail_fraction,
        "lower_tail_fraction": geometry.lower_tail_fraction,
        "direction": geometry.direction,
        "close_location": geometry.close_location,
        "open_location": geometry.open_location,
        "midpoint": geometry.midpoint,
        "volume": geometry.volume,
        **relative,
        **{k: v for k, v in relationship.items() if not k.startswith("_")},
    }
