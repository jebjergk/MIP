from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from statistics import mean
from typing import Any, Iterable

from .models import Annotation


@dataclass(frozen=True)
class Bar:
    day: date
    open: float
    high: float
    low: float
    close: float
    volume: float | None = None

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "Bar":
        raw_day = row["TS"]
        if isinstance(raw_day, datetime):
            raw_day = raw_day.date()
        return cls(
            day=raw_day,
            open=float(row["OPEN"]),
            high=float(row["HIGH"]),
            low=float(row["LOW"]),
            close=float(row["CLOSE"]),
            volume=float(row["VOLUME"]) if row.get("VOLUME") is not None else None,
        )


def _round(value: float | None, digits: int = 4) -> float | None:
    return round(value, digits) if value is not None else None


def _ema(values: list[float], period: int) -> list[float]:
    if not values:
        return []
    alpha = 2.0 / (period + 1)
    out = [values[0]]
    for value in values[1:]:
        out.append(alpha * value + (1 - alpha) * out[-1])
    return out


def _atr(bars: list[Bar], period: int = 14) -> float:
    ranges: list[float] = []
    for i, bar in enumerate(bars):
        previous = bars[i - 1].close if i else bar.close
        ranges.append(max(bar.high - bar.low, abs(bar.high - previous), abs(bar.low - previous)))
    return mean(ranges[-period:]) if ranges else 0.0


def _swings(bars: list[Bar], left: int = 2, right: int = 2) -> list[dict[str, Any]]:
    points: list[dict[str, Any]] = []
    for i in range(left, len(bars) - right):
        bar = bars[i]
        neighbours = bars[i - left:i] + bars[i + 1:i + right + 1]
        if bar.high > max(item.high for item in neighbours):
            points.append({"kind": "high", "index": i, "date": bar.day.isoformat(), "price": bar.high})
        if bar.low < min(item.low for item in neighbours):
            points.append({"kind": "low", "index": i, "date": bar.day.isoformat(), "price": bar.low})
    return points


def _clusters(points: Iterable[dict[str, Any]], tolerance: float) -> list[dict[str, Any]]:
    groups: list[list[dict[str, Any]]] = []
    for point in sorted(points, key=lambda item: item["price"]):
        if groups and abs(point["price"] - mean(p["price"] for p in groups[-1])) <= tolerance:
            groups[-1].append(point)
        else:
            groups.append([point])
    result = []
    for group in groups:
        prices = [p["price"] for p in group]
        result.append({
            "price_low": _round(min(prices)),
            "price_high": _round(max(prices)),
            "price": _round(mean(prices)),
            "touches": len(group),
            "first_date": group[0]["date"],
            "last_date": group[-1]["date"],
        })
    return sorted(result, key=lambda item: (item["touches"], item["last_date"]), reverse=True)


def _regression(points: list[dict[str, Any]]) -> tuple[float, float]:
    if len(points) < 2:
        return 0.0, 0.0
    xs = [float(p["index"]) for p in points]
    ys = [float(p["price"]) for p in points]
    x_bar, y_bar = mean(xs), mean(ys)
    denominator = sum((x - x_bar) ** 2 for x in xs)
    slope = sum((x - x_bar) * (y - y_bar) for x, y in zip(xs, ys)) / denominator if denominator else 0.0
    fitted = [y_bar + slope * (x - x_bar) for x in xs]
    total = sum((y - y_bar) ** 2 for y in ys)
    r2 = 1.0 - sum((y - fit) ** 2 for y, fit in zip(ys, fitted)) / total if total else 1.0
    return slope, max(0.0, min(1.0, r2))


def _structure(swings: list[dict[str, Any]]) -> dict[str, Any]:
    highs = [p for p in swings if p["kind"] == "high"]
    lows = [p for p in swings if p["kind"] == "low"]
    high_state = "INSUFFICIENT"
    low_state = "INSUFFICIENT"
    if len(highs) >= 2:
        high_state = "HIGHER_HIGH" if highs[-1]["price"] > highs[-2]["price"] else "LOWER_HIGH"
    if len(lows) >= 2:
        low_state = "HIGHER_LOW" if lows[-1]["price"] > lows[-2]["price"] else "LOWER_LOW"
    trend = "MIXED"
    if high_state == "HIGHER_HIGH" and low_state == "HIGHER_LOW":
        trend = "UPTREND"
    elif high_state == "LOWER_HIGH" and low_state == "LOWER_LOW":
        trend = "DOWNTREND"
    return {"trend": trend, "highs": high_state, "lows": low_state}


def _patterns(swings: list[dict[str, Any]], atr: float) -> dict[str, Any]:
    highs = [p for p in swings if p["kind"] == "high"]
    lows = [p for p in swings if p["kind"] == "low"]
    doubles: list[dict[str, Any]] = []
    tolerance = max(atr * 0.5, 1e-9)
    for name, points in (("DOUBLE_TOP_CANDIDATE", highs), ("DOUBLE_BOTTOM_CANDIDATE", lows)):
        if len(points) >= 2:
            left, right = points[-2], points[-1]
            if right["index"] - left["index"] >= 5 and abs(right["price"] - left["price"]) <= tolerance:
                doubles.append({
                    "pattern": name,
                    "first_date": left["date"],
                    "second_date": right["date"],
                    "level": _round(mean([left["price"], right["price"]])),
                    "confirmed": False,
                })
    wedge = None
    recent_highs, recent_lows = highs[-4:], lows[-4:]
    if len(recent_highs) >= 3 and len(recent_lows) >= 3:
        high_slope, high_r2 = _regression(recent_highs)
        low_slope, low_r2 = _regression(recent_lows)
        start_width = recent_highs[0]["price"] - recent_lows[0]["price"]
        end_width = recent_highs[-1]["price"] - recent_lows[-1]["price"]
        converging = start_width > 0 and 0 < end_width < start_width * 0.75
        if converging and high_r2 >= 0.65 and low_r2 >= 0.65 and high_slope < low_slope:
            wedge = {
                "pattern": "WEDGE_CANDIDATE",
                "high_slope": _round(high_slope),
                "low_slope": _round(low_slope),
                "high_r2": _round(high_r2),
                "low_r2": _round(low_r2),
                "confirmed": False,
            }
    return {"wedge_candidate": wedge, "double_candidates": doubles}


def analyse_geometry(
    bars: list[Bar],
    pivot_left: int = 2,
    pivot_right: int = 2,
) -> tuple[dict[str, Any], dict[str, Any], list[Annotation]]:
    if len(bars) < 30:
        raise ValueError("At least 30 daily bars are required")
    closes = [bar.close for bar in bars]
    latest = bars[-1]
    atr = _atr(bars)
    ema20_series = _ema(closes, 20)
    ema20 = ema20_series[-1]
    swings = _swings(bars, pivot_left, pivot_right)
    structure = _structure(swings)
    tolerance = max(atr * 0.5, latest.close * 0.003)
    supports = [
        level for level in _clusters((p for p in swings if p["kind"] == "low"), tolerance)
        if level["price"] <= latest.close + tolerance
    ][:5]
    resistances = [
        level for level in _clusters((p for p in swings if p["kind"] == "high"), tolerance)
        if level["price"] >= latest.close - tolerance
    ][:5]
    prior_highs = [p for p in swings if p["kind"] == "high" and p["index"] < len(bars) - 2]
    breakout = None
    failure = None
    if prior_highs:
        level = prior_highs[-1]["price"]
        breakout_indices = [
            i for i in range(max(prior_highs[-1]["index"] + 1, len(bars) - 10), len(bars))
            if bars[i].close > level + atr * 0.1
        ]
        if breakout_indices:
            index = breakout_indices[0]
            breakout = {"direction": "UP", "level": _round(level), "date": bars[index].day.isoformat()}
            failed = next((i for i in range(index + 1, len(bars)) if bars[i].close < level - atr * 0.1), None)
            if failed is not None:
                failure = {"type": "FAILED_UP_BREAKOUT", "level": _round(level), "date": bars[failed].day.isoformat()}
    period = min(60, len(bars))
    range_low = min(bar.low for bar in bars[-period:])
    range_high = max(bar.high for bar in bars[-period:])
    range_width_atr = (range_high - range_low) / atr if atr else None
    range_state = bool(range_width_atr is not None and range_width_atr <= 8 and structure["trend"] == "MIXED")
    high20 = max(bar.high for bar in bars[-20:])
    pullback_pct = (high20 - latest.close) / high20 if high20 else 0.0
    nearest_support = min(supports, key=lambda item: abs(item["price"] - latest.close), default=None)
    near_support = bool(nearest_support and abs(latest.close - nearest_support["price"]) <= atr)
    near_ema = abs(latest.close - ema20) <= atr * 0.75
    if latest.close < ema20 and not near_support:
        location = "UNFAVOURABLE"
    elif pullback_pct <= 0.02 and latest.close > ema20:
        location = "EXTENDED_NEAR_HIGHS"
    elif near_support or near_ema:
        location = "CONSTRUCTIVE_PULLBACK"
    else:
        location = "NEUTRAL"
    rendered_swings = []
    prior_by_kind: dict[str, float] = {}
    for point in swings[-20:]:
        previous = prior_by_kind.get(point["kind"])
        if point["kind"] == "high":
            label = "HH" if previous is not None and point["price"] > previous else "LH"
            annotation_type = "SWING_HIGH"
        else:
            label = "HL" if previous is not None and point["price"] > previous else "LL"
            annotation_type = "SWING_LOW"
        if previous is None:
            label = "SWING HIGH" if point["kind"] == "high" else "SWING LOW"
        prior_by_kind[point["kind"]] = point["price"]
        rendered_swings.append({
            **point,
            "price": _round(point["price"]),
            "bar_index": point["index"],
            "type": annotation_type,
            "label": label,
            "strength": "MEDIUM",
            "source": "detected_geometry",
        })
    zones = [
        {
            "type": "SUPPORT" if name == "support" else "RESISTANCE",
            "lower": level["price_low"],
            "upper": level["price_high"],
            "low": level["price_low"],
            "high": level["price_high"],
            "start_date": level["first_date"],
            "end_date": latest.day.isoformat(),
            "label": f"{name.title()} cluster ({level['touches']} touches)",
            "source": "detected_geometry",
            "detected_from": f"recent_swing_{'low' if name == 'support' else 'high'}_cluster",
            "touch_count": level["touches"],
            "recency_rank": index + 1,
            "distance_pct_from_close": _round(
                (level["price"] - latest.close) / latest.close * 100, 2
            ),
        }
        for name, levels in (("support", supports), ("resistance", resistances))
        for index, level in enumerate(levels)
    ]
    range_boxes = [{
        "type": "RANGE",
        "low": _round(range_low),
        "high": _round(range_high),
        "start_date": bars[-period].day.isoformat(),
        "end_date": latest.day.isoformat(),
        "label": "Trading range",
        "source": "detected_geometry",
    }] if range_state else []
    geometry = {
        "latest": {"date": latest.day.isoformat(), "close": _round(latest.close), "atr14": _round(atr)},
        "ema20": {
            "value": _round(ema20),
            "price_above": latest.close >= ema20,
            "slope_5": _round(ema20 - ema20_series[-6]),
        },
        "ema20_series": [
            {"date": bar.day.isoformat(), "value": _round(value)}
            for bar, value in zip(bars, ema20_series)
        ],
        "swings": rendered_swings,
        "support_resistance_zones": zones,
        "range_boxes": range_boxes,
        "levels": {"support": supports, "resistance": resistances},
        "structure": structure,
        "range": {
            "detected": range_state,
            "lookback_bars": period,
            "low": _round(range_low),
            "high": _round(range_high),
            "width_atr": _round(range_width_atr),
        },
        "breakout": breakout,
        "breakout_failure": failure,
        "patterns": _patterns(swings, atr),
        "pullback": {
            "from_20_bar_high_pct": _round(pullback_pct * 100, 2),
            "near_support": near_support,
            "near_ema20": near_ema,
        },
        "long_location": location,
    }
    summary = [
        f"{structure['trend']} ({structure['highs']}, {structure['lows']}).",
        f"Close {_round(latest.close)} is {'above' if latest.close >= ema20 else 'below'} EMA20 {_round(ema20)}.",
        f"LONG location: {location}.",
    ]
    if breakout:
        summary.append(f"Up-breakout detected above {_round(breakout['level'])}.")
    if failure:
        summary.append(f"Up-breakout failure detected at {_round(failure['level'])}.")
    if range_state:
        summary.append(f"Range candidate {_round(range_low)}–{_round(range_high)}.")
    situation = {
        "summary": summary[:8],
        "market_structure": structure["trend"],
        "swing_structure": {"highs": structure["highs"], "lows": structure["lows"]},
        "long_location": location,
        "ema20_state": "ABOVE" if latest.close >= ema20 else "BELOW",
        "breakout_state": "FAILED" if failure else ("UP_BREAKOUT" if breakout else "NONE"),
        "range_state": "DETECTED" if range_state else "NOT_DETECTED",
        "pattern_candidates": geometry["patterns"],
    }
    annotations = [
        Annotation(type="EMA_LINE", label="EMA20", source="detected_geometry", date=latest.day.isoformat(), price=_round(ema20)),
        Annotation(
            type="HORIZONTAL_LEVEL",
            label="Current price",
            source="detected_geometry",
            date=latest.day.isoformat(),
            price=_round(latest.close),
        ),
    ]
    annotations.extend(
        Annotation(
            type="MARKER",
            label=point["label"],
            source="detected_geometry",
            date=point["date"],
            price=point["price"],
            confidence="MEDIUM",
            metadata={"swing_type": point["type"]},
        )
        for point in rendered_swings[-12:]
    )
    for name, levels in (("support", supports[:3]), ("resistance", resistances[:3])):
        annotations.extend(
            Annotation(
                type="ZONE_BOX",
                label=f"{name.title()} cluster ({level['touches']} touches)",
                source="detected_geometry",
                start_date=level["first_date"],
                end_date=latest.day.isoformat(),
                y_min=level["price_low"],
                y_max=level["price_high"],
                confidence="MEDIUM" if level["touches"] < 3 else "HIGH",
                metadata={"zone_type": name.upper()},
            )
            for level in levels
        )
    if range_state:
        annotations.append(Annotation(
            type="RANGE_BOX",
            label="Trading range",
            source="detected_geometry",
            start_date=bars[-period].day.isoformat(),
            end_date=latest.day.isoformat(),
            y_min=_round(range_low),
            y_max=_round(range_high),
            confidence="MEDIUM",
        ))
    return geometry, situation, annotations
