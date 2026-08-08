"""Deterministic intraday volatility metrics and regime (no lookahead)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .bars import HistoricalBar
from .context_ruleset_v04 import (
    VOL_EXPANSION,
    VOL_HIGH_CHAOTIC,
    VOL_HIGH_DIRECTIONAL,
    VOL_LOW_COMPRESSION,
    VOL_NORMAL,
)


@dataclass
class VolatilitySessionState:
    true_ranges: list[float] = field(default_factory=list)
    bar_true_range: float = 0.0
    rolling_median_true_range: float = 0.0
    rolling_average_true_range: float = 0.0
    current_range_ratio: float = 1.0
    opening_range: float = 0.0
    session_realized_range: float = 0.0
    recent_directional_efficiency: float = 0.0
    overlap_ratio: float = 0.0
    regime: str = VOL_NORMAL
    session_high: float = -1e18
    session_low: float = 1e18
    opening_high: float = -1e18
    opening_low: float = 1e18
    closes: list[float] = field(default_factory=list)


def _median(values: list[float]) -> float:
    if not values:
        return 0.01
    s = sorted(values)
    m = len(s) // 2
    if len(s) % 2:
        return s[m]
    return (s[m - 1] + s[m]) / 2.0


def _true_range(bar: HistoricalBar, prior_close: float | None) -> float:
    tr = bar.high - bar.low
    if prior_close is not None:
        tr = max(tr, abs(bar.high - prior_close), abs(bar.low - prior_close))
    return max(tr, 0.0001)


def update_volatility(
    vol: VolatilitySessionState,
    *,
    bar: HistoricalBar,
    prior_close: float | None,
    bar_index: int,
    params: dict[str, Any],
) -> None:
    vol.session_high = max(vol.session_high, bar.high)
    vol.session_low = min(vol.session_low, bar.low)
    vol.session_realized_range = max(vol.session_high - vol.session_low, 0.0001)

    tr = _true_range(bar, prior_close)
    vol.bar_true_range = tr
    vol.true_ranges.append(tr)
    vol.closes.append(bar.close)

    win_med = int(params["vol_tr_median_window"])
    win_avg = int(params["vol_tr_avg_window"])
    recent = vol.true_ranges[-win_med:]
    vol.rolling_median_true_range = _median(recent)
    avg_slice = vol.true_ranges[-win_avg:]
    vol.rolling_average_true_range = sum(avg_slice) / max(len(avg_slice), 1)
    vol.current_range_ratio = tr / max(vol.rolling_median_true_range, 0.0001)

    orb = int(params["vol_opening_range_bars"])
    if bar_index < orb:
        vol.opening_high = max(vol.opening_high, bar.high)
        vol.opening_low = min(vol.opening_low, bar.low)
    if bar_index == orb - 1 or (bar_index >= orb and vol.opening_range <= 0):
        if vol.opening_high > -1e17 and vol.opening_low < 1e17:
            vol.opening_range = max(vol.opening_high - vol.opening_low, 0.0001)

    if len(vol.closes) >= 3:
        net = abs(vol.closes[-1] - vol.closes[-3])
        path = sum(abs(vol.closes[i] - vol.closes[i - 1]) for i in range(len(vol.closes) - 2, len(vol.closes)))
        vol.recent_directional_efficiency = net / max(path, 0.0001)
    else:
        vol.recent_directional_efficiency = 0.0

    br = max(bar.high - bar.low, 0.0001)
    body = abs(bar.close - bar.open)
    vol.overlap_ratio = 1.0 - min(body / br, 1.0)

    vol.regime = classify_regime(vol, params)


def classify_regime(vol: VolatilitySessionState, params: dict[str, Any]) -> str:
    low_r = float(params["vol_low_ratio"])
    high_r = float(params["vol_high_ratio"])
    ov_high = float(params["vol_overlap_high"])
    eff_hi = float(params["vol_directional_efficiency_high"])
    eff_lo = float(params["vol_directional_efficiency_low"])

    ratio = vol.current_range_ratio
    eff = vol.recent_directional_efficiency
    overlap = vol.overlap_ratio

    if ratio >= high_r and eff >= eff_hi and overlap < ov_high:
        return VOL_HIGH_DIRECTIONAL
    if ratio >= high_r and (eff <= eff_lo or overlap >= ov_high):
        return VOL_HIGH_CHAOTIC
    if ratio >= high_r * 0.95:
        return VOL_EXPANSION
    if ratio <= low_r and overlap >= ov_high * 0.85:
        return VOL_LOW_COMPRESSION
    return VOL_NORMAL


def volatility_snapshot(vol: VolatilitySessionState) -> dict[str, float | str]:
    return {
        "bar_true_range": vol.bar_true_range,
        "rolling_median_true_range": vol.rolling_median_true_range,
        "rolling_average_true_range": vol.rolling_average_true_range,
        "current_range_ratio": vol.current_range_ratio,
        "opening_range": vol.opening_range,
        "session_realized_range": vol.session_realized_range,
        "recent_directional_efficiency": vol.recent_directional_efficiency,
        "overlap_ratio": vol.overlap_ratio,
        "regime": vol.regime,
    }
