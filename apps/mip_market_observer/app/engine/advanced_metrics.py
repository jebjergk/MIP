"""Phase 2 deterministic microstructure scores (no ML)."""

from __future__ import annotations

import math
from collections import deque
from datetime import datetime
from typing import Any

_EPS = 1e-9


def _mid_at(quotes: deque, target_ts: float, max_age: float = 12.0) -> float | None:
    best = None
    best_dt = None
    for ts, bid, ask, _bs, _as in quotes:
        if bid is None or ask is None or ask < bid:
            continue
        m = (bid + ask) / 2
        dt = abs(ts.timestamp() - target_ts)
        if best is None or dt < best_dt:
            best = m
            best_dt = dt
    if best is None or best_dt is None or best_dt > max_age:
        return None
    return best


def mid_ret_seconds(quotes: deque, now: datetime, sec: float) -> float | None:
    if not quotes:
        return None
    cur = _mid_at(quotes, now.timestamp())
    past = _mid_at(quotes, now.timestamp() - sec)
    if cur is None or past is None or past < _EPS:
        return None
    return (cur - past) / past


def _agg_trades_window(trades: deque, now: datetime, sec: float) -> tuple[float, float, float, int]:
    t0 = now.timestamp() - sec
    total = buy_v = sell_v = 0.0
    n = 0
    for ts, _p, sz, side, _c in trades:
        if ts.timestamp() < t0:
            continue
        n += 1
        total += sz
        if side == "buy":
            buy_v += sz
        elif side == "sell":
            sell_v += sz
    return total, buy_v, sell_v, n


def _spread_series(quotes: deque, now: datetime, sec: float) -> list[float]:
    t0 = now.timestamp() - sec
    out: list[float] = []
    for ts, bid, ask, _bs, _as in quotes:
        if ts.timestamp() < t0:
            continue
        if bid and ask and ask >= bid:
            out.append(ask - bid)
    return out


def _depth_series(quotes: deque, now: datetime, sec: float) -> list[float]:
    t0 = now.timestamp() - sec
    out: list[float] = []
    for ts, bid, ask, bs, a_s in quotes:
        if ts.timestamp() < t0:
            continue
        if bs is not None and a_s is not None and bs + a_s > 0:
            out.append(bs + a_s)
    return out


def compute_advanced_metrics(
    *,
    trades: deque,
    quotes: deque,
    now: datetime,
    vol_60s: float,
    tape_signed: float,
    spread: float | None,
    burst_history: list[float],
) -> dict[str, Any]:
    """Returns burst, absorption, vacuum, exhaustion scores in [0,1] plus helpers."""
    mid_ret_5s = mid_ret_seconds(quotes, now, 5.0)
    mid_ret_15s = mid_ret_seconds(quotes, now, 15.0)
    mid_ret_30s = mid_ret_seconds(quotes, now, 30.0)

    vol_5s, buy_5s, sell_5s, n_5s = _agg_trades_window(trades, now, 5.0)
    vol_30s, buy_30s, sell_30s, n_30s = _agg_trades_window(trades, now, 30.0)

    known_5 = buy_5s + sell_5s
    buy_ratio_5 = buy_5s / (known_5 + _EPS) if known_5 > _EPS else 0.5
    known_30 = buy_30s + sell_30s
    buy_ratio_30 = buy_30s / (known_30 + _EPS) if known_30 > _EPS else 0.5

    # Burst: activity + short move
    c1 = min(1.0, n_5s / 22.0)
    c2 = min(1.0, vol_5s / max(vol_60s / 10.0, 50.0))
    c3 = min(1.0, abs(mid_ret_5s or 0.0) / 0.00035)
    dom = abs(buy_ratio_5 - 0.5) * 2.0
    burst_score = max(0.0, min(1.0, 0.3 * c1 + 0.3 * c2 + 0.28 * c3 + 0.12 * dom))

    spreads = _spread_series(quotes, now, 60.0)
    spread_mean = sum(spreads) / len(spreads) if spreads else None
    spread_z = None
    if spread is not None and spreads and len(spreads) >= 5:
        m = spread_mean or 0.0
        var = sum((s - m) ** 2 for s in spreads) / len(spreads)
        std = math.sqrt(var) if var > 0 else _EPS
        spread_z = (spread - m) / std

    depths = _depth_series(quotes, now, 60.0)
    depth_now = None
    if quotes:
        _ts, _b, _a, bs, a_s = quotes[-1]
        if bs is not None and a_s is not None:
            depth_now = bs + a_s
    depth_med = sorted(depths)[len(depths) // 2] if depths else None
    thin_book = (
        depth_now is not None
        and depth_med is not None
        and depth_med > _EPS
        and depth_now < 0.35 * depth_med
    )

    # Vacuum: fast move + (thin book OR wide spread)
    move = abs(mid_ret_5s or 0.0)
    wide = spread_z is not None and spread_z > 1.15
    vac_mag = min(
        1.0,
        move / 0.00025 * (0.45 + 0.35 * (1.0 if thin_book else 0.2) + 0.2 * (1.0 if wide else 0.15)),
    )
    sign = 1 if (mid_ret_5s or 0) > 0 else -1 if (mid_ret_5s or 0) < 0 else 0
    vacuum_up = vac_mag if sign > 0 else 0.0
    vacuum_down = vac_mag if sign < 0 else 0.0

    # Absorption: one-sided flow, flat mid
    flat_30 = mid_ret_30s is not None and abs(mid_ret_30s) < 0.00012
    buy_press = buy_ratio_30 > 0.62 and known_30 > 200
    sell_press = buy_ratio_30 < 0.38 and known_30 > 200
    abs_buy = min(1.0, (buy_ratio_30 - 0.5) * 2.5) * (1.0 if flat_30 else 0.25) * min(1.0, vol_30s / 2000.0)
    abs_sell = min(1.0, (0.5 - buy_ratio_30) * 2.5) * (1.0 if flat_30 else 0.25) * min(1.0, vol_30s / 2000.0)
    absorption_against_buyers = float(abs_buy if buy_press and flat_30 else 0.0)
    absorption_against_sellers = float(abs_sell if sell_press and flat_30 else 0.0)

    # Exhaustion: prior burst high, now decay
    peak = max(burst_history) if burst_history else 0.0
    burst_decay = peak > 0.55 and burst_score < peak * 0.55
    tape_decay = abs(tape_signed) < 0.22
    stall = mid_ret_15s is not None and abs(mid_ret_15s) < abs(mid_ret_5s or 0.0) * 0.85
    exh_base = (1.0 if burst_decay else 0.3) * (0.5 + 0.5 * (1.0 if tape_decay else 0.0)) * (0.4 + 0.6 * (1.0 if stall else 0.2))
    exhaustion_up = min(1.0, exh_base) if (tape_signed > 0.1 or buy_ratio_5 > 0.55) else 0.0
    exhaustion_down = min(1.0, exh_base) if (tape_signed < -0.1 or buy_ratio_5 < 0.45) else 0.0

    return {
        "burst_score": float(burst_score),
        "mid_ret_5s": mid_ret_5s,
        "mid_ret_15s": mid_ret_15s,
        "mid_ret_30s": mid_ret_30s,
        "vacuum_up_score": float(vacuum_up),
        "vacuum_down_score": float(vacuum_down),
        "absorption_against_buyers_score": absorption_against_buyers,
        "absorption_against_sellers_score": absorption_against_sellers,
        "exhaustion_up_score": float(exhaustion_up),
        "exhaustion_down_score": float(exhaustion_down),
        "spread_mean_60s": spread_mean,
        "spread_z": spread_z,
        "thin_book_touch": bool(thin_book),
    }
