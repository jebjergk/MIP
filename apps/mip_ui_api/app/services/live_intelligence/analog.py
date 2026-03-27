"""k-NN style analog matching over bootstrap episode features (no Snowflake)."""

from __future__ import annotations

import math
from typing import Any


def _feat_from_position(tile: dict[str, Any]) -> list[float]:
    bars = (tile.get("chart") or {}).get("bars") or []
    closes = [float(b["close"]) for b in bars if b.get("close") is not None]
    ret15 = 0.0
    if len(closes) >= 2:
        ret15 = (closes[-1] / closes[-2]) - 1 if closes[-2] else 0.0
    vol = float((tile.get("volatility_context") or {}).get("live_volatility") or 0.01)
    prog = float((tile.get("progress_metrics") or {}).get("expected_progress_pct") or 0.0)
    return [ret15, vol, prog, float(tile.get("unrealized_pnl") or 0) / max(len(closes), 1)]


def _feat_from_episode(ep: dict[str, Any]) -> list[float]:
    rr = float(ep.get("realized_return") or 0.0)
    hb = float(ep.get("horizon_bars") or 1)
    vp = float(ep.get("feature_vol_proxy") or abs(rr))
    win = 1.0 if ep.get("outcome_winner") else 0.0
    return [rr, 1.0 / max(hb, 1.0), vp, win]


def _dist(a: list[float], b: list[float]) -> float:
    if len(a) != len(b):
        return 1e9
    return math.sqrt(sum((x - y) ** 2 for x, y in zip(a, b)))


def match_analogs(
    tile: dict[str, Any],
    episodes: list[dict[str, Any]],
    *,
    k: int = 8,
) -> dict[str, Any]:
    if not episodes:
        return {
            "closest": [],
            "winners": 0,
            "losers": 0,
            "avg_forward_return": None,
            "match_quality": 0.0,
            "best_exit_hint_bars": None,
        }

    q = _feat_from_position(tile)
    scored = [( _dist(q, _feat_from_episode(ep)), ep) for ep in episodes]
    scored.sort(key=lambda x: x[0])
    closest = [ep for _, ep in scored[:k]]
    d0 = scored[0][0] if scored else 1.0
    quality = max(0.0, 1.0 - min(d0 / 0.5, 1.0))

    winners = sum(1 for ep in closest if ep.get("outcome_winner"))
    losers = len(closest) - winners
    rets = [float(ep.get("realized_return") or 0) for ep in closest if ep.get("realized_return") is not None]
    avg_ret = sum(rets) / len(rets) if rets else None
    horizons = [int(ep.get("horizon_bars") or 0) for ep in closest if ep.get("horizon_bars")]
    best_exit_hint = int(sum(horizons) / len(horizons)) if horizons else None

    return {
        "closest": [
            {
                "horizon_bars": ep.get("horizon_bars"),
                "realized_return": ep.get("realized_return"),
                "outcome_winner": ep.get("outcome_winner"),
            }
            for ep in closest[:5]
        ],
        "winners": winners,
        "losers": losers,
        "avg_forward_return": avg_ret,
        "match_quality": round(quality, 4),
        "best_exit_hint_bars": best_exit_hint,
    }
