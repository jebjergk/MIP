"""Learning View payload for Adviser V0.1 bar walkthrough."""

from __future__ import annotations

from datetime import date
from typing import Any

from .adviser_repository import load_adviser_bars, load_adviser_calls
from .historical_bar_repository import load_bars_from_store


def build_adviser_educational_grid(
    *,
    adviser_attempt_id: str,
    symbol: str,
    trading_date: date,
) -> list[dict[str, Any]]:
    bars = load_bars_from_store(symbol, trading_date)
    bar_map = {str(b.ts_ny): b for b in bars if b.rth}
    calls = {c.get("bar_ts_et"): c for c in load_adviser_calls(adviser_attempt_id)}
    rows: list[dict[str, Any]] = []
    for ab in load_adviser_bars(adviser_attempt_id):
        ts = ab.get("bar_ts_ny")
        b = bar_map.get(str(ts))
        ohlcv = (
            {"open": b.open, "high": b.high, "low": b.low, "close": b.close, "volume": b.volume}
            if b
            else {}
        )
        et = ab.get("bar_ts_et") or ""
        call = next((c for c in load_adviser_calls(adviser_attempt_id) if c.get("bar_ts_et") == et), None)
        if ab.get("adviser_called") and call:
            rows.append(
                {
                    "bar_ts": str(ts),
                    "bar_ts_ny": str(ts),
                    "time_ny": et,
                    "ohlcv": ohlcv,
                    "adviser_called": True,
                    "adviser_wake_reason": call.get("wake_reason"),
                    "adviser_retrieved": call.get("retrieved_concepts"),
                    "adviser_thesis": call.get("current_thesis"),
                    "adviser_watch": call.get("watch_conditions"),
                    "adviser_action": call.get("action"),
                    "adviser_summary": call.get("brooks_reasoning_summary"),
                    "selected_action": call.get("action"),
                    "market_story": call.get("brooks_reasoning_summary"),
                }
            )
        else:
            rows.append(
                {
                    "bar_ts": str(ts),
                    "bar_ts_ny": str(ts),
                    "time_ny": et,
                    "ohlcv": ohlcv,
                    "adviser_called": False,
                    "adviser_action": ab.get("action_snapshot"),
                    "adviser_thesis": ab.get("thesis_snapshot"),
                    "adviser_watch": ab.get("watch_snapshot"),
                    "market_story": ab.get("bar_note") or "No material change — continuing current Brooks thesis.",
                    "selected_action": ab.get("action_snapshot"),
                }
            )
    return rows
