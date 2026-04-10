"""warmup_state: cold | warming | ready — locked criteria."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from app.threshold_profile import (
    WARMUP_COLD_MAX_ELAPSED_SEC,
    WARMUP_COLD_MAX_TRADES,
    WARMUP_READY_MIN_BASELINE_SAMPLES,
    WARMUP_READY_MIN_ELAPSED_SEC,
    WARMUP_READY_MIN_TRADES,
)


@dataclass
class WarmupInputs:
    first_event_ts: datetime | None
    now: datetime
    trade_count_session: int
    baseline_sample_count: int


def compute_warmup_state(inp: WarmupInputs) -> str:
    if inp.first_event_ts is None:
        return "cold"
    elapsed = (inp.now - inp.first_event_ts).total_seconds()
    if elapsed < WARMUP_COLD_MAX_ELAPSED_SEC or inp.trade_count_session < WARMUP_COLD_MAX_TRADES:
        return "cold"
    if (
        elapsed < WARMUP_READY_MIN_ELAPSED_SEC
        or inp.trade_count_session < WARMUP_READY_MIN_TRADES
        or inp.baseline_sample_count < WARMUP_READY_MIN_BASELINE_SAMPLES
    ):
        return "warming"
    return "ready"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)
