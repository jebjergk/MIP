"""Single source of truth for Tape Phase 1 thresholds. Bump threshold_profile_version when changed."""

from __future__ import annotations

import os


def _env_int(key: str, default: int) -> int:
    try:
        return int((os.getenv(key) or str(default)).strip())
    except ValueError:
        return default


THRESHOLD_PROFILE_VERSION = "tape_phase2_v1"

# feed_health ages (seconds since last event)
STALE_LIVE_MAX = 3.0
STALE_DELAYED_MAX = 10.0

# disconnected: no events this long after subscription became "active"
DISCONNECTED_GRACE_SEC = 30.0

# subscription registry
IDLE_UNSUBSCRIBE_SEC = 90.0
SYMBOL_GRACE_SEC = 25.0

# warmup (per plan)
WARMUP_COLD_MAX_ELAPSED_SEC = 10.0
WARMUP_COLD_MAX_TRADES = 5
WARMUP_READY_MIN_ELAPSED_SEC = 45.0
WARMUP_READY_MIN_TRADES = 25
WARMUP_READY_MIN_BASELINE_SAMPLES = _env_int("TAPE_WARMUP_MIN_BASELINE", 30)

# baseline_confidence sample counts
BASELINE_LOW_MAX = 29
BASELINE_MED_MAX = 59

# opening price discovery (US equities RTH) — minutes after open
OPENING_WINDOW_MINUTES = 30

# classifier (raw)
DIRECTIONAL_TAPE_PRESSURE = 0.42
DIRECTIONAL_MID_RET = 0.00008
OPENING_TAPE_PRESSURE_MULT = 1.2

# hysteresis (server)
HYSTERESIS_SNAPSHOTS = 3

BASELINE_DEQUE_MAX = 120

# Phase 2 — vacuum / absorption / exhaustion / burst gates
VACUUM_SCORE_MIN = 0.52
VACUUM_RET_MULT_BASE = 0.00018  # scaled by regime_mult
ABSORPTION_SCORE_MIN = 0.45
EXHAUSTION_SCORE_MIN = 0.48
BURST_STRONG = 0.62
REGIME_AFTERHOURS_VACUUM_MULT = 0.82
REGIME_PREMARKET_VACUUM_MULT = 0.88
