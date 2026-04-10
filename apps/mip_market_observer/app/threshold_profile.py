"""Single source of truth for Tape Phase 1 thresholds. Bump threshold_profile_version when changed.

feed_health (see tape_coordinator.build_snapshot + feed_health.combine_worst):
  - disconnected: IB not connected (non-sim), OR no trade/quote yet and
    (now - created_ts) > DISCONNECTED_GRACE_SEC (30s).
  - delayed (bootstrap): no events yet and session_age <= DISCONNECTED_GRACE_SEC — internal label, not exchange delay.
  - Otherwise feed_health = combine_worst(trade_tier, quote_tier, quotes_expected) where each tier comes from
    tier_from_age_sec (feed_health.py):
      * live: age_sec <= STALE_LIVE_MAX (3.0s)
      * delayed: 3.0s < age_sec <= STALE_DELAYED_MAX (10.0s)
      * stale: age_sec > 10.0s
    If quotes_expected is False, combined uses trade_tier or \"stale\".

warmup_state (warmup.compute_warmup_state):
  - cold: no first_event_ts OR (elapsed < 10s AND trade_count_session < 5)
  - warming: not cold, but elapsed < 45s OR trades < 25 OR baseline_sample_count < WARMUP_READY_MIN_BASELINE (env TAPE_WARMUP_MIN_BASELINE, default 30)
  - ready: all of the above thresholds satisfied

Living Chart tape UI: strict gate + dwell in tape_ui_gate / TAPE_UI_DWELL_SEC / TAPE_UI_MIN_SNAPSHOTS.
"""

from __future__ import annotations

import os


def _env_int(key: str, default: int) -> int:
    try:
        return int((os.getenv(key) or str(default)).strip())
    except ValueError:
        return default


def _env_float(key: str, default: float) -> float:
    try:
        return float((os.getenv(key) or str(default)).strip())
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

# Living Chart — tape_active_for_ui dwell (strict gate must hold; reset on any failure)
TAPE_UI_DWELL_SEC = _env_float("TAPE_UI_DWELL_SEC", 8.0)
TAPE_UI_MIN_SNAPSHOTS = _env_int("TAPE_UI_MIN_SNAPSHOTS", 3)

# Phase 2 — vacuum / absorption / exhaustion / burst gates
VACUUM_SCORE_MIN = 0.52
VACUUM_RET_MULT_BASE = 0.00018  # scaled by regime_mult
ABSORPTION_SCORE_MIN = 0.45
EXHAUSTION_SCORE_MIN = 0.48
BURST_STRONG = 0.62
REGIME_AFTERHOURS_VACUUM_MULT = 0.82
REGIME_PREMARKET_VACUUM_MULT = 0.88
