"""Living Chart tape UI — strict binary gate + dwell (server-side only).

tape_active_for_ui is true only when strict_raw is true and dwell counters satisfied.
Inactive reason codes are for logs/diagnostics, not end-user chart copy.
"""

from __future__ import annotations

import logging
import math
from datetime import datetime
from typing import Any

from app.threshold_profile import TAPE_UI_DWELL_SEC, TAPE_UI_MIN_SNAPSHOTS

_log = logging.getLogger(__name__)

# Log at most once per symbol per TAPE_UI_LOG_THROTTLE_SEC while inactive
TAPE_UI_LOG_THROTTLE_SEC = 60.0


def strict_tape_ui_raw(
    *,
    feed_health: str,
    warmup_state: str,
    quote_sizes_available: bool,
    side_confidence_aggregate: str,
    buy_volume_60s: float,
    sell_volume_60s: float,
    ib_connected: bool,
    simulate_mode: bool,
) -> bool:
    if not simulate_mode and not ib_connected:
        return False
    if feed_health != "live":
        return False
    if warmup_state != "ready":
        return False
    if not quote_sizes_available:
        return False
    if side_confidence_aggregate != "high":
        return False
    if not math.isfinite(buy_volume_60s) or not math.isfinite(sell_volume_60s):
        return False
    if buy_volume_60s < 0 or sell_volume_60s < 0:
        return False
    return True


def tape_ui_inactive_reason_code(
    *,
    feed_health: str,
    warmup_state: str,
    quote_sizes_available: bool,
    side_confidence_aggregate: str,
    buy_volume_60s: float,
    sell_volume_60s: float,
    ib_connected: bool,
    simulate_mode: bool,
) -> str:
    if not simulate_mode and not ib_connected:
        return "ib_not_connected"
    if feed_health != "live":
        return f"feed_not_live:{feed_health}"
    if warmup_state != "ready":
        return f"warmup_not_ready:{warmup_state}"
    if not quote_sizes_available:
        return "quote_sizes_missing"
    if side_confidence_aggregate != "high":
        return f"side_confidence_not_high:{side_confidence_aggregate}"
    if not math.isfinite(buy_volume_60s) or not math.isfinite(sell_volume_60s):
        return "buy_sell_non_finite"
    return "dwell_pending"


def update_tape_ui_dwell(rt: Any, now: datetime, strict_raw: bool) -> bool:
    """Mutate rt tape_ui_* fields; return tape_active_for_ui."""
    if not strict_raw:
        rt.tape_ui_since = None
        rt.tape_ui_snap_streak = 0
        return False

    if rt.tape_ui_snap_streak == 0:
        rt.tape_ui_since = now
    rt.tape_ui_snap_streak += 1

    elapsed = (now - rt.tape_ui_since).total_seconds()
    return rt.tape_ui_snap_streak >= TAPE_UI_MIN_SNAPSHOTS and elapsed >= TAPE_UI_DWELL_SEC


def maybe_log_tape_ui_inactive(sym: str, rt: Any, now: datetime, *, tape_active: bool, reason_code: str) -> None:
    if tape_active:
        return
    last = getattr(rt, "tape_ui_last_log_ts", None)
    if last is not None and (now - last).total_seconds() < TAPE_UI_LOG_THROTTLE_SEC:
        return
    rt.tape_ui_last_log_ts = now
    if reason_code == "dwell_pending":
        _log.info(
            "tape_active_for_ui=false symbol=%s reason=dwell_pending streak=%s elapsed_sec=%.2f need_snapshots=%s need_sec=%.2f",
            sym,
            getattr(rt, "tape_ui_snap_streak", 0),
            (now - rt.tape_ui_since).total_seconds() if rt.tape_ui_since else 0.0,
            TAPE_UI_MIN_SNAPSHOTS,
            TAPE_UI_DWELL_SEC,
        )
    else:
        _log.info("tape_active_for_ui=false symbol=%s reason=%s", sym, reason_code)
