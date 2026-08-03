"""Phase 5C V0.3 — double-bottom hierarchy and symmetric micro channels."""

from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import Any

from .bars import HistoricalBar
from .pattern_engine import PatternRecord, SessionPatternState, _bar_index_at, _obs_id, _register, get_pattern_session
from .pattern_engine_v02 import (
    _breakout_v02,
    _ensure_v02_session,
    _expire_stale_v02,
    _update_pullback_and_h_v02,
    _update_swings_v02,
)
from .pattern_ruleset_v01 import (
    LIFECYCLE_CONFIRMED,
    LIFECYCLE_DEVELOPING,
    LIFECYCLE_EXPIRED,
    LIFECYCLE_FAILED,
    LIFECYCLE_POSSIBLE,
)
from .pattern_ruleset_v02 import median_bar_range, price_tolerance, tick_size
from .pattern_ruleset_v03 import RULESET_VERSION, resolve_params


def _low_key(symbol: str, price: float) -> str:
    t = tick_size(symbol)
    return f"{round(price / t) * t:.4f}"


def _canonical_pair_key(symbol: str, l1: float, l2: float) -> str:
    a, b = sorted([_low_key(symbol, l1), _low_key(symbol, l2)])
    return f"{a}|{b}"


def _classify_double_bottom(
    *,
    bars_apart: int,
    rally: float,
    med: float,
    near_structural: bool,
    params: dict[str, Any],
) -> str | None:
    sym_bounce = rally / max(med, tick_size("AAPL"))
    micro_max = int(params["micro_double_bottom_max_bars_apart"])
    local_max = int(params["local_double_bottom_max_bars_apart"])
    struct_min = int(params["structural_double_bottom_min_bars_apart"])

    if rally < float(params["low_retest_min_bounce_fraction"]) * med:
        return None
    if rally < float(params["low_retest_max_bounce_fraction"]) * med and bars_apart <= micro_max:
        return "LOW_RETEST"
    if bars_apart <= micro_max and sym_bounce >= float(params["micro_db_min_bounce_fraction"]):
        return "MICRO_DOUBLE_BOTTOM"
    if bars_apart <= local_max and sym_bounce >= float(params["local_db_min_bounce_fraction"]):
        return "LOCAL_DOUBLE_BOTTOM"
    if bars_apart >= struct_min and sym_bounce >= float(params["structural_db_min_bounce_fraction"]) and near_structural:
        return "STRUCTURAL_DOUBLE_BOTTOM"
    if bars_apart >= int(params["double_bottom_min_bars_apart"]) and sym_bounce >= float(params["local_db_min_bounce_fraction"]):
        return "LOCAL_DOUBLE_BOTTOM"
    return None


def _double_bottoms_v03(
    sess: SessionPatternState,
    *,
    bar: HistoricalBar,
    bar_ts: datetime,
    obs_id: str,
    params: dict[str, Any],
) -> None:
    v03 = sess._v03  # type: ignore[attr-defined]
    lows = [(b.low, b.ts_utc, i) for i, b in enumerate(sess.bars)]
    if len(lows) < 5:
        return
    i = len(lows) - 1
    med = median_bar_range(sess.bars, int(params["volatility_range_median_lookback"]))
    l2, t2, _ = lows[i]

    struct_lows = [s.get("price") for s in sess.confirmed_swing_lows]
    near_struct_ticks = int(params["structural_db_near_swing_ticks"]) * tick_size(sess.symbol)

    for j, (l1, t1, _ji) in enumerate(lows):
        if j >= i - 2:
            continue
        bars_apart = i - j
        if bars_apart > int(params["double_bottom_max_bars_apart"]):
            continue
        tol = price_tolerance(sess.symbol, bar.high - bar.low, params, micro=bars_apart <= int(params["micro_double_bottom_max_bars_apart"]))
        if abs(l1 - l2) > tol:
            continue
        between_high = max(b.high for b in sess.bars[j : i + 1])
        rally = between_high - max(l1, l2)
        near_structural = any(abs(l1 - sl) <= near_struct_ticks or abs(l2 - sl) <= near_struct_ticks for sl in struct_lows)
        fam = _classify_double_bottom(bars_apart=bars_apart, rally=rally, med=med, near_structural=near_structural, params=params)
        if not fam:
            continue

        pk = _canonical_pair_key(sess.symbol, l1, l2)
        active_key = f"{fam}:{pk}"
        existing_id = v03["active_db"].get(active_key)
        if existing_id and existing_id in sess.patterns:
            pat = sess.patterns[existing_id]
            pat.current_ts = bar_ts
            pat.relevant_prices.update(
                {
                    "second_low": l2,
                    "bars_between": bars_apart,
                    "intervening_rally": rally,
                    "median_range": med,
                    "rally_over_median": rally / max(med, tick_size(sess.symbol)),
                }
            )
            if bar.close >= between_high - tick_size(sess.symbol) * 2 and pat.lifecycle == LIFECYCLE_POSSIBLE:
                pat.transition(LIFECYCLE_CONFIRMED, ts=bar_ts, rule_id="DB_CONFIRM_V0_3", note="Neckline test")
            return

        if int(v03["pair_counts"].get(pk, 0)) >= int(params["double_bottom_max_reuse_per_pair"]):
            continue
        v03["pair_counts"][pk] = int(v03["pair_counts"].get(pk, 0)) + 1

        lifecycle = LIFECYCLE_POSSIBLE
        if bar.close >= between_high - tick_size(sess.symbol) * 2:
            lifecycle = LIFECYCLE_CONFIRMED
        pat = PatternRecord(
            pattern_instance_id=str(uuid.uuid4()),
            pattern_family=fam,
            direction="LONG",
            lifecycle=lifecycle,
            start_ts=t1,
            current_ts=bar_ts,
            trading_date=sess.trading_date,
            relevant_prices={
                "key": active_key,
                "pair_key": pk,
                "first_low": l1,
                "second_low": l2,
                "neckline": between_high,
                "bars_between": bars_apart,
                "intervening_rally": rally,
                "median_range": med,
                "rally_over_median": rally / max(med, tick_size(sess.symbol)),
                "near_structural_swing": near_structural,
            },
            supporting_observation_ids=[obs_id],
            supporting_rule_ids=["DOUBLE_BOTTOM_V0_3"],
            explanation=f"{fam.replace('_', ' ')} (V0.3 hierarchy).",
        )
        pat.lifecycle_history.append({"lifecycle": LIFECYCLE_POSSIBLE, "ts": str(t1), "rule_id": "DOUBLE_BOTTOM_V0_3", "note": ""})
        if lifecycle == LIFECYCLE_CONFIRMED:
            pat.transition(LIFECYCLE_CONFIRMED, ts=bar_ts, rule_id="DB_CONFIRM_V0_3", note="")
        v03["active_db"][active_key] = pat.pattern_instance_id
        _register(pat, sess)
        break


def _channel_tick_ok(prev: float, cur: float, *, bullish: bool, tol: float) -> bool:
    return cur >= prev - tol if bullish else cur <= prev + tol


def _micro_channels_v03(sess: SessionPatternState, *, bar: HistoricalBar, bar_ts: datetime, obs_id: str, params: dict[str, Any]) -> None:
    v03 = sess._v03  # type: ignore[attr-defined]
    min_len = int(params["micro_channel_min_bars"])
    confirm_len = int(params["micro_channel_confirm_bars"])
    tol = tick_size(sess.symbol) * int(params["micro_channel_tick_tolerance"])
    if len(sess.bars) < 2:
        return

    recent = sess.bars[-min_len:] if len(sess.bars) >= min_len else sess.bars
    bull_seq = len(recent) >= min_len and all(
        _channel_tick_ok(recent[k - 1].low, recent[k].low, bullish=True, tol=tol) for k in range(1, len(recent))
    )
    bear_seq = len(recent) >= min_len and all(
        _channel_tick_ok(recent[k - 1].high, recent[k].high, bullish=False, tol=tol) for k in range(1, len(recent))
    )

    def _extend_or_create(direction: str, fam: str, pat_id_key: str) -> None:
        pid = v03.get(pat_id_key)
        length = len(sess.bars) - (_bar_index_at(sess, recent[0].ts_utc) if recent else sess.bar_index)
        if pid and pid in sess.patterns:
            p = sess.patterns[pid]
            p.current_ts = bar_ts
            mx = int(p.relevant_prices.get("max_length") or 0)
            p.relevant_prices["channel_length"] = length
            p.relevant_prices["max_length"] = max(mx, length)
            if length >= confirm_len and p.lifecycle == LIFECYCLE_DEVELOPING:
                p.transition(LIFECYCLE_CONFIRMED, ts=bar_ts, rule_id="MICRO_CHANNEL_CONFIRM_V0_3", note=f"length>={confirm_len}")
            return
        pid = str(uuid.uuid4())
        v03[pat_id_key] = pid
        pat = PatternRecord(
            pattern_instance_id=pid,
            pattern_family=fam,
            direction="LONG" if direction == "BULL" else "BEARISH",
            lifecycle=LIFECYCLE_DEVELOPING,
            start_ts=recent[0].ts_utc,
            current_ts=bar_ts,
            trading_date=sess.trading_date,
            relevant_prices={"channel_length": length, "max_length": length, "direction": direction},
            supporting_observation_ids=[obs_id],
            supporting_rule_ids=["MICRO_CHANNEL_V0_3"],
            explanation=f"{fam} updating instance (V0.3).",
        )
        pat.lifecycle_history.append({"lifecycle": LIFECYCLE_DEVELOPING, "ts": str(bar_ts), "rule_id": "MICRO_CHANNEL_V0_3", "note": ""})
        _register(pat, sess)

    if bull_seq:
        _extend_or_create("BULL", "BULL_MICRO_CHANNEL", "bull_channel_pat_id")
    elif v03.get("bull_channel_pat_id"):
        p = sess.patterns.get(v03["bull_channel_pat_id"])
        if p and p.lifecycle in (LIFECYCLE_DEVELOPING, LIFECYCLE_CONFIRMED):
            p.transition(LIFECYCLE_FAILED, ts=bar_ts, rule_id="MICRO_CHANNEL_BREAK_V0_3", note="bull break")
            p.relevant_prices["termination"] = "low broke prior bar"
        v03["bull_channel_pat_id"] = None

    if bear_seq:
        _extend_or_create("BEAR", "BEAR_MICRO_CHANNEL", "bear_channel_pat_id")
    elif v03.get("bear_channel_pat_id"):
        p = sess.patterns.get(v03["bear_channel_pat_id"])
        if p and p.lifecycle in (LIFECYCLE_DEVELOPING, LIFECYCLE_CONFIRMED):
            p.transition(LIFECYCLE_FAILED, ts=bar_ts, rule_id="MICRO_CHANNEL_BREAK_V0_3", note="bear break")
            p.relevant_prices["termination"] = "high exceeded prior bar"
        v03["bear_channel_pat_id"] = None


def _ensure_v03_session(sess: SessionPatternState) -> None:
    _ensure_v02_session(sess)
    if not hasattr(sess, "_v03"):
        sess._v03 = {  # type: ignore[attr-defined]
            "active_db": {},
            "pair_counts": {},
            "bull_channel_pat_id": None,
            "bear_channel_pat_id": None,
        }


def advance_patterns_v03_for_bar(
    *,
    state: dict[str, Any],
    run_id: str,
    objective_baseline_attempt_id: str,
    symbol: str,
    trading_date: date,
    bar: HistoricalBar,
    objective_obs: dict[str, Any],
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    params = resolve_params(params)
    sess = get_pattern_session(state, symbol, trading_date)
    _ensure_v03_session(sess)
    bar_ts = bar.ts_utc
    sess.bars.append(bar)
    sess.bar_index += 1
    obs_id = _obs_id(run_id, objective_baseline_attempt_id, symbol, bar_ts)

    _expire_stale_v02(sess, bar_ts, params)
    _update_swings_v02(
        sess,
        bar=bar,
        bar_ts=bar_ts,
        obs_id=obs_id,
        params=params,
        run_id=run_id,
        baseline_attempt=objective_baseline_attempt_id,
    )
    _update_pullback_and_h_v02(sess, bar=bar, bar_ts=bar_ts, objective_obs=objective_obs, obs_id=obs_id, params=params)
    _double_bottoms_v03(sess, bar=bar, bar_ts=bar_ts, obs_id=obs_id, params=params)
    _breakout_v02(sess, bar=bar, bar_ts=bar_ts, objective_obs=objective_obs, obs_id=obs_id, params=params)
    _micro_channels_v03(sess, bar=bar, bar_ts=bar_ts, obs_id=obs_id, params=params)

    from . import pattern_engine as pe

    pe._wedge_three_push(sess, bar=bar, bar_ts=bar_ts, obs_id=obs_id, params=params)
    if sess.bar_index % 5 == 0:
        pe._two_leg_pullback(sess, bar=bar, bar_ts=bar_ts, obs_id=obs_id, params=params)
    pe._climax(sess, bar=bar, bar_ts=bar_ts, objective_obs=objective_obs, obs_id=obs_id, params=params)

    active = [p for p in sess.patterns.values() if p.current_ts == bar_ts or p.lifecycle in (LIFECYCLE_POSSIBLE, LIFECYCLE_DEVELOPING)]
    return {
        "pattern_instances_created_or_updated": [p for p in sess.patterns.values() if p.current_ts == bar_ts],
        "active_pattern_instance_ids": [p.pattern_instance_id for p in active[-20:]],
        "pattern_snapshot": [
            {"pattern_instance_id": p.pattern_instance_id, "pattern_family": p.pattern_family, "lifecycle": p.lifecycle, "direction": p.direction}
            for p in active
        ],
        "context_json": {"phase": "PATTERN_BROOKS_V0_3", "interpretation": "CONTEXTUAL_INTERPRETATION"},
        "action": "OBSERVE",
        "explanation": "V0.3 pattern step.",
        "ruleset_version": RULESET_VERSION,
    }
