"""Phase 5B V0.2 pattern engine — separate path; V0.1 logic unchanged in pattern_engine.py."""

from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import Any

from .bars import HistoricalBar
from .pattern_engine import (
    PatternRecord,
    SessionPatternState,
    _bar_index_at,
    _dm,
    _obs_id,
    _register,
    _term_set,
    get_pattern_session,
)
from .pattern_ruleset_v01 import ALLOWED_ACTIONS, LIFECYCLE_CONFIRMED, LIFECYCLE_DEVELOPING, LIFECYCLE_EXPIRED, LIFECYCLE_FAILED, LIFECYCLE_POSSIBLE
from .pattern_ruleset_v02 import (
    RULESET_VERSION,
    expiry_bars_for_family,
    median_bar_range,
    price_tolerance,
    resolve_params,
    tick_size,
)


def _ensure_v02_session(sess: SessionPatternState) -> None:
    if not hasattr(sess, "_v02"):
        sess._v02 = {  # type: ignore[attr-defined]
            "used_low_keys": {},
            "breakout_levels": {},
            "micro_channel_pat_id": None,
            "h1_instance_id": None,
        }


def _expire_stale_v02(sess: SessionPatternState, bar_ts: datetime, params: dict[str, Any]) -> None:
    for pat in list(sess.patterns.values()):
        if pat.lifecycle in (LIFECYCLE_CONFIRMED, LIFECYCLE_FAILED, LIFECYCLE_EXPIRED):
            continue
        age = sess.bar_index - _bar_index_at(sess, pat.start_ts)
        limit = expiry_bars_for_family(pat.pattern_family, params)
        if age > limit and pat.lifecycle in (LIFECYCLE_POSSIBLE, LIFECYCLE_DEVELOPING):
            pat.transition(LIFECYCLE_EXPIRED, ts=bar_ts, rule_id="PATTERN_EXPIRE_V0_2", note=f"Family expiry {limit} bars")


def _update_swings_v02(
    sess: SessionPatternState,
    *,
    bar: HistoricalBar,
    bar_ts: datetime,
    obs_id: str,
    params: dict[str, Any],
    run_id: str,
    baseline_attempt: str,
) -> None:
    idx = len(sess.bars) - 1
    if idx < 2:
        return
    left, mid, cur = sess.bars[idx - 1], sess.bars[idx - 2], bar
    med = median_bar_range(sess.bars, int(params["volatility_range_median_lookback"]))
    min_exc = tick_size(sess.symbol) * int(params["structural_min_excursion_ticks"])

    if left.high > mid.high and left.high > cur.high:
        local_fam = "LOCAL_PIVOT_HIGH"
        pat = PatternRecord(
            pattern_instance_id=str(uuid.uuid4()),
            pattern_family=local_fam,
            direction="NEUTRAL",
            lifecycle=LIFECYCLE_CONFIRMED,
            start_ts=left.ts_utc,
            current_ts=bar_ts,
            trading_date=sess.trading_date,
            relevant_prices={"pivot_high": left.high},
            supporting_observation_ids=[_obs_id(run_id, baseline_attempt, sess.symbol, left.ts_utc), obs_id],
            supporting_rule_ids=["LOCAL_PIVOT_HIGH_V0_2"],
            explanation="Local pivot high (1-bar each side visible).",
        )
        pat.lifecycle_history.append(
            {"lifecycle": LIFECYCLE_POSSIBLE, "ts": str(left.ts_utc), "rule_id": "LOCAL_PIVOT_CANDIDATE_V0_2", "note": ""}
        )
        pat.transition(LIFECYCLE_CONFIRMED, ts=bar_ts, rule_id="LOCAL_PIVOT_CONFIRM_V0_2", note="")
        _register(pat, sess)
        excursion = left.high - min(b.low for b in sess.bars[max(0, idx - 5) : idx])
        if excursion >= min_exc and (idx - 2) >= int(params["structural_min_separation_bars"]):
            rec = {
                "ts": left.ts_utc,
                "price": left.high,
                "obs_id": _obs_id(run_id, baseline_attempt, sess.symbol, left.ts_utc),
            }
            sess.confirmed_swing_highs.append(rec)
            sp = PatternRecord(
                pattern_instance_id=str(uuid.uuid4()),
                pattern_family="STRUCTURAL_SWING_HIGH",
                direction="NEUTRAL",
                lifecycle=LIFECYCLE_CONFIRMED,
                start_ts=left.ts_utc,
                current_ts=bar_ts,
                trading_date=sess.trading_date,
                relevant_prices={"swing_high": left.high, "excursion": excursion},
                supporting_observation_ids=[rec["obs_id"], obs_id],
                supporting_rule_ids=["STRUCTURAL_SWING_HIGH_V0_2"],
                parent_pattern_instance_id=pat.pattern_instance_id,
                explanation="Structural swing high: local pivot with minimum excursion and separation.",
            )
            sp.lifecycle_history.append(
                {"lifecycle": LIFECYCLE_CONFIRMED, "ts": str(bar_ts), "rule_id": "STRUCTURAL_SWING_HIGH_V0_2", "note": ""}
            )
            _register(sp, sess)

    if left.low < mid.low and left.low < cur.low:
        pat = PatternRecord(
            pattern_instance_id=str(uuid.uuid4()),
            pattern_family="LOCAL_PIVOT_LOW",
            direction="NEUTRAL",
            lifecycle=LIFECYCLE_CONFIRMED,
            start_ts=left.ts_utc,
            current_ts=bar_ts,
            trading_date=sess.trading_date,
            relevant_prices={"pivot_low": left.low},
            supporting_observation_ids=[_obs_id(run_id, baseline_attempt, sess.symbol, left.ts_utc), obs_id],
            supporting_rule_ids=["LOCAL_PIVOT_LOW_V0_2"],
            explanation="Local pivot low (1-bar each side visible).",
        )
        pat.lifecycle_history.append(
            {"lifecycle": LIFECYCLE_POSSIBLE, "ts": str(left.ts_utc), "rule_id": "LOCAL_PIVOT_CANDIDATE_V0_2", "note": ""}
        )
        pat.transition(LIFECYCLE_CONFIRMED, ts=bar_ts, rule_id="LOCAL_PIVOT_CONFIRM_V0_2", note="")
        _register(pat, sess)
        excursion = max(b.high for b in sess.bars[max(0, idx - 5) : idx]) - left.low
        if excursion >= min_exc:
            rec = {
                "ts": left.ts_utc,
                "price": left.low,
                "obs_id": _obs_id(run_id, baseline_attempt, sess.symbol, left.ts_utc),
            }
            sess.confirmed_swing_lows.append(rec)
            sp = PatternRecord(
                pattern_instance_id=str(uuid.uuid4()),
                pattern_family="STRUCTURAL_SWING_LOW",
                direction="NEUTRAL",
                lifecycle=LIFECYCLE_CONFIRMED,
                start_ts=left.ts_utc,
                current_ts=bar_ts,
                trading_date=sess.trading_date,
                relevant_prices={"swing_low": left.low, "excursion": excursion},
                supporting_observation_ids=[rec["obs_id"], obs_id],
                supporting_rule_ids=["STRUCTURAL_SWING_LOW_V0_2"],
                parent_pattern_instance_id=pat.pattern_instance_id,
                explanation="Structural swing low: local pivot with minimum excursion.",
            )
            sp.lifecycle_history.append(
                {"lifecycle": LIFECYCLE_CONFIRMED, "ts": str(bar_ts), "rule_id": "STRUCTURAL_SWING_LOW_V0_2", "note": ""}
            )
            _register(sp, sess)


def _update_pullback_and_h_v02(
    sess: SessionPatternState,
    *,
    bar: HistoricalBar,
    bar_ts: datetime,
    objective_obs: dict[str, Any],
    obs_id: str,
    params: dict[str, Any],
) -> None:
    v02 = sess._v02  # type: ignore[attr-defined]
    terms = _term_set(objective_obs)
    dm = _dm(objective_obs)
    bull = dm.get("direction") == "BULLISH" or "BULL_BAR" in terms
    bear = dm.get("direction") == "BEARISH" or "BEAR_BAR" in terms
    min_fail_bars = int(params["h1_min_bars_before_fail"])

    pb = sess.pullback
    if pb is None and bear and sess.bar_index >= 2 and sess.bars[-1].close < sess.bars[-2].close:
        sess.pullback = {"start_ts": bar_ts, "start_index": sess.bar_index, "extreme_low": bar.low, "attempts": 0}
        pb = sess.pullback
        pat = PatternRecord(
            pattern_instance_id=str(uuid.uuid4()),
            pattern_family="PULLBACK",
            direction="BEARISH",
            lifecycle=LIFECYCLE_DEVELOPING,
            start_ts=bar_ts,
            current_ts=bar_ts,
            trading_date=sess.trading_date,
            relevant_prices={"pullback_low": bar.low},
            supporting_observation_ids=[obs_id],
            supporting_rule_ids=["PULLBACK_START_V0_2"],
            explanation="Countertrend pullback (V0.2).",
        )
        pat.lifecycle_history.append({"lifecycle": LIFECYCLE_DEVELOPING, "ts": str(bar_ts), "rule_id": "PULLBACK_START_V0_2", "note": ""})
        _register(pat, sess)

    if not pb:
        return
    pb["extreme_low"] = min(pb.get("extreme_low", bar.low), bar.low)

    if bull and bar.close > bar.open:
        pb["attempts"] = int(pb.get("attempts", 0)) + 1
        n = pb["attempts"]
        if n == 1 and v02["h1_instance_id"] is None:
            pid = str(uuid.uuid4())
            v02["h1_instance_id"] = pid
            sess.h1 = {"instance_id": pid, "start_index": sess.bar_index, "status": "POSSIBLE", "bars_alive": 0}
            pat = PatternRecord(
                pattern_instance_id=pid,
                pattern_family="POSSIBLE_H1_LONG",
                direction="LONG",
                lifecycle=LIFECYCLE_POSSIBLE,
                start_ts=bar_ts,
                current_ts=bar_ts,
                trading_date=sess.trading_date,
                relevant_prices={"entry_zone": bar.close, "pullback_low": pb["extreme_low"]},
                supporting_observation_ids=[obs_id],
                supporting_rule_ids=["H1_CANDIDATE_V0_2"],
                explanation="Possible H1 long (V0.2): first resumption after pullback.",
            )
            pat.lifecycle_history.append({"lifecycle": LIFECYCLE_POSSIBLE, "ts": str(bar_ts), "rule_id": "H1_CANDIDATE_V0_2", "note": ""})
            _register(pat, sess)
        elif n == 1 and sess.h1:
            sess.h1["bars_alive"] = int(sess.h1.get("bars_alive", 0)) + 1
            if bar.close > pb["extreme_low"] + tick_size(sess.symbol) * 3:
                sess.h1["status"] = "CONFIRMED"
                pid = sess.h1.get("instance_id")
                p = sess.patterns.get(pid)
                if p and p.lifecycle == LIFECYCLE_POSSIBLE:
                    p.transition(LIFECYCLE_CONFIRMED, ts=bar_ts, rule_id="H1_CONFIRM_V0_2", note="")
                    p.pattern_family = "CONFIRMED_H1_LONG_CANDIDATE"
        elif n >= 2 and sess.h1 and sess.h1.get("status") in ("FAILED", "CONFIRMED"):
            parent = sess.h1.get("instance_id")
            if parent and not any(
                x.pattern_family == "POSSIBLE_H2_LONG" and x.lifecycle == LIFECYCLE_POSSIBLE for x in sess.patterns.values()
            ):
                pat = PatternRecord(
                    pattern_instance_id=str(uuid.uuid4()),
                    pattern_family="POSSIBLE_H2_LONG",
                    direction="LONG",
                    lifecycle=LIFECYCLE_POSSIBLE,
                    start_ts=bar_ts,
                    current_ts=bar_ts,
                    trading_date=sess.trading_date,
                    relevant_prices={"second_attempt": bar.close},
                    supporting_observation_ids=[obs_id],
                    supporting_rule_ids=["H2_CANDIDATE_V0_2"],
                    parent_pattern_instance_id=parent,
                    explanation="Possible H2 after ordered H1 (V0.2).",
                )
                pat.lifecycle_history.append({"lifecycle": LIFECYCLE_POSSIBLE, "ts": str(bar_ts), "rule_id": "H2_CANDIDATE_V0_2", "note": ""})
                _register(pat, sess)

    if bear and sess.h1 and sess.h1.get("status") == "POSSIBLE":
        sess.h1["bars_alive"] = int(sess.h1.get("bars_alive", 0)) + 1
        if int(sess.h1.get("bars_alive", 0)) >= min_fail_bars and pb.get("attempts") == 1:
            sess.h1["status"] = "FAILED"
            pid = sess.h1.get("instance_id")
            p = sess.patterns.get(pid)
            if p and p.lifecycle == LIFECYCLE_POSSIBLE:
                p.transition(LIFECYCLE_FAILED, ts=bar_ts, rule_id="H1_FAIL_V0_2", note="Bear bar after min H1 bars")
                p.pattern_family = "FAILED_H1_LONG"

    age = sess.bar_index - pb.get("start_index", sess.bar_index)
    if age > int(params.get("h1_h2_expiry_bars", 10)):
        pid = v02.get("h1_instance_id")
        if pid:
            p = sess.patterns.get(pid)
            if p and p.lifecycle == LIFECYCLE_POSSIBLE:
                p.transition(LIFECYCLE_EXPIRED, ts=bar_ts, rule_id="H1_EXPIRE_V0_2", note="Pullback window ended")
        sess.pullback = None
        sess.h1 = None
        v02["h1_instance_id"] = None


def _low_key(symbol: str, price: float) -> str:
    t = tick_size(symbol)
    return f"{round(price / t) * t:.4f}"


def _double_bottoms_v02(
    sess: SessionPatternState,
    *,
    bar: HistoricalBar,
    bar_ts: datetime,
    obs_id: str,
    params: dict[str, Any],
) -> None:
    v02 = sess._v02  # type: ignore[attr-defined]
    lows = [(b.low, b.ts_utc, i) for i, b in enumerate(sess.bars)]
    if len(lows) < 5:
        return
    i = len(lows) - 1
    med = median_bar_range(sess.bars, int(params["volatility_range_median_lookback"]))
    min_bounce = float(params["double_bottom_min_bounce_range_fraction"]) * max(med, tick_size(sess.symbol))
    normal_min = int(params["double_bottom_min_bars_apart"])
    micro_max = int(params["micro_double_bottom_max_bars_apart"])

    for j, (l1, t1, _ji) in enumerate(lows):
        if j >= i - normal_min:
            continue
        if i - j > int(params["double_bottom_max_bars_apart"]):
            continue
        l2, t2, _ = lows[i]
        bars_apart = i - j
        is_micro = micro_max >= bars_apart >= int(params["micro_double_bottom_min_bars_apart"]) and bars_apart < normal_min
        is_normal = bars_apart >= normal_min
        if not is_micro and not is_normal:
            continue
        tol = price_tolerance(sess.symbol, bar.high - bar.low, params, micro=is_micro)
        if abs(l1 - l2) > tol:
            continue
        between_high = max(b.high for b in sess.bars[j : i + 1])
        rally = between_high - max(l1, l2)
        if rally < min_bounce:
            continue
        lk = f"{_low_key(sess.symbol, l1)}|{_low_key(sess.symbol, l2)}"
        v02["used_low_keys"][lk] = int(v02["used_low_keys"].get(lk, 0)) + 1
        if v02["used_low_keys"][lk] > int(params["double_bottom_max_reuse_per_low"]):
            continue
        fam = "MICRO_DOUBLE_BOTTOM" if is_micro else "DOUBLE_BOTTOM"
        key = f"{fam}:{lk}"
        if any(p.relevant_prices.get("key") == key for p in sess.patterns.values()):
            continue
        lifecycle = LIFECYCLE_CONFIRMED if bar.close >= between_high - tick_size(sess.symbol) * 2 else LIFECYCLE_POSSIBLE
        pat = PatternRecord(
            pattern_instance_id=str(uuid.uuid4()),
            pattern_family=fam,
            direction="LONG",
            lifecycle=lifecycle,
            start_ts=t1,
            current_ts=bar_ts,
            trading_date=sess.trading_date,
            relevant_prices={
                "key": key,
                "first_low": l1,
                "second_low": l2,
                "neckline": between_high,
                "bars_between": bars_apart,
                "intervening_rally": rally,
                "median_range": med,
            },
            supporting_observation_ids=[obs_id],
            supporting_rule_ids=["DOUBLE_BOTTOM_V0_2"],
            explanation=f"{fam} with min bounce {min_bounce:.2f} and deduped lows (V0.2).",
        )
        pat.lifecycle_history.append({"lifecycle": LIFECYCLE_POSSIBLE, "ts": str(t1), "rule_id": "DOUBLE_BOTTOM_V0_2", "note": ""})
        if lifecycle == LIFECYCLE_CONFIRMED:
            pat.transition(LIFECYCLE_CONFIRMED, ts=bar_ts, rule_id="DOUBLE_BOTTOM_CONFIRM_V0_2", note="")
        _register(pat, sess)
        break


def _breakout_v02(
    sess: SessionPatternState,
    *,
    bar: HistoricalBar,
    bar_ts: datetime,
    objective_obs: dict[str, Any],
    obs_id: str,
    params: dict[str, Any],
) -> None:
    v02 = sess._v02  # type: ignore[attr-defined]
    if not sess.confirmed_swing_highs:
        return
    ref = sess.confirmed_swing_highs[-1]
    level = round(ref["price"], 4)
    last_use = v02["breakout_levels"].get(level)
    if last_use is not None and sess.bar_index - last_use < int(params["breakout_level_reuse_cooldown_bars"]):
        return
    beyond = tick_size(sess.symbol) * int(params["breakout_min_ticks_beyond_swing"])
    if bar.close <= ref["price"] + beyond:
        return
    if "POSSIBLE_BREAKOUT_BAR" not in _term_set(objective_obs):
        return
    v02["breakout_levels"][level] = sess.bar_index
    pat = PatternRecord(
        pattern_instance_id=str(uuid.uuid4()),
        pattern_family="STRUCTURAL_BREAKOUT",
        direction="LONG",
        lifecycle=LIFECYCLE_CONFIRMED,
        start_ts=bar_ts,
        current_ts=bar_ts,
        trading_date=sess.trading_date,
        relevant_prices={"reference": ref["price"], "break_close": bar.close},
        supporting_observation_ids=[obs_id, ref.get("obs_id", obs_id)],
        supporting_rule_ids=["STRUCTURAL_BREAKOUT_V0_2"],
        explanation="Structural breakout above STRUCTURAL_SWING_HIGH (V0.2).",
    )
    pat.lifecycle_history.append({"lifecycle": LIFECYCLE_CONFIRMED, "ts": str(bar_ts), "rule_id": "STRUCTURAL_BREAKOUT_V0_2", "note": ""})
    _register(pat, sess)
    if len(sess.bars) >= 2:
        prev = sess.bars[-2]
        if prev.close > ref["price"] and bar.close < ref["price"]:
            fail = PatternRecord(
                pattern_instance_id=str(uuid.uuid4()),
                pattern_family="FAILED_BREAKOUT",
                direction="LONG",
                lifecycle=LIFECYCLE_FAILED,
                start_ts=prev.ts_utc,
                current_ts=bar_ts,
                trading_date=sess.trading_date,
                relevant_prices={"reference": ref["price"]},
                supporting_observation_ids=[obs_id],
                supporting_rule_ids=["FAILED_BREAKOUT_V0_2"],
                explanation="Failed structural breakout (V0.2).",
            )
            fail.lifecycle_history.append({"lifecycle": LIFECYCLE_FAILED, "ts": str(bar_ts), "rule_id": "FAILED_BREAKOUT_V0_2", "note": ""})
            _register(fail, sess)


def _micro_channel_v02(sess: SessionPatternState, *, bar: HistoricalBar, bar_ts: datetime, obs_id: str, params: dict[str, Any]) -> None:
    v02 = sess._v02  # type: ignore[attr-defined]
    min_len = int(params["micro_channel_min_bars"])
    if len(sess.bars) < min_len:
        return
    recent = sess.bars[-min_len:]
    bull = all(recent[i].low >= recent[i - 1].low for i in range(1, len(recent)))
    if bull:
        pid = v02.get("micro_channel_pat_id")
        if pid and pid in sess.patterns:
            p = sess.patterns[pid]
            p.current_ts = bar_ts
            p.relevant_prices["channel_length"] = len(sess.bars) - _bar_index_at(sess, p.start_ts)
            p.lifecycle_history.append(
                {"lifecycle": LIFECYCLE_DEVELOPING, "ts": str(bar_ts), "rule_id": "MICRO_CHANNEL_EXTEND_V0_2", "note": "extend"}
            )
        else:
            pid = str(uuid.uuid4())
            v02["micro_channel_pat_id"] = pid
            pat = PatternRecord(
                pattern_instance_id=pid,
                pattern_family="BULL_MICRO_CHANNEL",
                direction="LONG",
                lifecycle=LIFECYCLE_DEVELOPING,
                start_ts=recent[0].ts_utc,
                current_ts=bar_ts,
                trading_date=sess.trading_date,
                relevant_prices={"channel_length": min_len},
                supporting_observation_ids=[obs_id],
                supporting_rule_ids=["MICRO_CHANNEL_V0_2"],
                explanation="Bull micro channel (single updating instance, V0.2).",
            )
            pat.lifecycle_history.append({"lifecycle": LIFECYCLE_DEVELOPING, "ts": str(bar_ts), "rule_id": "MICRO_CHANNEL_V0_2", "note": ""})
            _register(pat, sess)
    elif v02.get("micro_channel_pat_id"):
        p = sess.patterns.get(v02["micro_channel_pat_id"])
        if p and p.lifecycle == LIFECYCLE_DEVELOPING:
            p.transition(LIFECYCLE_FAILED, ts=bar_ts, rule_id="MICRO_CHANNEL_BREAK_V0_2", note="")
        v02["micro_channel_pat_id"] = None


def advance_patterns_v02_for_bar(
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
    _ensure_v02_session(sess)
    bar_ts = bar.ts_utc
    sess.bars.append(bar)
    sess.bar_index += 1
    obs_id = _obs_id(run_id, objective_baseline_attempt_id, symbol, bar_ts)

    _expire_stale_v02(sess, bar_ts, params)
    _update_swings_v02(sess, bar=bar, bar_ts=bar_ts, obs_id=obs_id, params=params, run_id=run_id, baseline_attempt=objective_baseline_attempt_id)
    _update_pullback_and_h_v02(sess, bar=bar, bar_ts=bar_ts, objective_obs=objective_obs, obs_id=obs_id, params=params)
    _double_bottoms_v02(sess, bar=bar, bar_ts=bar_ts, obs_id=obs_id, params=params)
    _breakout_v02(sess, bar=bar, bar_ts=bar_ts, objective_obs=objective_obs, obs_id=obs_id, params=params)
    _micro_channel_v02(sess, bar=bar, bar_ts=bar_ts, obs_id=obs_id, params=params)

    # reuse V0.1 wedge/two-leg/climax with v02 rule ids via minimal inline (import would re-run v01 expire)
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
        "context_json": {
            "phase": "PATTERN_BROOKS_V0_2",
            "interpretation": "CONTEXTUAL_INTERPRETATION",
            "note": "V0.2 validation ruleset; not Phase 6 thesis alignment.",
        },
        "action": "WAIT" if any(p.lifecycle == LIFECYCLE_DEVELOPING for p in active) else "OBSERVE",
        "explanation": "V0.2 pattern step.",
        "ruleset_version": RULESET_VERSION,
    }
