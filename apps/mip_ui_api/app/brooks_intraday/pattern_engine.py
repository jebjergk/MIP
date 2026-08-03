"""Deterministic stateful Brooks pattern engine (Phase 5) — visible history only."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

from .bars import HistoricalBar
from .pattern_ruleset_v01 import (
    ALLOWED_ACTIONS,
    DEFAULT_PARAMETERS,
    LIFECYCLE_CONFIRMED,
    LIFECYCLE_DEVELOPING,
    LIFECYCLE_EXPIRED,
    LIFECYCLE_FAILED,
    LIFECYCLE_POSSIBLE,
    RULESET_VERSION,
    price_tolerance,
    tick_size,
)


def _obs_id(run_id: str, baseline_attempt: str, symbol: str, bar_ts: datetime) -> str:
    return f"{baseline_attempt}:{symbol}:{str(bar_ts)[:19]}"


def _term_set(objective_obs: dict[str, Any]) -> set[str]:
    return {t.get("term") for t in (objective_obs.get("brooks_obs_json") or []) if isinstance(t, dict)}


def _dm(objective_obs: dict[str, Any]) -> dict[str, Any]:
    return objective_obs.get("derived_metrics_json") or {}


@dataclass
class PatternRecord:
    pattern_instance_id: str
    pattern_family: str
    direction: str
    lifecycle: str
    start_ts: datetime
    current_ts: datetime
    trading_date: date
    relevant_prices: dict[str, Any]
    supporting_observation_ids: list[str]
    supporting_rule_ids: list[str]
    confirmation_rule_id: str | None = None
    failure_rule_id: str | None = None
    expiry_rule_id: str | None = None
    confirmation_ts: datetime | None = None
    failure_ts: datetime | None = None
    expiry_ts: datetime | None = None
    parent_pattern_instance_id: str | None = None
    confidence: float | None = None
    explanation: str = ""
    action: str = "OBSERVE"
    lifecycle_history: list[dict[str, Any]] = field(default_factory=list)

    def transition(
        self,
        lifecycle: str,
        *,
        ts: datetime,
        rule_id: str,
        note: str,
    ) -> None:
        self.lifecycle = lifecycle
        self.current_ts = ts
        self.lifecycle_history.append(
            {"lifecycle": lifecycle, "ts": str(ts), "rule_id": rule_id, "note": note}
        )
        if lifecycle == LIFECYCLE_CONFIRMED:
            self.confirmation_ts = ts
            self.confirmation_rule_id = rule_id
        elif lifecycle == LIFECYCLE_FAILED:
            self.failure_ts = ts
            self.failure_rule_id = rule_id
        elif lifecycle == LIFECYCLE_EXPIRED:
            self.expiry_ts = ts
            self.expiry_rule_id = rule_id


@dataclass
class SessionPatternState:
    symbol: str
    trading_date: date
    bars: list[HistoricalBar] = field(default_factory=list)
    objective_by_ts: dict[str, dict[str, Any]] = field(default_factory=dict)
    patterns: dict[str, PatternRecord] = field(default_factory=dict)
    swing_highs: list[dict[str, Any]] = field(default_factory=list)
    swing_lows: list[dict[str, Any]] = field(default_factory=list)
    confirmed_swing_highs: list[dict[str, Any]] = field(default_factory=list)
    confirmed_swing_lows: list[dict[str, Any]] = field(default_factory=list)
    pullback: dict[str, Any] | None = None
    h1: dict[str, Any] | None = None
    micro_channel: dict[str, Any] | None = None
    wedge: dict[str, Any] | None = None
    two_leg: dict[str, Any] | None = None
    bar_index: int = -1


def get_pattern_session(state: dict[str, Any], symbol: str, trading_date: date) -> SessionPatternState:
    bucket = state.setdefault("_pattern_session_state", {})
    key = f"{symbol.upper()}|{trading_date.isoformat()}"
    if key not in bucket:
        bucket[key] = SessionPatternState(symbol=symbol.upper(), trading_date=trading_date)
    return bucket[key]


def reset_pattern_session(state: dict[str, Any], symbol: str, trading_date: date) -> None:
    bucket = state.setdefault("_pattern_session_state", {})
    bucket[f"{symbol.upper()}|{trading_date.isoformat()}"] = SessionPatternState(
        symbol=symbol.upper(), trading_date=trading_date
    )


def _register(pat: PatternRecord, sess: SessionPatternState) -> PatternRecord:
    sess.patterns[pat.pattern_instance_id] = pat
    return pat


def _expire_stale(sess: SessionPatternState, bar_ts: datetime, params: dict[str, Any]) -> None:
    expiry_bars = int(params["pattern_expiry_bars_default"])
    for pat in list(sess.patterns.values()):
        if pat.lifecycle in (LIFECYCLE_CONFIRMED, LIFECYCLE_FAILED, LIFECYCLE_EXPIRED):
            continue
        age = sess.bar_index - _bar_index_at(sess, pat.start_ts)
        if age > expiry_bars and pat.lifecycle in (LIFECYCLE_POSSIBLE, LIFECYCLE_DEVELOPING):
            pat.transition(LIFECYCLE_EXPIRED, ts=bar_ts, rule_id="PATTERN_EXPIRE_V0_1", note="Max bars without resolution")


def _bar_index_at(sess: SessionPatternState, ts: datetime) -> int:
    key = str(ts)[:19]
    for i, b in enumerate(sess.bars):
        if str(b.ts_utc)[:19] == key:
            return i
    return sess.bar_index


def _update_swings(
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
    left = sess.bars[idx - 1]
    mid = sess.bars[idx - 2] if idx >= 2 else None
    cur = bar
    if mid is None:
        return
    # Confirm swing at `left` when cur is visible (no future beyond cursor)
    if left.high > mid.high and left.high > cur.high:
        rec = {
            "ts": left.ts_utc,
            "price": left.high,
            "confirmed_at": bar_ts,
            "obs_id": _obs_id(run_id, baseline_attempt, sess.symbol, left.ts_utc),
        }
        sess.confirmed_swing_highs.append(rec)
        pat = PatternRecord(
            pattern_instance_id=str(uuid.uuid4()),
            pattern_family="SWING_HIGH",
            direction="NEUTRAL",
            lifecycle=LIFECYCLE_CONFIRMED,
            start_ts=left.ts_utc,
            current_ts=bar_ts,
            trading_date=sess.trading_date,
            relevant_prices={"swing_high": left.high},
            supporting_observation_ids=[rec["obs_id"], obs_id],
            supporting_rule_ids=["SWING_HIGH_CONFIRM_V0_1"],
            confirmation_rule_id="SWING_HIGH_CONFIRM_V0_1",
            explanation="Swing high confirmed once a lower high appears on the right (visible bar only).",
        )
        pat.lifecycle_history.append(
            {
                "lifecycle": LIFECYCLE_POSSIBLE,
                "ts": str(left.ts_utc),
                "rule_id": "SWING_HIGH_CANDIDATE_V0_1",
                "note": "Candidate before right neighbor visible",
            }
        )
        pat.transition(LIFECYCLE_CONFIRMED, ts=bar_ts, rule_id="SWING_HIGH_CONFIRM_V0_1", note="Right bar visible")
        _register(pat, sess)
    elif left.high > mid.high:
        cand = PatternRecord(
            pattern_instance_id=str(uuid.uuid4()),
            pattern_family="SWING_HIGH",
            direction="NEUTRAL",
            lifecycle=LIFECYCLE_POSSIBLE,
            start_ts=left.ts_utc,
            current_ts=bar_ts,
            trading_date=sess.trading_date,
            relevant_prices={"swing_high": left.high},
            supporting_observation_ids=[_obs_id(run_id, baseline_attempt, sess.symbol, left.ts_utc)],
            supporting_rule_ids=["SWING_HIGH_CANDIDATE_V0_1"],
            explanation="Possible swing high; awaiting visible confirmation bar.",
        )
        cand.lifecycle_history.append(
            {"lifecycle": LIFECYCLE_POSSIBLE, "ts": str(left.ts_utc), "rule_id": "SWING_HIGH_CANDIDATE_V0_1", "note": ""}
        )
        _register(cand, sess)

    if left.low < mid.low and left.low < cur.low:
        rec = {
            "ts": left.ts_utc,
            "price": left.low,
            "confirmed_at": bar_ts,
            "obs_id": _obs_id(run_id, baseline_attempt, sess.symbol, left.ts_utc),
        }
        sess.confirmed_swing_lows.append(rec)
        pat = PatternRecord(
            pattern_instance_id=str(uuid.uuid4()),
            pattern_family="SWING_LOW",
            direction="NEUTRAL",
            lifecycle=LIFECYCLE_CONFIRMED,
            start_ts=left.ts_utc,
            current_ts=bar_ts,
            trading_date=sess.trading_date,
            relevant_prices={"swing_low": left.low},
            supporting_observation_ids=[rec["obs_id"], obs_id],
            supporting_rule_ids=["SWING_LOW_CONFIRM_V0_1"],
            confirmation_rule_id="SWING_LOW_CONFIRM_V0_1",
            explanation="Swing low confirmed once a higher low appears on the right (visible bar only).",
        )
        pat.lifecycle_history.append(
            {
                "lifecycle": LIFECYCLE_POSSIBLE,
                "ts": str(left.ts_utc),
                "rule_id": "SWING_LOW_CANDIDATE_V0_1",
                "note": "Candidate before right neighbor visible",
            }
        )
        pat.transition(LIFECYCLE_CONFIRMED, ts=bar_ts, rule_id="SWING_LOW_CONFIRM_V0_1", note="Right bar visible")
        _register(pat, sess)


def _update_pullback_and_h(
    sess: SessionPatternState,
    *,
    bar: HistoricalBar,
    bar_ts: datetime,
    objective_obs: dict[str, Any],
    obs_id: str,
    params: dict[str, Any],
) -> None:
    terms = _term_set(objective_obs)
    dm = _dm(objective_obs)
    direction = dm.get("direction")
    bull = direction == "BULLISH" or "BULL_BAR" in terms
    bear = direction == "BEARISH" or "BEAR_BAR" in terms

    pb = sess.pullback
    if pb is None and bear and sess.bar_index >= 2:
        # start pullback in bull context after recent higher lows
        recent = sess.bars[-3:]
        if recent[-1].close < recent[-2].close:
            sess.pullback = {
                "start_ts": bar_ts,
                "start_index": sess.bar_index,
                "extreme_low": bar.low,
                "legs": 1,
                "attempts": 0,
            }
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
                supporting_rule_ids=["PULLBACK_START_V0_1"],
                explanation="Countertrend pullback leg after upward context.",
            )
            pat.lifecycle_history.append(
                {"lifecycle": LIFECYCLE_DEVELOPING, "ts": str(bar_ts), "rule_id": "PULLBACK_START_V0_1", "note": ""}
            )
            _register(pat, sess)
            pb = sess.pullback

    if pb:
        pb["extreme_low"] = min(pb.get("extreme_low", bar.low), bar.low)
        if bull and bar.close > bar.open:
            pb["attempts"] = int(pb.get("attempts", 0)) + 1
            attempt_n = pb["attempts"]
            if attempt_n == 1 and sess.h1 is None:
                sess.h1 = {"start_ts": bar_ts, "status": "POSSIBLE", "bars_since": 0}
                fam = "POSSIBLE_H1_LONG"
                pat = PatternRecord(
                    pattern_instance_id=str(uuid.uuid4()),
                    pattern_family=fam,
                    direction="LONG",
                    lifecycle=LIFECYCLE_POSSIBLE,
                    start_ts=bar_ts,
                    current_ts=bar_ts,
                    trading_date=sess.trading_date,
                    relevant_prices={"entry_zone": bar.close, "pullback_low": pb["extreme_low"]},
                    supporting_observation_ids=[obs_id],
                    supporting_rule_ids=["H1_CANDIDATE_V0_1"],
                    explanation=(
                        "Possible H1 long candidate: first upward resumption attempt after an active pullback. "
                        "Not evaluated against daily PAA thesis (Phase 6)."
                    ),
                )
                pat.lifecycle_history.append(
                    {"lifecycle": LIFECYCLE_POSSIBLE, "ts": str(bar_ts), "rule_id": "H1_CANDIDATE_V0_1", "note": ""}
                )
                _register(pat, sess)
            elif attempt_n == 1 and sess.h1 and sess.h1.get("status") == "POSSIBLE":
                if bar.close > pb["extreme_low"] + tick_size(sess.symbol) * 2:
                    sess.h1["status"] = "CONFIRMED"
                    for p in sess.patterns.values():
                        if p.pattern_family == "POSSIBLE_H1_LONG" and p.lifecycle == LIFECYCLE_POSSIBLE:
                            p.transition(
                                LIFECYCLE_CONFIRMED,
                                ts=bar_ts,
                                rule_id="H1_CONFIRM_V0_1",
                                note="Continuation above pullback extreme",
                            )
                            p.pattern_family = "CONFIRMED_H1_LONG_CANDIDATE"
            elif attempt_n >= 2 and sess.h1 and sess.h1.get("status") in ("FAILED", "CONFIRMED"):
                fam = "POSSIBLE_H2_LONG"
                if not any(p.pattern_family == fam and p.lifecycle == LIFECYCLE_POSSIBLE for p in sess.patterns.values()):
                    pat = PatternRecord(
                        pattern_instance_id=str(uuid.uuid4()),
                        pattern_family=fam,
                        direction="LONG",
                        lifecycle=LIFECYCLE_POSSIBLE,
                        start_ts=bar_ts,
                        current_ts=bar_ts,
                        trading_date=sess.trading_date,
                        relevant_prices={"second_attempt": bar.close, "pullback_low": pb["extreme_low"]},
                        supporting_observation_ids=[obs_id],
                        supporting_rule_ids=["H2_CANDIDATE_V0_1"],
                        parent_pattern_instance_id=next(
                            (p.pattern_instance_id for p in sess.patterns.values() if "H1" in p.pattern_family),
                            None,
                        ),
                        explanation=(
                            "Possible H2 long candidate: second upward attempt after prior H1 did not sustain. "
                            "Context-neutral in Phase 5."
                        ),
                    )
                    pat.lifecycle_history.append(
                        {"lifecycle": LIFECYCLE_POSSIBLE, "ts": str(bar_ts), "rule_id": "H2_CANDIDATE_V0_1", "note": ""}
                    )
                    _register(pat, sess)
        if bear and sess.h1 and sess.h1.get("status") == "POSSIBLE":
            sess.h1["status"] = "FAILED"
            for p in sess.patterns.values():
                if p.pattern_family == "POSSIBLE_H1_LONG":
                    p.transition(LIFECYCLE_FAILED, ts=bar_ts, rule_id="H1_FAIL_V0_1", note="Bear bar during H1 attempt")
                    p.pattern_family = "FAILED_H1_LONG"
        age = sess.bar_index - pb.get("start_index", sess.bar_index)
        if age > int(params["h1_h2_expiry_bars"]):
            sess.pullback = None
            sess.h1 = None


def _double_bottoms(
    sess: SessionPatternState,
    *,
    bar: HistoricalBar,
    bar_ts: datetime,
    obs_id: str,
    params: dict[str, Any],
) -> None:
    lows = [(b.low, b.ts_utc) for b in sess.bars]
    if len(lows) < 4:
        return
    i = len(lows) - 1
    for j in range(max(0, i - int(params["double_bottom_max_bars_apart"])), i - int(params["double_bottom_min_bars_apart"])):
        l1, t1 = lows[j]
        l2, t2 = lows[i]
        tol = price_tolerance(sess.symbol, bar.high - bar.low, params, micro=False)
        micro_tol = price_tolerance(sess.symbol, bar.high - bar.low, params, micro=True)
        bars_apart = i - j
        if abs(l1 - l2) <= tol and bars_apart >= int(params["double_bottom_min_bars_apart"]):
            between_high = max(b.high for b in sess.bars[j : i + 1])
            fam = "MICRO_DOUBLE_BOTTOM" if bars_apart <= int(params["micro_double_bottom_max_bars_apart"]) else "DOUBLE_BOTTOM"
            tol_used = micro_tol if fam == "MICRO_DOUBLE_BOTTOM" else tol
            if abs(l1 - l2) > tol_used:
                continue
            key = f"{fam}:{str(t1)[:19]}:{str(t2)[:19]}"
            if any(p.pattern_family == fam and p.relevant_prices.get("key") == key for p in sess.patterns.values()):
                continue
            lifecycle = LIFECYCLE_POSSIBLE
            if bar.close > between_high - tick_size(sess.symbol):
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
                    "key": key,
                    "first_low": l1,
                    "second_low": l2,
                    "neckline": between_high,
                    "bars_between": bars_apart,
                },
                supporting_observation_ids=[obs_id],
                supporting_rule_ids=["DOUBLE_BOTTOM_V0_1"],
                explanation=f"{fam.replace('_', ' ').title()} candidate with lows within tolerance and neckline at intervening high.",
            )
            pat.lifecycle_history.append(
                {"lifecycle": LIFECYCLE_POSSIBLE, "ts": str(t1), "rule_id": "DOUBLE_BOTTOM_V0_1", "note": "First low"}
            )
            if lifecycle == LIFECYCLE_CONFIRMED:
                pat.transition(LIFECYCLE_CONFIRMED, ts=bar_ts, rule_id="DOUBLE_BOTTOM_CONFIRM_V0_1", note="Close near neckline")
            _register(pat, sess)
            break


def _wedge_three_push(
    sess: SessionPatternState,
    *,
    bar: HistoricalBar,
    bar_ts: datetime,
    obs_id: str,
    params: dict[str, Any],
) -> None:
    if len(sess.bars) < 7:
        return
    lows = [b.low for b in sess.bars[-7:]]
    # three pushes down with diminishing progress
    p1 = lows[1] - lows[0]
    p2 = lows[3] - lows[2]
    p3 = lows[5] - lows[4]
    if p1 >= 0 or p2 >= 0 or p3 >= 0:
        return
    if not (abs(p3) <= abs(p2) * float(params["wedge_diminish_min_ratio"])):
        return
    if any(p.pattern_family.startswith("WEDGE") and p.lifecycle in (LIFECYCLE_POSSIBLE, LIFECYCLE_DEVELOPING) for p in sess.patterns.values()):
        return
    pat = PatternRecord(
        pattern_instance_id=str(uuid.uuid4()),
        pattern_family="WEDGE_BOTTOM",
        direction="LONG",
        lifecycle=LIFECYCLE_DEVELOPING,
        start_ts=sess.bars[-7].ts_utc,
        current_ts=bar_ts,
        trading_date=sess.trading_date,
        relevant_prices={"push1": lows[1], "push2": lows[3], "push3": lows[5]},
        supporting_observation_ids=[obs_id],
        supporting_rule_ids=["WEDGE_THREE_PUSH_V0_1"],
        explanation="Developing wedge bottom: three pushes with diminishing downside progress (deterministic V0_1).",
    )
    pat.lifecycle_history.append(
        {"lifecycle": LIFECYCLE_DEVELOPING, "ts": str(bar_ts), "rule_id": "WEDGE_THREE_PUSH_V0_1", "note": ""}
    )
    if bar.close > bar.open and bar.close > lows[-2]:
        pat.transition(LIFECYCLE_CONFIRMED, ts=bar_ts, rule_id="WEDGE_CONFIRM_V0_1", note="Bull resumption")
        pat.pattern_family = "CONFIRMED_WEDGE_BOTTOM"
    _register(pat, sess)


def _two_leg_pullback(
    sess: SessionPatternState,
    *,
    bar: HistoricalBar,
    bar_ts: datetime,
    obs_id: str,
    params: dict[str, Any],
) -> None:
    min_leg = int(params["two_leg_min_leg_bars"])
    if len(sess.bars) < min_leg * 2 + 2:
        return
    window = sess.bars[-(min_leg * 2 + 2) :]
    leg1 = window[:min_leg]
    leg2 = window[min_leg + 1 : min_leg + 1 + min_leg]
    if not leg1 or not leg2:
        return
    move1 = leg1[0].close - leg1[-1].close
    move2 = leg2[0].close - leg2[-1].close
    min_ticks = int(params["two_leg_min_counter_move_ticks"]) * tick_size(sess.symbol)
    if move1 < min_ticks or move2 < min_ticks:
        return
    pat = PatternRecord(
        pattern_instance_id=str(uuid.uuid4()),
        pattern_family="TWO_LEGGED_PULLBACK",
        direction="BEARISH",
        lifecycle=LIFECYCLE_POSSIBLE,
        start_ts=leg1[0].ts_utc,
        current_ts=bar_ts,
        trading_date=sess.trading_date,
        relevant_prices={"leg1_start": leg1[0].close, "leg2_end": leg2[-1].close},
        supporting_observation_ids=[obs_id],
        supporting_rule_ids=["TWO_LEG_PULLBACK_V0_1"],
        explanation="Two-legged pullback candidate with minimum countertrend progress on both legs.",
    )
    pat.lifecycle_history.append(
        {"lifecycle": LIFECYCLE_POSSIBLE, "ts": str(bar_ts), "rule_id": "TWO_LEG_PULLBACK_V0_1", "note": ""}
    )
    _register(pat, sess)


def _breakout_patterns(
    sess: SessionPatternState,
    *,
    bar: HistoricalBar,
    bar_ts: datetime,
    objective_obs: dict[str, Any],
    obs_id: str,
    params: dict[str, Any],
) -> None:
    if not sess.confirmed_swing_highs:
        return
    ref = sess.confirmed_swing_highs[-1]
    beyond = tick_size(sess.symbol) * int(params["breakout_min_ticks_beyond_swing"])
    if bar.close <= ref["price"] + beyond:
        return
    if "POSSIBLE_BREAKOUT_BAR" not in _term_set(objective_obs):
        return
    pat = PatternRecord(
        pattern_instance_id=str(uuid.uuid4()),
        pattern_family="STRUCTURAL_BREAKOUT",
        direction="LONG",
        lifecycle=LIFECYCLE_CONFIRMED,
        start_ts=bar_ts,
        current_ts=bar_ts,
        trading_date=sess.trading_date,
        relevant_prices={"reference_swing_high": ref["price"], "break_close": bar.close},
        supporting_observation_ids=[obs_id, ref.get("obs_id", obs_id)],
        supporting_rule_ids=["STRUCTURAL_BREAKOUT_V0_1", "OBJECTIVE_POSSIBLE_BREAKOUT"],
        explanation=(
            "Confirmed local breakout above a confirmed swing high. "
            "Phase 4 POSSIBLE_BREAKOUT alone is not sufficient; structural reference required."
        ),
    )
    pat.lifecycle_history.append(
        {"lifecycle": LIFECYCLE_POSSIBLE, "ts": str(bar_ts), "rule_id": "STRUCTURAL_BREAKOUT_V0_1", "note": ""}
    )
    pat.transition(LIFECYCLE_CONFIRMED, ts=bar_ts, rule_id="STRUCTURAL_BREAKOUT_CONFIRM_V0_1", note="Close beyond swing")
    _register(pat, sess)

    # failed breakout watch
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
                supporting_rule_ids=["FAILED_BREAKOUT_V0_1"],
                explanation="Failed breakout: re-entry below structural reference within visible bars.",
            )
            fail.lifecycle_history.append(
                {"lifecycle": LIFECYCLE_FAILED, "ts": str(bar_ts), "rule_id": "FAILED_BREAKOUT_V0_1", "note": ""}
            )
            _register(fail, sess)


def _micro_channel(
    sess: SessionPatternState,
    *,
    bar: HistoricalBar,
    bar_ts: datetime,
    obs_id: str,
    params: dict[str, Any],
) -> None:
    min_len = int(params["micro_channel_min_bars"])
    if len(sess.bars) < min_len:
        return
    recent = sess.bars[-min_len:]
    bull = all(recent[i].low >= recent[i - 1].low for i in range(1, len(recent)))
    bear = all(recent[i].high <= recent[i - 1].high for i in range(1, len(recent)))
    if bull:
        fam = "BULL_MICRO_CHANNEL"
        if sess.micro_channel and sess.micro_channel.get("type") == fam:
            sess.micro_channel["length"] += 1
        else:
            sess.micro_channel = {"type": fam, "start": recent[0].ts_utc, "length": min_len}
            pat = PatternRecord(
                pattern_instance_id=str(uuid.uuid4()),
                pattern_family=fam,
                direction="LONG",
                lifecycle=LIFECYCLE_DEVELOPING,
                start_ts=recent[0].ts_utc,
                current_ts=bar_ts,
                trading_date=sess.trading_date,
                relevant_prices={"channel_low": min(b.low for b in recent)},
                supporting_observation_ids=[obs_id],
                supporting_rule_ids=["MICRO_CHANNEL_V0_1"],
                explanation="Bull micro channel: consecutive bars without breaking prior low.",
            )
            pat.lifecycle_history.append(
                {"lifecycle": LIFECYCLE_DEVELOPING, "ts": str(bar_ts), "rule_id": "MICRO_CHANNEL_V0_1", "note": ""}
            )
            _register(pat, sess)
    elif bear:
        fam = "BEAR_MICRO_CHANNEL"
        pat = PatternRecord(
            pattern_instance_id=str(uuid.uuid4()),
            pattern_family=fam,
            direction="BEARISH",
            lifecycle=LIFECYCLE_DEVELOPING,
            start_ts=recent[0].ts_utc,
            current_ts=bar_ts,
            trading_date=sess.trading_date,
            relevant_prices={"channel_high": max(b.high for b in recent)},
            supporting_observation_ids=[obs_id],
            supporting_rule_ids=["MICRO_CHANNEL_V0_1"],
            explanation="Bear micro channel (context/risk observation; no short authority in Phase 5).",
        )
        pat.lifecycle_history.append(
            {"lifecycle": LIFECYCLE_DEVELOPING, "ts": str(bar_ts), "rule_id": "MICRO_CHANNEL_V0_1", "note": ""}
        )
        _register(pat, sess)
    elif sess.micro_channel:
        for p in sess.patterns.values():
            if p.pattern_family == sess.micro_channel.get("type") and p.lifecycle == LIFECYCLE_DEVELOPING:
                p.transition(LIFECYCLE_FAILED, ts=bar_ts, rule_id="MICRO_CHANNEL_BREAK_V0_1", note="Channel broken")
        sess.micro_channel = None


def _climax(
    sess: SessionPatternState,
    *,
    bar: HistoricalBar,
    bar_ts: datetime,
    objective_obs: dict[str, Any],
    obs_id: str,
    params: dict[str, Any],
) -> None:
    terms = _term_set(objective_obs)
    dm = _dm(objective_obs)
    n = int(params["climax_min_consecutive_dir_bars"])
    bulls = sum(1 for b in sess.bars[-n:] if b.close >= b.open) if len(sess.bars) >= n else 0
    bears = sum(1 for b in sess.bars[-n:] if b.close < b.open) if len(sess.bars) >= n else 0
    large = dm.get("relative_range_class") in ("LARGE", "VERY_LARGE")
    if bulls >= n and large and ("CLOSE_NEAR_HIGH" in terms or dm.get("close_location", 0) > 0.75):
        pat = PatternRecord(
            pattern_instance_id=str(uuid.uuid4()),
            pattern_family="POSSIBLE_BUY_CLIMAX",
            direction="LONG",
            lifecycle=LIFECYCLE_POSSIBLE,
            start_ts=bar_ts,
            current_ts=bar_ts,
            trading_date=sess.trading_date,
            relevant_prices={"close": bar.close},
            supporting_observation_ids=[obs_id],
            supporting_rule_ids=["CLIMAX_BUY_V0_1"],
            explanation="Possible buy climax: consecutive bull bars with large range and close near extreme (no reversal call).",
        )
        pat.lifecycle_history.append(
            {"lifecycle": LIFECYCLE_POSSIBLE, "ts": str(bar_ts), "rule_id": "CLIMAX_BUY_V0_1", "note": ""}
        )
        _register(pat, sess)
    if bears >= n and large and ("CLOSE_NEAR_LOW" in terms or dm.get("close_location", 1) < 0.25):
        pat = PatternRecord(
            pattern_instance_id=str(uuid.uuid4()),
            pattern_family="POSSIBLE_SELL_CLIMAX",
            direction="BEARISH",
            lifecycle=LIFECYCLE_POSSIBLE,
            start_ts=bar_ts,
            current_ts=bar_ts,
            trading_date=sess.trading_date,
            relevant_prices={"close": bar.close},
            supporting_observation_ids=[obs_id],
            supporting_rule_ids=["CLIMAX_SELL_V0_1"],
            explanation="Possible sell climax (context only; no short authority).",
        )
        pat.lifecycle_history.append(
            {"lifecycle": LIFECYCLE_POSSIBLE, "ts": str(bar_ts), "rule_id": "CLIMAX_SELL_V0_1", "note": ""}
        )
        _register(pat, sess)


def advance_patterns_for_bar(
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
    """Run pattern engine for one symbol/bar using Phase 4 objective observation (read-only)."""
    params = dict(DEFAULT_PARAMETERS if params is None else {**DEFAULT_PARAMETERS, **params})
    sess = get_pattern_session(state, symbol, trading_date)
    bar_ts = bar.ts_utc
    sess.bars.append(bar)
    sess.bar_index += 1
    sess.objective_by_ts[str(bar_ts)[:19]] = objective_obs
    obs_id = _obs_id(run_id, objective_baseline_attempt_id, symbol, bar_ts)

    _expire_stale(sess, bar_ts, params)
    _update_swings(sess, bar=bar, bar_ts=bar_ts, obs_id=obs_id, params=params, run_id=run_id, baseline_attempt=objective_baseline_attempt_id)
    _update_pullback_and_h(sess, bar=bar, bar_ts=bar_ts, objective_obs=objective_obs, obs_id=obs_id, params=params)
    _double_bottoms(sess, bar=bar, bar_ts=bar_ts, obs_id=obs_id, params=params)
    _wedge_three_push(sess, bar=bar, bar_ts=bar_ts, obs_id=obs_id, params=params)
    if sess.bar_index % 5 == 0:
        _two_leg_pullback(sess, bar=bar, bar_ts=bar_ts, obs_id=obs_id, params=params)
    _breakout_patterns(sess, bar=bar, bar_ts=bar_ts, objective_obs=objective_obs, obs_id=obs_id, params=params)
    _micro_channel(sess, bar=bar, bar_ts=bar_ts, obs_id=obs_id, params=params)
    _climax(sess, bar=bar, bar_ts=bar_ts, objective_obs=objective_obs, obs_id=obs_id, params=params)

    active = [
        p
        for p in sess.patterns.values()
        if p.current_ts == bar_ts or (p.lifecycle in (LIFECYCLE_POSSIBLE, LIFECYCLE_DEVELOPING) and p.start_ts <= bar_ts)
    ]
    active_ids = [p.pattern_instance_id for p in active[-20:]]

    contextual = {
        "phase": "PATTERN_BROOKS_V0_1",
        "interpretation": "CONTEXTUAL_INTERPRETATION",
        "note": "Not assessed against daily support, resistance, or PAA thesis until Phase 6.",
    }
    action = "WAIT" if any(p.lifecycle == LIFECYCLE_DEVELOPING for p in active) else "OBSERVE"
    if action not in ALLOWED_ACTIONS:
        action = "OBSERVE"

    explanations = [p.explanation for p in active if p.explanation][-3:]
    explanation = explanations[-1] if explanations else "No new pattern state change."

    return {
        "pattern_instances_created_or_updated": [p for p in sess.patterns.values() if p.current_ts == bar_ts],
        "active_pattern_instance_ids": active_ids,
        "pattern_snapshot": [
            {
                "pattern_instance_id": p.pattern_instance_id,
                "pattern_family": p.pattern_family,
                "lifecycle": p.lifecycle,
                "direction": p.direction,
            }
            for p in active
        ],
        "context_json": contextual,
        "action": action,
        "explanation": explanation,
    }


def pattern_instances_for_session(state: dict[str, Any], symbol: str, trading_date: date) -> list[PatternRecord]:
    sess = get_pattern_session(state, symbol, trading_date)
    return list(sess.patterns.values())
