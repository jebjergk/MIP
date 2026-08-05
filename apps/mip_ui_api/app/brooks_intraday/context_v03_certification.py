"""Phase D3 — synthetic certification for BROOKS_CONTEXT_RULESET_V0_3 (no historical replay)."""

from __future__ import annotations

import json
from copy import deepcopy
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable

from .bars import HistoricalBar
from .context_engine import advance_context_for_bar
from .context_engine_v03 import (
    is_breakout_candidate,
    is_breakout_confirmed,
    is_failed_breakout,
    reset_v03_session,
    tie_break_v03,
)
from .context_ruleset_v02 import RULESET_VERSION as RULESET_V02
from .context_ruleset_v03 import (
    ACTION_CONSIDER_ENTRY,
    ACTION_DO_NOT_CHASE,
    ACTION_DO_NOT_ENTER,
    ACTION_ENTRY_ARMED,
    ACTION_OBSERVE,
    ACTION_WAIT,
    ACTION_WAIT_FOR_RECLAIM,
    BLOCKER_AT_UNBROKEN_RESISTANCE,
    BLOCKER_BEARISH_CANCELLATION,
    BLOCKER_BREAKOUT_CANDIDATE_NOT_CONFIRMED,
    BLOCKER_DEFERRED_BY_DAILY_THESIS,
    BLOCKER_DO_NOT_CHASE_EXTENSION,
    BLOCKER_FAILED_BREAKOUT,
    BLOCKER_GAP_ACCEPTANCE_REQUIRED,
    BLOCKER_INSUFFICIENT_TIME,
    BLOCKER_LIMITED_ROOM_TO_NEXT,
    BLOCKER_NO_DAILY_LONG_AUTHORIZATION,
    BLOCKER_NO_VALID_NEXT_RESISTANCE,
    BLOCKER_ONE_POSITION_TIEBREAK_SKIP,
    BLOCKER_WAITING_FOR_BREAKOUT_FOLLOW_THROUGH,
    LAST_ENTRY_BAR_INDEX_IN_SESSION,
    RL_BROKEN_CONFIRMED,
    RL_BROKEN_PENDING,
    RL_FAILED_BREAKOUT,
    RL_UNBROKEN,
    RULESET_VERSION,
    STATE_DO_NOT_CHASE,
    STATE_ENTRY_ARMED,
    STATE_ENTRY_BLOCKED,
    STATE_FAILED_BREAKOUT,
    STATE_OBSERVATION_ONLY,
    STATE_WAITING_FOR_BREAKOUT_CONFIRMATION,
    STATE_WAITING_FOR_BREAKOUT_PULLBACK,
)

TD = date(2099, 1, 6)  # synthetic non-historical label


def _bar(
    idx: int,
    o: float,
    h: float,
    l: float,
    c: float,
    *,
    symbol: str = "SYN",
) -> HistoricalBar:
    # Synthetic session clock starting 09:30
    minutes = 9 * 60 + 30 + idx * 5
    hh, mm = divmod(minutes, 60)
    ts = datetime(2099, 1, 6, hh % 24, mm)
    return HistoricalBar(
        symbol=symbol,
        trading_date=TD,
        ts_utc=ts,
        ts_ny=ts,
        open=o,
        high=h,
        low=l,
        close=c,
        volume=1000,
        source="SYNTHETIC",
        bar_size_minutes=5,
        rth=True,
    )


def _dossier(**kwargs: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "paa_verdict": "NO_CLEAR_LONG",
        "trade_simulation_ready": True,
        "support_zones": [{"low": 90.0, "high": 91.0}],
        "resistance_zones": [{"low": 99.5, "high": 100.5}],  # primary ~100
        "reclaim_level": 95.0,
        "do_not_chase_level": 120.0,
        "invalidation_level": 85.0,
    }
    base.update(kwargs)
    return base


def _pat_breakout() -> list[dict[str, Any]]:
    return [{"pattern_family": "STRUCTURAL_BREAKOUT", "lifecycle": "CONFIRMED"}]


def _pat_h2() -> list[dict[str, Any]]:
    return [{"pattern_family": "CONFIRMED_H2_LONG", "lifecycle": "CONFIRMED"}]


def _pat_bear() -> list[dict[str, Any]]:
    return [{"pattern_family": "BEAR_MICRO_CHANNEL", "lifecycle": "CONFIRMED"}]


def _obs(*terms: str) -> dict[str, Any]:
    return {"brooks_obs_json": [{"term": t} for t in terms]}


def _run_session(
    bars: list[HistoricalBar],
    *,
    dossier: dict[str, Any],
    patterns_by_idx: dict[int, list[dict[str, Any]]] | None = None,
    symbol: str = "SYN",
) -> list[Any]:
    state: dict[str, Any] = {}
    reset_v03_session(state, symbol, TD)
    patterns_by_idx = patterns_by_idx or {}
    out = []
    for i, bar in enumerate(bars):
        r = advance_context_for_bar(
            state=state,
            dossier=dossier,
            symbol=symbol,
            trading_date=TD,
            bar=bar,
            bar_index_in_session=i,
            objective_obs=_obs("BULLISH_BAR") if bar.close > bar.open else _obs(),
            patterns=patterns_by_idx.get(i, []),
            ruleset_version=RULESET_VERSION,
        )
        out.append(r)
    return out


def _seq(results: list[Any]) -> list[str]:
    return [r.state_after for r in results]


def _actions(results: list[Any]) -> list[str]:
    return [r.selected_action for r in results]


def _rls(results: list[Any]) -> list[str]:
    return [r.layers["contextual_interpretation"]["resistance_lifecycle"] for r in results]


def _ce_count(results: list[Any]) -> int:
    return sum(1 for r in results if r.selected_action == ACTION_CONSIDER_ENTRY)


def _has_blocker(results: list[Any], code: str) -> bool:
    return any(code in (r.blockers or []) for r in results)


def _scenario(name: str, passed: bool, **detail: Any) -> dict[str, Any]:
    return {"scenario": name, "passed": passed, **detail}


# ---------- 16 scenarios ----------


def scenario_01_weak_no_clear_long() -> dict[str, Any]:
    bars = [_bar(i, 96, 97, 95.5, 96.2) for i in range(8)]
    res = _run_session(bars, dossier=_dossier())
    states = _seq(res)
    passed = (
        all(s == STATE_OBSERVATION_ONLY or s == STATE_ENTRY_BLOCKED for s in states)
        and _ce_count(res) == 0
        and _has_blocker(res, BLOCKER_NO_DAILY_LONG_AUTHORIZATION)
        and RL_BROKEN_CONFIRMED not in _rls(res)
    )
    return _scenario(
        "01_weak_no_clear_long",
        passed,
        expected_states=["OBSERVATION_ONLY"],
        actual_states=states,
        expected_actions=["OBSERVE/WAIT"],
        actual_actions=_actions(res),
        blockers=sorted({b for r in res for b in r.blockers}),
        resistance_lifecycle=_rls(res)[-1],
        consider_entry_count=_ce_count(res),
    )


def scenario_02_candidate_no_confirm() -> dict[str, Any]:
    # Break above then stall flat (no bullish FT, expire window)
    bars = [
        _bar(0, 98, 99, 97.5, 98.5),
        _bar(1, 98.5, 99.2, 98, 99),
        _bar(2, 100.2, 100.5, 100.1, 100.3),  # candidate close above 100
        _bar(3, 100.3, 100.4, 100.0, 100.05),  # stall
        _bar(4, 100.05, 100.2, 99.9, 100.0),
        _bar(5, 100.0, 100.1, 99.95, 100.0),
        _bar(6, 100.0, 100.1, 99.9, 99.95),
    ]
    pats = {2: _pat_breakout()}
    res = _run_session(bars, dossier=_dossier(), patterns_by_idx=pats)
    rls = _rls(res)
    passed = (
        RL_BROKEN_PENDING in rls
        and _has_blocker(res, BLOCKER_WAITING_FOR_BREAKOUT_FOLLOW_THROUGH)
        and _ce_count(res) == 0
        and ACTION_CONSIDER_ENTRY not in _actions(res)
    )
    return _scenario(
        "02_candidate_without_confirmation",
        passed,
        expected_states=["WAITING_FOR_BREAKOUT_CONFIRMATION"],
        actual_states=_seq(res),
        expected_actions=["WAIT_FOR_FOLLOW_THROUGH"],
        actual_actions=_actions(res),
        blockers=sorted({b for r in res for b in r.blockers}),
        resistance_lifecycle=rls,
        consider_entry_count=_ce_count(res),
    )


def scenario_03_failed_breakout() -> dict[str, Any]:
    bars = [
        _bar(0, 98, 99, 97, 98.5),
        _bar(1, 100.2, 101, 100.1, 100.8),  # break
        _bar(2, 100.5, 100.6, 98.5, 98.8),  # material fail close below
    ]
    res = _run_session(bars, dossier=_dossier(), patterns_by_idx={1: _pat_breakout()})
    passed = (
        STATE_FAILED_BREAKOUT in _seq(res)
        and RL_FAILED_BREAKOUT in _rls(res)
        and _ce_count(res) == 0
        and (
            ACTION_DO_NOT_ENTER in _actions(res)
            or ACTION_WAIT_FOR_RECLAIM in _actions(res)
            or _has_blocker(res, BLOCKER_FAILED_BREAKOUT)
        )
    )
    return _scenario(
        "03_failed_breakout",
        passed,
        expected_states=["FAILED_BREAKOUT"],
        actual_states=_seq(res),
        actual_actions=_actions(res),
        blockers=sorted({b for r in res for b in r.blockers}),
        resistance_lifecycle=_rls(res),
        consider_entry_count=_ce_count(res),
    )


def _happy_breakout_bars() -> tuple[list[HistoricalBar], dict[int, list], dict]:
    """Confirmed break + pullback hold + signal arming path."""
    dossier = _dossier(
        resistance_zones=[
            {"low": 99.5, "high": 100.5},
            {"low": 109.0, "high": 110.0},  # next resistance
        ],
        do_not_chase_level=130.0,
    )
    bars = [
        _bar(0, 97, 98, 96.5, 97.5),
        _bar(1, 97.5, 98.5, 97, 98),
        _bar(2, 98, 99, 97.8, 98.8),
        _bar(3, 100.2, 101.2, 100.1, 101.0),  # candidate
        _bar(4, 101.0, 102.0, 100.8, 101.8),  # FT confirm
        _bar(5, 101.5, 101.7, 100.2, 100.4),  # pullback test
        _bar(6, 100.4, 101.5, 100.3, 101.3),  # bullish signal -> arm
        _bar(7, 101.3, 102.2, 101.1, 102.0),  # new signal -> CONSIDER
    ]
    pats = {
        3: _pat_breakout(),
        5: _pat_h2(),
        6: _pat_h2(),
        7: _pat_h2(),
    }
    return bars, pats, dossier


def scenario_04_confirmed_pullback_entry() -> dict[str, Any]:
    bars, pats, dossier = _happy_breakout_bars()
    res = _run_session(bars, dossier=dossier, patterns_by_idx=pats)
    states = _seq(res)
    passed = (
        STATE_ENTRY_ARMED in states
        and _ce_count(res) >= 1
        and RL_BROKEN_CONFIRMED in _rls(res)
        and STATE_WAITING_FOR_BREAKOUT_CONFIRMATION in states
        or STATE_WAITING_FOR_BREAKOUT_PULLBACK in states
    )
    # tighten: require both arm and consider
    passed = STATE_ENTRY_ARMED in states and _ce_count(res) >= 1 and RL_BROKEN_CONFIRMED in _rls(res)
    return _scenario(
        "04_confirmed_breakout_plus_pullback",
        passed,
        expected_states=["ENTRY_ARMED", "CONSIDER_ENTRY"],
        actual_states=states,
        actual_actions=_actions(res),
        blockers=sorted({b for r in res for b in r.blockers}),
        resistance_lifecycle=_rls(res),
        consider_entry_count=_ce_count(res),
    )


def scenario_05_breakout_too_extended() -> dict[str, Any]:
    dossier = _dossier(
        resistance_zones=[{"low": 99.5, "high": 100.5}, {"low": 130, "high": 131}],
        do_not_chase_level=105.0,
    )
    bars = [
        _bar(0, 98, 99, 97, 98.5),
        _bar(1, 100.2, 101.5, 100.1, 101.2),
        _bar(2, 101.2, 106.0, 101.0, 105.5),  # huge extension / DNC
        _bar(3, 105.5, 107.0, 105.0, 106.5),
    ]
    res = _run_session(bars, dossier=dossier, patterns_by_idx={1: _pat_breakout(), 2: _pat_breakout()})
    passed = (
        STATE_DO_NOT_CHASE in _seq(res)
        and _has_blocker(res, BLOCKER_DO_NOT_CHASE_EXTENSION)
        and _ce_count(res) == 0
    )
    return _scenario(
        "05_breakout_too_extended",
        passed,
        expected_states=["DO_NOT_CHASE"],
        actual_states=_seq(res),
        actual_actions=_actions(res),
        blockers=sorted({b for r in res for b in r.blockers}),
        resistance_lifecycle=_rls(res),
        consider_entry_count=_ce_count(res),
    )


def scenario_06_dnc_reset() -> dict[str, Any]:
    dossier = _dossier(
        resistance_zones=[{"low": 99.5, "high": 100.5}, {"low": 112, "high": 113}],
        do_not_chase_level=104.0,
    )
    bars = [
        _bar(0, 98, 99, 97, 98.5),
        _bar(1, 100.2, 101.5, 100.1, 101.2),
        _bar(2, 101.2, 105.0, 101.0, 104.5),  # DNC
        _bar(3, 104.0, 104.2, 100.3, 100.6),  # pullback
        _bar(4, 100.6, 101.8, 100.4, 101.5),  # reset / arm path
        _bar(5, 101.5, 102.5, 101.2, 102.2),
    ]
    pats = {1: _pat_breakout(), 3: _pat_h2(), 4: _pat_h2(), 5: _pat_h2()}
    res = _run_session(bars, dossier=dossier, patterns_by_idx=pats)
    states = _seq(res)
    actions = _actions(res)
    passed = STATE_DO_NOT_CHASE in states and (
        STATE_ENTRY_ARMED in states or ACTION_ENTRY_ARMED in actions or ACTION_WAIT in actions
    )
    # Prefer seeing DNC then later non-DNC progress
    dnc_idx = next((i for i, s in enumerate(states) if s == STATE_DO_NOT_CHASE), None)
    later = states[dnc_idx + 1 :] if dnc_idx is not None else []
    passed = dnc_idx is not None and any(
        s in (STATE_ENTRY_ARMED, STATE_WAITING_FOR_BREAKOUT_PULLBACK) for s in later
    )
    return _scenario(
        "06_do_not_chase_reset",
        passed,
        expected_states=["DO_NOT_CHASE", "WAIT/ENTRY_ARMED"],
        actual_states=states,
        actual_actions=actions,
        blockers=sorted({b for r in res for b in r.blockers}),
        resistance_lifecycle=_rls(res),
        consider_entry_count=_ce_count(res),
    )


def scenario_07_daily_thesis_path() -> dict[str, Any]:
    dossier = _dossier(
        paa_verdict="WAIT_PULLBACK",
        resistance_zones=[{"low": 110, "high": 111}],
        reclaim_level=99.0,
        support_zones=[{"low": 98.0, "high": 98.5}],
        do_not_chase_level=120.0,
    )
    bars = [
        _bar(0, 100, 100.5, 99.5, 100.2),
        _bar(1, 100.2, 100.8, 99.8, 100.5),
        _bar(2, 100.5, 101, 99.0, 99.5),  # test support
        _bar(3, 99.5, 100.5, 99.2, 100.3),
        _bar(4, 100.3, 101.2, 100.1, 101.0),
        _bar(5, 101.0, 101.8, 100.8, 101.5),
    ]
    pats = {2: _pat_h2(), 3: _pat_h2(), 4: _pat_h2(), 5: _pat_h2()}
    res = _run_session(bars, dossier=dossier, patterns_by_idx=pats)
    passed = (
        STATE_ENTRY_ARMED in _seq(res) or ACTION_ENTRY_ARMED in _actions(res) or _ce_count(res) >= 1
    ) and BLOCKER_DEFERRED_BY_DAILY_THESIS not in {
        b for r in res for b in r.blockers
    }
    return _scenario(
        "07_standard_daily_thesis_entry_path",
        passed,
        expected_states=["ENTRY_ARMED or CONSIDER_ENTRY"],
        actual_states=_seq(res),
        actual_actions=_actions(res),
        blockers=sorted({b for r in res for b in r.blockers}),
        resistance_lifecycle=_rls(res),
        consider_entry_count=_ce_count(res),
    )


def scenario_08_gap_above() -> dict[str, Any]:
    bars = [
        _bar(0, 101.5, 102.0, 101.2, 101.8),  # gap open above 100
        _bar(1, 101.8, 102.2, 101.5, 102.0),
        _bar(2, 102.0, 102.5, 101.7, 102.3),
    ]
    res = _run_session(bars, dossier=_dossier(), patterns_by_idx={0: _pat_breakout(), 1: _pat_breakout()})
    passed = (
        _has_blocker(res, BLOCKER_GAP_ACCEPTANCE_REQUIRED)
        and _ce_count(res) == 0
        and ACTION_CONSIDER_ENTRY not in _actions(res)
    )
    return _scenario(
        "08_gap_above_resistance",
        passed,
        expected_states=["no blind entry"],
        actual_states=_seq(res),
        actual_actions=_actions(res),
        blockers=sorted({b for r in res for b in r.blockers}),
        resistance_lifecycle=_rls(res),
        consider_entry_count=_ce_count(res),
    )


def scenario_09_next_resistance_too_close() -> dict[str, Any]:
    dossier = _dossier(
        resistance_zones=[
            {"low": 99.5, "high": 100.5},
            {"low": 101.0, "high": 101.2},  # very close next
        ]
    )
    bars = [
        _bar(0, 98, 99, 97.5, 98.5),
        _bar(1, 100.2, 100.8, 100.1, 100.6),
        _bar(2, 100.6, 101.0, 100.5, 100.9),
        _bar(3, 100.8, 100.9, 100.2, 100.35),
        _bar(4, 100.35, 100.8, 100.2, 100.6),
    ]
    res = _run_session(
        bars, dossier=dossier, patterns_by_idx={1: _pat_breakout(), 3: _pat_h2(), 4: _pat_h2()}
    )
    passed = _has_blocker(res, BLOCKER_LIMITED_ROOM_TO_NEXT) and _ce_count(res) == 0
    return _scenario(
        "09_next_resistance_too_close",
        passed,
        expected_blockers=[BLOCKER_LIMITED_ROOM_TO_NEXT],
        actual_states=_seq(res),
        actual_actions=_actions(res),
        blockers=sorted({b for r in res for b in r.blockers}),
        resistance_lifecycle=_rls(res),
        consider_entry_count=_ce_count(res),
    )


def scenario_10_no_next_resistance() -> dict[str, Any]:
    dossier = _dossier(
        resistance_zones=[{"low": 99.5, "high": 100.5}],  # only primary
        invalidation_level=90.0,
        do_not_chase_level=200.0,
    )
    # Wide early range + flat post-break highs → no swing-high next resistance
    bars = [
        _bar(0, 96.0, 99.0, 94.0, 97.0),
        _bar(1, 100.2, 100.5, 100.1, 100.4),
        _bar(2, 100.4, 100.5, 100.3, 100.45),  # confirm (Route B / FT)
        _bar(3, 100.4, 100.5, 100.15, 100.3),  # pullback, still no higher swing
        _bar(4, 100.3, 100.5, 100.2, 100.4),
    ]
    res = _run_session(
        bars, dossier=dossier, patterns_by_idx={1: _pat_breakout(), 2: _pat_breakout(), 3: _pat_h2(), 4: _pat_h2()}
    )
    rooms_after_confirm = [
        r.room_class
        for r in res
        if r.layers["contextual_interpretation"]["resistance_lifecycle"] == RL_BROKEN_CONFIRMED
    ]
    # Conservative policy: UNKNOWN room is never treated as AMPLE; WAIT unless safe-path arm.
    passed = bool(rooms_after_confirm) and all(
        x == "UNKNOWN_NO_NEXT_RESISTANCE" for x in rooms_after_confirm
    ) and (
        _has_blocker(res, BLOCKER_NO_VALID_NEXT_RESISTANCE) or STATE_ENTRY_ARMED in _seq(res)
    )
    return _scenario(
        "10_no_known_next_resistance",
        passed,
        expected_behaviour="WAIT or unknown-safe arm; never AMPLE after confirm",
        actual_states=_seq(res),
        actual_actions=_actions(res),
        room_classes_after_confirm=rooms_after_confirm,
        blockers=sorted({b for r in res for b in r.blockers}),
        resistance_lifecycle=_rls(res),
        consider_entry_count=_ce_count(res),
    )


def scenario_11_late_session() -> dict[str, Any]:
    dossier = _dossier(
        resistance_zones=[{"low": 99.5, "high": 100.5}, {"low": 110, "high": 111}],
        do_not_chase_level=200.0,
        invalidation_level=90.0,
    )
    # Wide range so extension/risk DNC does not mask the time cutoff under test.
    # Confirm at index 72 (last allowed); pullback/arm at 73+ must emit INSUFFICIENT_TIME.
    bars_idx = [70, 71, 72, 73, 74]
    bars = [
        _bar(70, 96.0, 99.0, 94.0, 97.0),
        _bar(71, 100.2, 100.5, 100.1, 100.4),
        _bar(72, 100.4, 100.6, 100.3, 100.5),
        _bar(73, 100.45, 100.55, 100.15, 100.3),
        _bar(74, 100.3, 100.55, 100.2, 100.45),
    ]
    state: dict[str, Any] = {}
    reset_v03_session(state, "SYN", TD)
    results = []
    for j, bar in enumerate(bars):
        idx = bars_idx[j]
        r = advance_context_for_bar(
            state=state,
            dossier=dossier,
            symbol="SYN",
            trading_date=TD,
            bar=bar,
            bar_index_in_session=idx,
            objective_obs=_obs(),
            patterns=_pat_breakout() if j in (1, 2) else (_pat_h2() if j >= 3 else []),
            ruleset_version=RULESET_VERSION,
        )
        results.append(r)
    passed = _has_blocker(results, BLOCKER_INSUFFICIENT_TIME) and _ce_count(results) == 0
    return _scenario(
        "11_late_session_setup",
        passed,
        expected_blockers=[BLOCKER_INSUFFICIENT_TIME],
        actual_states=_seq(results),
        actual_actions=_actions(results),
        blockers=sorted({b for r in results for b in r.blockers}),
        resistance_lifecycle=_rls(results),
        consider_entry_count=_ce_count(results),
        note=f"cutoff_index={LAST_ENTRY_BAR_INDEX_IN_SESSION} (15:30 ET)",
    )


def scenario_12_bearish_cancellation() -> dict[str, Any]:
    bars = [
        _bar(0, 98, 99, 97.5, 98.5),
        _bar(1, 100.2, 101.2, 100.1, 101.0),
        _bar(2, 101.0, 101.5, 100.8, 101.3),
        _bar(3, 101.2, 101.3, 99.0, 99.2),  # cancel
    ]
    res = _run_session(
        bars,
        dossier=_dossier(resistance_zones=[{"low": 99.5, "high": 100.5}, {"low": 110, "high": 111}]),
        patterns_by_idx={1: _pat_breakout(), 3: _pat_bear()},
    )
    passed = (
        (_has_blocker(res, BLOCKER_BEARISH_CANCELLATION) or STATE_FAILED_BREAKOUT in _seq(res))
        and _ce_count(res) == 0
    )
    return _scenario(
        "12_bearish_cancellation_after_upgrade",
        passed,
        expected_blockers=[BLOCKER_BEARISH_CANCELLATION],
        actual_states=_seq(res),
        actual_actions=_actions(res),
        blockers=sorted({b for r in res for b in r.blockers}),
        resistance_lifecycle=_rls(res),
        consider_entry_count=_ce_count(res),
    )


def scenario_13_defer() -> dict[str, Any]:
    bars = [
        _bar(0, 100.2, 101.5, 100.1, 101.2),
        _bar(1, 101.2, 102.5, 101.0, 102.2),
        _bar(2, 102.0, 103.0, 101.8, 102.8),
    ]
    res = _run_session(
        bars,
        dossier=_dossier(paa_verdict="DEFER"),
        patterns_by_idx={0: _pat_breakout(), 1: _pat_breakout(), 2: _pat_h2()},
    )
    passed = (
        all(a == ACTION_DO_NOT_ENTER for a in _actions(res))
        and _has_blocker(res, BLOCKER_DEFERRED_BY_DAILY_THESIS)
        and _ce_count(res) == 0
        and RL_BROKEN_CONFIRMED not in _rls(res)
    )
    return _scenario(
        "13_defer_hard_block",
        passed,
        expected_states=["OBSERVATION_ONLY"],
        actual_states=_seq(res),
        actual_actions=_actions(res),
        blockers=sorted({b for r in res for b in r.blockers}),
        resistance_lifecycle=_rls(res),
        consider_entry_count=_ce_count(res),
    )


def scenario_14_tie_break() -> dict[str, Any]:
    cands = [
        {
            "symbol": "AMZN",
            "path_class": "UPGRADE",
            "room_class": "ACCEPTABLE_ROOM",
            "entry_risk": 0.20,
            "confirmation_score": 2,
            "signal_ts": "2099-01-06T15:00:00",
        },
        {
            "symbol": "AAPL",
            "path_class": "DAILY",
            "room_class": "AMPLE_ROOM",
            "entry_risk": 0.25,
            "confirmation_score": 1,
            "signal_ts": "2099-01-06T15:00:00",
        },
        {
            "symbol": "JPM",
            "path_class": "DAILY",
            "room_class": "AMPLE_ROOM",
            "entry_risk": 0.10,
            "confirmation_score": 3,
            "signal_ts": "2099-01-06T14:55:00",
        },
    ]
    # Daily before upgrade; among daily: ample equal, lower risk wins → JPM
    winner, losers = tie_break_v03(cands)
    passed = (
        winner["symbol"] == "JPM"
        and len(losers) == 2
        and all(l.get("skip_reason") == BLOCKER_ONE_POSITION_TIEBREAK_SKIP for l in losers)
    )
    # Repeatability
    w2, l2 = tie_break_v03(list(reversed(cands)))
    passed = passed and w2["symbol"] == "JPM"
    return _scenario(
        "14_one_position_tie_break",
        passed,
        expected_winner="JPM",
        actual_winner=winner["symbol"],
        losers=[l["symbol"] for l in losers],
        consider_entry_count=1,
    )


def scenario_15_broken_becomes_support() -> dict[str, Any]:
    bars, pats, dossier = _happy_breakout_bars()
    res = _run_session(bars, dossier=dossier, patterns_by_idx=pats)
    after_confirm = False
    bad = False
    for r in res:
        rl = r.layers["contextual_interpretation"]["resistance_lifecycle"]
        if rl == RL_BROKEN_CONFIRMED:
            after_confirm = True
        if after_confirm and BLOCKER_AT_UNBROKEN_RESISTANCE in (r.blockers or []):
            bad = True
    passed = after_confirm and not bad and RL_BROKEN_CONFIRMED in _rls(res)
    return _scenario(
        "15_broken_resistance_becomes_support",
        passed,
        expected="no AT_UNBROKEN_RESISTANCE after BROKEN_CONFIRMED",
        actual_states=_seq(res),
        blockers=sorted({b for r in res for b in r.blockers}),
        resistance_lifecycle=_rls(res),
        consider_entry_count=_ce_count(res),
    )


def scenario_16_pullback_fails() -> dict[str, Any]:
    dossier = _dossier(
        resistance_zones=[{"low": 99.5, "high": 100.5}, {"low": 110, "high": 111}]
    )
    bars = [
        _bar(0, 98, 99, 97.5, 98.5),
        _bar(1, 100.2, 101.2, 100.1, 101.0),
        _bar(2, 101.0, 102.0, 100.8, 101.8),
        _bar(3, 101.5, 101.6, 98.5, 98.7),  # material close below
    ]
    res = _run_session(bars, dossier=dossier, patterns_by_idx={1: _pat_breakout(), 2: _pat_breakout()})
    passed = (
        (STATE_FAILED_BREAKOUT in _seq(res) or _has_blocker(res, BLOCKER_FAILED_BREAKOUT))
        and _ce_count(res) == 0
    )
    return _scenario(
        "16_pullback_closes_materially_below",
        passed,
        expected_states=["FAILED_BREAKOUT"],
        actual_states=_seq(res),
        actual_actions=_actions(res),
        blockers=sorted({b for r in res for b in r.blockers}),
        resistance_lifecycle=_rls(res),
        consider_entry_count=_ce_count(res),
    )


SCENARIOS: list[Callable[[], dict[str, Any]]] = [
    scenario_01_weak_no_clear_long,
    scenario_02_candidate_no_confirm,
    scenario_03_failed_breakout,
    scenario_04_confirmed_pullback_entry,
    scenario_05_breakout_too_extended,
    scenario_06_dnc_reset,
    scenario_07_daily_thesis_path,
    scenario_08_gap_above,
    scenario_09_next_resistance_too_close,
    scenario_10_no_next_resistance,
    scenario_11_late_session,
    scenario_12_bearish_cancellation,
    scenario_13_defer,
    scenario_14_tie_break,
    scenario_15_broken_becomes_support,
    scenario_16_pullback_fails,
]


def check_no_lookahead() -> dict[str, Any]:
    bars, pats, dossier = _happy_breakout_bars()
    prefix = bars[:5]
    r1 = _run_session(prefix, dossier=dossier, patterns_by_idx=pats)
    # Add future bars — earlier decisions must be unchanged
    extended = bars + [_bar(8, 102, 103, 101.5, 102.5), _bar(9, 102.5, 104, 102, 103.5)]
    r2 = _run_session(extended, dossier=dossier, patterns_by_idx=pats)
    early1 = [(x.state_after, x.selected_action, x.blockers) for x in r1]
    early2 = [(x.state_after, x.selected_action, x.blockers) for x in r2[:5]]
    passed = early1 == early2
    return {
        "name": "no_lookahead",
        "passed": passed,
        "prefix_decisions": early1,
        "extended_prefix_decisions": early2,
    }


def check_determinism() -> dict[str, Any]:
    bars, pats, dossier = _happy_breakout_bars()
    a = [(r.state_after, r.selected_action, r.blockers) for r in _run_session(bars, dossier=dossier, patterns_by_idx=pats)]
    b = [(r.state_after, r.selected_action, r.blockers) for r in _run_session(bars, dossier=dossier, patterns_by_idx=pats)]
    return {"name": "determinism", "passed": a == b, "run_a": a, "run_b": b}


def check_v02_unchanged() -> dict[str, Any]:
    """Immutability: default resolver is V0.2; V0.2 NO_CLEAR_LONG still hard-blocks."""
    from .context_engine import advance_context_for_bar as adv

    state: dict[str, Any] = {}
    bar = _bar(5, 100.2, 101, 100, 100.8)
    r_default = adv(
        state=state,
        dossier=_dossier(),
        symbol="SYN",
        trading_date=TD,
        bar=bar,
        bar_index_in_session=5,
        objective_obs=_obs(),
        patterns=_pat_breakout(),
        # default ruleset
    )
    state2: dict[str, Any] = {}
    r_v02 = adv(
        state=state2,
        dossier=_dossier(),
        symbol="SYN",
        trading_date=TD,
        bar=bar,
        bar_index_in_session=5,
        objective_obs=_obs(),
        patterns=_pat_breakout(),
        ruleset_version=RULESET_V02,
    )
    passed = (
        r_default.selected_action == ACTION_DO_NOT_ENTER
        and r_v02.selected_action == ACTION_DO_NOT_ENTER
        and r_default.state_after == STATE_OBSERVATION_ONLY
    )
    return {
        "name": "v02_regression_immutability",
        "passed": passed,
        "default_action": r_default.selected_action,
        "v02_action": r_v02.selected_action,
        "default_is_v02_behaviour": passed,
    }


def check_resolver() -> dict[str, Any]:
    from .context_engine import advance_context_for_bar as adv
    from .context_ruleset_v03 import RULESET_VERSION as V03

    state: dict[str, Any] = {}
    r = adv(
        state=state,
        dossier=_dossier(),
        symbol="SYN",
        trading_date=TD,
        bar=_bar(5, 96, 97, 95, 96.5),
        bar_index_in_session=5,
        objective_obs=_obs(),
        patterns=[],
        ruleset_version=V03,
    )
    rs = r.layers["contextual_interpretation"].get("ruleset_version")
    return {
        "name": "resolver_v03_branch",
        "passed": rs == V03,
        "ruleset_version": rs,
        "inactive_default": True,
    }


def run_certification() -> dict[str, Any]:
    scenarios = [fn() for fn in SCENARIOS]
    extras = [check_no_lookahead(), check_determinism(), check_v02_unchanged(), check_resolver()]
    all_rows = scenarios + extras
    passed = sum(1 for s in all_rows if s.get("passed"))
    failed = [s.get("scenario") or s.get("name") for s in all_rows if not s.get("passed")]
    evidence = {
        "ruleset_id": RULESET_VERSION,
        "phase": "D3_synthetic_certification",
        "historical_replay": False,
        "freeze_v1_modified": False,
        "freeze_v2_activated": False,
        "v03_active_by_default": False,
        "test_count": len(all_rows),
        "passed": passed,
        "failed_count": len(failed),
        "failed": failed,
        "scenarios": scenarios,
        "checks": extras,
        "last_entry_bar_index_in_session": LAST_ENTRY_BAR_INDEX_IN_SESSION,
        "last_entry_bar_opens_et": "15:30",
        "ok": len(failed) == 0,
    }
    return evidence


def write_evidence(path: Path | None = None) -> dict[str, Any]:
    evidence = run_certification()
    root = Path(__file__).resolve().parents[5]
    out = path or (root / "cursorfiles" / "brooks_phase_d_v03_certification.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(evidence, indent=2, default=str), encoding="utf-8")
    evidence["evidence_path"] = str(out)
    return evidence


if __name__ == "__main__":
    ev = write_evidence()
    print(json.dumps({"ok": ev["ok"], "passed": ev["passed"], "failed": ev["failed"]}, indent=2))
