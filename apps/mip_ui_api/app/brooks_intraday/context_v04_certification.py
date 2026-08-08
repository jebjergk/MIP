"""Certification scenarios for BROOKS_CONTEXT_RULESET_V0_4."""

from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime
from typing import Any, Callable

from .bars import HistoricalBar
from .context_engine import advance_context_for_bar
from .context_engine_v04 import reset_v04_session
from .context_ruleset_v03 import (
    ACTION_CONSIDER_ENTRY,
    ACTION_DO_NOT_ENTER,
    ACTION_ENTRY_ARMED,
    BLOCKER_DEFERRED_BY_DAILY_THESIS,
    BLOCKER_LIMITED_ROOM_TO_NEXT,
    RULESET_VERSION as V03_RS,
)
from .context_ruleset_v04 import (
    BLOCKER_CHAOTIC_VOLATILITY,
    BLOCKER_COMPRESSION_NO_BREAKOUT,
    BLOCKER_POSSIBLE_PATTERN_ONLY,
    PATH_B_H2_TWO_LEG,
    RULESET_VERSION,
    THESIS_QUALIFIED,
    VOL_HIGH_CHAOTIC,
    daily_bias_class,
    effective_verdict_for_v03_engine,
)
from .context_volatility_v04 import VolatilitySessionState, update_volatility, classify_regime

TD = date(2099, 2, 3)


def _bar(idx: int, o: float, h: float, l: float, c: float) -> HistoricalBar:
    minutes = 9 * 60 + 30 + idx * 5
    hh, mm = divmod(minutes, 60)
    ts = datetime(2099, 2, 3, hh % 24, mm)
    return HistoricalBar(
        symbol="SYN",
        trading_date=TD,
        ts_utc=ts,
        ts_ny=ts,
        open=o,
        high=h,
        low=l,
        close=c,
        volume=1000,
        source="SYN",
        bar_size_minutes=5,
        rth=True,
    )


def _dossier(**kw: Any) -> dict[str, Any]:
    base = {
        "paa_verdict": "NO_CLEAR_LONG",
        "trade_simulation_ready": True,
        "support_zones": [{"low": 90.0, "high": 91.0}],
        "resistance_zones": [{"low": 99.5, "high": 100.5}],
        "reclaim_level": 95.0,
        "invalidation_level": 85.0,
    }
    base.update(kw)
    return base


def _run(
    bars: list[HistoricalBar],
    *,
    dossier: dict[str, Any],
    patterns_by_idx: dict[int, list[dict[str, Any]]] | None = None,
    obs_by_idx: dict[int, dict[str, Any]] | None = None,
    ruleset: str = RULESET_VERSION,
) -> list[Any]:
    state: dict[str, Any] = {}
    reset_v04_session(state, "SYN", TD)
    patterns_by_idx = patterns_by_idx or {}
    obs_by_idx = obs_by_idx or {}
    out = []
    for i, bar in enumerate(bars):
        obs = obs_by_idx.get(i) or {"brooks_obs_json": []}
        r = advance_context_for_bar(
            state=state,
            dossier=dossier,
            symbol="SYN",
            trading_date=TD,
            bar=bar,
            bar_index_in_session=i,
            objective_obs=obs,
            patterns=patterns_by_idx.get(i, []),
            ruleset_version=ruleset,
        )
        out.append(r)
    return out


def _has_blocker(res: Any, code: str) -> bool:
    return code in (res.blockers or [])


def scenario_defer_not_veto() -> dict[str, Any]:
    bars = [_bar(i, 96 + i * 0.1, 96.5 + i * 0.1, 95.8, 96.2 + i * 0.1) for i in range(8)]
    obs = {7: {"brooks_obs_json": [{"term": "TWO_LEGGED_PULLBACK"}]}}
    pat = {
        7: [{"pattern_family": "CONFIRMED_H2_LONG", "lifecycle": "CONFIRMED"}],
        8: [{"pattern_family": "CONFIRMED_H2_LONG", "lifecycle": "CONFIRMED"}],
    }
    bars.append(_bar(8, 96.5, 97.2, 96.4, 97.0))
    res = _run(bars, dossier=_dossier(paa_verdict="DEFER"), patterns_by_idx=pat, obs_by_idx=obs)
    ok = not any(_has_blocker(r, BLOCKER_DEFERRED_BY_DAILY_THESIS) for r in res)
    ok = ok and daily_bias_class("DEFER").endswith("CONFIRMATION")
    return {"name": "defer_not_permanent_veto", "ok": ok}


def scenario_no_clear_not_veto() -> dict[str, Any]:
    bars = [_bar(i, 96, 96.4, 95.9, 96.2) for i in range(7)]
    bars.append(_bar(7, 96.2, 97.1, 96.1, 97.0))
    pat = {7: [{"pattern_family": "CONFIRMED_H2_LONG", "lifecycle": "CONFIRMED"}]}
    obs = {7: {"brooks_obs_json": [{"term": "TWO_LEGGED_PULLBACK"}]}}
    res = _run(bars, dossier=_dossier(paa_verdict="NO_CLEAR_LONG"), patterns_by_idx=pat, obs_by_idx=obs)
    ok = not any(_has_blocker(r, BLOCKER_DEFERRED_BY_DAILY_THESIS) for r in res)
    return {"name": "no_clear_not_permanent_veto", "ok": ok}


def scenario_possible_h2_no_entry() -> dict[str, Any]:
    bars = [_bar(i, 96, 96.3, 95.9, 96.1) for i in range(10)]
    pat = {9: [{"pattern_family": "POSSIBLE_H2_LONG", "lifecycle": "DEVELOPING"}]}
    res = _run(bars, dossier=_dossier(paa_verdict="DEFER"), patterns_by_idx=pat)
    ok = all(r.selected_action != ACTION_CONSIDER_ENTRY for r in res)
    ok = ok or any(_has_blocker(r, BLOCKER_POSSIBLE_PATTERN_ONLY) for r in res)
    return {"name": "possible_h2_no_entry", "ok": ok}


def scenario_v03_defer_still_blocks() -> dict[str, Any]:
    r = _run([_bar(0, 96, 96.5, 95.5, 96.2)], dossier=_dossier(paa_verdict="DEFER"), ruleset=V03_RS)[0]
    ok = _has_blocker(r, BLOCKER_DEFERRED_BY_DAILY_THESIS)
    return {"name": "v03_defer_unchanged", "ok": ok}


def scenario_vol_chaotic_blocks() -> dict[str, Any]:
    vol = VolatilitySessionState()
    params = {"vol_tr_median_window": 6, "vol_tr_avg_window": 3, "vol_opening_range_bars": 3,
              "vol_low_ratio": 0.55, "vol_high_ratio": 1.2, "vol_overlap_high": 0.5,
              "vol_directional_efficiency_high": 0.55, "vol_directional_efficiency_low": 0.28}
    prior = None
    for i in range(6):
        b = _bar(i, 100, 100.2, 99.9, 100.05)
        pc = prior.close if prior else None
        update_volatility(vol, bar=b, prior_close=pc, bar_index=i, params=params)
        prior = b
    b = _bar(6, 100, 101.5, 99.5, 100.1)
    update_volatility(vol, bar=b, prior_close=prior.close, bar_index=6, params=params)
    ok = classify_regime(vol, params) == VOL_HIGH_CHAOTIC
    return {"name": "chaotic_vol_classification", "ok": ok}


def scenario_daily_bias_mapping() -> dict[str, Any]:
    ok = effective_verdict_for_v03_engine("DEFER") == "NO_CLEAR_LONG"
    ok = ok and daily_bias_class("WAIT_PULLBACK").startswith("NEUTRAL_POSITIVE")
    return {"name": "daily_bias_mapping", "ok": ok}


def scenario_insufficient_room_hard_block() -> dict[str, Any]:
    d = _dossier(
        paa_verdict="LONG_APPROVE",
        resistance_zones=[{"low": 96.0, "high": 96.05}],
    )
    bars = [_bar(i, 95.8, 96.02, 95.7, 96.01) for i in range(15)]
    res = _run(bars, dossier=d)
    ok = any(
        _has_blocker(r, BLOCKER_LIMITED_ROOM_TO_NEXT)
        or _has_blocker(r, "LIMITED_ROOM_TO_UNBROKEN_RESISTANCE")
        or r.room_class in ("LIMITED", "AT_RESISTANCE")
        for r in res
    )
    return {"name": "insufficient_room_hard_block", "ok": ok}


SCENARIOS: list[Callable[[], dict[str, Any]]] = [
    scenario_defer_not_veto,
    scenario_no_clear_not_veto,
    scenario_possible_h2_no_entry,
    scenario_v03_defer_still_blocks,
    scenario_vol_chaotic_blocks,
    scenario_daily_bias_mapping,
    scenario_insufficient_room_hard_block,
]


def run_certification() -> dict[str, Any]:
    results = [fn() for fn in SCENARIOS]
    return {"ok": all(r["ok"] for r in results), "scenarios": results}


__all__ = ["run_certification", "SCENARIOS", "RULESET_VERSION"]
