"""BROOKS_CONTEXT_RULESET_V0_4 — intraday-led engine (wraps V0.3 core with V0.4 policy)."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any

from .bars import HistoricalBar
from .context_engine_v01 import _pattern_families
from .context_engine_v02 import _meaningful_long_patterns_v02, _simulation_ready
from .context_engine_v03 import (
    RL_BROKEN_CONFIRMED,
    RL_FAILED_BREAKOUT,
    RL_UNBROKEN,
    STATE_ENTRY_ARMED,
    STATE_FAILED_BREAKOUT,
    STATE_OBSERVATION_ONLY,
    advance_context_v03_for_bar,
    get_v03_session,
    may_emit_consider_entry_v03,
    room_to_level,
    session_range,
    time_ok,
)
from .context_ruleset_v01 import ACTION_CONSIDER_ENTRY, ACTION_ENTRY_ARMED, ACTION_OBSERVE
from .context_ruleset_v03 import (
    BLOCKER_LIMITED_ROOM_TO_NEXT,
    BLOCKER_LIMITED_ROOM_TO_UNBROKEN,
    BLOCKER_NO_DAILY_LONG_AUTHORIZATION,
    BLOCKER_WAITING_FOR_INTRADAY_UPGRADE,
    normalize_verdict,
)
from .context_ruleset_v04 import (
    ALIGN_ALIGNED,
    ALIGN_CONFLICT_STRONG_CONFIRM,
    ALIGN_DAILY_NEUTRAL,
    ALIGN_INTRADAY_OVERRIDE,
    BLOCKER_CHAOTIC_VOLATILITY,
    BLOCKER_COMPRESSION_NO_BREAKOUT,
    BLOCKER_DAILY_CONFLICT_CONFIRMATION,
    BLOCKER_POSSIBLE_PATTERN_ONLY,
    PATH_A_BREAKOUT_PULLBACK,
    PATH_B_H2_TWO_LEG,
    PATH_C_FAILED_BEAR_SUPPORT,
    PATH_D_DOUBLE_BOTTOM,
    PATH_E_TREND_RESUMPTION,
    PATH_NONE,
    RULESET_VERSION,
    THESIS_CONSUMED,
    THESIS_DEVELOPING,
    THESIS_ENTRY_ARMED,
    THESIS_FAILED,
    THESIS_NONE,
    THESIS_QUALIFIED,
    VOL_HIGH_CHAOTIC,
    VOL_LOW_COMPRESSION,
    daily_bias_class,
    effective_verdict_for_v03_engine,
    resolve_params,
)
from .context_volatility_v04 import VolatilitySessionState, update_volatility, volatility_snapshot


def _session_key(symbol: str, trading_date: date) -> str:
    return f"{symbol.upper()}|{trading_date.isoformat()}"


@dataclass
class V04AuxState:
    vol: VolatilitySessionState = field(default_factory=VolatilitySessionState)
    intraday_thesis_type: str = PATH_NONE
    intraday_thesis_status: str = THESIS_NONE
    daily_bias: str = ""
    daily_intraday_alignment: str = ALIGN_DAILY_NEUTRAL
    confirmation_quality: int = 0
    risk_feasible: bool = True
    plain_explanation: str = ""
    pullback_legs: int = 0
    failed_bear_stage: int = 0
    path_b_armed_bar: int | None = None


def get_v04_aux(state: dict[str, Any], symbol: str, trading_date: date) -> V04AuxState:
    bucket = state.setdefault("_context_v04_aux", {})
    key = _session_key(symbol, trading_date)
    if key not in bucket:
        bucket[key] = V04AuxState()
    return bucket[key]


def reset_v04_session(state: dict[str, Any], symbol: str, trading_date: date) -> None:
    bucket = state.setdefault("_context_v04_aux", {})
    bucket[_session_key(symbol, trading_date)] = V04AuxState()
    v03_bucket = state.setdefault("_context_v04_v03_sessions", {})
    v03_bucket.pop(_session_key(symbol, trading_date), None)


def _v03_state_bridge(state: dict[str, Any]) -> dict[str, Any]:
    return {"_context_v03_session_state": state.setdefault("_context_v04_v03_sessions", {})}


def _terms(objective_obs: dict[str, Any]) -> set[str]:
    out: set[str] = set()
    for t in objective_obs.get("brooks_obs_json") or []:
        if isinstance(t, dict) and t.get("term"):
            out.add(str(t["term"]).upper())
    return out


def _has_confirmed_pattern(patterns: list[dict[str, Any]], family: str) -> bool:
    fam = family.upper()
    for p in patterns:
        if str(p.get("pattern_family", "")).upper() == fam and str(p.get("lifecycle", "")).upper() == "CONFIRMED":
            return True
    return False


def _has_possible_only(patterns: list[dict[str, Any]], family: str) -> bool:
    fam = family.upper()
    confirmed = False
    possible = False
    for p in patterns:
        if str(p.get("pattern_family", "")).upper() != fam:
            continue
        lc = str(p.get("lifecycle", "")).upper()
        if lc == "CONFIRMED":
            confirmed = True
        if lc in ("POSSIBLE", "DEVELOPING"):
            possible = True
    return possible and not confirmed


def _daily_bearish(dossier: dict[str, Any], params: dict[str, Any]) -> bool:
    trend = str(dossier.get("daily_trend") or "").upper()
    allowed = params.get("daily_bearish_trends") or []
    return trend in {str(x).upper() for x in allowed}


def _compute_alignment(
    *,
    true_verdict: str,
    thesis_type: str,
    thesis_status: str,
    daily_bear: bool,
) -> str:
    auth = {"LONG_APPROVE", "LONG_APPROVE_REDUCED", "WAIT_PULLBACK", "WAIT_RECLAIM"}
    v = true_verdict.upper()
    if daily_bear and thesis_status in (THESIS_QUALIFIED, THESIS_ENTRY_ARMED, THESIS_DEVELOPING):
        return ALIGN_CONFLICT_STRONG_CONFIRM
    if v in auth and thesis_type != PATH_NONE:
        return ALIGN_ALIGNED
    if v in ("DEFER", "NO_CLEAR_LONG") and thesis_type != PATH_NONE:
        return ALIGN_INTRADAY_OVERRIDE
    if v in ("DEFER", "NO_CLEAR_LONG"):
        return ALIGN_DAILY_NEUTRAL
    return ALIGN_DAILY_NEUTRAL


def _explain_v04(
    *,
    aux: V04AuxState,
    selected: str,
    blockers: list[str],
    vol_regime: str,
) -> str:
    if BLOCKER_CHAOTIC_VOLATILITY in blockers:
        return "High volatility is currently chaotic rather than directional."
    if BLOCKER_COMPRESSION_NO_BREAKOUT in blockers:
        return "Low volatility compression — watch for breakout, but do not enter before confirmation."
    if BLOCKER_LIMITED_ROOM_TO_NEXT in blockers or BLOCKER_LIMITED_ROOM_TO_UNBROKEN in blockers:
        return "The intraday setup is valid, but resistance is too close for acceptable risk."
    if aux.intraday_thesis_status == THESIS_DEVELOPING and aux.intraday_thesis_type == PATH_B_H2_TWO_LEG:
        return "A possible H2 exists, but the second attempt has not produced bullish confirmation."
    if selected == ACTION_CONSIDER_ENTRY:
        return "A fresh intraday setup has qualified despite the neutral daily view."
    if selected == ACTION_ENTRY_ARMED and vol_regime.startswith("HIGH_VOLATILITY_DIRECTIONAL"):
        return "Volatility is expanding directionally and the breakout is holding."
    if aux.daily_bias.endswith("CAUTIOUS") and aux.intraday_thesis_status == THESIS_DEVELOPING:
        return "Daily view is cautious, but a new intraday bullish thesis is developing."
    return aux.plain_explanation or f"V0.4 regime={vol_regime}; thesis={aux.intraday_thesis_type}; action={selected}"


def _try_intraday_paths(
    *,
    aux: V04AuxState,
    sess,
    bar: HistoricalBar,
    bar_index: int,
    patterns: list[dict[str, Any]],
    terms: set[str],
    meaningful: list,
    dossier: dict[str, Any],
    params: dict[str, Any],
    state_before: str,
    daily_bear: bool,
) -> None:
    """Upgrade aux thesis when V0.3 blocked on daily authorization but intraday evidence exists."""
    if sess.advisory_state == STATE_FAILED_BREAKOUT or sess.resistance_lifecycle == RL_FAILED_BREAKOUT:
        aux.intraday_thesis_status = THESIS_FAILED
        aux.intraday_thesis_type = PATH_A_BREAKOUT_PULLBACK
        return

    sr = session_range(sess, bar)

    # Path A — delegated to V0.3 breakout lifecycle when upgrade path active
    if sess.resistance_lifecycle != RL_UNBROKEN:
        aux.intraday_thesis_type = PATH_A_BREAKOUT_PULLBACK
        if sess.advisory_state == STATE_ENTRY_ARMED:
            aux.intraday_thesis_status = THESIS_ENTRY_ARMED
        elif sess.resistance_lifecycle == RL_BROKEN_CONFIRMED:
            aux.intraday_thesis_status = THESIS_DEVELOPING
        return

    # Path B — confirmed H2 + two-legged pullback
    if _has_confirmed_pattern(patterns, "CONFIRMED_H2_LONG") and "TWO_LEGGED_PULLBACK" in terms:
        aux.intraday_thesis_type = PATH_B_H2_TWO_LEG
        aux.pullback_legs = 2
        if aux.vol.regime != VOL_HIGH_CHAOTIC:
            aux.intraday_thesis_status = THESIS_QUALIFIED
            aux.confirmation_quality = 2
        else:
            aux.intraday_thesis_status = THESIS_DEVELOPING
        return

    if _has_possible_only(patterns, "POSSIBLE_H2_LONG") or _has_possible_only(patterns, "H2_LONG"):
        aux.intraday_thesis_type = PATH_B_H2_TWO_LEG
        aux.intraday_thesis_status = THESIS_DEVELOPING
        return

    # Path C — failed bear breakout near support
    support = dossier.get("support_zones")
    sup_low = None
    if isinstance(support, list) and support:
        z = support[0]
        if isinstance(z, dict):
            sup_low = z.get("low")
    if sup_low is not None and bar.low < float(sup_low) and bar.close > float(sup_low):
        if _has_confirmed_pattern(patterns, "BULL_REVERSAL") or "BULL_REVERSAL" in terms:
            aux.intraday_thesis_type = PATH_C_FAILED_BEAR_SUPPORT
            aux.intraday_thesis_status = THESIS_QUALIFIED
            aux.confirmation_quality = 2
            return

    # Path D — confirmed double bottom
    if _has_confirmed_pattern(patterns, "LOCAL_DOUBLE_BOTTOM"):
        aux.intraday_thesis_type = PATH_D_DOUBLE_BOTTOM
        aux.intraday_thesis_status = THESIS_QUALIFIED
        aux.confirmation_quality = 2
        return

    # Path E — trend resumption after contracting pullback
    if (
        "CONTROLLED_PULLBACK" in terms
        and aux.vol.regime == VOL_LOW_COMPRESSION
        and bar.close > bar.open
    ):
        aux.intraday_thesis_type = PATH_E_TREND_RESUMPTION
        aux.intraday_thesis_status = THESIS_QUALIFIED
        aux.confirmation_quality = 1
        return

    if meaningful and aux.intraday_thesis_status == THESIS_NONE:
        aux.intraday_thesis_status = THESIS_DEVELOPING

    if daily_bear:
        aux.confirmation_quality = max(aux.confirmation_quality, 2)


def _min_confirmation_required(aux: V04AuxState, daily_bear: bool, alignment: str) -> int:
    if alignment == ALIGN_CONFLICT_STRONG_CONFIRM or daily_bear:
        return 2
    if aux.daily_bias.endswith("CAUTIOUS"):
        return 2
    return 1


def _apply_vol_gates(
    *,
    aux: V04AuxState,
    result: ContextResult,
    sess,
    meaningful: list,
) -> None:
    blockers = list(result.blockers)
    selected = result.selected_action
    regime = aux.vol.regime

    if regime == VOL_HIGH_CHAOTIC and selected in (ACTION_CONSIDER_ENTRY, ACTION_ENTRY_ARMED):
        if aux.confirmation_quality < 2 and not _has_confirmed_structural(meaningful):
            if BLOCKER_CHAOTIC_VOLATILITY not in blockers:
                blockers.append(BLOCKER_CHAOTIC_VOLATILITY)
            selected = ACTION_OBSERVE
            result.layers.setdefault("advisory_action", {})["action"] = selected

    if regime == VOL_LOW_COMPRESSION and selected == ACTION_CONSIDER_ENTRY:
        if sess.resistance_lifecycle != RL_BROKEN_CONFIRMED and aux.intraday_thesis_status != THESIS_QUALIFIED:
            blockers.append(BLOCKER_COMPRESSION_NO_BREAKOUT)
            selected = ACTION_OBSERVE
            result.layers.setdefault("advisory_action", {})["action"] = selected

    result.blockers = blockers
    result.selected_action = selected


def _has_confirmed_structural(meaningful: list) -> bool:
    for p in meaningful:
        if str(p.get("pattern_family", "")).upper() == "STRUCTURAL_BREAKOUT" and str(
            p.get("lifecycle", "")
        ).upper() == "CONFIRMED":
            return True
    return False


def _maybe_arm_from_intraday_thesis(
    *,
    result: ContextResult,
    aux: V04AuxState,
    sess,
    bar: HistoricalBar,
    bar_index: int,
    meaningful: list,
    params: dict[str, Any],
    state_before: str,
    daily_bear: bool,
    patterns: list[dict[str, Any]],
) -> None:
    if aux.intraday_thesis_status not in (THESIS_QUALIFIED, THESIS_ENTRY_ARMED):
        return
    if not time_ok(bar_index, params):
        return
    if _has_possible_only(patterns, "POSSIBLE_H2_LONG"):
        result.blockers = list(result.blockers) + [BLOCKER_POSSIBLE_PATTERN_ONLY]
        return

    need = _min_confirmation_required(aux, daily_bear, aux.daily_intraday_alignment)
    if aux.confirmation_quality < need:
        if BLOCKER_DAILY_CONFLICT_CONFIRMATION not in result.blockers and daily_bear:
            result.blockers = list(result.blockers) + [BLOCKER_DAILY_CONFLICT_CONFIRMATION]
        return

    blockers = [b for b in result.blockers if b not in (BLOCKER_NO_DAILY_LONG_AUTHORIZATION, BLOCKER_WAITING_FOR_INTRADAY_UPGRADE)]
    if sess.advisory_state == STATE_OBSERVATION_ONLY and aux.intraday_thesis_type != PATH_NONE:
        from .context_engine_v03 import _arm_entry_on_bar

        _arm_entry_on_bar(sess, bar_index, state_before)
        aux.intraday_thesis_status = THESIS_ENTRY_ARMED
        candidates = dict(result.layers.get("advisory_action") or {})
        result.selected_action = ACTION_ENTRY_ARMED
        if may_emit_consider_entry_v03(
            state_before=state_before,
            bar_index_in_session=bar_index,
            sess=sess,
            bar=bar,
            meaningful=meaningful,
            params=params,
            opening=bar_index < int(params["opening_observation_bars"]),
        ):
            result.selected_action = ACTION_CONSIDER_ENTRY
        result.blockers = blockers
        result.state_after = sess.advisory_state


def advance_context_v04_for_bar(
    *,
    state: dict[str, Any],
    dossier: dict[str, Any],
    symbol: str,
    trading_date: date,
    bar: HistoricalBar,
    bar_index_in_session: int,
    objective_obs: dict[str, Any],
    patterns: list[dict[str, Any]],
    params: dict[str, Any] | None = None,
    prior_bar: HistoricalBar | None = None,
    open_position_symbol: str | None = None,
) -> ContextResult:
    params = resolve_params(params)
    aux = get_v04_aux(state, symbol, trading_date)
    true_verdict = normalize_verdict(str(dossier.get("paa_verdict") or ""))
    aux.daily_bias = daily_bias_class(true_verdict)
    daily_bear = _daily_bearish(dossier, params)

    prior_close = prior_bar.close if prior_bar else (aux.vol.closes[-1] if aux.vol.closes else None)
    update_volatility(aux.vol, bar=bar, prior_close=prior_close, bar_index=bar_index_in_session, params=params)

    if not _simulation_ready(dossier):
        dossier_eff = dict(dossier)
    else:
        dossier_eff = {**dossier, "paa_verdict": effective_verdict_for_v03_engine(true_verdict)}

    bridge = _v03_state_bridge(state)
    result = advance_context_v03_for_bar(
        state=bridge,
        dossier=dossier_eff,
        symbol=symbol,
        trading_date=trading_date,
        bar=bar,
        bar_index_in_session=bar_index_in_session,
        objective_obs=objective_obs,
        patterns=patterns,
        params=params,
        prior_bar=prior_bar,
        open_position_symbol=open_position_symbol,
    )

    sess = get_v03_session(bridge, symbol, trading_date)
    state_before = result.state_before
    pat_list = _pattern_families(patterns, params=params)
    meaningful = _meaningful_long_patterns_v02(pat_list, params)
    terms = _terms(objective_obs)

    _try_intraday_paths(
        aux=aux,
        sess=sess,
        bar=bar,
        bar_index=bar_index_in_session,
        patterns=pat_list,
        terms=terms,
        meaningful=meaningful,
        dossier=dossier,
        params=params,
        state_before=state_before,
        daily_bear=daily_bear,
    )

    aux.daily_intraday_alignment = _compute_alignment(
        true_verdict=true_verdict,
        thesis_type=aux.intraday_thesis_type,
        thesis_status=aux.intraday_thesis_status,
        daily_bear=daily_bear,
    )

    if result.blockers and any(
        b in result.blockers
        for b in (BLOCKER_NO_DAILY_LONG_AUTHORIZATION, BLOCKER_WAITING_FOR_INTRADAY_UPGRADE)
    ):
        _maybe_arm_from_intraday_thesis(
            result=result,
            aux=aux,
            sess=sess,
            bar=bar,
            bar_index=bar_index_in_session,
            meaningful=meaningful,
            params=params,
            state_before=state_before,
            daily_bear=daily_bear,
            patterns=pat_list,
        )

    _apply_vol_gates(aux=aux, result=result, sess=sess, meaningful=meaningful)

    vol_snap = volatility_snapshot(aux.vol)
    diag = result.layers.setdefault("diagnostics", {})
    diag.update(
        {
            "ruleset_version": RULESET_VERSION,
            "daily_bias_class": aux.daily_bias,
            "daily_verdict_raw": true_verdict,
            "intraday_volatility_regime": vol_snap["regime"],
            "volatility_metrics": vol_snap,
            "intraday_structure": sess.advisory_state,
            "intraday_thesis_type": aux.intraday_thesis_type,
            "intraday_thesis_status": aux.intraday_thesis_status,
            "confirmation_quality": aux.confirmation_quality,
            "risk_feasible": aux.risk_feasible,
            "daily_intraday_alignment": aux.daily_intraday_alignment,
            "setup_cycle_id": sess.current_setup_cycle_id,
        }
    )
    result.layers.setdefault("contextual_interpretation", {})["ruleset_version"] = RULESET_VERSION
    result.context_classifications = list(result.context_classifications or []) + [
        f"V04_VOL_{vol_snap['regime']}",
        f"V04_BIAS_{aux.daily_bias}",
        f"V04_THESIS_{aux.intraday_thesis_type}",
    ]
    result.explanation = _explain_v04(
        aux=aux,
        selected=result.selected_action,
        blockers=result.blockers,
        vol_regime=str(vol_snap["regime"]),
    )
    if "CTX_V0_3" in (result.rule_ids or []):
        result.rule_ids = ["CTX_V0_4"] + [r for r in result.rule_ids if r != "CTX_V0_3"]
    else:
        result.rule_ids = ["CTX_V0_4"] + list(result.rule_ids or [])
    return result


__all__ = [
    "advance_context_v04_for_bar",
    "get_v04_aux",
    "reset_v04_session",
]
