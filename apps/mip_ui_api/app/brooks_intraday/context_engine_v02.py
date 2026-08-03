"""Phase 6B — calibrated context engine (V0.2)."""

from __future__ import annotations

from datetime import date
from typing import Any

from .bars import HistoricalBar
from .context_engine_v01 import (
    ContextResult,
    SessionContextState,
    _bar_range,
    _dossier_levels,
    _init_from_dossier,
    _intraday_setup_invalidation,
    _pattern_families,
    _reclaim_stage_update,
    _select_action,
    _support_interaction,
    _terms,
    get_context_session,
)
from .context_ruleset_v01 import (
    ACTION_CONSIDER_ENTRY,
    ACTION_DO_NOT_CHASE,
    ACTION_DO_NOT_ENTER,
    ACTION_ENTRY_ARMED,
    ACTION_OBSERVE,
    ACTION_THESIS_INVALIDATED,
    ACTION_THESIS_WEAKENED,
    ACTION_WAIT,
    ACTION_WAIT_FOR_NEW_SETUP,
    ACTION_WAIT_FOR_PULLBACK,
    ACTION_WAIT_FOR_RECLAIM,
    ACTION_WAIT_FOR_SUPPORT,
    EFFECT_CONFIRMS,
    EFFECT_INVALIDATES,
    EFFECT_NEUTRAL,
    EFFECT_SLIGHTLY_CONFIRMS,
    EFFECT_WEAKENS,
    INV_DAILY_THESIS,
    INV_INTRADAY_SETUP,
    RECLAIM_BELOW,
    RECLAIM_CLOSED_ABOVE,
    RECLAIM_CONFIRMED,
    ROOM_ABOVE_DNC,
    ROOM_ACCEPTABLE,
    ROOM_AMPLE,
    ROOM_AT_RESISTANCE,
    ROOM_LIMITED,
    STATE_DO_NOT_CHASE,
    STATE_ENTRY_ARMED,
    STATE_ENTRY_BLOCKED,
    STATE_OBSERVATION_ONLY,
    STATE_OBSERVING_OPEN,
    STATE_SETUP_DEVELOPING,
    STATE_THESIS_INVALIDATED,
    STATE_THESIS_WEAKENED,
    STATE_WAITING_FOR_NEW_SETUP,
    STATE_WAITING_FOR_PULLBACK,
    STATE_WAITING_FOR_RECLAIM,
    STATE_WAITING_FOR_RTH_CONFIRMATION,
    STATE_WAITING_FOR_SUPPORT_TEST,
    SUPPORT_HELD,
    SUPPORT_TESTING,
)
from .context_ruleset_v02 import resolve_params
from .level_derivation import assess_readiness


def _simulation_ready(dossier: dict[str, Any]) -> bool:
    frozen = dossier.get("trade_simulation_ready")
    if frozen is True:
        return True
    _obs, sim, _st, _msg = assess_readiness(dossier, verdict=str(dossier.get("paa_verdict") or ""))
    return bool(sim)


def _normalize_pattern_family(fam: str, params: dict[str, Any]) -> str:
    aliases = params.get("pattern_family_aliases") or {}
    if isinstance(aliases, dict):
        return str(aliases.get(fam, fam)).upper()
    return fam.upper()


def _meaningful_long_patterns_v02(patterns: list[dict[str, Any]], params: dict[str, Any]) -> list[dict[str, Any]]:
    allowed = {x.upper() for x in params["pattern_significance_entry_capable"]}  # type: ignore[index]
    obs_only = {x.upper() for x in params["pattern_significance_observation_only"]}  # type: ignore[index]
    out = []
    for p in patterns:
        fam = _normalize_pattern_family(str(p.get("pattern_family", "")).upper(), params)
        lc = str(p.get("lifecycle", "")).upper()
        if fam in obs_only:
            continue
        if fam in allowed and lc in ("CONFIRMED", "DEVELOPING", "POSSIBLE"):
            if fam == "LOCAL_DOUBLE_BOTTOM" and lc != "CONFIRMED":
                continue
            out.append({**p, "pattern_family": fam, "lifecycle": lc})
    return out


def _room_class_v02(
    *,
    close: float,
    resistance: float | None,
    dnc: float | None,
    sess: SessionContextState,
    bar: HistoricalBar,
    params: dict[str, Any],
) -> tuple[str, list[str]]:
    rules: list[str] = []
    if dnc is not None and close >= dnc:
        rules.append("CTX_ROOM_ABOVE_DNC_V0_2")
        return ROOM_ABOVE_DNC, rules
    if resistance is None:
        return ROOM_ACCEPTABLE, rules
    day_range = max(sess.session_high - sess.session_low, _bar_range(bar))
    dist = resistance - close
    if dist <= 0:
        rules.append("CTX_ROOM_AT_RESISTANCE_V0_2")
        return ROOM_AT_RESISTANCE, rules
    frac = dist / day_range
    if frac >= float(params["min_room_ample_fraction"]):
        return ROOM_AMPLE, rules
    if frac >= float(params["min_room_acceptable_fraction"]):
        return ROOM_ACCEPTABLE, rules
    rules.append("CTX_ROOM_LIMITED_V0_2")
    return ROOM_LIMITED, rules


def _entry_matrix(
    *,
    dossier: dict[str, Any],
    opening: bool,
    meaningful: list,
    support_status: str,
    setup_inv: float | None,
    reclaim_stage: str,
    room: str,
    levels: dict,
    thesis_effect: str,
    blockers: list[str],
    close: float,
) -> dict[str, str]:
    sim_ok = _simulation_ready(dossier)
    verdict = str(dossier.get("paa_verdict", "")).upper()
    daily_ok = thesis_effect != EFFECT_INVALIDATES and verdict not in ("NO_CLEAR_LONG", "DEFER")

    def _cell(ok: bool | None, rule: str) -> str:
        if ok is True:
            return "passed"
        if ok is False:
            return "failed"
        return "not_assessed"

    return {
        "daily_thesis_valid": _cell(daily_ok, "CTX_ENTRY_DAILY_V0_2"),
        "dossier_simulation_ready": _cell(sim_ok, "CTX_ENTRY_SIM_READY_V0_2"),
        "opening_complete": _cell(not opening, "CTX_ENTRY_OPENING_V0_2"),
        "meaningful_pattern": _cell(bool(meaningful), "CTX_ENTRY_PATTERN_V0_2"),
        "support_held_or_testing": _cell(support_status in (SUPPORT_HELD, SUPPORT_TESTING), "CTX_ENTRY_SUPPORT_V0_2"),
        "setup_invalidation_available": _cell(setup_inv is not None, "CTX_ENTRY_SETUP_INV_V0_2"),
        "reclaim_progress": _cell(reclaim_stage not in (RECLAIM_BELOW,), "CTX_ENTRY_RECLAIM_V0_2"),
        "room_acceptable": _cell(room in (ROOM_AMPLE, ROOM_ACCEPTABLE), "CTX_ENTRY_ROOM_V0_2"),
        "below_do_not_chase": _cell(
            levels.get("do_not_chase") is None or close < float(levels["do_not_chase"]),
            "CTX_ENTRY_DNC_V0_2",
        ),
        "no_higher_blocker": _cell(not blockers, "CTX_ENTRY_BLOCKERS_V0_2"),
    }


def _entry_score(matrix: dict[str, str]) -> int:
    return sum(1 for v in matrix.values() if v == "passed")


def advance_context_v02_for_bar(
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
) -> ContextResult:
    params = resolve_params(params)
    sess = get_context_session(state, symbol, trading_date)
    _init_from_dossier(sess, dossier, params)
    state_before = sess.advisory_state
    sess.session_bars += 1
    sess.session_high = max(sess.session_high, bar.high)
    sess.session_low = min(sess.session_low, bar.low)

    levels = _dossier_levels(dossier)
    terms = _terms(objective_obs)
    pat_list = _pattern_families(patterns, params=params)
    meaningful = _meaningful_long_patterns_v02(pat_list, params)

    daily_inv = levels.get("daily_thesis_invalidation")
    setup_inv = _intraday_setup_invalidation(sess, pat_list, levels)
    sess.setup_invalidation_level = setup_inv

    support_status, sup_rules = _support_interaction(
        close=bar.close,
        low=bar.low,
        support=levels.get("primary_support"),
        bar=bar,
        terms=terms,
        params=params,
    )
    sess.support_status = support_status
    reclaim_stage, rec_rules = _reclaim_stage_update(
        sess,
        close=bar.close,
        high=bar.high,
        reclaim=levels.get("reclaim"),
        bar=bar,
        params=params,
    )
    sess.reclaim_stage = reclaim_stage
    room, room_rules = _room_class_v02(
        close=bar.close,
        resistance=levels.get("primary_resistance"),
        dnc=levels.get("do_not_chase"),
        sess=sess,
        bar=bar,
        params=params,
    )

    supporting: list[dict[str, Any]] = []
    opposing: list[dict[str, Any]] = []
    blockers: list[str] = []
    rule_ids = list(sup_rules) + list(rec_rules) + list(room_rules)
    markers: list[str] = []
    do_not_enter_reason: str | None = None

    thesis_effect = EFFECT_NEUTRAL
    if daily_inv is not None and bar.close < daily_inv:
        thesis_effect = EFFECT_INVALIDATES
        opposing.append({"type": INV_DAILY_THESIS, "level": daily_inv, "rule_id": "CTX_DAILY_THESIS_INVALID_V0_2"})
        rule_ids.append("CTX_DAILY_THESIS_INVALID_V0_2")
    elif setup_inv is not None and bar.close < setup_inv:
        thesis_effect = EFFECT_WEAKENS
        opposing.append({"type": INV_INTRADAY_SETUP, "level": setup_inv, "rule_id": "CTX_INTRADAY_SETUP_BREAK_V0_2"})
        rule_ids.append("CTX_INTRADAY_SETUP_BREAK_V0_2")
    elif support_status == SUPPORT_HELD and meaningful:
        thesis_effect = EFFECT_SLIGHTLY_CONFIRMS
    elif reclaim_stage == RECLAIM_CONFIRMED:
        thesis_effect = EFFECT_CONFIRMS

    opening = bar_index_in_session < int(params["opening_observation_bars"])
    if opening and state_before in (STATE_WAITING_FOR_RTH_CONFIRMATION, STATE_OBSERVATION_ONLY):
        sess.advisory_state = STATE_OBSERVING_OPEN

    candidates: dict[str, str] = {}
    candidates["OBSERVE"] = ACTION_OBSERVE
    candidates["WAIT"] = ACTION_WAIT

    verdict = str(dossier.get("paa_verdict", "")).upper()
    sim_ready = _simulation_ready(dossier)

    if state_before == STATE_THESIS_INVALIDATED and thesis_effect == EFFECT_INVALIDATES:
        candidates["OBSERVE"] = ACTION_OBSERVE
        sess.advisory_state = STATE_THESIS_INVALIDATED
    elif thesis_effect == EFFECT_INVALIDATES:
        candidates["THESIS_INVALIDATED"] = ACTION_THESIS_INVALIDATED
        sess.advisory_state = STATE_THESIS_INVALIDATED
        markers.append("thesis_invalidated")
    elif room == ROOM_ABOVE_DNC or (levels.get("do_not_chase") and bar.close >= levels["do_not_chase"]):
        candidates["DO_NOT_CHASE"] = ACTION_DO_NOT_CHASE
        sess.advisory_state = STATE_DO_NOT_CHASE
        blockers.append("DO_NOT_CHASE_LEVEL")
        markers.append("do_not_chase")
    elif verdict in ("NO_CLEAR_LONG", "DEFER"):
        candidates["DO_NOT_ENTER"] = ACTION_DO_NOT_ENTER
        do_not_enter_reason = "no_clear_long_verdict"
        blockers.append("NO_CLEAR_LONG_VERDICT")
        sess.advisory_state = STATE_OBSERVATION_ONLY
    elif not sim_ready:
        candidates["WAIT"] = ACTION_WAIT
        blockers.append("DOSSIER_NOT_SIMULATION_READY")
    elif room in (ROOM_LIMITED, ROOM_AT_RESISTANCE):
        candidates["WAIT"] = ACTION_WAIT
        sess.advisory_state = STATE_ENTRY_BLOCKED
        blockers.append("LIMITED_ROOM_TO_RESISTANCE")
    elif thesis_effect == EFFECT_WEAKENS:
        candidates["THESIS_WEAKENED"] = ACTION_THESIS_WEAKENED
        candidates["WAIT_FOR_NEW_SETUP"] = ACTION_WAIT_FOR_NEW_SETUP
        sess.advisory_state = STATE_WAITING_FOR_NEW_SETUP
        markers.append("thesis_weakened")
    else:
        has_meaningful = bool(meaningful)
        reclaim_ok = reclaim_stage in (RECLAIM_CLOSED_ABOVE, RECLAIM_CONFIRMED)
        if has_meaningful and support_status in (SUPPORT_HELD, SUPPORT_TESTING):
            sess.advisory_state = STATE_SETUP_DEVELOPING
            markers.append("setup_developing")
        if (
            has_meaningful
            and support_status in (SUPPORT_HELD, SUPPORT_TESTING)
            and setup_inv
            and room in (ROOM_AMPLE, ROOM_ACCEPTABLE)
            and sim_ready
        ):
            if reclaim_stage == RECLAIM_BELOW and levels.get("reclaim"):
                sess.advisory_state = STATE_WAITING_FOR_RECLAIM
                candidates["WAIT_FOR_RECLAIM"] = ACTION_WAIT_FOR_RECLAIM
            elif reclaim_ok:
                candidates["ENTRY_ARMED"] = ACTION_ENTRY_ARMED
                sess.advisory_state = STATE_ENTRY_ARMED
                markers.append("entry_armed")
                if reclaim_stage == RECLAIM_CONFIRMED and not opening:
                    candidates["CONSIDER_ENTRY"] = ACTION_CONSIDER_ENTRY
                    markers.append("consider_entry")
            else:
                candidates["WAIT_FOR_RECLAIM"] = ACTION_WAIT_FOR_RECLAIM
        elif verdict == "WAIT_PULLBACK":
            candidates["WAIT_FOR_PULLBACK"] = ACTION_WAIT_FOR_PULLBACK
        elif support_status in (SUPPORT_TESTING,):
            candidates["WAIT_FOR_SUPPORT"] = ACTION_WAIT_FOR_SUPPORT
            sess.advisory_state = STATE_WAITING_FOR_SUPPORT_TEST
        else:
            candidates["WAIT"] = ACTION_WAIT

    if opening:
        candidates.pop("CONSIDER_ENTRY", None)

    matrix = _entry_matrix(
        dossier=dossier,
        opening=opening,
        meaningful=meaningful,
        support_status=support_status,
        setup_inv=setup_inv,
        reclaim_stage=reclaim_stage,
        room=room,
        levels=levels,
        thesis_effect=thesis_effect,
        blockers=blockers,
        close=bar.close,
    )
    score = _entry_score(matrix)

    selected, blocked_candidates = _select_action(candidates, params)
    if selected == ACTION_OBSERVE and hasattr(sess, "_initial_action"):
        selected = getattr(sess, "_initial_action", ACTION_OBSERVE)

    layers = {
        "objective_fact": {"layer": "OBJECTIVE_FACT", "terms": list(terms)},
        "pattern_instance": {
            "layer": "PATTERN_INSTANCE",
            "patterns": [{"family": p.get("pattern_family"), "lifecycle": p.get("lifecycle")} for p in pat_list],
        },
        "contextual_interpretation": {
            "layer": "CONTEXTUAL_INTERPRETATION",
            "support_status": support_status,
            "reclaim_stage": reclaim_stage,
            "room_class": room,
            "simulation_ready": sim_ready,
        },
        "advisory_state": {"layer": "ADVISORY_STATE", "state": sess.advisory_state},
        "advisory_action": {"layer": "ADVISORY_ACTION", "action": selected},
        "diagnostics": {
            "entry_condition_matrix": matrix,
            "entry_score": score,
            "do_not_enter_reason": do_not_enter_reason,
        },
    }

    return ContextResult(
        layers=layers,
        thesis_effect=thesis_effect,
        state_before=state_before,
        state_after=sess.advisory_state,
        selected_action=selected,
        candidate_actions=list(candidates.values()),
        blocked_candidates=blocked_candidates,
        blockers=blockers,
        supporting_evidence=supporting,
        opposing_evidence=opposing,
        active_levels={
            "reclaim": levels.get("reclaim"),
            "do_not_chase": levels.get("do_not_chase"),
            "primary_support": levels.get("primary_support"),
            "primary_resistance": levels.get("primary_resistance"),
        },
        daily_thesis_invalidation=daily_inv,
        intraday_setup_invalidation=setup_inv,
        reclaim_stage=reclaim_stage,
        support_status=support_status,
        room_class=room,
        explanation=f"Support={support_status}; Reclaim={reclaim_stage}; Room={room}; score={score}",
        context_classifications=[support_status, reclaim_stage, room],
        marker_flags=markers,
        rule_ids=rule_ids,
    )
