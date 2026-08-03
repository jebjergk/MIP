"""Phase 6 — deterministic context engine (PAA thesis + patterns + advisory FSM)."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

from .bars import HistoricalBar
from .context_ruleset_v01 import (
    ACTION_CONSIDER_ENTRY,
    ACTION_DO_NOT_CHASE,
    ACTION_DO_NOT_ENTER,
    ACTION_ENTRY_ARMED,
    ACTION_OBSERVE,
    ACTION_THESIS_INVALIDATED,
    ACTION_THESIS_WEAKENED,
    ACTION_WAIT,
    ACTION_WAIT_FOR_FOLLOW_THROUGH,
    ACTION_WAIT_FOR_PULLBACK,
    ACTION_WAIT_FOR_RECLAIM,
    ACTION_WAIT_FOR_SUPPORT,
    EFFECT_CONFIRMS,
    EFFECT_INVALIDATES,
    EFFECT_NEUTRAL,
    EFFECT_SLIGHTLY_CONFIRMS,
    EFFECT_SLIGHTLY_WEAKENS,
    EFFECT_WEAKENS,
    INV_DAILY_THESIS,
    INV_INTRADAY_SETUP,
    RECLAIM_BELOW,
    RECLAIM_CLOSED_ABOVE,
    RECLAIM_CONFIRMED,
    RECLAIM_FAILED,
    RECLAIM_TESTING,
    ROOM_ABOVE_DNC,
    ROOM_ACCEPTABLE,
    ROOM_AMPLE,
    ROOM_AT_RESISTANCE,
    ROOM_LIMITED,
    RULESET_VERSION,
    STATE_DO_NOT_CHASE,
    STATE_ENTRY_ARMED,
    STATE_ENTRY_BLOCKED,
    STATE_OBSERVATION_ONLY,
    STATE_OBSERVING_OPEN,
    STATE_SETUP_DEVELOPING,
    STATE_THESIS_INVALIDATED,
    STATE_THESIS_WEAKENED,
    STATE_WAITING_FOR_FOLLOW_THROUGH,
    STATE_WAITING_FOR_PULLBACK,
    STATE_WAITING_FOR_RECLAIM,
    STATE_WAITING_FOR_RTH_CONFIRMATION,
    STATE_WAITING_FOR_STRONGER_CONFIRMATION,
    STATE_WAITING_FOR_SUPPORT_TEST,
    SUPPORT_ABOVE,
    SUPPORT_APPROACHING,
    SUPPORT_FAILED,
    SUPPORT_HELD,
    SUPPORT_PIERCED,
    SUPPORT_TESTING,
    resolve_params,
)

@dataclass
class ContextResult:
    layers: dict[str, Any]
    thesis_effect: str
    state_before: str
    state_after: str
    selected_action: str
    candidate_actions: list[str]
    blocked_candidates: list[dict[str, str]]
    blockers: list[str]
    supporting_evidence: list[dict[str, Any]]
    opposing_evidence: list[dict[str, Any]]
    active_levels: dict[str, Any]
    daily_thesis_invalidation: float | None
    intraday_setup_invalidation: float | None
    reclaim_stage: str
    support_status: str
    room_class: str
    explanation: str
    context_classifications: list[str]
    marker_flags: list[str]
    rule_ids: list[str]


@dataclass
class SessionContextState:
    symbol: str
    trading_date: date
    advisory_state: str = STATE_OBSERVATION_ONLY
    reclaim_stage: str = RECLAIM_BELOW
    support_status: str = SUPPORT_ABOVE
    session_bars: int = 0
    reclaim_bars_above: int = 0
    setup_invalidation_level: float | None = None
    dossier_id: str | None = None
    initialized: bool = False
    session_high: float = -1e18
    session_low: float = 1e18


def _session_key(symbol: str, trading_date: date) -> str:
    return f"{symbol.upper()}|{trading_date.isoformat()}"


def get_context_session(state: dict[str, Any], symbol: str, trading_date: date) -> SessionContextState:
    bucket = state.setdefault("_context_session_state", {})
    key = _session_key(symbol, trading_date)
    if key not in bucket:
        bucket[key] = SessionContextState(symbol=symbol.upper(), trading_date=trading_date)
    return bucket[key]


def reset_context_session(state: dict[str, Any], symbol: str, trading_date: date) -> None:
    bucket = state.setdefault("_context_session_state", {})
    bucket[_session_key(symbol, trading_date)] = SessionContextState(symbol=symbol.upper(), trading_date=trading_date)


def _dossier_levels(dossier: dict[str, Any]) -> dict[str, float | None]:
    dl = dossier.get("derived_levels") or {}
    reclaim = dossier.get("reclaim_level")
    if isinstance(reclaim, dict):
        reclaim = reclaim.get("value")
    dnc = dossier.get("do_not_chase_level")
    if isinstance(dnc, dict):
        dnc = dnc.get("value")
    inv = dossier.get("invalidation_level")
    if isinstance(inv, dict):
        inv = inv.get("value")
    supports = dossier.get("support_zones") or []
    resistances = dossier.get("resistance_zones") or []

    def _mid(z: dict) -> float | None:
        lo = z.get("lower", z.get("low"))
        hi = z.get("upper", z.get("high"))
        if lo is not None and hi is not None:
            return (float(lo) + float(hi)) / 2
        if z.get("price") is not None:
            return float(z["price"])
        return None

    primary_sup = _mid(supports[0]) if supports else (dl.get("primary_support") or {}).get("value")
    primary_res = _mid(resistances[0]) if resistances else (dl.get("primary_resistance") or {}).get("value")
    return {
        "reclaim": float(reclaim) if reclaim is not None else None,
        "do_not_chase": float(dnc) if dnc is not None else None,
        "daily_thesis_invalidation": float(inv) if inv is not None else None,
        "primary_support": float(primary_sup) if primary_sup is not None else None,
        "primary_resistance": float(primary_res) if primary_res is not None else None,
    }


def _bar_range(bar: HistoricalBar) -> float:
    return max(bar.high - bar.low, 0.01)


def _median_range_proxy(bar: HistoricalBar) -> float:
    return _bar_range(bar)


def _terms(objective_obs: dict[str, Any]) -> set[str]:
    terms = objective_obs.get("brooks_obs_json") or []
    return {str(t.get("term") if isinstance(t, dict) else t).upper() for t in terms}


def _pattern_families(patterns: list[dict[str, Any]], *, params: dict[str, Any]) -> list[dict[str, Any]]:
    stale = int(params["stale_pattern_bars"])
    out = []
    for p in patterns:
        fam = str(p.get("pattern_family", "")).upper()
        lc = str(p.get("lifecycle") or p.get("lifecycle_status", "")).upper()
        if lc in ("EXPIRED", "FAILED") and fam not in ("BEAR_MICRO_CHANNEL",):
            continue
        out.append({**p, "pattern_family": fam, "lifecycle": lc})
    return out


def _meaningful_long_patterns(patterns: list[dict[str, Any]], params: dict[str, Any]) -> list[dict[str, Any]]:
    allowed = {x.upper() for x in params["pattern_significance_entry_capable"]}  # type: ignore[index]
    obs_only = {x.upper() for x in params["pattern_significance_observation_only"]}  # type: ignore[index]
    out = []
    for p in patterns:
        fam = str(p.get("pattern_family", "")).upper()
        lc = str(p.get("lifecycle", "")).upper()
        if fam in obs_only:
            continue
        if fam in allowed and lc in ("CONFIRMED", "DEVELOPING", "POSSIBLE"):
            if fam == "LOCAL_DOUBLE_BOTTOM" and lc != "CONFIRMED":
                continue
            out.append(p)
    return out


def _support_interaction(
    *,
    close: float,
    low: float,
    support: float | None,
    bar: HistoricalBar,
    terms: set[str],
    params: dict[str, Any],
) -> tuple[str, list[str]]:
    rules: list[str] = []
    if support is None:
        return SUPPORT_ABOVE, rules
    med = _median_range_proxy(bar)
    tol = med * float(params["support_proximity_fraction"])
    dist = close - support
    if dist > tol * 2:
        return SUPPORT_ABOVE, rules
    if low > support + tol * 0.25:
        if dist <= tol * 2:
            rules.append("CTX_SUPPORT_APPROACH_V0_1")
            return SUPPORT_APPROACHING, rules
        return SUPPORT_ABOVE, rules
    if low <= support and close >= support:
        rules.append("CTX_SUPPORT_TEST_V0_1")
        return SUPPORT_TESTING, rules
    if close < support - tol * 0.5:
        rules.append("CTX_SUPPORT_FAIL_V0_1")
        return SUPPORT_FAILED, rules
    if low < support and close > support:
        rules.append("CTX_SUPPORT_PIERCE_V0_1")
        return SUPPORT_PIERCED, rules
    if "HIGHER_LOW" in terms or close > bar.open:
        rules.append("CTX_SUPPORT_HOLD_V0_1")
        return SUPPORT_HELD, rules
    rules.append("CTX_SUPPORT_TEST_V0_1")
    return SUPPORT_TESTING, rules


def _reclaim_stage_update(
    sess: SessionContextState,
    *,
    close: float,
    high: float,
    reclaim: float | None,
    bar: HistoricalBar,
    params: dict[str, Any],
) -> tuple[str, list[str]]:
    rules: list[str] = []
    if reclaim is None:
        return sess.reclaim_stage, rules
    med = _median_range_proxy(bar)
    if close < reclaim:
        if high >= reclaim and close < reclaim:
            rules.append("CTX_RECLAIM_TEST_V0_1")
            sess.reclaim_bars_above = 0
            return RECLAIM_TESTING, rules
        sess.reclaim_bars_above = 0
        return RECLAIM_BELOW, rules
    quality = (close - bar.low) / max(bar.high - bar.low, 0.01)
    if quality < float(params["reclaim_min_close_quality_fraction"]):
        rules.append("CTX_RECLAIM_WEAK_CLOSE_V0_1")
        return RECLAIM_TESTING, rules
    rules.append("CTX_RECLAIM_CLOSE_ABOVE_V0_1")
    sess.reclaim_bars_above += 1
    need = int(params["reclaim_confirm_bars"])
    if sess.reclaim_bars_above >= need:
        rules.append("CTX_RECLAIM_CONFIRMED_V0_1")
        return RECLAIM_CONFIRMED, rules
    return RECLAIM_CLOSED_ABOVE, rules


def _room_class(
    *,
    close: float,
    resistance: float | None,
    dnc: float | None,
    setup_inv: float | None,
    params: dict[str, Any],
) -> tuple[str, list[str]]:
    rules: list[str] = []
    if dnc is not None and close >= dnc:
        rules.append("CTX_ROOM_ABOVE_DNC_V0_1")
        return ROOM_ABOVE_DNC, rules
    if resistance is None:
        return ROOM_ACCEPTABLE, rules
    if setup_inv is not None and setup_inv < close:
        span = resistance - setup_inv
        rem = resistance - close
        if span <= 0:
            return ROOM_LIMITED, rules
        frac = rem / span
        if close >= resistance * 0.998:
            rules.append("CTX_ROOM_AT_RESISTANCE_V0_1")
            return ROOM_AT_RESISTANCE, rules
        if frac >= float(params["min_room_ample_fraction"]):
            return ROOM_AMPLE, rules
        if frac >= float(params["min_room_acceptable_fraction"]):
            return ROOM_ACCEPTABLE, rules
        rules.append("CTX_ROOM_LIMITED_V0_1")
        return ROOM_LIMITED, rules
    dist = resistance - close
    if dist <= 0:
        return ROOM_AT_RESISTANCE, rules
    return ROOM_ACCEPTABLE, rules


def _intraday_setup_invalidation(
    sess: SessionContextState,
    patterns: list[dict[str, Any]],
    levels: dict[str, float | None],
) -> float | None:
    for p in patterns:
        fam = str(p.get("pattern_family", "")).upper()
        if fam == "STRUCTURAL_DOUBLE_BOTTOM" and str(p.get("lifecycle", "")).upper() == "CONFIRMED":
            prices = p.get("relevant_prices_json") or p.get("relevant_prices") or {}
            l1 = prices.get("first_low")
            l2 = prices.get("second_low")
            if l1 is not None and l2 is not None:
                return min(float(l1), float(l2)) - 0.01
    if sess.setup_invalidation_level is not None:
        return sess.setup_invalidation_level
    if levels.get("primary_support") is not None:
        return float(levels["primary_support"]) - 0.05
    return None


def _init_from_dossier(sess: SessionContextState, dossier: dict[str, Any], params: dict[str, Any]) -> None:
    if sess.initialized:
        return
    verdict = str(dossier.get("paa_verdict") or "DEFAULT").upper()
    vmap = params["verdict_initial_state"]  # type: ignore[index]
    amap = params["verdict_initial_action"]  # type: ignore[index]
    sess.advisory_state = str(vmap.get(verdict) or vmap.get("DEFAULT"))
    sess.dossier_id = dossier.get("paa_analysis_id") or dossier.get("reconstruction_id")
    sess.initialized = True
    sess._initial_action = str(amap.get(verdict) or amap.get("DEFAULT"))  # type: ignore[attr-defined]


def _select_action(candidates: dict[str, str], params: dict[str, Any]) -> tuple[str, list[dict[str, str]]]:
    order = list(params["action_precedence"])  # type: ignore[arg-type]
    blocked: list[dict[str, str]] = []
    chosen: str | None = None
    chosen_key: str | None = None
    for key in order:
        action = candidates.get(key)
        if not action:
            continue
        if chosen is None:
            chosen = action
            chosen_key = key
            continue
        blocked.append({"candidate": action, "blocked_by": chosen})
    return chosen or ACTION_OBSERVE, blocked


def advance_context_v01_for_bar(
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
    levels = _dossier_levels(dossier)
    terms = _terms(objective_obs)
    pat_list = _pattern_families(patterns, params=params)
    meaningful = _meaningful_long_patterns(pat_list, params)

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
    room, room_rules = _room_class(
        close=bar.close,
        resistance=levels.get("primary_resistance"),
        dnc=levels.get("do_not_chase"),
        setup_inv=setup_inv,
        params=params,
    )

    supporting: list[dict[str, Any]] = []
    opposing: list[dict[str, Any]] = []
    blockers: list[str] = []
    rule_ids = list(sup_rules) + list(rec_rules) + list(room_rules)
    markers: list[str] = []

    if support_status == SUPPORT_TESTING:
        markers.append("support_test")
    if support_status == SUPPORT_HELD:
        markers.append("support_hold")
    if reclaim_stage == RECLAIM_TESTING:
        markers.append("reclaim_test")
    if reclaim_stage == RECLAIM_CONFIRMED:
        markers.append("reclaim_confirmed")

    thesis_effect = EFFECT_NEUTRAL
    if daily_inv is not None and bar.close < daily_inv:
        thesis_effect = EFFECT_INVALIDATES
        opposing.append({"type": INV_DAILY_THESIS, "level": daily_inv, "rule_id": "CTX_DAILY_THESIS_INVALID_V0_1"})
        rule_ids.append("CTX_DAILY_THESIS_INVALID_V0_1")
    elif setup_inv is not None and bar.close < setup_inv:
        thesis_effect = EFFECT_WEAKENS
        opposing.append({"type": INV_INTRADAY_SETUP, "level": setup_inv, "rule_id": "CTX_INTRADAY_SETUP_BREAK_V0_1"})
        rule_ids.append("CTX_INTRADAY_SETUP_BREAK_V0_1")
    elif support_status == SUPPORT_HELD and meaningful:
        thesis_effect = EFFECT_SLIGHTLY_CONFIRMS
        supporting.append({"fact": "support_held_with_pattern", "patterns": [p.get("pattern_family") for p in meaningful]})
    elif support_status == SUPPORT_HELD:
        thesis_effect = EFFECT_SLIGHTLY_CONFIRMS
        supporting.append({"fact": "support_held", "rule_id": "CTX_SUPPORT_HOLD_V0_1"})
    elif reclaim_stage == RECLAIM_CONFIRMED:
        thesis_effect = EFFECT_CONFIRMS
        supporting.append({"fact": "reclaim_confirmed", "rule_id": "CTX_RECLAIM_CONFIRMED_V0_1"})

    opening = bar_index_in_session < int(params["opening_observation_bars"])
    if opening and state_before in (STATE_WAITING_FOR_RTH_CONFIRMATION, STATE_OBSERVATION_ONLY):
        sess.advisory_state = STATE_OBSERVING_OPEN

    candidates: dict[str, str] = {}
    candidates["OBSERVE"] = ACTION_OBSERVE

    if thesis_effect == EFFECT_INVALIDATES:
        candidates["THESIS_INVALIDATED"] = ACTION_THESIS_INVALIDATED
        sess.advisory_state = STATE_THESIS_INVALIDATED
        markers.append("thesis_invalidated")
    elif room == ROOM_ABOVE_DNC or (levels.get("do_not_chase") and bar.close >= levels["do_not_chase"]):
        candidates["DO_NOT_CHASE"] = ACTION_DO_NOT_CHASE
        sess.advisory_state = STATE_DO_NOT_CHASE
        markers.append("do_not_chase")
        blockers.append("DO_NOT_CHASE_LEVEL")
    elif not dossier.get("trade_simulation_ready"):
        candidates["DO_NOT_ENTER"] = ACTION_DO_NOT_ENTER
        blockers.append("DOSSIER_NOT_SIMULATION_READY")
    elif room in (ROOM_LIMITED, ROOM_AT_RESISTANCE):
        candidates["ENTRY_BLOCKED"] = ACTION_DO_NOT_ENTER
        sess.advisory_state = STATE_ENTRY_BLOCKED
        blockers.append("LIMITED_ROOM_TO_RESISTANCE")
    elif thesis_effect == EFFECT_WEAKENS:
        candidates["THESIS_WEAKENED"] = ACTION_THESIS_WEAKENED
        sess.advisory_state = STATE_THESIS_WEAKENED
        markers.append("thesis_weakened")
    else:
        has_meaningful = bool(meaningful)
        reclaim_ok = reclaim_stage in (RECLAIM_CLOSED_ABOVE, RECLAIM_CONFIRMED)
        if has_meaningful and support_status in (SUPPORT_HELD, SUPPORT_TESTING, SUPPORT_APPROACHING):
            if sess.advisory_state not in (STATE_ENTRY_ARMED, STATE_WAITING_FOR_FOLLOW_THROUGH):
                sess.advisory_state = STATE_SETUP_DEVELOPING
                markers.append("setup_developing")
        if has_meaningful and support_status == SUPPORT_HELD and setup_inv and room in (ROOM_AMPLE, ROOM_ACCEPTABLE):
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
        elif str(dossier.get("paa_verdict", "")).upper() == "WAIT_PULLBACK":
            candidates["WAIT_FOR_PULLBACK"] = ACTION_WAIT_FOR_PULLBACK
            if sess.advisory_state == STATE_WAITING_FOR_PULLBACK:
                pass
        elif support_status in (SUPPORT_TESTING, SUPPORT_APPROACHING):
            candidates["WAIT_FOR_SUPPORT"] = ACTION_WAIT_FOR_SUPPORT
            sess.advisory_state = STATE_WAITING_FOR_SUPPORT_TEST

    if opening:
        candidates.pop("CONSIDER_ENTRY", None)
        if "CONSIDER_ENTRY" in [c.get("candidate") for c in []]:
            pass

    selected, blocked_candidates = _select_action(candidates, params)
    if selected == ACTION_OBSERVE and hasattr(sess, "_initial_action"):
        selected = getattr(sess, "_initial_action", ACTION_OBSERVE)

    objective_layer = {
        "layer": "OBJECTIVE_FACT",
        "terms": list(terms),
        "ohlcv": {"open": bar.open, "high": bar.high, "low": bar.low, "close": bar.close},
    }
    pattern_layer = {
        "layer": "PATTERN_INSTANCE",
        "patterns": [{"family": p.get("pattern_family"), "lifecycle": p.get("lifecycle")} for p in pat_list],
    }
    context_layer = {
        "layer": "CONTEXTUAL_INTERPRETATION",
        "support_status": support_status,
        "reclaim_stage": reclaim_stage,
        "room_class": room,
        "opening_period": opening,
        "narrative": _narrative(support_status, reclaim_stage, room, meaningful),
    }
    advisory_state_layer = {"layer": "ADVISORY_STATE", "state": sess.advisory_state}
    advisory_action_layer = {"layer": "ADVISORY_ACTION", "action": selected}

    layers = {
        "objective_fact": objective_layer,
        "pattern_instance": pattern_layer,
        "contextual_interpretation": context_layer,
        "advisory_state": advisory_state_layer,
        "advisory_action": advisory_action_layer,
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
        explanation=_narrative(support_status, reclaim_stage, room, meaningful),
        context_classifications=[support_status, reclaim_stage, room],
        marker_flags=markers,
        rule_ids=rule_ids,
    )


def _narrative(support: str, reclaim: str, room: str, meaningful: list) -> str:
    parts = [f"Support={support}", f"Reclaim={reclaim}", f"Room={room}"]
    if meaningful:
        parts.append(f"Patterns={','.join(p.get('pattern_family', '') for p in meaningful[:3])}")
    return "; ".join(parts)


def context_sequence_hash(run_id: str, *, context_attempt_id: str) -> str:
    from .context_repository import context_sequence_hash as repo_hash

    return repo_hash(run_id, context_attempt_id=context_attempt_id)
