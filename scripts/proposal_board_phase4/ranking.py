"""
Phase 4 proposal board — deterministic pre-screen ranking.

Ranks eligible dossiers before the max_candidates cap. Combines:
  * structural appeal (fresh setups, levels, trust history)
  * execution readiness (dossier actionability_context — continuation,
    confirmation, entry location, regime alignment, path metrics)

Higher combined_rank_score = more likely to survive shadow-board revalidation.
Direction and PROPOSE/NO_TRADE remain with the chair agents.
"""
from __future__ import annotations

from datetime import date as _date
from typing import Any, Dict, List, Optional, Tuple

from .review_eligibility import (
    _has_abnormal_candle,
    _has_any_history,
    _has_recent_setup_event,
    _has_recent_state_or_regime_change,
    _has_strong_structural_trust,
    _is_near_key_level,
    _parse_date,
    _to_float,
)

_FRESH_SETUP_STATUSES = frozenset({"ELIGIBLE", "DETECTED", "ACTIVE", "CONFIRMED", "WAITING"})
# Statuses that can anchor chair PROPOSE + publish (matches dossier primary_evidence rule).
_BOARD_ELIGIBLE_SETUP_STATUSES = frozenset({"DETECTED", "ELIGIBLE", "WAITING", "STALE"})
_PROPOSAL_READY_TRUST_LABELS = frozenset({"TRUSTED", "PROVISIONAL"})


def _count_fresh_eligible_setups(payload: Dict[str, Any], as_of: _date) -> int:
    events = payload.get("setup_events_evidence_only") if isinstance(payload, dict) else None
    if not isinstance(events, list):
        return 0
    count = 0
    for ev in events:
        if not isinstance(ev, dict):
            continue
        status = str(ev.get("setup_status") or "").upper()
        if status not in _FRESH_SETUP_STATUSES:
            continue
        sdate = _parse_date(ev.get("setup_date"))
        if sdate is None:
            continue
        age = (as_of - sdate).days
        if 0 <= age <= 10:
            count += 1
    return count


def _count_board_eligible_setups(payload: Dict[str, Any], as_of: _date) -> int:
    events = payload.get("setup_events_evidence_only") if isinstance(payload, dict) else None
    if not isinstance(events, list):
        return 0
    count = 0
    for ev in events:
        if not isinstance(ev, dict):
            continue
        status = str(ev.get("setup_status") or "").upper()
        if status not in _BOARD_ELIGIBLE_SETUP_STATUSES:
            continue
        sdate = _parse_date(ev.get("setup_date"))
        if sdate is None:
            continue
        age = (as_of - sdate).days
        if 0 <= age <= 10:
            count += 1
    return count


def _trust_label_by_family(history: Dict[str, Any]) -> Dict[str, str]:
    labels: Dict[str, str] = {}
    if not isinstance(history, dict):
        return labels
    for side_key in ("long_history", "short_history"):
        side = history.get(side_key)
        if not isinstance(side, list):
            continue
        for entry in side:
            if not isinstance(entry, dict):
                continue
            fam = str(entry.get("setup_family") or "").strip().upper()
            if not fam:
                continue
            labels[fam] = str(entry.get("trust_label") or "").strip().upper()
    return labels


def _has_proposal_ready_active_setup(payload: Dict[str, Any], as_of: _date) -> bool:
    events = payload.get("setup_events_evidence_only") if isinstance(payload, dict) else None
    if not isinstance(events, list):
        return False
    trust_by_family = _trust_label_by_family(payload.get("history") or {})
    for ev in events:
        if not isinstance(ev, dict):
            continue
        status = str(ev.get("setup_status") or "").upper()
        if status not in _BOARD_ELIGIBLE_SETUP_STATUSES:
            continue
        sdate = _parse_date(ev.get("setup_date"))
        if sdate is None:
            continue
        age = (as_of - sdate).days
        if age < 0 or age > 10:
            continue
        fam = str(ev.get("setup_family") or "").strip().upper()
        if trust_by_family.get(fam) in _PROPOSAL_READY_TRUST_LABELS:
            return True
    return False


def _count_setup_evidence(payload: Dict[str, Any]) -> int:
    events = payload.get("setup_events_evidence_only") if isinstance(payload, dict) else None
    if not isinstance(events, list):
        return 0
    return sum(1 for ev in events if isinstance(ev, dict))


def _infer_setup_direction(
    payload: Dict[str, Any],
    setup_events: List[Any],
) -> Optional[str]:
    """Best-effort direction from primary evidence setup or newest eligible event."""
    if not isinstance(setup_events, list):
        return None
    primary_id = payload.get("primary_evidence_setup_event_id")
    if primary_id is not None:
        try:
            pid = int(primary_id)
        except (TypeError, ValueError):
            pid = None
        if pid is not None:
            for ev in setup_events:
                if not isinstance(ev, dict):
                    continue
                eid = ev.get("setup_event_id")
                try:
                    if int(eid) == pid:
                        d = str(ev.get("event_direction") or ev.get("direction") or "").upper()
                        if d in {"LONG", "SHORT"}:
                            return d
                except (TypeError, ValueError):
                    continue
    for ev in setup_events:
        if not isinstance(ev, dict):
            continue
        status = str(ev.get("setup_status") or "").upper()
        if status not in _BOARD_ELIGIBLE_SETUP_STATUSES:
            continue
        d = str(ev.get("event_direction") or ev.get("direction") or "").upper()
        if d in {"LONG", "SHORT"}:
            return d
    return None


def _best_path_metrics(
    history: Dict[str, Any],
    setup_events: List[Any],
    payload: Dict[str, Any],
) -> Tuple[Optional[float], Optional[float]]:
    """Meaningful hit rate and path survival for the primary setup family."""
    if not isinstance(history, dict):
        return None, None
    fam = None
    primary_id = payload.get("primary_evidence_setup_event_id")
    if isinstance(setup_events, list) and primary_id is not None:
        try:
            pid = int(primary_id)
        except (TypeError, ValueError):
            pid = None
        if pid is not None:
            for ev in setup_events:
                if isinstance(ev, dict) and int(ev.get("setup_event_id") or -1) == pid:
                    fam = str(ev.get("setup_family") or "").strip().upper()
                    break
    best_mhr: Optional[float] = None
    best_survival: Optional[float] = None
    for side_key in ("long_history", "short_history"):
        side = history.get(side_key)
        if not isinstance(side, list):
            continue
        for entry in side:
            if not isinstance(entry, dict):
                continue
            if fam and str(entry.get("setup_family") or "").strip().upper() != fam:
                continue
            mhr = _to_float(entry.get("meaningful_hit_rate"))
            surv = _to_float(entry.get("path_survival_hit_rate"))
            if mhr is not None and (best_mhr is None or mhr > best_mhr):
                best_mhr = mhr
            if surv is not None and (best_survival is None or surv > best_survival):
                best_survival = surv
    return best_mhr, best_survival


def score_structural_appeal(
    payload: Dict[str, Any],
    as_of: _date,
) -> Tuple[float, Dict[str, Any]]:
    """Legacy structural-only appeal score (setup freshness, levels, trust)."""
    payload = payload if isinstance(payload, dict) else {}

    structure = payload.get("structure") or {}
    regime = payload.get("regime") or {}
    levels = payload.get("levels") or {}
    setup_events = payload.get("setup_events_evidence_only") or []
    candle_sequence = payload.get("candle_sequence") or []
    price = payload.get("price") or {}
    history = payload.get("history") or {}

    _recent_setup, recent_setup_event = _has_recent_setup_event(setup_events, as_of)
    state_change, _state_src = _has_recent_state_or_regime_change(structure, regime, as_of)
    near_level, nearest_pct = _is_near_key_level(levels)
    abnormal_candle = _has_abnormal_candle(candle_sequence, price)
    strong_trust = _has_strong_structural_trust(history)
    has_history = _has_any_history(history)
    fresh_setups = _count_fresh_eligible_setups(payload, as_of)
    board_eligible = _count_board_eligible_setups(payload, as_of)
    proposal_ready_active = _has_proposal_ready_active_setup(payload, as_of)
    primary_setup_id = payload.get("primary_evidence_setup_event_id")
    has_primary_setup = primary_setup_id is not None
    n_events = _count_setup_evidence(payload)

    score = 0.0
    breakdown: Dict[str, Any] = {
        "fresh_setups_10d": fresh_setups,
        "board_eligible_setups_10d": board_eligible,
        "has_primary_setup_event": has_primary_setup,
        "proposal_ready_active_setup": proposal_ready_active,
        "setup_evidence_count": n_events,
        "nearest_level_distance_pct": nearest_pct,
    }

    if fresh_setups > 0:
        setup_pts = min(90.0, float(fresh_setups) * 30.0)
        score += setup_pts
        breakdown["fresh_setup_points"] = setup_pts
        if recent_setup_event:
            breakdown["best_setup_status"] = recent_setup_event.get("setup_status")
            breakdown["best_setup_family"] = recent_setup_event.get("setup_family")

    if board_eligible > 0:
        board_pts = min(75.0, 45.0 + float(max(0, board_eligible - 1)) * 15.0)
        score += board_pts
        breakdown["board_eligible_setup_points"] = board_pts
    elif not has_primary_setup:
        score -= 50.0
        breakdown["no_board_eligible_penalty"] = -50.0

    if has_primary_setup:
        score += 35.0
        breakdown["primary_setup_event_points"] = 35.0

    if proposal_ready_active:
        score += 30.0
        breakdown["proposal_ready_family_points"] = 30.0

    if near_level and nearest_pct is not None:
        level_pts = max(5.0, 35.0 - (nearest_pct * 4.0))
        score += level_pts
        breakdown["near_level_points"] = round(level_pts, 2)

    if abnormal_candle:
        score += 12.0
        breakdown["abnormal_candle_points"] = 12.0

    if strong_trust:
        score += 18.0
        breakdown["strong_trust_points"] = 18.0

    if state_change:
        score += 6.0
        breakdown["state_change_points"] = 6.0

    if n_events > 0:
        depth_pts = min(12.0, float(n_events) * 0.4)
        score += depth_pts
        breakdown["evidence_depth_points"] = round(depth_pts, 2)

    if not has_history:
        score -= 15.0
        breakdown["no_history_penalty"] = -15.0

    conf = _to_float(payload.get("structure_confidence"))
    if conf is not None and conf > 0:
        conf_pts = min(10.0, conf * 10.0)
        score += conf_pts
        breakdown["structure_confidence_points"] = round(conf_pts, 2)

    breakdown["structural_appeal_score"] = round(score, 2)
    return round(score, 4), breakdown


def score_execution_readiness(
    payload: Dict[str, Any],
    as_of: _date,  # noqa: ARG001 — reserved for future bar-age checks
    *,
    symbol: str = "",
) -> Tuple[float, Dict[str, Any]]:
    """
    Deterministic execution readiness from dossier actionability_context.
    Aligns pre-screen ranking with shadow-board entry geometry / regime lenses.
    """
    _ = symbol
    payload = payload if isinstance(payload, dict) else {}
    act = payload.get("actionability_context") or {}
    if not isinstance(act, dict):
        act = {}
    regime = payload.get("regime") or {}
    tags = regime.get("tags") if isinstance(regime.get("tags"), dict) else {}
    trend = str(
        tags.get("trend_regime")
        or regime.get("trend_regime")
        or ""
    ).upper()
    setup_events = payload.get("setup_events_evidence_only") or []
    history = payload.get("history") or {}

    score = 0.0
    breakdown: Dict[str, Any] = {
        "continuation_quality": act.get("continuation_quality"),
        "confirmation_needed": act.get("confirmation_needed"),
        "target_path_clear": act.get("target_path_clear"),
        "entry_location_quality": act.get("entry_location_quality"),
        "resistance_overhead_risk": act.get("resistance_overhead_risk"),
    }

    cont = str(act.get("continuation_quality") or "").upper()
    if cont == "CONFIRMED":
        score += 50.0
        breakdown["continuation_points"] = 50.0
    elif cont == "UNCONFIRMED":
        score += 10.0
        breakdown["continuation_points"] = 10.0
    elif cont == "CONTESTED":
        score -= 10.0
        breakdown["continuation_points"] = -10.0
    elif cont == "REJECTED":
        score -= 40.0
        breakdown["continuation_points"] = -40.0

    if act.get("target_path_clear") is True:
        score += 30.0
        breakdown["target_path_clear_points"] = 30.0
    elif act.get("target_path_clear") is False:
        score -= 10.0
        breakdown["target_path_clear_points"] = -10.0

    if act.get("confirmation_needed") is False:
        score += 25.0
        breakdown["no_confirmation_needed_points"] = 25.0
    elif act.get("confirmation_needed") is True:
        score -= 20.0
        breakdown["confirmation_needed_penalty"] = -20.0

    entry = str(act.get("entry_location_quality") or "").upper()
    if entry in {"AT_SUPPORT", "AT_BROKEN_RESISTANCE_SUPPORT"}:
        score += 20.0
        breakdown["entry_location_points"] = 20.0
    elif entry == "BELOW_RESISTANCE_OVERHEAD":
        score += 10.0
        breakdown["entry_location_points"] = 10.0
    elif entry == "MID_RANGE":
        score += 5.0
        breakdown["entry_location_points"] = 5.0
    elif entry == "AT_RESISTANCE":
        score -= 15.0
        breakdown["entry_location_points"] = -15.0

    oh = str(act.get("resistance_overhead_risk") or "").upper()
    if oh == "CLEAR":
        score += 15.0
        breakdown["overhead_risk_points"] = 15.0
    elif oh == "LOW":
        score += 8.0
        breakdown["overhead_risk_points"] = 8.0
    elif oh == "MODERATE":
        breakdown["overhead_risk_points"] = 0.0
    elif oh == "HIGH":
        score -= 20.0
        breakdown["overhead_risk_points"] = -20.0

    direction = _infer_setup_direction(payload, setup_events)
    breakdown["inferred_setup_direction"] = direction
    if direction == "LONG":
        if "TREND_UP" in trend:
            score += 15.0
            breakdown["regime_alignment_points"] = 15.0
        elif "TREND_DOWN" in trend:
            score -= 20.0
            breakdown["regime_alignment_points"] = -20.0
    elif direction == "SHORT":
        if "TREND_DOWN" in trend:
            score += 15.0
            breakdown["regime_alignment_points"] = 15.0
        elif "TREND_UP" in trend:
            score -= 20.0
            breakdown["regime_alignment_points"] = -20.0

    mhr, survival = _best_path_metrics(history, setup_events, payload)
    if mhr is not None:
        breakdown["meaningful_hit_rate"] = mhr
        if mhr >= 0.70:
            score += 15.0
            breakdown["path_hit_rate_points"] = 15.0
        elif mhr >= 0.55:
            score += 5.0
            breakdown["path_hit_rate_points"] = 5.0
    if survival is not None:
        breakdown["path_survival_hit_rate"] = survival
        if survival >= 0.45:
            score += 10.0
            breakdown["path_survival_points"] = 10.0

    cluster = str(act.get("active_candle_cluster") or "").upper()
    if cluster in {"UPPER_ZONE_REJECTION_CLUSTER", "SELLER_PRESSURE_AFTER_ADVANCE"}:
        if direction != "SHORT":
            score -= 15.0
            breakdown["rejection_cluster_penalty"] = -15.0
    elif cluster in {"BREAKOUT_FOLLOW_THROUGH", "ORDERLY_PULLBACK", "LOWER_WICK_ACCUMULATION"}:
        if direction == "LONG":
            score += 10.0
            breakdown["bullish_cluster_points"] = 10.0

    breakdown["execution_readiness_score"] = round(score, 2)
    return round(score, 4), breakdown


def score_candidate(
    payload: Dict[str, Any],
    as_of: _date,
    *,
    symbol: str = "",
) -> Tuple[float, Dict[str, Any]]:
    """Return (combined_rank_score, breakdown_dict) for one dossier payload."""
    structural, struct_bd = score_structural_appeal(payload, as_of)
    execution, exec_bd = score_execution_readiness(payload, as_of, symbol=symbol)
    breakdown = {**struct_bd, **exec_bd}
    combined = structural + execution
    breakdown["prescreen_score"] = round(combined, 2)
    breakdown["combined_rank_score"] = round(combined, 2)
    return round(combined, 4), breakdown


def rank_eligible_rows(
    rows: List[Tuple[int, str, str, Dict[str, Any]]],
    as_of: _date,
) -> List[Tuple[int, str, str, Dict[str, Any], float, Dict[str, Any]]]:
    """Sort eligible dossier rows by descending combined rank score."""
    scored: List[Tuple[int, str, str, Dict[str, Any], float, Dict[str, Any]]] = []
    for did, sym, mkt, payload in rows:
        pts, breakdown = score_candidate(payload, as_of, symbol=sym)
        scored.append((did, sym, mkt, payload, pts, breakdown))
    scored.sort(key=lambda r: (-r[4], r[1]))
    return scored
