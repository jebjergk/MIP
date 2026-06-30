"""
Phase 4 proposal board — deterministic pre-screen appeal scoring.

Ranks eligible dossiers before the max_candidates cap. Higher score = more
appealing structural opportunity. Portfolio-agnostic: uses dossier evidence
only (setup events, levels, candles, history trust).

Not a trade verdict — direction and PROPOSE/NO_TRADE remain with the chair.
"""
from __future__ import annotations

from datetime import date as _date
from typing import Any, Dict, List, Tuple

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


def score_candidate(
    payload: Dict[str, Any],
    as_of: _date,
    *,
    symbol: str = "",
) -> Tuple[float, Dict[str, Any]]:
    """Return (appeal_score, breakdown_dict) for one dossier payload."""
    payload = payload if isinstance(payload, dict) else {}

    structure = payload.get("structure") or {}
    regime = payload.get("regime") or {}
    levels = payload.get("levels") or {}
    setup_events = payload.get("setup_events_evidence_only") or []
    candle_sequence = payload.get("candle_sequence") or []
    price = payload.get("price") or {}
    history = payload.get("history") or {}

    recent_setup, recent_setup_event = _has_recent_setup_event(setup_events, as_of)
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

    # Fresh actionable setups — strongest signal.
    if fresh_setups > 0:
        setup_pts = min(90.0, float(fresh_setups) * 30.0)
        score += setup_pts
        breakdown["fresh_setup_points"] = setup_pts
        if recent_setup_event:
            breakdown["best_setup_status"] = recent_setup_event.get("setup_status")
            breakdown["best_setup_family"] = recent_setup_event.get("setup_family")

    # Board-publishable setup anchor — required for executable PROPOSE.
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

    # Proximity to key level (closer is better).
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

    # Depth of structural evidence (weaker tie-breaker).
    if n_events > 0:
        depth_pts = min(12.0, float(n_events) * 0.4)
        score += depth_pts
        breakdown["evidence_depth_points"] = round(depth_pts, 2)

    if not has_history:
        score -= 15.0
        breakdown["no_history_penalty"] = -15.0

    # Trust / confidence hints from dossier payload when present.
    conf = _to_float(payload.get("structure_confidence"))
    if conf is not None and conf > 0:
        conf_pts = min(10.0, conf * 10.0)
        score += conf_pts
        breakdown["structure_confidence_points"] = round(conf_pts, 2)

    breakdown["prescreen_score"] = round(score, 2)
    return round(score, 4), breakdown


def rank_eligible_rows(
    rows: List[Tuple[int, str, str, Dict[str, Any]]],
    as_of: _date,
) -> List[Tuple[int, str, str, Dict[str, Any], float, Dict[str, Any]]]:
    """Sort eligible dossier rows by descending appeal score (fresh each day)."""
    scored: List[Tuple[int, str, str, Dict[str, Any], float, Dict[str, Any]]] = []
    for did, sym, mkt, payload in rows:
        pts, breakdown = score_candidate(payload, as_of, symbol=sym)
        scored.append((did, sym, mkt, payload, pts, breakdown))
    scored.sort(key=lambda r: (-r[4], r[1]))
    return scored
