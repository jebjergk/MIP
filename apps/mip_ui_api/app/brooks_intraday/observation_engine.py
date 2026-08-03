"""Deterministic objective bar observation for Phase 4 (visible history only)."""

from __future__ import annotations

from datetime import date
from typing import Any

from .bars import HistoricalBar
from .objective_ruleset_v01 import (
    DEFAULT_PARAMETERS,
    RULESET_VERSION,
    build_explanation,
    compute_geometry,
    evaluate_brooks_terms,
    geometry_to_derived_metrics,
    prior_bar_relationship,
    relative_metrics,
)


def _session_key(symbol: str, trading_date: date) -> str:
    return f"{symbol.upper()}|{trading_date.isoformat()}"


def get_session_state(state: dict[str, Any], symbol: str, trading_date: date) -> dict[str, Any]:
    bucket = state.setdefault("_obs_session_state", {})
    key = _session_key(symbol, trading_date)
    if key not in bucket:
        bucket[key] = {
            "consecutive_bull": 0,
            "consecutive_bear": 0,
            "prior_bar_flags": {},
            "geometries": [],
        }
    return bucket[key]


def reset_session_state_for_date(state: dict[str, Any], symbol: str, trading_date: date) -> None:
    bucket = state.setdefault("_obs_session_state", {})
    bucket[_session_key(symbol, trading_date)] = {
        "consecutive_bull": 0,
        "consecutive_bear": 0,
        "prior_bar_flags": {},
        "geometries": [],
    }


def observe_bar(
    *,
    state: dict[str, Any],
    symbol: str,
    trading_date: date,
    bar: HistoricalBar,
    session_bar_index: int,
    visible_same_session: list[HistoricalBar],
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Analyze one bar using only bars at or before the cursor within the same session."""
    params = dict(DEFAULT_PARAMETERS if params is None else {**DEFAULT_PARAMETERS, **params})
    sess = get_session_state(state, symbol, trading_date)

    geometry = compute_geometry(
        open_=bar.open,
        high=bar.high,
        low=bar.low,
        close=bar.close,
        volume=bar.volume,
    )
    prior_geom = sess["geometries"][-1] if sess["geometries"] else None
    prior_bar = visible_same_session[-2] if len(visible_same_session) >= 2 else None
    if session_bar_index == 0:
        prior_geom = None
        prior_bar = None

    rel = relative_metrics(geometry, sess["geometries"], params)
    relationship = prior_bar_relationship(
        geometry,
        prior_geom,
        params=params,
    )
    if session_bar_index == 0:
        relationship = prior_bar_relationship(geometry, None, params=params)

    if prior_geom is not None:
        relationship["_prior_high"] = prior_geom.high
        relationship["_prior_low"] = prior_geom.low
        session_state_copy = {**sess, "_prior_high": prior_geom.high, "_prior_low": prior_geom.low}
    else:
        session_state_copy = dict(sess)

    facts, brooks_terms, rule_evaluations = evaluate_brooks_terms(
        geometry=geometry,
        relative=rel,
        relationship=relationship,
        session_state=session_state_copy,
        params=params,
    )
    # Apply session counter updates from evaluate
    sess["consecutive_bull"] = session_state_copy.get("consecutive_bull", 0)
    sess["consecutive_bear"] = session_state_copy.get("consecutive_bear", 0)
    sess["prior_bar_flags"] = session_state_copy.get("prior_bar_flags", {})

    derived = geometry_to_derived_metrics(geometry, rel, relationship)
    explanation = build_explanation(geometry, rel, relationship, facts)
    rule_ids = [r["rule_id"] for r in rule_evaluations if r.get("result")]

    sess["geometries"].append(geometry)

    objective_facts = {
        "facts": facts,
        "ohlcv": {
            "open": bar.open,
            "high": bar.high,
            "low": bar.low,
            "close": bar.close,
            "volume": bar.volume,
        },
    }
    context = {
        "phase": "OBJECTIVE_BROOKS_V0_1",
        "ruleset_version": RULESET_VERSION,
        "session_bar_index": session_bar_index,
        "brooks_terms": brooks_terms,
    }
    action = "WAIT" if geometry.data_quality != "COMPLETE" else "OBSERVE"

    return {
        "objective_facts_json": objective_facts,
        "derived_metrics_json": derived,
        "brooks_obs_json": brooks_terms,
        "rule_evaluations_json": rule_evaluations,
        "pattern_state_json": [],
        "context_json": context,
        "state_before": "DOSSIER_READY",
        "state_after": "OBSERVING",
        "action": action,
        "blockers_json": [],
        "explanation": explanation,
        "rule_ids": rule_ids,
        "data_quality_status": geometry.data_quality if geometry.data_quality != "COMPLETE" else "COMPLETE",
        "visible_history_count": len(visible_same_session),
        "ruleset_version": RULESET_VERSION,
    }
