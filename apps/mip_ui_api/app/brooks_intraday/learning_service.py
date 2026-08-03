"""Phase 8 learning view API wiring."""

from __future__ import annotations

from datetime import date

from fastapi import HTTPException

from . import store
from .learning_view import (
    build_account_timeline,
    build_certification_summary,
    build_run_learning_payload,
    build_state_transitions_payload,
    build_symbol_learning_payload,
    resolve_attempt_chain,
)
from .repository import load_dossiers_for_run


def _run_state(run_id: str) -> tuple[dict, dict, list[str]]:
    run = store.get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")
    with store._lock:
        state = dict(store._runs.get(run_id) or {})
    cfg = dict(run.configuration or {})
    symbols = list(run.symbols or [])
    return state, cfg, symbols


def get_learning_view(
    run_id: str,
    *,
    mode: str = "full",
    active_symbol: str | None = None,
    trading_date: date | None = None,
) -> dict:
    state, cfg, symbols = _run_state(run_id)
    dossiers = load_dossiers_for_run(run_id)
    payload = build_run_learning_payload(
        run_id=run_id,
        state=state,
        cfg=cfg,
        symbols=symbols,
        dossiers=dossiers,
        mode=mode,
        active_symbol=active_symbol,
        active_trading_date=trading_date,
    )
    payload["simulation_banner"] = payload["provenance"].get("simulation_only_banner")
    return payload


def get_symbol_learning_view(
    run_id: str,
    symbol: str,
    *,
    mode: str = "full",
    trading_date: date | None = None,
    offset: int = 0,
    limit: int = 500,
    pattern_family: str | None = None,
    lifecycle: str | None = None,
    thesis_effect: str | None = None,
    advisory_state: str | None = None,
    advisory_action: str | None = None,
    transition_events_only: bool = False,
    blockers: bool = False,
    simulated_trade_events: bool = False,
    action_changed_only: bool = False,
    meaningful_pattern_no_entry: bool = False,
) -> dict:
    state, cfg, symbols = _run_state(run_id)
    sym = symbol.strip().upper()
    if sym not in [s.upper() for s in symbols]:
        raise HTTPException(status_code=404, detail=f"Symbol {sym} is not in this run.")
    attempts = resolve_attempt_chain(run_id, state, cfg)
    dossiers = load_dossiers_for_run(run_id)
    dossier = None
    if trading_date:
        for d in dossiers:
            if str(d.get("symbol", "")).upper() == sym and str(d.get("trading_date", "")).startswith(
                trading_date.isoformat()
            ):
                dossier = d
                break
    filters = {
        "pattern_family": pattern_family,
        "lifecycle": lifecycle,
        "thesis_effect": thesis_effect,
        "advisory_state": advisory_state,
        "advisory_action": advisory_action,
        "transition_events_only": transition_events_only,
        "blockers": blockers,
        "simulated_trade_events": simulated_trade_events,
        "action_changed_only": action_changed_only,
        "meaningful_pattern_no_entry": meaningful_pattern_no_entry,
    }
    return build_symbol_learning_payload(
        run_id=run_id,
        symbol=sym,
        state=state,
        cfg=cfg,
        attempts=attempts,
        mode=mode,
        trading_date=trading_date,
        offset=offset,
        limit=limit,
        filters=filters,
        dossier=dossier,
    )


def get_state_transitions(
    run_id: str,
    *,
    symbol: str | None = None,
    mode: str = "full",
) -> dict:
    state, cfg, _ = _run_state(run_id)
    attempts = resolve_attempt_chain(run_id, state, cfg)
    return build_state_transitions_payload(
        run_id, attempts, symbol=symbol, mode=mode, state=state
    )


def get_account_timeline(run_id: str) -> dict:
    state, cfg, _ = _run_state(run_id)
    attempts = resolve_attempt_chain(run_id, state, cfg)
    return build_account_timeline(run_id, attempts.get("simulation_attempt_id"))


def get_certification_summary(*, include_fixtures: bool = False) -> dict:
    return build_certification_summary(include_fixtures=include_fixtures)
