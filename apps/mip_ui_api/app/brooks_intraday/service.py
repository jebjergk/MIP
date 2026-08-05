"""
Brooks Intraday Lab API service.

Isolation: this module reads/writes only BROOKS_INTRADAY_* tables and in-memory
simulation state. It must not call live execution, IBKR, LPA, or portfolio APIs.
"""

from __future__ import annotations

from datetime import date

from fastapi import HTTPException

from . import store
from .constants import SIMULATION_BANNER
from .errors import BrooksIntradayError
from . import replay_service
from .models import (
    CreateRunRequest,
    CreateRunResponse,
    DossierPreview,
    ObservationsPage,
    PreparationResponse,
    RunDetail,
    RulesetInfo,
    SimpleStatusResponse,
    TradesPage,
)
from .repository import load_dossiers_for_run


def _http_error(exc: BrooksIntradayError) -> HTTPException:
    return HTTPException(status_code=exc.status_code, detail=exc.to_payload())


def create_run(request: CreateRunRequest) -> CreateRunResponse:
    try:
        run = store.create_run(request)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return CreateRunResponse(run=run)


def get_run_or_404(run_id: str) -> RunDetail:
    run = store.get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")
    return run


def prepare_run(run_id: str) -> PreparationResponse:
    try:
        get_run_or_404(run_id)
        preparation = store.run_prepare(run_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}") from exc
    except BrooksIntradayError as exc:
        raise _http_error(exc) from exc
    return PreparationResponse(
        run_id=run_id,
        preparation_status=preparation.get("preparation_status", "PREPARATION_FAILED"),
        preparation=preparation,
    )


def get_preparation(run_id: str) -> PreparationResponse:
    run = get_run_or_404(run_id)
    preparation = run.preparation or {}
    return PreparationResponse(
        run_id=run_id,
        preparation_status=run.preparation_status,
        preparation=preparation,
    )


def start_run(run_id: str) -> SimpleStatusResponse:
    return replay_service.start_run(run_id)


def pause_run(run_id: str) -> SimpleStatusResponse:
    return replay_service.pause_run(run_id)


def resume_run(run_id: str) -> SimpleStatusResponse:
    return replay_service.resume_run(run_id)


def stop_run(run_id: str) -> SimpleStatusResponse:
    return replay_service.stop_run(run_id)


def reset_run(run_id: str) -> SimpleStatusResponse:
    return replay_service.reset_run(run_id)


def next_bar(run_id: str, *, request_token: str | None = None) -> dict:
    return replay_service.next_bar(run_id, request_token=request_token)


def get_replay_state(run_id: str) -> dict:
    return replay_service.get_replay_state(run_id)


def get_visible_bars(run_id: str, symbol: str | None = None) -> dict:
    return replay_service.get_visible_bars(run_id, symbol)


def get_summary(run_id: str) -> RunDetail:
    return get_run_or_404(run_id)


def get_symbols(run_id: str) -> dict:
    run = get_run_or_404(run_id)
    return {
        "run_id": run.run_id,
        "simulation_banner": SIMULATION_BANNER,
        "symbols": [s.model_dump() for s in run.symbol_snapshots],
    }


def get_symbol_detail(run_id: str, symbol: str) -> dict:
    run = get_run_or_404(run_id)
    sym = symbol.strip().upper()
    if sym not in run.symbols:
        raise HTTPException(status_code=404, detail=f"Symbol {sym} is not in this run.")
    match = next((s for s in run.symbol_snapshots if s.symbol == sym), None)
    return {
        "run_id": run.run_id,
        "symbol": sym,
        "snapshot": match.model_dump() if match else {},
        "workspace_ready": bool(run.preparation_status == "READY"),
        "message": "Dossier loaded when preparation is READY.",
    }


def get_observations(run_id: str, symbol: str, *, review_filter: str | None = None) -> ObservationsPage:
    payload = replay_service.get_observations_for_symbol(run_id, symbol, review_filter=review_filter)
    return ObservationsPage(
        run_id=run_id,
        symbol=symbol.upper(),
        observations=payload.get("observations") or [],
        total=payload.get("total") or 0,
    )


def list_replay_attempts(run_id: str) -> dict:
    get_run_or_404(run_id)
    return replay_service.list_replay_attempts(run_id)


def reset_pattern_run(run_id: str) -> SimpleStatusResponse:
    from . import replay_service

    return replay_service.reset_pattern_run(run_id)


def get_pattern_review(run_id: str) -> dict:
    from . import replay_service

    return replay_service.pattern_review_summary(run_id)


def get_context_observations(run_id: str, symbol: str) -> dict:
    from .context_repository import load_context_observations

    run = get_run_or_404(run_id)
    cfg = run.configuration or {}
    with store._lock:
        state = store._runs.get(run_id) or {}
    ctx_id = (
        state.get("phase6b_context_attempt_id")
        or cfg.get("phase6b_context_attempt_id")
        or state.get("phase6_context_attempt_id")
        or cfg.get("phase6_context_attempt_id")
    )
    if not ctx_id:
        return {"run_id": run_id, "symbol": symbol.upper(), "observations": [], "total": 0}
    rows = load_context_observations(run_id, context_attempt_id=str(ctx_id), symbol=symbol, limit=5000)
    return {"run_id": run_id, "symbol": symbol.upper(), "context_attempt_id": ctx_id, "observations": rows, "total": len(rows)}


def get_trades(run_id: str) -> TradesPage:
    from .simulation_repository import load_sim_trades

    run = get_run_or_404(run_id)
    cfg = run.configuration or {}
    sim_id = cfg.get("phase7_simulation_attempt_id")
    rows = load_sim_trades(run_id, simulation_attempt_id=sim_id)
    return TradesPage(run_id=run_id, trades=rows)


def get_rulesets() -> list[RulesetInfo]:
    return [RulesetInfo(**row) for row in store.list_rulesets()]


def get_dossier_preview(symbol: str, trading_date: date) -> DossierPreview:
    preview = store.dossier_preview(symbol, trading_date)
    return DossierPreview(**preview)


def list_run_dossiers(run_id: str) -> dict:
    get_run_or_404(run_id)
    rows = load_dossiers_for_run(run_id)
    return {"run_id": run_id, "dossiers": rows, "count": len(rows)}


def get_run_dossier(run_id: str, symbol: str, trading_date: date) -> dict:
    get_run_or_404(run_id)
    dossier = store.get_frozen_dossier(run_id, symbol, trading_date)
    if not dossier:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "FROZEN_DOSSIER_NOT_FOUND",
                "message": "No compiled dossier for this run, symbol, and date.",
                "run_id": run_id,
                "symbol": symbol.upper(),
                "trading_date": trading_date.isoformat(),
            },
        )
    return {"run_id": run_id, "symbol": symbol.upper(), "trading_date": trading_date.isoformat(), "dossier": dossier}


def acquire_historical_bars(run_id: str, *, dry_run: bool = False, retry_failed: bool = False) -> dict:
    try:
        get_run_or_404(run_id)
        return store.run_acquire(run_id, dry_run=dry_run, retry_failed=retry_failed)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}") from exc


def reconstruct_dossiers(run_id: str) -> dict:
    try:
        get_run_or_404(run_id)
        return store.run_reconstruct_dossiers(run_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}") from exc
    except BrooksIntradayError as exc:
        raise _http_error(exc) from exc


def get_historical_data_status(run_id: str) -> dict:
    run = get_run_or_404(run_id)
    hist = run.configuration.get("historical_data") or {}
    freeze = run.configuration.get("bar_dataset_freeze") or {}
    return {
        "run_id": run_id,
        "sessions": hist,
        "bar_dataset_freeze": freeze,
        "readiness": run.readiness,
        "source": (run.preparation or {}).get("historical_bar_source") if run.preparation else None,
    }


def get_sessions(run_id: str) -> dict:
    run = get_run_or_404(run_id)
    sessions = (run.preparation or {}).get("sessions") or run.configuration.get("sessions") or []
    return {"run_id": run_id, "sessions": sessions, "week_resolution_status": (run.preparation or {}).get("week_resolution_status")}
