"""
Brooks Intraday Lab HTTP routes.

Prefix: /research/brooks-intraday (client uses /api/research/brooks-intraday via Vite proxy).

This router is simulation-only and must remain isolated from live trading paths.
"""

from __future__ import annotations

from datetime import date

from fastapi import APIRouter

from .constants import SIMULATION_BANNER
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
from . import service
from . import store

router = APIRouter(prefix="/research/brooks-intraday", tags=["brooks-intraday-lab"])


@router.get("/meta")
def lab_meta() -> dict:
    return {
        "module": "brooks-intraday-lab",
        "simulation_banner": SIMULATION_BANNER,
        "execution_authority": False,
        "real_portfolio_integration": False,
        "phase": "9_unseen_validation",
    }


@router.post("/runs", response_model=CreateRunResponse)
def post_create_run(body: CreateRunRequest) -> CreateRunResponse:
    return service.create_run(body)


@router.get("/runs/{run_id}", response_model=RunDetail)
def get_run(run_id: str) -> RunDetail:
    return service.get_run_or_404(run_id)


@router.post("/runs/{run_id}/prepare", response_model=PreparationResponse)
def post_prepare(run_id: str) -> PreparationResponse:
    return service.prepare_run(run_id)


@router.get("/runs/{run_id}/preparation", response_model=PreparationResponse)
def get_preparation(run_id: str) -> PreparationResponse:
    return service.get_preparation(run_id)


@router.post("/runs/{run_id}/start", response_model=SimpleStatusResponse)
def post_start(run_id: str) -> SimpleStatusResponse:
    return service.start_run(run_id)


@router.post("/runs/{run_id}/pause", response_model=SimpleStatusResponse)
def post_pause(run_id: str) -> SimpleStatusResponse:
    return service.pause_run(run_id)


@router.post("/runs/{run_id}/resume", response_model=SimpleStatusResponse)
def post_resume(run_id: str) -> SimpleStatusResponse:
    return service.resume_run(run_id)


@router.post("/runs/{run_id}/stop", response_model=SimpleStatusResponse)
def post_stop(run_id: str) -> SimpleStatusResponse:
    return service.stop_run(run_id)


@router.post("/runs/{run_id}/reset", response_model=SimpleStatusResponse)
def post_reset(run_id: str) -> SimpleStatusResponse:
    return service.reset_run(run_id)


@router.post("/runs/{run_id}/next-bar")
def post_next_bar(run_id: str, request_token: str | None = None) -> dict:
    return service.next_bar(run_id, request_token=request_token)


@router.get("/runs/{run_id}/replay-state")
def get_replay_state(run_id: str) -> dict:
    return service.get_replay_state(run_id)


@router.get("/runs/{run_id}/visible-bars")
def get_visible_bars(run_id: str) -> dict:
    return service.get_visible_bars(run_id)


@router.get("/runs/{run_id}/visible-bars/{symbol}")
def get_visible_bars_symbol(run_id: str, symbol: str) -> dict:
    return service.get_visible_bars(run_id, symbol)


@router.get("/runs/{run_id}/summary", response_model=RunDetail)
def get_summary(run_id: str) -> RunDetail:
    return service.get_summary(run_id)


@router.get("/runs/{run_id}/symbols")
def get_symbols(run_id: str) -> dict:
    return service.get_symbols(run_id)


@router.get("/runs/{run_id}/symbols/{symbol}")
def get_symbol(run_id: str, symbol: str) -> dict:
    return service.get_symbol_detail(run_id, symbol)


@router.get("/runs/{run_id}/replay-attempts")
def get_replay_attempts(run_id: str) -> dict:
    return service.list_replay_attempts(run_id)


@router.post("/runs/{run_id}/reset-pattern-replay", response_model=SimpleStatusResponse)
def post_reset_pattern_replay(run_id: str) -> SimpleStatusResponse:
    from . import replay_service

    return replay_service.reset_pattern_run(run_id)


@router.get("/runs/{run_id}/patterns/review")
def get_pattern_review(run_id: str) -> dict:
    from . import replay_service

    return replay_service.pattern_review_summary(run_id)


@router.get("/runs/{run_id}/patterns")
def get_patterns(run_id: str, symbol: str | None = None, pattern_filter: str | None = None) -> dict:
    from . import replay_service

    return replay_service.list_patterns(run_id, symbol=symbol, pattern_filter=pattern_filter)


@router.get("/runs/{run_id}/symbols/{symbol}/context")
def get_context_observations(run_id: str, symbol: str) -> dict:
    return service.get_context_observations(run_id, symbol)


@router.post("/runs/{run_id}/simulation/bulk")
def post_simulation_bulk(run_id: str, context_attempt_id: str | None = None) -> dict:
    from .simulation_ruleset_v01 import DEFAULT_CONTEXT_ATTEMPT_ID

    return store.run_simulation_bulk(
        run_id, context_attempt_id=context_attempt_id or DEFAULT_CONTEXT_ATTEMPT_ID
    )


@router.get("/runs/{run_id}/blocked-signals")
def get_blocked_signals(run_id: str) -> dict:
    from .simulation_repository import load_blocked_signals

    service.get_run_or_404(run_id)
    rows = load_blocked_signals(run_id)
    return {"run_id": run_id, "blocked_signals": rows, "count": len(rows)}


@router.post("/runs/{run_id}/context/bulk")
def post_context_bulk(run_id: str) -> dict:
    return store.run_context_bulk(run_id)


@router.get("/runs/{run_id}/observations/review")
def get_observation_review(run_id: str) -> dict:
    from . import replay_service
    from .errors import BrooksIntradayError

    try:
        return replay_service.observation_review_summary(run_id)
    except BrooksIntradayError as exc:
        from .service import _http_error
        raise _http_error(exc) from exc


@router.get("/runs/{run_id}/symbols/{symbol}/observations", response_model=ObservationsPage)
def get_observations(run_id: str, symbol: str, review_filter: str | None = None) -> ObservationsPage:
    return service.get_observations(run_id, symbol, review_filter=review_filter)


@router.get("/runs/{run_id}/trades", response_model=TradesPage)
def get_trades(run_id: str) -> TradesPage:
    return service.get_trades(run_id)


@router.get("/runs/{run_id}/dossiers")
def get_dossiers(run_id: str) -> dict:
    return service.list_run_dossiers(run_id)


@router.get("/runs/{run_id}/dossiers/{symbol}/{trading_date}")
def get_dossier(run_id: str, symbol: str, trading_date: date) -> dict:
    return service.get_run_dossier(run_id, symbol, trading_date)


@router.post("/runs/{run_id}/reconstruct-dossiers")
def post_reconstruct_dossiers(run_id: str) -> dict:
    return service.reconstruct_dossiers(run_id)


@router.post("/runs/{run_id}/acquire-historical-bars")
def post_acquire_historical_bars(
    run_id: str,
    dry_run: bool = False,
    retry_failed: bool = False,
) -> dict:
    return service.acquire_historical_bars(run_id, dry_run=dry_run, retry_failed=retry_failed)


@router.get("/runs/{run_id}/acquire-historical-bars")
def get_acquire_plan(run_id: str) -> dict:
    return service.acquire_historical_bars(run_id, dry_run=True)


@router.get("/runs/{run_id}/historical-data-status")
def get_historical_data_status(run_id: str) -> dict:
    return service.get_historical_data_status(run_id)


@router.get("/runs/{run_id}/sessions")
def get_sessions(run_id: str) -> dict:
    return service.get_sessions(run_id)


@router.get("/rulesets", response_model=list[RulesetInfo])
def get_rulesets() -> list[RulesetInfo]:
    return service.get_rulesets()


@router.get("/runs/{run_id}/certification-summary")
def get_certification_summary(include_fixtures: bool = False) -> dict:
    from . import learning_service

    return learning_service.get_certification_summary(include_fixtures=include_fixtures)


@router.get("/runs/{run_id}/learning-view")
def get_learning_view(
    run_id: str,
    mode: str = "full",
    active_symbol: str | None = None,
    trading_date: date | None = None,
) -> dict:
    from . import learning_service

    return learning_service.get_learning_view(
        run_id, mode=mode, active_symbol=active_symbol, trading_date=trading_date
    )


@router.get("/runs/{run_id}/symbols/{symbol}/learning-view")
def get_symbol_learning_view(
    run_id: str,
    symbol: str,
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
    from . import learning_service

    return learning_service.get_symbol_learning_view(
        run_id,
        symbol,
        mode=mode,
        trading_date=trading_date,
        offset=offset,
        limit=limit,
        pattern_family=pattern_family,
        lifecycle=lifecycle,
        thesis_effect=thesis_effect,
        advisory_state=advisory_state,
        advisory_action=advisory_action,
        transition_events_only=transition_events_only,
        blockers=blockers,
        simulated_trade_events=simulated_trade_events,
        action_changed_only=action_changed_only,
        meaningful_pattern_no_entry=meaningful_pattern_no_entry,
    )


@router.get("/runs/{run_id}/state-transitions")
def get_state_transitions(run_id: str, symbol: str | None = None, mode: str = "full") -> dict:
    from . import learning_service

    return learning_service.get_state_transitions(run_id, symbol=symbol, mode=mode)


@router.get("/runs/{run_id}/account-timeline")
def get_account_timeline(run_id: str) -> dict:
    from . import learning_service

    return learning_service.get_account_timeline(run_id)


@router.get("/experiments/ruleset-freeze")
def get_experiment_freeze() -> dict:
    from . import experiment_comparison

    return experiment_comparison.get_ruleset_freeze()


@router.get("/experiments/validation-runs")
def get_validation_runs_overview() -> dict:
    from . import experiment_comparison

    return experiment_comparison.get_validation_overview()


@router.get("/experiments/comparison")
def get_experiment_comparison(
    run_ids: str | None = None,
    include_baseline: bool = True,
) -> dict:
    from . import experiment_comparison

    ids = [x.strip() for x in run_ids.split(",") if x.strip()] if run_ids else []
    if not ids:
        overview = experiment_comparison.get_validation_overview()
        ids = [r["run_id"] for r in overview.get("validation_runs") or []]
    return experiment_comparison.build_comparison(run_ids=ids, include_baseline=include_baseline)


@router.get("/experiments/aggregate-report")
def get_experiment_aggregate(run_ids: str | None = None) -> dict:
    from . import experiment_comparison

    ids = [x.strip() for x in run_ids.split(",") if x.strip()] if run_ids else None
    return experiment_comparison.get_aggregate_report(run_ids=ids)


@router.get("/experiments/trades")
def get_experiment_trades(
    run_id: str | None = None,
    symbol: str | None = None,
    win_only: bool | None = None,
    exit_reason: str | None = None,
) -> dict:
    from . import experiment_comparison

    return experiment_comparison.list_trades_filtered(
        run_id=run_id, symbol=symbol, win_only=win_only, exit_reason=exit_reason
    )


@router.get("/experiments/week-selection")
def get_week_selection_rationale() -> dict:
    from .experiment_week_selection import build_week_selection_rationale

    return build_week_selection_rationale()


@router.get("/runs/{run_id}/experiment-week-report")
def get_run_week_report(run_id: str) -> dict:
    from .experiment_analytics import build_week_report

    service.get_run_or_404(run_id)
    return build_week_report(run_id)


# --- Phase 9A: UI-controlled validation execution ---


@router.get("/experiments/phase9/status")
def phase9_status() -> dict:
    from .experiment_phase9_service import build_phase9_status

    return build_phase9_status()


@router.post("/experiments/phase9/check-tws")
def phase9_check_tws() -> dict:
    from .experiment_phase9_service import build_phase9_status, check_tws_and_store

    tws = check_tws_and_store()
    return {"tws": tws, "status": build_phase9_status()}


@router.post("/experiments/phase9/run-next")
def phase9_run_next() -> dict:
    from .experiment_phase9_service import build_phase9_status, request_run_next

    result = request_run_next()
    return {**result, "status": build_phase9_status()}


@router.post("/experiments/phase9/run-remaining")
def phase9_run_remaining() -> dict:
    from .experiment_phase9_service import build_phase9_status, request_run_remaining

    result = request_run_remaining()
    return {**result, "status": build_phase9_status()}


@router.post("/experiments/phase9/pause")
def phase9_pause() -> dict:
    from .experiment_phase9_service import build_phase9_status, request_pause

    result = request_pause()
    return {**result, "status": build_phase9_status()}


@router.post("/experiments/phase9/resume")
def phase9_resume() -> dict:
    from .experiment_phase9_service import build_phase9_status, request_resume

    result = request_resume()
    return {**result, "status": build_phase9_status()}


@router.post("/experiments/phase9/retry")
def phase9_retry() -> dict:
    from .experiment_phase9_service import build_phase9_status, request_retry

    result = request_retry()
    return {**result, "status": build_phase9_status()}


@router.post("/experiments/phase9/cancel-pending")
def phase9_cancel_pending() -> dict:
    from .experiment_phase9_service import build_phase9_status, request_cancel_pending

    result = request_cancel_pending()
    return {**result, "status": build_phase9_status()}


@router.get("/experiments/phase9/events")
def phase9_events(limit: int = 100) -> dict:
    from .experiment_phase9_service import get_phase9_events

    return get_phase9_events(limit=min(limit, 200))


@router.get("/dossiers/{symbol}/{trading_date}", response_model=DossierPreview)
def get_dossier_preview(symbol: str, trading_date: date) -> DossierPreview:
    return service.get_dossier_preview(symbol, trading_date)
