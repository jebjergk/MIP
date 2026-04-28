"""
Cockpit aggregator endpoint.

Single backend call (`GET /cockpit/overview`) returns the entire
operator-facing dashboard payload defined in spec §8. The frontend
makes one fetch and renders the cockpit from this object.

Pydantic models below describe the response shape so OpenAPI / clients
have a contract; the actual composition lives in
`app.services.cockpit.overview_service`.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Response
from pydantic import BaseModel, Field

from app.services.cockpit.overview_service import build_cockpit_overview
from app.services.cockpit.intraday_overlay import clear_overlay_cache

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/cockpit", tags=["cockpit"])


# --- Pydantic response models ----------------------------------------------


class CockpitStatus(BaseModel):
    broker_connected: bool
    latest_broker_snapshot_ts: Optional[str] = None
    latest_market_data_ts: Optional[str] = None
    market_data_fresh: bool = False
    daily_pipeline_status: Optional[str] = None
    latest_daily_run_ts: Optional[str] = None
    latest_intraday_eval_ts: Optional[str] = None
    intraday_overlay_status: str = "UNAVAILABLE"
    shadow_run_status_summary: Dict[str, int] = Field(default_factory=dict)
    active_portfolio_id: Optional[int] = None
    note: Optional[str] = None


class LivePortfolioOverview(BaseModel):
    portfolio_id: Optional[int] = None
    ibkr_account_id: Optional[str] = None
    nav: Optional[float] = None
    cash: Optional[float] = None
    gross_exposure: Optional[float] = None
    invested_pct: Optional[float] = None
    open_position_count: int = 0
    working_order_count: int = 0
    freshness_ts: Optional[str] = None


class MarketPulseSparklinePoint(BaseModel):
    ts: str
    close: float


class MarketPulseMover(BaseModel):
    symbol: Optional[str] = None
    day_return_pct: Optional[float] = None
    last_close: Optional[float] = None
    sparkline: List[MarketPulseSparklinePoint] = Field(default_factory=list)


class MarketPulseIndexPoint(BaseModel):
    ts: str
    index_return_pct: float


class MarketPulseCompact(BaseModel):
    available: bool
    market_type: Optional[str] = None
    breadth_up: Optional[int] = None
    breadth_total: Optional[int] = None
    avg_return_pct: Optional[float] = None
    top_symbol: Optional[str] = None
    top_return_pct: Optional[float] = None
    bottom_symbol: Optional[str] = None
    bottom_return_pct: Optional[float] = None
    pulse_label: Optional[str] = None
    direction: Optional[str] = None
    error: Optional[str] = None
    index_series: List[MarketPulseIndexPoint] = Field(default_factory=list)
    top_movers: List[MarketPulseMover] = Field(default_factory=list)
    bottom_movers: List[MarketPulseMover] = Field(default_factory=list)


class TradeChartPoint(BaseModel):
    kind: str            # 'DAILY' | 'INTRADAY'
    ts: str
    label: str
    close: float
    date: str


class RecommendationFraming(BaseModel):
    plan: str
    now: str
    on_plan: str
    advice: str


class PositionHealthSummaryRowModel(BaseModel):
    position_episode_key: str
    portfolio_id: int
    symbol: str
    side: Optional[str] = None
    days_held: Optional[int] = None
    entry_date: Optional[str] = None

    quantity: Optional[float] = None
    avg_cost: Optional[float] = None
    current_price: Optional[float] = None
    unrealized_pnl: Optional[float] = None
    unrealized_pnl_pct: Optional[float] = None
    market_value: Optional[float] = None

    tp_price: Optional[float] = None
    tp_label: Optional[str] = None
    sl_price: Optional[float] = None
    sl_label: Optional[str] = None
    sl_is_dynamic: bool = False

    invalidation_level: Optional[float] = None
    supporting_level: Optional[float] = None
    thesis_line: Optional[str] = None
    expectation_line: Optional[str] = None
    distance_to_invalidation_pct: Optional[float] = None

    real_verdict: Optional[str] = None
    real_health_state: Optional[str] = None
    real_verdict_label: str

    shadow_verdict: Optional[str] = None
    shadow_action_bias: Optional[str] = None
    shadow_run_status: Optional[str] = None
    shadow_verdict_label: str
    shadow_relation: str
    shadow_relation_label: str
    shadow_relation_level: str
    shadow_summary_text: str

    intraday_status: Optional[str] = None
    intraday_action: Optional[str] = None
    intraday_reason: Optional[str] = None
    today_label: str
    today_level: str
    today_change_pct: Optional[float] = None
    today_open: Optional[float] = None
    last_price: Optional[float] = None
    intraday_summary_text: str

    why_text: str

    plan_status: str
    plan_status_label: str
    plan_status_level: str

    recommendation: str
    recommendation_label: str
    recommendation_level: str
    recommendation_text: str
    recommendation_framing: RecommendationFraming

    trade_chart_series: List[TradeChartPoint] = Field(default_factory=list)
    session_open_ts: Optional[str] = None

    real_summary_text: str = ""
    invalidation_summary_text: str = ""


class PriorityReviewListItem(BaseModel):
    position_episode_key: str
    symbol: str
    real_verdict: Optional[str] = None
    shadow_verdict: Optional[str] = None
    shadow_action_bias: Optional[str] = None
    intraday_action: Optional[str] = None
    why_text: str
    attention_label: str
    today_label: str
    detail_route: str


class CappedList(BaseModel):
    items: List[PriorityReviewListItem]
    total_count: int
    more_count: int


class PriorityReview(BaseModel):
    real_exit_review_rows: CappedList
    shadow_exit_now_rows: CappedList
    disagreement_rows: CappedList
    intraday_review_now_rows: CappedList
    intraday_sell_now_rows: CappedList
    pending_shadow_rows: CappedList
    detail_index_route: str


class ShadowSummary(BaseModel):
    harsher_count: int
    softer_count: int
    no_shadow_count: int
    pending_shadow_count: int
    exit_now_count: int


class IntradaySummary(BaseModel):
    overlay_status: str
    latest_eval_ts: str
    hold_count: int
    watch_now_count: int
    review_now_count: int
    sell_now_count: int
    no_live_check_count: int
    market_closed_count: int
    note: Optional[str] = None


class TradeProposalChartPoint(BaseModel):
    ts: str
    close: float
    kind: str = "DAILY"  # 'DAILY' | 'INTRADAY'


class TradeProposal(BaseModel):
    proposal_id: int
    symbol: str
    direction: str
    setup_family: Optional[str] = None
    committee_stance: Optional[str] = None
    committee_stance_source: Optional[str] = None
    committee_confidence: Optional[float] = None
    entry_zone_low: Optional[float] = None
    entry_zone_high: Optional[float] = None
    invalidation_level: Optional[float] = None
    # Live "Now" — driven by IBKR snapshot quote, then 15m bar close,
    # then None (LIVE_UNAVAILABLE).
    current_price: Optional[float] = None
    current_price_source: Optional[str] = None  # 'LIVE_TICK' | 'INTRADAY_BAR' | None
    current_price_ts: Optional[str] = None
    # Stale fallback (footer text only).
    last_close: Optional[float] = None
    last_close_date: Optional[str] = None
    zone_status: str
    zone_status_label: str
    entry_readiness: str
    entry_readiness_label: str
    distance_to_zone_pct: Optional[float] = None
    intraday_status: str = "UNAVAILABLE"  # 'OK' | 'EMPTY' | 'FAILED' | 'UNAVAILABLE'
    mini_chart_series: List[TradeProposalChartPoint] = Field(default_factory=list)
    detail_route: Optional[str] = None
    created_at: Optional[str] = None


class TradeProposalsBlock(BaseModel):
    available: bool
    total_count: int = 0
    intraday_overlay_status: str = "UNAVAILABLE"   # OK | PARTIAL | UNAVAILABLE | MARKET_CLOSED
    intraday_evaluated_ts: Optional[str] = None
    proposals: List[TradeProposal] = Field(default_factory=list)
    note: Optional[str] = None


class CockpitOverview(BaseModel):
    schema_version: str
    as_of_ts: str
    status: CockpitStatus
    live_portfolio_overview: LivePortfolioOverview
    trade_proposals: TradeProposalsBlock = Field(
        default_factory=lambda: TradeProposalsBlock(available=False)
    )
    market_pulse: MarketPulseCompact
    position_health_summary_rows: List[PositionHealthSummaryRowModel]
    priority_review: PriorityReview
    shadow_summary: ShadowSummary
    intraday_summary: IntradaySummary


# --- Endpoints -------------------------------------------------------------


@router.get("/overview", response_model=CockpitOverview)
def get_cockpit_overview(
    response: Response,
    portfolio_id: Optional[int] = Query(
        None,
        ge=1,
        description=(
            "Optional override. When omitted, uses the single active "
            "live portfolio from MIP.LIVE.LIVE_PORTFOLIO_CONFIG."
        ),
    ),
    force_refresh: bool = Query(
        False,
        description=(
            "Bypass the 60s intraday-overlay cache and force a fresh "
            "TWS fetch. Used by the cockpit Refresh button so the "
            "intraday card reflects the latest 15m bars on demand."
        ),
    ),
):
    # The cockpit payload contains live tick prices and intraday bars
    # that change every poll — never cache it anywhere along the path.
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    try:
        return build_cockpit_overview(
            portfolio_id=portfolio_id,
            force_refresh_intraday=force_refresh,
        )
    except Exception as exc:
        logger.exception("cockpit.overview: composition failed")
        raise HTTPException(status_code=500, detail=f"cockpit overview failed: {exc}")


@router.post("/overview/refresh-cache")
def refresh_intraday_cache(
    portfolio_id: Optional[int] = Query(None, ge=1),
) -> Dict[str, Any]:
    """Drop the cached intraday overlay so the next /overview call
    forces a fresh TWS fetch. Useful for ops/debug; the cache TTL is
    only 60s so this is rarely needed."""
    clear_overlay_cache(portfolio_id)
    return {"ok": True, "cleared_portfolio_id": portfolio_id}
