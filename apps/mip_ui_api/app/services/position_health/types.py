"""
Daily Position Health V1 - typed models.

Three categories of types:
  - PositionPayload     : compact per-position context built by payload_builder
  - ShadowReviewResult  : structured result from one Cortex agent call
  - OrchestrationResult : aggregate of one orchestrator run
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Any, Dict, List, Optional


class ShadowRunStatus(str, Enum):
    """Per-position status persisted on DAILY_POSITION_SHADOW_REVIEW.SHADOW_RUN_STATUS."""

    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    PARSE_ERROR = "PARSE_ERROR"
    API_ERROR = "API_ERROR"
    SKIPPED = "SKIPPED"


# ---------------------------------------------------------------------------
# Payload built by payload_builder for each open position.
# ---------------------------------------------------------------------------

@dataclass
class CommitteeBaselineContext:
    """Original committee baseline context (best-effort; may be empty)."""

    proposal_id: Optional[int] = None
    stance: Optional[str] = None
    confidence: Optional[float] = None
    chair_summary: Optional[str] = None
    has_record: bool = False


@dataclass
class PathSummary:
    """Compact aggregated path metrics over the shadow lookback window."""

    bars_observed: int = 0
    up_days: int = 0
    comparable_days: int = 0
    up_days_pct: Optional[float] = None
    high_water_close: Optional[float] = None
    trough_close: Optional[float] = None
    last_close: Optional[float] = None
    first_close: Optional[float] = None
    max_drawdown_pct: Optional[float] = None
    net_change_pct: Optional[float] = None
    recent_5d_closes: List[float] = field(default_factory=list)


@dataclass
class RealVerdictContext:
    """The deterministic real verdict for the same position on AS_OF_DATE.

    Phase 1 re-anchor: horizon-based TIME_EFFICIENCY field is removed
    because the verdict engine no longer derives it.
    """

    verdict: str
    health_state: str
    severity: str
    baseline_quality: Optional[str] = None
    thesis_integrity: Optional[str] = None
    path_quality: Optional[str] = None
    regime_alignment: Optional[str] = None
    fragility: Optional[str] = None
    primary_reason_code: Optional[str] = None
    observation_summary: Optional[str] = None
    why_summary: Optional[str] = None


@dataclass
class PositionPayload:
    """Complete per-position context handed to the Cortex agent.

    Phase 1 re-anchor: horizon scoring fields
    (expected_horizon_days, horizon_source_code) are removed - the
    shadow agent now reasons about live broker positions, not
    horizon-based research positions.
    """

    position_episode_key: str
    portfolio_id: int
    episode_id: Optional[int]
    symbol: str
    side: str
    as_of_date: date
    entry_date: date
    entry_price: Optional[float]
    days_held: int
    latest_close: Optional[float]
    unrealized_pnl_pct: Optional[float]

    structural_state_now: Optional[str] = None
    trend_regime_now: Optional[str] = None
    vol_regime_now: Optional[str] = None
    range_regime_now: Optional[str] = None
    distance_to_invalidation_pct: Optional[float] = None

    baseline_context: CommitteeBaselineContext = field(default_factory=CommitteeBaselineContext)
    path_summary: PathSummary = field(default_factory=PathSummary)
    real_verdict_context: Optional[RealVerdictContext] = None

    def to_user_message(self) -> Dict[str, Any]:
        """Serialize into the JSON-safe object the agent reads as user content."""
        return {
            "position": {
                "symbol": self.symbol,
                "side": self.side,
                "as_of_date": self.as_of_date.isoformat(),
                "entry_date": self.entry_date.isoformat(),
                "entry_price": self.entry_price,
                "days_held": self.days_held,
                "latest_close": self.latest_close,
                "unrealized_pnl_pct": self.unrealized_pnl_pct,
            },
            "current_context": {
                "structural_state": self.structural_state_now,
                "trend_regime": self.trend_regime_now,
                "vol_regime": self.vol_regime_now,
                "range_regime": self.range_regime_now,
                "distance_to_invalidation_pct": self.distance_to_invalidation_pct,
            },
            "baseline_thesis": {
                "has_committee_record": self.baseline_context.has_record,
                "proposal_id": self.baseline_context.proposal_id,
                "stance": self.baseline_context.stance,
                "confidence": self.baseline_context.confidence,
                "chair_summary": self.baseline_context.chair_summary,
            },
            "path_summary": {
                "bars_observed": self.path_summary.bars_observed,
                "up_days": self.path_summary.up_days,
                "comparable_days": self.path_summary.comparable_days,
                "up_days_pct": self.path_summary.up_days_pct,
                "high_water_close": self.path_summary.high_water_close,
                "trough_close": self.path_summary.trough_close,
                "first_close": self.path_summary.first_close,
                "last_close": self.path_summary.last_close,
                "max_drawdown_pct": self.path_summary.max_drawdown_pct,
                "net_change_pct": self.path_summary.net_change_pct,
                "recent_5d_closes": self.path_summary.recent_5d_closes,
            },
            "real_verdict": (
                {
                    "verdict": self.real_verdict_context.verdict,
                    "health_state": self.real_verdict_context.health_state,
                    "severity": self.real_verdict_context.severity,
                    "baseline_quality": self.real_verdict_context.baseline_quality,
                    "thesis_integrity": self.real_verdict_context.thesis_integrity,
                    "path_quality": self.real_verdict_context.path_quality,
                    "regime_alignment": self.real_verdict_context.regime_alignment,
                    "fragility": self.real_verdict_context.fragility,
                    "primary_reason_code": self.real_verdict_context.primary_reason_code,
                    "observation_summary": self.real_verdict_context.observation_summary,
                    "why_summary": self.real_verdict_context.why_summary,
                }
                if self.real_verdict_context is not None
                else None
            ),
        }


# ---------------------------------------------------------------------------
# Shadow review result.
# ---------------------------------------------------------------------------

@dataclass
class ShadowReviewResult:
    position_episode_key: str
    portfolio_id: int
    symbol: str
    as_of_date: date

    shadow_run_status: ShadowRunStatus = ShadowRunStatus.PENDING
    shadow_run_ts: Optional[datetime] = None
    shadow_run_elapsed_ms: Optional[int] = None
    shadow_run_error: Optional[str] = None

    shadow_verdict: Optional[str] = None
    shadow_thesis_status: Optional[str] = None
    shadow_action_bias: Optional[str] = None
    shadow_severity: Optional[str] = None
    primary_reason_code: Optional[str] = None
    primary_reason_text: Optional[str] = None
    observation_summary: Optional[str] = None
    verdict_summary: Optional[str] = None
    why_summary: Optional[str] = None
    rationale_text: Optional[str] = None
    rationale_json: Optional[Dict[str, Any]] = None

    agrees_with_real: Optional[bool] = None
    disagreement_class: Optional[str] = None


# ---------------------------------------------------------------------------
# Orchestration aggregate.
# ---------------------------------------------------------------------------

@dataclass
class OrchestrationResult:
    as_of_date: date
    started_at: datetime
    completed_at: Optional[datetime] = None
    positions_total: int = 0
    reviews_attempted: int = 0
    reviews_success: int = 0
    reviews_parse_error: int = 0
    reviews_api_error: int = 0
    reviews_skipped: int = 0
    lifecycle_summary: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    status: str = "PENDING"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "as_of_date": self.as_of_date.isoformat(),
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "positions_total": self.positions_total,
            "reviews_attempted": self.reviews_attempted,
            "reviews_success": self.reviews_success,
            "reviews_parse_error": self.reviews_parse_error,
            "reviews_api_error": self.reviews_api_error,
            "reviews_skipped": self.reviews_skipped,
            "lifecycle_summary": self.lifecycle_summary,
            "error": self.error,
            "status": self.status,
        }
