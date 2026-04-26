"""
Daily Position Health V1 - shadow review service module.

Public entry points:
  - run_shadow_health_review(as_of_date) -> OrchestrationResult
  - get_latest_comparison_rows() -> list[dict]
  - get_history_for_symbol(symbol, portfolio_id) -> list[dict]
  - get_lifecycle_rows() -> list[dict]
"""
from .orchestrator import (
    run_shadow_health_review,
    get_latest_comparison_rows,
    get_history_for_position,
    get_lifecycle_rows,
)
from .types import (
    OrchestrationResult,
    PositionPayload,
    ShadowReviewResult,
    ShadowRunStatus,
)

__all__ = [
    "run_shadow_health_review",
    "get_latest_comparison_rows",
    "get_history_for_position",
    "get_lifecycle_rows",
    "OrchestrationResult",
    "PositionPayload",
    "ShadowReviewResult",
    "ShadowRunStatus",
]
