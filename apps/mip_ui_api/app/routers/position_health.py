"""
Daily Position Health V1 - FastAPI router.

Thin delegation layer over app.services.position_health.

Endpoints:
  POST /position-health/run-shadow            - manually trigger shadow review
  GET  /position-health/comparison/latest     - latest real-vs-shadow rows
  GET  /position-health/lifecycle             - shadow bake-off lifecycle
  GET  /position-health/history/{episode_key} - real and shadow history for one position
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.services.position_health import (
    get_history_for_position,
    get_latest_comparison_rows,
    get_lifecycle_rows,
    run_shadow_health_review,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/position-health", tags=["position-health"])


class RunShadowRequest(BaseModel):
    as_of_date: date = Field(..., description="Business date to evaluate.")
    max_parallel: Optional[int] = Field(None, ge=1, le=16)
    force: bool = Field(False, description="Run even if feature flag is disabled.")


@router.post("/run-shadow")
async def run_shadow(req: RunShadowRequest) -> Dict[str, Any]:
    """Manually trigger the shadow review for as_of_date. Returns the OrchestrationResult dict."""
    try:
        result = await run_shadow_health_review(
            as_of_date=req.as_of_date,
            max_parallel=req.max_parallel,
            force=req.force,
        )
    except Exception as e:
        logger.exception("position_health.router: shadow run failed")
        raise HTTPException(status_code=500, detail=f"shadow run failed: {e}")
    return result.to_dict()


@router.get("/comparison/latest")
def comparison_latest(
    portfolio_id: Optional[int] = Query(None, ge=1),
) -> List[Dict[str, Any]]:
    try:
        return get_latest_comparison_rows(portfolio_id=portfolio_id)
    except Exception as e:
        logger.exception("position_health.router: comparison query failed")
        raise HTTPException(status_code=500, detail=f"comparison query failed: {e}")


@router.get("/lifecycle")
def lifecycle(
    portfolio_id: Optional[int] = Query(None, ge=1),
) -> List[Dict[str, Any]]:
    try:
        return get_lifecycle_rows(portfolio_id=portfolio_id)
    except Exception as e:
        logger.exception("position_health.router: lifecycle query failed")
        raise HTTPException(status_code=500, detail=f"lifecycle query failed: {e}")


@router.get("/history/{position_episode_key}")
def history(position_episode_key: str) -> Dict[str, List[Dict[str, Any]]]:
    if not position_episode_key or len(position_episode_key) < 8:
        raise HTTPException(status_code=400, detail="invalid position_episode_key")
    try:
        return get_history_for_position(position_episode_key)
    except Exception as e:
        logger.exception("position_health.router: history query failed")
        raise HTTPException(status_code=500, detail=f"history query failed: {e}")
