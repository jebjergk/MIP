"""
management.py — Portfolio & Profile Management API

First write endpoints in the MIP UI API. All mutations go through Snowflake
stored procedures (EXECUTE AS OWNER) so the API role never needs direct
INSERT/UPDATE access to tables.

Endpoints:
    POST  /manage/portfolios              – Create portfolio
    PUT   /manage/portfolios/{id}         – Update portfolio
    POST  /manage/portfolios/{id}/cash    – Deposit or withdraw cash
    PUT   /manage/portfolios/{id}/profile – Attach a profile
    GET   /manage/portfolios/{id}/lifecycle – Lifecycle event timeline
    GET   /manage/portfolios/{id}/narrative – Latest AI narrative
    POST  /manage/portfolios/{id}/narrative – Generate (regenerate) AI narrative
    GET   /manage/profiles                – List all profiles
    GET   /manage/profiles/{id}           – Get single profile
    POST  /manage/profiles                – Create profile
    PUT   /manage/profiles/{id}           – Update profile
"""

import json
import logging
from datetime import datetime

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Optional

from app.db import get_connection, fetch_all, serialize_rows, serialize_row

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/manage", tags=["management"])


# ─── Request models ───────────────────────────────────────────────────────────

class PortfolioCreate(BaseModel):
    name: str
    base_currency: str = "USD"
    starting_cash: float = Field(gt=0)
    profile_id: int
    notes: Optional[str] = None


class PortfolioUpdate(BaseModel):
    name: Optional[str] = None
    base_currency: Optional[str] = None
    notes: Optional[str] = None


class CashEvent(BaseModel):
    event_type: str = Field(pattern="^(DEPOSIT|WITHDRAW)$")
    amount: float = Field(gt=0)
    notes: Optional[str] = None


class AttachProfile(BaseModel):
    profile_id: int


class ProfileUpsert(BaseModel):
    name: Optional[str] = None
    max_positions: Optional[int] = None
    max_position_pct: Optional[float] = None
    bust_equity_pct: Optional[float] = None
    bust_action: Optional[str] = None
    drawdown_stop_pct: Optional[float] = None
    crystallize_enabled: Optional[bool] = None
    profit_target_pct: Optional[float] = None
    crystallize_mode: Optional[str] = None
    cooldown_days: Optional[int] = None
    max_episode_days: Optional[int] = None
    take_profit_on: Optional[str] = None
    description: Optional[str] = None


# ─── Helper: call a stored procedure and parse the VARIANT result ─────────────

def _call_sp(sql: str, params: tuple) -> dict:
    """Call a Snowflake stored procedure and return its VARIANT result as a dict."""
    logger.info("[_call_sp] SQL: %s", sql)
    logger.info("[_call_sp] Params (%d): %s", len(params), params)
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        row = cur.fetchone()
        if row is None:
            raise HTTPException(status_code=500, detail="Stored procedure returned no result")
        result = row[0]
        # Result may be a string (JSON) or already parsed
        if isinstance(result, str):
            result = json.loads(result)
        if isinstance(result, dict) and result.get("status") == "ERROR":
            error_msg = result.get("error", "Unknown error")
            # Map to appropriate HTTP status
            if "pipeline is currently running" in error_msg.lower():
                raise HTTPException(status_code=409, detail=error_msg)
            elif "not found" in error_msg.lower():
                raise HTTPException(status_code=404, detail=error_msg)
            else:
                raise HTTPException(status_code=422, detail=error_msg)
        # Explicit commit: although Snowflake defaults to AUTOCOMMIT=TRUE,
        # pooled connections may carry stale transaction state.  This ensures
        # DML executed inside the stored procedure is persisted.
        try:
            cur.execute("COMMIT")
        except Exception:
            pass  # harmless if no active transaction
        return result
    finally:
        conn.close()


# ─── Portfolio CRUD ───────────────────────────────────────────────────────────

@router.post("/portfolios")
def create_portfolio(body: PortfolioCreate):
    """Create a new portfolio with starting cash and profile."""
    return _call_sp(
        "CALL MIP.APP.SP_UPSERT_PORTFOLIO(%s, %s, %s, %s, %s, %s)",
        (None, body.name, body.base_currency, body.starting_cash, body.profile_id, body.notes),
    )


@router.put("/portfolios/{portfolio_id}")
def update_portfolio(portfolio_id: int, body: PortfolioUpdate):
    """Update allowed portfolio fields (name, currency, notes). Starting cash cannot be changed here."""
    return _call_sp(
        "CALL MIP.APP.SP_UPSERT_PORTFOLIO(%s, %s, %s, %s, %s, %s)",
        (portfolio_id, body.name, body.base_currency, None, None, body.notes),
    )


# ─── Cash Events ──────────────────────────────────────────────────────────────

@router.post("/portfolios/{portfolio_id}/cash")
def portfolio_cash_event(portfolio_id: int, body: CashEvent):
    """Register a DEPOSIT or WITHDRAW. Lifetime P&L tracking stays intact."""
    return _call_sp(
        "CALL MIP.APP.SP_PORTFOLIO_CASH_EVENT(%s, %s, %s, %s)",
        (portfolio_id, body.event_type, body.amount, body.notes),
    )


# ─── Profile Attachment ──────────────────────────────────────────────────────

@router.put("/portfolios/{portfolio_id}/profile")
def attach_profile(portfolio_id: int, body: AttachProfile):
    """Attach a different profile to the portfolio (ends current episode, starts new one)."""
    return _call_sp(
        "CALL MIP.APP.SP_ATTACH_PROFILE(%s, %s)",
        (portfolio_id, body.profile_id),
    )


# ─── Lifecycle Timeline ──────────────────────────────────────────────────────

@router.get("/portfolios/{portfolio_id}/lifecycle")
def get_lifecycle_timeline(portfolio_id: int):
    """Get the full lifecycle event timeline for a portfolio."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT *
              FROM MIP.MART.V_PORTFOLIO_LIFECYCLE_TIMELINE
             WHERE PORTFOLIO_ID = %s
             ORDER BY EVENT_TS ASC, EVENT_ID ASC
            """,
            (portfolio_id,),
        )
        rows = fetch_all(cur)
        return {
            "portfolio_id": portfolio_id,
            "event_count": len(rows),
            "events": serialize_rows(rows),
        }
    finally:
        conn.close()


# ─── AI Narrative ─────────────────────────────────────────────────────────────

@router.get("/portfolios/{portfolio_id}/narrative")
def get_narrative(portfolio_id: int):
    """Get the latest AI-generated portfolio lifecycle narrative."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT NARRATIVE_ID, PORTFOLIO_ID, AS_OF_TS, RUN_ID, AGENT_NAME,
                   NARRATIVE_TEXT, NARRATIVE_JSON, MODEL_INFO, SOURCE_FACTS_HASH, CREATED_AT
              FROM MIP.AGENT_OUT.PORTFOLIO_LIFECYCLE_NARRATIVE
             WHERE PORTFOLIO_ID = %s
             ORDER BY CREATED_AT DESC
             LIMIT 1
            """,
            (portfolio_id,),
        )
        rows = fetch_all(cur)
        if not rows:
            return {"portfolio_id": portfolio_id, "narrative": None, "message": "No narrative generated yet."}
        row = serialize_row(rows[0])
        # Parse NARRATIVE_JSON if it's a string
        if isinstance(row.get("NARRATIVE_JSON"), str):
            try:
                row["NARRATIVE_JSON"] = json.loads(row["NARRATIVE_JSON"])
            except (json.JSONDecodeError, TypeError):
                pass
        return {"portfolio_id": portfolio_id, "narrative": row}
    finally:
        conn.close()


@router.post("/portfolios/{portfolio_id}/narrative")
def generate_narrative(portfolio_id: int):
    """Generate (or regenerate) the AI portfolio lifecycle narrative using Cortex."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "CALL MIP.APP.SP_AGENT_GENERATE_PORTFOLIO_NARRATIVE(%s)",
            (portfolio_id,),
        )
        row = cur.fetchone()
        if row is None:
            raise HTTPException(status_code=500, detail="Stored procedure returned no result")
        result = row[0]
        if isinstance(result, str):
            import json as _json
            result = _json.loads(result)
        if isinstance(result, dict) and result.get("status") == "ERROR":
            raise HTTPException(status_code=422, detail=result.get("error", "Unknown error"))
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# ─── Profile CRUD ─────────────────────────────────────────────────────────────

@router.get("/profiles")
def list_profiles():
    """List all portfolio profiles with usage counts."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                pp.*,
                coalesce(usage.portfolio_count, 0) as PORTFOLIO_COUNT,
                usage.portfolio_names
            FROM MIP.APP.PORTFOLIO_PROFILE pp
            LEFT JOIN (
                SELECT PROFILE_ID,
                       count(*) as portfolio_count,
                       listagg(NAME, ', ') within group (order by NAME) as portfolio_names
                  FROM MIP.APP.PORTFOLIO
                 WHERE STATUS = 'ACTIVE'
                 GROUP BY PROFILE_ID
            ) usage ON usage.PROFILE_ID = pp.PROFILE_ID
            ORDER BY pp.PROFILE_ID
            """
        )
        rows = fetch_all(cur)
        return {"profiles": serialize_rows(rows)}
    finally:
        conn.close()


@router.get("/profiles/{profile_id}")
def get_profile(profile_id: int):
    """Get a single profile by ID."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM MIP.APP.PORTFOLIO_PROFILE WHERE PROFILE_ID = %s",
            (profile_id,),
        )
        rows = fetch_all(cur)
        if not rows:
            raise HTTPException(status_code=404, detail="Profile not found")
        return serialize_row(rows[0])
    finally:
        conn.close()


@router.post("/profiles")
def create_profile(body: ProfileUpsert):
    """Create a new portfolio profile."""
    if not body.name:
        raise HTTPException(status_code=422, detail="name is required for profile creation")
    return _call_sp(
        "CALL MIP.APP.SP_UPSERT_PORTFOLIO_PROFILE(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (
            None, body.name, body.max_positions, body.max_position_pct,
            body.bust_equity_pct, body.bust_action, body.drawdown_stop_pct,
            body.crystallize_enabled, body.profit_target_pct, body.crystallize_mode,
            body.cooldown_days, body.max_episode_days, body.take_profit_on,
            body.description,
        ),
    )


@router.put("/profiles/{profile_id}")
def update_profile(profile_id: int, body: ProfileUpsert):
    """Update an existing portfolio profile."""
    logger.info("[update_profile] profile_id=%s body=%s", profile_id, body.model_dump())
    result = _call_sp(
        "CALL MIP.APP.SP_UPSERT_PORTFOLIO_PROFILE(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (
            profile_id, body.name, body.max_positions, body.max_position_pct,
            body.bust_equity_pct, body.bust_action, body.drawdown_stop_pct,
            body.crystallize_enabled, body.profit_target_pct, body.crystallize_mode,
            body.cooldown_days, body.max_episode_days, body.take_profit_on,
            body.description,
        ),
    )
    # ── DEBUG: read back the row to confirm persistence ──
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT * FROM MIP.APP.PORTFOLIO_PROFILE WHERE PROFILE_ID = %s", (profile_id,))
        readback = fetch_all(cur)
        logger.info("[update_profile] READ-BACK after SP: %s", readback)
        conn.close()
    except Exception as e:
        logger.warning("[update_profile] read-back failed: %s", e)
    return result
