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
import os
import subprocess
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from typing import Any, Optional

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

def _normalize_sp_result(result):
    """Normalize Snowflake SP results to JSON-serializable Python types."""
    # Snowflake connectors may return VARIANT as bytes.
    if isinstance(result, (bytes, bytearray)):
        result = result.decode("utf-8", errors="replace")

    # Most procedures return a JSON stringified VARIANT.
    if isinstance(result, str):
        try:
            return json.loads(result)
        except json.JSONDecodeError:
            return result

    # Keep already-serializable objects as-is.
    if isinstance(result, (dict, list, int, float, bool)) or result is None:
        return result

    # Fallback for connector-specific objects (e.g. Decimal-like wrappers).
    return str(result)


def _sql_literal(value):
    """Render a safe SQL literal for direct CALL statements."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value).replace("'", "''")
    return f"'{text}'"


def _render_call_sql(sql: str, params: tuple) -> str:
    """Inline params into CALL SQL to avoid connector CALL binding quirks."""
    parts = sql.split("%s")
    if len(parts) - 1 != len(params):
        raise ValueError("CALL SQL placeholder count does not match params")
    out = []
    for i, part in enumerate(parts):
        out.append(part)
        if i < len(params):
            out.append(_sql_literal(params[i]))
    return "".join(out)


def _call_sp(sql: str, params: tuple) -> dict:
    """Call a Snowflake stored procedure and return its VARIANT result as a dict."""
    logger.info("[_call_sp] SQL: %s", sql)
    logger.info("[_call_sp] Params (%d): %s", len(params), params)
    conn = get_connection()
    try:
        cur = conn.cursor()
        if sql.strip().upper().startswith("CALL "):
            rendered = _render_call_sql(sql, params)
            logger.info("[_call_sp] Rendered CALL SQL: %s", rendered)
            cur.execute(rendered)
        else:
            cur.execute(sql, params)
        row = cur.fetchone()
        if row is None:
            raise HTTPException(status_code=500, detail="Stored procedure returned no result")
        result = _normalize_sp_result(row[0])
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
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("[_call_sp] Unhandled error while executing SP call")
        raise HTTPException(status_code=500, detail=f"Stored procedure call failed: {e}")
    finally:
        conn.close()


# ─── Portfolio CRUD ───────────────────────────────────────────────────────────

@router.post("/portfolios")
def create_portfolio(body: PortfolioCreate):
    """Create a new portfolio with starting cash and profile."""
    return _call_sp(
        "CALL MIP.APP.SP_UPSERT_PORTFOLIO(%s::number, %s::varchar, %s::varchar, %s::number(18,2), %s::number, %s::varchar)",
        (None, body.name, body.base_currency, body.starting_cash, body.profile_id, body.notes),
    )


@router.put("/portfolios/{portfolio_id}")
def update_portfolio(portfolio_id: int, body: PortfolioUpdate):
    """Update allowed portfolio fields (name, currency, notes). Starting cash cannot be changed here."""
    return _call_sp(
        "CALL MIP.APP.SP_UPSERT_PORTFOLIO(%s::number, %s::varchar, %s::varchar, %s::number(18,2), %s::number, %s::varchar)",
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
    """Get the full lifecycle event timeline for a portfolio, plus daily series for charts."""
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

        # Daily series: PORTFOLIO_DAILY gives real cash vs equity divergence
        cur.execute(
            """
            SELECT TS, TOTAL_EQUITY, CASH, EQUITY_VALUE, EPISODE_ID
              FROM MIP.APP.PORTFOLIO_DAILY
             WHERE PORTFOLIO_ID = %s
            QUALIFY ROW_NUMBER() OVER (PARTITION BY TS ORDER BY CREATED_AT DESC NULLS LAST, RUN_ID DESC) = 1
             ORDER BY TS ASC
            """,
            (portfolio_id,),
        )
        daily_rows = fetch_all(cur)
        daily_series = []
        for d in daily_rows:
            ts = d.get("TS")
            if ts is not None:
                tss = ts.isoformat() if hasattr(ts, "isoformat") else str(ts)
                daily_series.append({
                    "ts": tss,
                    "equity": float(d["TOTAL_EQUITY"]) if d.get("TOTAL_EQUITY") is not None else None,
                    "cash": float(d["CASH"]) if d.get("CASH") is not None else None,
                    "equity_value": float(d["EQUITY_VALUE"]) if d.get("EQUITY_VALUE") is not None else None,
                    "episode_id": d.get("EPISODE_ID"),
                })

        return {
            "portfolio_id": portfolio_id,
            "event_count": len(rows),
            "events": serialize_rows(rows),
            "daily_series": daily_series,
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
    return _call_sp(
        "CALL MIP.APP.SP_UPSERT_PORTFOLIO_PROFILE(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (
            profile_id, body.name, body.max_positions, body.max_position_pct,
            body.bust_equity_pct, body.bust_action, body.drawdown_stop_pct,
            body.crystallize_enabled, body.profit_target_pct, body.crystallize_mode,
            body.cooldown_days, body.max_episode_days, body.take_profit_on,
            body.description,
        ),
    )


def _try_parse_json_blob(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, (bytes, bytearray)):
        try:
            value = value.decode("utf-8", errors="replace")
        except Exception:
            return str(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            return json.loads(raw)
        except Exception:
            return raw
    return value


@router.post("/ib/daily-job/run")
def run_ib_manual_daily_job(
    target_date: str = Query("current_date()", description="Snowflake date literal, e.g. current_date() or '2026-03-13'"),
    dry_run: bool = Query(False),
    skip_ingest: bool = Query(False),
):
    """
    Manual IB-only daily operator run:
    - Optional IBKR 1440m bar ingest
    - Snowflake catch-up replay for missing eligible days
    """
    project_root = Path(__file__).resolve().parents[5]
    py = project_root / "cursorfiles" / ".venv" / "Scripts" / "python.exe"
    runner = project_root / "cursorfiles" / "run_ib_manual_daily_job.py"
    if not py.exists() or not runner.exists():
        raise HTTPException(
            status_code=500,
            detail="Manual IB daily runner runtime not found (cursorfiles venv or script missing).",
        )

    cmd = [str(py), str(runner), "--target-date", target_date]
    if dry_run:
        cmd.append("--dry-run")
    if skip_ingest:
        cmd.append("--skip-ingest")

    child_env = dict(os.environ)
    for key in list(child_env.keys()):
        if key.startswith("SNOWFLAKE_"):
            child_env.pop(key, None)

    proc = subprocess.run(
        cmd,
        cwd=str(project_root),
        env=child_env,
        capture_output=True,
        text=True,
        timeout=900,
    )
    stdout = (proc.stdout or "").strip()
    stderr = (proc.stderr or "").strip()

    payload: Any = None
    for stream in (stdout, stderr):
        if not stream:
            continue
        idx_arr = stream.find("[")
        idx_obj = stream.find("{")
        idx = idx_arr if idx_arr >= 0 and (idx_obj < 0 or idx_arr < idx_obj) else idx_obj
        if idx < 0:
            continue
        try:
            payload = json.loads(stream[idx:])
            break
        except Exception:
            continue

    if proc.returncode != 0:
        raise HTTPException(
            status_code=500,
            detail={
                "message": "Manual IB daily job failed.",
                "payload": payload,
                "stdout": stdout[-4000:],
                "stderr": stderr[-4000:],
            },
        )

    return {
        "status": "SUCCESS",
        "payload": payload,
    }


@router.get("/ib/daily-job/health")
def get_ib_manual_daily_health():
    """
    Operational status for IB-only daily process:
    - current daily-bar coverage (latest date vs enabled universe)
    - latest successful daily pipeline date
    - catch-up dry-run status
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            with latest as (
                select
                    max(TS::date) as LATEST_DATE,
                    max(TS) as LATEST_TS
                from MIP.MART.MARKET_BARS
                where INTERVAL_MINUTES = 1440
            ),
            u as (
                select distinct upper(replace(SYMBOL, '/', '')) as SYMBOL_N, upper(MARKET_TYPE) as MARKET_TYPE
                from MIP.APP.INGEST_UNIVERSE
                where coalesce(IS_ENABLED, true)
                  and INTERVAL_MINUTES = 1440
            ),
            b as (
                select distinct
                    upper(replace(SYMBOL, '/', '')) as SYMBOL_N,
                    upper(MARKET_TYPE) as MARKET_TYPE
                from MIP.MART.MARKET_BARS
                where INTERVAL_MINUTES = 1440
                  and TS::date = (select LATEST_DATE from latest)
            )
            select
                (select LATEST_DATE from latest) as LATEST_DAILY_BAR_DATE,
                (select LATEST_TS from latest) as LATEST_DAILY_BAR_TS,
                (select count(*) from u) as UNIVERSE_SYMBOLS,
                (select count(*) from b) as BAR_SYMBOLS_ON_LATEST_DATE,
                (
                    select count(*)
                    from u
                    left join b
                      on b.SYMBOL_N = u.SYMBOL_N
                     and b.MARKET_TYPE = u.MARKET_TYPE
                    where b.SYMBOL_N is null
                ) as MISSING_SYMBOLS_ON_LATEST_DATE
            """
        )
        coverage_row = fetch_all(cur)[0]

        cur.execute(
            """
            select
                max(EVENT_TS) as LATEST_PIPELINE_EVENT_TS,
                max(DETAILS:effective_to_ts::timestamp_ntz)::date as LATEST_EFFECTIVE_TO_DATE
            from MIP.APP.MIP_AUDIT_LOG
            where EVENT_TYPE = 'PIPELINE'
              and EVENT_NAME = 'SP_RUN_DAILY_PIPELINE'
              and STATUS in ('SUCCESS', 'SUCCESS_WITH_SKIPS')
            """
        )
        pipeline_row = fetch_all(cur)[0]

        catchup = None
        try:
            cur.execute("call MIP.APP.SP_RUN_IB_DAILY_CATCHUP(current_date(), true)")
            row = cur.fetchone()
            catchup_cell = row[0] if row else None
            catchup = _try_parse_json_blob(catchup_cell)
            if isinstance(catchup, str):
                catchup = _try_parse_json_blob(catchup)
        except Exception as catchup_error:
            catchup = {
                "status": "UNAVAILABLE",
                "error": str(catchup_error),
                "missing_days": None,
            }

        latest_date = coverage_row.get("LATEST_DAILY_BAR_DATE")
        if latest_date is not None and hasattr(latest_date, "isoformat"):
            latest_date = latest_date.isoformat()
        latest_ts = coverage_row.get("LATEST_DAILY_BAR_TS")
        if latest_ts is not None and hasattr(latest_ts, "isoformat"):
            latest_ts = latest_ts.isoformat()
        latest_event_ts = pipeline_row.get("LATEST_PIPELINE_EVENT_TS")
        if latest_event_ts is not None and hasattr(latest_event_ts, "isoformat"):
            latest_event_ts = latest_event_ts.isoformat()
        latest_effective = pipeline_row.get("LATEST_EFFECTIVE_TO_DATE")
        if latest_effective is not None and hasattr(latest_effective, "isoformat"):
            latest_effective = latest_effective.isoformat()

        missing = int(coverage_row.get("MISSING_SYMBOLS_ON_LATEST_DATE") or 0)
        bar_symbols = int(coverage_row.get("BAR_SYMBOLS_ON_LATEST_DATE") or 0)
        universe = int(coverage_row.get("UNIVERSE_SYMBOLS") or 0)
        up_to_date = bool(universe > 0 and missing == 0 and bar_symbols == universe)
        bars_lag_days = None
        raw_latest_date = coverage_row.get("LATEST_DAILY_BAR_DATE")
        if raw_latest_date is not None and hasattr(raw_latest_date, "toordinal"):
            bars_lag_days = (datetime.utcnow().date() - raw_latest_date).days

        return {
            "status": "SUCCESS",
            "up_to_date": up_to_date,
            "coverage": {
                "latest_daily_bar_date": latest_date,
                "latest_daily_bar_ts": latest_ts,
                "universe_symbols": universe,
                "bar_symbols_on_latest_date": bar_symbols,
                "missing_symbols_on_latest_date": missing,
                "bars_lag_days": bars_lag_days,
            },
            "pipeline": {
                "latest_pipeline_event_ts": latest_event_ts,
                "latest_effective_to_date": latest_effective,
            },
            "catchup_dry_run": catchup,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load IB daily job health: {e}")
    finally:
        conn.close()
