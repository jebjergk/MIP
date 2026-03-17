import json
import os
import subprocess
from datetime import date, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, Field

from app.db import get_connection, fetch_all

router = APIRouter(prefix="/manage", tags=["management"])

DEPRECATION_MESSAGE = (
    "Legacy sim management endpoints are retired. "
    "Use /live endpoints for portfolio configuration and activity."
)


def _retired(path: str):
    raise HTTPException(
        status_code=410,
        detail={
            "status": "DEPRECATED",
            "path": path,
            "message": DEPRECATION_MESSAGE,
        },
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


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _compute_ib_duration_str(start_date_iso: str, end_date_iso: str) -> str:
    try:
        start_dt = date.fromisoformat(start_date_iso)
        end_dt = date.fromisoformat(end_date_iso)
    except ValueError:
        return "2 Y"
    days = max((end_dt - start_dt).days + 10, 30)
    years = (days + 364) // 365
    if years <= 1:
        return "1 Y"
    return f"{years} Y"


class IBOnboardingRunRequest(BaseModel):
    symbols: list[str] = Field(..., min_length=1)
    market_type: str = Field(default="STOCK")
    start_date: str = Field(default="2025-08-01")
    end_date: str = Field(default_factory=lambda: date.today().isoformat())
    auto_activate_if_trusted: bool = True
    priority: int = 50
    symbol_cohort: str | None = None
    duration_str: str | None = None
    use_rth: bool = True


@router.post("/ib/daily-job/run")
def run_ib_manual_daily_job(
    target_date: str = Query(
        "to_date(convert_timezone('America/New_York', current_timestamp()))",
        description="Snowflake date literal, e.g. New York market date expression or '2026-03-13'",
    ),
    dry_run: bool = Query(False),
    skip_ingest: bool = Query(False),
    run_pipeline: bool = Query(True, description="After successful IB job, run SP_RUN_DAILY_PIPELINE."),
):
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

    response = {
        "status": "SUCCESS",
        "payload": payload,
        "pipeline_triggered": False,
    }

    if not dry_run and run_pipeline:
        snow_script = project_root / "cursorfiles" / "query_snowflake.py"
        pipeline_cmd = [str(py), str(snow_script), "-q", "call MIP.APP.SP_RUN_DAILY_PIPELINE()", "--json"]
        pipeline_proc = subprocess.run(
            pipeline_cmd,
            cwd=str(project_root),
            env=child_env,
            capture_output=True,
            text=True,
            timeout=1800,
        )
        pipeline_stdout = (pipeline_proc.stdout or "").strip()
        pipeline_stderr = (pipeline_proc.stderr or "").strip()
        pipeline_payload: Any = None
        for stream in (pipeline_stdout, pipeline_stderr):
            if not stream:
                continue
            idx_arr = stream.find("[")
            idx_obj = stream.find("{")
            idx = idx_arr if idx_arr >= 0 and (idx_obj < 0 or idx_arr < idx_obj) else idx_obj
            if idx < 0:
                continue
            try:
                pipeline_payload = json.loads(stream[idx:])
                break
            except Exception:
                continue
        if pipeline_proc.returncode != 0:
            raise HTTPException(
                status_code=500,
                detail={
                    "message": "IB daily job succeeded, but SP_RUN_DAILY_PIPELINE failed.",
                    "payload": payload,
                    "pipeline_payload": pipeline_payload,
                    "pipeline_stdout": pipeline_stdout[-4000:],
                    "pipeline_stderr": pipeline_stderr[-4000:],
                },
            )
        response["pipeline_triggered"] = True
        response["pipeline_result"] = pipeline_payload

    return response


@router.post("/ib/onboarding/run")
def run_ib_symbol_onboarding(payload: IBOnboardingRunRequest):
    project_root = Path(__file__).resolve().parents[5]
    py = project_root / "cursorfiles" / ".venv" / "Scripts" / "python.exe"
    ingest_script = project_root / "cursorfiles" / "ingest_ibkr_bars.py"
    snow_script = project_root / "cursorfiles" / "query_snowflake.py"
    if not py.exists() or not ingest_script.exists() or not snow_script.exists():
        raise HTTPException(
            status_code=500,
            detail="IB onboarding runtime not found (cursorfiles venv/scripts missing).",
        )

    symbols = []
    seen = set()
    for raw in payload.symbols:
        normalized = (raw or "").strip().upper()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        symbols.append(normalized)
    if not symbols:
        raise HTTPException(status_code=400, detail="symbols must include at least one non-empty value.")

    market_type = (payload.market_type or "STOCK").strip().upper()
    run_id = str(uuid4())
    cohort = payload.symbol_cohort or f"IB_ONBOARD_{run_id.replace('-', '')[:8].upper()}"
    duration_str = payload.duration_str or _compute_ib_duration_str(payload.start_date, payload.end_date)
    symbols_csv = ",".join(symbols)

    child_env = dict(os.environ)
    for key in list(child_env.keys()):
        if key.startswith("SNOWFLAKE_"):
            child_env.pop(key, None)

    values_sql = ",".join([f"({_sql_literal(symbol)})" for symbol in symbols])
    upsert_sql = f"""
merge into MIP.APP.INGEST_UNIVERSE t
using (
    select
        v.column1::string as SYMBOL,
        {_sql_literal(market_type)} as MARKET_TYPE,
        1440 as INTERVAL_MINUTES,
        {int(payload.priority)} as PRIORITY,
        {_sql_literal(cohort)} as SYMBOL_COHORT,
        {_sql_literal(f'IB onboarding pre-ingest {run_id}')} as NOTES
    from values {values_sql} v
) s
on upper(t.SYMBOL) = upper(s.SYMBOL)
and upper(t.MARKET_TYPE) = upper(s.MARKET_TYPE)
and t.INTERVAL_MINUTES = s.INTERVAL_MINUTES
when matched then update set
    t.IS_ENABLED = true,
    t.PRIORITY = greatest(coalesce(t.PRIORITY, 0), s.PRIORITY),
    t.SYMBOL_COHORT = s.SYMBOL_COHORT,
    t.NOTES = coalesce(t.NOTES, s.NOTES)
when not matched then insert (
    SYMBOL, MARKET_TYPE, INTERVAL_MINUTES, IS_ENABLED, PRIORITY, SYMBOL_COHORT, NOTES
) values (
    s.SYMBOL, s.MARKET_TYPE, s.INTERVAL_MINUTES, true, s.PRIORITY, s.SYMBOL_COHORT, s.NOTES
)
"""

    pre_cmd = [str(py), str(snow_script), "-q", upsert_sql]
    pre_proc = subprocess.run(
        pre_cmd,
        cwd=str(project_root),
        env=child_env,
        capture_output=True,
        text=True,
        timeout=600,
    )
    if pre_proc.returncode != 0:
        raise HTTPException(
            status_code=500,
            detail={
                "message": "Failed to upsert ingest universe rows for onboarding.",
                "stdout": (pre_proc.stdout or "")[-4000:],
                "stderr": (pre_proc.stderr or "")[-4000:],
            },
        )

    ingest_cmd = [
        str(py),
        str(ingest_script),
        "--interval-minutes",
        "1440",
        "--duration-str",
        duration_str,
        "--symbols",
        symbols_csv,
    ]
    if payload.use_rth:
        ingest_cmd.append("--use-rth")

    ingest_proc = subprocess.run(
        ingest_cmd,
        cwd=str(project_root),
        env=child_env,
        capture_output=True,
        text=True,
        timeout=1800,
    )
    ingest_stdout = (ingest_proc.stdout or "").strip()
    ingest_stderr = (ingest_proc.stderr or "").strip()
    ingest_payload = _try_parse_json_blob(ingest_stdout) or _try_parse_json_blob(ingest_stderr)
    if ingest_proc.returncode != 0:
        raise HTTPException(
            status_code=500,
            detail={
                "message": "IB daily ingest failed for onboarding symbols.",
                "ingest_payload": ingest_payload,
                "stdout": ingest_stdout[-4000:],
                "stderr": ingest_stderr[-4000:],
            },
        )

    symbols_json = json.dumps(symbols).replace("'", "''")
    onboarding_sql = (
        "call MIP.APP.SP_RUN_IB_SYMBOL_ONBOARDING("
        f"parse_json('{symbols_json}'), "
        f"{_sql_literal(market_type)}, "
        f"{_sql_literal(payload.start_date)}::date, "
        f"{_sql_literal(payload.end_date)}::date, "
        f"{'true' if payload.auto_activate_if_trusted else 'false'}, "
        f"{_sql_literal(run_id)}, "
        f"{_sql_literal(cohort)}, "
        f"{int(payload.priority)}"
        ")"
    )
    onboarding_cmd = [str(py), str(snow_script), "-q", onboarding_sql, "--json"]
    onboard_proc = subprocess.run(
        onboarding_cmd,
        cwd=str(project_root),
        env=child_env,
        capture_output=True,
        text=True,
        timeout=7200,
    )
    onboard_stdout = (onboard_proc.stdout or "").strip()
    onboard_stderr = (onboard_proc.stderr or "").strip()
    onboard_payload = _try_parse_json_blob(onboard_stdout) or _try_parse_json_blob(onboard_stderr)
    if onboard_proc.returncode != 0:
        raise HTTPException(
            status_code=500,
            detail={
                "message": "IB onboarding procedure failed.",
                "sql": onboarding_sql,
                "ingest_payload": ingest_payload,
                "onboarding_payload": onboard_payload,
                "stdout": onboard_stdout[-4000:],
                "stderr": onboard_stderr[-4000:],
            },
        )

    return {
        "status": "SUCCESS",
        "run_id": run_id,
        "symbol_cohort": cohort,
        "market_type": market_type,
        "symbols_requested": symbols,
        "duration_str": duration_str,
        "ingest_result": ingest_payload,
        "onboarding_result": onboard_payload,
    }


@router.get("/ib/daily-job/health")
def get_ib_manual_daily_health():
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


@router.api_route("/{subpath:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
def retired_management_subpaths(subpath: str, request: Request):
    if subpath.startswith("ib/daily-job/"):
        raise HTTPException(status_code=404, detail="Not found")
    _retired(f"/manage/{subpath} [{request.method}]")
