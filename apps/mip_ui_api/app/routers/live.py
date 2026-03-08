"""
GET /live/metrics — lightweight live metrics for header and Suggestions.
Read-only. Returns api_ok, snowflake_ok, updated_at, last_run, last_brief, outcomes.
"""
import json
import os
import subprocess
import uuid
from pathlib import Path
from datetime import datetime, timezone

from fastapi import APIRouter, Query, HTTPException
from pydantic import BaseModel, Field

from app.config import get_snowflake_config
from app.db import get_connection, fetch_all, serialize_row, serialize_rows, SnowflakeAuthError

router = APIRouter(prefix="/live", tags=["live"])


class PmAcceptRequest(BaseModel):
    actor: str


class ComplianceDecisionRequest(BaseModel):
    actor: str
    decision: str = Field(pattern="^(APPROVE|DENY)$")
    notes: str | None = None
    reference_id: str | None = None


class LivePortfolioConfigUpsertRequest(BaseModel):
    sim_portfolio_id: int | None = None
    ibkr_account_id: str | None = None
    adapter_mode: str | None = Field(default=None, pattern="^(PAPER|LIVE)$")
    base_currency: str | None = None
    max_positions: int | None = None
    max_position_pct: float | None = None
    cash_buffer_pct: float | None = None
    max_slippage_pct: float | None = None
    validity_window_sec: int | None = None
    quote_freshness_threshold_sec: int | None = None
    snapshot_freshness_threshold_sec: int | None = None
    drawdown_stop_pct: float | None = None
    bust_pct: float | None = None
    cooldown_bars: int | None = None
    is_active: bool | None = None


class ImportLiveActionsFromProposalsRequest(BaseModel):
    live_portfolio_id: int
    run_id: str | None = None
    limit: int = Field(default=100, ge=1, le=1000)


class ExecuteLiveActionRequest(BaseModel):
    actor: str
    attempt_n: int = 1


class UpdateLiveOrderStatusRequest(BaseModel):
    actor: str
    status: str = Field(pattern="^(PARTIAL_FILL|FILLED|CANCELED|REJECTED)$")
    qty_filled: float | None = None
    avg_fill_price: float | None = None
    broker_order_id: str | None = None
    notes: str | None = None


_ALLOWED_TRANSITIONS = {
    "RESEARCH_IMPORTED": {"PM_ACCEPTED"},
    "PROPOSED": {"PM_ACCEPTED"},
    "PM_ACCEPTED": {"COMPLIANCE_APPROVED", "COMPLIANCE_DENIED"},
    "COMPLIANCE_APPROVED": {"REVALIDATED_PASS", "REVALIDATED_FAIL"},
    "REVALIDATED_FAIL": {"REVALIDATED_PASS", "REVALIDATED_FAIL"},
    "REVALIDATED_PASS": {"REVALIDATED_PASS", "EXECUTION_REQUESTED"},
}

LIVE_POLICY_VERSION = "phase2_session_realism_v1"
EXECUTION_CLICK_MAX_REVALIDATION_SEC = 300


def _fetch_live_action(cur, action_id: str) -> dict | None:
    cur.execute(
        """
        select
          ACTION_ID, PROPOSAL_ID, PORTFOLIO_ID, SYMBOL, SIDE, PROPOSED_QTY, PROPOSED_PRICE,
          STATUS, VALIDITY_WINDOW_END, COMPLIANCE_STATUS, REVALIDATION_TS, REVALIDATION_PRICE,
          PRICE_DEVIATION_PCT, PRICE_GUARD_RESULT, REASON_CODES, EXECUTION_PRICE_SOURCE,
          PARAM_SNAPSHOT, ONE_MIN_BAR_TS
        from MIP.LIVE.LIVE_ACTIONS
        where ACTION_ID = %s
        """,
        (action_id,),
    )
    rows = fetch_all(cur)
    return rows[0] if rows else None


def _assert_transition_allowed(current_status: str | None, target_status: str) -> None:
    allowed = _ALLOWED_TRANSITIONS.get((current_status or "").upper(), set())
    if target_status not in allowed:
        raise HTTPException(
            status_code=409,
            detail=f"Invalid status transition: {current_status} -> {target_status}",
        )


def _write_reason_codes(cur, action_id: str, reason_codes: list[str]) -> None:
    cur.execute(
        """
        update MIP.LIVE.LIVE_ACTIONS
           set REASON_CODES = parse_json(%s),
               UPDATED_AT = current_timestamp()
         where ACTION_ID = %s
        """,
        (json.dumps(reason_codes), action_id),
    )


def _append_learning_ledger_event(
    cur,
    *,
    event_name: str,
    status: str,
    action_before: dict | None,
    action_after: dict | None,
    influence_delta: dict | None = None,
    outcome_state: dict | None = None,
    policy_version: str | None = None,
) -> None:
    """
    Best-effort append to canonical learning ledger.
    Never raise to caller.
    """
    try:
        after = action_after or {}
        before = action_before or {}
        after_snapshot = _parse_variant(after.get("PARAM_SNAPSHOT"))
        before_snapshot = _parse_variant(before.get("PARAM_SNAPSHOT"))
        run_id = (
            after.get("RUN_ID_VARCHAR")
            or before.get("RUN_ID_VARCHAR")
            or after_snapshot.get("run_id")
            or before_snapshot.get("run_id")
            or None
        )
        portfolio_id = after.get("PORTFOLIO_ID") or before.get("PORTFOLIO_ID")
        proposal_id = after.get("PROPOSAL_ID") or before.get("PROPOSAL_ID")
        symbol = after.get("SYMBOL") or before.get("SYMBOL")
        market_type = after.get("ASSET_CLASS") or before.get("ASSET_CLASS")
        live_action_id = after.get("ACTION_ID") or before.get("ACTION_ID")

        cur.execute(
            """
            call MIP.APP.SP_LEDGER_APPEND_EVENT(
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                try_parse_json(%s), try_parse_json(%s), try_parse_json(%s), try_parse_json(%s), try_parse_json(%s), %s
            )
            """,
            (
                "LIVE_EVENT",
                event_name,
                status,
                run_id,
                run_id,
                portfolio_id,
                proposal_id,
                live_action_id,
                None,  # live_order_id
                symbol,
                market_type,
                None,  # training_version
                policy_version,
                None,  # source facts hash
                json.dumps({"source": "live_router"}),
                json.dumps(before),
                json.dumps(after),
                json.dumps(influence_delta or {}),
                json.dumps({
                    "action_id": live_action_id,
                    "proposal_id": proposal_id,
                    "run_id": run_id,
                }),
                json.dumps(outcome_state or {}),
            ),
        )
    except Exception:
        # Non-fatal by design.
        return


def _parse_variant(v):
    if v is None:
        return {}
    if isinstance(v, dict):
        return v
    if isinstance(v, str):
        try:
            return json.loads(v)
        except Exception:
            return {}
    return {}


def _first_session_realism_checks(cur, action: dict, cfg: dict) -> tuple[list[str], dict]:
    """
    Fail-closed realism checks:
    require 1m-bar-sourced revalidation and fresh 1m market data.
    """
    reason_codes: list[str] = []
    symbol = action.get("SYMBOL")
    if not symbol:
        reason_codes.append("FIRST_SESSION_REALISM_MISSING_SYMBOL")
        return reason_codes, {"has_symbol": False}

    source = (action.get("EXECUTION_PRICE_SOURCE") or "").upper()
    if source != "ONE_MINUTE_BAR":
        reason_codes.append("FIRST_SESSION_REALISM_SOURCE_REQUIRED")

    one_min_bar_ts = action.get("ONE_MIN_BAR_TS")
    if not one_min_bar_ts:
        reason_codes.append("FIRST_SESSION_REALISM_MISSING_1M_REFERENCE")

    cur.execute(
        """
        select TS, CLOSE
        from MIP.MART.MARKET_BARS
        where SYMBOL = %s
          and INTERVAL_MINUTES = 1
        order by TS desc
        limit 1
        """,
        (symbol,),
    )
    latest_bar = cur.fetchone()
    latest_ts = latest_bar[0] if latest_bar else None
    latest_close = latest_bar[1] if latest_bar else None
    if not latest_ts:
        reason_codes.append("FIRST_SESSION_REALISM_NO_1M_BAR")
    else:
        now_utc = datetime.now(timezone.utc)
        bar_ts_utc = latest_ts.replace(tzinfo=timezone.utc)
        bar_age_sec = (now_utc - bar_ts_utc).total_seconds()
        max_age_sec = int(cfg.get("QUOTE_FRESHNESS_THRESHOLD_SEC") or 60)
        if bar_age_sec > max_age_sec:
            reason_codes.append("FIRST_SESSION_REALISM_1M_STALE")
        if one_min_bar_ts and latest_ts and one_min_bar_ts != latest_ts:
            reason_codes.append("FIRST_SESSION_REALISM_REVALIDATION_NOT_LATEST")

    details = {
        "symbol": symbol,
        "execution_price_source": source or None,
        "one_min_bar_ts": one_min_bar_ts,
        "latest_one_min_bar_ts": latest_ts,
        "latest_one_min_close": latest_close,
        "quote_freshness_threshold_sec": cfg.get("QUOTE_FRESHNESS_THRESHOLD_SEC"),
    }
    return reason_codes, details


def _safe_early_exit_details(details: dict) -> dict:
    steps = details.get("steps") if isinstance(details, dict) else {}
    ingestion = steps.get("ingestion") if isinstance(steps, dict) else {}
    early_exit = steps.get("early_exit") if isinstance(steps, dict) else {}
    return {
        "run_id": details.get("run_id"),
        "started_at": details.get("started_at"),
        "completed_at": details.get("completed_at"),
        "interval_minutes": details.get("interval_minutes"),
        "bars_ingested": details.get("bars_ingested"),
        "symbols_processed": details.get("symbols_processed"),
        "positions_evaluated": details.get("positions_evaluated"),
        "exit_signals": details.get("exit_signals"),
        "exits_executed": details.get("exits_executed"),
        "steps": {
            "ingestion_status": ingestion.get("status") if isinstance(ingestion, dict) else None,
            "early_exit_status": early_exit.get("status") if isinstance(early_exit, dict) else None,
        },
    }


def _summary_hint_from_status_and_details(status: str | None, details: dict | None) -> str | None:
    """Derive a short summary hint for the run (same as runs router)."""
    if not status:
        return None
    s = (status or "").upper()
    if s == "SKIPPED_NO_NEW_BARS":
        return "No new bars"
    if s == "SKIP_RATE_LIMIT":
        return "Rate limit"
    if s == "SUCCESS_WITH_SKIPS":
        return "Success with skips"
    if s == "FAIL":
        return "Failed"
    if s == "SUCCESS":
        return None
    return None


@router.get("/metrics")
def get_live_metrics(portfolio_id: int = Query(1, description="Portfolio ID for latest brief")):
    """
    Single cheap request for UI to poll every 30–60s.
    Returns: api_ok, snowflake_ok, updated_at, last_run, last_brief, outcomes.
    last_brief uses found: false when no brief exists for portfolio_id.
    outcomes.since_last_run = count of outcomes with CALCULATED_AT > last_run.completed_at (or null/0 when no run).
    """
    updated_at = datetime.now(timezone.utc).isoformat()
    api_ok = True
    snowflake_ok = False
    last_run = None
    last_intraday_run = None
    last_brief = {"found": False}
    outcomes = {"total": 0, "last_calculated_at": None, "since_last_run": None}

    try:
        conn = get_connection()
        snowflake_ok = True
    except SnowflakeAuthError:
        pass
    except Exception:
        pass

    if not snowflake_ok:
        return {
            "api_ok": api_ok,
            "snowflake_ok": snowflake_ok,
            "updated_at": updated_at,
            "last_run": last_run,
            "last_intraday_run": last_intraday_run,
            "last_brief": last_brief,
            "outcomes": outcomes,
        }

    try:
        cur = conn.cursor()

        # --- Last run: same logic as /runs (MIP_AUDIT_LOG, PIPELINE, SP_RUN_DAILY_PIPELINE), most recent by completion
        runs_sql = """
        select EVENT_TS, RUN_ID, STATUS, DETAILS
        from MIP.APP.MIP_AUDIT_LOG
        where EVENT_TYPE = 'PIPELINE' and EVENT_NAME = 'SP_RUN_DAILY_PIPELINE'
        order by EVENT_TS desc
        limit 200
        """
        cur.execute(runs_sql)
        run_rows = fetch_all(cur)
        runs_by_id = {}
        for r in run_rows:
            run_id = r.get("RUN_ID")
            if not run_id:
                continue
            ts = r.get("EVENT_TS")
            status = r.get("STATUS") or ""
            details = r.get("DETAILS")
            if isinstance(details, str):
                try:
                    details = json.loads(details) if details else {}
                except Exception:
                    details = {}
            if run_id not in runs_by_id:
                runs_by_id[run_id] = {"started_at": ts, "completed_at": ts, "status": status, "details": details}
            else:
                run = runs_by_id[run_id]
                if ts:
                    if run["started_at"] is None or (ts < run["started_at"]):
                        run["started_at"] = ts
                    if run["completed_at"] is None or (ts > run["completed_at"]):
                        run["completed_at"] = ts
                        run["status"] = status
                        run["details"] = details
        run_list = [
            {
                "run_id": rid,
                "started_at": r["started_at"].isoformat() if hasattr(r["started_at"], "isoformat") else r["started_at"],
                "completed_at": r["completed_at"].isoformat() if hasattr(r["completed_at"], "isoformat") else r["completed_at"],
                "status": r["status"] if r["status"] != "START" else "RUNNING",
                "summary_hint": _summary_hint_from_status_and_details(r["status"], r.get("details")),
            }
            for rid, r in runs_by_id.items()
        ]
        run_list.sort(key=lambda x: (x["completed_at"] or x["started_at"] or ""), reverse=True)
        if run_list:
            last_run = serialize_row(run_list[0])

        # --- Last intraday run: most recent from INTRADAY_PIPELINE_RUN_LOG
        intraday_sql = """
        select RUN_ID, STARTED_AT, COMPLETED_AT, STATUS,
               BARS_INGESTED, SIGNALS_GENERATED, SYMBOLS_PROCESSED
        from MIP.APP.INTRADAY_PIPELINE_RUN_LOG
        order by STARTED_AT desc
        limit 1
        """
        try:
            cur.execute(intraday_sql)
            irow = cur.fetchone()
            if irow:
                icols = [d[0] for d in cur.description]
                ir = dict(zip(icols, irow))
                ir_status = ir.get("STATUS") or ""
                last_intraday_run = serialize_row({
                    "run_id": ir.get("RUN_ID"),
                    "started_at": ir["STARTED_AT"].isoformat() if hasattr(ir.get("STARTED_AT"), "isoformat") else ir.get("STARTED_AT"),
                    "completed_at": ir["COMPLETED_AT"].isoformat() if hasattr(ir.get("COMPLETED_AT"), "isoformat") else ir.get("COMPLETED_AT"),
                    "status": ir_status if ir_status != "START" else "RUNNING",
                    "bars_ingested": ir.get("BARS_INGESTED"),
                    "signals_generated": ir.get("SIGNALS_GENERATED"),
                    "symbols_processed": ir.get("SYMBOLS_PROCESSED"),
                })
        except Exception:
            pass

        # --- Last brief: same as /briefs/latest
        brief_sql = """
        select
          mb.PORTFOLIO_ID as portfolio_id,
          coalesce(
            try_cast(mb.BRIEF:as_of_ts::varchar as timestamp_ntz),
            try_cast(get_path(mb.BRIEF, 'attribution.as_of_ts')::varchar as timestamp_ntz),
            mb.AS_OF_TS
          ) as as_of_ts,
          coalesce(
            get_path(mb.BRIEF, 'attribution.pipeline_run_id')::varchar,
            mb.PIPELINE_RUN_ID
          ) as pipeline_run_id,
          mb.AGENT_NAME as agent_name
        from MIP.AGENT_OUT.MORNING_BRIEF mb
        where mb.PORTFOLIO_ID = %s and coalesce(mb.AGENT_NAME, '') = 'MORNING_BRIEF'
        order by mb.AS_OF_TS desc
        limit 1
        """
        cur.execute(brief_sql, (portfolio_id,))
        brief_row = cur.fetchone()
        if brief_row:
            cols = [d[0] for d in cur.description]
            last_brief = serialize_row(dict(zip(cols, brief_row)))
            last_brief["found"] = True
        else:
            last_brief = {"found": False}

        # --- Outcomes: total, max(CALCULATED_AT), and since_last_run
        outcomes_sql = """
        select count(*) as total, max(CALCULATED_AT) as last_calculated_at
        from MIP.APP.RECOMMENDATION_OUTCOMES
        """
        cur.execute(outcomes_sql)
        out_row = cur.fetchone()
        if out_row:
            outcomes["total"] = int(out_row[0]) if out_row[0] is not None else 0
            lca = out_row[1]
            outcomes["last_calculated_at"] = lca.isoformat() if hasattr(lca, "isoformat") else (str(lca) if lca else None)

        last_run_completed_at = None
        if last_run and last_run.get("completed_at"):
            last_run_completed_at = last_run["completed_at"]

        if last_run_completed_at is not None:
            # Count outcomes where CALCULATED_AT > last_run.completed_at
            # last_run_completed_at is already ISO string from serialize_row
            since_sql = """
            select count(*) as cnt from MIP.APP.RECOMMENDATION_OUTCOMES
            where CALCULATED_AT > %s
            """
            cur.execute(since_sql, (last_run_completed_at,))
            since_row = cur.fetchone()
            outcomes["since_last_run"] = int(since_row[0]) if since_row and since_row[0] is not None else 0
        else:
            outcomes["since_last_run"] = 0

        conn.close()
    except Exception:
        snowflake_ok = False
        try:
            conn.close()
        except Exception:
            pass

    return {
        "api_ok": api_ok,
        "snowflake_ok": snowflake_ok,
        "updated_at": updated_at,
        "last_run": last_run,
        "last_intraday_run": last_intraday_run,
        "last_brief": last_brief,
        "outcomes": outcomes,
    }


def _project_root() -> Path:
    # .../MIP/apps/mip_ui_api/app/routers/live.py -> repo root
    return Path(__file__).resolve().parent.parent.parent.parent.parent.parent


def _run_on_demand_snapshot_sync(
    host: str,
    port: int,
    client_id: int,
    account: str | None,
    portfolio_id: int | None,
) -> dict:
    root = _project_root()
    py = root / "cursorfiles" / ".venv" / "Scripts" / "python.exe"
    script = root / "cursorfiles" / "sync_ibkr_paper_snapshot.py"
    if not py.exists() or not script.exists():
        raise HTTPException(
            status_code=500,
            detail="Snapshot sync runtime not found (cursorfiles venv or sync script missing).",
        )

    cmd = [str(py), str(script), "--once", "--host", host, "--port", str(port), "--client-id", str(client_id)]
    if account:
        cmd.extend(["--account", account])
    if portfolio_id is not None:
        cmd.extend(["--portfolio-id", str(portfolio_id)])

    # Ensure the snapshot script resolves credentials from .env.agent instead of
    # inheriting API process Snowflake env (read-only role).
    child_env = dict(os.environ)
    for key in list(child_env.keys()):
        if key.startswith("SNOWFLAKE_"):
            child_env.pop(key, None)

    proc = subprocess.run(
        cmd,
        cwd=str(root),
        env=child_env,
        capture_output=True,
        text=True,
        timeout=90,
    )
    if proc.returncode != 0:
        raise HTTPException(
            status_code=502,
            detail={
                "message": "On-demand snapshot sync failed.",
                "stderr": proc.stderr[-4000:],
                "stdout": proc.stdout[-4000:],
            },
        )
    out = (proc.stdout or "").strip()
    # script prints JSON payload on success
    try:
        json_start = out.rfind("{")
        payload = json.loads(out[json_start:]) if json_start >= 0 else {}
    except Exception:
        payload = {"raw_output": out}
    return payload


def _run_agent_snowflake_query(query: str, timeout_sec: int = 120) -> list | dict:
    """
    Execute a Snowflake query via agent runtime (.env.agent / CURSOR_AGENT).
    """
    root = _project_root()
    py = root / "cursorfiles" / ".venv" / "Scripts" / "python.exe"
    script = root / "cursorfiles" / "query_snowflake.py"
    if not py.exists() or not script.exists():
        raise HTTPException(
            status_code=500,
            detail="Agent Snowflake runtime not found (cursorfiles venv or query script missing).",
        )

    cmd = [str(py), str(script), "-q", query, "--json"]
    child_env = dict(os.environ)
    for key in list(child_env.keys()):
        if key.startswith("SNOWFLAKE_"):
            child_env.pop(key, None)

    proc = subprocess.run(
        cmd,
        cwd=str(root),
        env=child_env,
        capture_output=True,
        text=True,
        timeout=timeout_sec,
    )
    if proc.returncode != 0:
        raise HTTPException(
            status_code=502,
            detail={
                "message": "Agent Snowflake query failed.",
                "stderr": proc.stderr[-4000:],
                "stdout": proc.stdout[-4000:],
            },
        )

    out = (proc.stdout or "").strip()
    json_start = out.find("[")
    if json_start < 0:
        json_start = out.find("{")
    if json_start < 0:
        return []
    try:
        return json.loads(out[json_start:])
    except Exception:
        return []


@router.post("/snapshot/refresh")
def refresh_live_snapshot(
    portfolio_id: int | None = Query(None, description="Optional LIVE portfolio ID to stamp snapshot rows"),
    account: str | None = Query(None, description="IBKR account code (optional if only one managed account)"),
    host: str = Query("127.0.0.1", description="IB Gateway/TWS host"),
    port: int = Query(4002, description="IB paper port (4002 for Gateway paper, 7497 for TWS paper)"),
    client_id: int = Query(9402, description="IB client id"),
):
    """
    On-demand snapshot refresh.
    Triggers a single IBKR read-only pull and stores results in MIP.LIVE.BROKER_SNAPSHOTS.
    Intended for:
      - opening Live Portfolio page
      - opening Live Trade/Approval page
      - pre-trade and post-trade refresh
    """
    result = _run_on_demand_snapshot_sync(
        host=host,
        port=port,
        client_id=client_id,
        account=account,
        portfolio_id=portfolio_id,
    )
    return {
        "ok": True,
        "mode": "on_demand",
        "result": result,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/snapshot/latest")
def get_latest_live_snapshot(
    portfolio_id: int | None = Query(None, description="Optional portfolio filter"),
    account: str | None = Query(None, description="Optional IBKR account filter"),
):
    """
    Latest snapshot records from MIP.LIVE for display.
    Returns latest NAV, cash rows, positions, and open orders (trimmed).
    """
    conn = get_connection()
    try:
        cur = conn.cursor()

        where = []
        params: list = []
        if portfolio_id is not None:
            where.append("PORTFOLIO_ID = %s")
            params.append(portfolio_id)
        if account:
            where.append("IBKR_ACCOUNT_ID = %s")
            params.append(account)
        where_sql = (" where " + " and ".join(where)) if where else ""

        nav_sql = f"""
        select SNAPSHOT_TS, IBKR_ACCOUNT_ID, PORTFOLIO_ID, CURRENCY,
               NET_LIQUIDATION_EUR, TOTAL_CASH_EUR, GROSS_POSITION_VALUE_EUR
        from MIP.LIVE.BROKER_SNAPSHOTS
        {where_sql}
          and SNAPSHOT_TYPE = 'NAV'
        order by SNAPSHOT_TS desc
        limit 1
        """ if where else """
        select SNAPSHOT_TS, IBKR_ACCOUNT_ID, PORTFOLIO_ID, CURRENCY,
               NET_LIQUIDATION_EUR, TOTAL_CASH_EUR, GROSS_POSITION_VALUE_EUR
        from MIP.LIVE.BROKER_SNAPSHOTS
        where SNAPSHOT_TYPE = 'NAV'
        order by SNAPSHOT_TS desc
        limit 1
        """
        if params:
            cur.execute(nav_sql, tuple(params))
        else:
            cur.execute(nav_sql)
        nav_row = cur.fetchone()
        nav_cols = [d[0] for d in cur.description] if cur.description else []
        nav = serialize_row(dict(zip(nav_cols, nav_row))) if nav_row else None

        # Determine timestamp/account from latest nav when possible
        latest_ts = nav.get("SNAPSHOT_TS") if nav else None
        latest_account = nav.get("IBKR_ACCOUNT_ID") if nav else account

        cash = []
        positions = []
        open_orders = []
        if latest_ts and latest_account:
            cur.execute(
                """
                select SNAPSHOT_TS, IBKR_ACCOUNT_ID, CURRENCY, CASH_BALANCE, SETTLED_CASH
                from MIP.LIVE.BROKER_SNAPSHOTS
                where SNAPSHOT_TYPE = 'CASH'
                  and SNAPSHOT_TS = %s
                  and IBKR_ACCOUNT_ID = %s
                order by CURRENCY
                """,
                (latest_ts, latest_account),
            )
            cash = fetch_all(cur)

            cur.execute(
                """
                select SNAPSHOT_TS, IBKR_ACCOUNT_ID, SYMBOL, SECURITY_TYPE, EXCHANGE, CURRENCY,
                       POSITION_QTY, AVG_COST
                from MIP.LIVE.BROKER_SNAPSHOTS
                where SNAPSHOT_TYPE = 'POSITION'
                  and SNAPSHOT_TS = %s
                  and IBKR_ACCOUNT_ID = %s
                order by SYMBOL
                limit 200
                """,
                (latest_ts, latest_account),
            )
            positions = fetch_all(cur)

            cur.execute(
                """
                select SNAPSHOT_TS, IBKR_ACCOUNT_ID, OPEN_ORDER_ID, OPEN_ORDER_STATUS,
                       SYMBOL, OPEN_ORDER_QTY, OPEN_ORDER_FILLED, OPEN_ORDER_REMAINING,
                       OPEN_ORDER_LIMIT_PRICE
                from MIP.LIVE.BROKER_SNAPSHOTS
                where SNAPSHOT_TYPE = 'OPEN_ORDER'
                  and SNAPSHOT_TS = %s
                  and IBKR_ACCOUNT_ID = %s
                order by OPEN_ORDER_ID
                limit 200
                """,
                (latest_ts, latest_account),
            )
            open_orders = fetch_all(cur)

        return {
            "ok": True,
            "mode": "on_demand",
            "latest_nav": nav,
            "cash": serialize_rows(cash),
            "positions": serialize_rows(positions),
            "open_orders": serialize_rows(open_orders),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
    finally:
        conn.close()


@router.get("/early-exit/status")
def get_early_exit_status(
    limit: int = Query(25, ge=1, le=200),
    include_raw: bool = Query(False, description="Include raw DETAILS payload from audit log"),
):
    """
    Hourly early-exit monitor status and recent run history.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            select
              max(case when CONFIG_KEY = 'EARLY_EXIT_ENABLED' then CONFIG_VALUE end) as EARLY_EXIT_ENABLED,
              max(case when CONFIG_KEY = 'EARLY_EXIT_INTERVAL_MINUTES' then CONFIG_VALUE end) as EARLY_EXIT_INTERVAL_MINUTES
            from MIP.APP.APP_CONFIG
            where CONFIG_KEY in ('EARLY_EXIT_ENABLED', 'EARLY_EXIT_INTERVAL_MINUTES')
            """
        )
        cfg_rows = fetch_all(cur)
        cfg = cfg_rows[0] if cfg_rows else {}
        enabled = str(cfg.get("EARLY_EXIT_ENABLED") or "").strip().lower() in ("1", "true", "yes", "on")
        interval_minutes = int(cfg.get("EARLY_EXIT_INTERVAL_MINUTES") or 60)

        cur.execute(
            """
            select
              EVENT_TS,
              RUN_ID,
              STATUS,
              ROWS_AFFECTED,
              DETAILS
            from MIP.APP.MIP_AUDIT_LOG
            where EVENT_TYPE = 'EARLY_EXIT_PIPELINE'
              and EVENT_NAME = 'SP_RUN_HOURLY_EARLY_EXIT_MONITOR'
            order by EVENT_TS desc
            limit %s
            """,
            (limit,),
        )
        rows = fetch_all(cur)
        runs = []
        for r in rows:
            details = _parse_variant(r.get("DETAILS"))
            safe_details = _safe_early_exit_details(details)
            runs.append({
                "event_ts": r.get("EVENT_TS"),
                "run_id": r.get("RUN_ID"),
                "status": r.get("STATUS"),
                "rows_affected": r.get("ROWS_AFFECTED"),
                "interval_minutes": safe_details.get("interval_minutes"),
                "bars_ingested": safe_details.get("bars_ingested"),
                "positions_evaluated": safe_details.get("positions_evaluated"),
                "exit_signals": safe_details.get("exit_signals"),
                "exits_executed": safe_details.get("exits_executed"),
                "details": safe_details,
                "details_raw": details if include_raw else None,
            })

        return {
            "enabled": enabled,
            "interval_minutes": interval_minutes,
            "latest": serialize_row(runs[0]) if runs else None,
            "runs": serialize_rows(runs),
            "count": len(runs),
        }
    finally:
        conn.close()


@router.post("/early-exit/run")
def run_early_exit_monitor():
    """
    Trigger one on-demand hourly early-exit monitor run.
    """
    raw_results = _run_agent_snowflake_query("call MIP.APP.SP_RUN_HOURLY_EARLY_EXIT_MONITOR()", timeout_sec=300)
    raw = raw_results[0] if isinstance(raw_results, list) and raw_results else (raw_results if isinstance(raw_results, dict) else {})
    payload = raw
    if isinstance(raw, dict) and len(raw) == 1:
        payload = next(iter(raw.values()))
    result = _parse_variant(payload) if not isinstance(payload, dict) else payload
    status = str(result.get("status") or "UNKNOWN").upper()

    # Best-effort local ledger append through API connection.
    conn = get_connection()
    try:
        cur = conn.cursor()
        _append_learning_ledger_event(
            cur,
            event_name="LIVE_EARLY_EXIT_MONITOR_RUN",
            status=status,
            action_before=None,
            action_after={
                "RUN_ID_VARCHAR": result.get("run_id"),
                "PARAM_SNAPSHOT": {
                    "interval_minutes": result.get("interval_minutes"),
                },
            },
            policy_version=LIVE_POLICY_VERSION,
            influence_delta={
                "positions_evaluated": result.get("positions_evaluated"),
                "exit_signals": result.get("exit_signals"),
                "exits_executed": result.get("exits_executed"),
            },
            outcome_state=result,
        )
    finally:
        conn.close()

    return {"ok": True, "result": serialize_row(result), "status": status}


@router.get("/drift/status")
def get_broker_drift_status(portfolio_id: int | None = Query(None, description="Live portfolio ID (optional)")):
    """
    Broker-truth drift status for a live portfolio.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        if portfolio_id is None:
            cur.execute(
                """
                select
                  PORTFOLIO_ID,
                  IBKR_ACCOUNT_ID,
                  DRIFT_STATUS,
                  SNAPSHOT_FRESHNESS_THRESHOLD_SEC,
                  QUOTE_FRESHNESS_THRESHOLD_SEC,
                  UPDATED_AT
                from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
                where coalesce(IS_ACTIVE, true) = true
                order by PORTFOLIO_ID
                limit 1
                """
            )
        else:
            cur.execute(
                """
                select
                  PORTFOLIO_ID,
                  IBKR_ACCOUNT_ID,
                  DRIFT_STATUS,
                  SNAPSHOT_FRESHNESS_THRESHOLD_SEC,
                  QUOTE_FRESHNESS_THRESHOLD_SEC,
                  UPDATED_AT
                from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
                where PORTFOLIO_ID = %s
                """,
                (portfolio_id,),
            )
        cfg_rows = fetch_all(cur)
        if not cfg_rows:
            return {
                "portfolio_id": portfolio_id,
                "ibkr_account_id": None,
                "drift_status": "NO_CONFIG",
                "snapshot_freshness_threshold_sec": None,
                "latest_snapshot": None,
                "snapshot_age_sec": None,
                "unresolved_drift_count": 0,
                "latest_unresolved_drift": None,
            }
        cfg = cfg_rows[0]
        resolved_portfolio_id = cfg.get("PORTFOLIO_ID")
        account_id = cfg.get("IBKR_ACCOUNT_ID")

        cur.execute(
            """
            select SNAPSHOT_TS, NET_LIQUIDATION_EUR, TOTAL_CASH_EUR
            from MIP.LIVE.BROKER_SNAPSHOTS
            where SNAPSHOT_TYPE = 'NAV'
              and IBKR_ACCOUNT_ID = %s
            order by SNAPSHOT_TS desc
            limit 1
            """,
            (account_id,),
        )
        nav_rows = fetch_all(cur)
        nav = nav_rows[0] if nav_rows else {}
        snapshot_ts = nav.get("SNAPSHOT_TS")
        snapshot_age_sec = None
        if snapshot_ts and hasattr(snapshot_ts, "replace"):
            snapshot_age_sec = int((datetime.now(timezone.utc) - snapshot_ts.replace(tzinfo=timezone.utc)).total_seconds())

        cur.execute(
            """
            select
              DRIFT_ID, RECONCILIATION_TS, NAV_DRIFT_PCT, CASH_DRIFT_EUR, POSITION_DRIFT_COUNT, DRIFT_DETECTED,
              RESOLUTION_TS, RESOLUTION_METHOD, DETAILS
            from MIP.LIVE.DRIFT_LOG
            where PORTFOLIO_ID = %s
              and coalesce(DRIFT_DETECTED, false) = true
            order by RECONCILIATION_TS desc
            limit 20
            """,
            (resolved_portfolio_id,),
        )
        drift_rows = fetch_all(cur)
        unresolved = [r for r in drift_rows if not r.get("RESOLUTION_TS")]
        latest_unresolved = unresolved[0] if unresolved else None

        return {
            "portfolio_id": resolved_portfolio_id,
            "ibkr_account_id": account_id,
            "drift_status": cfg.get("DRIFT_STATUS"),
            "snapshot_freshness_threshold_sec": cfg.get("SNAPSHOT_FRESHNESS_THRESHOLD_SEC"),
            "latest_snapshot": serialize_row(nav) if nav else None,
            "snapshot_age_sec": snapshot_age_sec,
            "unresolved_drift_count": len(unresolved),
            "latest_unresolved_drift": serialize_row(latest_unresolved) if latest_unresolved else None,
        }
    finally:
        conn.close()


@router.get("/trades/actions")
def list_live_trade_actions(
    portfolio_id: int | None = Query(None),
    pending_only: bool = Query(True),
    limit: int = Query(200, ge=1, le=1000),
):
    conn = get_connection()
    try:
        cur = conn.cursor()
        wheres = ["1=1"]
        params = []
        if portfolio_id is not None:
            wheres.append("PORTFOLIO_ID = %s")
            params.append(portfolio_id)
        if pending_only:
            wheres.append(
                "STATUS in ('RESEARCH_IMPORTED','PROPOSED','PM_ACCEPTED','COMPLIANCE_APPROVED','REVALIDATED_PASS','REVALIDATED_FAIL','EXECUTION_REQUESTED')"
            )
        params.append(limit)
        sql = f"""
        select
          ACTION_ID, PROPOSAL_ID, PORTFOLIO_ID, SYMBOL, SIDE, PROPOSED_QTY, PROPOSED_PRICE, ASSET_CLASS,
          STATUS, VALIDITY_WINDOW_END,
          PM_APPROVED_BY, PM_APPROVED_TS,
          COMPLIANCE_STATUS, COMPLIANCE_APPROVED_BY, COMPLIANCE_DECISION_TS, COMPLIANCE_NOTES, COMPLIANCE_REFERENCE_ID,
          REVALIDATION_TS, REVALIDATION_PRICE, PRICE_DEVIATION_PCT, PRICE_GUARD_RESULT,
          REASON_CODES,
          ONE_MIN_BAR_TS, ONE_MIN_BAR_CLOSE, EXECUTION_PRICE_SOURCE,
          CREATED_AT, UPDATED_AT
        from MIP.LIVE.LIVE_ACTIONS
        where {' and '.join(wheres)}
        order by coalesce(COMPLIANCE_DECISION_TS, PM_APPROVED_TS, CREATED_AT) desc
        limit %s
        """
        cur.execute(sql, params)
        rows = fetch_all(cur)
        return {"actions": serialize_rows(rows), "count": len(rows)}
    finally:
        conn.close()


@router.post("/trades/actions/{action_id}/pm-accept")
def pm_accept_live_action(action_id: str, req: PmAcceptRequest):
    conn = get_connection()
    try:
        cur = conn.cursor()
        action = _fetch_live_action(cur, action_id)
        if not action:
            raise HTTPException(status_code=404, detail="Action not found.")
        _assert_transition_allowed(action.get("STATUS"), "PM_ACCEPTED")
        cur.execute(
            """
            update MIP.LIVE.LIVE_ACTIONS
               set STATUS = 'PM_ACCEPTED',
                   PM_APPROVED_BY = %s,
                   PM_APPROVED_TS = current_timestamp(),
                   COMPLIANCE_STATUS = 'PENDING',
                   REASON_CODES = null,
                   UPDATED_AT = current_timestamp()
             where ACTION_ID = %s
            """,
            (req.actor, action_id),
        )
        action_after = _fetch_live_action(cur, action_id)
        _append_learning_ledger_event(
            cur,
            event_name="LIVE_PM_ACCEPT",
            status="PM_ACCEPTED",
            action_before=action,
            action_after=action_after,
            policy_version=LIVE_POLICY_VERSION,
            influence_delta={
                "approval_transition": f"{action.get('STATUS')}->PM_ACCEPTED",
                "actor": req.actor,
            },
        )
        return {"ok": True, "action_id": action_id, "status": "PM_ACCEPTED"}
    finally:
        conn.close()


@router.post("/trades/actions/{action_id}/compliance")
def compliance_decide_live_action(action_id: str, req: ComplianceDecisionRequest):
    status = "COMPLIANCE_APPROVED" if req.decision == "APPROVE" else "COMPLIANCE_DENIED"
    conn = get_connection()
    try:
        cur = conn.cursor()
        action = _fetch_live_action(cur, action_id)
        if not action:
            raise HTTPException(status_code=404, detail="Action not found.")
        _assert_transition_allowed(action.get("STATUS"), status)
        cur.execute(
            """
            update MIP.LIVE.LIVE_ACTIONS
               set STATUS = %s,
                   COMPLIANCE_STATUS = %s,
                   COMPLIANCE_APPROVED_BY = %s,
                   COMPLIANCE_DECISION_TS = current_timestamp(),
                   COMPLIANCE_NOTES = %s,
                   COMPLIANCE_REFERENCE_ID = %s,
                   REASON_CODES = null,
                   UPDATED_AT = current_timestamp()
             where ACTION_ID = %s
            """,
            (status, req.decision, req.actor, req.notes, req.reference_id, action_id),
        )
        action_after = _fetch_live_action(cur, action_id)
        _append_learning_ledger_event(
            cur,
            event_name="LIVE_COMPLIANCE_DECISION",
            status=status,
            action_before=action,
            action_after=action_after,
            policy_version=LIVE_POLICY_VERSION,
            influence_delta={
                "approval_transition": f"{action.get('STATUS')}->{status}",
                "decision": req.decision,
                "actor": req.actor,
            },
            outcome_state={
                "compliance_notes": req.notes,
                "compliance_reference_id": req.reference_id,
            },
        )
        return {"ok": True, "action_id": action_id, "status": status}
    finally:
        conn.close()


@router.post("/trades/actions/{action_id}/revalidate")
def revalidate_live_action(action_id: str):
    conn = get_connection()
    try:
        cur = conn.cursor()
        action = _fetch_live_action(cur, action_id)
        if not action:
            raise HTTPException(status_code=404, detail="Action not found.")
        current_status = action.get("STATUS")
        if current_status == "COMPLIANCE_APPROVED":
            _assert_transition_allowed(current_status, "REVALIDATED_PASS")
        elif current_status == "REVALIDATED_FAIL":
            _assert_transition_allowed(current_status, "REVALIDATED_PASS")
        elif current_status == "REVALIDATED_PASS":
            _assert_transition_allowed(current_status, "REVALIDATED_PASS")
        else:
            raise HTTPException(
                status_code=409,
                detail=f"Revalidation allowed only from COMPLIANCE_APPROVED/REVALIDATED_FAIL/REVALIDATED_PASS (current: {current_status})",
            )
        symbol = action.get("SYMBOL")
        proposed_price = action.get("PROPOSED_PRICE")

        cur.execute(
            """
            select TS, CLOSE
            from MIP.MART.MARKET_BARS
            where SYMBOL = %s
              and INTERVAL_MINUTES = 1
            order by TS desc
            limit 1
            """,
            (symbol,),
        )
        bar = cur.fetchone()
        source = "ONE_MINUTE_BAR"
        if bar:
            ref_ts, ref_price = bar
        else:
            cur.execute(
                """
                select TS, CLOSE
                from MIP.MART.MARKET_BARS
                where SYMBOL = %s
                  and INTERVAL_MINUTES in (15, 60, 1440)
                order by TS desc
                limit 1
                """,
                (symbol,),
            )
            fallback = cur.fetchone()
            if not fallback:
                raise HTTPException(status_code=400, detail="No market bar found for symbol, revalidation blocked.")
            ref_ts, ref_price = fallback
            source = "BAR_FALLBACK"

        deviation = None
        if proposed_price and ref_price:
            try:
                deviation = abs(float(ref_price) - float(proposed_price)) / max(float(proposed_price), 1e-9)
            except Exception:
                deviation = None
        guard_pass = deviation is None or deviation <= 0.02
        status = "REVALIDATED_PASS" if guard_pass else "REVALIDATED_FAIL"
        reason_codes = [] if guard_pass else ["PRICE_GUARD_FAIL"]

        cur.execute(
            """
            update MIP.LIVE.LIVE_ACTIONS
               set REVALIDATION_TS = current_timestamp(),
                   REVALIDATION_PRICE = %s,
                   PRICE_DEVIATION_PCT = %s,
                   PRICE_GUARD_RESULT = %s,
                   ONE_MIN_BAR_TS = %s,
                   ONE_MIN_BAR_CLOSE = %s,
                   EXECUTION_PRICE_SOURCE = %s,
                   STATUS = %s,
                   REASON_CODES = parse_json(%s),
                   UPDATED_AT = current_timestamp()
             where ACTION_ID = %s
            """,
            (
                ref_price,
                deviation,
                "PASS" if guard_pass else "FAIL",
                ref_ts if source == "ONE_MINUTE_BAR" else None,
                ref_price if source == "ONE_MINUTE_BAR" else None,
                source,
                status,
                json.dumps(reason_codes),
                action_id,
            ),
        )
        action_after = _fetch_live_action(cur, action_id)
        _append_learning_ledger_event(
            cur,
            event_name="LIVE_REVALIDATION",
            status=status,
            action_before=action,
            action_after=action_after,
            policy_version=LIVE_POLICY_VERSION,
            influence_delta={
                "price_deviation_pct": float(deviation) if deviation is not None else None,
                "price_guard_result": "PASS" if guard_pass else "FAIL",
                "price_source": source,
            },
        )
        return {
            "ok": True,
            "action_id": action_id,
            "status": status,
            "price_source": source,
            "revalidation_price": float(ref_price) if ref_price is not None else None,
            "price_deviation_pct": float(deviation) if deviation is not None else None,
        }
    finally:
        conn.close()


@router.post("/trades/actions/{action_id}/execute")
def execute_live_action(action_id: str, req: ExecuteLiveActionRequest):
    conn = get_connection()
    try:
        cur = conn.cursor()
        action = _fetch_live_action(cur, action_id)
        if not action:
            raise HTTPException(status_code=404, detail="Action not found.")

        reason_codes: list[str] = []
        now_utc = datetime.now(timezone.utc)
        current_status = (action.get("STATUS") or "").upper()
        compliance_status = (action.get("COMPLIANCE_STATUS") or "").upper()
        if current_status != "REVALIDATED_PASS":
            reason_codes.append("EXECUTION_REQUIRES_REVALIDATED_PASS")
        if compliance_status != "APPROVE":
            reason_codes.append("COMPLIANCE_NOT_APPROVED")

        compliance_decision_ts = action.get("COMPLIANCE_DECISION_TS")
        revalidation_ts = action.get("REVALIDATION_TS")
        if compliance_decision_ts and revalidation_ts:
            cd_ts = compliance_decision_ts.replace(tzinfo=timezone.utc)
            rv_ts = revalidation_ts.replace(tzinfo=timezone.utc)
            if rv_ts <= cd_ts:
                reason_codes.append("REVALIDATION_REQUIRED_AFTER_COMPLIANCE")

        cur.execute(
            """
            select
              IBKR_ACCOUNT_ID, MAX_POSITIONS, MAX_POSITION_PCT, CASH_BUFFER_PCT,
              VALIDITY_WINDOW_SEC, QUOTE_FRESHNESS_THRESHOLD_SEC, SNAPSHOT_FRESHNESS_THRESHOLD_SEC, DRIFT_STATUS, IS_ACTIVE
            from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
            where PORTFOLIO_ID = %s
            """,
            (action.get("PORTFOLIO_ID"),),
        )
        cfg_rows = fetch_all(cur)
        if not cfg_rows:
            raise HTTPException(status_code=400, detail="Live portfolio config missing.")
        cfg = cfg_rows[0]
        if cfg.get("IS_ACTIVE") is False:
            raise HTTPException(status_code=400, detail="Live portfolio config is inactive.")
        drift_status = (cfg.get("DRIFT_STATUS") or "").upper()
        if drift_status and drift_status not in ("OK", "CLEAR", "HEALTHY"):
            reason_codes.append("BROKER_TRUTH_DRIFT_UNRESOLVED")

        unresolved_drift_row = None
        cur.execute(
            """
            select
              DRIFT_ID, RECONCILIATION_TS, NAV_DRIFT_PCT, CASH_DRIFT_EUR, POSITION_DRIFT_COUNT, DETAILS
            from MIP.LIVE.DRIFT_LOG
            where PORTFOLIO_ID = %s
              and coalesce(DRIFT_DETECTED, false) = true
              and RESOLUTION_TS is null
            order by RECONCILIATION_TS desc
            limit 1
            """,
            (action.get("PORTFOLIO_ID"),),
        )
        unresolved_drift_rows = fetch_all(cur)
        if unresolved_drift_rows:
            unresolved_drift_row = unresolved_drift_rows[0]
            reason_codes.append("UNRESOLVED_DRIFT_LOG_PRESENT")

        validity_window_end = action.get("VALIDITY_WINDOW_END")
        if validity_window_end and hasattr(validity_window_end, "replace"):
            vw = validity_window_end.replace(tzinfo=timezone.utc)
            if vw < now_utc:
                reason_codes.append("ACTION_EXPIRED")

        if not revalidation_ts:
            reason_codes.append("MISSING_REVALIDATION")
        else:
            rv_ts = revalidation_ts.replace(tzinfo=timezone.utc)
            rv_age_sec = (now_utc - rv_ts).total_seconds()
            validity_window_sec = int(cfg.get("VALIDITY_WINDOW_SEC") or 14400)
            max_reval_age = min(validity_window_sec, EXECUTION_CLICK_MAX_REVALIDATION_SEC)
            if rv_age_sec > max_reval_age:
                reason_codes.append("EXECUTION_CLICK_REVALIDATION_STALE")

        if (action.get("PRICE_GUARD_RESULT") or "").upper() != "PASS":
            reason_codes.append("PRICE_GUARD_FAIL")
        realism_reason_codes, realism_details = _first_session_realism_checks(cur, action, cfg)
        reason_codes.extend(realism_reason_codes)

        account_id = cfg.get("IBKR_ACCOUNT_ID")
        cur.execute(
            """
            select SNAPSHOT_TS, NET_LIQUIDATION_EUR, TOTAL_CASH_EUR
            from MIP.LIVE.BROKER_SNAPSHOTS
            where SNAPSHOT_TYPE = 'NAV'
              and IBKR_ACCOUNT_ID = %s
            order by SNAPSHOT_TS desc
            limit 1
            """,
            (account_id,),
        )
        nav_rows = fetch_all(cur)
        if not nav_rows:
            reason_codes.append("MISSING_SNAPSHOT")
            nav_eur = None
            cash_eur = None
            snapshot_ts = None
        else:
            nav_row = nav_rows[0]
            snapshot_ts = nav_row.get("SNAPSHOT_TS")
            nav_eur = float(nav_row.get("NET_LIQUIDATION_EUR") or 0.0)
            cash_eur = float(nav_row.get("TOTAL_CASH_EUR") or 0.0)
            if snapshot_ts:
                snap_age_sec = (now_utc - snapshot_ts.replace(tzinfo=timezone.utc)).total_seconds()
                max_snap_age = cfg.get("SNAPSHOT_FRESHNESS_THRESHOLD_SEC") or 300
                if snap_age_sec > max_snap_age:
                    reason_codes.append("SNAPSHOT_STALE")

        cur.execute(
            """
            select count(distinct SYMBOL) as OPEN_POSITIONS
            from MIP.LIVE.BROKER_SNAPSHOTS
            where SNAPSHOT_TYPE = 'POSITION'
              and IBKR_ACCOUNT_ID = %s
              and SNAPSHOT_TS = (
                select max(SNAPSHOT_TS)
                from MIP.LIVE.BROKER_SNAPSHOTS
                where SNAPSHOT_TYPE = 'POSITION'
                  and IBKR_ACCOUNT_ID = %s
              )
              and coalesce(POSITION_QTY, 0) <> 0
            """,
            (account_id, account_id),
        )
        pos_rows = fetch_all(cur)
        open_positions = int((pos_rows[0] or {}).get("OPEN_POSITIONS") or 0)

        max_positions = cfg.get("MAX_POSITIONS")
        if max_positions is not None and open_positions >= int(max_positions):
            reason_codes.append("MAX_POSITIONS_EXCEEDED")

        proposed_qty = action.get("PROPOSED_QTY")
        px = action.get("REVALIDATION_PRICE") or action.get("PROPOSED_PRICE")
        if proposed_qty is None or px is None:
            reason_codes.append("MISSING_NOTIONAL_INPUT")
            est_notional = None
        else:
            est_notional = abs(float(proposed_qty) * float(px))

        max_position_pct = cfg.get("MAX_POSITION_PCT")
        if est_notional is not None and nav_eur and max_position_pct is not None and nav_eur > 0:
            if (est_notional / nav_eur) > float(max_position_pct):
                reason_codes.append("MAX_POSITION_PCT_EXCEEDED")

        if est_notional is not None and nav_eur and (action.get("SIDE") or "").upper() == "BUY":
            cash_buffer_pct = float(cfg.get("CASH_BUFFER_PCT") or 0.0)
            min_cash_after = nav_eur * cash_buffer_pct
            if (cash_eur - est_notional) < min_cash_after:
                reason_codes.append("CASH_BUFFER_BREACH")

        cur.execute(
            """
            select ORDER_ID
            from MIP.LIVE.LIVE_ORDERS
            where ACTION_ID = %s
              and STATUS in ('SUBMITTED','ACKNOWLEDGED','PARTIAL_FILL','FILLED')
            limit 1
            """,
            (action_id,),
        )
        if cur.fetchone():
            reason_codes.append("DUPLICATE_EXECUTION_BLOCKED")

        if reason_codes:
            final_reason_codes = sorted(set(reason_codes))
            _write_reason_codes(cur, action_id, final_reason_codes)
            _append_learning_ledger_event(
                cur,
                event_name="LIVE_EXECUTION_BLOCKED",
                status="BLOCKED",
                action_before=action,
                action_after=_fetch_live_action(cur, action_id),
                policy_version=LIVE_POLICY_VERSION,
                influence_delta={
                    "safety_gates_passed": False,
                    "reason_codes": final_reason_codes,
                },
                outcome_state={
                    "validator": "FIRST_SESSION_REALISM",
                    "realism_details": realism_details,
                    "actor": req.actor,
                    "required_status": "REVALIDATED_PASS",
                    "required_compliance": "APPROVE",
                    "drift_status": drift_status,
                    "latest_unresolved_drift": serialize_row(unresolved_drift_row) if unresolved_drift_row else None,
                },
            )
            raise HTTPException(
                status_code=409,
                detail={"message": "Execution blocked by safety gates.", "reason_codes": final_reason_codes},
            )

        order_id = str(uuid.uuid4())
        proposal_or_action = action.get("PROPOSAL_ID") if action.get("PROPOSAL_ID") is not None else action_id
        idempotency_key = f"{action.get('PORTFOLIO_ID')}:{proposal_or_action}:{req.attempt_n}"
        cur.execute(
            """
            select ORDER_ID, STATUS
            from MIP.LIVE.LIVE_ORDERS
            where IDEMPOTENCY_KEY = %s
            limit 1
            """,
            (idempotency_key,),
        )
        existing_order_rows = fetch_all(cur)
        if existing_order_rows:
            existing_order = existing_order_rows[0]
            return {
                "ok": True,
                "action_id": action_id,
                "status": "EXECUTION_REQUESTED",
                "order_id": existing_order.get("ORDER_ID"),
                "idempotency_key": idempotency_key,
                "mode": "PAPER_PLACEHOLDER",
                "idempotent_replay": True,
            }

        cur.execute(
            """
            insert into MIP.LIVE.LIVE_ORDERS (
              ORDER_ID, ACTION_ID, PORTFOLIO_ID, IBKR_ACCOUNT_ID, IDEMPOTENCY_KEY, STATUS,
              SYMBOL, SIDE, ORDER_TYPE, QTY_ORDERED, LIMIT_PRICE,
              SUBMITTED_AT, ACKNOWLEDGED_AT, LAST_UPDATED_AT, CREATED_AT
            )
            values (
              %s, %s, %s, %s, %s, 'ACKNOWLEDGED',
              %s, %s, 'MKT_PAPER', %s, %s,
              current_timestamp(), current_timestamp(), current_timestamp(), current_timestamp()
            )
            """,
            (
                order_id,
                action_id,
                action.get("PORTFOLIO_ID"),
                account_id,
                idempotency_key,
                action.get("SYMBOL"),
                action.get("SIDE"),
                proposed_qty,
                action.get("REVALIDATION_PRICE") or action.get("PROPOSED_PRICE"),
            ),
        )

        cur.execute(
            """
            insert into MIP.LIVE.BROKER_EVENT_LEDGER (
              EVENT_ID, EVENT_TS, EVENT_TYPE, PORTFOLIO_ID, PROPOSAL_ID, ACTION_ID,
              IDEMPOTENCY_KEY, BROKER_ORDER_ID, SYMBOL, SIDE, QTY, PRICE, PAYLOAD
            )
            values (
              %s, current_timestamp(), 'EXECUTION_REQUESTED', %s, %s, %s,
              %s, %s, %s, %s, %s, %s, parse_json(%s)
            )
            """,
            (
                str(uuid.uuid4()),
                action.get("PORTFOLIO_ID"),
                action.get("PROPOSAL_ID"),
                action_id,
                idempotency_key,
                order_id,
                action.get("SYMBOL"),
                action.get("SIDE"),
                proposed_qty,
                action.get("REVALIDATION_PRICE") or action.get("PROPOSED_PRICE"),
                json.dumps({"actor": req.actor, "mode": "PAPER_PLACEHOLDER"}),
            ),
        )

        cur.execute(
            """
            update MIP.LIVE.LIVE_ACTIONS
               set STATUS = 'EXECUTION_REQUESTED',
                   REASON_CODES = parse_json(%s),
                   UPDATED_AT = current_timestamp()
             where ACTION_ID = %s
            """,
            (json.dumps(["EXECUTION_REQUESTED"]), action_id),
        )
        action_after = _fetch_live_action(cur, action_id)
        _append_learning_ledger_event(
            cur,
            event_name="LIVE_EXECUTION_REQUESTED",
            status="EXECUTION_REQUESTED",
            action_before=action,
            action_after=action_after,
            policy_version=LIVE_POLICY_VERSION,
            influence_delta={
                "safety_gates_passed": True,
                "idempotency_key": idempotency_key,
                "actor": req.actor,
            },
            outcome_state={
                "order_id": order_id,
                "mode": "PAPER_PLACEHOLDER",
            },
        )

        return {
            "ok": True,
            "action_id": action_id,
            "status": "EXECUTION_REQUESTED",
            "order_id": order_id,
            "idempotency_key": idempotency_key,
            "mode": "PAPER_PLACEHOLDER",
        }
    finally:
        conn.close()


@router.get("/trades/orders")
def list_live_orders(
    portfolio_id: int | None = Query(None),
    action_id: str | None = Query(None),
    limit: int = Query(200, ge=1, le=1000),
):
    conn = get_connection()
    try:
        cur = conn.cursor()
        wheres = ["1=1"]
        params: list = []
        if portfolio_id is not None:
            wheres.append("PORTFOLIO_ID = %s")
            params.append(portfolio_id)
        if action_id:
            wheres.append("ACTION_ID = %s")
            params.append(action_id)
        params.append(limit)
        cur.execute(
            f"""
            select
              ORDER_ID, ACTION_ID, PORTFOLIO_ID, IBKR_ACCOUNT_ID, IDEMPOTENCY_KEY, BROKER_ORDER_ID,
              STATUS, SYMBOL, SIDE, ORDER_TYPE, QTY_ORDERED, LIMIT_PRICE,
              QTY_FILLED, AVG_FILL_PRICE,
              SUBMITTED_AT, ACKNOWLEDGED_AT, FILLED_AT, LAST_UPDATED_AT, CREATED_AT
            from MIP.LIVE.LIVE_ORDERS
            where {' and '.join(wheres)}
            order by LAST_UPDATED_AT desc, CREATED_AT desc
            limit %s
            """,
            tuple(params),
        )
        rows = fetch_all(cur)
        return {"orders": serialize_rows(rows), "count": len(rows)}
    finally:
        conn.close()


@router.post("/trades/orders/{order_id}/status")
def update_live_order_status(order_id: str, req: UpdateLiveOrderStatusRequest):
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            select
              ORDER_ID, ACTION_ID, PORTFOLIO_ID, IBKR_ACCOUNT_ID, IDEMPOTENCY_KEY, BROKER_ORDER_ID,
              STATUS, SYMBOL, SIDE, QTY_ORDERED, QTY_FILLED, AVG_FILL_PRICE
            from MIP.LIVE.LIVE_ORDERS
            where ORDER_ID = %s
            """,
            (order_id,),
        )
        order_rows = fetch_all(cur)
        if not order_rows:
            raise HTTPException(status_code=404, detail="Order not found.")
        order = order_rows[0]
        target_status = req.status.upper()
        current_status = (order.get("STATUS") or "").upper()
        if current_status == target_status:
            return {"ok": True, "order_id": order_id, "status": target_status, "idempotent_replay": True}

        qty_ordered = float(order.get("QTY_ORDERED") or 0.0)
        existing_qty_filled = float(order.get("QTY_FILLED") or 0.0)
        new_qty_filled = req.qty_filled if req.qty_filled is not None else existing_qty_filled
        if target_status == "PARTIAL_FILL":
            if new_qty_filled <= 0 or (qty_ordered > 0 and new_qty_filled >= qty_ordered):
                raise HTTPException(status_code=400, detail="PARTIAL_FILL requires qty_filled between 0 and qty_ordered.")
        if target_status == "FILLED":
            new_qty_filled = qty_ordered if qty_ordered > 0 else (req.qty_filled or existing_qty_filled)

        cur.execute(
            """
            update MIP.LIVE.LIVE_ORDERS
               set STATUS = %s,
                   BROKER_ORDER_ID = coalesce(%s, BROKER_ORDER_ID),
                   QTY_FILLED = %s,
                   AVG_FILL_PRICE = coalesce(%s, AVG_FILL_PRICE),
                   FILLED_AT = case when %s = 'FILLED' then current_timestamp() else FILLED_AT end,
                   LAST_UPDATED_AT = current_timestamp()
             where ORDER_ID = %s
            """,
            (
                target_status,
                req.broker_order_id,
                new_qty_filled,
                req.avg_fill_price,
                target_status,
                order_id,
            ),
        )

        action_id = order.get("ACTION_ID")
        if action_id:
            action_status = None
            action_reason_codes: list[str] | None = None
            if target_status == "FILLED":
                action_status = "EXECUTED"
                action_reason_codes = ["ORDER_FILLED"]
            elif target_status == "PARTIAL_FILL":
                action_status = "EXECUTION_PARTIAL"
                action_reason_codes = ["ORDER_PARTIAL_FILL"]
            elif target_status == "CANCELED":
                action_status = "EXECUTION_CANCELED"
                action_reason_codes = ["ORDER_CANCELED"]
            elif target_status == "REJECTED":
                action_status = "EXECUTION_REJECTED"
                action_reason_codes = ["ORDER_REJECTED"]

            if action_status:
                cur.execute(
                    """
                    update MIP.LIVE.LIVE_ACTIONS
                       set STATUS = %s,
                           REASON_CODES = parse_json(%s),
                           UPDATED_AT = current_timestamp()
                     where ACTION_ID = %s
                    """,
                    (action_status, json.dumps(action_reason_codes), action_id),
                )

        cur.execute(
            """
            insert into MIP.LIVE.BROKER_EVENT_LEDGER (
              EVENT_ID, EVENT_TS, EVENT_TYPE, PORTFOLIO_ID, ACTION_ID,
              IDEMPOTENCY_KEY, BROKER_ORDER_ID, SYMBOL, SIDE, QTY, PRICE, PAYLOAD
            )
            values (
              %s, current_timestamp(), %s, %s, %s,
              %s, %s, %s, %s, %s, %s, parse_json(%s)
            )
            """,
            (
                str(uuid.uuid4()),
                f"ORDER_{target_status}",
                order.get("PORTFOLIO_ID"),
                action_id,
                order.get("IDEMPOTENCY_KEY"),
                req.broker_order_id or order.get("BROKER_ORDER_ID"),
                order.get("SYMBOL"),
                order.get("SIDE"),
                new_qty_filled if target_status in ("PARTIAL_FILL", "FILLED") else (order.get("QTY_ORDERED") or 0.0),
                req.avg_fill_price if req.avg_fill_price is not None else order.get("AVG_FILL_PRICE"),
                json.dumps({"actor": req.actor, "notes": req.notes}),
            ),
        )

        if action_id:
            action_after = _fetch_live_action(cur, action_id)
            _append_learning_ledger_event(
                cur,
                event_name="LIVE_ORDER_STATUS_UPDATE",
                status=target_status,
                action_before=None,
                action_after=action_after,
                policy_version=LIVE_POLICY_VERSION,
                influence_delta={
                    "order_id": order_id,
                    "from_status": current_status,
                    "to_status": target_status,
                    "qty_filled": new_qty_filled,
                },
                outcome_state={
                    "actor": req.actor,
                    "broker_order_id": req.broker_order_id or order.get("BROKER_ORDER_ID"),
                    "notes": req.notes,
                },
            )

        return {
            "ok": True,
            "order_id": order_id,
            "status": target_status,
            "qty_filled": new_qty_filled,
            "avg_fill_price": req.avg_fill_price if req.avg_fill_price is not None else order.get("AVG_FILL_PRICE"),
        }
    finally:
        conn.close()


@router.get("/portfolio-config")
def list_live_portfolio_configs():
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            select
              PORTFOLIO_ID, SIM_PORTFOLIO_ID, IBKR_ACCOUNT_ID, ADAPTER_MODE, BASE_CURRENCY,
              MAX_POSITIONS, MAX_POSITION_PCT, CASH_BUFFER_PCT, MAX_SLIPPAGE_PCT,
              VALIDITY_WINDOW_SEC, QUOTE_FRESHNESS_THRESHOLD_SEC, SNAPSHOT_FRESHNESS_THRESHOLD_SEC,
              DRAWDOWN_STOP_PCT, BUST_PCT, COOLDOWN_BARS,
              DRIFT_STATUS, CONFIG_VERSION, IS_ACTIVE, CREATED_AT, UPDATED_AT
            from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
            order by PORTFOLIO_ID
            """
        )
        rows = fetch_all(cur)
        return {"configs": serialize_rows(rows), "count": len(rows)}
    finally:
        conn.close()


@router.get("/portfolio-config/{portfolio_id}")
def get_live_portfolio_config(portfolio_id: int):
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            select
              PORTFOLIO_ID, SIM_PORTFOLIO_ID, IBKR_ACCOUNT_ID, ADAPTER_MODE, BASE_CURRENCY,
              MAX_POSITIONS, MAX_POSITION_PCT, CASH_BUFFER_PCT, MAX_SLIPPAGE_PCT,
              VALIDITY_WINDOW_SEC, QUOTE_FRESHNESS_THRESHOLD_SEC, SNAPSHOT_FRESHNESS_THRESHOLD_SEC,
              DRAWDOWN_STOP_PCT, BUST_PCT, COOLDOWN_BARS,
              DRIFT_STATUS, CONFIG_VERSION, IS_ACTIVE, CREATED_AT, UPDATED_AT
            from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
            where PORTFOLIO_ID = %s
            """,
            (portfolio_id,),
        )
        rows = fetch_all(cur)
        if not rows:
            raise HTTPException(status_code=404, detail="Live portfolio config not found.")
        return {"config": serialize_row(rows[0])}
    finally:
        conn.close()


@router.put("/portfolio-config/{portfolio_id}")
def upsert_live_portfolio_config(portfolio_id: int, req: LivePortfolioConfigUpsertRequest):
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "select PORTFOLIO_ID, IBKR_ACCOUNT_ID, CONFIG_VERSION from MIP.LIVE.LIVE_PORTFOLIO_CONFIG where PORTFOLIO_ID = %s",
            (portfolio_id,),
        )
        existing = cur.fetchone()

        if existing:
            cur.execute(
                """
                update MIP.LIVE.LIVE_PORTFOLIO_CONFIG
                   set SIM_PORTFOLIO_ID = coalesce(%s, SIM_PORTFOLIO_ID),
                       IBKR_ACCOUNT_ID = coalesce(%s, IBKR_ACCOUNT_ID),
                       ADAPTER_MODE = coalesce(%s, ADAPTER_MODE),
                       BASE_CURRENCY = coalesce(%s, BASE_CURRENCY),
                       MAX_POSITIONS = coalesce(%s, MAX_POSITIONS),
                       MAX_POSITION_PCT = coalesce(%s, MAX_POSITION_PCT),
                       CASH_BUFFER_PCT = coalesce(%s, CASH_BUFFER_PCT),
                       MAX_SLIPPAGE_PCT = coalesce(%s, MAX_SLIPPAGE_PCT),
                       VALIDITY_WINDOW_SEC = coalesce(%s, VALIDITY_WINDOW_SEC),
                       QUOTE_FRESHNESS_THRESHOLD_SEC = coalesce(%s, QUOTE_FRESHNESS_THRESHOLD_SEC),
                       SNAPSHOT_FRESHNESS_THRESHOLD_SEC = coalesce(%s, SNAPSHOT_FRESHNESS_THRESHOLD_SEC),
                       DRAWDOWN_STOP_PCT = coalesce(%s, DRAWDOWN_STOP_PCT),
                       BUST_PCT = coalesce(%s, BUST_PCT),
                       COOLDOWN_BARS = coalesce(%s, COOLDOWN_BARS),
                       IS_ACTIVE = coalesce(%s, IS_ACTIVE),
                       CONFIG_VERSION = coalesce(CONFIG_VERSION, 1) + 1,
                       UPDATED_AT = current_timestamp()
                 where PORTFOLIO_ID = %s
                """,
                (
                    req.sim_portfolio_id,
                    req.ibkr_account_id,
                    req.adapter_mode,
                    req.base_currency.upper() if req.base_currency else None,
                    req.max_positions,
                    req.max_position_pct,
                    req.cash_buffer_pct,
                    req.max_slippage_pct,
                    req.validity_window_sec,
                    req.quote_freshness_threshold_sec,
                    req.snapshot_freshness_threshold_sec,
                    req.drawdown_stop_pct,
                    req.bust_pct,
                    req.cooldown_bars,
                    req.is_active,
                    portfolio_id,
                ),
            )
        else:
            if not req.ibkr_account_id:
                raise HTTPException(status_code=400, detail="ibkr_account_id is required when creating config.")
            cur.execute(
                """
                insert into MIP.LIVE.LIVE_PORTFOLIO_CONFIG (
                  PORTFOLIO_ID, SIM_PORTFOLIO_ID, IBKR_ACCOUNT_ID, ADAPTER_MODE, BASE_CURRENCY,
                  MAX_POSITIONS, MAX_POSITION_PCT, CASH_BUFFER_PCT, MAX_SLIPPAGE_PCT,
                  VALIDITY_WINDOW_SEC, QUOTE_FRESHNESS_THRESHOLD_SEC, SNAPSHOT_FRESHNESS_THRESHOLD_SEC,
                  DRAWDOWN_STOP_PCT, BUST_PCT, COOLDOWN_BARS, IS_ACTIVE, CONFIG_VERSION,
                  CREATED_AT, UPDATED_AT
                )
                values (
                  %s, %s, %s, coalesce(%s, 'PAPER'), coalesce(%s, 'EUR'),
                  %s, %s, %s, %s,
                  coalesce(%s, 14400), coalesce(%s, 60), coalesce(%s, 300),
                  %s, %s, coalesce(%s, 3), coalesce(%s, true), 1,
                  current_timestamp(), current_timestamp()
                )
                """,
                (
                    portfolio_id,
                    req.sim_portfolio_id,
                    req.ibkr_account_id,
                    req.adapter_mode,
                    req.base_currency.upper() if req.base_currency else None,
                    req.max_positions,
                    req.max_position_pct,
                    req.cash_buffer_pct,
                    req.max_slippage_pct,
                    req.validity_window_sec,
                    req.quote_freshness_threshold_sec,
                    req.snapshot_freshness_threshold_sec,
                    req.drawdown_stop_pct,
                    req.bust_pct,
                    req.cooldown_bars,
                    req.is_active,
                ),
            )

        cur.execute(
            """
            select
              PORTFOLIO_ID, SIM_PORTFOLIO_ID, IBKR_ACCOUNT_ID, ADAPTER_MODE, BASE_CURRENCY,
              MAX_POSITIONS, MAX_POSITION_PCT, CASH_BUFFER_PCT, MAX_SLIPPAGE_PCT,
              VALIDITY_WINDOW_SEC, QUOTE_FRESHNESS_THRESHOLD_SEC, SNAPSHOT_FRESHNESS_THRESHOLD_SEC,
              DRAWDOWN_STOP_PCT, BUST_PCT, COOLDOWN_BARS,
              DRIFT_STATUS, CONFIG_VERSION, IS_ACTIVE, CREATED_AT, UPDATED_AT
            from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
            where PORTFOLIO_ID = %s
            """,
            (portfolio_id,),
        )
        rows = fetch_all(cur)
        return {"ok": True, "config": serialize_row(rows[0]) if rows else None}
    finally:
        conn.close()


@router.post("/trades/actions/import-proposals")
def import_live_actions_from_proposals(req: ImportLiveActionsFromProposalsRequest):
    conn = get_connection()
    try:
        cur = conn.cursor()

        cur.execute(
            """
            select SIM_PORTFOLIO_ID, coalesce(VALIDITY_WINDOW_SEC, 14400) as VALIDITY_WINDOW_SEC
            from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
            where PORTFOLIO_ID = %s
              and coalesce(IS_ACTIVE, true) = true
            """,
            (req.live_portfolio_id,),
        )
        cfg = cur.fetchone()
        if not cfg:
            raise HTTPException(
                status_code=400,
                detail="Live portfolio config not found or inactive.",
            )
        sim_portfolio_id, validity_window_sec = cfg
        if sim_portfolio_id is None:
            raise HTTPException(
                status_code=400,
                detail="SIM_PORTFOLIO_ID is not configured for this live portfolio.",
            )

        wheres = [
            "PORTFOLIO_ID = %s",
            "STATUS in ('PROPOSED', 'APPROVED')",
            "SYMBOL is not null",
            "SIDE in ('BUY', 'SELL')",
        ]
        params = [sim_portfolio_id]
        if req.run_id:
            wheres.append("RUN_ID_VARCHAR = %s")
            params.append(req.run_id)
        params.append(req.limit)

        cur.execute(
            f"""
            select
              PROPOSAL_ID, RUN_ID_VARCHAR, SYMBOL, MARKET_TYPE, SIDE, TARGET_WEIGHT,
              STATUS, SIGNAL_PATTERN_ID, RECOMMENDATION_ID, PROPOSED_AT
            from MIP.AGENT_OUT.ORDER_PROPOSALS
            where {' and '.join(wheres)}
            order by PROPOSED_AT desc
            limit %s
            """,
            tuple(params),
        )
        proposals = fetch_all(cur)

        imported = 0
        skipped_existing = 0
        skipped_invalid = 0
        imported_action_ids: list[str] = []

        for p in proposals:
            proposal_id = p.get("PROPOSAL_ID")
            if proposal_id is None:
                skipped_invalid += 1
                continue

            cur.execute(
                """
                select ACTION_ID
                from MIP.LIVE.LIVE_ACTIONS
                where PORTFOLIO_ID = %s
                  and PROPOSAL_ID = %s
                limit 1
                """,
                (req.live_portfolio_id, proposal_id),
            )
            if cur.fetchone():
                skipped_existing += 1
                continue

            action_id = str(uuid.uuid4())
            snapshot_payload = json.dumps(
                {
                    "source": "ORDER_PROPOSALS",
                    "sim_portfolio_id": sim_portfolio_id,
                    "run_id": p.get("RUN_ID_VARCHAR"),
                    "proposal_status": p.get("STATUS"),
                    "signal_pattern_id": p.get("SIGNAL_PATTERN_ID"),
                    "recommendation_id": p.get("RECOMMENDATION_ID"),
                    "target_weight": p.get("TARGET_WEIGHT"),
                    "proposed_at": str(p.get("PROPOSED_AT")) if p.get("PROPOSED_AT") is not None else None,
                }
            )

            import_reason_codes = [
                "RESEARCH_IMPORTED",
                "NON_EXECUTABLE_UNTIL_PM_COMPLIANCE_REVALIDATION",
            ]
            cur.execute(
                """
                insert into MIP.LIVE.LIVE_ACTIONS (
                  ACTION_ID, PROPOSAL_ID, PORTFOLIO_ID, SYMBOL, SIDE, PROPOSED_QTY, ASSET_CLASS,
                  STATUS, VALIDITY_WINDOW_END, COMPLIANCE_STATUS, PARAM_SNAPSHOT, REASON_CODES,
                  CREATED_AT, UPDATED_AT
                )
                values (
                  %s, %s, %s, %s, %s, %s, %s,
                  'RESEARCH_IMPORTED', dateadd(second, %s, current_timestamp()), 'PENDING', parse_json(%s), parse_json(%s),
                  current_timestamp(), current_timestamp()
                )
                """,
                (
                    action_id,
                    proposal_id,
                    req.live_portfolio_id,
                    (p.get("SYMBOL") or "").upper(),
                    (p.get("SIDE") or "").upper(),
                    None,
                    p.get("MARKET_TYPE"),
                    int(validity_window_sec) if validity_window_sec is not None else 14400,
                    snapshot_payload,
                    json.dumps(import_reason_codes),
                ),
            )
            _append_learning_ledger_event(
                cur,
                event_name="LIVE_RESEARCH_IMPORT",
                status="RESEARCH_IMPORTED",
                action_before=None,
                action_after={
                    "ACTION_ID": action_id,
                    "PROPOSAL_ID": proposal_id,
                    "PORTFOLIO_ID": req.live_portfolio_id,
                    "SYMBOL": (p.get("SYMBOL") or "").upper(),
                    "SIDE": (p.get("SIDE") or "").upper(),
                    "ASSET_CLASS": p.get("MARKET_TYPE"),
                    "RUN_ID_VARCHAR": p.get("RUN_ID_VARCHAR"),
                },
                influence_delta={
                    "default_executable": False,
                    "required_sequence": [
                        "PM_ACCEPTED",
                        "COMPLIANCE_APPROVED",
                        "REVALIDATED_PASS",
                        "EXECUTION_REQUESTED",
                    ],
                    "proposal_status_source": p.get("STATUS"),
                },
                policy_version=LIVE_POLICY_VERSION,
                outcome_state={
                    "import_source": "ORDER_PROPOSALS",
                    "reason_codes": import_reason_codes,
                },
            )
            imported += 1
            imported_action_ids.append(action_id)

        return {
            "ok": True,
            "live_portfolio_id": req.live_portfolio_id,
            "sim_portfolio_id": sim_portfolio_id,
            "run_id_filter": req.run_id,
            "candidate_count": len(proposals),
            "imported_count": imported,
            "skipped_existing_count": skipped_existing,
            "skipped_invalid_count": skipped_invalid,
            "imported_action_ids": imported_action_ids[:50],
        }
    finally:
        conn.close()
