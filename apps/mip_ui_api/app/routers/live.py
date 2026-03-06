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


class CreateLiveActionRequest(BaseModel):
    portfolio_id: int
    symbol: str
    side: str = Field(pattern="^(BUY|SELL)$")
    proposed_qty: float
    proposed_price: float | None = None
    asset_class: str | None = None
    proposal_id: int | None = None
    validity_window_hours: int = 4


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
                "STATUS in ('PROPOSED','PM_ACCEPTED','COMPLIANCE_APPROVED','REVALIDATED_PASS','REVALIDATED_FAIL')"
            )
        params.append(limit)
        sql = f"""
        select
          ACTION_ID, PROPOSAL_ID, PORTFOLIO_ID, SYMBOL, SIDE, PROPOSED_QTY, PROPOSED_PRICE, ASSET_CLASS,
          STATUS, VALIDITY_WINDOW_END,
          PM_APPROVED_BY, PM_APPROVED_TS,
          COMPLIANCE_STATUS, COMPLIANCE_APPROVED_BY, COMPLIANCE_DECISION_TS, COMPLIANCE_NOTES, COMPLIANCE_REFERENCE_ID,
          REVALIDATION_TS, REVALIDATION_PRICE, PRICE_DEVIATION_PCT, PRICE_GUARD_RESULT,
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


@router.post("/trades/actions")
def create_live_trade_action(req: CreateLiveActionRequest):
    action_id = str(__import__("uuid").uuid4())
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            insert into MIP.LIVE.LIVE_ACTIONS (
              ACTION_ID, PROPOSAL_ID, PORTFOLIO_ID, SYMBOL, SIDE, PROPOSED_QTY, PROPOSED_PRICE, ASSET_CLASS,
              STATUS, VALIDITY_WINDOW_END, COMPLIANCE_STATUS, CREATED_AT, UPDATED_AT
            )
            values (
              %s, %s, %s, %s, %s, %s, %s, %s,
              'PROPOSED', dateadd(hour, %s, current_timestamp()), 'PENDING', current_timestamp(), current_timestamp()
            )
            """,
            (
                action_id,
                req.proposal_id,
                req.portfolio_id,
                req.symbol.upper(),
                req.side.upper(),
                req.proposed_qty,
                req.proposed_price,
                req.asset_class,
                req.validity_window_hours,
            ),
        )
        return {"ok": True, "action_id": action_id}
    finally:
        conn.close()


@router.post("/trades/actions/{action_id}/pm-accept")
def pm_accept_live_action(action_id: str, req: PmAcceptRequest):
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            update MIP.LIVE.LIVE_ACTIONS
               set STATUS = 'PM_ACCEPTED',
                   PM_APPROVED_BY = %s,
                   PM_APPROVED_TS = current_timestamp(),
                   COMPLIANCE_STATUS = 'PENDING',
                   UPDATED_AT = current_timestamp()
             where ACTION_ID = %s
            """,
            (req.actor, action_id),
        )
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Action not found.")
        return {"ok": True, "action_id": action_id, "status": "PM_ACCEPTED"}
    finally:
        conn.close()


@router.post("/trades/actions/{action_id}/compliance")
def compliance_decide_live_action(action_id: str, req: ComplianceDecisionRequest):
    status = "COMPLIANCE_APPROVED" if req.decision == "APPROVE" else "COMPLIANCE_DENIED"
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            update MIP.LIVE.LIVE_ACTIONS
               set STATUS = %s,
                   COMPLIANCE_STATUS = %s,
                   COMPLIANCE_APPROVED_BY = %s,
                   COMPLIANCE_DECISION_TS = current_timestamp(),
                   COMPLIANCE_NOTES = %s,
                   COMPLIANCE_REFERENCE_ID = %s,
                   UPDATED_AT = current_timestamp()
             where ACTION_ID = %s
            """,
            (status, req.decision, req.actor, req.notes, req.reference_id, action_id),
        )
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Action not found.")
        return {"ok": True, "action_id": action_id, "status": status}
    finally:
        conn.close()


@router.post("/trades/actions/{action_id}/revalidate")
def revalidate_live_action(action_id: str):
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            select ACTION_ID, PORTFOLIO_ID, SYMBOL, PROPOSED_PRICE
            from MIP.LIVE.LIVE_ACTIONS
            where ACTION_ID = %s
            """,
            (action_id,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Action not found.")
        _, portfolio_id, symbol, proposed_price = row

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
                action_id,
            ),
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

            cur.execute(
                """
                insert into MIP.LIVE.LIVE_ACTIONS (
                  ACTION_ID, PROPOSAL_ID, PORTFOLIO_ID, SYMBOL, SIDE, PROPOSED_QTY, ASSET_CLASS,
                  STATUS, VALIDITY_WINDOW_END, COMPLIANCE_STATUS, PARAM_SNAPSHOT,
                  CREATED_AT, UPDATED_AT
                )
                values (
                  %s, %s, %s, %s, %s, %s, %s,
                  'PROPOSED', dateadd(second, %s, current_timestamp()), 'PENDING', parse_json(%s),
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
                ),
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
