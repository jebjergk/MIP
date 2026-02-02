from fastapi import APIRouter, HTTPException

from app.db import get_connection, fetch_all, serialize_rows, serialize_row

router = APIRouter(prefix="/portfolios", tags=["portfolios"])


@router.get("")
def list_portfolios():
    """Portfolio list: PORTFOLIO_ID, NAME, STATUS, LAST_SIMULATED_AT, etc."""
    sql = """
    select
        PORTFOLIO_ID,
        NAME,
        STATUS,
        LAST_SIMULATED_AT,
        PROFILE_ID,
        STARTING_CASH,
        FINAL_EQUITY,
        TOTAL_RETURN
    from MIP.APP.PORTFOLIO
    order by PORTFOLIO_ID
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = fetch_all(cur)
        return serialize_rows(rows)
    finally:
        conn.close()


@router.get("/{portfolio_id}")
def get_portfolio(portfolio_id: int):
    """Portfolio header: all columns from MIP.APP.PORTFOLIO."""
    sql = """
    select
        PORTFOLIO_ID,
        PROFILE_ID,
        NAME,
        BASE_CURRENCY,
        STARTING_CASH,
        LAST_SIMULATION_RUN_ID,
        LAST_SIMULATED_AT,
        FINAL_EQUITY,
        TOTAL_RETURN,
        MAX_DRAWDOWN,
        WIN_DAYS,
        LOSS_DAYS,
        STATUS,
        BUST_AT,
        NOTES,
        CREATED_AT,
        UPDATED_AT
    from MIP.APP.PORTFOLIO
    where PORTFOLIO_ID = %s
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, (portfolio_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Portfolio not found")
        columns = [d[0] for d in cur.description]
        return serialize_row(dict(zip(columns, row)))
    finally:
        conn.close()


def _first(d: list) -> dict | None:
    """First element of list or None."""
    return d[0] if d else None


@router.get("/{portfolio_id}/snapshot")
def get_portfolio_snapshot(portfolio_id: int, run_id: str | None = None):
    """Combined read: positions, trades, daily, KPIs, risk + operator-clarity cards (optional run_id filter)."""
    conn = get_connection()
    try:
        cur = conn.cursor()

        # Positions
        cur.execute(
            """
            select * from MIP.APP.PORTFOLIO_POSITIONS
            where PORTFOLIO_ID = %s and (%s is null or RUN_ID = %s)
            order by ENTRY_TS desc
            """,
            (portfolio_id, run_id, run_id),
        )
        positions = serialize_rows(fetch_all(cur))

        # Trades
        cur.execute(
            """
            select * from MIP.APP.PORTFOLIO_TRADES
            where PORTFOLIO_ID = %s and (%s is null or RUN_ID = %s)
            order by TRADE_TS desc
            """,
            (portfolio_id, run_id, run_id),
        )
        trades = serialize_rows(fetch_all(cur))

        # Daily
        cur.execute(
            """
            select * from MIP.APP.PORTFOLIO_DAILY
            where PORTFOLIO_ID = %s and (%s is null or RUN_ID = %s)
            order by TS desc
            """,
            (portfolio_id, run_id, run_id),
        )
        daily = serialize_rows(fetch_all(cur))

        # KPIs
        cur.execute(
            """
            select * from MIP.MART.V_PORTFOLIO_RUN_KPIS
            where PORTFOLIO_ID = %s and (%s is null or RUN_ID = %s)
            order by TO_TS desc
            """,
            (portfolio_id, run_id, run_id),
        )
        kpis = serialize_rows(fetch_all(cur))

        # Risk (gate)
        cur.execute(
            "select * from MIP.MART.V_PORTFOLIO_RISK_GATE where PORTFOLIO_ID = %s",
            (portfolio_id,),
        )
        risk_gate_rows = serialize_rows(fetch_all(cur))

        # Risk (state)
        cur.execute(
            "select * from MIP.MART.V_PORTFOLIO_RISK_STATE where PORTFOLIO_ID = %s",
            (portfolio_id,),
        )
        risk_state = serialize_rows(fetch_all(cur))

        # --- Operator-clarity cards ---
        latest_daily = _first(daily)
        rg = _first(risk_gate_rows)

        cash_and_exposure = None
        if latest_daily:
            cash = latest_daily.get("CASH") or latest_daily.get("cash")
            equity_value = latest_daily.get("EQUITY_VALUE") or latest_daily.get("equity_value")
            total_equity = latest_daily.get("TOTAL_EQUITY") or latest_daily.get("total_equity")
            cash_and_exposure = {
                "cash": cash,
                "exposure": equity_value,
                "total_equity": total_equity,
                "as_of_ts": latest_daily.get("TS") or latest_daily.get("ts"),
            }
        else:
            latest_kpi = _first(kpis)
            if latest_kpi:
                fe = latest_kpi.get("FINAL_EQUITY") or latest_kpi.get("final_equity")
                cash_and_exposure = {
                    "cash": None,
                    "exposure": None,
                    "total_equity": fe,
                    "as_of_ts": latest_kpi.get("TO_TS") or latest_kpi.get("to_ts"),
                }

        entries_blocked = None
        block_reason = None
        if rg:
            entries_blocked = rg.get("ENTRIES_BLOCKED") if rg.get("ENTRIES_BLOCKED") is not None else rg.get("entries_blocked")
            block_reason = rg.get("BLOCK_REASON") or rg.get("block_reason") or rg.get("STOP_REASON") or rg.get("stop_reason")

        risk_gate_status = {
            "entries_blocked": bool(entries_blocked),
            "exits_allowed": True,
            "summary": "Entries blocked but exits allowed." if entries_blocked else "Trading allowed.",
            "stop_reason": block_reason,
        }

        cards = {
            "cash_and_exposure": cash_and_exposure,
            "open_positions": positions,
            "recent_trades": trades[:20],
            "risk_gate_status": risk_gate_status,
        }

        return {
            "positions": positions,
            "trades": trades,
            "daily": daily,
            "kpis": kpis,
            "risk_gate": risk_gate_rows,
            "risk_state": risk_state,
            "cards": cards,
        }
    finally:
        conn.close()
