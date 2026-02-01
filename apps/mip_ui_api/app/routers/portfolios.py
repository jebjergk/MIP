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


@router.get("/{portfolio_id}/snapshot")
def get_portfolio_snapshot(portfolio_id: int, run_id: str | None = None):
    """Combined read: positions, trades, daily, KPIs, risk for this portfolio (optional run_id filter)."""
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
        risk_gate = serialize_rows(fetch_all(cur))

        # Risk (state)
        cur.execute(
            "select * from MIP.MART.V_PORTFOLIO_RISK_STATE where PORTFOLIO_ID = %s",
            (portfolio_id,),
        )
        risk_state = serialize_rows(fetch_all(cur))

        return {
            "positions": positions,
            "trades": trades,
            "daily": daily,
            "kpis": kpis,
            "risk_gate": risk_gate,
            "risk_state": risk_state,
        }
    finally:
        conn.close()
