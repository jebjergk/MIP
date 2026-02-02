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


# Snapshot semantics:
# - pos_as_of_col: AS_OF_TS in V_PORTFOLIO_OPEN_POSITIONS_CANONICAL (latest bar snapshot)
# - trade_ts_col: TRADE_TS in PORTFOLIO_TRADES
# - run_id_col: RUN_ID in both tables
# - open_filter: use canonical view (IS_OPEN) so we return only open positions


@router.get("/{portfolio_id}/snapshot")
def get_portfolio_snapshot(portfolio_id: int, run_id: str | None = None):
    """
    Combined read: latest open positions (canonical view), trades aligned to same run, daily, KPIs, risk + cards.
    Positions: from V_PORTFOLIO_OPEN_POSITIONS_CANONICAL (only open; snapshot = max(AS_OF_TS)).
    Trades: same RUN_ID as snapshot (run_id param, or PORTFOLIO.LAST_SIMULATION_RUN_ID, or latest from PORTFOLIO_DAILY).
    """
    conn = get_connection()
    try:
        cur = conn.cursor()

        # Resolve effective run for trades/daily/kpis (run_id param, or latest from portfolio)
        effective_run_id = run_id
        if effective_run_id is None:
            cur.execute(
                "select LAST_SIMULATION_RUN_ID from MIP.APP.PORTFOLIO where PORTFOLIO_ID = %s",
                (portfolio_id,),
            )
            row = cur.fetchone()
            if row and row[0]:
                effective_run_id = row[0]
            else:
                cur.execute(
                    """
                    select RUN_ID from MIP.APP.PORTFOLIO_DAILY
                    where PORTFOLIO_ID = %s
                    order by TS desc
                    limit 1
                    """,
                    (portfolio_id,),
                )
                r = cur.fetchone()
                if r and r[0]:
                    effective_run_id = r[0]

        # Open positions: latest snapshot, only open (canonical view uses AS_OF_TS + IS_OPEN)
        positions = []
        snapshot_ts = None
        try:
            cur.execute(
                """
                select PORTFOLIO_ID, RUN_ID, SYMBOL, MARKET_TYPE, INTERVAL_MINUTES,
                       ENTRY_TS, ENTRY_PRICE, QUANTITY, COST_BASIS, ENTRY_SCORE,
                       ENTRY_INDEX, HOLD_UNTIL_INDEX, AS_OF_TS, CURRENT_BAR_INDEX, IS_OPEN, OPEN_POSITIONS
                from MIP.MART.V_PORTFOLIO_OPEN_POSITIONS_CANONICAL
                where PORTFOLIO_ID = %s
                order by ENTRY_TS desc
                """,
                (portfolio_id,),
            )
            positions = serialize_rows(fetch_all(cur))
            if positions:
                snapshot_ts = positions[0].get("AS_OF_TS") or positions[0].get("as_of_ts")
        except Exception:
            # Fallback: raw positions for effective run (no open filter) if canonical view not granted
            if effective_run_id:
                cur.execute(
                    """
                    select * from MIP.APP.PORTFOLIO_POSITIONS
                    where PORTFOLIO_ID = %s and RUN_ID = %s
                    order by ENTRY_TS desc
                    """,
                    (portfolio_id, effective_run_id),
                )
                positions = serialize_rows(fetch_all(cur))

        # Trades: aligned to same run (trade_ts_col = TRADE_TS, run_id_col = RUN_ID)
        cur.execute(
            """
            select * from MIP.APP.PORTFOLIO_TRADES
            where PORTFOLIO_ID = %s and (%s is null or RUN_ID = %s)
            order by TRADE_TS desc
            """,
            (portfolio_id, effective_run_id, effective_run_id),
        )
        trades = serialize_rows(fetch_all(cur))

        # Daily: same run
        cur.execute(
            """
            select * from MIP.APP.PORTFOLIO_DAILY
            where PORTFOLIO_ID = %s and (%s is null or RUN_ID = %s)
            order by TS desc
            """,
            (portfolio_id, effective_run_id, effective_run_id),
        )
        daily = serialize_rows(fetch_all(cur))

        # KPIs: same run
        cur.execute(
            """
            select * from MIP.MART.V_PORTFOLIO_RUN_KPIS
            where PORTFOLIO_ID = %s and (%s is null or RUN_ID = %s)
            order by TO_TS desc
            """,
            (portfolio_id, effective_run_id, effective_run_id),
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
                    "as_of_ts": snapshot_ts or latest_kpi.get("TO_TS") or latest_kpi.get("to_ts"),
                }
            elif snapshot_ts is not None:
                cash_and_exposure = {"cash": None, "exposure": None, "total_equity": None, "as_of_ts": snapshot_ts}

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
            "snapshot_ts": snapshot_ts,
            "run_id": effective_run_id,
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
