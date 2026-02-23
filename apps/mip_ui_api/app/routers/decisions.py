"""
Decision Console API — live decision feed, open positions, gate traces.
Includes SSE streaming endpoint for real-time updates.
"""
import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Query, Request
from fastapi.responses import StreamingResponse

from app.db import get_connection, fetch_all, serialize_rows

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/decisions", tags=["decisions"])

# ── helpers ──────────────────────────────────────────────────────────

_STAGE_MAP = {
    True:  "EXIT_TRIGGERED",
    False: None,
}


def _build_event_from_log(row: dict) -> dict:
    """Transform an EARLY_EXIT_LOG row into a Decision Event."""
    exit_signal = row.get("EXIT_SIGNAL") or False
    executed = (row.get("EXECUTION_STATUS") or "") == "EXECUTED"

    if executed:
        decision_type = "EXIT_EXECUTED"
        stage = "exited"
        severity = "red"
    elif exit_signal:
        decision_type = "EARLY_EXIT_TRIGGER"
        stage = "exit-triggered"
        severity = "red"
    else:
        decision_type = "POSITION_MONITOR"
        stage = "on-track"
        severity = "green"

    target_ret = row.get("TARGET_RETURN")
    current_ret = row.get("UNREALIZED_RETURN")
    mfe = row.get("MFE_RETURN")
    multiplier = row.get("PAYOFF_MULTIPLIER")
    effective_target = row.get("EFFECTIVE_TARGET")

    summary_parts = []
    if current_ret is not None and target_ret is not None:
        ret_pct = f"{current_ret * 100:+.2f}%"
        tgt_pct = f"{target_ret * 100:.2f}%"
        summary_parts.append(f"Return {ret_pct} vs target {tgt_pct}")
    if exit_signal and multiplier is not None and target_ret is not None:
        mult_str = f"{float(multiplier):.1f}"
        eff_pct = f"{float(effective_target or (target_ret * float(multiplier))) * 100:.2f}%"
        summary_parts.append(f"Threshold reached: {mult_str}\u00d7 expected payoff ({eff_pct})")
    if exit_signal and not executed:
        summary_parts.append(f"Exit signal in {(row.get('MODE') or 'SHADOW')} mode")
    if executed:
        summary_parts.append("Position closed via early exit")

    reason_codes = row.get("REASON_CODES")
    if isinstance(reason_codes, str):
        try:
            reason_codes = json.loads(reason_codes)
        except Exception:
            pass

    return {
        "event_id": row.get("LOG_ID"),
        "run_id": row.get("RUN_ID"),
        "decision_ts": _iso(row.get("DECISION_TS")),
        "bar_close_ts": _iso(row.get("BAR_CLOSE_TS")),
        "portfolio_id": row.get("PORTFOLIO_ID"),
        "symbol": row.get("SYMBOL"),
        "market_type": row.get("MARKET_TYPE"),
        "entry_ts": _iso(row.get("ENTRY_TS")),
        "decision_type": decision_type,
        "stage": stage,
        "severity": severity,
        "summary": " · ".join(summary_parts) if summary_parts else "Monitoring",
        "metrics": {
            "entry_price": _flt(row.get("ENTRY_PRICE")),
            "current_price": _flt(row.get("CURRENT_PRICE")),
            "target_return": _flt(target_ret),
            "unrealized_return": _flt(current_ret),
            "mfe_return": _flt(mfe),
            "effective_target": _flt(effective_target),
            "multiplier": _flt(multiplier),
            "early_exit_pnl": _flt(row.get("EARLY_EXIT_PNL")),
            "hold_to_end_pnl": _flt(row.get("HOLD_TO_END_PNL")),
            "pnl_delta": _flt(row.get("PNL_DELTA")),
        },
        "gates": {
            "threshold_reached": exit_signal,
        },
        "mode": row.get("MODE"),
        "execution_status": row.get("EXECUTION_STATUS"),
        "reason_codes": reason_codes,
    }


def _iso(v):
    if v is None:
        return None
    if hasattr(v, "isoformat"):
        return v.isoformat()
    return str(v)


def _flt(v):
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


# ── REST endpoints ───────────────────────────────────────────────────

@router.get("/open-positions")
def get_open_positions():
    """Current open daily positions with early-exit state."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        sql = """
        with latest_bars as (
            select SYMBOL, MARKET_TYPE, CLOSE, TS
            from MIP.MART.MARKET_BARS
            where INTERVAL_MINUTES = 15
            qualify row_number() over (
                partition by SYMBOL, MARKET_TYPE order by TS desc
            ) = 1
        ),
        best_targets as (
            select
                rl.SYMBOL, rl.MARKET_TYPE, rl.TS::date as SIGNAL_DATE,
                ts2.AVG_RETURN
            from MIP.APP.RECOMMENDATION_LOG rl
            join MIP.MART.V_TRUSTED_SIGNALS ts2
              on ts2.PATTERN_ID = rl.PATTERN_ID
             and ts2.MARKET_TYPE = rl.MARKET_TYPE
             and ts2.INTERVAL_MINUTES = 1440
             and ts2.IS_TRUSTED = true
            where rl.INTERVAL_MINUTES = 1440
            qualify row_number() over (
                partition by rl.SYMBOL, rl.MARKET_TYPE, rl.TS::date
                order by ts2.AVG_RETURN desc
            ) = 1
        )
        select
            op.PORTFOLIO_ID,
            p.NAME as PORTFOLIO_NAME,
            op.SYMBOL,
            op.MARKET_TYPE,
            op.ENTRY_TS,
            op.ENTRY_PRICE,
            op.QUANTITY,
            op.COST_BASIS,
            op.HOLD_UNTIL_INDEX,
            op.CURRENT_BAR_INDEX,

            lb.CLOSE as CURRENT_PRICE,
            lb.TS as LATEST_BAR_TS,
            case when op.ENTRY_PRICE > 0 and lb.CLOSE is not null
                 then (lb.CLOSE - op.ENTRY_PRICE) / op.ENTRY_PRICE
                 else null end as CURRENT_RETURN,

            bt.AVG_RETURN as TARGET_RETURN,

            ps.FIRST_HIT_TS,
            ps.FIRST_HIT_RETURN,
            ps.MFE_RETURN,
            ps.MFE_TS,
            ps.LAST_EVALUATED_TS,
            ps.EARLY_EXIT_FIRED,
            ps.EARLY_EXIT_TS,

            case
                when ps.EARLY_EXIT_FIRED then 'exited'
                when ps.FIRST_HIT_TS is not null then 'exit-triggered'
                else 'on-track'
            end as STAGE,

            datediff('minute', op.ENTRY_TS, current_timestamp()) as MINUTES_IN_TRADE

        from MIP.MART.V_PORTFOLIO_OPEN_POSITIONS_CANONICAL op
        join MIP.APP.PORTFOLIO p on p.PORTFOLIO_ID = op.PORTFOLIO_ID
        left join MIP.APP.EARLY_EXIT_POSITION_STATE ps
          on ps.PORTFOLIO_ID = op.PORTFOLIO_ID
         and ps.SYMBOL = op.SYMBOL
         and ps.ENTRY_TS = op.ENTRY_TS
        left join latest_bars lb
          on lb.SYMBOL = op.SYMBOL
         and lb.MARKET_TYPE = op.MARKET_TYPE
         and lb.TS > op.ENTRY_TS
        left join best_targets bt
          on bt.SYMBOL = op.SYMBOL
         and bt.MARKET_TYPE = op.MARKET_TYPE
         and bt.SIGNAL_DATE = (
                select max(rl2.TS::date)
                from MIP.APP.RECOMMENDATION_LOG rl2
                where rl2.SYMBOL = op.SYMBOL
                  and rl2.MARKET_TYPE = op.MARKET_TYPE
                  and rl2.INTERVAL_MINUTES = 1440
                  and rl2.TS < op.ENTRY_TS
            )
        where op.INTERVAL_MINUTES = 1440
          and op.IS_OPEN = true
        order by op.PORTFOLIO_ID, op.SYMBOL
        """
        cur.execute(sql)
        rows = fetch_all(cur)
        return {"positions": serialize_rows(rows)}
    finally:
        conn.close()


@router.get("/events")
def get_events(
    portfolio_id: Optional[int] = Query(None),
    symbol: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=500),
    after_id: Optional[int] = Query(None, description="Only events with LOG_ID > this value"),
    date: Optional[str] = Query(None, description="Filter by date (YYYY-MM-DD)"),
):
    """Decision events from EARLY_EXIT_LOG, newest first."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        wheres = ["1=1"]
        params = []

        if portfolio_id is not None:
            wheres.append("PORTFOLIO_ID = %s")
            params.append(portfolio_id)
        if symbol:
            wheres.append("SYMBOL = %s")
            params.append(symbol.upper())
        if after_id is not None:
            wheres.append("LOG_ID > %s")
            params.append(after_id)
        if date:
            wheres.append("BAR_CLOSE_TS::date = %s")
            params.append(date)

        sql = f"""
        select *
        from MIP.APP.EARLY_EXIT_LOG
        where {' and '.join(wheres)}
        order by LOG_ID desc
        limit %s
        """
        params.append(limit)
        cur.execute(sql, params)
        rows = fetch_all(cur)
        events = [_build_event_from_log(r) for r in rows]
        return {"events": events, "count": len(events)}
    finally:
        conn.close()


@router.get("/position-trace")
def get_position_trace(
    portfolio_id: int = Query(...),
    symbol: str = Query(...),
    entry_ts: str = Query(...),
):
    """Full gate trace timeline for a single position."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        sql = """
        select *
        from MIP.APP.EARLY_EXIT_LOG
        where PORTFOLIO_ID = %s
          and SYMBOL = %s
          and ENTRY_TS = %s
        order by BAR_CLOSE_TS asc
        """
        cur.execute(sql, (portfolio_id, symbol.upper(), entry_ts))
        rows = fetch_all(cur)
        events = [_build_event_from_log(r) for r in rows]

        # Position state
        state_sql = """
        select * from MIP.APP.EARLY_EXIT_POSITION_STATE
        where PORTFOLIO_ID = %s and SYMBOL = %s and ENTRY_TS = %s
        """
        cur.execute(state_sql, (portfolio_id, symbol.upper(), entry_ts))
        state_rows = fetch_all(cur)
        state = serialize_rows(state_rows)[0] if state_rows else None

        return {"timeline": events, "state": state}
    finally:
        conn.close()


@router.get("/config")
def get_config():
    """Current early-exit configuration."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            select CONFIG_KEY, CONFIG_VALUE, DESCRIPTION
            from MIP.APP.APP_CONFIG
            where CONFIG_KEY like 'EARLY_EXIT%%'
               or CONFIG_KEY = 'LIVE_DECISION_CONSOLE_ENABLED'
            order by CONFIG_KEY
        """)
        rows = fetch_all(cur)
        config = {}
        for r in rows:
            config[r["CONFIG_KEY"]] = {
                "value": r["CONFIG_VALUE"],
                "description": r.get("DESCRIPTION"),
            }
        return {"config": config}
    finally:
        conn.close()


@router.get("/decision-diff")
def get_decision_diff(
    portfolio_id: int = Query(...),
    symbol: str = Query(...),
    entry_ts: str = Query(...),
):
    """Compare baseline hold-to-horizon vs early-exit for an open position."""
    conn = get_connection()
    try:
        cur = conn.cursor()

        # Get position + current state
        pos_sql = """
        select
            op.ENTRY_PRICE, op.QUANTITY, op.COST_BASIS,
            op.HOLD_UNTIL_INDEX, op.CURRENT_BAR_INDEX,
            ps.MFE_RETURN, ps.FIRST_HIT_RETURN, ps.FIRST_HIT_TS
        from MIP.MART.V_PORTFOLIO_OPEN_POSITIONS_CANONICAL op
        left join MIP.APP.EARLY_EXIT_POSITION_STATE ps
          on ps.PORTFOLIO_ID = op.PORTFOLIO_ID
         and ps.SYMBOL = op.SYMBOL
         and ps.ENTRY_TS = op.ENTRY_TS
        where op.PORTFOLIO_ID = %s
          and op.SYMBOL = %s
          and op.ENTRY_TS = %s
        limit 1
        """
        cur.execute(pos_sql, (portfolio_id, symbol.upper(), entry_ts))
        pos_rows = fetch_all(cur)
        if not pos_rows:
            return {"diff": None, "reason": "Position not found"}
        pos = pos_rows[0]

        # Latest 15-min bar
        bar_sql = """
        select mb.CLOSE, mb.TS
        from MIP.MART.MARKET_BARS mb
        join MIP.MART.V_PORTFOLIO_OPEN_POSITIONS_CANONICAL op
          on op.SYMBOL = mb.SYMBOL and op.MARKET_TYPE = mb.MARKET_TYPE
        where mb.SYMBOL = %s
          and mb.INTERVAL_MINUTES = 15
          and mb.TS > %s
          and op.PORTFOLIO_ID = %s and op.ENTRY_TS = %s
        order by mb.TS desc limit 1
        """
        cur.execute(bar_sql, (symbol.upper(), entry_ts, portfolio_id, entry_ts))
        bar_rows = fetch_all(cur)
        current_price = float(bar_rows[0]["CLOSE"]) if bar_rows else None

        entry_price = float(pos["ENTRY_PRICE"])
        quantity = float(pos["QUANTITY"])
        cost_basis = float(pos["COST_BASIS"])

        # Target return
        tgt_sql = """
        select ts2.AVG_RETURN
        from MIP.APP.RECOMMENDATION_LOG rl
        join MIP.MART.V_TRUSTED_SIGNALS ts2
          on ts2.PATTERN_ID = rl.PATTERN_ID
         and ts2.MARKET_TYPE = rl.MARKET_TYPE
         and ts2.INTERVAL_MINUTES = 1440
         and ts2.IS_TRUSTED = true
        where rl.SYMBOL = %s and rl.INTERVAL_MINUTES = 1440
          and rl.TS::date = (
                select max(rl2.TS::date)
                from MIP.APP.RECOMMENDATION_LOG rl2
                where rl2.SYMBOL = rl.SYMBOL
                  and rl2.MARKET_TYPE = rl.MARKET_TYPE
                  and rl2.INTERVAL_MINUTES = 1440
                  and rl2.TS < %s
            )
        order by ts2.AVG_RETURN desc limit 1
        """
        cur.execute(tgt_sql, (symbol.upper(), entry_ts))
        tgt_rows = fetch_all(cur)
        target_return = float(tgt_rows[0]["AVG_RETURN"]) if tgt_rows else None

        exit_now_return = ((current_price - entry_price) / entry_price) if current_price else None
        exit_now_pnl = ((current_price - entry_price) * quantity) if current_price else None
        hold_expected_pnl = (target_return * cost_basis) if target_return else None

        return {
            "diff": {
                "entry_price": entry_price,
                "current_price": current_price,
                "target_return": target_return,
                "exit_now_return": exit_now_return,
                "exit_now_pnl": exit_now_pnl,
                "hold_expected_pnl": hold_expected_pnl,
                "pnl_delta": (exit_now_pnl - hold_expected_pnl)
                    if exit_now_pnl is not None and hold_expected_pnl is not None else None,
                "mfe_return": _flt(pos.get("MFE_RETURN")),
                "bars_remaining": (pos.get("HOLD_UNTIL_INDEX") or 0) - (pos.get("CURRENT_BAR_INDEX") or 0),
            }
        }
    finally:
        conn.close()


# ── SSE streaming ────────────────────────────────────────────────────

@router.get("/stream")
async def stream_events(
    request: Request,
    portfolio_id: Optional[int] = Query(None),
):
    """
    Server-Sent Events stream.
    Polls decision data every 15 minutes to reduce warehouse churn.
    """

    async def event_generator():
        last_event_id = 0

        yield _sse_msg("connected", {"ts": datetime.now(timezone.utc).isoformat()})

        while True:
            if await request.is_disconnected():
                break

            try:
                conn = get_connection()
                try:
                    cur = conn.cursor()

                    # Fetch new events since last check
                    wheres = ["LOG_ID > %s"]
                    params = [last_event_id]
                    if portfolio_id is not None:
                        wheres.append("PORTFOLIO_ID = %s")
                        params.append(portfolio_id)
                    params.append(50)

                    sql = f"""
                    select * from MIP.APP.EARLY_EXIT_LOG
                    where {' and '.join(wheres)}
                    order by LOG_ID asc
                    limit %s
                    """
                    cur.execute(sql, params)
                    rows = fetch_all(cur)

                    if rows:
                        events = [_build_event_from_log(r) for r in rows]
                        last_event_id = max(r.get("LOG_ID", 0) for r in rows)
                        yield _sse_msg("events", {
                            "events": events,
                            "last_id": last_event_id,
                        })

                    # Send position summary every cycle (same 15-minute cadence)
                    cur.execute("""
                    select
                        (select count(*) from MIP.MART.V_PORTFOLIO_OPEN_POSITIONS_CANONICAL
                         where INTERVAL_MINUTES = 1440 and IS_OPEN = true) as OPEN_COUNT,
                        (select count(*) from MIP.APP.EARLY_EXIT_POSITION_STATE
                         where EARLY_EXIT_FIRED = true) as EXITED_COUNT
                    """)
                    hb_rows = fetch_all(cur)
                    if hb_rows:
                        yield _sse_msg("heartbeat", {
                            "ts": datetime.now(timezone.utc).isoformat(),
                            "open": hb_rows[0].get("OPEN_COUNT", 0),
                            "exited": hb_rows[0].get("EXITED_COUNT", 0),
                        })
                finally:
                    conn.close()
            except Exception as e:
                logger.warning("SSE poll error: %s", e)
                yield _sse_msg("error", {"message": str(e)})

            await asyncio.sleep(900)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


def _sse_msg(event_type: str, data: dict) -> str:
    return f"event: {event_type}\ndata: {json.dumps(data, default=str)}\n\n"
