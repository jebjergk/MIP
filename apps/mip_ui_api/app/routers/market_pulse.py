"""
Market Pulse endpoint: GET /market/pulse
Aggregate market overview across the full symbol universe.
Returns per-symbol latest stats, aggregate KPIs, and recent bar history for charts.
Read-only; never writes to Snowflake.
"""
from fastapi import APIRouter, Query

from app.db import get_connection, fetch_all, serialize_rows

router = APIRouter(prefix="/market", tags=["market"])


@router.get("/pulse")
def get_market_pulse(
    lookback_days: int = Query(30, ge=1, le=90, description="Days of bar history for charts"),
):
    """
    Market Pulse — one-stop overview of the full symbol universe.

    Returns:
      - symbols: per-symbol latest close, return, OHLC
      - aggregate: up/down/flat counts, avg return, breadth
      - bars: recent daily bars for charting (last N days, all symbols)
    """
    conn = get_connection()
    try:
        cur = conn.cursor()

        # 1. Per-symbol latest daily bar with return vs previous close
        cur.execute(
            """
            with latest as (
                select
                    SYMBOL, MARKET_TYPE, TS,
                    OPEN, HIGH, LOW, CLOSE, VOLUME,
                    lag(CLOSE) over (
                        partition by SYMBOL, MARKET_TYPE
                        order by TS
                    ) as PREV_CLOSE
                from (
                    select SYMBOL, MARKET_TYPE, TS, OPEN, HIGH, LOW, CLOSE, VOLUME,
                        row_number() over (
                            partition by SYMBOL, MARKET_TYPE, TS
                            order by INGESTED_AT desc
                        ) as rn
                    from MIP.MART.MARKET_BARS
                    where INTERVAL_MINUTES = 1440
                      and TS >= dateadd('day', -5, current_date())
                )
                where rn = 1
            )
            select
                SYMBOL, MARKET_TYPE, TS,
                OPEN, HIGH, LOW, CLOSE, VOLUME, PREV_CLOSE,
                case
                    when PREV_CLOSE is not null and PREV_CLOSE <> 0
                    then (CLOSE - PREV_CLOSE) / PREV_CLOSE
                    else null
                end as DAY_RETURN
            from latest
            qualify row_number() over (
                partition by SYMBOL, MARKET_TYPE
                order by TS desc
            ) = 1
            order by SYMBOL
            """
        )
        symbol_rows = serialize_rows(fetch_all(cur))

        # Build per-symbol summary
        symbols = []
        up_count = 0
        down_count = 0
        flat_count = 0
        total_return = 0.0
        return_count = 0

        for r in symbol_rows:
            sym = r.get("SYMBOL") or r.get("symbol")
            mt = r.get("MARKET_TYPE") or r.get("market_type")
            close_val = r.get("CLOSE") or r.get("close")
            prev_close = r.get("PREV_CLOSE") or r.get("prev_close")
            day_return = r.get("DAY_RETURN") or r.get("day_return")

            if day_return is not None:
                try:
                    dr = float(day_return)
                    if dr > 0.0001:
                        up_count += 1
                    elif dr < -0.0001:
                        down_count += 1
                    else:
                        flat_count += 1
                    total_return += dr
                    return_count += 1
                except (TypeError, ValueError):
                    flat_count += 1
            else:
                flat_count += 1

            symbols.append({
                "symbol": sym,
                "market_type": mt,
                "ts": r.get("TS") or r.get("ts"),
                "open": _safe_float(r.get("OPEN") or r.get("open")),
                "high": _safe_float(r.get("HIGH") or r.get("high")),
                "low": _safe_float(r.get("LOW") or r.get("low")),
                "close": _safe_float(close_val),
                "prev_close": _safe_float(prev_close),
                "volume": r.get("VOLUME") or r.get("volume"),
                "day_return": _safe_float(day_return),
            })

        # Sort by return descending for top movers
        symbols.sort(key=lambda x: (x.get("day_return") or 0), reverse=True)

        avg_return = (total_return / return_count) if return_count > 0 else 0.0
        total_symbols = up_count + down_count + flat_count

        # Market direction
        if total_symbols == 0:
            direction = "NO_DATA"
        elif up_count > down_count * 1.5:
            direction = "UP"
        elif down_count > up_count * 1.5:
            direction = "DOWN"
        else:
            direction = "MIXED"

        aggregate = {
            "direction": direction,
            "total_symbols": total_symbols,
            "up_count": up_count,
            "down_count": down_count,
            "flat_count": flat_count,
            "avg_return": round(avg_return, 6),
            "avg_return_pct": round(avg_return * 100, 2),
            "breadth_pct": round((up_count / total_symbols * 100) if total_symbols > 0 else 0, 1),
        }

        # 2. Recent bars for chart (aggregate: equal-weight index of all symbols)
        cur.execute(
            f"""
            with deduped as (
                select TS, SYMBOL, MARKET_TYPE, OPEN, HIGH, LOW, CLOSE, VOLUME,
                    row_number() over (
                        partition by SYMBOL, MARKET_TYPE, TS
                        order by INGESTED_AT desc
                    ) as rn
                from MIP.MART.MARKET_BARS
                where INTERVAL_MINUTES = 1440
                  and TS >= dateadd('day', -{lookback_days}, current_date())
            ),
            bars as (
                select TS, SYMBOL, MARKET_TYPE, OPEN, HIGH, LOW, CLOSE, VOLUME
                from deduped where rn = 1
            ),
            indexed as (
                select
                    TS, SYMBOL, MARKET_TYPE, CLOSE,
                    first_value(CLOSE) over (
                        partition by SYMBOL, MARKET_TYPE
                        order by TS
                    ) as BASE_CLOSE,
                    lag(CLOSE) over (
                        partition by SYMBOL, MARKET_TYPE
                        order by TS
                    ) as PREV_CLOSE
                from bars
            )
            select
                TS,
                avg(case when BASE_CLOSE > 0 then (CLOSE / BASE_CLOSE - 1) * 100 end) as INDEX_RETURN_PCT,
                count(distinct SYMBOL) as SYMBOL_COUNT,
                sum(case when PREV_CLOSE is not null and CLOSE > PREV_CLOSE then 1 else 0 end) as UP_COUNT_DAY,
                count(*) as TOTAL_COUNT_DAY
            from indexed
            group by TS
            order by TS
            """
        )
        index_rows = serialize_rows(fetch_all(cur))

        index_series = []
        for r in index_rows:
            ts = r.get("TS") or r.get("ts")
            idx_ret = r.get("INDEX_RETURN_PCT") or r.get("index_return_pct")
            index_series.append({
                "ts": ts,
                "index_return_pct": round(float(idx_ret), 2) if idx_ret is not None else 0,
            })

        # 3. Per-symbol bars for sparklines (last 30 bars per symbol)
        cur.execute(
            f"""
            with deduped as (
                select TS, SYMBOL, MARKET_TYPE, CLOSE,
                    row_number() over (
                        partition by SYMBOL, MARKET_TYPE, TS
                        order by INGESTED_AT desc
                    ) as rn
                from MIP.MART.MARKET_BARS
                where INTERVAL_MINUTES = 1440
                  and TS >= dateadd('day', -{lookback_days}, current_date())
            )
            select TS, SYMBOL, MARKET_TYPE, CLOSE
            from deduped
            where rn = 1
            order by SYMBOL, TS
            """
        )
        sparkline_rows = serialize_rows(fetch_all(cur))

        # Group by symbol
        sparklines = {}
        for r in sparkline_rows:
            sym = r.get("SYMBOL") or r.get("symbol")
            if sym not in sparklines:
                sparklines[sym] = []
            sparklines[sym].append({
                "ts": r.get("TS") or r.get("ts"),
                "close": _safe_float(r.get("CLOSE") or r.get("close")),
            })

        return {
            "symbols": symbols,
            "aggregate": aggregate,
            "index_series": index_series,
            "sparklines": sparklines,
        }
    finally:
        conn.close()


def _safe_float(v):
    """Convert to float safely, return None on failure."""
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Intraday Market Pulse
# ---------------------------------------------------------------------------

@router.get("/pulse/intraday")
def get_intraday_market_pulse():
    """
    Intraday Market Pulse — session-level overview of the intraday symbol universe.

    Picks the most recent trading session (today if bars exist, otherwise yesterday).
    Returns per-symbol session stats, aggregate KPIs, and the session bar series.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()

        # Find the latest session date with 15-min bars
        cur.execute(
            """
            select max(TS::date) as SESSION_DATE
            from MIP.MART.MARKET_BARS
            where INTERVAL_MINUTES = 15
              and TS >= dateadd('day', -5, current_date())
            """
        )
        row = cur.fetchone()
        session_date = row[0] if row else None

        if not session_date:
            return {
                "session_date": None,
                "is_today": False,
                "symbols": [],
                "aggregate": {
                    "direction": "NO_DATA",
                    "total_symbols": 0,
                    "up_count": 0,
                    "down_count": 0,
                    "flat_count": 0,
                    "avg_return": 0,
                    "avg_return_pct": 0,
                    "breadth_pct": 0,
                },
                "bars": [],
            }

        import datetime
        today = datetime.date.today()
        is_today = (session_date == today)

        # Per-symbol session stats: open (first bar), close (last bar), high, low, return
        cur.execute(
            """
            with session_bars as (
                select
                    SYMBOL, MARKET_TYPE, TS, OPEN, HIGH, LOW, CLOSE, VOLUME,
                    row_number() over (partition by SYMBOL, MARKET_TYPE order by TS asc)  as rn_first,
                    row_number() over (partition by SYMBOL, MARKET_TYPE order by TS desc) as rn_last
                from MIP.MART.MARKET_BARS
                where INTERVAL_MINUTES = 15
                  and TS::date = %s
            )
            select
                SYMBOL, MARKET_TYPE,
                max(case when rn_first = 1 then OPEN end)  as SESSION_OPEN,
                max(HIGH)                                    as SESSION_HIGH,
                min(LOW)                                     as SESSION_LOW,
                max(case when rn_last = 1 then CLOSE end)   as SESSION_CLOSE,
                max(case when rn_last = 1 then TS end)      as LAST_BAR_TS,
                sum(VOLUME)                                  as SESSION_VOLUME,
                count(*)                                     as BAR_COUNT
            from session_bars
            group by SYMBOL, MARKET_TYPE
            order by SYMBOL
            """,
            (str(session_date),),
        )
        symbol_rows = serialize_rows(fetch_all(cur))

        symbols = []
        up_count = 0
        down_count = 0
        flat_count = 0
        total_return = 0.0
        return_count = 0

        for r in symbol_rows:
            sym = r.get("SYMBOL")
            mt = r.get("MARKET_TYPE")
            s_open = _safe_float(r.get("SESSION_OPEN"))
            s_close = _safe_float(r.get("SESSION_CLOSE"))

            session_return = None
            if s_open and s_close and s_open != 0:
                session_return = (s_close - s_open) / s_open

            if session_return is not None:
                if session_return > 0.0001:
                    up_count += 1
                elif session_return < -0.0001:
                    down_count += 1
                else:
                    flat_count += 1
                total_return += session_return
                return_count += 1
            else:
                flat_count += 1

            symbols.append({
                "symbol": sym,
                "market_type": mt,
                "session_open": s_open,
                "session_high": _safe_float(r.get("SESSION_HIGH")),
                "session_low": _safe_float(r.get("SESSION_LOW")),
                "session_close": s_close,
                "session_return": round(session_return, 6) if session_return is not None else None,
                "last_bar_ts": r.get("LAST_BAR_TS"),
                "session_volume": r.get("SESSION_VOLUME"),
                "bar_count": r.get("BAR_COUNT"),
            })

        symbols.sort(key=lambda x: (x.get("session_return") or 0), reverse=True)

        avg_return = (total_return / return_count) if return_count > 0 else 0.0
        total_symbols = up_count + down_count + flat_count

        if total_symbols == 0:
            direction = "NO_DATA"
        elif up_count > down_count * 1.5:
            direction = "UP"
        elif down_count > up_count * 1.5:
            direction = "DOWN"
        else:
            direction = "MIXED"

        aggregate = {
            "direction": direction,
            "total_symbols": total_symbols,
            "up_count": up_count,
            "down_count": down_count,
            "flat_count": flat_count,
            "avg_return": round(avg_return, 6),
            "avg_return_pct": round(avg_return * 100, 2),
            "breadth_pct": round((up_count / total_symbols * 100) if total_symbols > 0 else 0, 1),
        }

        # Session bar series (all 15m bars for the session date)
        cur.execute(
            """
            select TS, SYMBOL, MARKET_TYPE, OPEN, HIGH, LOW, CLOSE, VOLUME
            from MIP.MART.MARKET_BARS
            where INTERVAL_MINUTES = 15
              and TS::date = %s
            order by TS, SYMBOL
            """,
            (str(session_date),),
        )
        bar_rows = serialize_rows(fetch_all(cur))

        bars = []
        for r in bar_rows:
            bars.append({
                "ts": r.get("TS"),
                "symbol": r.get("SYMBOL"),
                "market_type": r.get("MARKET_TYPE"),
                "close": _safe_float(r.get("CLOSE")),
                "high": _safe_float(r.get("HIGH")),
                "low": _safe_float(r.get("LOW")),
                "volume": r.get("VOLUME"),
            })

        return {
            "session_date": str(session_date),
            "is_today": is_today,
            "symbols": symbols,
            "aggregate": aggregate,
            "bars": bars,
        }
    finally:
        conn.close()
