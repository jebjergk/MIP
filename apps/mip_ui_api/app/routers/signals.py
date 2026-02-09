"""
Signals Explorer endpoint: GET /signals
Returns actual signal/recommendation rows with filters.
Used by Cockpit deep-links for opportunity drill-down.
"""
from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Query

from app.db import get_connection, fetch_all, serialize_row

router = APIRouter(prefix="/signals", tags=["signals"])


def _serialize_rows(rows):
    return [serialize_row(r) for r in rows] if rows else []


@router.get("")
def get_signals(
    symbol: Optional[str] = Query(None, description="Filter by symbol (e.g., AAPL)"),
    market_type: Optional[str] = Query(None, description="Filter by market type (STOCK, FX)"),
    pattern_id: Optional[str] = Query(None, description="Filter by pattern ID"),
    horizon_bars: Optional[int] = Query(None, description="Filter by horizon bars"),
    run_id: Optional[str] = Query(None, description="Filter by pipeline run ID"),
    as_of_ts: Optional[str] = Query(None, description="Filter by as-of timestamp (ISO format)"),
    trust_label: Optional[str] = Query(None, description="Filter by trust label (TRUSTED, WATCH, UNTRUSTED)"),
    limit: int = Query(100, ge=1, le=500, description="Max rows to return"),
    include_fallback: bool = Query(True, description="Include fallback results if primary query returns 0"),
):
    """
    Signal Explorer: Fetch actual signal/recommendation rows with flexible filters.
    
    Primary source: V_SIGNALS_ELIGIBLE_TODAY joined with trust info.
    Fallback sources (when primary returns 0):
    1. Drop run_id filter, keep other filters
    2. Drop as_of_ts filter, use 7-day window
    3. Show all recent signals for symbol
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        
        # Build primary query with all filters
        primary_result = _query_signals(
            cur, 
            symbol=symbol,
            market_type=market_type,
            pattern_id=pattern_id,
            horizon_bars=horizon_bars,
            run_id=run_id,
            as_of_ts=as_of_ts,
            trust_label=trust_label,
            limit=limit,
        )
        
        if primary_result["count"] > 0 or not include_fallback:
            return {
                "signals": primary_result["rows"],
                "count": primary_result["count"],
                "query_type": "primary",
                "filters_applied": primary_result["filters"],
                "fallback_used": False,
                "fallback_reason": None,
            }
        
        # Fallback 1: Drop run_id filter
        if run_id:
            fallback1 = _query_signals(
                cur,
                symbol=symbol,
                market_type=market_type,
                pattern_id=pattern_id,
                horizon_bars=horizon_bars,
                run_id=None,  # Drop run_id
                as_of_ts=as_of_ts,
                trust_label=trust_label,
                limit=limit,
            )
            if fallback1["count"] > 0:
                return {
                    "signals": fallback1["rows"],
                    "count": fallback1["count"],
                    "query_type": "fallback_no_run_id",
                    "filters_applied": fallback1["filters"],
                    "fallback_used": True,
                    "fallback_reason": f"No signals matched run_id={run_id}. Showing signals without run filter.",
                }
        
        # Fallback 2: Use 7-day window instead of exact as_of_ts
        if as_of_ts:
            fallback2 = _query_signals(
                cur,
                symbol=symbol,
                market_type=market_type,
                pattern_id=pattern_id,
                horizon_bars=horizon_bars,
                run_id=None,
                as_of_ts=None,  # Drop as_of_ts
                trust_label=trust_label,
                limit=limit,
                days_window=7,
            )
            if fallback2["count"] > 0:
                return {
                    "signals": fallback2["rows"],
                    "count": fallback2["count"],
                    "query_type": "fallback_7day_window",
                    "filters_applied": fallback2["filters"],
                    "fallback_used": True,
                    "fallback_reason": f"No signals matched as_of_ts={as_of_ts}. Showing signals from last 7 days.",
                }
        
        # Fallback 3: Show all recent signals for symbol (drop most filters)
        if symbol:
            fallback3 = _query_signals(
                cur,
                symbol=symbol,
                market_type=market_type,
                pattern_id=None,  # Drop pattern_id
                horizon_bars=None,
                run_id=None,
                as_of_ts=None,
                trust_label=None,
                limit=limit,
                days_window=30,
            )
            if fallback3["count"] > 0:
                return {
                    "signals": fallback3["rows"],
                    "count": fallback3["count"],
                    "query_type": "fallback_symbol_only",
                    "filters_applied": fallback3["filters"],
                    "fallback_used": True,
                    "fallback_reason": f"No exact matches. Showing all recent signals for {symbol}.",
                }
        
        # No results even with fallback
        return {
            "signals": [],
            "count": 0,
            "query_type": "no_results",
            "filters_applied": {
                "symbol": symbol,
                "market_type": market_type,
                "pattern_id": pattern_id,
            },
            "fallback_used": True,
            "fallback_reason": "No signals found matching any criteria. Try clearing filters or check if the data is stale.",
        }
        
    finally:
        conn.close()


def _query_signals(
    cur,
    symbol: Optional[str] = None,
    market_type: Optional[str] = None,
    pattern_id: Optional[str] = None,
    horizon_bars: Optional[int] = None,
    run_id: Optional[str] = None,
    as_of_ts: Optional[str] = None,
    trust_label: Optional[str] = None,
    limit: int = 100,
    days_window: Optional[int] = None,
) -> dict:
    """Execute signal query with given filters."""
    
    # Build WHERE conditions
    conditions = ["s.INTERVAL_MINUTES = 1440"]  # Daily signals only
    params = []
    filters_applied = {}
    
    if symbol:
        conditions.append("s.SYMBOL = %s")
        params.append(symbol.upper())
        filters_applied["symbol"] = symbol.upper()
    
    if market_type:
        conditions.append("s.MARKET_TYPE = %s")
        params.append(market_type.upper())
        filters_applied["market_type"] = market_type.upper()
    
    if pattern_id:
        conditions.append("s.PATTERN_ID = %s")
        params.append(pattern_id)
        filters_applied["pattern_id"] = pattern_id
    
    if run_id:
        conditions.append("s.RUN_ID = %s")
        params.append(run_id)
        filters_applied["run_id"] = run_id
    
    if as_of_ts:
        # Parse and use exact date
        try:
            ts = datetime.fromisoformat(as_of_ts.replace('Z', '+00:00'))
            conditions.append("date(s.TS) = date(%s)")
            params.append(ts)
            filters_applied["as_of_ts"] = as_of_ts
        except ValueError:
            pass
    
    if days_window:
        conditions.append(f"s.TS >= dateadd(day, -{days_window}, current_timestamp())")
        filters_applied["days_window"] = days_window
    
    if trust_label:
        conditions.append("s.TRUST_LABEL = %s")
        params.append(trust_label.upper())
        filters_applied["trust_label"] = trust_label.upper()
    
    where_clause = " AND ".join(conditions)
    
    # Query with trust info
    sql = f"""
    select
        s.RECOMMENDATION_ID,
        s.RUN_ID,
        s.TS as signal_ts,
        s.SYMBOL,
        s.MARKET_TYPE,
        s.INTERVAL_MINUTES,
        s.PATTERN_ID,
        s.SCORE,
        s.DETAILS,
        s.TRUST_LABEL,
        s.RECOMMENDED_ACTION,
        s.IS_ELIGIBLE,
        s.GATING_REASON
    from MIP.APP.V_SIGNALS_ELIGIBLE_TODAY s
    where {where_clause}
    order by s.TS desc, s.SCORE desc
    limit {limit}
    """
    
    cur.execute(sql, tuple(params))
    rows = fetch_all(cur)
    
    return {
        "rows": _serialize_rows(rows),
        "count": len(rows),
        "filters": filters_applied,
    }


@router.get("/latest-run")
def get_latest_run():
    """
    Get latest successful pipeline run info.
    Used to determine if digest data is stale.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        
        # Get latest pipeline run from PORTFOLIO table
        sql = """
        select
            LAST_SIMULATION_RUN_ID as run_id,
            LAST_SIMULATED_AT as run_ts
        from MIP.APP.PORTFOLIO
        where STATUS = 'ACTIVE'
          and LAST_SIMULATION_RUN_ID is not null
        order by LAST_SIMULATED_AT desc
        limit 1
        """
        cur.execute(sql)
        row = cur.fetchone()
        
        if not row:
            return {
                "found": False,
                "message": "No pipeline runs found.",
            }
        
        columns = [d[0].lower() for d in cur.description]
        data = serialize_row(dict(zip(columns, row)))
        
        return {
            "found": True,
            "latest_run_id": data.get("run_id"),
            "latest_run_ts": data.get("run_ts"),
        }
    finally:
        conn.close()
