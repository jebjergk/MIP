"""
Performance summary: outcomes-based stats per (market_type, symbol, pattern_id).
Uses MIP.APP.RECOMMENDATION_LOG + MIP.APP.RECOMMENDATION_OUTCOMES.
"""
from fastapi import APIRouter, HTTPException, Query

from app.db import get_connection, fetch_all, serialize_rows

router = APIRouter(prefix="/performance", tags=["performance"])

# Aggregate by HORIZON_BARS for the given triple; EVAL_STATUS = SUCCESS, REALIZED_RETURN not null
SUMMARY_SQL = """
select
  o.HORIZON_BARS as horizon_bars,
  count(*) as n,
  avg(o.REALIZED_RETURN) as mean_outcome,
  sum(case when o.REALIZED_RETURN > 0 then 1 else 0 end)::float / nullif(count(*), 0) as pct_positive,
  min(o.REALIZED_RETURN) as min_outcome,
  max(o.REALIZED_RETURN) as max_outcome
from MIP.APP.RECOMMENDATION_LOG r
join MIP.APP.RECOMMENDATION_OUTCOMES o on o.RECOMMENDATION_ID = r.RECOMMENDATION_ID
where r.MARKET_TYPE = %(market_type)s
  and r.SYMBOL = %(symbol)s
  and r.PATTERN_ID = %(pattern_id)s
  and r.INTERVAL_MINUTES = 1440
  and o.EVAL_STATUS = 'SUCCESS'
  and o.REALIZED_RETURN is not null
group by o.HORIZON_BARS
order by o.HORIZON_BARS
"""

LAST_TS_SQL = """
select max(r.TS) as last_recommendation_ts
from MIP.APP.RECOMMENDATION_LOG r
where r.MARKET_TYPE = %(market_type)s
  and r.SYMBOL = %(symbol)s
  and r.PATTERN_ID = %(pattern_id)s
  and r.INTERVAL_MINUTES = 1440
"""


@router.get("/summary")
def get_performance_summary(
    market_type: str = Query(..., description="Market type (e.g. STOCK, ETF, FX)"),
    symbol: str = Query(..., description="Symbol (ticker)"),
    pattern_id: int = Query(..., description="Pattern ID"),
):
    """
    Outcomes-based performance summary for one (market_type, symbol, pattern_id).
    Returns counts, mean outcome, pct positive, min/max per horizon, and last_recommendation_ts.
    """
    params = (market_type, symbol, pattern_id)
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(SUMMARY_SQL, params)
        horizon_rows = fetch_all(cur)
        cur.execute(LAST_TS_SQL, params)
        ts_row = cur.fetchone()
        last_ts = ts_row[0] if ts_row and ts_row[0] is not None else None
        horizons = serialize_rows(horizon_rows)
        if last_ts is not None and hasattr(last_ts, "isoformat"):
            last_ts = last_ts.isoformat()
        return {
            "market_type": market_type,
            "symbol": symbol,
            "pattern_id": pattern_id,
            "last_recommendation_ts": last_ts,
            "horizons": horizons,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    finally:
        conn.close()
