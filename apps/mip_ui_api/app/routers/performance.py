"""
Performance summary: outcomes-based stats per (market_type, symbol, pattern_id).
GET /performance/summary — bridge to Suggestions.
Uses MIP.APP.RECOMMENDATION_LOG (daily bars only) + MIP.APP.RECOMMENDATION_OUTCOMES.
"""
from fastapi import APIRouter, HTTPException, Query

from app.db import get_connection, fetch_all

router = APIRouter(prefix="/performance", tags=["performance"])

# Daily bars only (INTERVAL_MINUTES = 1440)
# Aggregate by HORIZON_BARS: n_outcomes, mean_outcome, pct_positive, min/max, last_recommendation_ts per horizon
SUMMARY_SQL = """
select
  o.HORIZON_BARS as horizon_bars,
  count(*) as n_outcomes,
  avg(o.REALIZED_RETURN) as mean_outcome,
  sum(case when o.REALIZED_RETURN > 0 then 1 else 0 end)::float / nullif(count(*), 0) as pct_positive,
  min(o.REALIZED_RETURN) as min_outcome,
  max(o.REALIZED_RETURN) as max_outcome,
  max(r.TS) as last_recommendation_ts
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

# Total recommendation count for the triple (daily only)
N_RECS_SQL = """
select count(*) as n_recs
from MIP.APP.RECOMMENDATION_LOG r
where r.MARKET_TYPE = %(market_type)s
  and r.SYMBOL = %(symbol)s
  and r.PATTERN_ID = %(pattern_id)s
  and r.INTERVAL_MINUTES = 1440
"""

# Overall last recommendation timestamp (daily only)
LAST_TS_SQL = """
select max(r.TS) as last_recommendation_ts
from MIP.APP.RECOMMENDATION_LOG r
where r.MARKET_TYPE = %(market_type)s
  and r.SYMBOL = %(symbol)s
  and r.PATTERN_ID = %(pattern_id)s
  and r.INTERVAL_MINUTES = 1440
"""


def _serialize_horizon(row: dict) -> dict:
    """Serialize one horizon row: isoformat timestamps, float-safe numbers."""
    out = {}
    for k, v in row.items():
        if hasattr(v, "isoformat"):
            out[k] = v.isoformat()
        elif hasattr(v, "__float__") and v is not None and not isinstance(v, (int, bool)):
            try:
                out[k] = float(v)
            except (TypeError, ValueError):
                out[k] = v
        else:
            out[k] = v
    return out


@router.get("/summary")
def get_performance_summary(
    market_type: str = Query(..., description="Market type (e.g. STOCK, ETF, FX)"),
    symbol: str = Query(..., description="Symbol (ticker)"),
    pattern_id: int = Query(..., description="Pattern ID"),
):
    """
    Outcomes-powered performance summary for one (market_type, symbol, pattern_id).
    Aggregates from RECOMMENDATION_LOG (daily bars only) and RECOMMENDATION_OUTCOMES by HORIZON_BARS.
    Returns: n_recs, and per horizon: n_outcomes, mean_outcome, pct_positive, min_outcome, max_outcome, last_recommendation_ts.
    """
    params = {"market_type": market_type, "symbol": symbol, "pattern_id": pattern_id}
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(SUMMARY_SQL, params)
        horizon_rows = fetch_all(cur)
        cur.execute(N_RECS_SQL, params)
        n_recs_row = cur.fetchone()
        n_recs = n_recs_row[0] if n_recs_row and n_recs_row[0] is not None else 0
        cur.execute(LAST_TS_SQL, params)
        ts_row = cur.fetchone()
        last_ts = ts_row[0] if ts_row and ts_row[0] is not None else None
        horizons = [_serialize_horizon(d) for d in horizon_rows]
        for h in horizons:
            ts = h.get("last_recommendation_ts")
            if ts is not None and hasattr(ts, "isoformat"):
                h["last_recommendation_ts"] = ts.isoformat()
        if last_ts is not None and hasattr(last_ts, "isoformat"):
            last_ts = last_ts.isoformat()
        return {
            "market_type": market_type,
            "symbol": symbol,
            "pattern_id": pattern_id,
            "n_recs": n_recs,
            "last_recommendation_ts": last_ts,
            "horizons": horizons,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    finally:
        conn.close()
