"""
Training Status v1: per (market_type, symbol, pattern_id, interval_minutes) for INTERVAL_MINUTES=1440.
Uses MIP.APP.RECOMMENDATION_LOG, MIP.APP.RECOMMENDATION_OUTCOMES;
optional MIP.APP.PATTERN_DEFINITION (labels), MIP.APP.TRAINING_GATE_PARAMS (thresholds).
"""
from fastapi import APIRouter, HTTPException

from app.db import get_connection, fetch_all, serialize_rows
from app.training_status import (
    apply_scoring_to_rows,
    DEFAULT_MIN_SIGNALS,
)

router = APIRouter(prefix="/training", tags=["training"])

# Training Status v1: INTERVAL_MINUTES = 1440 only. No placeholders required for base query.
TRAINING_STATUS_SQL = """
with recs as (
  select
    r.MARKET_TYPE,
    r.SYMBOL,
    r.PATTERN_ID,
    r.INTERVAL_MINUTES,
    count(*) as recs_total,
    max(r.TS) as as_of_ts
  from MIP.APP.RECOMMENDATION_LOG r
  where r.INTERVAL_MINUTES = 1440
  group by r.MARKET_TYPE, r.SYMBOL, r.PATTERN_ID, r.INTERVAL_MINUTES
),
outcomes_agg as (
  select
    r.MARKET_TYPE,
    r.SYMBOL,
    r.PATTERN_ID,
    r.INTERVAL_MINUTES,
    count(*) as outcomes_total,
    count(distinct o.HORIZON_BARS) as horizons_covered,
    avg(case when o.HORIZON_BARS = 1 and o.EVAL_STATUS = 'SUCCESS' then o.REALIZED_RETURN end) as avg_outcome_h1,
    avg(case when o.HORIZON_BARS = 3 and o.EVAL_STATUS = 'SUCCESS' then o.REALIZED_RETURN end) as avg_outcome_h3,
    avg(case when o.HORIZON_BARS = 5 and o.EVAL_STATUS = 'SUCCESS' then o.REALIZED_RETURN end) as avg_outcome_h5,
    avg(case when o.HORIZON_BARS = 10 and o.EVAL_STATUS = 'SUCCESS' then o.REALIZED_RETURN end) as avg_outcome_h10,
    avg(case when o.HORIZON_BARS = 20 and o.EVAL_STATUS = 'SUCCESS' then o.REALIZED_RETURN end) as avg_outcome_h20
  from MIP.APP.RECOMMENDATION_LOG r
  join MIP.APP.RECOMMENDATION_OUTCOMES o on o.RECOMMENDATION_ID = r.RECOMMENDATION_ID
  where r.INTERVAL_MINUTES = 1440
  group by r.MARKET_TYPE, r.SYMBOL, r.PATTERN_ID, r.INTERVAL_MINUTES
)
select
  recs.MARKET_TYPE as market_type,
  recs.SYMBOL as symbol,
  recs.PATTERN_ID as pattern_id,
  recs.INTERVAL_MINUTES as interval_minutes,
  recs.as_of_ts as as_of_ts,
  recs.recs_total as recs_total,
  coalesce(o.outcomes_total, 0) as outcomes_total,
  coalesce(o.horizons_covered, 0) as horizons_covered,
  case when recs.recs_total > 0 and (recs.recs_total * 5) > 0
    then least(1.0, coalesce(o.outcomes_total, 0)::float / (recs.recs_total * 5))
    else 0.0 end as coverage_ratio,
  o.avg_outcome_h1 as avg_outcome_h1,
  o.avg_outcome_h3 as avg_outcome_h3,
  o.avg_outcome_h5 as avg_outcome_h5,
  o.avg_outcome_h10 as avg_outcome_h10,
  o.avg_outcome_h20 as avg_outcome_h20
from recs
left join outcomes_agg o
  on o.MARKET_TYPE = recs.MARKET_TYPE and o.SYMBOL = recs.SYMBOL
  and o.PATTERN_ID = recs.PATTERN_ID and o.INTERVAL_MINUTES = recs.INTERVAL_MINUTES
order by recs.MARKET_TYPE, recs.SYMBOL, recs.PATTERN_ID
"""

# Optional: fetch MIN_SIGNALS from TRAINING_GATE_PARAMS (one active row)
GATE_PARAMS_SQL = """
select MIN_SIGNALS
from MIP.APP.TRAINING_GATE_PARAMS
where IS_ACTIVE
qualify row_number() over (order by PARAM_SET) = 1
"""


def _get_min_signals(conn) -> int:
    """Return MIN_SIGNALS from TRAINING_GATE_PARAMS if present, else default."""
    cur = conn.cursor()
    try:
        cur.execute(GATE_PARAMS_SQL)
        row = cur.fetchone()
        if row and row[0] is not None:
            return int(row[0])
    except Exception:
        pass
    return DEFAULT_MIN_SIGNALS


@router.get("/status")
def get_training_status():
    """
    Training Status v1: per (market_type, symbol, pattern_id, interval_minutes) for daily (1440) only.
    Returns recs_total, outcomes_total, horizons_covered, coverage_ratio, avg_outcome_h1..h20,
    maturity_score (0–100), maturity_stage (INSUFFICIENT/WARMING_UP/LEARNING/CONFIDENT), reasons[].
    """
    conn = get_connection()
    try:
        min_signals = _get_min_signals(conn)
        cur = conn.cursor()
        cur.execute(TRAINING_STATUS_SQL)
        rows = fetch_all(cur)
        scored = apply_scoring_to_rows(rows, min_signals=min_signals)
        return {"rows": serialize_rows(scored)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    finally:
        conn.close()
