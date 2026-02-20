"""
Training Status: per (market_type, symbol, pattern_id, interval_minutes).
Supports both daily (1440) and intraday intervals via query parameter.
Uses MIP.APP.RECOMMENDATION_LOG, MIP.APP.RECOMMENDATION_OUTCOMES;
optional MIP.APP.PATTERN_DEFINITION (labels), MIP.APP.TRAINING_GATE_PARAMS (thresholds).
"""
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from app.config import training_debug_enabled
from app.db import get_connection, fetch_all, serialize_row, serialize_rows
from app.training_status import (
    _get_int,
    apply_scoring_to_rows,
    score_training_status_row_debug,
    DEFAULT_MIN_SIGNALS,
)
from app.training_timeline import build_training_timeline

router = APIRouter(prefix="/training", tags=["training"])


HORIZON_DEFS_CACHE: dict = {}


def _get_horizon_defs(conn, interval_minutes: int) -> list[dict]:
    """Fetch active horizon definitions for a given interval. Cached per interval."""
    if interval_minutes in HORIZON_DEFS_CACHE:
        return HORIZON_DEFS_CACHE[interval_minutes]
    cur = conn.cursor()
    cur.execute(
        "SELECT HORIZON_LENGTH, DISPLAY_SHORT, DISPLAY_LABEL, HORIZON_TYPE "
        "FROM MIP.APP.HORIZON_DEFINITION "
        "WHERE INTERVAL_MINUTES = %s AND IS_ACTIVE = TRUE "
        "ORDER BY HORIZON_ID",
        (interval_minutes,),
    )
    rows = fetch_all(cur)
    defs = [
        {
            "horizon_bars": int(r["HORIZON_LENGTH"]),
            "key": r["DISPLAY_SHORT"],
            "label": r["DISPLAY_LABEL"],
            "type": r["HORIZON_TYPE"],
        }
        for r in rows
    ]
    HORIZON_DEFS_CACHE[interval_minutes] = defs
    return defs


def _training_status_sql(interval_minutes: int = 1440, horizon_defs: list[dict] | None = None) -> str:
    if not horizon_defs:
        if interval_minutes == 15:
            horizon_defs = [
                {"horizon_bars": 1, "key": "H1"},
                {"horizon_bars": 4, "key": "H4"},
                {"horizon_bars": 8, "key": "H8"},
                {"horizon_bars": -1, "key": "EOD"},
            ]
        else:
            horizon_defs = [
                {"horizon_bars": 1, "key": "H1"},
                {"horizon_bars": 3, "key": "H3"},
                {"horizon_bars": 5, "key": "H5"},
                {"horizon_bars": 10, "key": "H10"},
                {"horizon_bars": 20, "key": "H20"},
            ]

    n_horizons = len(horizon_defs)

    avg_cols = ",\n    ".join(
        f"avg(case when o.HORIZON_BARS = {h['horizon_bars']} and o.EVAL_STATUS = 'SUCCESS' "
        f"then o.REALIZED_RETURN end) as avg_outcome_{h['key'].lower()}"
        for h in horizon_defs
    )

    select_cols = ",\n  ".join(
        f"o.avg_outcome_{h['key'].lower()} as avg_outcome_{h['key'].lower()}"
        for h in horizon_defs
    )

    return f"""
with recs as (
  select
    r.MARKET_TYPE,
    r.SYMBOL,
    r.PATTERN_ID,
    r.INTERVAL_MINUTES,
    count(*) as recs_total,
    max(r.TS) as as_of_ts
  from MIP.APP.RECOMMENDATION_LOG r
  where r.INTERVAL_MINUTES = {interval_minutes}
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
    {avg_cols}
  from MIP.APP.RECOMMENDATION_LOG r
  join MIP.APP.RECOMMENDATION_OUTCOMES o on o.RECOMMENDATION_ID = r.RECOMMENDATION_ID
  where r.INTERVAL_MINUTES = {interval_minutes}
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
  case when recs.recs_total > 0 and (recs.recs_total * {n_horizons}) > 0
    then least(1.0, coalesce(o.outcomes_total, 0)::float / (recs.recs_total * {n_horizons}))
    else 0.0 end as coverage_ratio,
  {select_cols}
from recs
left join outcomes_agg o
  on o.MARKET_TYPE = recs.MARKET_TYPE and o.SYMBOL = recs.SYMBOL
  and o.PATTERN_ID = recs.PATTERN_ID and o.INTERVAL_MINUTES = recs.INTERVAL_MINUTES
order by recs.MARKET_TYPE, recs.SYMBOL, recs.PATTERN_ID
"""


TRAINING_STATUS_SQL = _training_status_sql(1440)

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


@router.get("/horizons")
def get_horizon_definitions(
    interval_minutes: Optional[int] = Query(None, description="Filter by interval (15, 1440). Omit for all."),
):
    """Return active horizon definitions, optionally filtered by interval."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        if interval_minutes:
            cur.execute(
                "SELECT HORIZON_ID, HORIZON_TYPE, HORIZON_LENGTH, RESOLUTION, "
                "INTERVAL_MINUTES, DISPLAY_LABEL, DISPLAY_SHORT, DESCRIPTION "
                "FROM MIP.APP.HORIZON_DEFINITION WHERE IS_ACTIVE = TRUE AND INTERVAL_MINUTES = %s "
                "ORDER BY HORIZON_ID",
                (interval_minutes,),
            )
        else:
            cur.execute(
                "SELECT HORIZON_ID, HORIZON_TYPE, HORIZON_LENGTH, RESOLUTION, "
                "INTERVAL_MINUTES, DISPLAY_LABEL, DISPLAY_SHORT, DESCRIPTION "
                "FROM MIP.APP.HORIZON_DEFINITION WHERE IS_ACTIVE = TRUE "
                "ORDER BY HORIZON_ID",
            )
        return {"horizons": serialize_rows(fetch_all(cur))}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    finally:
        conn.close()


@router.get("/status")
def get_training_status(
    interval_minutes: Optional[int] = Query(None, description="Bar interval: 1440=daily (default), 15=intraday, etc."),
):
    """
    Training Status: per (market_type, symbol, pattern_id, interval_minutes).
    Returns recs_total, outcomes_total, horizons_covered, coverage_ratio,
    dynamic avg_outcome columns based on HORIZON_DEFINITION,
    maturity_score (0–100), maturity_stage, reasons[].
    """
    iv = interval_minutes if interval_minutes and interval_minutes in (15, 30, 60, 1440) else 1440
    conn = get_connection()
    try:
        hdefs = _get_horizon_defs(conn, iv)
        min_signals = _get_min_signals(conn)
        cur = conn.cursor()
        cur.execute(_training_status_sql(iv, hdefs))
        rows = fetch_all(cur)
        scored = apply_scoring_to_rows(rows, min_signals=min_signals, max_horizons=len(hdefs) or 5)
        return {
            "rows": serialize_rows(scored),
            "interval_minutes": iv,
            "horizon_definitions": hdefs,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    finally:
        conn.close()


@router.get("/status/debug")
def get_training_status_debug():
    """
    Dev-only: raw aggregated metrics before scoring + scoring inputs and computed
    maturity_score, maturity_stage, reasons. Enable with ENABLE_TRAINING_DEBUG=1.
    """
    if not training_debug_enabled():
        raise HTTPException(status_code=404, detail="Training debug not enabled")
    conn = get_connection()
    try:
        min_signals = _get_min_signals(conn)
        cur = conn.cursor()
        cur.execute(TRAINING_STATUS_SQL)
        rows = fetch_all(cur)
        out = []
        for r in rows:
            recs = _get_int(r, "recs_total")
            outcomes = _get_int(r, "outcomes_total")
            horizons = _get_int(r, "horizons_covered")
            raw = {
                "market_type": r.get("market_type") or r.get("MARKET_TYPE"),
                "symbol": r.get("symbol") or r.get("SYMBOL"),
                "pattern_id": r.get("pattern_id") or r.get("PATTERN_ID"),
                "interval_minutes": r.get("interval_minutes") or r.get("INTERVAL_MINUTES"),
                "as_of_ts": r.get("as_of_ts") or r.get("AS_OF_TS"),
                "recs_total": recs,
                "outcomes_total": outcomes,
                "horizons_covered": horizons,
                "coverage_ratio": r.get("coverage_ratio") or r.get("COVERAGE_RATIO"),
                "avg_outcome_h1": r.get("avg_outcome_h1") or r.get("AVG_OUTCOME_H1"),
                "avg_outcome_h3": r.get("avg_outcome_h3") or r.get("AVG_OUTCOME_H3"),
                "avg_outcome_h5": r.get("avg_outcome_h5") or r.get("AVG_OUTCOME_H5"),
                "avg_outcome_h10": r.get("avg_outcome_h10") or r.get("AVG_OUTCOME_H10"),
                "avg_outcome_h20": r.get("avg_outcome_h20") or r.get("AVG_OUTCOME_H20"),
            }
            scoring = score_training_status_row_debug(recs, outcomes, horizons, min_signals)
            out.append({"raw": serialize_row(raw), "scoring": scoring})
        return {"min_signals": min_signals, "rows": out}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    finally:
        conn.close()


@router.get("/timeline")
def get_training_timeline(
    symbol: str = Query(..., description="Symbol to query"),
    market_type: str = Query(..., description="Market type (STOCK, ETF, FX)"),
    pattern_id: Optional[int] = Query(None, description="Pattern ID (optional, defaults to pattern 1)"),
    horizon_bars: Optional[int] = Query(None, description="Horizon bars (default 5)"),
    rolling_window: Optional[int] = Query(None, description="Rolling window size for hit rate (default 20)"),
    max_points: Optional[int] = Query(None, description="Max points to return (default 250)"),
    interval_minutes: Optional[int] = Query(None, description="Interval minutes (default 1440 for daily)"),
):
    """
    Training Timeline: time series of rolling hit rate and state transitions for a symbol.
    
    Returns confidence over time (evidence accumulation), derived from evaluated outcomes.
    Includes narrative bullets explaining key turning points.
    """
    conn = get_connection()
    try:
        result = build_training_timeline(
            conn,
            symbol=symbol,
            market_type=market_type,
            pattern_id=pattern_id or 1,
            horizon_bars=horizon_bars or 5,
            rolling_window=rolling_window or 20,
            max_points=max_points or 250,
            interval_minutes=interval_minutes or 1440,
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Intraday-specific endpoints (read from mart views deployed in Phase 1a)
# ---------------------------------------------------------------------------

@router.get("/intraday/pipeline-status")
def get_intraday_pipeline_status():
    """Operational health of the intraday pipeline: latest run, data coverage, compute usage."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM MIP.MART.V_INTRADAY_PIPELINE_STATUS")
        rows = fetch_all(cur)
        return serialize_row(rows[0]) if rows else {}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    finally:
        conn.close()


@router.get("/intraday/trust-scoreboard")
def get_intraday_trust_scoreboard():
    """Fee-adjusted pattern trust scores for intraday patterns."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT * FROM MIP.MART.V_INTRADAY_TRUST_SCOREBOARD
            ORDER BY TRUST_STATUS DESC, NET_HIT_RATE DESC NULLS LAST
        """)
        rows = fetch_all(cur)
        return {"rows": serialize_rows(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    finally:
        conn.close()


@router.get("/intraday/pattern-stability")
def get_intraday_pattern_stability():
    """Rolling-window stability for intraday patterns (detecting drift or improvement)."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT * FROM MIP.MART.V_INTRADAY_PATTERN_STABILITY
            ORDER BY PATTERN_NAME, HORIZON_BARS
        """)
        rows = fetch_all(cur)
        return {"rows": serialize_rows(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    finally:
        conn.close()


@router.get("/intraday/signal-chart")
def get_intraday_signal_chart(
    symbol: str = Query(..., description="Symbol"),
    market_type: str = Query(..., description="Market type (STOCK, ETF, FX)"),
    days: int = Query(5, description="Number of days of bar history"),
):
    """Price bars + signal markers for a symbol, used by the intraday signal chart."""
    import json as _json

    conn = get_connection()
    try:
        cur = conn.cursor()

        bars_sql = """
        SELECT TS, OPEN, HIGH, LOW, CLOSE, VOLUME
        FROM MIP.MART.MARKET_BARS
        WHERE SYMBOL = %s AND MARKET_TYPE = %s AND INTERVAL_MINUTES = 15
          AND TS >= dateadd(day, -%s, current_timestamp())
        ORDER BY TS
        """
        cur.execute(bars_sql, (symbol, market_type, days))
        bars = fetch_all(cur)

        signals_sql = """
        SELECT
            r.TS,
            r.PATTERN_ID,
            p.NAME AS PATTERN_NAME,
            p.PATTERN_TYPE,
            r.SCORE,
            r.DETAILS
        FROM MIP.APP.RECOMMENDATION_LOG r
        LEFT JOIN MIP.APP.PATTERN_DEFINITION p ON p.PATTERN_ID = r.PATTERN_ID
        WHERE r.SYMBOL = %s AND r.MARKET_TYPE = %s AND r.INTERVAL_MINUTES = 15
          AND r.TS >= dateadd(day, -%s, current_timestamp())
        ORDER BY r.TS
        """
        cur.execute(signals_sql, (symbol, market_type, days))
        signals = fetch_all(cur)

        sig_rows = serialize_rows(signals)
        for row in sig_rows:
            raw = row.get("DETAILS")
            if isinstance(raw, str):
                try:
                    row["DETAILS"] = _json.loads(raw)
                except Exception:
                    pass

        symbols_sql = """
        SELECT DISTINCT SYMBOL, MARKET_TYPE
        FROM MIP.APP.RECOMMENDATION_LOG
        WHERE INTERVAL_MINUTES = 15
        ORDER BY MARKET_TYPE, SYMBOL
        """
        cur.execute(symbols_sql)
        available = fetch_all(cur)

        return {
            "symbol": symbol,
            "market_type": market_type,
            "bars": serialize_rows(bars),
            "signals": sig_rows,
            "available_symbols": serialize_rows(available),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    finally:
        conn.close()


@router.get("/intraday/excursion-stats")
def get_intraday_excursion_stats():
    """Max favorable/adverse excursion stats per intraday pattern (stop-loss/take-profit design)."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT * FROM MIP.MART.V_INTRADAY_EXCURSION_STATS
            ORDER BY PATTERN_NAME, HORIZON_BARS
        """)
        rows = fetch_all(cur)
        return {"rows": serialize_rows(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    finally:
        conn.close()
