"""
Performance summary: outcomes-based stats per (market_type, symbol, pattern_id).
GET /performance/summary — items[] grouped by triple; optional filters; daily bars only.
Uses MIP.APP.RECOMMENDATION_LOG (rl) + MIP.APP.RECOMMENDATION_OUTCOMES (ro).
EVAL_STATUS = 'COMPLETED'; REALIZED_RETURN; null-safe HIT_FLAG. No writes.
Canonical SQL: docs/ux/72_UX_QUERIES.md.
"""
from collections import defaultdict
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.db import get_connection, fetch_all

router = APIRouter(prefix="/performance", tags=["performance"])


# --- Response models (Pydantic) ---
class ByHorizonItem(BaseModel):
    horizon_bars: int
    n: int
    mean_realized_return: float | None = None
    pct_positive: float | None = None
    pct_hit: float | None = None
    min_realized_return: float | None = None
    max_realized_return: float | None = None


class SummaryItem(BaseModel):
    market_type: str | None = None
    symbol: str | None = None
    pattern_id: int | None = None
    interval_minutes: int | None = Field(default=1440, description="Bar interval; 1440 = daily")
    recs_total: int = 0
    outcomes_total: int = 0
    horizons_covered: int = 0
    last_recommendation_ts: str | None = None
    by_horizon: list[ByHorizonItem] = Field(default_factory=list)


class SummaryResponse(BaseModel):
    items: list[SummaryItem] = Field(default_factory=list)


# Daily bars only (INTERVAL_MINUTES = 1440). Optional filters: pass None to skip.
# Triple-level: recs_total = count(distinct RECOMMENDATION_ID), last_recommendation_ts, interval_minutes
SUMMARY_RECS_SQL = """
select
  rl.MARKET_TYPE as market_type,
  rl.SYMBOL as symbol,
  rl.PATTERN_ID as pattern_id,
  rl.INTERVAL_MINUTES as interval_minutes,
  count(distinct rl.RECOMMENDATION_ID) as recs_total,
  max(rl.TS) as last_recommendation_ts
from MIP.APP.RECOMMENDATION_LOG rl
where rl.INTERVAL_MINUTES = 1440
  and (%(market_type)s is null or rl.MARKET_TYPE = %(market_type)s)
  and (%(symbol)s is null or rl.SYMBOL = %(symbol)s)
  and (%(pattern_id)s is null or rl.PATTERN_ID = %(pattern_id)s)
group by rl.MARKET_TYPE, rl.SYMBOL, rl.PATTERN_ID, rl.INTERVAL_MINUTES
order by rl.MARKET_TYPE, rl.SYMBOL, rl.PATTERN_ID
"""

# By-horizon: EVAL_STATUS = 'COMPLETED'; mean_realized_return, pct_positive, pct_hit (null-safe), min/max
SUMMARY_BY_HORIZON_SQL = """
select
  rl.MARKET_TYPE as market_type,
  rl.SYMBOL as symbol,
  rl.PATTERN_ID as pattern_id,
  ro.HORIZON_BARS as horizon_bars,
  count(*) as n,
  avg(ro.REALIZED_RETURN) as mean_realized_return,
  avg(case when ro.REALIZED_RETURN > 0 then 1 else 0 end) as pct_positive,
  avg(case when coalesce(ro.HIT_FLAG, false) then 1 else 0 end) as pct_hit,
  min(ro.REALIZED_RETURN) as min_realized_return,
  max(ro.REALIZED_RETURN) as max_realized_return
from MIP.APP.RECOMMENDATION_LOG rl
join MIP.APP.RECOMMENDATION_OUTCOMES ro on ro.RECOMMENDATION_ID = rl.RECOMMENDATION_ID
where rl.INTERVAL_MINUTES = 1440
  and (%(market_type)s is null or rl.MARKET_TYPE = %(market_type)s)
  and (%(symbol)s is null or rl.SYMBOL = %(symbol)s)
  and (%(pattern_id)s is null or rl.PATTERN_ID = %(pattern_id)s)
  and ro.EVAL_STATUS = 'COMPLETED'
group by rl.MARKET_TYPE, rl.SYMBOL, rl.PATTERN_ID, ro.HORIZON_BARS
order by rl.MARKET_TYPE, rl.SYMBOL, rl.PATTERN_ID, ro.HORIZON_BARS
"""


def _get(row: dict, *keys: str) -> Any:
    """First value found for any of the keys (case-insensitive)."""
    for k in keys:
        if k in row and row[k] is not None:
            return row[k]
        k_upper = k.upper() if isinstance(k, str) else k
        if k_upper in row and row[k_upper] is not None:
            return row[k_upper]
    return None


def _serialize_by_horizon_row(row: dict) -> dict:
    """One by_horizon element: horizon_bars, n, mean_realized_return, pct_positive, pct_hit, min/max_realized_return."""
    hb = _get(row, "horizon_bars", "HORIZON_BARS")
    n = _get(row, "n", "N")
    mean_r = _get(row, "mean_realized_return", "MEAN_REALIZED_RETURN")
    pct_pos = _get(row, "pct_positive", "PCT_POSITIVE")
    pct_hit = _get(row, "pct_hit", "PCT_HIT")
    min_r = _get(row, "min_realized_return", "MIN_REALIZED_RETURN")
    max_r = _get(row, "max_realized_return", "MAX_REALIZED_RETURN")
    return {
        "horizon_bars": int(hb) if hb is not None else None,
        "n": int(n) if n is not None else None,
        "mean_realized_return": float(mean_r) if mean_r is not None else None,
        "pct_positive": float(pct_pos) if pct_pos is not None else None,
        "pct_hit": float(pct_hit) if pct_hit is not None else None,
        "min_realized_return": float(min_r) if min_r is not None else None,
        "max_realized_return": float(max_r) if max_r is not None else None,
    }


@router.get("/summary", response_model=SummaryResponse)
def get_performance_summary(
    market_type: str | None = Query(None, description="Market type (e.g. STOCK, ETF, FX)"),
    symbol: str | None = Query(None, description="Symbol (ticker)"),
    pattern_id: int | None = Query(None, description="Pattern ID"),
):
    """
    Outcomes-based performance summary using REALIZED_RETURN.
    Items grouped by (market_type, symbol, pattern_id, interval_minutes).
    Filter: rl.INTERVAL_MINUTES = 1440; ro.EVAL_STATUS = 'COMPLETED'; null-safe HIT_FLAG.
    Optional query params. No writes. Canonical SQL: docs/ux/72_UX_QUERIES.md.
    """
    params = {"market_type": market_type, "symbol": symbol, "pattern_id": pattern_id}
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(SUMMARY_RECS_SQL, params)
        recs_rows = fetch_all(cur)
        cur.execute(SUMMARY_BY_HORIZON_SQL, params)
        horizon_rows = fetch_all(cur)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    finally:
        conn.close()

    # Key by (market_type, symbol, pattern_id, interval_minutes)
    def key(r):
        return (
            _get(r, "market_type", "MARKET_TYPE"),
            _get(r, "symbol", "SYMBOL"),
            _get(r, "pattern_id", "PATTERN_ID"),
            _get(r, "interval_minutes", "INTERVAL_MINUTES"),
        )

    recs_by_key = {}
    for r in recs_rows:
        k = key(r)
        recs_by_key[k] = {
            "recs_total": int(_get(r, "recs_total", "RECS_TOTAL") or 0),
            "last_recommendation_ts": _get(r, "last_recommendation_ts", "LAST_RECOMMENDATION_TS"),
            "interval_minutes": _get(r, "interval_minutes", "INTERVAL_MINUTES"),
        }

    horizons_by_key: dict[tuple, list] = defaultdict(list)
    for r in horizon_rows:
        k = key(r)
        horizons_by_key[k].append(_serialize_by_horizon_row(r))

    items: list[SummaryItem] = []
    seen: set[tuple] = set()
    for r in recs_rows:
        k = key(r)
        if k in seen:
            continue
        seen.add(k)
        mt, sy, pid, interval_min = k
        recs_info = recs_by_key.get(k, {})
        recs_total = recs_info.get("recs_total", 0)
        last_ts = recs_info.get("last_recommendation_ts")
        interval_minutes = recs_info.get("interval_minutes")
        if interval_minutes is not None:
            try:
                interval_minutes = int(interval_minutes)
            except (TypeError, ValueError):
                interval_minutes = 1440
        else:
            interval_minutes = 1440
        by_horizon_raw = horizons_by_key.get(k, [])
        by_horizon_raw.sort(key=lambda h: (h.get("horizon_bars") or 0))
        outcomes_total = sum((h.get("n") or 0) for h in by_horizon_raw)
        horizons_covered = len({h["horizon_bars"] for h in by_horizon_raw if h.get("horizon_bars") is not None})

        if last_ts is not None and hasattr(last_ts, "isoformat"):
            last_ts = last_ts.isoformat()

        by_horizon = [
            ByHorizonItem(
                horizon_bars=h["horizon_bars"],
                n=h.get("n") or 0,
                mean_realized_return=h.get("mean_realized_return"),
                pct_positive=h.get("pct_positive"),
                pct_hit=h.get("pct_hit"),
                min_realized_return=h.get("min_realized_return"),
                max_realized_return=h.get("max_realized_return"),
            )
            for h in by_horizon_raw
            if h.get("horizon_bars") is not None
        ]

        items.append(
            SummaryItem(
                market_type=mt,
                symbol=sy,
                pattern_id=pid,
                interval_minutes=interval_minutes,
                recs_total=recs_total,
                outcomes_total=outcomes_total,
                horizons_covered=horizons_covered,
                last_recommendation_ts=last_ts,
                by_horizon=by_horizon,
            )
        )
    items.sort(key=lambda x: (x.market_type or "", x.symbol or "", x.pattern_id or 0))
    return SummaryResponse(items=items)


# --- Distribution: bounded array of REALIZED_RETURN for histogram (daily only; COMPLETED only) ---
# Rules: rl.INTERVAL_MINUTES = 1440, ro.EVAL_STATUS = 'COMPLETED'; filter by market_type, symbol, pattern_id, horizon_bars.
# Order: ro.CALCULATED_AT desc (stable recency), then limit.
DISTRIBUTION_SQL = """
select ro.REALIZED_RETURN
from MIP.APP.RECOMMENDATION_LOG rl
join MIP.APP.RECOMMENDATION_OUTCOMES ro on ro.RECOMMENDATION_ID = rl.RECOMMENDATION_ID
where rl.INTERVAL_MINUTES = 1440
  and rl.MARKET_TYPE = %(market_type)s
  and rl.SYMBOL = %(symbol)s
  and rl.PATTERN_ID = %(pattern_id)s
  and ro.HORIZON_BARS = %(horizon_bars)s
  and ro.EVAL_STATUS = 'COMPLETED'
  and ro.REALIZED_RETURN is not null
order by ro.CALCULATED_AT desc
limit %(limit)s
"""


class DistributionResponse(BaseModel):
    market_type: str
    symbol: str
    pattern_id: int
    horizon_bars: int
    n: int
    realized_returns: list[float] = Field(default_factory=list, description="Bounded array of REALIZED_RETURN (decimal, e.g. 0.02 = 2%)")


@router.get("/distribution", response_model=DistributionResponse)
def get_performance_distribution(
    market_type: str = Query(..., description="Market type (e.g. STOCK, ETF, FX)"),
    symbol: str = Query(..., description="Symbol (ticker)"),
    pattern_id: int = Query(..., description="Pattern ID"),
    horizon_bars: int = Query(..., description="Horizon in bars (e.g. 1, 3, 5, 10, 20)"),
    limit: int = Query(2000, ge=1, le=5000, description="Max number of points (default 2000, max 5000)"),
):
    """
    Bounded array of REALIZED_RETURN values for (market_type, symbol, pattern_id, horizon_bars).
    Daily bars only; EVAL_STATUS = 'COMPLETED'. Order: CALCULATED_AT desc. Used for distribution/histogram charts.
    For an item from /performance/summary, returns a non-empty array for at least one horizon when data exists.
    """
    params = {
        "market_type": market_type,
        "symbol": symbol,
        "pattern_id": pattern_id,
        "horizon_bars": horizon_bars,
        "limit": limit,
    }
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(DISTRIBUTION_SQL, params)
        rows = fetch_all(cur)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    finally:
        conn.close()

    realized_returns: list[float] = []
    for r in rows:
        v = _get(r, "REALIZED_RETURN", "realized_return")
        if v is not None:
            try:
                realized_returns.append(float(v))
            except (TypeError, ValueError):
                pass
    return DistributionResponse(
        market_type=market_type,
        symbol=symbol,
        pattern_id=pattern_id,
        horizon_bars=horizon_bars,
        n=len(realized_returns),
        realized_returns=realized_returns,
    )


# --- Suggestions: all pairs with per-horizon outcomes (daily only) ---
SUGGESTIONS_RECS_SQL = """
select
  r.MARKET_TYPE as market_type,
  r.SYMBOL as symbol,
  r.PATTERN_ID as pattern_id,
  count(*) as n_recs
from MIP.APP.RECOMMENDATION_LOG r
where r.INTERVAL_MINUTES = 1440
group by r.MARKET_TYPE, r.SYMBOL, r.PATTERN_ID
"""

SUGGESTIONS_HORIZONS_SQL = """
select
  r.MARKET_TYPE as market_type,
  r.SYMBOL as symbol,
  r.PATTERN_ID as pattern_id,
  o.HORIZON_BARS as horizon_bars,
  count(*) as n_outcomes,
  avg(o.REALIZED_RETURN) as mean_outcome,
  sum(case when o.REALIZED_RETURN > 0 then 1 else 0 end)::float / nullif(count(*), 0) as pct_positive,
  min(o.REALIZED_RETURN) as min_outcome,
  max(o.REALIZED_RETURN) as max_outcome
from MIP.APP.RECOMMENDATION_LOG r
join MIP.APP.RECOMMENDATION_OUTCOMES o on o.RECOMMENDATION_ID = r.RECOMMENDATION_ID
where r.INTERVAL_MINUTES = 1440
  and o.EVAL_STATUS = 'SUCCESS'
  and o.REALIZED_RETURN is not null
group by r.MARKET_TYPE, r.SYMBOL, r.PATTERN_ID, o.HORIZON_BARS
"""


def _float(v):
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _build_suggestion_explanation(
    n_outcomes: int,
    pct_positive: float | None,
    best_horizon_bars: int | None,
    best_mean: float | None,
    best_pct: float | None,
) -> str:
    """Plain English template (no LLM)."""
    n_str = str(n_outcomes) if n_outcomes is not None else "N"
    pct_str = f"{pct_positive * 100:.1f}%" if pct_positive is not None else "—"
    part1 = f"Based on {n_str} evaluated signals, outcomes were positive {pct_str} of the time."
    if best_horizon_bars is not None and best_mean is not None:
        mean_pct = f"{best_mean * 100:.2f}%"
        part2 = f" Strongest at horizon {best_horizon_bars} bars (mean return {mean_pct})."
    else:
        part2 = ""
    return part1 + part2


def _rank_score(n_outcomes: int, pct_positive: float | None, mean_outcome: float | None) -> float:
    """Simple deterministic score: favor more outcomes and positive win rate + mean."""
    if n_outcomes <= 0:
        return 0.0
    pct = pct_positive if pct_positive is not None else 0.0
    mean = mean_outcome if mean_outcome is not None else 0.0
    return n_outcomes * (0.5 + 0.4 * pct + 0.1 * max(0, mean) * 10)


@router.get("/suggestions")
def get_performance_suggestions(
    min_sample: int = Query(10, ge=1, le=500, description="Minimum number of outcomes to include"),
):
    """
    Ranked symbol/pattern pairs from outcomes (daily bars only).
    Returns deterministic rank score, metrics per horizon, and plain-English explanation per row.
    Research guidance only; no execution.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(SUGGESTIONS_RECS_SQL)
        recs_rows = fetch_all(cur)
        cur.execute(SUGGESTIONS_HORIZONS_SQL)
        horizon_rows = fetch_all(cur)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    finally:
        conn.close()

    n_recs_by_key = {}
    for r in recs_rows:
        key = (r.get("market_type") or r.get("MARKET_TYPE"), r.get("symbol") or r.get("SYMBOL"), r.get("pattern_id") or r.get("PATTERN_ID"))
        n_recs_by_key[key] = int(r.get("n_recs") or r.get("N_RECS") or 0)

    def _int(v):
        if v is None:
            return None
        try:
            return int(v)
        except (TypeError, ValueError):
            return None

    horizons_by_key = defaultdict(list)
    for r in horizon_rows:
        key = (r.get("market_type") or r.get("MARKET_TYPE"), r.get("symbol") or r.get("SYMBOL"), r.get("pattern_id") or r.get("PATTERN_ID"))
        horizons_by_key[key].append({
            "horizon_bars": _int(r.get("horizon_bars") or r.get("HORIZON_BARS")),
            "n_outcomes": _int(r.get("n_outcomes") or r.get("N_OUTCOMES")),
            "mean_outcome": _float(r.get("mean_outcome") or r.get("MEAN_OUTCOME")),
            "pct_positive": _float(r.get("pct_positive") or r.get("PCT_POSITIVE")),
            "min_outcome": _float(r.get("min_outcome") or r.get("MIN_OUTCOME")),
            "max_outcome": _float(r.get("max_outcome") or r.get("MAX_OUTCOME")),
        })

    suggestions = []
    for key, horizons in horizons_by_key.items():
        market_type, symbol, pattern_id = key
        n_recs = n_recs_by_key.get(key, 0)
        n_outcomes = sum(h.get("n_outcomes") or 0 for h in horizons)
        if n_outcomes < min_sample:
            continue
        best = None
        best_mean = None
        for h in horizons:
            mean_val = h.get("mean_outcome")
            if mean_val is not None and (best_mean is None or mean_val > best_mean):
                best_mean = mean_val
                best = h
        pct_positive = None
        if horizons:
            total_n = sum(h.get("n_outcomes") or 0 for h in horizons)
            if total_n > 0:
                weighted = sum((h.get("pct_positive") or 0) * (h.get("n_outcomes") or 0) for h in horizons) / total_n
                pct_positive = weighted
        best_horizon_bars = best.get("horizon_bars") if best else None
        best_pct = best.get("pct_positive") if best else None
        score = _rank_score(n_outcomes, pct_positive, best_mean)
        explanation = _build_suggestion_explanation(n_outcomes, pct_positive, best_horizon_bars, best_mean, best_pct)
        suggestions.append({
            "market_type": market_type,
            "symbol": symbol,
            "pattern_id": pattern_id,
            "n_recs": n_recs,
            "n_outcomes": n_outcomes,
            "rank_score": round(score, 2),
            "pct_positive": round(pct_positive * 100, 1) if pct_positive is not None else None,
            "best_horizon_bars": best_horizon_bars,
            "best_mean_outcome": round(best_mean * 100, 2) if best_mean is not None else None,
            "explanation": explanation,
            "horizons": horizons,
        })
    suggestions.sort(key=lambda x: (-x["rank_score"], -x["n_outcomes"]))
    return {"min_sample": min_sample, "suggestions": suggestions}
