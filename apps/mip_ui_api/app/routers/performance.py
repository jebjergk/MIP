"""
Performance summary: outcomes-based stats per (market_type, symbol, pattern_id).
GET /performance/summary — items[] grouped by triple; optional filters; daily bars only.
GET /performance/suggestions — ranked symbol/pattern pairs with deterministic narratives.
Uses MIP.APP.RECOMMENDATION_LOG (daily bars only) + MIP.APP.RECOMMENDATION_OUTCOMES.
No writes.
"""
from collections import defaultdict
from fastapi import APIRouter, HTTPException, Query

from app.db import get_connection, fetch_all

router = APIRouter(prefix="/performance", tags=["performance"])

# Daily bars only (INTERVAL_MINUTES = 1440). Optional filters: pass None to skip.
# Recs per (market_type, symbol, pattern_id)
SUMMARY_RECS_SQL = """
select
  r.MARKET_TYPE as market_type,
  r.SYMBOL as symbol,
  r.PATTERN_ID as pattern_id,
  count(*) as recs_total,
  max(r.TS) as last_recommendation_ts
from MIP.APP.RECOMMENDATION_LOG r
where r.INTERVAL_MINUTES = 1440
  and (%(market_type)s is null or r.MARKET_TYPE = %(market_type)s)
  and (%(symbol)s is null or r.SYMBOL = %(symbol)s)
  and (%(pattern_id)s is null or r.PATTERN_ID = %(pattern_id)s)
group by r.MARKET_TYPE, r.SYMBOL, r.PATTERN_ID
order by r.MARKET_TYPE, r.SYMBOL, r.PATTERN_ID
"""

# Outcomes per (market_type, symbol, pattern_id, horizon_bars); daily only; optional filters
SUMMARY_BY_HORIZON_SQL = """
select
  r.MARKET_TYPE as market_type,
  r.SYMBOL as symbol,
  r.PATTERN_ID as pattern_id,
  o.HORIZON_BARS as horizon_bars,
  count(*) as n,
  avg(o.REALIZED_RETURN) as mean_outcome,
  sum(case when o.REALIZED_RETURN > 0 then 1 else 0 end)::float / nullif(count(*), 0) as pct_positive,
  min(o.REALIZED_RETURN) as min_outcome,
  max(o.REALIZED_RETURN) as max_outcome
from MIP.APP.RECOMMENDATION_LOG r
join MIP.APP.RECOMMENDATION_OUTCOMES o on o.RECOMMENDATION_ID = r.RECOMMENDATION_ID
where r.INTERVAL_MINUTES = 1440
  and (%(market_type)s is null or r.MARKET_TYPE = %(market_type)s)
  and (%(symbol)s is null or r.SYMBOL = %(symbol)s)
  and (%(pattern_id)s is null or r.PATTERN_ID = %(pattern_id)s)
  and o.EVAL_STATUS = 'SUCCESS'
  and o.REALIZED_RETURN is not null
group by r.MARKET_TYPE, r.SYMBOL, r.PATTERN_ID, o.HORIZON_BARS
order by r.MARKET_TYPE, r.SYMBOL, r.PATTERN_ID, o.HORIZON_BARS
"""


def _get(row: dict, *keys) -> any:
    """First value found for any of the keys (case-insensitive)."""
    for k in keys:
        if k in row and row[k] is not None:
            return row[k]
        k_upper = k.upper() if isinstance(k, str) else k
        if k_upper in row and row[k_upper] is not None:
            return row[k_upper]
    return None


def _serialize_by_horizon_row(row: dict) -> dict:
    """One by_horizon element: horizon_bars, n, mean_outcome, pct_positive, min_outcome, max_outcome."""
    hb = _get(row, "horizon_bars", "HORIZON_BARS")
    n = _get(row, "n", "N")
    mean_o = _get(row, "mean_outcome", "MEAN_OUTCOME")
    pct = _get(row, "pct_positive", "PCT_POSITIVE")
    min_o = _get(row, "min_outcome", "MIN_OUTCOME")
    max_o = _get(row, "max_outcome", "MAX_OUTCOME")
    out = {
        "horizon_bars": int(hb) if hb is not None else None,
        "n": int(n) if n is not None else None,
        "mean_outcome": float(mean_o) if mean_o is not None else None,
        "pct_positive": float(pct) if pct is not None else None,
        "min_outcome": float(min_o) if min_o is not None else None,
        "max_outcome": float(max_o) if max_o is not None else None,
    }
    return out


@router.get("/summary")
def get_performance_summary(
    market_type: str | None = Query(None, description="Market type (e.g. STOCK, ETF, FX)"),
    symbol: str | None = Query(None, description="Symbol (ticker)"),
    pattern_id: int | None = Query(None, description="Pattern ID"),
):
    """
    Outcomes-based performance summary. Items grouped by (market_type, symbol, pattern_id).
    Uses MIP.APP.RECOMMENDATION_LOG and MIP.APP.RECOMMENDATION_OUTCOMES only; daily bars (INTERVAL_MINUTES = 1440).
    Optional query params filter which triples are returned. No writes.
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

    # Key by (market_type, symbol, pattern_id)
    def key(r):
        return (
            _get(r, "market_type", "MARKET_TYPE"),
            _get(r, "symbol", "SYMBOL"),
            _get(r, "pattern_id", "PATTERN_ID"),
        )

    recs_by_key = {}
    for r in recs_rows:
        k = key(r)
        recs_by_key[k] = {
            "recs_total": int(_get(r, "recs_total", "RECS_TOTAL") or 0),
            "last_recommendation_ts": _get(r, "last_recommendation_ts", "LAST_RECOMMENDATION_TS"),
        }

    horizons_by_key = defaultdict(list)
    for r in horizon_rows:
        k = key(r)
        horizons_by_key[k].append(_serialize_by_horizon_row(r))

    items = []
    seen = set()
    for r in recs_rows:
        k = key(r)
        if k in seen:
            continue
        seen.add(k)
        mt, sy, pid = k
        recs_info = recs_by_key.get(k, {})
        recs_total = recs_info.get("recs_total", 0)
        last_ts = recs_info.get("last_recommendation_ts")
        by_horizon = horizons_by_key.get(k, [])
        by_horizon.sort(key=lambda h: (h["horizon_bars"] or 0))
        outcomes_total = sum((h.get("n") or 0) for h in by_horizon)
        horizons_covered = sorted({h["horizon_bars"] for h in by_horizon if h.get("horizon_bars") is not None})

        # Serialize timestamp for JSON
        if last_ts is not None and hasattr(last_ts, "isoformat"):
            last_ts = last_ts.isoformat()

        items.append({
            "market_type": mt,
            "symbol": sy,
            "pattern_id": pid,
            "recs_total": recs_total,
            "outcomes_total": outcomes_total,
            "horizons_covered": horizons_covered,
            "last_recommendation_ts": last_ts,
            "by_horizon": by_horizon,
        })
    items.sort(key=lambda x: (x["market_type"] or "", x["symbol"] or "", x["pattern_id"] or 0))
    return {"items": items}


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
