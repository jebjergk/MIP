"""
Training Timeline: time series of rolling hit rate and state transitions.

Derives confidence over time from evaluated outcomes.
State classification mirrors MIP.APP.TRAINING_GATE_PARAMS thresholds.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from app.db import fetch_all


# Default thresholds (mirroring TRAINING_GATE_PARAMS)
DEFAULT_MIN_SIGNALS = 40
DEFAULT_MIN_SIGNALS_BOOTSTRAP = 5
DEFAULT_MIN_HIT_RATE = 0.55
DEFAULT_MIN_AVG_RETURN = 0.0005


# SQL to fetch gate params
GATE_PARAMS_SQL = """
select
    MIN_SIGNALS,
    MIN_SIGNALS_BOOTSTRAP,
    MIN_HIT_RATE,
    MIN_AVG_RETURN
from MIP.APP.TRAINING_GATE_PARAMS
where IS_ACTIVE
qualify row_number() over (order by PARAM_SET) = 1
"""


# SQL to fetch evaluated outcomes time series
# Returns one row per evaluated recommendation at the specified horizon
TIMELINE_SERIES_SQL = """
with base as (
    select
        r.RECOMMENDATION_ID,
        r.TS as signal_ts,
        r.GENERATED_AT,
        o.ENTRY_TS,
        o.EXIT_TS,
        o.REALIZED_RETURN,
        o.HIT_FLAG,
        o.EVAL_STATUS,
        o.CALCULATED_AT
    from MIP.APP.RECOMMENDATION_LOG r
    join MIP.APP.RECOMMENDATION_OUTCOMES o
      on o.RECOMMENDATION_ID = r.RECOMMENDATION_ID
    where r.SYMBOL = %(symbol)s
      and r.MARKET_TYPE = %(market_type)s
      and r.PATTERN_ID = %(pattern_id)s
      and r.INTERVAL_MINUTES = 1440
      and o.HORIZON_BARS = %(horizon_bars)s
      and o.EVAL_STATUS = 'SUCCESS'
      and o.REALIZED_RETURN is not null
),
ordered as (
    select *,
        row_number() over (order by signal_ts, RECOMMENDATION_ID) as rn
    from base
)
select
    signal_ts,
    ENTRY_TS,
    EXIT_TS,
    REALIZED_RETURN,
    HIT_FLAG,
    rn as evaluated_count,
    CALCULATED_AT
from ordered
order by rn
"""


# SQL to count pending evaluations (waiting for future bars)
PENDING_EVALUATIONS_SQL = """
select 
    count(*) as pending_count,
    min(r.TS) as oldest_pending,
    max(r.TS) as newest_pending
from MIP.APP.RECOMMENDATION_LOG r
join MIP.APP.RECOMMENDATION_OUTCOMES o
  on o.RECOMMENDATION_ID = r.RECOMMENDATION_ID
where r.SYMBOL = %(symbol)s
  and r.MARKET_TYPE = %(market_type)s
  and r.PATTERN_ID = %(pattern_id)s
  and r.INTERVAL_MINUTES = 1440
  and o.HORIZON_BARS = %(horizon_bars)s
  and o.EVAL_STATUS = 'INSUFFICIENT_FUTURE_DATA'
"""


# SQL to get first signal date (even if not yet evaluated)
FIRST_SIGNAL_SQL = """
select min(TS) as first_signal_ts
from MIP.APP.RECOMMENDATION_LOG
where SYMBOL = %(symbol)s
  and MARKET_TYPE = %(market_type)s
  and PATTERN_ID = %(pattern_id)s
  and INTERVAL_MINUTES = 1440
"""


# SQL to get pattern-level trust status (aggregated across all symbols)
PATTERN_TRUST_SQL = """
select
    PATTERN_ID,
    MARKET_TYPE,
    INTERVAL_MINUTES,
    HORIZON_BARS,
    N_SIGNALS,
    HIT_RATE_SUCCESS,
    AVG_RETURN_SUCCESS,
    CONFIDENCE
from MIP.MART.V_TRUSTED_PATTERN_HORIZONS
where PATTERN_ID = %(pattern_id)s
  and MARKET_TYPE = %(market_type)s
  and INTERVAL_MINUTES = 1440
  and HORIZON_BARS = %(horizon_bars)s
"""


# SQL to get pattern stats aggregated across ALL symbols
PATTERN_AGGREGATE_SQL = """
select
    count(*) as total_outcomes,
    sum(case when HIT_FLAG then 1 else 0 end) as total_hits,
    avg(case when EVAL_STATUS = 'SUCCESS' then REALIZED_RETURN end) as avg_return
from MIP.APP.RECOMMENDATION_OUTCOMES o
join MIP.APP.RECOMMENDATION_LOG r on r.RECOMMENDATION_ID = o.RECOMMENDATION_ID
where r.PATTERN_ID = %(pattern_id)s
  and r.MARKET_TYPE = %(market_type)s
  and r.INTERVAL_MINUTES = 1440
  and o.HORIZON_BARS = %(horizon_bars)s
  and o.EVAL_STATUS = 'SUCCESS'
"""


@dataclass
class GateParams:
    """Thresholds from TRAINING_GATE_PARAMS."""
    min_signals: int
    min_signals_bootstrap: int
    min_hit_rate: float
    min_avg_return: float


def get_gate_params(conn) -> GateParams:
    """Fetch active gate params from Snowflake, with defaults."""
    cur = conn.cursor()
    try:
        cur.execute(GATE_PARAMS_SQL)
        row = cur.fetchone()
        if row:
            return GateParams(
                min_signals=int(row[0]) if row[0] is not None else DEFAULT_MIN_SIGNALS,
                min_signals_bootstrap=int(row[1]) if row[1] is not None else DEFAULT_MIN_SIGNALS_BOOTSTRAP,
                min_hit_rate=float(row[2]) if row[2] is not None else DEFAULT_MIN_HIT_RATE,
                min_avg_return=float(row[3]) if row[3] is not None else DEFAULT_MIN_AVG_RETURN,
            )
    except Exception:
        pass
    return GateParams(
        min_signals=DEFAULT_MIN_SIGNALS,
        min_signals_bootstrap=DEFAULT_MIN_SIGNALS_BOOTSTRAP,
        min_hit_rate=DEFAULT_MIN_HIT_RATE,
        min_avg_return=DEFAULT_MIN_AVG_RETURN,
    )


def classify_state(
    evaluated_count: int,
    rolling_hit_rate: float | None,
    rolling_avg_return: float | None,
    params: GateParams,
) -> str:
    """
    Classify state based on thresholds.
    
    Mirrors the logic from V_TRUSTED_SIGNAL_POLICY:
    - TRUSTED: N_SUCCESS >= min_signals, hit_rate >= min_hit_rate, avg_return > min_avg_return
    - WATCH: N_SUCCESS >= min_signals_bootstrap (but not yet trusted)
    - UNTRUSTED: otherwise
    """
    if evaluated_count < params.min_signals_bootstrap:
        return "UNTRUSTED"
    
    # Check if we meet trusted criteria
    if (
        evaluated_count >= params.min_signals
        and rolling_hit_rate is not None
        and rolling_hit_rate >= params.min_hit_rate
        and rolling_avg_return is not None
        and rolling_avg_return > params.min_avg_return
    ):
        return "TRUSTED"
    
    # We have bootstrap signals but not trusted -> WATCH
    if evaluated_count >= params.min_signals_bootstrap:
        return "WATCH"
    
    return "UNTRUSTED"


def compute_rolling_stats(
    returns: list[float],
    hits: list[bool],
    window: int,
) -> tuple[float | None, float | None]:
    """
    Compute rolling hit rate and avg return over the last `window` items.
    Returns (rolling_hit_rate, rolling_avg_return).
    """
    if not returns or not hits:
        return None, None
    
    # Use min of window and available data
    n = min(window, len(returns))
    recent_returns = returns[-n:]
    recent_hits = hits[-n:]
    
    hit_count = sum(1 for h in recent_hits if h)
    rolling_hit_rate = hit_count / n if n > 0 else None
    rolling_avg_return = sum(recent_returns) / n if n > 0 else None
    
    return rolling_hit_rate, rolling_avg_return


def detect_event(
    i: int,
    evaluated_count: int,
    prev_state: str | None,
    curr_state: str,
    params: GateParams,
    recent_misses: int,
) -> str | None:
    """Detect special events for narrative markers."""
    if i == 0:
        return "FIRST_OUTCOME"
    
    # Crossed minimum signals threshold
    if evaluated_count == params.min_signals:
        return "MIN_SIGNALS_REACHED"
    
    # State transitions
    if prev_state != curr_state:
        if curr_state == "TRUSTED":
            return "ENTERED_TRUSTED"
        elif curr_state == "WATCH" and prev_state == "UNTRUSTED":
            return "ENTERED_WATCH"
        elif curr_state == "WATCH" and prev_state == "TRUSTED":
            return "DROPPED_FROM_TRUSTED"
    
    # Streak of misses (3+)
    if recent_misses >= 3:
        return "MISS_STREAK"
    
    return None


def generate_narrative(
    series: list[dict[str, Any]],
    first_signal_ts: datetime | None,
    params: GateParams,
    symbol: str,
) -> list[str]:
    """
    Generate 3-6 narrative bullets explaining the training journey.
    """
    if not series:
        return [f"No evaluated outcomes yet for {symbol} — still observing."]
    
    bullets: list[str] = []
    
    # 1. First signal date
    if first_signal_ts:
        bullets.append(f"Observing since {str(first_signal_ts)[:10]} (first signal).")
    
    # 2. First evaluated outcome
    first_outcome = series[0]
    if first_outcome.get("ts"):
        bullets.append(f"First outcomes evaluated on {str(first_outcome['ts'])[:10]}.")
    
    # 3. Find when min_signals was reached
    min_signals_point = None
    for pt in series:
        if pt.get("evaluated_count", 0) >= params.min_signals:
            min_signals_point = pt
            break
    if min_signals_point:
        bullets.append(
            f"Reached minimum evidence ({params.min_signals} outcomes) on {str(min_signals_point['ts'])[:10]}."
        )
    
    # 4. Find first TRUSTED entry
    first_trusted = None
    for pt in series:
        if pt.get("state") == "TRUSTED":
            first_trusted = pt
            break
    if first_trusted:
        hr = first_trusted.get("rolling_hit_rate")
        hr_str = f"{hr*100:.0f}%" if hr is not None else "N/A"
        bullets.append(
            f"Entered TRUSTED on {str(first_trusted['ts'])[:10]} with rolling hit rate at {hr_str}."
        )
    
    # 5. Find biggest dip / miss streak
    max_miss_streak = 0
    max_miss_date = None
    current_streak = 0
    for pt in series:
        if pt.get("event") == "MISS_STREAK":
            max_miss_date = pt.get("ts")
            break  # Just note the first significant miss streak
    if max_miss_date:
        bullets.append(
            f"Confidence dipped after a miss streak around {str(max_miss_date)[:10]}."
        )
    
    # 6. Latest state summary
    latest = series[-1]
    latest_state = latest.get("state", "UNTRUSTED")
    latest_hr = latest.get("rolling_hit_rate")
    latest_count = latest.get("evaluated_count", 0)
    
    if latest_state == "TRUSTED":
        hr_str = f"{latest_hr*100:.0f}%" if latest_hr is not None else "N/A"
        bullets.append(f"Currently TRUSTED with {latest_count} evaluated outcomes (hit rate: {hr_str}).")
    elif latest_state == "WATCH":
        hr_str = f"{latest_hr*100:.0f}%" if latest_hr is not None else "N/A"
        bullets.append(f"Currently in WATCH mode with {latest_count} outcomes (hit rate: {hr_str}). Needs higher hit rate to trust.")
    else:
        bullets.append(f"Currently UNTRUSTED with {latest_count} outcomes. More evidence needed.")
    
    return bullets[:6]  # Cap at 6 bullets


def get_pattern_trust_status(
    conn,
    pattern_id: int,
    market_type: str,
    horizon_bars: int,
    params: GateParams,
) -> dict[str, Any]:
    """
    Get pattern-level trust status (aggregated across all symbols).
    This is what actually determines if signals can be traded.
    """
    cur = conn.cursor()
    
    # Check if pattern is in trusted patterns view
    try:
        cur.execute(PATTERN_TRUST_SQL, {
            "pattern_id": pattern_id,
            "market_type": market_type,
            "horizon_bars": horizon_bars,
        })
        trust_row = cur.fetchone()
        
        if trust_row:
            return {
                "is_trusted": True,
                "n_signals": int(trust_row[4]) if trust_row[4] else 0,
                "hit_rate": float(trust_row[5]) if trust_row[5] else None,
                "avg_return": float(trust_row[6]) if trust_row[6] else None,
                "confidence": trust_row[7],
                "reason": "Pattern meets trust thresholds across all symbols",
            }
    except Exception:
        pass
    
    # Pattern not trusted - get aggregate stats to explain why
    try:
        cur.execute(PATTERN_AGGREGATE_SQL, {
            "pattern_id": pattern_id,
            "market_type": market_type,
            "horizon_bars": horizon_bars,
        })
        agg_row = cur.fetchone()
        
        total_outcomes = int(agg_row[0]) if agg_row and agg_row[0] else 0
        total_hits = int(agg_row[1]) if agg_row and agg_row[1] else 0
        avg_return = float(agg_row[2]) if agg_row and agg_row[2] else None
        hit_rate = total_hits / total_outcomes if total_outcomes > 0 else None
        
        # Determine reason not trusted
        reasons = []
        if total_outcomes < params.min_signals:
            reasons.append(f"needs {params.min_signals - total_outcomes} more outcomes")
        if hit_rate is not None and hit_rate < params.min_hit_rate:
            reasons.append(f"hit rate {hit_rate*100:.1f}% < {params.min_hit_rate*100:.0f}% threshold")
        if avg_return is not None and avg_return <= params.min_avg_return:
            reasons.append(f"avg return too low")
        
        return {
            "is_trusted": False,
            "n_signals": total_outcomes,
            "hit_rate": hit_rate,
            "avg_return": avg_return,
            "confidence": None,
            "reason": "; ".join(reasons) if reasons else "Does not meet trust thresholds",
        }
    except Exception:
        return {
            "is_trusted": False,
            "n_signals": 0,
            "hit_rate": None,
            "avg_return": None,
            "confidence": None,
            "reason": "Unable to determine pattern status",
        }


def build_training_timeline(
    conn,
    symbol: str,
    market_type: str,
    pattern_id: int,
    horizon_bars: int = 5,
    rolling_window: int = 20,
    max_points: int = 250,
) -> dict[str, Any]:
    """
    Build the training timeline response.
    
    Returns:
    {
        "symbol": "AAPL",
        "market_type": "STOCK",
        "pattern_id": 1,
        "horizon_bars": 5,
        "thresholds": {...},
        "pattern_trust": {...},  // NEW: pattern-level trust status
        "series": [...],
        "narrative": [...]
    }
    """
    # Get gate params
    params = get_gate_params(conn)
    
    # Get pattern-level trust status (what actually matters for trading)
    pattern_trust = get_pattern_trust_status(conn, pattern_id, market_type, horizon_bars, params)
    
    # Get first signal date
    cur = conn.cursor()
    cur.execute(FIRST_SIGNAL_SQL, {
        "symbol": symbol,
        "market_type": market_type,
        "pattern_id": pattern_id,
    })
    first_signal_row = cur.fetchone()
    first_signal_ts = first_signal_row[0] if first_signal_row else None
    
    # Get pending evaluations (waiting for future bars)
    pending_info = {"count": 0, "oldest": None, "newest": None}
    try:
        cur.execute(PENDING_EVALUATIONS_SQL, {
            "symbol": symbol,
            "market_type": market_type,
            "pattern_id": pattern_id,
            "horizon_bars": horizon_bars,
        })
        pending_row = cur.fetchone()
        if pending_row and pending_row[0]:
            pending_info = {
                "count": int(pending_row[0]),
                "oldest": pending_row[1].isoformat() if hasattr(pending_row[1], 'isoformat') else str(pending_row[1]) if pending_row[1] else None,
                "newest": pending_row[2].isoformat() if hasattr(pending_row[2], 'isoformat') else str(pending_row[2]) if pending_row[2] else None,
            }
    except Exception:
        pass
    
    # Get timeline series
    cur.execute(TIMELINE_SERIES_SQL, {
        "symbol": symbol,
        "market_type": market_type,
        "pattern_id": pattern_id,
        "horizon_bars": horizon_bars,
    })
    rows = fetch_all(cur)
    
    # Handle empty state
    if not rows:
        narrative = [f"No evaluated outcomes yet for {symbol} — still observing."]
        if pending_info["count"] > 0:
            narrative.append(f"{pending_info['count']} signal(s) pending evaluation (waiting for {horizon_bars} more bars of data).")
        return {
            "symbol": symbol,
            "market_type": market_type,
            "pattern_id": pattern_id,
            "horizon_bars": horizon_bars,
            "thresholds": {
                "min_signals": params.min_signals,
                "min_signals_bootstrap": params.min_signals_bootstrap,
                "min_hit_rate": params.min_hit_rate,
                "min_avg_return": params.min_avg_return,
            },
            "pattern_trust": pattern_trust,
            "pending_evaluations": pending_info,
            "series": [],
            "narrative": narrative,
        }
    
    # Build series with rolling stats
    returns_so_far: list[float] = []
    hits_so_far: list[bool] = []
    series: list[dict[str, Any]] = []
    prev_state: str | None = None
    recent_misses = 0
    
    for i, row in enumerate(rows):
        ret = float(row["REALIZED_RETURN"]) if row["REALIZED_RETURN"] is not None else 0.0
        hit = bool(row["HIT_FLAG"]) if row["HIT_FLAG"] is not None else False
        
        returns_so_far.append(ret)
        hits_so_far.append(hit)
        
        # Track miss streak
        if not hit:
            recent_misses += 1
        else:
            recent_misses = 0
        
        # Compute rolling stats
        rolling_hr, rolling_avg_ret = compute_rolling_stats(
            returns_so_far, hits_so_far, rolling_window
        )
        
        evaluated_count = int(row["EVALUATED_COUNT"]) if row.get("EVALUATED_COUNT") else i + 1
        
        # Classify state
        curr_state = classify_state(
            evaluated_count, rolling_hr, rolling_avg_ret, params
        )
        
        # Detect events
        event = detect_event(
            i, evaluated_count, prev_state, curr_state, params, recent_misses
        )
        
        # Format timestamp
        ts = row.get("SIGNAL_TS") or row.get("ENTRY_TS")
        ts_str = ts.isoformat() if hasattr(ts, "isoformat") else str(ts) if ts else None
        
        point = {
            "ts": ts_str,
            "evaluated_count": evaluated_count,
            "rolling_hit_rate": round(rolling_hr, 4) if rolling_hr is not None else None,
            "rolling_avg_return": round(rolling_avg_ret, 6) if rolling_avg_ret is not None else None,
            "state": curr_state,
        }
        if event:
            point["event"] = event
        
        series.append(point)
        prev_state = curr_state
    
    # Limit to last max_points
    if len(series) > max_points:
        series = series[-max_points:]
    
    # Generate narrative
    narrative = generate_narrative(series, first_signal_ts, params, symbol)
    
    # Add pending info to narrative if any
    if pending_info["count"] > 0:
        narrative.append(f"{pending_info['count']} recent signal(s) pending evaluation (need {horizon_bars} more bars).")
    
    return {
        "symbol": symbol,
        "market_type": market_type,
        "pattern_id": pattern_id,
        "horizon_bars": horizon_bars,
        "thresholds": {
            "min_signals": params.min_signals,
            "min_signals_bootstrap": params.min_signals_bootstrap,
            "min_hit_rate": params.min_hit_rate,
            "min_avg_return": params.min_avg_return,
        },
        "pattern_trust": pattern_trust,
        "pending_evaluations": pending_info,
        "series": series,
        "narrative": narrative,
    }
