"""
Signals / Decision Explorer endpoints.

GET /signals          — Original signal rows with filters (backward compat).
GET /signals/decisions — Enriched Decision Explorer: signals joined to trades
                         with outcome taxonomy, human-readable explanations,
                         and step-by-step decision traces.
"""
import json
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


# ── Decision Explorer helpers ──────────────────────────────────────────


def _parse_gating(raw):
    """Parse GATING_REASON from Snowflake VARIANT (str, dict, or None)."""
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return {}
    return {}


def _safe_float(val):
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _derive_why(outcome: str, trust_label: str, gating: dict) -> str:
    """One-line human-readable explanation for why this outcome happened."""
    pr = gating.get("policy_reason") or {}
    if isinstance(pr, str):
        try:
            pr = json.loads(pr)
        except Exception:
            pr = {}

    if outcome == "TRADED":
        return "Trusted pattern — selected and executed"

    if outcome == "REJECTED_BY_TRUST":
        if trust_label == "WATCH":
            hr = _safe_float(pr.get("hit_rate"))
            if hr is not None:
                return f"Under evaluation — hit rate {hr:.0%}, building track record"
            return "Under evaluation — insufficient history to trust"
        n = pr.get("n_success")
        hr = _safe_float(pr.get("hit_rate"))
        parts = []
        if n is not None:
            parts.append(f"{n} signals")
        if hr is not None:
            parts.append(f"hit rate {hr:.0%}")
        extra = f" ({', '.join(parts)})" if parts else ""
        return f"Not trusted — performance below threshold{extra}"

    if outcome == "REJECTED_BY_RISK":
        return "Risk constraints prevent trading"

    if outcome == "REJECTED_BY_CAPACITY":
        return "Portfolio at capacity"

    if outcome == "ELIGIBLE_NOT_SELECTED":
        return "Passed all gates — not selected by portfolio"

    return ""


def _build_decision_trace(
    outcome: str,
    trust_label: str,
    is_eligible: bool,
    gating: dict,
    score,
) -> list:
    """Build step-by-step decision trace for the UI stepper."""
    pr = gating.get("policy_reason") or {}
    if isinstance(pr, str):
        try:
            pr = json.loads(pr)
        except Exception:
            pr = {}
    thresholds = gating.get("score_thresholds") or {}
    if isinstance(thresholds, str):
        try:
            thresholds = json.loads(thresholds)
        except Exception:
            thresholds = {}
    horizon = gating.get("horizon_bars")
    score_f = _safe_float(score)

    steps = []

    # 1 — Signal detected
    detail = f"Score: {score_f:.2f}" if score_f is not None else None
    steps.append({"label": "Signal Detected", "passed": True, "detail": detail})

    # 2 — Trust gate
    trust_passed = trust_label == "TRUSTED"
    parts = [trust_label]
    trusted_min = _safe_float(thresholds.get("trusted_min"))
    if trusted_min is not None and score_f is not None:
        parts.append(f"score {score_f:.2f} vs min {trusted_min:.2f}")
    hr = _safe_float(pr.get("hit_rate"))
    if hr is not None:
        parts.append(f"hit rate {hr:.0%}")
    ar = _safe_float(pr.get("avg_return"))
    if ar is not None:
        parts.append(f"avg return {ar:.2%}")
    steps.append({"label": "Trust Gate", "passed": trust_passed, "detail": " · ".join(parts)})

    # 3 — Eligibility
    steps.append({
        "label": "Eligibility",
        "passed": is_eligible,
        "detail": "Eligible for trading" if is_eligible else f"Not eligible ({trust_label})",
    })

    # 4 — Selection (only meaningful when eligible)
    traded = outcome == "TRADED"
    if is_eligible:
        steps.append({
            "label": "Selection",
            "passed": traded,
            "detail": "Selected for execution" if traded else "Not selected — may be ranked below alternatives",
        })

    # 5 — Final
    steps.append({
        "label": "Final Decision",
        "passed": traded,
        "detail": "Trade executed" if traded else "Not traded",
    })

    return steps


def _build_decision_metrics(gating: dict) -> dict:
    """Extract supporting metrics from gating for display."""
    pr = gating.get("policy_reason") or {}
    if isinstance(pr, str):
        try:
            pr = json.loads(pr)
        except Exception:
            pr = {}
    thresholds = gating.get("score_thresholds") or {}
    if isinstance(thresholds, str):
        try:
            thresholds = json.loads(thresholds)
        except Exception:
            thresholds = {}

    metrics = {}
    mapping = {
        "hit_rate": "Hit Rate",
        "avg_return": "Avg Return",
        "coverage_rate": "Coverage",
        "n_success": "Training Signals",
        "recent_hit_rate": "Recent Hit Rate",
        "recent_avg_return": "Recent Avg Return",
        "score_return_corr": "Score–Return Corr",
    }
    for key, label in mapping.items():
        val = pr.get(key)
        if val is not None:
            metrics[label] = _safe_float(val)

    horizon = gating.get("horizon_bars")
    if horizon is not None:
        metrics["Horizon (bars)"] = horizon

    if thresholds.get("trusted_min") is not None:
        metrics["Trusted Min Score"] = _safe_float(thresholds["trusted_min"])
    if thresholds.get("watch_min") is not None:
        metrics["Watch Min Score"] = _safe_float(thresholds["watch_min"])

    return metrics


def _build_decision_summary(decisions: list) -> dict:
    counts = {}
    for d in decisions:
        o = d.get("outcome", "UNKNOWN")
        counts[o] = counts.get(o, 0) + 1
    return {
        "total": len(decisions),
        "traded": counts.get("TRADED", 0),
        "rejected_by_trust": counts.get("REJECTED_BY_TRUST", 0),
        "rejected_by_risk": counts.get("REJECTED_BY_RISK", 0),
        "rejected_by_capacity": counts.get("REJECTED_BY_CAPACITY", 0),
        "eligible_not_selected": counts.get("ELIGIBLE_NOT_SELECTED", 0),
        "by_outcome": counts,
    }


# ── Decision Explorer endpoint ─────────────────────────────────────────


@router.get("/decisions")
def get_decisions(
    symbol: Optional[str] = Query(None),
    market_type: Optional[str] = Query(None),
    pattern_id: Optional[str] = Query(None),
    trust_label: Optional[str] = Query(None),
    outcome: Optional[str] = Query(None, description="TRADED | REJECTED_BY_TRUST | ELIGIBLE_NOT_SELECTED"),
    run_id: Optional[str] = Query(None),
    as_of_ts: Optional[str] = Query(None),
    portfolio_id: Optional[int] = Query(None),
    limit: int = Query(200, ge=1, le=500),
    include_fallback: bool = Query(True),
):
    """
    Decision Explorer: enrich signals with trade linkage, outcome taxonomy,
    human-readable explanations, and decision traces.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()

        # ── 1. Fetch signals ───────────────────────────────────────────
        conditions = ["s.INTERVAL_MINUTES = 1440"]
        params: list = []
        filters_applied: dict = {}

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
            try:
                ts = datetime.fromisoformat(as_of_ts.replace("Z", "+00:00"))
                conditions.append("date(s.TS) = date(%s)")
                params.append(ts)
                filters_applied["as_of_ts"] = as_of_ts
            except ValueError:
                pass
        if trust_label:
            conditions.append("s.TRUST_LABEL = %s")
            params.append(trust_label.upper())
            filters_applied["trust_label"] = trust_label.upper()

        where_clause = " AND ".join(conditions)

        signal_sql = f"""
        SELECT
            s.RECOMMENDATION_ID,
            s.RUN_ID,
            s.TS          AS SIGNAL_TS,
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
        FROM MIP.APP.V_SIGNALS_ELIGIBLE_TODAY s
        WHERE {where_clause}
        ORDER BY s.TS DESC, s.SCORE DESC
        LIMIT {limit}
        """

        cur.execute(signal_sql, tuple(params))
        raw_signals = fetch_all(cur)

        # ── Fallback (re-use original logic) ───────────────────────────
        if not raw_signals and include_fallback and (run_id or as_of_ts):
            fb_conditions = ["s.INTERVAL_MINUTES = 1440"]
            fb_params: list = []
            if symbol:
                fb_conditions.append("s.SYMBOL = %s")
                fb_params.append(symbol.upper())
            if market_type:
                fb_conditions.append("s.MARKET_TYPE = %s")
                fb_params.append(market_type.upper())
            if trust_label:
                fb_conditions.append("s.TRUST_LABEL = %s")
                fb_params.append(trust_label.upper())
            fb_conditions.append("s.TS >= dateadd(day, -7, current_timestamp())")
            fb_where = " AND ".join(fb_conditions)
            fb_sql = f"""
            SELECT
                s.RECOMMENDATION_ID, s.RUN_ID, s.TS AS SIGNAL_TS,
                s.SYMBOL, s.MARKET_TYPE, s.INTERVAL_MINUTES,
                s.PATTERN_ID, s.SCORE, s.DETAILS,
                s.TRUST_LABEL, s.RECOMMENDED_ACTION,
                s.IS_ELIGIBLE, s.GATING_REASON
            FROM MIP.APP.V_SIGNALS_ELIGIBLE_TODAY s
            WHERE {fb_where}
            ORDER BY s.TS DESC, s.SCORE DESC
            LIMIT {limit}
            """
            cur.execute(fb_sql, tuple(fb_params))
            raw_signals = fetch_all(cur)

        # ── 2. Fetch traded RECOMMENDATION_IDs ─────────────────────────
        rec_ids = [
            s["RECOMMENDATION_ID"]
            for s in raw_signals
            if s.get("RECOMMENDATION_ID") is not None
        ]

        traded_map: dict = {}
        if rec_ids:
            ph = ",".join(["%s"] * len(rec_ids))
            trade_sql = f"""
            SELECT
                op.RECOMMENDATION_ID,
                pt.TRADE_ID,
                pt.TRADE_TS,
                pt.SIDE,
                pt.PRICE,
                pt.QUANTITY,
                pt.NOTIONAL,
                pt.PORTFOLIO_ID
            FROM MIP.AGENT_OUT.ORDER_PROPOSALS op
            INNER JOIN MIP.APP.PORTFOLIO_TRADES pt
                ON pt.PROPOSAL_ID = op.PROPOSAL_ID
            WHERE op.RECOMMENDATION_ID IN ({ph})
            """
            trade_params = list(rec_ids)
            if portfolio_id:
                trade_sql += " AND pt.PORTFOLIO_ID = %s"
                trade_params.append(portfolio_id)
            try:
                cur.execute(trade_sql, tuple(trade_params))
                for tr in fetch_all(cur):
                    rid = tr.get("RECOMMENDATION_ID")
                    if rid is not None and rid not in traded_map:
                        traded_map[rid] = tr
            except Exception:
                pass  # ORDER_PROPOSALS or linkage unavailable

        # ── 3. Classify & enrich ───────────────────────────────────────
        all_decisions: list = []
        for sig in raw_signals:
            rec_id = sig.get("RECOMMENDATION_ID")
            is_eligible = bool(sig.get("IS_ELIGIBLE"))
            trust = (sig.get("TRUST_LABEL") or "UNTRUSTED").upper()
            score_raw = sig.get("SCORE")
            gating = _parse_gating(sig.get("GATING_REASON"))

            if rec_id in traded_map:
                outcome_val = "TRADED"
                trade_info = serialize_row(traded_map[rec_id])
            elif not is_eligible:
                outcome_val = "REJECTED_BY_TRUST"
                trade_info = None
            else:
                outcome_val = "ELIGIBLE_NOT_SELECTED"
                trade_info = None

            row = serialize_row(sig)
            row["outcome"] = outcome_val
            row["trade_info"] = trade_info
            row["gating_parsed"] = gating
            row["why_summary"] = _derive_why(outcome_val, trust, gating)
            row["decision_trace"] = _build_decision_trace(
                outcome_val, trust, is_eligible, gating, score_raw,
            )
            row["metrics"] = _build_decision_metrics(gating)
            all_decisions.append(row)

        # Summary BEFORE outcome filter (so banner shows full picture)
        summary = _build_decision_summary(all_decisions)

        # ── 4. Apply outcome filter ────────────────────────────────────
        if outcome:
            decisions = [
                d for d in all_decisions
                if d["outcome"] == outcome.upper()
            ]
        else:
            decisions = all_decisions

        # ── 5. Collect filter options from full set ────────────────────
        filter_options = {
            "symbols": sorted({d.get("SYMBOL") or "" for d in all_decisions} - {""}),
            "markets": sorted({d.get("MARKET_TYPE") or "" for d in all_decisions} - {""}),
            "patterns": sorted({str(d.get("PATTERN_ID") or "") for d in all_decisions} - {""}),
            "trusts": sorted({d.get("TRUST_LABEL") or "" for d in all_decisions} - {""}),
            "outcomes": sorted({d.get("outcome") or "" for d in all_decisions} - {""}),
        }

        return {
            "decisions": decisions,
            "summary": summary,
            "count": len(decisions),
            "total": len(all_decisions),
            "filters_applied": filters_applied,
            "filter_options": filter_options,
        }
    finally:
        conn.close()
