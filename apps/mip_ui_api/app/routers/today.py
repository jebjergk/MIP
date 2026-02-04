"""
Today composition endpoint: GET /today?portfolio_id=...
Single JSON with status, portfolio (risk state/gate, KPIs, run events), brief, insights (ranked candidates).
Read-only; never writes to Snowflake.
"""
from datetime import datetime, timezone

from fastapi import APIRouter, Query

from app.config import get_snowflake_config
from app.db import get_connection, fetch_all, SnowflakeAuthError
from app.training_status import apply_scoring_to_rows, _get_int, DEFAULT_MIN_SIGNALS

router = APIRouter(tags=["today"])


def _get_status():
    """Reuse /status logic: api_ok, snowflake_ok, message."""
    cfg = get_snowflake_config()
    snowflake_ok = False
    snowflake_message = None
    try:
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
            snowflake_ok = True
        finally:
            conn.close()
    except SnowflakeAuthError as e:
        snowflake_message = str(e)
    except Exception:
        snowflake_message = "Connection failed"
    return {
        "api_ok": True,
        "snowflake_ok": snowflake_ok,
        "message": snowflake_message if not snowflake_ok else "OK",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def _serialize_row(row):
    """JSON-serializable row (dates, decimals)."""
    if row is None:
        return None
    out = {}
    for k, v in row.items():
        if hasattr(v, "isoformat"):
            out[k] = v.isoformat()
        elif hasattr(v, "__float__") and v is not None and not isinstance(v, (int, bool, str)):
            try:
                out[k] = float(v)
            except (TypeError, ValueError):
                out[k] = v
        else:
            out[k] = v
    return out


def _serialize_rows(rows):
    return [_serialize_row(r) for r in rows] if rows else []


@router.get("/today")
def get_today(portfolio_id: int | None = Query(None, description="Portfolio ID for portfolio/brief sections")):
    """
    Composed view: status, portfolio (risk state/gate, KPIs, run events), latest brief, today's insights (ranked candidates).
    Read-only.
    """
    status = _get_status()
    portfolio = None
    brief = None
    insights = []

    if not status["snowflake_ok"]:
        return {
            "status": status,
            "portfolio": portfolio,
            "brief": brief,
            "insights": insights,
        }

    try:
        conn = get_connection()
    except Exception:
        return {"status": status, "portfolio": portfolio, "brief": brief, "insights": insights}

    try:
        cur = conn.cursor()

        # --- Portfolio: risk state, risk gate, KPIs, run events ---
        if portfolio_id is not None:
            cur.execute(
                "select * from MIP.MART.V_PORTFOLIO_RISK_STATE where PORTFOLIO_ID = %s",
                (portfolio_id,),
            )
            risk_state_rows = fetch_all(cur)
            cur.execute(
                "select * from MIP.MART.V_PORTFOLIO_RISK_GATE where PORTFOLIO_ID = %s",
                (portfolio_id,),
            )
            risk_gate_rows = fetch_all(cur)
            cur.execute(
                """
                select * from MIP.MART.V_PORTFOLIO_RUN_KPIS
                where PORTFOLIO_ID = %s
                order by TO_TS desc
                limit 5
                """,
                (portfolio_id,),
            )
            kpis_rows = fetch_all(cur)
            cur.execute(
                """
                select * from MIP.MART.V_PORTFOLIO_RUN_EVENTS
                where PORTFOLIO_ID = %s
                order by RUN_ID desc
                limit 20
                """,
                (portfolio_id,),
            )
            run_events_rows = fetch_all(cur)
            portfolio = {
                "risk_state": _serialize_rows(risk_state_rows)[:1],
                "risk_gate": _serialize_rows(risk_gate_rows)[:1],
                "kpis": _serialize_rows(kpis_rows),
                "run_events": _serialize_rows(run_events_rows),
            }

        # --- Brief: latest for portfolio (ordered by CREATED_AT, not AS_OF_TS) ---
        if portfolio_id is not None:
            cur.execute(
                """
                select
                  mb.PORTFOLIO_ID as portfolio_id,
                  coalesce(try_cast(mb.BRIEF:as_of_ts::varchar as timestamp_ntz), mb.AS_OF_TS) as as_of_ts,
                  coalesce(mb.CREATED_AT, mb.AS_OF_TS) as created_at,
                  coalesce(get_path(mb.BRIEF, 'attribution.pipeline_run_id')::varchar, mb.BRIEF:pipeline_run_id::string) as pipeline_run_id,
                  mb.BRIEF as brief_json
                from MIP.AGENT_OUT.MORNING_BRIEF mb
                where mb.PORTFOLIO_ID = %s and coalesce(mb.AGENT_NAME, '') = 'MORNING_BRIEF'
                order by coalesce(mb.CREATED_AT, mb.AS_OF_TS) desc, mb.PIPELINE_RUN_ID desc
                limit 1
                """,
                (portfolio_id,),
            )
            brief_row = cur.fetchone()
            if brief_row and cur.description:
                cols = [d[0] for d in cur.description]
                br = dict(zip(cols, brief_row))
                brief = {
                    "as_of_ts": br.get("as_of_ts").isoformat() if hasattr(br.get("as_of_ts"), "isoformat") else br.get("as_of_ts"),
                    "created_at": br.get("created_at").isoformat() if hasattr(br.get("created_at"), "isoformat") else br.get("created_at"),
                    "pipeline_run_id": br.get("pipeline_run_id"),
                    "brief_json": br.get("brief_json"),
                }
                if brief.get("brief_json") is not None and not isinstance(brief["brief_json"], (dict, list, str, type(None))):
                    brief["brief_json"] = str(brief["brief_json"])
            else:
                brief = None

        # --- Insights: distinct candidates from V_SIGNALS_ELIGIBLE_TODAY (1440), enrich with training + performance ---
        cur.execute(
            """
            select distinct SYMBOL, MARKET_TYPE, PATTERN_ID
            from MIP.APP.V_SIGNALS_ELIGIBLE_TODAY
            where INTERVAL_MINUTES = 1440
            """
        )
        candidate_rows = fetch_all(cur)
        if not candidate_rows:
            insights = []
        else:
            # Training status (all rows for 1440)
            cur.execute(
                """
                with recs as (
                  select r.MARKET_TYPE, r.SYMBOL, r.PATTERN_ID, r.INTERVAL_MINUTES,
                    count(*) as recs_total, max(r.TS) as as_of_ts
                  from MIP.APP.RECOMMENDATION_LOG r
                  where r.INTERVAL_MINUTES = 1440
                  group by r.MARKET_TYPE, r.SYMBOL, r.PATTERN_ID, r.INTERVAL_MINUTES
                ),
                outcomes_agg as (
                  select r.MARKET_TYPE, r.SYMBOL, r.PATTERN_ID, r.INTERVAL_MINUTES,
                    count(*) as outcomes_total, count(distinct o.HORIZON_BARS) as horizons_covered,
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
                select recs.MARKET_TYPE as market_type, recs.SYMBOL as symbol, recs.PATTERN_ID as pattern_id,
                  recs.recs_total as recs_total, coalesce(o.outcomes_total, 0) as outcomes_total,
                  coalesce(o.horizons_covered, 0) as horizons_covered,
                  o.avg_outcome_h1 as avg_outcome_h1, o.avg_outcome_h3 as avg_outcome_h3,
                  o.avg_outcome_h5 as avg_outcome_h5, o.avg_outcome_h10 as avg_outcome_h10, o.avg_outcome_h20 as avg_outcome_h20
                from recs
                left join outcomes_agg o on o.MARKET_TYPE = recs.MARKET_TYPE and o.SYMBOL = recs.SYMBOL and o.PATTERN_ID = recs.PATTERN_ID
                """
            )
            training_rows = fetch_all(cur)
            scored_training = apply_scoring_to_rows(training_rows, min_signals=DEFAULT_MIN_SIGNALS)
            training_by_key = {}
            for r in scored_training:
                key = (
                    r.get("MARKET_TYPE") or r.get("market_type"),
                    r.get("SYMBOL") or r.get("symbol"),
                    r.get("PATTERN_ID") or r.get("pattern_id"),
                )
                training_by_key[key] = r

            # Performance by horizon (all pairs)
            cur.execute(
                """
                select r.MARKET_TYPE as market_type, r.SYMBOL as symbol, r.PATTERN_ID as pattern_id,
                  o.HORIZON_BARS as horizon_bars, count(*) as n_outcomes,
                  avg(o.REALIZED_RETURN) as mean_outcome,
                  sum(case when o.REALIZED_RETURN > 0 then 1 else 0 end)::float / nullif(count(*), 0) as pct_positive
                from MIP.APP.RECOMMENDATION_LOG r
                join MIP.APP.RECOMMENDATION_OUTCOMES o on o.RECOMMENDATION_ID = r.RECOMMENDATION_ID
                where r.INTERVAL_MINUTES = 1440 and o.EVAL_STATUS = 'SUCCESS' and o.REALIZED_RETURN is not null
                group by r.MARKET_TYPE, r.SYMBOL, r.PATTERN_ID, o.HORIZON_BARS
                """
            )
            perf_rows = fetch_all(cur)
            perf_by_key = {}
            for r in perf_rows:
                key = (r.get("market_type") or r.get("MARKET_TYPE"), r.get("symbol") or r.get("SYMBOL"), r.get("pattern_id") or r.get("PATTERN_ID"))
                if key not in perf_by_key:
                    perf_by_key[key] = []
                perf_by_key[key].append({
                    "horizon_bars": r.get("horizon_bars") or r.get("HORIZON_BARS"),
                    "n_outcomes": r.get("n_outcomes") or r.get("N_OUTCOMES"),
                    "mean_outcome": float(r.get("mean_outcome") or r.get("MEAN_OUTCOME")) if r.get("mean_outcome") is not None or r.get("MEAN_OUTCOME") is not None else None,
                    "pct_positive": float(r.get("pct_positive") or r.get("PCT_POSITIVE")) if r.get("pct_positive") is not None or r.get("PCT_POSITIVE") is not None else None,
                })

            # Build insights: filter maturity_stage != INSUFFICIENT, score = w1*maturity + w2*mean_h5 + w3*pct_pos_h5, top 10
            w1, w2, w3 = 0.5, 0.3, 0.2
            min_recs = max(1, DEFAULT_MIN_SIGNALS // 2)

            list_insights = []
            for r in candidate_rows:
                sym = r.get("SYMBOL") or r.get("symbol")
                mt = r.get("MARKET_TYPE") or r.get("market_type")
                pid = r.get("PATTERN_ID") or r.get("pattern_id")
                key = (mt, sym, pid)
                tr = training_by_key.get(key)
                if tr is None:
                    continue
                maturity_stage = tr.get("maturity_stage") or tr.get("MATURITY_STAGE")
                if maturity_stage == "INSUFFICIENT":
                    continue
                recs_total = _get_int(tr, "recs_total")
                if recs_total < min_recs:
                    continue
                maturity_score = float(tr.get("maturity_score") or tr.get("MATURITY_SCORE") or 0)
                reasons = tr.get("reasons") or []
                avg_h5 = tr.get("avg_outcome_h5") or tr.get("AVG_OUTCOME_H5")
                if avg_h5 is not None:
                    try:
                        avg_h5 = float(avg_h5)
                    except (TypeError, ValueError):
                        avg_h5 = 0.0
                else:
                    avg_h5 = 0.0
                perf_list = perf_by_key.get(key, [])
                mean_h5 = None
                pct_pos_h5 = None
                for p in perf_list:
                    if (p.get("horizon_bars") or 0) == 5:
                        mean_h5 = p.get("mean_outcome")
                        pct_pos_h5 = p.get("pct_positive")
                        break
                if mean_h5 is None and perf_list:
                    mean_h5 = perf_list[0].get("mean_outcome")
                    pct_pos_h5 = perf_list[0].get("pct_positive")
                mean_h5 = float(mean_h5) if mean_h5 is not None else 0.0
                pct_pos_h5 = float(pct_pos_h5) if pct_pos_h5 is not None else 0.0
                today_score = (w1 * maturity_score / 100.0) + (w2 * max(0, mean_h5) * 10) + (w3 * pct_pos_h5)
                performance_summary = {h.get("horizon_bars"): {"mean_outcome": h.get("mean_outcome"), "pct_positive": h.get("pct_positive"), "n_outcomes": h.get("n_outcomes")} for h in perf_list}
                why = (
                    f"Ranked by data maturity (score {maturity_score:.0f}) and outcome history. "
                    f"At 5-bar horizon: mean return {mean_h5 * 100:.2f}%, positive {pct_pos_h5 * 100:.0f}% of the time."
                )
                list_insights.append({
                    "symbol": sym,
                    "market_type": mt,
                    "pattern_id": pid,
                    "maturity_stage": maturity_stage,
                    "maturity_score": round(maturity_score, 1),
                    "reasons": reasons,
                    "performance_summary": performance_summary,
                    "why_this_is_here": why,
                    "today_score": round(today_score, 3),
                })
            list_insights.sort(key=lambda x: (-x["today_score"], -x["maturity_score"]))
            insights = list_insights[:10]

    except Exception:
        pass
    finally:
        conn.close()

    return {
        "status": status,
        "portfolio": portfolio,
        "brief": brief,
        "insights": insights,
    }
