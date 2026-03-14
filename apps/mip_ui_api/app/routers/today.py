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

def _as_float(v, default=0.0):
    try:
        if v is None:
            return default
        return float(v)
    except (TypeError, ValueError):
        return default

def _as_int(v, default=0):
    try:
        if v is None:
            return default
        return int(v)
    except (TypeError, ValueError):
        return default

def _proposal_committee_assessment(sample_size: int, hit_rate: float, avg_return: float) -> str:
    if sample_size < 10:
        return "LOW_EVIDENCE"
    if sample_size >= 30 and hit_rate >= 0.58 and avg_return >= 0.0010:
        return "STRONG"
    if sample_size >= 20 and hit_rate >= 0.52 and avg_return >= 0.0003:
        return "WATCH"
    return "WEAK"

def _proposal_committee_reason(sample_size: int, hit_rate: float, avg_return: float) -> str:
    if sample_size < 10:
        return f"Only {sample_size} historical outcomes (need >=10 for reliable confidence)."
    if sample_size >= 30 and hit_rate >= 0.58 and avg_return >= 0.0010:
        return (
            f"Strong evidence: n={sample_size}, hit rate {hit_rate * 100:.1f}% "
            f"and avg return {avg_return * 100:.2f}%."
        )
    if sample_size >= 20 and hit_rate >= 0.52 and avg_return >= 0.0003:
        return (
            f"Borderline quality: n={sample_size}, hit rate {hit_rate * 100:.1f}%, "
            f"avg return {avg_return * 100:.2f}% (watch for consistency)."
        )
    return (
        f"Weak edge: n={sample_size}, hit rate {hit_rate * 100:.1f}% "
        f"and avg return {avg_return * 100:.2f}% below strong thresholds."
    )


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
    daily_readiness = {"found": False}

    if not status["snowflake_ok"]:
        return {
            "status": status,
            "portfolio": portfolio,
            "brief": brief,
            "insights": insights,
            "daily_readiness": daily_readiness,
        }

    try:
        conn = get_connection()
    except Exception:
        return {
            "status": status,
            "portfolio": portfolio,
            "brief": brief,
            "insights": insights,
            "daily_readiness": daily_readiness,
        }

    try:
        cur = conn.cursor()
        resolved_portfolio_id = portfolio_id
        if resolved_portfolio_id is None:
            cur.execute(
                """
                select PORTFOLIO_ID
                from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
                where coalesce(IS_ACTIVE, true)
                order by UPDATED_AT desc, PORTFOLIO_ID asc
                limit 1
                """
            )
            row = cur.fetchone()
            if row:
                resolved_portfolio_id = int(row[0])

        # --- Portfolio: risk state, risk gate, KPIs, run events ---
        if resolved_portfolio_id is not None:
            cur.execute(
                "select * from MIP.MART.V_PORTFOLIO_RISK_STATE where PORTFOLIO_ID = %s",
                (resolved_portfolio_id,),
            )
            risk_state_rows = fetch_all(cur)
            cur.execute(
                "select * from MIP.MART.V_PORTFOLIO_RISK_GATE where PORTFOLIO_ID = %s",
                (resolved_portfolio_id,),
            )
            risk_gate_rows = fetch_all(cur)
            cur.execute(
                """
                select * from MIP.MART.V_PORTFOLIO_RUN_KPIS
                where PORTFOLIO_ID = %s
                order by TO_TS desc
                limit 5
                """,
                (resolved_portfolio_id,),
            )
            kpis_rows = fetch_all(cur)
            cur.execute(
                """
                select * from MIP.MART.V_PORTFOLIO_RUN_EVENTS
                where PORTFOLIO_ID = %s
                order by RUN_ID desc
                limit 20
                """,
                (resolved_portfolio_id,),
            )
            run_events_rows = fetch_all(cur)
            portfolio = {
                "risk_state": _serialize_rows(risk_state_rows)[:1],
                "risk_gate": _serialize_rows(risk_gate_rows)[:1],
                "kpis": _serialize_rows(kpis_rows),
                "run_events": _serialize_rows(run_events_rows),
            }

        # --- Brief: latest for portfolio (ordered by CREATED_AT, not AS_OF_TS) ---
        if resolved_portfolio_id is not None:
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
                (resolved_portfolio_id,),
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

        # --- Daily readiness block for Cockpit (deterministic, no Cortex) ---
        try:
            cur.execute(
                """
                select RUN_ID, EVENT_TS, STATUS, DETAILS
                from MIP.APP.MIP_AUDIT_LOG
                where EVENT_TYPE = 'PIPELINE'
                  and EVENT_NAME = 'SP_RUN_DAILY_PIPELINE'
                order by EVENT_TS desc
                limit 1
                """
            )
            latest_run_row = cur.fetchone()
            latest_run = None
            if latest_run_row and cur.description:
                cols = [d[0] for d in cur.description]
                latest_run = dict(zip(cols, latest_run_row))

            if latest_run and latest_run.get("RUN_ID"):
                run_id = str(latest_run.get("RUN_ID"))
                run_details = latest_run.get("DETAILS") if isinstance(latest_run.get("DETAILS"), dict) else {}
                event_ts = latest_run.get("EVENT_TS")
                completed_at = event_ts.isoformat() if hasattr(event_ts, "isoformat") else event_ts
                active_live_portfolio_id = None

                # Canonical live portfolio for readiness/proposals: latest active mapping.
                cur.execute(
                    """
                    select PORTFOLIO_ID
                    from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
                    where coalesce(IS_ACTIVE, true)
                    order by UPDATED_AT desc, PORTFOLIO_ID asc
                    limit 1
                    """
                )
                live_row = cur.fetchone()
                if live_row:
                    active_live_portfolio_id = _as_int(live_row[0], None)

                # Signals generated this run from pipeline step audit (run-scoped, deterministic).
                cur.execute(
                    """
                    select ROWS_AFFECTED, DETAILS
                    from MIP.APP.MIP_AUDIT_LOG
                    where EVENT_TYPE = 'PIPELINE_STEP'
                      and PARENT_RUN_ID = %s
                      and EVENT_NAME = 'RECOMMENDATIONS'
                      and STATUS = 'SUCCESS'
                    order by EVENT_TS desc
                    limit 1
                    """,
                    (run_id,),
                )
                rec_step_row = cur.fetchone()
                signals_generated = 0
                rec_details = {}
                if rec_step_row and cur.description:
                    rows_aff = rec_step_row[0]
                    det = rec_step_row[1]
                    signals_generated = _as_int(rows_aff, 0)
                    if isinstance(det, dict):
                        rec_details = det

                if signals_generated == 0:
                    # Fallback: sum recommendation insert deltas from pipeline details
                    recs = run_details.get("recommendations") if isinstance(run_details, dict) else None
                    if isinstance(recs, list):
                        def _rec_delta(r):
                            if not isinstance(r, dict):
                                return 0
                            return max(
                                _as_int(r.get("inserted_count"), 0),
                                _as_int(r.get("rows_delta"), 0),
                                _as_int(r.get("rows_after"), 0) - _as_int(r.get("rows_before"), 0),
                            )
                        signals_generated = sum(_rec_delta(r) for r in recs)

                # Per-market signal counts from pipeline step details when available.
                run_signal_map = {}
                step_recs = rec_details.get("recommendations")
                if isinstance(step_recs, list):
                    for r in step_recs:
                        if not isinstance(r, dict):
                            continue
                        mt = str(r.get("market_type") or r.get("MARKET_TYPE") or "").upper()
                        if not mt:
                            continue
                        delta = max(
                            _as_int(r.get("inserted_count"), 0),
                            _as_int(r.get("rows_delta"), 0),
                            _as_int(r.get("rows_after"), 0) - _as_int(r.get("rows_before"), 0),
                        )
                        run_signal_map[mt] = run_signal_map.get(mt, 0) + delta

                if not run_signal_map:
                    # Final fallback: latest recommendation bar date (not strictly run-scoped, but operationally useful).
                    cur.execute(
                        """
                        with latest as (
                          select max(TS) as latest_ts
                          from MIP.APP.RECOMMENDATION_LOG
                          where INTERVAL_MINUTES = 1440
                        )
                        select MARKET_TYPE, count(*) as SIGNALS_GENERATED
                        from MIP.APP.RECOMMENDATION_LOG r
                        join latest l on r.TS = l.latest_ts
                        where r.INTERVAL_MINUTES = 1440
                        group by MARKET_TYPE
                        """
                    )
                    run_signal_rows = fetch_all(cur)
                    run_signal_map = {
                        str(r.get("MARKET_TYPE")): _as_int(r.get("SIGNALS_GENERATED"), 0)
                        for r in run_signal_rows
                    }
                    if signals_generated == 0:
                        signals_generated = sum(run_signal_map.values())

                # Proposal counts and details for this run
                cur.execute(
                    """
                    select
                      PROPOSAL_ID,
                      PORTFOLIO_ID,
                      PROPOSED_AT,
                      SYMBOL,
                      MARKET_TYPE,
                      SIDE,
                      TARGET_WEIGHT,
                      SIGNAL_PATTERN_ID,
                      STATUS
                    from MIP.AGENT_OUT.ORDER_PROPOSALS
                    where RUN_ID_VARCHAR = %s
                      and (%s is null or PORTFOLIO_ID = %s)
                    order by PROPOSED_AT desc
                    """,
                    (run_id, active_live_portfolio_id, active_live_portfolio_id),
                )
                proposal_rows = fetch_all(cur)
                proposals_total = len(proposal_rows)
                proposals_preview = []

                for p in proposal_rows[:10]:
                    symbol = p.get("SYMBOL")
                    market_type = p.get("MARKET_TYPE")
                    pattern_id = p.get("SIGNAL_PATTERN_ID")

                    # Historical evidence snapshot for committee-style readout
                    if pattern_id is not None:
                        cur.execute(
                            """
                            select
                              o.HORIZON_BARS as horizon_bars,
                              count(*) as n_outcomes,
                              avg(o.REALIZED_RETURN) as mean_return,
                              sum(case when o.REALIZED_RETURN > 0 then 1 else 0 end)::float / nullif(count(*), 0) as hit_rate
                            from MIP.APP.RECOMMENDATION_LOG r
                            join MIP.APP.RECOMMENDATION_OUTCOMES o
                              on o.RECOMMENDATION_ID = r.RECOMMENDATION_ID
                            where r.INTERVAL_MINUTES = 1440
                              and r.SYMBOL = %s
                              and r.MARKET_TYPE = %s
                              and r.PATTERN_ID = %s
                              and o.EVAL_STATUS = 'SUCCESS'
                              and o.REALIZED_RETURN is not null
                            group by o.HORIZON_BARS
                            order by n_outcomes desc, mean_return desc
                            """,
                            (symbol, market_type, pattern_id),
                        )
                    else:
                        cur.execute(
                            """
                            select
                              o.HORIZON_BARS as horizon_bars,
                              count(*) as n_outcomes,
                              avg(o.REALIZED_RETURN) as mean_return,
                              sum(case when o.REALIZED_RETURN > 0 then 1 else 0 end)::float / nullif(count(*), 0) as hit_rate
                            from MIP.APP.RECOMMENDATION_LOG r
                            join MIP.APP.RECOMMENDATION_OUTCOMES o
                              on o.RECOMMENDATION_ID = r.RECOMMENDATION_ID
                            where r.INTERVAL_MINUTES = 1440
                              and r.SYMBOL = %s
                              and r.MARKET_TYPE = %s
                              and o.EVAL_STATUS = 'SUCCESS'
                              and o.REALIZED_RETURN is not null
                            group by o.HORIZON_BARS
                            order by n_outcomes desc, mean_return desc
                            """,
                            (symbol, market_type),
                        )

                    evidence = fetch_all(cur)
                    chosen = None
                    if evidence:
                        # Prefer horizons with decent sample + better return.
                        with_sample = [e for e in evidence if _as_int(e.get("N_OUTCOMES") or e.get("n_outcomes"), 0) >= 10]
                        pool = with_sample if with_sample else evidence
                        chosen = sorted(
                            pool,
                            key=lambda e: (
                                _as_float(e.get("MEAN_RETURN") or e.get("mean_return"), -999),
                                _as_float(e.get("HIT_RATE") or e.get("hit_rate"), -999),
                                _as_int(e.get("N_OUTCOMES") or e.get("n_outcomes"), 0),
                            ),
                            reverse=True,
                        )[0]

                    hold_bars = _as_int((chosen or {}).get("HORIZON_BARS") or (chosen or {}).get("horizon_bars"), 0)
                    sample_size = _as_int((chosen or {}).get("N_OUTCOMES") or (chosen or {}).get("n_outcomes"), 0)
                    hit_rate = _as_float((chosen or {}).get("HIT_RATE") or (chosen or {}).get("hit_rate"), 0.0)
                    avg_return = _as_float((chosen or {}).get("MEAN_RETURN") or (chosen or {}).get("mean_return"), 0.0)

                    proposals_preview.append({
                        "proposal_id": p.get("PROPOSAL_ID"),
                        "portfolio_id": p.get("PORTFOLIO_ID"),
                        "proposed_at": p.get("PROPOSED_AT").isoformat() if hasattr(p.get("PROPOSED_AT"), "isoformat") else p.get("PROPOSED_AT"),
                        "symbol": symbol,
                        "market_type": market_type,
                        "side": p.get("SIDE"),
                        "status": p.get("STATUS"),
                        "target_weight": _as_float(p.get("TARGET_WEIGHT"), None),
                        "signal_pattern_id": pattern_id,
                        "committee_assessment": _proposal_committee_assessment(sample_size, hit_rate, avg_return),
                        "committee_reason": _proposal_committee_reason(sample_size, hit_rate, avg_return),
                        "historical_hit_rate": hit_rate,
                        "historical_mean_return": avg_return,
                        "suggested_hold_bars": hold_bars if hold_bars > 0 else None,
                        "evidence_samples": sample_size,
                    })

                # Training/trust distribution by market type
                cur.execute(
                    """
                    select
                      MARKET_TYPE,
                      count_if(TRUST_LABEL = 'TRUSTED') as TRUSTED_COUNT,
                      count_if(TRUST_LABEL = 'WATCH') as WATCH_COUNT,
                      count_if(TRUST_LABEL = 'UNTRUSTED') as UNTRUSTED_COUNT,
                      count(*) as TOTAL_COUNT
                    from MIP.MART.V_TRUSTED_SIGNAL_POLICY
                    group by MARKET_TYPE
                    order by MARKET_TYPE
                    """
                )
                trust_rows = fetch_all(cur)

                cur.execute(
                    """
                    select
                      MARKET_TYPE,
                      count(*) as PROPOSALS_GENERATED
                    from MIP.AGENT_OUT.ORDER_PROPOSALS
                    where RUN_ID_VARCHAR = %s
                      and (%s is null or PORTFOLIO_ID = %s)
                    group by MARKET_TYPE
                    order by MARKET_TYPE
                    """,
                    (run_id, active_live_portfolio_id, active_live_portfolio_id),
                )
                run_prop_rows = fetch_all(cur)
                run_prop_map = {
                    str(r.get("MARKET_TYPE")): _as_int(r.get("PROPOSALS_GENERATED"), 0)
                    for r in run_prop_rows
                }

                by_market_type = []
                for r in trust_rows:
                    mt = str(r.get("MARKET_TYPE") or "")
                    by_market_type.append({
                        "market_type": mt,
                        "trusted_count": _as_int(r.get("TRUSTED_COUNT"), 0),
                        "watch_count": _as_int(r.get("WATCH_COUNT"), 0),
                        "untrusted_count": _as_int(r.get("UNTRUSTED_COUNT"), 0),
                        "total_count": _as_int(r.get("TOTAL_COUNT"), 0),
                        "signals_generated_last_run": run_signal_map.get(mt, 0),
                        "proposals_generated_last_run": run_prop_map.get(mt, 0),
                    })

                eligible_signals = _as_int(run_details.get("eligible_signals") if isinstance(run_details, dict) else 0, 0)
                proposals_from_summary = _as_int(run_details.get("proposals_proposed") if isinstance(run_details, dict) else 0, 0)

                daily_readiness = {
                    "found": True,
                    "last_run": {
                        "run_id": run_id,
                        "status": latest_run.get("STATUS"),
                        "completed_at": completed_at,
                        "live_portfolio_id": active_live_portfolio_id,
                    },
                    "counts": {
                        "signals_generated": signals_generated,
                        "signals_eligible": eligible_signals,
                        "proposals_generated": proposals_total if proposals_total > 0 else proposals_from_summary,
                    },
                    "training_by_market_type": by_market_type,
                    "proposals_preview": proposals_preview,
                }
        except Exception:
            daily_readiness = {"found": False}

    except Exception:
        pass
    finally:
        conn.close()

    return {
        "status": status,
        "portfolio": portfolio,
        "brief": brief,
        "insights": insights,
        "daily_readiness": daily_readiness,
    }
