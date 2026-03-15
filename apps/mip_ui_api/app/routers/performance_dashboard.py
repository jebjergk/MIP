"""
MIP Performance Dashboard API.

Leadership-facing and operator-facing aggregated metrics in one payload.
Designed to remain useful even when certain baseline objects are unavailable.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Query

from app.db import fetch_all, get_connection, serialize_row, serialize_rows

router = APIRouter(prefix="/performance-dashboard", tags=["performance-dashboard"])

FIXED_MONTHLY_COST_USD = 80.0  # ChatGPT Plus ($20) + Cursor Pro ($60)
DEFAULT_CREDIT_USD = 3.0
DASHBOARD_DAY0_CONFIG_KEY = "PERFORMANCE_DASHBOARD_DAY0_TS"


def _to_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _to_int(v: Any) -> int:
    try:
        return int(v or 0)
    except (TypeError, ValueError):
        return 0


def _safe_rows(cur, sql: str, params: tuple = ()) -> list[dict]:
    try:
        cur.execute(sql, params)
        return fetch_all(cur)
    except Exception:
        return []


def _safe_row(cur, sql: str, params: tuple = ()) -> dict:
    rows = _safe_rows(cur, sql, params)
    if not rows:
        return {}
    return rows[0]


@router.get("/overview")
def get_performance_dashboard_overview(
    lookback_days: int = Query(90, ge=30, le=365),
):
    conn = get_connection()
    try:
        cur = conn.cursor()
        effective_start_row = _safe_row(
            cur,
            """
            with cfg as (
              select try_to_timestamp_ntz(CONFIG_VALUE) as DAY0_TS
              from MIP.APP.APP_CONFIG
              where CONFIG_KEY = %s
            )
            select
              greatest(
                dateadd(day, -%s, current_timestamp()),
                coalesce((select DAY0_TS from cfg), '1970-01-01'::timestamp_ntz)
              ) as EFFECTIVE_START_TS,
              (select DAY0_TS from cfg) as DAY0_TS
            """,
            (DASHBOARD_DAY0_CONFIG_KEY, lookback_days),
        )
        effective_start_ts = effective_start_row.get("EFFECTIVE_START_TS")
        configured_day0_ts = effective_start_row.get("DAY0_TS")

        portfolio_kpis = serialize_row(
            _safe_row(
                cur,
                """
                with cfg as (
                  select
                    PORTFOLIO_ID,
                    IBKR_ACCOUNT_ID,
                    coalesce(IS_ACTIVE, true) as IS_ACTIVE
                  from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
                ),
                nav_window as (
                  select
                    IBKR_ACCOUNT_ID,
                    NET_LIQUIDATION_EUR,
                    SNAPSHOT_TS,
                    row_number() over (
                      partition by IBKR_ACCOUNT_ID
                      order by SNAPSHOT_TS asc
                    ) as RN_ASC,
                    row_number() over (
                      partition by IBKR_ACCOUNT_ID
                      order by SNAPSHOT_TS desc
                    ) as RN_DESC
                  from MIP.LIVE.BROKER_SNAPSHOTS
                  where SNAPSHOT_TYPE = 'NAV'
                    and SNAPSHOT_TS >= %s
                ),
                nav_rollup as (
                  select
                    IBKR_ACCOUNT_ID,
                    max(case when RN_ASC = 1 then NET_LIQUIDATION_EUR end) as START_EQUITY_EUR,
                    max(case when RN_DESC = 1 then NET_LIQUIDATION_EUR end) as END_EQUITY_EUR
                  from nav_window
                  group by IBKR_ACCOUNT_ID
                ),
                drawdown_rollup as (
                  select
                    IBKR_ACCOUNT_ID,
                    max(coalesce(DRAWDOWN_PCT, 0)) as MAX_DRAWDOWN
                  from MIP.LIVE.DRAWDOWN_LOG
                  where LOG_TS >= %s
                  group by IBKR_ACCOUNT_ID
                )
                select
                  count_if(cfg.IS_ACTIVE) as ACTIVE_PORTFOLIOS,
                  count(*) as PORTFOLIOS_TOTAL,
                  sum(coalesce(nav.START_EQUITY_EUR, 0)) as STARTING_CASH_TOTAL,
                  sum(coalesce(nav.END_EQUITY_EUR, 0)) as FINAL_EQUITY_TOTAL,
                  avg(
                    case
                      when coalesce(nav.START_EQUITY_EUR, 0) <> 0
                        then (coalesce(nav.END_EQUITY_EUR, 0) - nav.START_EQUITY_EUR) / nullif(nav.START_EQUITY_EUR, 0)
                      else null
                    end
                  ) as AVG_PORTFOLIO_RETURN,
                  case
                    when count(dd.MAX_DRAWDOWN) = 0 then null
                    else max(dd.MAX_DRAWDOWN)
                  end as MAX_DRAWDOWN
                from cfg
                left join nav_rollup nav
                  on nav.IBKR_ACCOUNT_ID = cfg.IBKR_ACCOUNT_ID
                left join drawdown_rollup dd
                  on dd.IBKR_ACCOUNT_ID = cfg.IBKR_ACCOUNT_ID
                where cfg.IS_ACTIVE
                """,
                (effective_start_ts, effective_start_ts),
            )
        )

        trade_kpis = serialize_row(
            _safe_row(
                cur,
                """
                select
                  count(*) as TRADE_COUNT,
                  null::number as WIN_TRADES,
                  null::float as AVG_PNL_PER_TRADE,
                  null::float as TOTAL_REALIZED_PNL
                from MIP.LIVE.LIVE_ORDERS
                where PORTFOLIO_ID in (
                  select PORTFOLIO_ID
                  from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
                  where coalesce(IS_ACTIVE, true)
                )
                  and upper(coalesce(STATUS, '')) in ('FILLED', 'PARTIAL_FILL')
                  and coalesce(QTY_FILLED, 0) > 0
                  and coalesce(LAST_UPDATED_AT, CREATED_AT) >= %s
                """,
                (effective_start_ts,),
            )
        )

        decision_quality = serialize_row(
            _safe_row(
                cur,
                """
                select
                  count(*) as OUTCOMES_N,
                  avg(REALIZED_RETURN) as EXPECTANCY,
                  avg(case when REALIZED_RETURN > 0 then 1 else 0 end) as PCT_POSITIVE,
                  avg(case when coalesce(HIT_FLAG, false) then 1 else 0 end) as PCT_HIT
                from MIP.APP.RECOMMENDATION_OUTCOMES ro
                where EVAL_STATUS in ('COMPLETED', 'SUCCESS')
                  and CALCULATED_AT >= %s
                  and exists (
                    select 1
                    from MIP.AGENT_OUT.ORDER_PROPOSALS op
                    join MIP.LIVE.LIVE_PORTFOLIO_CONFIG cfg
                      on cfg.PORTFOLIO_ID = op.PORTFOLIO_ID
                     and coalesce(cfg.IS_ACTIVE, true)
                    where op.RECOMMENDATION_ID = ro.RECOMMENDATION_ID
                  )
                """,
                (effective_start_ts,),
            )
        )

        equity_curve = serialize_rows(
            _safe_rows(
                cur,
                """
                with active_accounts as (
                  select distinct IBKR_ACCOUNT_ID
                  from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
                  where coalesce(IS_ACTIVE, true)
                ),
                latest_nav_per_day as (
                  select
                    date_trunc('day', s.SNAPSHOT_TS)::date as DAY,
                    s.IBKR_ACCOUNT_ID,
                    s.NET_LIQUIDATION_EUR,
                    row_number() over (
                      partition by date_trunc('day', s.SNAPSHOT_TS)::date, s.IBKR_ACCOUNT_ID
                      order by s.SNAPSHOT_TS desc
                    ) as RN
                  from MIP.LIVE.BROKER_SNAPSHOTS s
                  join active_accounts a
                    on a.IBKR_ACCOUNT_ID = s.IBKR_ACCOUNT_ID
                  where s.SNAPSHOT_TYPE = 'NAV'
                    and s.SNAPSHOT_TS >= %s
                )
                select
                  DAY,
                  sum(coalesce(NET_LIQUIDATION_EUR, 0)) as TOTAL_EQUITY
                from latest_nav_per_day
                where RN = 1
                group by DAY
                order by DAY
                """,
                (effective_start_ts,),
            )
        )

        quality_trend = serialize_rows(
            _safe_rows(
                cur,
                """
                select
                  date_trunc('week', CALCULATED_AT)::date as PERIOD,
                  avg(REALIZED_RETURN) as EXPECTANCY,
                  avg(case when REALIZED_RETURN > 0 then 1 else 0 end) as PCT_POSITIVE
                from MIP.APP.RECOMMENDATION_OUTCOMES ro
                where EVAL_STATUS in ('COMPLETED', 'SUCCESS')
                  and CALCULATED_AT >= %s
                  and exists (
                    select 1
                    from MIP.AGENT_OUT.ORDER_PROPOSALS op
                    join MIP.LIVE.LIVE_PORTFOLIO_CONFIG cfg
                      on cfg.PORTFOLIO_ID = op.PORTFOLIO_ID
                     and coalesce(cfg.IS_ACTIVE, true)
                    where op.RECOMMENDATION_ID = ro.RECOMMENDATION_ID
                  )
                group by 1
                order by 1
                """,
                (effective_start_ts,),
            )
        )

        selectivity_trend = serialize_rows(
            _safe_rows(
                cur,
                """
                with candidate_flow as (
                  select
                    date_trunc('week', EVENT_TS)::date as PERIOD,
                    sum(coalesce(try_to_number(DETAILS:candidate_count_raw::string), 0)) as CANDIDATES_CREATED,
                    sum(coalesce(try_to_number(DETAILS:candidate_count_trusted::string), 0)) as TRAINING_QUALIFIED,
                    sum(coalesce(try_to_number(DETAILS:proposed_count::string), 0)) as COMMITTEE_PASSED
                  from MIP.APP.MIP_AUDIT_LOG
                  where EVENT_NAME = 'SP_AGENT_PROPOSE_TRADES'
                    and EVENT_TS >= %s
                  group by 1
                ),
                proposal_flow as (
                  select
                    date_trunc('week', PROPOSED_AT)::date as PERIOD,
                    count(*) as PM_ACCEPTED,
                    count_if(STATUS = 'EXECUTED') as EXECUTED
                  from MIP.AGENT_OUT.ORDER_PROPOSALS
                  where PROPOSED_AT >= %s
                    and PORTFOLIO_ID in (
                      select PORTFOLIO_ID
                      from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
                      where coalesce(IS_ACTIVE, true)
                    )
                  group by 1
                )
                select
                  coalesce(c.PERIOD, p.PERIOD) as PERIOD,
                  coalesce(c.CANDIDATES_CREATED, 0) as CANDIDATES_CREATED,
                  coalesce(c.TRAINING_QUALIFIED, 0) as TRAINING_QUALIFIED,
                  coalesce(c.COMMITTEE_PASSED, 0) as COMMITTEE_PASSED,
                  coalesce(p.PM_ACCEPTED, 0) as PM_ACCEPTED,
                  coalesce(p.EXECUTED, 0) as EXECUTED
                from candidate_flow c
                full outer join proposal_flow p on p.PERIOD = c.PERIOD
                order by 1
                """,
                (effective_start_ts, effective_start_ts),
            )
        )

        snowflake_cost_trend = _safe_rows(
            cur,
            """
            select
              date_trunc('month', START_TIME)::date as PERIOD_MONTH,
              sum(coalesce(CREDITS_USED_COMPUTE, 0) + coalesce(CREDITS_USED_CLOUD_SERVICES, 0)) as CREDITS_USED
            from SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
            where WAREHOUSE_NAME = 'MIP_WH_XS'
              and START_TIME >= dateadd(month, -11, current_date())
            group by 1
            order by 1
            """
        )
        if not snowflake_cost_trend:
            # Fallback for roles without SNOWFLAKE.ACCOUNT_USAGE visibility.
            snowflake_cost_trend = _safe_rows(
                cur,
                """
                select
                  date_trunc('month', START_TIME)::date as PERIOD_MONTH,
                  sum(coalesce(CREDITS_USED, 0)) as CREDITS_USED
                from table(
                  information_schema.warehouse_metering_history(
                    date_range_start => dateadd(month, -11, current_date()),
                    date_range_end => current_date()
                  )
                )
                where WAREHOUSE_NAME = 'MIP_WH_XS'
                group by 1
                order by 1
                """,
            )
        snowflake_cost_trend = serialize_rows(snowflake_cost_trend)

        committee_influence = serialize_row(
            _safe_row(
                cur,
                """
                select
                  count(*) as TOTAL_DECISIONS,
                  count_if(
                    try_parse_json(RATIONALE):committee is not null
                    or try_parse_json(RATIONALE):live_committee is not null
                  ) as COMMITTEE_INFLUENCED,
                  count_if(
                    coalesce(
                      try_parse_json(RATIONALE):committee:should_enter::boolean,
                      try_parse_json(RATIONALE):live_committee:should_enter::boolean,
                      true
                    ) = false
                  ) as BLOCKED_BY_COMMITTEE,
                  count_if(
                    abs(
                      coalesce(
                        try_to_double(try_parse_json(RATIONALE):committee:size_factor::string),
                        try_to_double(try_parse_json(RATIONALE):live_committee:size_factor::string),
                        1
                      ) - 1
                    ) > 0.001
                  ) as RESIZED_BY_COMMITTEE
                from MIP.AGENT_OUT.ORDER_PROPOSALS
                where PROPOSED_AT >= %s
                  and PORTFOLIO_ID in (
                    select PORTFOLIO_ID
                    from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
                    where coalesce(IS_ACTIVE, true)
                  )
                """,
                (effective_start_ts,),
            )
        )

        role_contribution = serialize_rows(
            _safe_rows(
                cur,
                """
                select
                  coalesce(value:role::string, value:agent::string, 'UNKNOWN') as ROLE_NAME,
                  count(*) as INFLUENCE_COUNT
                from MIP.AGENT_OUT.ORDER_PROPOSALS op,
                     lateral flatten(
                       input => coalesce(
                         try_parse_json(op.RATIONALE):committee:agent_dialogue,
                         try_parse_json(op.RATIONALE):live_committee:agent_dialogue
                       )
                     )
                where op.PROPOSED_AT >= %s
                  and op.PORTFOLIO_ID in (
                    select PORTFOLIO_ID
                    from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
                    where coalesce(IS_ACTIVE, true)
                  )
                group by 1
                order by INFLUENCE_COUNT desc
                limit 12
                """,
                (effective_start_ts,),
            )
        )

        pw_effectiveness = serialize_row(
            _safe_row(
                cur,
                """
                select
                  count(*) as PW_SCENARIO_ROWS,
                  avg(case when OUTPERFORMED then 1 else 0 end) as PW_OUTPERFORM_RATE,
                  avg(PNL_DELTA) as PW_AVG_PNL_DELTA
                from MIP.MART.V_PARALLEL_WORLD_DIFF
                where AS_OF_TS >= %s
                """,
                (effective_start_ts,),
            )
        )

        training_summary = serialize_row(
            _safe_row(
                cur,
                """
                with t as (
                  select * from MIP.AGENT_OUT.TRAINING_DIGEST_SNAPSHOT
                  where SCOPE = 'GLOBAL_TRAINING'
                    and CREATED_AT >= %s
                )
                select
                  count(*) as TRAINING_EVENTS,
                  avg(coalesce(try_to_double(SNAPSHOT_JSON:trust:trusted_count::string), 0)) as AVG_TRUSTED_COUNT
                from t
                """,
                (effective_start_ts,),
            )
        )

        news_summary = serialize_row(
            _safe_row(
                cur,
                """
                select
                  count(*) as NEWS_DECISIONS,
                  avg(abs(coalesce(try_to_double(try_parse_json(RATIONALE):news_score_adj::string), 0))) as AVG_NEWS_ADJ_MAGNITUDE,
                  count_if(coalesce(try_parse_json(RATIONALE):news_block_new_entry::boolean, false) = true) as NEWS_BLOCK_COUNT
                from MIP.AGENT_OUT.ORDER_PROPOSALS
                where PROPOSED_AT >= %s
                  and PORTFOLIO_ID in (
                    select PORTFOLIO_ID
                    from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
                    where coalesce(IS_ACTIVE, true)
                  )
                  and (
                    try_parse_json(RATIONALE):news_score_adj is not null
                    or try_parse_json(RATIONALE):news_block_new_entry is not null
                    or try_parse_json(SOURCE_SIGNALS):news_context is not null
                  )
                """,
                (effective_start_ts,),
            )
        )

        target_realism = serialize_rows(
            _safe_rows(
                cur,
                """
                select
                  SYMBOL,
                  MARKET_TYPE,
                  avg(coalesce(PATTERN_TARGET, TARGET_RETURN)) as DEFAULT_TARGET,
                  avg(coalesce(TARGET_RETURN, EFFECTIVE_TARGET)) as REALISTIC_TARGET,
                  avg(UNREALIZED_RETURN) as REALIZED_RETURN_PROXY,
                  avg(case when coalesce(UNREALIZED_RETURN, -1e9) >= coalesce(TARGET_RETURN, EFFECTIVE_TARGET, 1e9) then 1 else 0 end) as TARGET_HIT_RATE
                from MIP.APP.EARLY_EXIT_LOG
                where DECISION_TS >= %s
                group by SYMBOL, MARKET_TYPE
                qualify row_number() over (order by count(*) desc, SYMBOL) <= 20
                """,
                (effective_start_ts,),
            )
        )

        learning_ledger = serialize_row(
            _safe_row(
                cur,
                """
                select
                  count(*) as LEDGER_EVENTS,
                  count(distinct RUN_ID) as RUNS_TOUCHED,
                  count_if(EVENT_TYPE = 'TRAINING_EVENT') as TRAINING_EVENTS,
                  count_if(EVENT_TYPE = 'LIVE_EVENT') as LIVE_EVENTS,
                  count_if(
                    INFLUENCE_DELTA:news_context_state is not null
                    or INFLUENCE_DELTA:news_event_shock_flag is not null
                  ) as NEWS_INFLUENCED_EVENTS
                from MIP.AGENT_OUT.LEARNING_DECISION_LEDGER
                where EVENT_TS >= %s
                """,
                (effective_start_ts,),
            )
        )

        funnel = serialize_row(
            _safe_row(
                cur,
                """
                with s as (
                  select count(*) as SIGNALS
                  from MIP.APP.RECOMMENDATION_LOG
                  where TS >= %s
                ),
                p as (
                  select
                    count(*) as PROPOSALS,
                    count_if(STATUS in ('APPROVED', 'EXECUTED')) as PM_ACCEPTED,
                    count_if(STATUS = 'EXECUTED') as EXECUTED
                  from MIP.AGENT_OUT.ORDER_PROPOSALS
                  where PROPOSED_AT >= %s
                    and PORTFOLIO_ID in (
                      select PORTFOLIO_ID
                      from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
                      where coalesce(IS_ACTIVE, true)
                    )
                ),
                c as (
                  select count_if(COMPLIANCE_STATUS = 'APPROVED') as COMPLIANCE_APPROVED
                  from MIP.LIVE.LIVE_ACTIONS
                  where CREATED_AT >= %s
                    and PORTFOLIO_ID in (
                      select PORTFOLIO_ID
                      from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
                      where coalesce(IS_ACTIVE, true)
                    )
                ),
                o as (
                  select count_if(coalesce(REALIZED_RETURN, 0) > 0) as SUCCESSFUL_OUTCOMES
                  from MIP.APP.RECOMMENDATION_OUTCOMES ro
                  where EVAL_STATUS in ('COMPLETED', 'SUCCESS')
                    and CALCULATED_AT >= %s
                    and exists (
                      select 1
                      from MIP.AGENT_OUT.ORDER_PROPOSALS op
                      join MIP.LIVE.LIVE_PORTFOLIO_CONFIG cfg
                        on cfg.PORTFOLIO_ID = op.PORTFOLIO_ID
                       and coalesce(cfg.IS_ACTIVE, true)
                      where op.RECOMMENDATION_ID = ro.RECOMMENDATION_ID
                    )
                )
                select
                  s.SIGNALS,
                  p.PROPOSALS,
                  p.PM_ACCEPTED,
                  c.COMPLIANCE_APPROVED,
                  p.EXECUTED,
                  o.SUCCESSFUL_OUTCOMES
                from s cross join p cross join c cross join o
                """,
                (effective_start_ts, effective_start_ts, effective_start_ts, effective_start_ts),
            )
        )

        current_month_snowflake_cost = 0.0
        current_month_start = datetime.now(timezone.utc).date().replace(day=1)
        current_month_key = current_month_start.isoformat()

        credits_by_month: dict[str, float] = {}
        for r in snowflake_cost_trend:
            period_month = r.get("PERIOD_MONTH")
            period_month_key = str(period_month)[:10] if period_month is not None else None
            if period_month_key:
                credits_by_month[period_month_key] = _to_float(r.get("CREDITS_USED")) or 0.0

        def _shift_month(month_start, delta_months: int):
            month_index = (month_start.year * 12 + (month_start.month - 1)) + delta_months
            year = month_index // 12
            month = (month_index % 12) + 1
            return month_start.replace(year=year, month=month, day=1)

        monthly_cost_rows = []
        for delta in range(-11, 1):
            period_month = _shift_month(current_month_start, delta)
            period_month_key = period_month.isoformat()
            credits = credits_by_month.get(period_month_key, 0.0)
            snowflake_cost = credits * DEFAULT_CREDIT_USD
            total_cost = snowflake_cost + FIXED_MONTHLY_COST_USD
            monthly_cost_rows.append(
                {
                    "period_month": period_month,
                    "credits_used": credits,
                    "snowflake_cost_usd": snowflake_cost,
                    "fixed_tools_usd": FIXED_MONTHLY_COST_USD,
                    "total_cost_usd": total_cost,
                }
            )
            if period_month_key == current_month_key:
                current_month_snowflake_cost = snowflake_cost

        nav_start_total = _to_float(portfolio_kpis.get("STARTING_CASH_TOTAL")) or 0.0
        nav_end_total = _to_float(portfolio_kpis.get("FINAL_EQUITY_TOTAL")) or 0.0
        nav_period_pnl = nav_end_total - nav_start_total
        total_realized_pnl = _to_float(trade_kpis.get("TOTAL_REALIZED_PNL"))
        if total_realized_pnl is None:
            total_realized_pnl = nav_period_pnl
        monthly_total_cost = current_month_snowflake_cost + FIXED_MONTHLY_COST_USD
        trade_count = _to_int(trade_kpis.get("TRADE_COUNT"))
        outcomes_n = _to_int(decision_quality.get("OUTCOMES_N"))
        win_rate = (_to_float(decision_quality.get("PCT_POSITIVE")) or 0.0) if outcomes_n > 0 else None
        decision_quality_score = (
            ((_to_float(decision_quality.get("PCT_POSITIVE")) or 0.0) * 50)
            + ((_to_float(decision_quality.get("PCT_HIT")) or 0.0) * 30)
            + (max(-0.05, min(0.05, (_to_float(decision_quality.get("EXPECTANCY")) or 0.0))) * 400)
        )
        decision_quality_score = max(0.0, min(100.0, decision_quality_score))
        cost_efficiency = total_realized_pnl / monthly_total_cost if monthly_total_cost > 0 else 0.0

        max_drawdown = _to_float(portfolio_kpis.get("MAX_DRAWDOWN"))
        avg_portfolio_return = _to_float(portfolio_kpis.get("AVG_PORTFOLIO_RETURN")) or 0.0
        reliability_proxy = 1.0 if _to_int(portfolio_kpis.get("PORTFOLIOS_TOTAL")) > 0 else 0.0
        win_rate_for_score = win_rate if win_rate is not None else 0.0
        max_drawdown_for_score = max_drawdown if max_drawdown is not None else 0.0
        performance_score = (
            max(0.0, min(100.0, 50 + avg_portfolio_return * 600))
            + (win_rate_for_score * 100)
            + (decision_quality_score)
            + (max(0.0, 100 - (max_drawdown_for_score * 300)))
            + (max(0.0, min(100.0, (cost_efficiency + 1) * 40)))
            + (reliability_proxy * 100)
        ) / 6

        committee_total = _to_int(committee_influence.get("TOTAL_DECISIONS"))
        committee_influenced = _to_int(committee_influence.get("COMMITTEE_INFLUENCED"))
        committee_influence_rate = (committee_influenced / committee_total) if committee_total > 0 else 0.0

        pw_influence_rate = (_to_float(pw_effectiveness.get("PW_OUTPERFORM_RATE")) or 0.0)
        training_influence_rate = min(1.0, (_to_int(training_summary.get("TRAINING_EVENTS")) / max(1, lookback_days)))
        news_influence_rate = (
            (_to_int(news_summary.get("NEWS_DECISIONS")) / committee_total)
            if committee_total > 0
            else 0.0
        )

        verdict = "MIP is stable but needs more outcome density."
        if performance_score >= 70 and cost_efficiency > 0:
            verdict = "MIP is improving with positive cost-adjusted performance."
        elif performance_score < 45:
            verdict = "MIP is underperforming and requires targeted intervention."

        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "lookback_days": lookback_days,
            "definitions": {
                "fixed_monthly_tools_cost_usd": FIXED_MONTHLY_COST_USD,
                "snowflake_credit_cost_assumption_usd": DEFAULT_CREDIT_USD,
                "baseline_policy_note": "Uses intervention, cohort, and counterfactual proxies when legacy baseline replay is unavailable.",
                "dashboard_day0_config_key": DASHBOARD_DAY0_CONFIG_KEY,
                "dashboard_day0_ts": configured_day0_ts,
                "effective_start_ts": effective_start_ts,
            },
            "executive": {
                "kpis": {
                    "mip_performance_score": round(performance_score, 2),
                    "period_realized_pnl": round(total_realized_pnl, 2),
                    "win_rate": round(win_rate, 4) if win_rate is not None else None,
                    "avg_return_per_trade": _to_float(decision_quality.get("EXPECTANCY")),
                    "max_drawdown": round(max_drawdown, 4) if max_drawdown is not None else None,
                    "decision_quality_score": round(decision_quality_score, 2),
                    "monthly_cost_usd": round(monthly_total_cost, 2),
                    "monthly_snowflake_cost_usd": round(current_month_snowflake_cost, 2),
                    "monthly_fixed_tools_cost_usd": FIXED_MONTHLY_COST_USD,
                    "cost_efficiency_ratio": round(cost_efficiency, 4),
                },
                "trends": {
                    "equity_curve": equity_curve,
                    "monthly_cost_trend": monthly_cost_rows,
                    "decision_quality_trend": quality_trend,
                    "selectivity_trend": selectivity_trend,
                },
                "intelligence_impact": {
                    "committee": {
                        "influence_rate": round(committee_influence_rate, 4),
                        "blocked_count": _to_int(committee_influence.get("BLOCKED_BY_COMMITTEE")),
                        "resized_count": _to_int(committee_influence.get("RESIZED_BY_COMMITTEE")),
                        "status": "positive" if committee_influence_rate >= 0.4 else "under_review",
                    },
                    "parallel_worlds": {
                        "influence_rate": round(pw_influence_rate, 4),
                        "avg_pnl_delta": _to_float(pw_effectiveness.get("PW_AVG_PNL_DELTA")) or 0.0,
                        "status": "positive" if (_to_float(pw_effectiveness.get("PW_AVG_PNL_DELTA")) or 0.0) > 0 else "neutral",
                    },
                    "training": {
                        "influence_rate": round(training_influence_rate, 4),
                        "avg_trusted_count": _to_float(training_summary.get("AVG_TRUSTED_COUNT")) or 0.0,
                        "status": "positive" if (_to_int(training_summary.get("TRAINING_EVENTS")) > 0) else "under_review",
                    },
                    "news": {
                        "influence_rate": round(news_influence_rate, 4),
                        "blocked_new_entries": _to_int(news_summary.get("NEWS_BLOCK_COUNT")),
                        "status": "positive" if (_to_int(news_summary.get("NEWS_DECISIONS")) > 0) else "neutral",
                    },
                },
                "verdict": verdict,
            },
            "diagnostics": {
                "decision_funnel": {
                    "signals": _to_int(funnel.get("SIGNALS")),
                    "proposals": _to_int(funnel.get("PROPOSALS")),
                    "pm_accepted": _to_int(funnel.get("PM_ACCEPTED")),
                    "compliance_approved": _to_int(funnel.get("COMPLIANCE_APPROVED")),
                    "executed": _to_int(funnel.get("EXECUTED")),
                    "successful_outcomes": _to_int(funnel.get("SUCCESSFUL_OUTCOMES")),
                },
                "committee_effectiveness": {
                    "overall": committee_influence,
                    "role_contribution": role_contribution,
                },
                "parallel_worlds_effectiveness": pw_effectiveness,
                "training_influence_effectiveness": training_summary,
                "news_influence_effectiveness": news_summary,
                "target_realism_analysis": target_realism,
                "cost_attribution": {
                    "monthly_cost_trend": monthly_cost_rows,
                    "current_month_total_usd": round(monthly_total_cost, 2),
                    "current_month_snowflake_usd": round(current_month_snowflake_cost, 2),
                    "current_month_fixed_tools_usd": FIXED_MONTHLY_COST_USD,
                    "cost_per_executed_trade_usd": (
                        round(monthly_total_cost / max(1, _to_int(funnel.get("EXECUTED"))), 2)
                    ),
                    "cost_per_profitable_trade_usd": (
                        round(monthly_total_cost / max(1, _to_int(funnel.get("SUCCESSFUL_OUTCOMES"))), 2)
                    ),
                    "cost_per_committee_review_usd": (
                        round(monthly_total_cost / max(1, committee_influenced), 2)
                    ),
                },
                "learning_to_decision_ledger": learning_ledger,
            },
        }
    finally:
        conn.close()
