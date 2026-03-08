use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.APP.V_LIVE_ACTION_PARALLEL_WORLDS_EVIDENCE as
with latest_pw_asof as (
    select
        PORTFOLIO_ID,
        max(AS_OF_TS) as PW_AS_OF_TS
    from MIP.MART.V_PARALLEL_WORLD_DIFF
    group by PORTFOLIO_ID
),
pw_top_outperformers as (
    with ranked as (
        select
            d.PORTFOLIO_ID,
            d.AS_OF_TS as PW_AS_OF_TS,
            d.SCENARIO_ID,
            d.SCENARIO_NAME,
            d.SCENARIO_TYPE,
            d.PNL_DELTA,
            d.RETURN_PCT_DELTA,
            d.DRAWDOWN_DELTA,
            row_number() over (
                partition by d.PORTFOLIO_ID, d.AS_OF_TS
                order by d.PNL_DELTA desc
            ) as RN
        from MIP.MART.V_PARALLEL_WORLD_DIFF d
        join latest_pw_asof l
          on l.PORTFOLIO_ID = d.PORTFOLIO_ID
         and l.PW_AS_OF_TS = d.AS_OF_TS
        where d.OUTPERFORMED = true
    )
    select
        PORTFOLIO_ID,
        PW_AS_OF_TS,
        array_agg(
            object_construct(
                'scenario_id', SCENARIO_ID,
                'scenario_name', SCENARIO_NAME,
                'scenario_type', SCENARIO_TYPE,
                'pnl_delta', PNL_DELTA,
                'return_delta', RETURN_PCT_DELTA,
                'drawdown_delta', DRAWDOWN_DELTA
            )
        ) within group (order by PNL_DELTA desc) as TOP_OUTPERFORMERS
    from ranked
    where RN <= 3
    group by PORTFOLIO_ID, PW_AS_OF_TS
),
pw_latest_recs as (
    with ranked as (
        select
            r.PORTFOLIO_ID,
            r.AS_OF_TS as PW_AS_OF_TS,
            r.REC_ID,
            r.RECOMMENDATION_TYPE,
            r.DOMAIN,
            r.SWEEP_FAMILY,
            r.PARAMETER_NAME,
            r.CURRENT_VALUE,
            r.RECOMMENDED_VALUE,
            r.EXPECTED_DAILY_DELTA,
            r.EXPECTED_CUMULATIVE_DELTA,
            r.CONFIDENCE_CLASS,
            r.APPROVAL_STATUS,
            row_number() over (
                partition by r.PORTFOLIO_ID, r.AS_OF_TS
                order by r.EXPECTED_DAILY_DELTA desc
            ) as RN
        from MIP.APP.PARALLEL_WORLD_RECOMMENDATION r
        join (
            select PORTFOLIO_ID, max(AS_OF_TS) as AS_OF_TS
            from MIP.APP.PARALLEL_WORLD_RECOMMENDATION
            group by PORTFOLIO_ID
        ) mx
          on mx.PORTFOLIO_ID = r.PORTFOLIO_ID
         and mx.AS_OF_TS = r.AS_OF_TS
        where r.APPROVAL_STATUS in ('NOT_REVIEWED', 'APPROVED')
    )
    select
        PORTFOLIO_ID,
        PW_AS_OF_TS,
        array_agg(
            object_construct(
                'rec_id', REC_ID,
                'recommendation_type', RECOMMENDATION_TYPE,
                'domain', DOMAIN,
                'sweep_family', SWEEP_FAMILY,
                'parameter_name', PARAMETER_NAME,
                'current_value', CURRENT_VALUE,
                'recommended_value', RECOMMENDED_VALUE,
                'expected_daily_delta', EXPECTED_DAILY_DELTA,
                'expected_cumulative_delta', EXPECTED_CUMULATIVE_DELTA,
                'confidence_class', CONFIDENCE_CLASS,
                'approval_status', APPROVAL_STATUS
            )
        ) within group (order by EXPECTED_DAILY_DELTA desc) as TOP_RECOMMENDATIONS
    from ranked
    where RN <= 3
    group by PORTFOLIO_ID, PW_AS_OF_TS
)
select
    a.ACTION_ID,
    a.PORTFOLIO_ID,
    l.PW_AS_OF_TS,
    coalesce(o.TOP_OUTPERFORMERS, array_construct()) as TOP_OUTPERFORMERS,
    coalesce(r.TOP_RECOMMENDATIONS, array_construct()) as TOP_RECOMMENDATIONS,
    object_construct(
        'scope', 'PORTFOLIO_LEVEL',
        'pw_as_of_ts', l.PW_AS_OF_TS,
        'outperformer_count', coalesce(array_size(o.TOP_OUTPERFORMERS), 0),
        'recommendation_count', coalesce(array_size(r.TOP_RECOMMENDATIONS), 0),
        'note', 'Parallel Worlds evidence is portfolio-scoped and attached as committee context.'
    ) as EVIDENCE_SUMMARY
from MIP.LIVE.LIVE_ACTIONS a
left join latest_pw_asof l
  on l.PORTFOLIO_ID = a.PORTFOLIO_ID
left join pw_top_outperformers o
  on o.PORTFOLIO_ID = a.PORTFOLIO_ID
 and o.PW_AS_OF_TS = l.PW_AS_OF_TS
left join pw_latest_recs r
  on r.PORTFOLIO_ID = a.PORTFOLIO_ID
 and r.PW_AS_OF_TS = l.PW_AS_OF_TS;

