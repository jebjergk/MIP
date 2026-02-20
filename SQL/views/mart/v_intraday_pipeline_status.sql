-- v_intraday_pipeline_status.sql
-- Purpose: Operational health view for the intraday subsystem.
-- Shows latest run status, bar coverage, signal/outcome counts, cost tracking.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_INTRADAY_PIPELINE_STATUS as
with cfg as (
    select coalesce(
        try_to_number((select CONFIG_VALUE from MIP.APP.APP_CONFIG
                       where CONFIG_KEY = 'INTRADAY_INTERVAL_MINUTES')),
        60
    ) as INTERVAL_MINUTES,
    coalesce(
        try_to_boolean((select CONFIG_VALUE from MIP.APP.APP_CONFIG
                        where CONFIG_KEY = 'INTRADAY_ENABLED')),
        false
    ) as IS_ENABLED
),
latest_run as (
    select *
    from MIP.APP.INTRADAY_PIPELINE_RUN_LOG
    qualify row_number() over (order by STARTED_AT desc) = 1
),
bar_stats as (
    select
        count(*) as TOTAL_INTRADAY_BARS,
        count(distinct SYMBOL) as SYMBOLS_WITH_DATA,
        min(TS) as EARLIEST_BAR_TS,
        max(TS) as LATEST_BAR_TS
    from MIP.MART.MARKET_BARS b
    cross join cfg c
    where b.INTERVAL_MINUTES = c.INTERVAL_MINUTES
),
signal_stats as (
    select
        count(*) as TOTAL_INTRADAY_SIGNALS,
        count(distinct PATTERN_ID) as ACTIVE_PATTERNS,
        count(distinct SYMBOL) as SYMBOLS_WITH_SIGNALS,
        min(TS) as EARLIEST_SIGNAL_TS,
        max(TS) as LATEST_SIGNAL_TS
    from MIP.APP.RECOMMENDATION_LOG r
    cross join cfg c
    where r.INTERVAL_MINUTES = c.INTERVAL_MINUTES
),
outcome_stats as (
    select
        count(*) as TOTAL_INTRADAY_OUTCOMES,
        count_if(EVAL_STATUS = 'SUCCESS') as EVALUATED_OUTCOMES,
        count_if(NET_HIT_FLAG and EVAL_STATUS = 'SUCCESS') as NET_HITS
    from MIP.MART.V_INTRADAY_OUTCOMES_FEE_ADJUSTED
),
run_stats as (
    select
        count(*) as TOTAL_RUNS,
        count_if(STATUS = 'SUCCESS') as SUCCESSFUL_RUNS,
        count_if(STATUS = 'FAIL') as FAILED_RUNS,
        sum(COMPUTE_SECONDS) as TOTAL_COMPUTE_SECONDS,
        max(STARTED_AT) as LAST_RUN_AT
    from MIP.APP.INTRADAY_PIPELINE_RUN_LOG
    where STARTED_AT >= dateadd(day, -7, current_timestamp())
)
select
    cfg.IS_ENABLED,
    cfg.INTERVAL_MINUTES,
    lr.RUN_ID as LATEST_RUN_ID,
    lr.STATUS as LATEST_RUN_STATUS,
    lr.STARTED_AT as LATEST_RUN_STARTED_AT,
    lr.COMPLETED_AT as LATEST_RUN_COMPLETED_AT,
    lr.BARS_INGESTED as LATEST_BARS_INGESTED,
    lr.SIGNALS_GENERATED as LATEST_SIGNALS_GENERATED,
    lr.OUTCOMES_EVALUATED as LATEST_OUTCOMES_EVALUATED,
    bs.TOTAL_INTRADAY_BARS,
    bs.SYMBOLS_WITH_DATA,
    bs.EARLIEST_BAR_TS,
    bs.LATEST_BAR_TS,
    ss.TOTAL_INTRADAY_SIGNALS,
    ss.ACTIVE_PATTERNS,
    ss.SYMBOLS_WITH_SIGNALS,
    os.TOTAL_INTRADAY_OUTCOMES,
    os.EVALUATED_OUTCOMES,
    os.NET_HITS,
    rs.TOTAL_RUNS as RUNS_LAST_7_DAYS,
    rs.SUCCESSFUL_RUNS as SUCCESSFUL_RUNS_LAST_7_DAYS,
    rs.FAILED_RUNS as FAILED_RUNS_LAST_7_DAYS,
    rs.TOTAL_COMPUTE_SECONDS as COMPUTE_SECONDS_LAST_7_DAYS
from cfg
left join latest_run lr on 1=1
left join bar_stats bs on 1=1
left join signal_stats ss on 1=1
left join outcome_stats os on 1=1
left join run_stats rs on 1=1;
