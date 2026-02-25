-- 352_sp_intra_build_state_transitions.sql
-- Purpose: Phase 2a optional state transition builder from STATE_SNAPSHOT_15M.
-- Writes one row per bucket-change event with prior regime duration.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_INTRA_BUILD_STATE_TRANSITIONS(
    P_START_TS timestamp_ntz,
    P_END_TS timestamp_ntz,
    P_METRIC_VERSION string default 'v1_1',
    P_BUCKET_VERSION string default 'v1'
)
returns variant
language sql
execute as caller
as
$$
declare
    v_start_ts timestamp_ntz;
    v_end_ts timestamp_ntz;
    v_rows_merged number := 0;
begin
    v_end_ts := coalesce(:P_END_TS, current_timestamp());
    v_start_ts := coalesce(:P_START_TS, dateadd(day, -30, :v_end_ts));

    merge into MIP.APP.STATE_TRANSITIONS t
    using (
        with base as (
            select
                MARKET_TYPE,
                SYMBOL,
                INTERVAL_MINUTES,
                TS,
                STATE_BUCKET_ID,
                lag(STATE_BUCKET_ID) over (
                    partition by MARKET_TYPE, SYMBOL, INTERVAL_MINUTES
                    order by TS
                ) as PREV_BUCKET,
                lag(TS) over (
                    partition by MARKET_TYPE, SYMBOL, INTERVAL_MINUTES
                    order by TS
                ) as PREV_TS
            from MIP.APP.STATE_SNAPSHOT_15M
            where INTERVAL_MINUTES = 15
              and METRIC_VERSION = :P_METRIC_VERSION
              and BUCKET_VERSION = :P_BUCKET_VERSION
              and TS >= dateadd(day, -2, :v_start_ts)
              and TS <= :v_end_ts
        ),
        run_marked as (
            select
                MARKET_TYPE,
                SYMBOL,
                INTERVAL_MINUTES,
                TS,
                STATE_BUCKET_ID,
                PREV_BUCKET,
                PREV_TS,
                case
                    when PREV_BUCKET is null then 1
                    when PREV_BUCKET <> STATE_BUCKET_ID then 1
                    else 0
                end as IS_NEW_RUN
            from base
        ),
        run_numbered as (
            select
                MARKET_TYPE,
                SYMBOL,
                INTERVAL_MINUTES,
                TS,
                STATE_BUCKET_ID,
                sum(IS_NEW_RUN) over (
                    partition by MARKET_TYPE, SYMBOL, INTERVAL_MINUTES
                    order by TS
                    rows between unbounded preceding and current row
                ) as RUN_ID
            from run_marked
        ),
        run_summary as (
            select
                MARKET_TYPE,
                SYMBOL,
                INTERVAL_MINUTES,
                RUN_ID,
                min(TS) as RUN_START_TS,
                max(TS) as RUN_END_TS,
                any_value(STATE_BUCKET_ID) as RUN_BUCKET,
                count(*) as RUN_BARS
            from run_numbered
            group by 1, 2, 3, 4
        ),
        transitions as (
            select
                r1.MARKET_TYPE,
                r1.SYMBOL,
                r1.INTERVAL_MINUTES,
                r1.RUN_END_TS as TS_FROM,
                r2.RUN_START_TS as TS_TO,
                r1.RUN_BUCKET as FROM_STATE_BUCKET_ID,
                r2.RUN_BUCKET as TO_STATE_BUCKET_ID,
                r1.RUN_BARS as DURATION_BARS,
                :P_METRIC_VERSION as METRIC_VERSION,
                :P_BUCKET_VERSION as BUCKET_VERSION,
                current_timestamp() as CALCULATED_AT
            from run_summary r1
            join run_summary r2
              on r2.MARKET_TYPE = r1.MARKET_TYPE
             and r2.SYMBOL = r1.SYMBOL
             and r2.INTERVAL_MINUTES = r1.INTERVAL_MINUTES
             and r2.RUN_ID = r1.RUN_ID + 1
            where r1.RUN_END_TS >= :v_start_ts
              and r1.RUN_END_TS <= :v_end_ts
        )
        select * from transitions
    ) s
    on t.MARKET_TYPE = s.MARKET_TYPE
   and t.SYMBOL = s.SYMBOL
   and t.INTERVAL_MINUTES = s.INTERVAL_MINUTES
   and t.TS_FROM = s.TS_FROM
   and t.TS_TO = s.TS_TO
    when matched then update set
        t.FROM_STATE_BUCKET_ID = s.FROM_STATE_BUCKET_ID,
        t.TO_STATE_BUCKET_ID = s.TO_STATE_BUCKET_ID,
        t.DURATION_BARS = s.DURATION_BARS,
        t.METRIC_VERSION = s.METRIC_VERSION,
        t.BUCKET_VERSION = s.BUCKET_VERSION,
        t.CALCULATED_AT = s.CALCULATED_AT
    when not matched then insert (
        MARKET_TYPE, SYMBOL, INTERVAL_MINUTES, TS_FROM, TS_TO,
        FROM_STATE_BUCKET_ID, TO_STATE_BUCKET_ID, DURATION_BARS,
        METRIC_VERSION, BUCKET_VERSION, CALCULATED_AT
    ) values (
        s.MARKET_TYPE, s.SYMBOL, s.INTERVAL_MINUTES, s.TS_FROM, s.TS_TO,
        s.FROM_STATE_BUCKET_ID, s.TO_STATE_BUCKET_ID, s.DURATION_BARS,
        s.METRIC_VERSION, s.BUCKET_VERSION, s.CALCULATED_AT
    );

    v_rows_merged := sqlrowcount;

    return object_construct(
        'status', 'SUCCESS',
        'start_ts', :v_start_ts,
        'end_ts', :v_end_ts,
        'metric_version', :P_METRIC_VERSION,
        'bucket_version', :P_BUCKET_VERSION,
        'rows_merged', :v_rows_merged
    );
end;
$$;
