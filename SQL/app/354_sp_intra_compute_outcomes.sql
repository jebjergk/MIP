-- 354_sp_intra_compute_outcomes.sql
-- Purpose: Phase 4 outcomes computation for INTRA_SIGNALS -> INTRA_OUTCOMES.
-- Enforces SIGNAL_ID resolution via SIGNAL_NK_HASH and uses active INTRA_HORIZON_DEF bars.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_INTRA_COMPUTE_OUTCOMES(
    P_START_TS timestamp_ntz,
    P_END_TS timestamp_ntz,
    P_METRIC_VERSION string default 'v1_1',
    P_BUCKET_VERSION string default 'v1',
    P_PATTERN_SET string default 'ALL',
    P_MIN_RETURN_THRESHOLD float default 0.0
)
returns variant
language sql
execute as caller
as
$$
declare
    v_start_ts timestamp_ntz;
    v_end_ts timestamp_ntz;
    v_thr float := coalesce(:P_MIN_RETURN_THRESHOLD, 0.0);
    v_rows_merged number := 0;
begin
    v_end_ts := coalesce(:P_END_TS, current_timestamp());
    v_start_ts := coalesce(:P_START_TS, dateadd(day, -30, :v_end_ts));

    merge into MIP.APP.INTRA_OUTCOMES t
    using (
        with active_horizons as (
            select HORIZON_BARS
            from MIP.APP.INTRA_HORIZON_DEF
            where IS_ACTIVE = true
        ),
        signals as (
            select
                s.SIGNAL_ID,
                s.SIGNAL_NK_HASH,
                s.PATTERN_ID,
                s.SYMBOL,
                s.MARKET_TYPE,
                s.INTERVAL_MINUTES,
                s.SIGNAL_TS,
                s.SIGNAL_SIDE,
                s.METRIC_VERSION,
                s.BUCKET_VERSION
            from MIP.APP.INTRA_SIGNALS s
            where s.INTERVAL_MINUTES = 15
              and s.SIGNAL_TS between :v_start_ts and :v_end_ts
              and s.METRIC_VERSION = :P_METRIC_VERSION
              and s.BUCKET_VERSION = :P_BUCKET_VERSION
              and (
                    :P_PATTERN_SET is null
                    or upper(trim(:P_PATTERN_SET)) = 'ALL'
                    or s.PATTERN_ID in (
                        select try_to_number(trim(value))
                        from table(split_to_table(:P_PATTERN_SET, ','))
                        where try_to_number(trim(value)) is not null
                    )
              )
        ),
        entry_bars as (
            select
                s.*,
                b.CLOSE::float as ENTRY_PX
            from signals s
            left join MIP.MART.MARKET_BARS b
              on b.MARKET_TYPE = s.MARKET_TYPE
             and b.SYMBOL = s.SYMBOL
             and b.INTERVAL_MINUTES = s.INTERVAL_MINUTES
             and b.TS = s.SIGNAL_TS
        ),
        signal_horizons as (
            select
                e.SIGNAL_ID,
                e.SIGNAL_NK_HASH,
                e.PATTERN_ID,
                e.SYMBOL,
                e.MARKET_TYPE,
                e.INTERVAL_MINUTES,
                e.SIGNAL_TS,
                e.SIGNAL_SIDE,
                e.ENTRY_PX,
                e.METRIC_VERSION,
                e.BUCKET_VERSION,
                h.HORIZON_BARS
            from entry_bars e
            cross join active_horizons h
        ),
        future_ranked as (
            select
                sh.SIGNAL_ID,
                sh.HORIZON_BARS,
                b.TS as EXIT_TS,
                b.CLOSE::float as EXIT_PX,
                row_number() over (
                    partition by sh.SIGNAL_ID
                    order by b.TS
                ) as FUTURE_RN
            from signal_horizons sh
            join MIP.MART.MARKET_BARS b
              on b.MARKET_TYPE = sh.MARKET_TYPE
             and b.SYMBOL = sh.SYMBOL
             and b.INTERVAL_MINUTES = sh.INTERVAL_MINUTES
             and b.TS > sh.SIGNAL_TS
        ),
        chosen_exit as (
            select
                sh.*,
                fr.EXIT_TS,
                fr.EXIT_PX
            from signal_horizons sh
            left join future_ranked fr
              on fr.SIGNAL_ID = sh.SIGNAL_ID
             and fr.HORIZON_BARS = sh.HORIZON_BARS
             and fr.FUTURE_RN = sh.HORIZON_BARS
        ),
        path_stats as (
            select
                ce.SIGNAL_ID,
                ce.HORIZON_BARS,
                max(b.HIGH::float) as PATH_MAX_HIGH,
                min(b.LOW::float) as PATH_MIN_LOW
            from chosen_exit ce
            join MIP.MART.MARKET_BARS b
              on b.MARKET_TYPE = ce.MARKET_TYPE
             and b.SYMBOL = ce.SYMBOL
             and b.INTERVAL_MINUTES = ce.INTERVAL_MINUTES
             and b.TS > ce.SIGNAL_TS
             and b.TS <= ce.EXIT_TS
            group by 1, 2
        )
        select
            ce.SIGNAL_ID,
            ce.SIGNAL_NK_HASH,
            ce.HORIZON_BARS,
            ce.SIGNAL_TS as ENTRY_TS,
            ce.EXIT_TS,
            ce.ENTRY_PX,
            ce.EXIT_PX,
            case
                when ce.ENTRY_PX is null or ce.ENTRY_PX = 0 or ce.EXIT_PX is null or ce.EXIT_PX = 0 then null
                when ce.SIGNAL_SIDE = 'SHORT' then (ce.ENTRY_PX / ce.EXIT_PX) - 1
                else (ce.EXIT_PX / ce.ENTRY_PX) - 1
            end as RETURN_GROSS,
            case
                when ce.ENTRY_PX is null or ce.ENTRY_PX = 0 or ce.EXIT_PX is null or ce.EXIT_PX = 0 then null
                when ce.SIGNAL_SIDE = 'SHORT' then (ce.ENTRY_PX / ce.EXIT_PX) - 1
                else (ce.EXIT_PX / ce.ENTRY_PX) - 1
            end as RETURN_NET,
            ce.SIGNAL_SIDE as DIRECTION,
            case
                when ce.ENTRY_PX is null or ce.ENTRY_PX = 0 or ce.EXIT_PX is null or ce.EXIT_PX = 0 then null
                when ce.SIGNAL_SIDE = 'SHORT' then ((ce.ENTRY_PX / ce.EXIT_PX) - 1) >= :v_thr
                else ((ce.EXIT_PX / ce.ENTRY_PX) - 1) >= :v_thr
            end as HIT_FLAG,
            case
                when ce.ENTRY_PX is null or ce.ENTRY_PX = 0 then 'FAILED_NO_ENTRY_BAR'
                when ce.EXIT_PX is null or ce.EXIT_PX = 0 then 'INSUFFICIENT_FUTURE_DATA'
                else 'SUCCESS'
            end as EVAL_STATUS,
            case
                when ce.ENTRY_PX is null or ce.ENTRY_PX = 0 or ps.PATH_MAX_HIGH is null or ps.PATH_MIN_LOW is null then null
                when ce.SIGNAL_SIDE = 'SHORT' then (ce.ENTRY_PX / ps.PATH_MIN_LOW) - 1
                else (ps.PATH_MAX_HIGH / ce.ENTRY_PX) - 1
            end as MFE,
            case
                when ce.ENTRY_PX is null or ce.ENTRY_PX = 0 or ps.PATH_MAX_HIGH is null or ps.PATH_MIN_LOW is null then null
                when ce.SIGNAL_SIDE = 'SHORT' then (ce.ENTRY_PX / ps.PATH_MAX_HIGH) - 1
                else (ps.PATH_MIN_LOW / ce.ENTRY_PX) - 1
            end as MAE,
            ce.METRIC_VERSION,
            ce.BUCKET_VERSION,
            current_timestamp() as CALCULATED_AT
        from chosen_exit ce
        left join path_stats ps
          on ps.SIGNAL_ID = ce.SIGNAL_ID
         and ps.HORIZON_BARS = ce.HORIZON_BARS
    ) s
    on t.SIGNAL_ID = s.SIGNAL_ID
   and t.HORIZON_BARS = s.HORIZON_BARS
    when matched then update set
        t.SIGNAL_NK_HASH = s.SIGNAL_NK_HASH,
        t.ENTRY_TS = s.ENTRY_TS,
        t.EXIT_TS = s.EXIT_TS,
        t.ENTRY_PX = s.ENTRY_PX,
        t.EXIT_PX = s.EXIT_PX,
        t.RETURN_GROSS = s.RETURN_GROSS,
        t.RETURN_NET = s.RETURN_NET,
        t.DIRECTION = s.DIRECTION,
        t.HIT_FLAG = s.HIT_FLAG,
        t.EVAL_STATUS = s.EVAL_STATUS,
        t.MFE = s.MFE,
        t.MAE = s.MAE,
        t.METRIC_VERSION = s.METRIC_VERSION,
        t.BUCKET_VERSION = s.BUCKET_VERSION,
        t.CALCULATED_AT = s.CALCULATED_AT
    when not matched then insert (
        SIGNAL_ID, SIGNAL_NK_HASH, HORIZON_BARS, ENTRY_TS, EXIT_TS,
        ENTRY_PX, EXIT_PX, RETURN_GROSS, RETURN_NET, DIRECTION,
        HIT_FLAG, EVAL_STATUS, MFE, MAE, METRIC_VERSION, BUCKET_VERSION, CALCULATED_AT
    ) values (
        s.SIGNAL_ID, s.SIGNAL_NK_HASH, s.HORIZON_BARS, s.ENTRY_TS, s.EXIT_TS,
        s.ENTRY_PX, s.EXIT_PX, s.RETURN_GROSS, s.RETURN_NET, s.DIRECTION,
        s.HIT_FLAG, s.EVAL_STATUS, s.MFE, s.MAE, s.METRIC_VERSION, s.BUCKET_VERSION, s.CALCULATED_AT
    );

    v_rows_merged := sqlrowcount;

    return object_construct(
        'status', 'SUCCESS',
        'start_ts', :v_start_ts,
        'end_ts', :v_end_ts,
        'metric_version', :P_METRIC_VERSION,
        'bucket_version', :P_BUCKET_VERSION,
        'pattern_set', :P_PATTERN_SET,
        'rows_merged', :v_rows_merged
    );
end;
$$;
