-- 351_sp_intra_build_state_snapshot_15m.sql
-- Purpose: Phase 2 state engine v1 for intraday 15m bars.
-- Builds STATE_SNAPSHOT_15M from OHLC-only metrics and assigns <=12 buckets.

use role MIP_ADMIN_ROLE;
use database MIP;

-- Seed v1 bucket defs (idempotent).
merge into MIP.APP.STATE_BUCKET_DEF t
using (
    select 'v1' as BUCKET_VERSION, 'UP_TREND_HIGH' as STATE_BUCKET_ID, 'UP' as DIRECTION_CLASS, 'TREND_PERSIST' as REGIME_CLASS, 'HIGH' as CONFIDENCE_TIER
    union all select 'v1','UP_TREND_LOW','UP','TREND_PERSIST','LOW'
    union all select 'v1','UP_REVERSAL_LOW','UP','IMPULSE_REVERSAL','LOW'
    union all select 'v1','UP_CHOP_LOW','UP','CHOP','LOW'
    union all select 'v1','DOWN_TREND_HIGH','DOWN','TREND_PERSIST','HIGH'
    union all select 'v1','DOWN_TREND_LOW','DOWN','TREND_PERSIST','LOW'
    union all select 'v1','DOWN_REVERSAL_LOW','DOWN','IMPULSE_REVERSAL','LOW'
    union all select 'v1','DOWN_CHOP_LOW','DOWN','CHOP','LOW'
    union all select 'v1','NEUTRAL_TREND_HIGH','NEUTRAL','TREND_PERSIST','HIGH'
    union all select 'v1','NEUTRAL_TREND_LOW','NEUTRAL','TREND_PERSIST','LOW'
    union all select 'v1','NEUTRAL_REVERSAL_LOW','NEUTRAL','IMPULSE_REVERSAL','LOW'
    union all select 'v1','NEUTRAL_CHOP_LOW','NEUTRAL','CHOP','LOW'
) s
on t.BUCKET_VERSION = s.BUCKET_VERSION
and t.STATE_BUCKET_ID = s.STATE_BUCKET_ID
when matched then update set
    t.DIRECTION_CLASS = s.DIRECTION_CLASS,
    t.REGIME_CLASS = s.REGIME_CLASS,
    t.CONFIDENCE_TIER = s.CONFIDENCE_TIER,
    t.IS_ACTIVE = true,
    t.UPDATED_AT = current_timestamp()
when not matched then insert (
    BUCKET_VERSION, STATE_BUCKET_ID, DIRECTION_CLASS, REGIME_CLASS, CONFIDENCE_TIER, IS_ACTIVE
) values (
    s.BUCKET_VERSION, s.STATE_BUCKET_ID, s.DIRECTION_CLASS, s.REGIME_CLASS, s.CONFIDENCE_TIER, true
);

create or replace procedure MIP.APP.SP_INTRA_BUILD_STATE_SNAPSHOT_15M(
    P_START_TS timestamp_ntz,
    P_END_TS timestamp_ntz,
    P_METRIC_VERSION string default 'v1',
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
    v_start_ts := coalesce(:P_START_TS, dateadd(day, -45, :v_end_ts));

    merge into MIP.APP.STATE_SNAPSHOT_15M t
    using (
        with bars as (
            select
                MARKET_TYPE,
                SYMBOL,
                INTERVAL_MINUTES,
                TS,
                OPEN,
                HIGH,
                LOW,
                CLOSE,
                VOLUME,
                lag(CLOSE) over (
                    partition by MARKET_TYPE, SYMBOL, INTERVAL_MINUTES
                    order by TS
                ) as PREV_CLOSE
            from MIP.MART.MARKET_BARS
            where INTERVAL_MINUTES = 15
              and TS >= dateadd(day, -5, :v_start_ts)
              and TS <= :v_end_ts
        ),
        feat0 as (
            select
                MARKET_TYPE,
                SYMBOL,
                INTERVAL_MINUTES,
                TS,
                OPEN,
                HIGH,
                LOW,
                CLOSE,
                VOLUME,
                case
                    when PREV_CLOSE is null or PREV_CLOSE = 0 then null
                    else (CLOSE - PREV_CLOSE) / PREV_CLOSE
                end as RET1
            from bars
        ),
        feat1 as (
            select
                *,
                lag(RET1, 4) over (partition by MARKET_TYPE, SYMBOL, INTERVAL_MINUTES order by TS) as RET1_LAG4,
                lag(RET1, 16) over (partition by MARKET_TYPE, SYMBOL, INTERVAL_MINUTES order by TS) as RET1_LAG16,
                lag(RET1, 1) over (partition by MARKET_TYPE, SYMBOL, INTERVAL_MINUTES order by TS) as RET1_PREV,
                avg(abs(RET1)) over (
                    partition by MARKET_TYPE, SYMBOL, INTERVAL_MINUTES
                    order by TS
                    rows between 31 preceding and current row
                ) as ABS_RET_AVG_32,
                stddev(RET1) over (
                    partition by MARKET_TYPE, SYMBOL, INTERVAL_MINUTES
                    order by TS
                    rows between 31 preceding and current row
                ) as RET_STD_32,
                stddev(RET1) over (
                    partition by MARKET_TYPE, SYMBOL, INTERVAL_MINUTES
                    order by TS
                    rows between 7 preceding and current row
                ) as RET_STD_8,
                sum(RET1) over (
                    partition by MARKET_TYPE, SYMBOL, INTERVAL_MINUTES
                    order by TS
                    rows between 3 preceding and current row
                ) as RET_SUM_4,
                sum(RET1) over (
                    partition by MARKET_TYPE, SYMBOL, INTERVAL_MINUTES
                    order by TS
                    rows between 15 preceding and current row
                ) as RET_SUM_16,
                sum(abs(RET1)) over (
                    partition by MARKET_TYPE, SYMBOL, INTERVAL_MINUTES
                    order by TS
                    rows between 15 preceding and current row
                ) as ABS_RET_SUM_16,
                max(abs(RET1)) over (
                    partition by MARKET_TYPE, SYMBOL, INTERVAL_MINUTES
                    order by TS
                    rows between 7 preceding and current row
                ) as ABS_RET_MAX_8
            from feat0
        ),
        feat2 as (
            select
                *,
                case
                    when RET1 is null then 0
                    when RET1 > 0 then 1
                    when RET1 < 0 then -1
                    else 0
                end as RET_SIGN,
                case
                    when RET1_PREV is null or RET1 is null then 0
                    when sign(RET1) <> sign(RET1_PREV) then 1
                    else 0
                end as SIGN_FLIP
            from feat1
        ),
        metrics as (
            select
                MARKET_TYPE,
                SYMBOL,
                INTERVAL_MINUTES,
                TS,
                -- Belief metrics
                least(greatest(coalesce(RET_SUM_4 / nullif(ABS_RET_AVG_32, 0), 0), -3), 3) / 3 as BELIEF_DIRECTION,
                least(coalesce(abs(RET_SUM_4) / nullif(RET_STD_32, 0), 0), 5) / 5 as BELIEF_STRENGTH,
                1 - least(
                    coalesce(
                        avg(SIGN_FLIP) over (
                            partition by MARKET_TYPE, SYMBOL, INTERVAL_MINUTES
                            order by TS
                            rows between 15 preceding and current row
                        ),
                        0
                    ),
                    1
                ) as BELIEF_STABILITY,

                -- Time elasticity metrics
                least(coalesce(abs(RET1) / nullif(RET_STD_8, 0), 0), 5) / 5 as REACTION_SPEED,
                least(
                    coalesce(
                        ln(1 + abs(RET_SUM_16) / nullif(ABS_RET_MAX_8, 0)) / ln(11),
                        0
                    ),
                    1
                ) as DRIFT_VS_IMPULSE,
                least(coalesce(abs(RET_SUM_16) / nullif(ABS_RET_AVG_32, 0), 0), 10) / 10 as RECOVERY_TIME,

                -- Narrative consistency metrics
                case
                    when sign(RET_SUM_4) = sign(RET_SUM_16) then 1
                    when sign(RET_SUM_4) = 0 or sign(RET_SUM_16) = 0 then 0
                    else -1
                end as MTF_ALIGNMENT,
                least(coalesce(ABS_RET_SUM_16 / nullif(abs(RET_SUM_16), 0), 10), 10) as CHOP_INDEX,
                least(greatest(coalesce((RET_STD_8 - RET_STD_32) * sign(RET_SUM_4) / nullif(RET_STD_32, 0), 0), -3), 3) / 3 as VOL_DIRECTION_ALIGNMENT,

                OPEN,
                HIGH,
                LOW,
                CLOSE,
                VOLUME
            from feat2
            where TS >= :v_start_ts
              and TS <= :v_end_ts
        ),
        bucketed as (
            select
                MARKET_TYPE,
                SYMBOL,
                INTERVAL_MINUTES,
                TS,
                BELIEF_DIRECTION,
                BELIEF_STRENGTH,
                BELIEF_STABILITY,
                REACTION_SPEED,
                DRIFT_VS_IMPULSE,
                RECOVERY_TIME,
                MTF_ALIGNMENT,
                CHOP_INDEX,
                VOL_DIRECTION_ALIGNMENT,
                case
                    when BELIEF_DIRECTION >= 0.2 then 'UP'
                    when BELIEF_DIRECTION <= -0.2 then 'DOWN'
                    else 'NEUTRAL'
                end as DIRECTION_CLASS,
                case
                    when CHOP_INDEX >= 2.5 then 'CHOP'
                    when MTF_ALIGNMENT < 0 then 'IMPULSE_REVERSAL'
                    else 'TREND_PERSIST'
                end as REGIME_CLASS,
                case
                    when BELIEF_STRENGTH >= 0.45 and BELIEF_STABILITY >= 0.60 then 'HIGH'
                    else 'LOW'
                end as CONFIDENCE_TIER,
                OPEN,
                HIGH,
                LOW,
                CLOSE,
                VOLUME
            from metrics
        ),
        raw_bucketed as (
            select
                MARKET_TYPE,
                SYMBOL,
                INTERVAL_MINUTES,
                TS,
                BELIEF_DIRECTION,
                BELIEF_STRENGTH,
                BELIEF_STABILITY,
                REACTION_SPEED,
                DRIFT_VS_IMPULSE,
                RECOVERY_TIME,
                MTF_ALIGNMENT,
                CHOP_INDEX,
                VOL_DIRECTION_ALIGNMENT,
                case
                    when DIRECTION_CLASS = 'UP' and REGIME_CLASS = 'TREND_PERSIST' and CONFIDENCE_TIER = 'HIGH' then 'UP_TREND_HIGH'
                    when DIRECTION_CLASS = 'UP' and REGIME_CLASS = 'TREND_PERSIST' then 'UP_TREND_LOW'
                    when DIRECTION_CLASS = 'UP' and REGIME_CLASS = 'IMPULSE_REVERSAL' then 'UP_REVERSAL_LOW'
                    when DIRECTION_CLASS = 'UP' and REGIME_CLASS = 'CHOP' then 'UP_CHOP_LOW'
                    when DIRECTION_CLASS = 'DOWN' and REGIME_CLASS = 'TREND_PERSIST' and CONFIDENCE_TIER = 'HIGH' then 'DOWN_TREND_HIGH'
                    when DIRECTION_CLASS = 'DOWN' and REGIME_CLASS = 'TREND_PERSIST' then 'DOWN_TREND_LOW'
                    when DIRECTION_CLASS = 'DOWN' and REGIME_CLASS = 'IMPULSE_REVERSAL' then 'DOWN_REVERSAL_LOW'
                    when DIRECTION_CLASS = 'DOWN' and REGIME_CLASS = 'CHOP' then 'DOWN_CHOP_LOW'
                    when DIRECTION_CLASS = 'NEUTRAL' and REGIME_CLASS = 'TREND_PERSIST' and CONFIDENCE_TIER = 'HIGH' then 'NEUTRAL_TREND_HIGH'
                    when DIRECTION_CLASS = 'NEUTRAL' and REGIME_CLASS = 'TREND_PERSIST' then 'NEUTRAL_TREND_LOW'
                    when DIRECTION_CLASS = 'NEUTRAL' and REGIME_CLASS = 'IMPULSE_REVERSAL' then 'NEUTRAL_REVERSAL_LOW'
                    else 'NEUTRAL_CHOP_LOW'
                end as RAW_STATE_BUCKET_ID,
                lag(
                    case
                        when DIRECTION_CLASS = 'UP' and REGIME_CLASS = 'TREND_PERSIST' and CONFIDENCE_TIER = 'HIGH' then 'UP_TREND_HIGH'
                        when DIRECTION_CLASS = 'UP' and REGIME_CLASS = 'TREND_PERSIST' then 'UP_TREND_LOW'
                        when DIRECTION_CLASS = 'UP' and REGIME_CLASS = 'IMPULSE_REVERSAL' then 'UP_REVERSAL_LOW'
                        when DIRECTION_CLASS = 'UP' and REGIME_CLASS = 'CHOP' then 'UP_CHOP_LOW'
                        when DIRECTION_CLASS = 'DOWN' and REGIME_CLASS = 'TREND_PERSIST' and CONFIDENCE_TIER = 'HIGH' then 'DOWN_TREND_HIGH'
                        when DIRECTION_CLASS = 'DOWN' and REGIME_CLASS = 'TREND_PERSIST' then 'DOWN_TREND_LOW'
                        when DIRECTION_CLASS = 'DOWN' and REGIME_CLASS = 'IMPULSE_REVERSAL' then 'DOWN_REVERSAL_LOW'
                        when DIRECTION_CLASS = 'DOWN' and REGIME_CLASS = 'CHOP' then 'DOWN_CHOP_LOW'
                        when DIRECTION_CLASS = 'NEUTRAL' and REGIME_CLASS = 'TREND_PERSIST' and CONFIDENCE_TIER = 'HIGH' then 'NEUTRAL_TREND_HIGH'
                        when DIRECTION_CLASS = 'NEUTRAL' and REGIME_CLASS = 'TREND_PERSIST' then 'NEUTRAL_TREND_LOW'
                        when DIRECTION_CLASS = 'NEUTRAL' and REGIME_CLASS = 'IMPULSE_REVERSAL' then 'NEUTRAL_REVERSAL_LOW'
                        else 'NEUTRAL_CHOP_LOW'
                    end
                ) over (
                    partition by MARKET_TYPE, SYMBOL, INTERVAL_MINUTES
                    order by TS
                ) as PREV_RAW_STATE_BUCKET_ID,
                sha2(
                    concat(
                        coalesce(MARKET_TYPE, ''), '|',
                        coalesce(SYMBOL, ''), '|',
                        coalesce(to_varchar(INTERVAL_MINUTES), ''), '|',
                        coalesce(to_varchar(TS), ''), '|',
                        coalesce(to_varchar(OPEN), ''), '|',
                        coalesce(to_varchar(HIGH), ''), '|',
                        coalesce(to_varchar(LOW), ''), '|',
                        coalesce(to_varchar(CLOSE), ''), '|',
                        coalesce(to_varchar(VOLUME), '')
                    ),
                    256
                ) as SOURCE_BAR_KEY_HASH
            from bucketed
        ),
        finalized as (
            select
                MARKET_TYPE,
                SYMBOL,
                INTERVAL_MINUTES,
                TS,
                BELIEF_DIRECTION,
                BELIEF_STRENGTH,
                BELIEF_STABILITY,
                REACTION_SPEED,
                DRIFT_VS_IMPULSE,
                RECOVERY_TIME,
                MTF_ALIGNMENT,
                CHOP_INDEX,
                VOL_DIRECTION_ALIGNMENT,
                -- Debounce: bucket switch is accepted only after 2 consecutive raw bars.
                case
                    when PREV_RAW_STATE_BUCKET_ID is null then RAW_STATE_BUCKET_ID
                    when RAW_STATE_BUCKET_ID = PREV_RAW_STATE_BUCKET_ID then RAW_STATE_BUCKET_ID
                    else PREV_RAW_STATE_BUCKET_ID
                end as STATE_BUCKET_ID,
                SOURCE_BAR_KEY_HASH
            from raw_bucketed
        )
        select
            MARKET_TYPE,
            SYMBOL,
            INTERVAL_MINUTES,
            TS,
            BELIEF_DIRECTION,
            BELIEF_STRENGTH,
            BELIEF_STABILITY,
            REACTION_SPEED,
            DRIFT_VS_IMPULSE,
            RECOVERY_TIME,
            MTF_ALIGNMENT,
            CHOP_INDEX,
            VOL_DIRECTION_ALIGNMENT,
            STATE_BUCKET_ID,
            :P_METRIC_VERSION as METRIC_VERSION,
            :P_BUCKET_VERSION as BUCKET_VERSION,
            SOURCE_BAR_KEY_HASH,
            current_timestamp() as CALCULATED_AT
        from finalized
    ) s
    on t.MARKET_TYPE = s.MARKET_TYPE
   and t.SYMBOL = s.SYMBOL
   and t.INTERVAL_MINUTES = s.INTERVAL_MINUTES
   and t.TS = s.TS
    when matched then update set
        t.BELIEF_DIRECTION = s.BELIEF_DIRECTION,
        t.BELIEF_STRENGTH = s.BELIEF_STRENGTH,
        t.BELIEF_STABILITY = s.BELIEF_STABILITY,
        t.REACTION_SPEED = s.REACTION_SPEED,
        t.DRIFT_VS_IMPULSE = s.DRIFT_VS_IMPULSE,
        t.RECOVERY_TIME = s.RECOVERY_TIME,
        t.MTF_ALIGNMENT = s.MTF_ALIGNMENT,
        t.CHOP_INDEX = s.CHOP_INDEX,
        t.VOL_DIRECTION_ALIGNMENT = s.VOL_DIRECTION_ALIGNMENT,
        t.STATE_BUCKET_ID = s.STATE_BUCKET_ID,
        t.METRIC_VERSION = s.METRIC_VERSION,
        t.BUCKET_VERSION = s.BUCKET_VERSION,
        t.SOURCE_BAR_KEY_HASH = s.SOURCE_BAR_KEY_HASH,
        t.CALCULATED_AT = s.CALCULATED_AT
    when not matched then insert (
        MARKET_TYPE, SYMBOL, INTERVAL_MINUTES, TS,
        BELIEF_DIRECTION, BELIEF_STRENGTH, BELIEF_STABILITY,
        REACTION_SPEED, DRIFT_VS_IMPULSE, RECOVERY_TIME,
        MTF_ALIGNMENT, CHOP_INDEX, VOL_DIRECTION_ALIGNMENT,
        STATE_BUCKET_ID, METRIC_VERSION, BUCKET_VERSION,
        SOURCE_BAR_KEY_HASH, CALCULATED_AT
    ) values (
        s.MARKET_TYPE, s.SYMBOL, s.INTERVAL_MINUTES, s.TS,
        s.BELIEF_DIRECTION, s.BELIEF_STRENGTH, s.BELIEF_STABILITY,
        s.REACTION_SPEED, s.DRIFT_VS_IMPULSE, s.RECOVERY_TIME,
        s.MTF_ALIGNMENT, s.CHOP_INDEX, s.VOL_DIRECTION_ALIGNMENT,
        s.STATE_BUCKET_ID, s.METRIC_VERSION, s.BUCKET_VERSION,
        s.SOURCE_BAR_KEY_HASH, s.CALCULATED_AT
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
