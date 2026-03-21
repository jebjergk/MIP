-- 313_sp_detect_bearish_momentum.sql
-- Purpose: Bearish momentum detector for daily pipeline.
-- Detects sustained negative price momentum (consecutive down days, new lows).
-- These signals are DEFENSIVE: they suppress bullish entries and trigger early exits.
-- They do NOT generate short positions.
-- Writes to RECOMMENDATION_LOG with DETAILS containing the bearish context.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_DETECT_BEARISH_MOMENTUM(
    P_PATTERN_ID       number,
    P_MARKET_TYPE      string,
    P_INTERVAL_MINUTES number,
    P_PARENT_RUN_ID    string default null
)
returns variant
language sql
execute as caller
as
$$
declare
    v_run_id                  string := coalesce(nullif(current_query_tag(), ''), uuid_string());
    v_fast_window             number;
    v_min_drop_pct            float;
    v_min_negative_lag_count  number;
    v_min_zscore              float;
    v_as_of_ts                timestamp_ntz;
    v_effective_cap           timestamp_ntz;
    v_before                  number;
    v_after                   number;
    v_inserted                number := 0;
    v_lookback_days           number;
begin
    select EFFECTIVE_TO_TS into :v_effective_cap
      from MIP.APP.RUN_SCOPE_OVERRIDE
     where RUN_ID = :v_run_id
     limit 1;

    select
        coalesce(PARAMS_JSON:fast_window::number, 3),
        coalesce(PARAMS_JSON:min_drop_pct::float, 0.01),
        coalesce(PARAMS_JSON:min_negative_lag_count::number, 2),
        coalesce(PARAMS_JSON:min_zscore::float, 1.0),
        coalesce(PARAMS_JSON:lookback_days::number, 30)
      into :v_fast_window, :v_min_drop_pct, :v_min_negative_lag_count,
           :v_min_zscore, :v_lookback_days
      from MIP.APP.PATTERN_DEFINITION
     where PATTERN_ID = :P_PATTERN_ID;

    select max(TS) into :v_as_of_ts
      from MIP.MART.MARKET_BARS
     where MARKET_TYPE = :P_MARKET_TYPE
       and INTERVAL_MINUTES = :P_INTERVAL_MINUTES
       and (:v_effective_cap is null or TS <= :v_effective_cap);

    if (v_as_of_ts is null) then
        return object_construct('status', 'SKIP', 'reason', 'NO_BARS', 'pattern_id', :P_PATTERN_ID);
    end if;

    select count(*) into :v_before
      from MIP.APP.RECOMMENDATION_LOG
     where PATTERN_ID = :P_PATTERN_ID
       and INTERVAL_MINUTES = :P_INTERVAL_MINUTES;

    create or replace temporary table MIP.APP.TMP_BEARISH_BARS as
    select
        SYMBOL,
        MARKET_TYPE,
        TS,
        OPEN, HIGH, LOW, CLOSE, VOLUME,
        row_number() over (partition by SYMBOL, MARKET_TYPE order by TS) as RN,
        (CLOSE - LAG(CLOSE) OVER (partition by SYMBOL, MARKET_TYPE order by TS))
            / nullif(LAG(CLOSE) OVER (partition by SYMBOL, MARKET_TYPE order by TS), 0) as RETURN_SIMPLE
    from MIP.MART.MARKET_BARS
    where MARKET_TYPE = :P_MARKET_TYPE
      and INTERVAL_MINUTES = :P_INTERVAL_MINUTES
      and TS >= dateadd(day, -:v_lookback_days, :v_as_of_ts)
      and (:v_effective_cap is null or TS <= :v_effective_cap);

    insert into MIP.APP.RECOMMENDATION_LOG (
        PATTERN_ID, SYMBOL, MARKET_TYPE, INTERVAL_MINUTES, TS, SCORE, DETAILS
    )
    with scored as (
        select
            b.SYMBOL,
            b.MARKET_TYPE,
            b.TS,
            b.CLOSE,
            b.VOLUME,
            b.RETURN_SIMPLE,
            (select count(*) from MIP.APP.TMP_BEARISH_BARS b2
              where b2.SYMBOL = b.SYMBOL
                and b2.MARKET_TYPE = b.MARKET_TYPE
                and b2.RN <= b.RN
                and b2.RN > b.RN - :v_fast_window
                and b2.RETURN_SIMPLE < 0) as NEGATIVE_LAG_COUNT,
            (select min(b2.CLOSE) from MIP.APP.TMP_BEARISH_BARS b2
              where b2.SYMBOL = b.SYMBOL
                and b2.MARKET_TYPE = b.MARKET_TYPE
                and b2.RN < b.RN
                and b2.RN >= b.RN - :v_fast_window) as MIN_PREV_CLOSE,
            (select stddev_samp(b2.RETURN_SIMPLE) from MIP.APP.TMP_BEARISH_BARS b2
              where b2.SYMBOL = b.SYMBOL
                and b2.MARKET_TYPE = b.MARKET_TYPE
                and b2.RN > b.RN - :v_fast_window
                and b2.RN <= b.RN) as STDDEV_WINDOW
        from MIP.APP.TMP_BEARISH_BARS b
        where b.RETURN_SIMPLE is not null
    ),
    bearish_signals as (
        select
            :P_PATTERN_ID as PATTERN_ID,
            SYMBOL,
            MARKET_TYPE,
            :P_INTERVAL_MINUTES as INTERVAL_MINUTES,
            TS,
            abs(RETURN_SIMPLE) as SCORE,
            object_construct(
                'pattern_type', 'BEARISH_MOMENTUM',
                'signal_use', 'DEFENSIVE_ONLY',
                'return_simple', RETURN_SIMPLE,
                'close_price', CLOSE,
                'negative_lag_count', NEGATIVE_LAG_COUNT,
                'min_prev_close', MIN_PREV_CLOSE,
                'made_new_low', (CLOSE <= MIN_PREV_CLOSE),
                'volume', VOLUME,
                'zscore', case when STDDEV_WINDOW > 0
                    then abs(RETURN_SIMPLE) / STDDEV_WINDOW
                    else null end,
                'run_id', :v_run_id,
                'params_used', object_construct(
                    'fast_window', :v_fast_window,
                    'min_drop_pct', :v_min_drop_pct,
                    'min_negative_lag_count', :v_min_negative_lag_count,
                    'min_zscore', :v_min_zscore
                )
            ) as DETAILS
        from scored
        where RETURN_SIMPLE <= -:v_min_drop_pct
          and NEGATIVE_LAG_COUNT >= :v_min_negative_lag_count
          and (MIN_PREV_CLOSE is null or CLOSE <= MIN_PREV_CLOSE)
          and (:v_min_zscore is null or STDDEV_WINDOW is null
               or (STDDEV_WINDOW > 0 and abs(RETURN_SIMPLE) / STDDEV_WINDOW >= :v_min_zscore))
          and TS >= dateadd(day, -1, :v_as_of_ts)
    )
    select PATTERN_ID, SYMBOL, MARKET_TYPE, INTERVAL_MINUTES, TS, SCORE, DETAILS
    from bearish_signals bs
    where not exists (
        select 1 from MIP.APP.RECOMMENDATION_LOG r
        where r.PATTERN_ID = bs.PATTERN_ID
          and r.SYMBOL = bs.SYMBOL
          and r.MARKET_TYPE = bs.MARKET_TYPE
          and r.INTERVAL_MINUTES = bs.INTERVAL_MINUTES
          and r.TS = bs.TS
    );

    select count(*) into :v_after
      from MIP.APP.RECOMMENDATION_LOG
     where PATTERN_ID = :P_PATTERN_ID
       and INTERVAL_MINUTES = :P_INTERVAL_MINUTES;

    v_inserted := :v_after - :v_before;

    drop table if exists MIP.APP.TMP_BEARISH_BARS;

    return object_construct(
        'status', 'SUCCESS',
        'pattern_id', :P_PATTERN_ID,
        'pattern_type', 'BEARISH_MOMENTUM',
        'signal_use', 'DEFENSIVE_ONLY',
        'signals_inserted', :v_inserted,
        'as_of_ts', :v_as_of_ts
    );
end;
$$;
