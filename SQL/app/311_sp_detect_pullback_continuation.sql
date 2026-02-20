-- 311_sp_detect_pullback_continuation.sql
-- Purpose: Pullback Continuation detector for intraday pipeline.
-- Detects impulse → consolidation → breakout continuation patterns.
-- Writes to RECOMMENDATION_LOG with rich DETAILS for the learning loop.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_DETECT_PULLBACK_CONTINUATION(
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
    v_run_id                    string := coalesce(nullif(current_query_tag(), ''), uuid_string());
    v_impulse_bars              number;
    v_impulse_min_return        float;
    v_consolidation_max_bars    number;
    v_consolidation_max_range   float;
    v_breakout_buffer_pct       float;
    v_direction                 string;
    v_as_of_ts                  timestamp_ntz;
    v_before                    number;
    v_after                     number;
    v_inserted                  number := 0;
begin
    select
        coalesce(PARAMS_JSON:impulse_bars::number, 3),
        coalesce(PARAMS_JSON:impulse_min_return::float, 0.008),
        coalesce(PARAMS_JSON:consolidation_max_bars::number, 4),
        coalesce(PARAMS_JSON:consolidation_max_range_pct::float, 0.005),
        coalesce(PARAMS_JSON:breakout_buffer_pct::float, 0.001),
        coalesce(PARAMS_JSON:direction::string, 'BOTH')
      into :v_impulse_bars, :v_impulse_min_return, :v_consolidation_max_bars,
           :v_consolidation_max_range, :v_breakout_buffer_pct, :v_direction
      from MIP.APP.PATTERN_DEFINITION
     where PATTERN_ID = :P_PATTERN_ID;

    select max(TS) into :v_as_of_ts
      from MIP.MART.MARKET_BARS
     where MARKET_TYPE = :P_MARKET_TYPE
       and INTERVAL_MINUTES = :P_INTERVAL_MINUTES;

    if (v_as_of_ts is null) then
        return object_construct('status', 'SKIP', 'reason', 'NO_BARS', 'pattern_id', :P_PATTERN_ID);
    end if;

    select count(*) into :v_before
      from MIP.APP.RECOMMENDATION_LOG
     where PATTERN_ID = :P_PATTERN_ID
       and INTERVAL_MINUTES = :P_INTERVAL_MINUTES;

    insert into MIP.APP.RECOMMENDATION_LOG (
        PATTERN_ID, SYMBOL, MARKET_TYPE, INTERVAL_MINUTES, TS, SCORE, DETAILS
    )
    with bars_ranked as (
        select
            SYMBOL,
            MARKET_TYPE,
            TS,
            OPEN, HIGH, LOW, CLOSE, VOLUME,
            row_number() over (
                partition by SYMBOL, MARKET_TYPE
                order by TS desc
            ) as BAR_IDX
        from MIP.MART.MARKET_BARS
        where MARKET_TYPE = :P_MARKET_TYPE
          and INTERVAL_MINUTES = :P_INTERVAL_MINUTES
          and TS >= dateadd(day, -5, :v_as_of_ts)
    ),
    current_bar as (
        select * from bars_ranked where BAR_IDX = 1
    ),
    consolidation_window as (
        select
            SYMBOL, MARKET_TYPE,
            max(HIGH) as CONSOLIDATION_HIGH,
            min(LOW) as CONSOLIDATION_LOW,
            max(HIGH) - min(LOW) as CONSOLIDATION_RANGE,
            avg(CLOSE) as CONSOLIDATION_AVG,
            count(*) as CONSOLIDATION_BARS
        from bars_ranked
        where BAR_IDX between 2 and (:v_consolidation_max_bars + 1)
        group by SYMBOL, MARKET_TYPE
    ),
    impulse_start as (
        select SYMBOL, MARKET_TYPE, CLOSE as IMPULSE_START_CLOSE
        from bars_ranked
        where BAR_IDX = :v_consolidation_max_bars + 1 + :v_impulse_bars
    ),
    impulse_end as (
        select SYMBOL, MARKET_TYPE, CLOSE as IMPULSE_END_CLOSE
        from bars_ranked
        where BAR_IDX = :v_consolidation_max_bars + 1
    ),
    candidates as (
        select
            cb.SYMBOL,
            cb.MARKET_TYPE,
            cb.TS,
            cb.CLOSE as CURRENT_CLOSE,
            cb.HIGH as CURRENT_HIGH,
            cb.LOW as CURRENT_LOW,
            cb.VOLUME,
            cw.CONSOLIDATION_HIGH,
            cw.CONSOLIDATION_LOW,
            cw.CONSOLIDATION_RANGE,
            cw.CONSOLIDATION_AVG,
            cw.CONSOLIDATION_BARS,
            ist.IMPULSE_START_CLOSE,
            ie.IMPULSE_END_CLOSE,
            (ie.IMPULSE_END_CLOSE - ist.IMPULSE_START_CLOSE)
                / nullif(ist.IMPULSE_START_CLOSE, 0) as IMPULSE_RETURN,
            case
                when (ie.IMPULSE_END_CLOSE - ist.IMPULSE_START_CLOSE)
                     / nullif(ist.IMPULSE_START_CLOSE, 0) >= :v_impulse_min_return
                then 'BULLISH'
                when (ie.IMPULSE_END_CLOSE - ist.IMPULSE_START_CLOSE)
                     / nullif(ist.IMPULSE_START_CLOSE, 0) <= -:v_impulse_min_return
                then 'BEARISH'
            end as IMPULSE_DIRECTION,
            cw.CONSOLIDATION_RANGE / nullif(cw.CONSOLIDATION_AVG, 0) as CONSOLIDATION_RANGE_PCT
        from current_bar cb
        join consolidation_window cw
          on cw.SYMBOL = cb.SYMBOL
         and cw.MARKET_TYPE = cb.MARKET_TYPE
        join impulse_start ist
          on ist.SYMBOL = cb.SYMBOL
         and ist.MARKET_TYPE = cb.MARKET_TYPE
        join impulse_end ie
          on ie.SYMBOL = cb.SYMBOL
         and ie.MARKET_TYPE = cb.MARKET_TYPE
    ),
    breakout_signals as (
        select
            c.*,
            case
                when c.IMPULSE_DIRECTION = 'BULLISH'
                     and c.CURRENT_CLOSE > c.CONSOLIDATION_HIGH * (1 + :v_breakout_buffer_pct)
                then 'BULLISH'
                when c.IMPULSE_DIRECTION = 'BEARISH'
                     and c.CURRENT_CLOSE < c.CONSOLIDATION_LOW * (1 - :v_breakout_buffer_pct)
                then 'BEARISH'
            end as SIGNAL_DIRECTION,
            case
                when c.IMPULSE_DIRECTION = 'BULLISH'
                then (c.CURRENT_CLOSE - c.CONSOLIDATION_HIGH) / nullif(c.CONSOLIDATION_HIGH, 0)
                when c.IMPULSE_DIRECTION = 'BEARISH'
                then (c.CONSOLIDATION_LOW - c.CURRENT_CLOSE) / nullif(c.CONSOLIDATION_LOW, 0)
            end as BREAKOUT_DISTANCE
        from candidates c
        where c.CONSOLIDATION_RANGE_PCT <= :v_consolidation_max_range
          and abs(c.IMPULSE_RETURN) >= :v_impulse_min_return
    )
    select
        :P_PATTERN_ID,
        SYMBOL,
        MARKET_TYPE,
        :P_INTERVAL_MINUTES,
        TS,
        abs(IMPULSE_RETURN) as SCORE,
        object_construct(
            'pattern_type', 'PULLBACK_CONTINUATION',
            'direction', SIGNAL_DIRECTION,
            'impulse_return', IMPULSE_RETURN,
            'impulse_bars', :v_impulse_bars,
            'consolidation_bars', CONSOLIDATION_BARS,
            'consolidation_range_pct', CONSOLIDATION_RANGE_PCT,
            'consolidation_high', CONSOLIDATION_HIGH,
            'consolidation_low', CONSOLIDATION_LOW,
            'breakout_distance', BREAKOUT_DISTANCE,
            'breakout_price', CURRENT_CLOSE,
            'volume', VOLUME,
            'run_id', :v_run_id,
            'params_used', object_construct(
                'impulse_bars', :v_impulse_bars,
                'impulse_min_return', :v_impulse_min_return,
                'consolidation_max_bars', :v_consolidation_max_bars,
                'consolidation_max_range_pct', :v_consolidation_max_range,
                'breakout_buffer_pct', :v_breakout_buffer_pct
            )
        )
    from breakout_signals
    where SIGNAL_DIRECTION is not null
      and (:v_direction = 'BOTH' or SIGNAL_DIRECTION = :v_direction)
      and not exists (
          select 1 from MIP.APP.RECOMMENDATION_LOG r
          where r.PATTERN_ID = :P_PATTERN_ID
            and r.SYMBOL = breakout_signals.SYMBOL
            and r.MARKET_TYPE = breakout_signals.MARKET_TYPE
            and r.INTERVAL_MINUTES = :P_INTERVAL_MINUTES
            and r.TS = breakout_signals.TS
      );

    select count(*) into :v_after
      from MIP.APP.RECOMMENDATION_LOG
     where PATTERN_ID = :P_PATTERN_ID
       and INTERVAL_MINUTES = :P_INTERVAL_MINUTES;

    v_inserted := :v_after - :v_before;

    return object_construct(
        'status', 'SUCCESS',
        'pattern_id', :P_PATTERN_ID,
        'pattern_type', 'PULLBACK_CONTINUATION',
        'signals_inserted', :v_inserted,
        'as_of_ts', :v_as_of_ts
    );
end;
$$;
