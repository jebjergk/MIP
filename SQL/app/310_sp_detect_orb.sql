-- 310_sp_detect_orb.sql
-- Purpose: Opening Range Breakout detector for intraday pipeline.
-- Detects breakout from the first-hour range of each trading session.
-- Writes to RECOMMENDATION_LOG with rich DETAILS for the learning loop.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_DETECT_ORB(
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
    v_run_id              string := coalesce(nullif(current_query_tag(), ''), uuid_string());
    v_range_bars          number;
    v_breakout_buffer_pct float;
    v_min_range_pct       float;
    v_session_start_hour  number;
    v_direction           string;
    v_as_of_ts            timestamp_ntz;
    v_before              number;
    v_after               number;
    v_inserted            number := 0;
begin
    select
        coalesce(PARAMS_JSON:range_bars::number, 1),
        coalesce(PARAMS_JSON:breakout_buffer_pct::float, 0.001),
        coalesce(PARAMS_JSON:min_range_pct::float, 0.003),
        coalesce(PARAMS_JSON:session_start_hour_utc::number, 14),
        coalesce(PARAMS_JSON:direction::string, 'BOTH')
      into :v_range_bars, :v_breakout_buffer_pct, :v_min_range_pct,
           :v_session_start_hour, :v_direction
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
    with session_bars as (
        select
            SYMBOL,
            MARKET_TYPE,
            TS,
            OPEN, HIGH, LOW, CLOSE, VOLUME,
            TS::date as SESSION_DATE,
            hour(TS) as BAR_HOUR,
            row_number() over (
                partition by SYMBOL, MARKET_TYPE, TS::date
                order by TS
            ) as SESSION_BAR_NUM
        from MIP.MART.MARKET_BARS
        where MARKET_TYPE = :P_MARKET_TYPE
          and INTERVAL_MINUTES = :P_INTERVAL_MINUTES
          and hour(TS) >= :v_session_start_hour
    ),
    opening_range as (
        select
            SYMBOL,
            MARKET_TYPE,
            SESSION_DATE,
            max(HIGH) as RANGE_HIGH,
            min(LOW) as RANGE_LOW,
            min(OPEN) as SESSION_OPEN,
            max(HIGH) - min(LOW) as RANGE_SIZE
        from session_bars
        where SESSION_BAR_NUM <= :v_range_bars
        group by SYMBOL, MARKET_TYPE, SESSION_DATE
        having RANGE_SIZE > 0
    ),
    breakouts as (
        select
            sb.SYMBOL,
            sb.MARKET_TYPE,
            sb.TS,
            sb.CLOSE,
            sb.HIGH,
            sb.LOW,
            sb.VOLUME,
            sb.SESSION_BAR_NUM,
            sb.BAR_HOUR,
            orng.RANGE_HIGH,
            orng.RANGE_LOW,
            orng.RANGE_SIZE,
            orng.SESSION_OPEN,
            orng.RANGE_SIZE / nullif(orng.SESSION_OPEN, 0) as RANGE_PCT,
            case
                when sb.CLOSE > orng.RANGE_HIGH * (1 + :v_breakout_buffer_pct) then 'BULLISH'
                when sb.CLOSE < orng.RANGE_LOW * (1 - :v_breakout_buffer_pct) then 'BEARISH'
            end as DIRECTION,
            case
                when sb.CLOSE > orng.RANGE_HIGH * (1 + :v_breakout_buffer_pct)
                    then (sb.CLOSE - orng.RANGE_HIGH) / nullif(orng.RANGE_HIGH, 0)
                when sb.CLOSE < orng.RANGE_LOW * (1 - :v_breakout_buffer_pct)
                    then (orng.RANGE_LOW - sb.CLOSE) / nullif(orng.RANGE_LOW, 0)
            end as BREAKOUT_DISTANCE_PCT
        from session_bars sb
        join opening_range orng
          on orng.SYMBOL = sb.SYMBOL
         and orng.MARKET_TYPE = sb.MARKET_TYPE
         and orng.SESSION_DATE = sb.SESSION_DATE
        where sb.SESSION_BAR_NUM > :v_range_bars
          and orng.RANGE_SIZE / nullif(orng.SESSION_OPEN, 0) >= :v_min_range_pct
          and (
              sb.CLOSE > orng.RANGE_HIGH * (1 + :v_breakout_buffer_pct)
              or sb.CLOSE < orng.RANGE_LOW * (1 - :v_breakout_buffer_pct)
          )
    ),
    first_breakout as (
        select *
        from breakouts
        where (:v_direction = 'BOTH' or DIRECTION = :v_direction)
        qualify row_number() over (
            partition by SYMBOL, MARKET_TYPE, TS::date
            order by TS
        ) = 1
    )
    select
        :P_PATTERN_ID,
        SYMBOL,
        MARKET_TYPE,
        :P_INTERVAL_MINUTES,
        TS,
        BREAKOUT_DISTANCE_PCT as SCORE,
        object_construct(
            'pattern_type', 'ORB',
            'direction', DIRECTION,
            'session_bar_number', SESSION_BAR_NUM,
            'opening_range_high', RANGE_HIGH,
            'opening_range_low', RANGE_LOW,
            'range_pct', RANGE_PCT,
            'breakout_price', CLOSE,
            'breakout_distance_pct', BREAKOUT_DISTANCE_PCT,
            'time_bucket', case
                when BAR_HOUR < 16 then 'MORNING'
                when BAR_HOUR < 18 then 'MIDDAY'
                else 'AFTERNOON'
            end,
            'volume', VOLUME,
            'run_id', :v_run_id,
            'params_used', object_construct(
                'range_bars', :v_range_bars,
                'breakout_buffer_pct', :v_breakout_buffer_pct,
                'min_range_pct', :v_min_range_pct,
                'session_start_hour_utc', :v_session_start_hour
            )
        )
    from first_breakout
    where TS::date = :v_as_of_ts::date
      and not exists (
          select 1 from MIP.APP.RECOMMENDATION_LOG r
          where r.PATTERN_ID = :P_PATTERN_ID
            and r.SYMBOL = first_breakout.SYMBOL
            and r.MARKET_TYPE = first_breakout.MARKET_TYPE
            and r.INTERVAL_MINUTES = :P_INTERVAL_MINUTES
            and r.TS = first_breakout.TS
      );

    select count(*) into :v_after
      from MIP.APP.RECOMMENDATION_LOG
     where PATTERN_ID = :P_PATTERN_ID
       and INTERVAL_MINUTES = :P_INTERVAL_MINUTES;

    v_inserted := :v_after - :v_before;

    return object_construct(
        'status', 'SUCCESS',
        'pattern_id', :P_PATTERN_ID,
        'pattern_type', 'ORB',
        'signals_inserted', :v_inserted,
        'as_of_ts', :v_as_of_ts
    );
end;
$$;
