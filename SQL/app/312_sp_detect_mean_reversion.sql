-- 312_sp_detect_mean_reversion.sql
-- Purpose: Mean-Reversion Overshoot detector for intraday pipeline.
-- Detects extreme deviations from a short-term rolling average (VWAP proxy).
-- Writes to RECOMMENDATION_LOG with rich DETAILS for the learning loop.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_DETECT_MEAN_REVERSION(
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
    v_anchor_window           number;
    v_deviation_threshold_pct float;
    v_min_bars_for_anchor     number;
    v_direction               string;
    v_as_of_ts                timestamp_ntz;
    v_before                  number;
    v_after                   number;
    v_inserted                number := 0;
begin
    select
        coalesce(PARAMS_JSON:anchor_window::number, 6),
        coalesce(PARAMS_JSON:deviation_threshold_pct::float, 0.008),
        coalesce(PARAMS_JSON:min_bars_for_anchor::number, 4),
        coalesce(PARAMS_JSON:direction::string, 'BOTH')
      into :v_anchor_window, :v_deviation_threshold_pct,
           :v_min_bars_for_anchor, :v_direction
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

    -- Stage bars with session numbering so we can self-join for the rolling anchor.
    create or replace temporary table MIP.APP.TMP_MEANREV_BARS as
    select
        SYMBOL,
        MARKET_TYPE,
        TS,
        OPEN, HIGH, LOW, CLOSE, VOLUME,
        TS::date as SESSION_DATE,
        row_number() over (
            partition by SYMBOL, MARKET_TYPE, TS::date
            order by TS
        ) as SESSION_BAR_NUM
    from MIP.MART.MARKET_BARS
    where MARKET_TYPE = :P_MARKET_TYPE
      and INTERVAL_MINUTES = :P_INTERVAL_MINUTES
      and TS >= dateadd(day, -3, :v_as_of_ts);

    insert into MIP.APP.RECOMMENDATION_LOG (
        PATTERN_ID, SYMBOL, MARKET_TYPE, INTERVAL_MINUTES, TS, SCORE, DETAILS
    )
    with bars_with_anchor as (
        select
            b.SYMBOL,
            b.MARKET_TYPE,
            b.TS,
            b.OPEN, b.HIGH, b.LOW, b.CLOSE, b.VOLUME,
            b.SESSION_DATE,
            b.SESSION_BAR_NUM,
            agg.VWAP_PROXY,
            agg.WINDOW_BARS,
            agg.PRICE_STDDEV
        from MIP.APP.TMP_MEANREV_BARS b
        join lateral (
            select
                sum(w.CLOSE * coalesce(w.VOLUME, 1))
                    / nullif(sum(coalesce(w.VOLUME, 1)), 0) as VWAP_PROXY,
                count(*) as WINDOW_BARS,
                stddev(w.CLOSE) as PRICE_STDDEV
            from MIP.APP.TMP_MEANREV_BARS w
            where w.SYMBOL = b.SYMBOL
              and w.MARKET_TYPE = b.MARKET_TYPE
              and w.SESSION_DATE = b.SESSION_DATE
              and w.SESSION_BAR_NUM between (b.SESSION_BAR_NUM - :v_anchor_window + 1)
                                        and b.SESSION_BAR_NUM
              and w.SESSION_BAR_NUM >= 1
        ) agg
    ),
    deviations as (
        select
            SYMBOL,
            MARKET_TYPE,
            TS,
            CLOSE,
            HIGH,
            LOW,
            VOLUME,
            SESSION_DATE,
            SESSION_BAR_NUM,
            VWAP_PROXY,
            PRICE_STDDEV,
            (CLOSE - VWAP_PROXY) / nullif(VWAP_PROXY, 0) as DEVIATION_PCT,
            case
                when (CLOSE - VWAP_PROXY) / nullif(VWAP_PROXY, 0) < -:v_deviation_threshold_pct
                then 'BULLISH'
                when (CLOSE - VWAP_PROXY) / nullif(VWAP_PROXY, 0) > :v_deviation_threshold_pct
                then 'BEARISH'
            end as REVERSION_DIRECTION
        from bars_with_anchor
        where WINDOW_BARS >= :v_min_bars_for_anchor
          and SESSION_BAR_NUM >= :v_min_bars_for_anchor
          and abs((CLOSE - VWAP_PROXY) / nullif(VWAP_PROXY, 0)) >= :v_deviation_threshold_pct
    )
    select
        :P_PATTERN_ID,
        SYMBOL,
        MARKET_TYPE,
        :P_INTERVAL_MINUTES,
        TS,
        abs(DEVIATION_PCT) as SCORE,
        object_construct(
            'pattern_type', 'MEAN_REVERSION',
            'direction', REVERSION_DIRECTION,
            'deviation_pct', DEVIATION_PCT,
            'close_price', CLOSE,
            'vwap_proxy', VWAP_PROXY,
            'price_stddev', PRICE_STDDEV,
            'session_bar_number', SESSION_BAR_NUM,
            'time_bucket', case
                when hour(TS) < 16 then 'MORNING'
                when hour(TS) < 18 then 'MIDDAY'
                else 'AFTERNOON'
            end,
            'volume', VOLUME,
            'run_id', :v_run_id,
            'params_used', object_construct(
                'anchor_window', :v_anchor_window,
                'deviation_threshold_pct', :v_deviation_threshold_pct,
                'min_bars_for_anchor', :v_min_bars_for_anchor
            )
        )
    from deviations
    where TS::date = :v_as_of_ts::date
      and (:v_direction = 'BOTH' or REVERSION_DIRECTION = :v_direction)
      and not exists (
          select 1 from MIP.APP.RECOMMENDATION_LOG r
          where r.PATTERN_ID = :P_PATTERN_ID
            and r.SYMBOL = deviations.SYMBOL
            and r.MARKET_TYPE = deviations.MARKET_TYPE
            and r.INTERVAL_MINUTES = :P_INTERVAL_MINUTES
            and r.TS = deviations.TS
      );

    select count(*) into :v_after
      from MIP.APP.RECOMMENDATION_LOG
     where PATTERN_ID = :P_PATTERN_ID
       and INTERVAL_MINUTES = :P_INTERVAL_MINUTES;

    v_inserted := :v_after - :v_before;

    drop table if exists MIP.APP.TMP_MEANREV_BARS;

    return object_construct(
        'status', 'SUCCESS',
        'pattern_id', :P_PATTERN_ID,
        'pattern_type', 'MEAN_REVERSION',
        'signals_inserted', :v_inserted,
        'as_of_ts', :v_as_of_ts
    );
end;
$$;
