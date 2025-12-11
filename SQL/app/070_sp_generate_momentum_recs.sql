use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_GENERATE_MOMENTUM_RECS(
    P_MIN_RETURN       number,      -- e.g. 0.002 for +0.2% threshold
    P_MARKET_TYPE      string default 'STOCK',
    P_INTERVAL_MINUTES number default 5
)
returns varchar
language sql
as
$$
declare
    v_inserted                number := 0;
    v_min_volume              number := 1000;
    v_vol_adj_threshold       number := 1.0;
    v_min_trades_for_usage    number := 30;
    v_default_fast_window     number := 20;
    v_default_slow_window     number := 3;
    v_default_lookback_days   number := 1;
    v_default_min_return      float  := 0.002;
    v_default_min_zscore      float  := 1.0;
    v_default_market_type     string := 'STOCK';
    v_default_interval_minutes number := 5;
    v_pattern_market_type     string;
    v_pattern_interval        number;
    v_pattern_fast_window     number;
    v_pattern_slow_window     number;
    v_pattern_lookback_days   number;
    v_pattern_min_return      float;
    v_pattern_min_zscore      float;
    v_pattern_id              number;
    v_pattern_key             string;
begin
    -- Purge any recommendations tied to inactive patterns so they disappear once deactivated
    delete from MIP.APP.RECOMMENDATION_LOG
     where PATTERN_ID in (
            select PATTERN_ID
              from MIP.APP.PATTERN_DEFINITION
             where coalesce(IS_ACTIVE, 'N') <> 'Y'
        );

    select try_to_number(CONFIG_VALUE)
      into :v_min_trades_for_usage
    from MIP.APP.APP_CONFIG
    where CONFIG_KEY = 'PATTERN_MIN_TRADES'
    limit 1;

    if (v_min_trades_for_usage is null) then
        v_min_trades_for_usage := 30;
    end if;

    -- Configurable thresholds
    select try_to_number(CONFIG_VALUE)
      into :v_min_volume
    from MIP.APP.APP_CONFIG
    where CONFIG_KEY = 'MIN_VOLUME'
    limit 1;

    if (v_min_volume is null) then
        v_min_volume := 1000;
    end if;

    select try_to_number(CONFIG_VALUE)
      into :v_vol_adj_threshold
    from MIP.APP.APP_CONFIG
    where CONFIG_KEY = 'VOL_ADJ_THRESHOLD'
    limit 1;

    if (v_vol_adj_threshold is null) then
        v_vol_adj_threshold := 1.0;
    end if;

    -- Default parameters based on MOMENTUM_DEMO (fallback to literals if missing)
    begin
        select
            coalesce(PARAMS_JSON:fast_window::number, v_default_fast_window),
            coalesce(PARAMS_JSON:slow_window::number, v_default_slow_window),
            coalesce(PARAMS_JSON:lookback_days::number, v_default_lookback_days),
            coalesce(PARAMS_JSON:min_return::float, v_default_min_return),
            coalesce(PARAMS_JSON:min_zscore::float, v_default_min_zscore),
            coalesce(PARAMS_JSON:market_type::string, v_default_market_type),
            coalesce(PARAMS_JSON:interval_minutes::number, v_default_interval_minutes)
        into :v_default_fast_window,
             :v_default_slow_window,
             :v_default_lookback_days,
             :v_default_min_return,
             :v_default_min_zscore,
             :v_default_market_type,
             :v_default_interval_minutes
        from MIP.APP.PATTERN_DEFINITION
        where upper(NAME) = 'MOMENTUM_DEMO'
        limit 1;
    exception
        when statement_error then
            null;
    end;

    for pattern in (
        select
            PATTERN_ID,
            upper(NAME) as PATTERN_KEY,
            coalesce(PARAMS_JSON:market_type::string, P_MARKET_TYPE, v_default_market_type) as MARKET_TYPE,
            coalesce(PARAMS_JSON:interval_minutes::number, P_INTERVAL_MINUTES, v_default_interval_minutes) as INTERVAL_MINUTES,
            coalesce(PARAMS_JSON:fast_window::number, v_default_fast_window) as FAST_WINDOW,
            coalesce(PARAMS_JSON:slow_window::number, v_default_slow_window) as SLOW_WINDOW,
            coalesce(PARAMS_JSON:lookback_days::number, v_default_lookback_days) as LOOKBACK_DAYS,
            coalesce(PARAMS_JSON:min_return::float, P_MIN_RETURN, v_default_min_return) as MIN_RETURN,
            coalesce(PARAMS_JSON:min_zscore::float, v_vol_adj_threshold, v_default_min_zscore) as MIN_ZSCORE
        from MIP.APP.PATTERN_DEFINITION
        where coalesce(IS_ACTIVE, 'N') = 'Y'
          and coalesce(ENABLED, true)
          and (P_MARKET_TYPE is null or upper(P_MARKET_TYPE) = coalesce(upper(PARAMS_JSON:market_type::string), v_default_market_type))
          and (P_INTERVAL_MINUTES is null or P_INTERVAL_MINUTES = coalesce(PARAMS_JSON:interval_minutes::number, v_default_interval_minutes))
          and (LAST_TRADE_COUNT is null or LAST_TRADE_COUNT >= :v_min_trades_for_usage)
    ) do
        v_pattern_market_type   := pattern.MARKET_TYPE;
        v_pattern_interval      := pattern.INTERVAL_MINUTES;
        v_pattern_fast_window   := pattern.FAST_WINDOW;
        v_pattern_slow_window   := pattern.SLOW_WINDOW;
        v_pattern_lookback_days := pattern.LOOKBACK_DAYS;
        v_pattern_min_return    := pattern.MIN_RETURN;
        v_pattern_min_zscore    := pattern.MIN_ZSCORE;
        v_pattern_id            := pattern.PATTERN_ID;
        v_pattern_key           := pattern.PATTERN_KEY;

        if (pattern.MARKET_TYPE = 'STOCK') then
            execute immediate '
                insert into MIP.APP.RECOMMENDATION_LOG (
                    PATTERN_ID,
                    SYMBOL,
                    MARKET_TYPE,
                    INTERVAL_MINUTES,
                    TS,
                    SCORE,
                    DETAILS
                )
                with returns_filtered as (
                    select
                        r.*,
                        row_number() over (
                            partition by r.SYMBOL, r.MARKET_TYPE, r.INTERVAL_MINUTES
                            order by r.TS
                        ) as RN
                    from MIP.MART.MARKET_RETURNS r
                  where r.MARKET_TYPE = ?
                    and r.INTERVAL_MINUTES = ?
                      and r.RETURN_SIMPLE is not null
                      and r.VOLUME >= ?
                      and r.TS >= dateadd(day, -?, current_timestamp())
                ),
                scored as (
                    select
                        rf.*,
                        (select count(*) from returns_filtered rf2
                          where rf2.SYMBOL = rf.SYMBOL
                            and rf2.MARKET_TYPE = rf.MARKET_TYPE
                            and rf2.INTERVAL_MINUTES = rf.INTERVAL_MINUTES
                            and rf2.RN < rf.RN
                            and rf2.RN >= rf.RN - ?
                            and rf2.RETURN_SIMPLE > 0) as POSITIVE_LAG_COUNT,
                        (select max(rf2.CLOSE) from returns_filtered rf2
                          where rf2.SYMBOL = rf.SYMBOL
                            and rf2.MARKET_TYPE = rf.MARKET_TYPE
                            and rf2.INTERVAL_MINUTES = rf.INTERVAL_MINUTES
                            and rf2.RN < rf.RN
                            and rf2.RN >= rf.RN - ?) as MAX_PREV_CLOSE,
                        (select stddev_samp(rf2.RETURN_SIMPLE) from returns_filtered rf2
                          where rf2.SYMBOL = rf.SYMBOL
                            and rf2.MARKET_TYPE = rf.MARKET_TYPE
                            and rf2.INTERVAL_MINUTES = rf.INTERVAL_MINUTES
                            and rf2.RN > rf.RN - ?
                            and rf2.RN <= rf.RN) as STDDEV_WINDOW
                    from returns_filtered rf
                ),
                pattern_recs as (
                    select
                        ? as PATTERN_ID,
                        rf.SYMBOL,
                        rf.MARKET_TYPE,
                        rf.INTERVAL_MINUTES,
                        rf.TS,
                        rf.RETURN_SIMPLE as SCORE,
                        object_construct(
                            ''pattern_key'', ?,
                            ''return_simple'', rf.RETURN_SIMPLE,
                            ''prev_close'', rf.PREV_CLOSE,
                            ''close'', rf.CLOSE
                        ) as DETAILS
                    from scored rf
                    where rf.RETURN_SIMPLE >= ?
                      and rf.POSITIVE_LAG_COUNT >= ?
                      and (rf.MAX_PREV_CLOSE is null or rf.CLOSE >= rf.MAX_PREV_CLOSE)
                      and (? is null or rf.STDDEV_WINDOW is null or (rf.STDDEV_WINDOW > 0 and rf.RETURN_SIMPLE / rf.STDDEV_WINDOW >= ?))
                )
                select PATTERN_ID,
                       SYMBOL,
                       MARKET_TYPE,
                       INTERVAL_MINUTES,
                       TS,
                       SCORE,
                       DETAILS
                from pattern_recs p
                where not exists (
                    select 1
                    from MIP.APP.RECOMMENDATION_LOG existing
                    where existing.PATTERN_ID = p.PATTERN_ID
                      and existing.SYMBOL = p.SYMBOL
                      and existing.MARKET_TYPE = p.MARKET_TYPE
                      and existing.INTERVAL_MINUTES = p.INTERVAL_MINUTES
                      and existing.TS = p.TS
                )
            ' using (
                v_pattern_market_type,
                v_pattern_interval,
                v_min_volume,
                v_pattern_lookback_days,
                v_pattern_slow_window,
                v_pattern_fast_window,
                v_pattern_fast_window,
                v_pattern_id,
                v_pattern_key,
                v_pattern_min_return,
                v_pattern_slow_window,
                v_pattern_min_zscore,
                v_pattern_min_zscore
            );

            v_inserted := v_inserted + sqlrowcount;
        elseif (pattern.MARKET_TYPE = 'FX') then
            execute immediate '
                insert into MIP.APP.RECOMMENDATION_LOG (
                    PATTERN_ID,
                    SYMBOL,
                    MARKET_TYPE,
                    INTERVAL_MINUTES,
                    TS,
                    SCORE,
                    DETAILS
                )
                with bars as (
                    select
                        mb.*,
                        row_number() over (
                            partition by mb.SYMBOL, mb.MARKET_TYPE, mb.INTERVAL_MINUTES
                            order by mb.TS
                        ) as RN,
                        lag(mb.CLOSE) over (
                            partition by mb.SYMBOL, mb.MARKET_TYPE, mb.INTERVAL_MINUTES
                            order by mb.TS
                        ) as PREV_CLOSE
                    from MIP.MART.MARKET_BARS mb
                  where mb.MARKET_TYPE = ?
                    and mb.INTERVAL_MINUTES = ?
                      and mb.TS >= dateadd(day, -?, current_timestamp())
                ),
                returns as (
                    select
                        b.*,
                        case when b.PREV_CLOSE is null or b.PREV_CLOSE = 0 then null else (b.CLOSE / b.PREV_CLOSE) - 1 end as RETURN_SIMPLE
                    from bars b
                ),
                scored as (
                    select
                        r.*,
                        (select avg(r2.CLOSE) from returns r2
                          where r2.SYMBOL = r.SYMBOL
                            and r2.MARKET_TYPE = r.MARKET_TYPE
                            and r2.INTERVAL_MINUTES = r.INTERVAL_MINUTES
                            and r2.RN > r.RN - ?
                            and r2.RN <= r.RN) as SMA_FAST,
                        (select avg(r2.CLOSE) from returns r2
                          where r2.SYMBOL = r.SYMBOL
                            and r2.MARKET_TYPE = r.MARKET_TYPE
                            and r2.INTERVAL_MINUTES = r.INTERVAL_MINUTES
                            and r2.RN > r.RN - ?
                            and r2.RN <= r.RN) as SMA_SLOW,
                        (select avg(r2.RETURN_SIMPLE) from returns r2
                          where r2.SYMBOL = r.SYMBOL
                            and r2.MARKET_TYPE = r.MARKET_TYPE
                            and r2.INTERVAL_MINUTES = r.INTERVAL_MINUTES
                            and r2.RN > r.RN - ?
                            and r2.RN <= r.RN) as AVG_RETURN_WINDOW,
                        (select stddev_samp(r2.RETURN_SIMPLE) from returns r2
                          where r2.SYMBOL = r.SYMBOL
                            and r2.MARKET_TYPE = r.MARKET_TYPE
                            and r2.INTERVAL_MINUTES = r.INTERVAL_MINUTES
                            and r2.RN > r.RN - ?
                            and r2.RN <= r.RN) as STDDEV_WINDOW
                    from returns r
                ),
                pattern_recs as (
                    select
                        ? as PATTERN_ID,
                        r.SYMBOL,
                        r.MARKET_TYPE,
                        r.INTERVAL_MINUTES,
                        r.TS,
                        r.RETURN_SIMPLE as SCORE,
                        object_construct(
                            ''pattern_key'', ?,
                            ''return_simple'', r.RETURN_SIMPLE,
                            ''prev_close'', r.PREV_CLOSE,
                            ''close'', r.CLOSE,
                            ''sma_fast'', r.SMA_FAST,
                            ''sma_slow'', r.SMA_SLOW,
                            ''avg_return_window'', r.AVG_RETURN_WINDOW
                        ) as DETAILS
                    from scored r
                    where r.RETURN_SIMPLE is not null
                      and r.SMA_FAST is not null
                      and r.SMA_SLOW is not null
                      and r.CLOSE >= r.SMA_FAST
                      and r.CLOSE >= r.SMA_SLOW
                      and coalesce(r.AVG_RETURN_WINDOW, 0) >= ?
                      and (? is null or r.STDDEV_WINDOW is null or r.STDDEV_WINDOW = 0 or r.RETURN_SIMPLE / r.STDDEV_WINDOW >= ?)
                )
                select PATTERN_ID,
                       SYMBOL,
                       MARKET_TYPE,
                       INTERVAL_MINUTES,
                       TS,
                       SCORE,
                       DETAILS
                from pattern_recs p
                where not exists (
                    select 1
                    from MIP.APP.RECOMMENDATION_LOG existing
                    where existing.PATTERN_ID = p.PATTERN_ID
                      and existing.SYMBOL = p.SYMBOL
                      and existing.MARKET_TYPE = p.MARKET_TYPE
                      and existing.INTERVAL_MINUTES = p.INTERVAL_MINUTES
                      and existing.TS = p.TS
                )
            ' using (
                v_pattern_market_type,
                v_pattern_interval,
                v_pattern_lookback_days,
                v_pattern_fast_window,
                v_pattern_slow_window,
                v_pattern_fast_window,
                v_pattern_fast_window,
                v_pattern_id,
                v_pattern_key,
                v_pattern_min_return,
                v_pattern_min_zscore,
                v_pattern_min_zscore
            );

            v_inserted := v_inserted + sqlrowcount;
        end if;
    end for;

    return 'Inserted ' || v_inserted || ' momentum recommendations.';
end;
$$;
