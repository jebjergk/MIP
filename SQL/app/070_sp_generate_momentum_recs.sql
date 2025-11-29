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
    v_consecutive_up_bars     number := 3;
    v_slow_consecutive_bars   number := 2;
    v_min_trades_for_usage    number := 30;
    v_market_type             string;
    v_interval_minutes        number;
begin
    v_market_type := coalesce(P_MARKET_TYPE, 'STOCK');
    v_interval_minutes := coalesce(P_INTERVAL_MINUTES, 5);

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

    -- Insert new recommendations across active patterns
    insert into MIP.APP.RECOMMENDATION_LOG (
        PATTERN_ID,
        SYMBOL,
        MARKET_TYPE,
        INTERVAL_MINUTES,
        TS,
        SCORE,
        DETAILS
    )
    with active_patterns as (
        select
            PATTERN_ID,
            NAME as PATTERN_KEY,
            NAME as PATTERN_NAME
        from MIP.APP.PATTERN_DEFINITION
        where coalesce(IS_ACTIVE, 'N') = 'Y'
          and (LAST_TRADE_COUNT is null or LAST_TRADE_COUNT >= :v_min_trades_for_usage)
    ),
    stock_fast_base as (
        select
            r.*,
            lag(r.RETURN_SIMPLE, 1) over (
                partition by r.SYMBOL, r.MARKET_TYPE, r.INTERVAL_MINUTES
                order by r.TS
            ) as RET_LAG_1,
            lag(r.RETURN_SIMPLE, 2) over (
                partition by r.SYMBOL, r.MARKET_TYPE, r.INTERVAL_MINUTES
                order by r.TS
            ) as RET_LAG_2,
            lag(r.RETURN_SIMPLE, 3) over (
                partition by r.SYMBOL, r.MARKET_TYPE, r.INTERVAL_MINUTES
                order by r.TS
            ) as RET_LAG_3,
            max(r.CLOSE) over (
                partition by r.SYMBOL, r.MARKET_TYPE, r.INTERVAL_MINUTES
                order by r.TS
                rows between 20 preceding and 1 preceding
            ) as MAX_PREV_20_CLOSE,
            stddev_samp(r.RETURN_SIMPLE) over (
                partition by r.SYMBOL, r.MARKET_TYPE, r.INTERVAL_MINUTES
                order by r.TS
                rows between 19 preceding and current row
            ) as STDDEV_20
        from MIP.MART.MARKET_RETURNS r
        where r.MARKET_TYPE       = 'STOCK'
          and r.INTERVAL_MINUTES  = 5
          and r.RETURN_SIMPLE     is not null
          and r.VOLUME            >= :v_min_volume
          and r.TS                >= dateadd(day, -1, current_timestamp())
    ),
    stock_fast_scored as (
        select
            b.*,
            (case when b.RET_LAG_1 > 0 then 1 else 0 end
           + case when b.RET_LAG_2 > 0 then 1 else 0 end
           + case when b.RET_LAG_3 > 0 then 1 else 0 end) as POSITIVE_LAG_COUNT
        from stock_fast_base b
    ),
    stock_fast as (
        select
            ap.PATTERN_ID,
            'STOCK' as MARKET_TYPE,
            5       as INTERVAL_MINUTES,
            r.SYMBOL,
            r.TS,
            r.RETURN_SIMPLE                         as SCORE,
            object_construct(
                'pattern_key',   'STOCK_MOMENTUM_FAST',
                'return_simple', r.RETURN_SIMPLE,
                'prev_close',    r.PREV_CLOSE,
                'close',         r.CLOSE
            )                                       as DETAILS
        from active_patterns ap
        join stock_fast_scored r
          on ap.PATTERN_KEY = 'STOCK_MOMENTUM_FAST'
        where :v_market_type     = 'STOCK'
          and :v_interval_minutes = 5
          and r.RETURN_SIMPLE    >= :P_MIN_RETURN
          and r.POSITIVE_LAG_COUNT >= :v_consecutive_up_bars
          and r.MAX_PREV_20_CLOSE is not null
          and r.CLOSE >= r.MAX_PREV_20_CLOSE
          and (
                r.STDDEV_20 is null
             or (r.STDDEV_20 > 0 and r.RETURN_SIMPLE / r.STDDEV_20 >= :v_vol_adj_threshold)
          )
    ),
    stock_slow_base as (
        select
            r.*,
            lag(r.RETURN_SIMPLE, 1) over (
                partition by r.SYMBOL, r.MARKET_TYPE, r.INTERVAL_MINUTES
                order by r.TS
            ) as RET_LAG_1,
            lag(r.RETURN_SIMPLE, 2) over (
                partition by r.SYMBOL, r.MARKET_TYPE, r.INTERVAL_MINUTES
                order by r.TS
            ) as RET_LAG_2,
            max(r.CLOSE) over (
                partition by r.SYMBOL, r.MARKET_TYPE, r.INTERVAL_MINUTES
                order by r.TS
                rows between 30 preceding and 1 preceding
            ) as MAX_PREV_30_CLOSE,
            stddev_samp(r.RETURN_SIMPLE) over (
                partition by r.SYMBOL, r.MARKET_TYPE, r.INTERVAL_MINUTES
                order by r.TS
                rows between 29 preceding and current row
            ) as STDDEV_30
        from MIP.MART.MARKET_RETURNS r
        where r.MARKET_TYPE       = 'STOCK'
          and r.INTERVAL_MINUTES  = 5
          and r.RETURN_SIMPLE     is not null
          and r.VOLUME            >= (:v_min_volume / 2)
          and r.TS                >= dateadd(day, -3, current_timestamp())
    ),
    stock_slow_scored as (
        select
            b.*,
            (case when b.RET_LAG_1 > 0 then 1 else 0 end
           + case when b.RET_LAG_2 > 0 then 1 else 0 end) as POSITIVE_LAG_COUNT
        from stock_slow_base b
    ),
    stock_slow as (
        select
            ap.PATTERN_ID,
            'STOCK' as MARKET_TYPE,
            5       as INTERVAL_MINUTES,
            r.SYMBOL,
            r.TS,
            r.RETURN_SIMPLE                         as SCORE,
            object_construct(
                'pattern_key',   'STOCK_MOMENTUM_SLOW',
                'return_simple', r.RETURN_SIMPLE,
                'prev_close',    r.PREV_CLOSE,
                'close',         r.CLOSE
            )                                       as DETAILS
        from active_patterns ap
        join stock_slow_scored r
          on ap.PATTERN_KEY = 'STOCK_MOMENTUM_SLOW'
        where :v_market_type     = 'STOCK'
          and :v_interval_minutes = 5
          and r.RETURN_SIMPLE    >= (:P_MIN_RETURN / 2)
          and r.POSITIVE_LAG_COUNT >= :v_slow_consecutive_bars
          and r.MAX_PREV_30_CLOSE is not null
          and r.CLOSE >= r.MAX_PREV_30_CLOSE
          and (
                r.STDDEV_30 is null
             or (r.STDDEV_30 > 0 and r.RETURN_SIMPLE / r.STDDEV_30 >= (:v_vol_adj_threshold * 0.75))
          )
    ),
    fx_base as (
        select
            mb.*,
            lag(mb.CLOSE) over (
                partition by mb.SYMBOL, mb.MARKET_TYPE, mb.INTERVAL_MINUTES
                order by mb.TS
            ) as PREV_CLOSE,
            avg(mb.CLOSE) over (
                partition by mb.SYMBOL, mb.MARKET_TYPE, mb.INTERVAL_MINUTES
                order by mb.TS
                rows between 9 preceding and current row
            ) as SMA_SHORT,
            avg(mb.CLOSE) over (
                partition by mb.SYMBOL, mb.MARKET_TYPE, mb.INTERVAL_MINUTES
                order by mb.TS
                rows between 19 preceding and current row
            ) as SMA_20
        from MIP.MART.MARKET_BARS mb
        where mb.MARKET_TYPE       = 'FX'
          and mb.INTERVAL_MINUTES  = 1440
          and mb.TS                >= dateadd(day, -60, current_timestamp())
    ),
    fx_returns as (
        select
            f.*,
            case when f.PREV_CLOSE is null or f.PREV_CLOSE = 0 then null else (f.CLOSE / f.PREV_CLOSE) - 1 end as RETURN_SIMPLE
        from fx_base f
    ),
    fx_scored as (
        select
            r.*,
            lag(r.RETURN_SIMPLE, 1) over (
                partition by r.SYMBOL, r.MARKET_TYPE, r.INTERVAL_MINUTES
                order by r.TS
            ) as RET_LAG_1,
            lag(r.RETURN_SIMPLE, 2) over (
                partition by r.SYMBOL, r.MARKET_TYPE, r.INTERVAL_MINUTES
                order by r.TS
            ) as RET_LAG_2,
            avg(r.RETURN_SIMPLE) over (
                partition by r.SYMBOL, r.MARKET_TYPE, r.INTERVAL_MINUTES
                order by r.TS
                rows between 4 preceding and current row
            ) as AVG_RET_5
        from fx_returns r
    ),
    fx_daily as (
        select
            ap.PATTERN_ID,
            'FX'    as MARKET_TYPE,
            1440    as INTERVAL_MINUTES,
            r.SYMBOL,
            r.TS,
            r.RETURN_SIMPLE                         as SCORE,
            object_construct(
                'pattern_key',     'FX_MOMENTUM_DAILY',
                'return_simple',   r.RETURN_SIMPLE,
                'prev_close',      r.PREV_CLOSE,
                'close',           r.CLOSE,
                'sma_short',       r.SMA_SHORT,
                'sma_20',          r.SMA_20,
                'avg_ret_5',       r.AVG_RET_5
            )                                       as DETAILS
        from active_patterns ap
        join fx_scored r
          on ap.PATTERN_KEY = 'FX_MOMENTUM_DAILY'
        where :v_market_type      = 'FX'
          and :v_interval_minutes = 1440
          and r.RETURN_SIMPLE is not null
          and r.SMA_SHORT is not null
          and r.CLOSE >= r.SMA_SHORT
          and r.SMA_20  is not null
          and r.CLOSE >= r.SMA_20
          and coalesce(r.AVG_RET_5, 0) >= (:P_MIN_RETURN / 2)
          and ((r.RET_LAG_1 is null or r.RET_LAG_1 > 0) and (r.RET_LAG_2 is null or r.RET_LAG_2 > 0))
    ),
    combined_recs as (
        select * from stock_fast
        union all
        select * from stock_slow
        union all
        select * from fx_daily
    )
    select
        r.PATTERN_ID,
        r.SYMBOL,
        r.MARKET_TYPE,
        r.INTERVAL_MINUTES,
        r.TS,
        r.SCORE,
        r.DETAILS
    from combined_recs r
    left join MIP.APP.RECOMMENDATION_LOG existing
        on existing.PATTERN_ID       = r.PATTERN_ID
       and existing.SYMBOL           = r.SYMBOL
       and existing.MARKET_TYPE      = r.MARKET_TYPE
       and existing.INTERVAL_MINUTES = r.INTERVAL_MINUTES
       and existing.TS               = r.TS
    where existing.RECOMMENDATION_ID is null;  -- avoid duplicates

    v_inserted := sqlrowcount;

    return 'Inserted ' || v_inserted || ' momentum recommendations.';
end;
$$;
