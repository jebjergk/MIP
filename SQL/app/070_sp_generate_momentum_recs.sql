use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_GENERATE_MOMENTUM_RECS(
    P_MIN_RETURN number       -- e.g. 0.002 for +0.2% threshold
)
returns varchar
language sql
as
$$
declare
    v_pattern_id number;
    v_inserted   number := 0;
    v_min_volume number := 1000;
    v_vol_adj_threshold number := 1.0;
    v_consecutive_up_bars number := 3;
begin
    -- Find the MOMENTUM_DEMO pattern
    select PATTERN_ID
      into :v_pattern_id
    from MIP.APP.PATTERN_DEFINITION
    where NAME = 'MOMENTUM_DEMO'
      and ENABLED = true
    limit 1;

    if (v_pattern_id is null) then
        return 'No enabled MOMENTUM_DEMO pattern found. Run SP_SEED_MIP_DEMO() first.';
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

    -- Insert new recommendations for stocks with RETURN_SIMPLE >= threshold
    -- Limit to a recent time window (e.g. last 2 days) to keep volume manageable
    insert into MIP.APP.RECOMMENDATION_LOG (
        PATTERN_ID,
        SYMBOL,
        MARKET_TYPE,
        INTERVAL_MINUTES,
        TS,
        SCORE,
        DETAILS
    )
    select
        :v_pattern_id                           as PATTERN_ID,
        r.SYMBOL,
        r.MARKET_TYPE,
        r.INTERVAL_MINUTES,
        r.TS,
        r.RETURN_SIMPLE                         as SCORE,
        object_construct(
            'return_simple', r.RETURN_SIMPLE,
            'prev_close',    r.PREV_CLOSE,
            'close',         r.CLOSE
        )                                       as DETAILS
    from (
        with base as (
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
            where r.MARKET_TYPE      = 'STOCK'
              and r.INTERVAL_MINUTES = 5
              and r.RETURN_SIMPLE    is not null
              and r.VOLUME           >= :v_min_volume
              and r.TS               >= dateadd(day, -2, current_timestamp())
        ),
        scored as (
            select
                b.*,
                (case when b.RET_LAG_1 > 0 then 1 else 0 end
               + case when b.RET_LAG_2 > 0 then 1 else 0 end
               + case when b.RET_LAG_3 > 0 then 1 else 0 end) as POSITIVE_LAG_COUNT
            from base b
        )
        select *
        from scored
        where RETURN_SIMPLE >= :P_MIN_RETURN
          and POSITIVE_LAG_COUNT >= :v_consecutive_up_bars
          and MAX_PREV_20_CLOSE is not null
          and CLOSE >= MAX_PREV_20_CLOSE
          and (
                STDDEV_20 is null
             or (STDDEV_20 > 0 and RETURN_SIMPLE / STDDEV_20 >= :v_vol_adj_threshold)
          )
    ) r
    left join MIP.APP.RECOMMENDATION_LOG existing
        on existing.PATTERN_ID       = :v_pattern_id
       and existing.SYMBOL           = r.SYMBOL
       and existing.MARKET_TYPE      = r.MARKET_TYPE
       and existing.INTERVAL_MINUTES = r.INTERVAL_MINUTES
       and existing.TS               = r.TS
    where existing.RECOMMENDATION_ID is null;  -- avoid duplicates

    v_inserted := sqlrowcount;

    return 'Inserted ' || v_inserted || ' momentum recommendations.';
end;
$$;
