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
    from MIP.MART.MARKET_RETURNS r
    left join MIP.APP.RECOMMENDATION_LOG existing
        on existing.PATTERN_ID       = :v_pattern_id
       and existing.SYMBOL           = r.SYMBOL
       and existing.MARKET_TYPE      = r.MARKET_TYPE
       and existing.INTERVAL_MINUTES = r.INTERVAL_MINUTES
       and existing.TS               = r.TS
    where r.MARKET_TYPE      = 'STOCK'
      and r.INTERVAL_MINUTES = 5
      and r.RETURN_SIMPLE    is not null
      and r.RETURN_SIMPLE    >= :P_MIN_RETURN
      and r.TS               >= dateadd(day, -2, current_timestamp())
      and existing.RECOMMENDATION_ID is null;  -- avoid duplicates

    v_inserted := sqlrowcount;

    return 'Inserted ' || v_inserted || ' momentum recommendations.';
end;
$$;
