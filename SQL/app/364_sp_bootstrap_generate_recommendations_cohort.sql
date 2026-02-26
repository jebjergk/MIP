-- 364_sp_bootstrap_generate_recommendations_cohort.sql
-- Purpose: Cohort-scoped daily recommendation generation for bootstrap replay.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_BOOTSTRAP_GENERATE_RECOMMENDATIONS_COHORT(
    P_EFFECTIVE_TO_TS timestamp_ntz,
    P_SYMBOL_COHORT string default 'VOL_EXP',
    P_MARKET_TYPE string default 'STOCK',
    P_INTERVAL_MINUTES number default 1440,
    P_PARENT_RUN_ID string default null
)
returns variant
language sql
execute as caller
as
$$
declare
    v_rows_before number := 0;
    v_rows_after number := 0;
    v_inserted number := 0;
begin
    select count(*)
      into :v_rows_before
      from MIP.APP.RECOMMENDATION_LOG r
     where r.MARKET_TYPE = :P_MARKET_TYPE
       and r.INTERVAL_MINUTES = :P_INTERVAL_MINUTES
       and r.TS::date = :P_EFFECTIVE_TO_TS::date;

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
            p.PATTERN_ID,
            upper(coalesce(p.PARAMS_JSON:market_type::string, 'STOCK')) as MARKET_TYPE,
            coalesce(p.PARAMS_JSON:interval_minutes::number, 1440) as INTERVAL_MINUTES,
            coalesce(p.PARAMS_JSON:min_return::float, 0.002) as MIN_RETURN,
            coalesce(p.PARAMS_JSON:slow_window::number, 3) as POSITIVE_LAG_COUNT_MIN,
            coalesce(p.PARAMS_JSON:fast_window::number, 20) as FAST_WINDOW
        from MIP.APP.PATTERN_DEFINITION p
        where coalesce(p.IS_ACTIVE, 'N') = 'Y'
          and coalesce(p.ENABLED, true)
          and upper(coalesce(p.PARAMS_JSON:market_type::string, 'STOCK')) = upper(:P_MARKET_TYPE)
          and coalesce(p.PARAMS_JSON:interval_minutes::number, 1440) = :P_INTERVAL_MINUTES
    ),
    cohort_symbols as (
        select distinct upper(iu.SYMBOL) as SYMBOL
        from MIP.APP.INGEST_UNIVERSE iu
        where coalesce(iu.IS_ENABLED, true)
          and iu.INTERVAL_MINUTES = :P_INTERVAL_MINUTES
          and upper(iu.MARKET_TYPE) = upper(:P_MARKET_TYPE)
          and upper(coalesce(iu.SYMBOL_COHORT, 'CORE')) = upper(:P_SYMBOL_COHORT)
    ),
    eligible_returns as (
        select
            r.SYMBOL,
            r.MARKET_TYPE,
            r.INTERVAL_MINUTES,
            r.TS,
            r.RETURN_SIMPLE,
            r.PREV_CLOSE,
            r.CLOSE,
            row_number() over (
                partition by r.SYMBOL, r.MARKET_TYPE, r.INTERVAL_MINUTES
                order by r.TS
            ) as RN
        from MIP.MART.MARKET_RETURNS r
        join cohort_symbols c
          on upper(r.SYMBOL) = c.SYMBOL
        where r.TS::date = :P_EFFECTIVE_TO_TS::date
          and upper(r.MARKET_TYPE) = upper(:P_MARKET_TYPE)
          and r.INTERVAL_MINUTES = :P_INTERVAL_MINUTES
          and r.RETURN_SIMPLE is not null
    ),
    staged as (
        select
            p.PATTERN_ID,
            er.SYMBOL,
            er.MARKET_TYPE,
            er.INTERVAL_MINUTES,
            er.TS,
            er.RETURN_SIMPLE as SCORE,
            object_construct(
                'bootstrap_mode', true,
                'symbol_cohort', :P_SYMBOL_COHORT,
                'effective_to_ts', :P_EFFECTIVE_TO_TS,
                'return_simple', er.RETURN_SIMPLE,
                'min_return', p.MIN_RETURN,
                'prev_close', er.PREV_CLOSE,
                'close', er.CLOSE
            ) as DETAILS
        from eligible_returns er
        join active_patterns p
          on p.MARKET_TYPE = upper(er.MARKET_TYPE)
         and p.INTERVAL_MINUTES = er.INTERVAL_MINUTES
        where er.RETURN_SIMPLE >= p.MIN_RETURN
    )
    select
        s.PATTERN_ID,
        s.SYMBOL,
        s.MARKET_TYPE,
        s.INTERVAL_MINUTES,
        s.TS,
        s.SCORE,
        s.DETAILS
    from staged s
    where not exists (
        select 1
        from MIP.APP.RECOMMENDATION_LOG r
        where r.PATTERN_ID = s.PATTERN_ID
          and r.SYMBOL = s.SYMBOL
          and r.MARKET_TYPE = s.MARKET_TYPE
          and r.INTERVAL_MINUTES = s.INTERVAL_MINUTES
          and r.TS = s.TS
    );

    select count(*)
      into :v_rows_after
      from MIP.APP.RECOMMENDATION_LOG r
     where r.MARKET_TYPE = :P_MARKET_TYPE
       and r.INTERVAL_MINUTES = :P_INTERVAL_MINUTES
       and r.TS::date = :P_EFFECTIVE_TO_TS::date;

    v_inserted := :v_rows_after - :v_rows_before;

    return object_construct(
        'status', 'SUCCESS',
        'effective_to_ts', :P_EFFECTIVE_TO_TS,
        'symbol_cohort', :P_SYMBOL_COHORT,
        'market_type', :P_MARKET_TYPE,
        'interval_minutes', :P_INTERVAL_MINUTES,
        'rows_before', :v_rows_before,
        'rows_after', :v_rows_after,
        'signals_created_count', :v_inserted
    );
end;
$$;

