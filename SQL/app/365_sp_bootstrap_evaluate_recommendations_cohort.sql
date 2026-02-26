-- 365_sp_bootstrap_evaluate_recommendations_cohort.sql
-- Purpose: Cohort-scoped outcome evaluation (no rewrite of unrelated symbols).

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_BOOTSTRAP_EVALUATE_RECOMMENDATIONS_COHORT(
    P_FROM_TS timestamp_ntz,
    P_TO_TS timestamp_ntz,
    P_SYMBOL_COHORT string default 'VOL_EXP',
    P_MARKET_TYPE string default 'STOCK',
    P_INTERVAL_MINUTES number default 1440,
    P_MIN_RETURN_THRESHOLD float default 0.0
)
returns variant
language sql
execute as caller
as
$$
declare
    v_from_ts timestamp_ntz := coalesce(:P_FROM_TS, dateadd(day, -90, current_timestamp()::timestamp_ntz));
    v_to_ts timestamp_ntz := coalesce(:P_TO_TS, current_timestamp()::timestamp_ntz);
    v_thr float := coalesce(:P_MIN_RETURN_THRESHOLD, 0.0);
    v_before number := 0;
    v_after number := 0;
    v_delta number := 0;
begin
    select count(*)
      into :v_before
      from MIP.APP.RECOMMENDATION_OUTCOMES o
      join MIP.APP.RECOMMENDATION_LOG r
        on r.RECOMMENDATION_ID = o.RECOMMENDATION_ID
      join MIP.APP.INGEST_UNIVERSE iu
        on upper(iu.SYMBOL) = upper(r.SYMBOL)
       and upper(iu.MARKET_TYPE) = upper(r.MARKET_TYPE)
       and iu.INTERVAL_MINUTES = r.INTERVAL_MINUTES
     where upper(coalesce(iu.SYMBOL_COHORT, 'CORE')) = upper(:P_SYMBOL_COHORT)
       and upper(r.MARKET_TYPE) = upper(:P_MARKET_TYPE)
       and r.INTERVAL_MINUTES = :P_INTERVAL_MINUTES
       and r.TS >= :v_from_ts
       and r.TS <= :v_to_ts;

    merge into MIP.APP.RECOMMENDATION_OUTCOMES t
    using (
        with cohort_recs as (
            select
                r.RECOMMENDATION_ID,
                r.SYMBOL,
                r.MARKET_TYPE,
                r.INTERVAL_MINUTES,
                r.TS as ENTRY_TS
            from MIP.APP.RECOMMENDATION_LOG r
            join MIP.APP.INGEST_UNIVERSE iu
              on upper(iu.SYMBOL) = upper(r.SYMBOL)
             and upper(iu.MARKET_TYPE) = upper(r.MARKET_TYPE)
             and iu.INTERVAL_MINUTES = r.INTERVAL_MINUTES
            where upper(coalesce(iu.SYMBOL_COHORT, 'CORE')) = upper(:P_SYMBOL_COHORT)
              and upper(r.MARKET_TYPE) = upper(:P_MARKET_TYPE)
              and r.INTERVAL_MINUTES = :P_INTERVAL_MINUTES
              and r.TS >= :v_from_ts
              and r.TS <= :v_to_ts
        ),
        entry_bars as (
            select
                c.RECOMMENDATION_ID,
                c.SYMBOL,
                c.MARKET_TYPE,
                c.INTERVAL_MINUTES,
                c.ENTRY_TS,
                b.CLOSE::float as ENTRY_PRICE
            from cohort_recs c
            left join MIP.MART.MARKET_BARS b
              on b.SYMBOL = c.SYMBOL
             and b.MARKET_TYPE = c.MARKET_TYPE
             and b.INTERVAL_MINUTES = c.INTERVAL_MINUTES
             and b.TS = c.ENTRY_TS
        ),
        rec_bar_horizons as (
            select distinct
                e.RECOMMENDATION_ID,
                h.HORIZON_LENGTH as HORIZON_BARS
            from entry_bars e
            join MIP.APP.HORIZON_DEFINITION h
              on h.INTERVAL_MINUTES = e.INTERVAL_MINUTES
             and h.IS_ACTIVE = true
             and h.HORIZON_TYPE in ('BAR', 'DAY')
        ),
        future_ranked as (
            select
                e.RECOMMENDATION_ID,
                b.TS as EXIT_TS,
                b.CLOSE::float as EXIT_PRICE,
                row_number() over (
                    partition by e.RECOMMENDATION_ID
                    order by b.TS
                ) as FUTURE_RN
            from entry_bars e
            join MIP.MART.MARKET_BARS b
              on b.SYMBOL = e.SYMBOL
             and b.MARKET_TYPE = e.MARKET_TYPE
             and b.INTERVAL_MINUTES = e.INTERVAL_MINUTES
             and b.TS > e.ENTRY_TS
            where e.ENTRY_PRICE is not null
              and e.ENTRY_PRICE <> 0
        ),
        bar_outcomes as (
            select
                e.RECOMMENDATION_ID,
                e.ENTRY_TS,
                e.ENTRY_PRICE,
                rh.HORIZON_BARS,
                fr.EXIT_TS,
                fr.EXIT_PRICE
            from entry_bars e
            join rec_bar_horizons rh
              on rh.RECOMMENDATION_ID = e.RECOMMENDATION_ID
            left join future_ranked fr
              on fr.RECOMMENDATION_ID = e.RECOMMENDATION_ID
             and fr.FUTURE_RN = rh.HORIZON_BARS
        ),
        eod_exits as (
            select
                e.RECOMMENDATION_ID,
                b.TS as EXIT_TS,
                b.CLOSE::float as EXIT_PRICE
            from entry_bars e
            join MIP.APP.HORIZON_DEFINITION h
              on h.INTERVAL_MINUTES = e.INTERVAL_MINUTES
             and h.IS_ACTIVE = true
             and h.HORIZON_TYPE = 'SESSION'
            join MIP.MART.MARKET_BARS b
              on b.SYMBOL = e.SYMBOL
             and b.MARKET_TYPE = e.MARKET_TYPE
             and b.INTERVAL_MINUTES = e.INTERVAL_MINUTES
             and b.TS::date = e.ENTRY_TS::date
             and b.TS > e.ENTRY_TS
            where e.ENTRY_PRICE is not null
              and e.ENTRY_PRICE <> 0
            qualify row_number() over (partition by e.RECOMMENDATION_ID order by b.TS desc) = 1
        ),
        eod_outcomes as (
            select
                e.RECOMMENDATION_ID,
                e.ENTRY_TS,
                e.ENTRY_PRICE,
                -1 as HORIZON_BARS,
                x.EXIT_TS,
                x.EXIT_PRICE
            from entry_bars e
            join eod_exits x
              on x.RECOMMENDATION_ID = e.RECOMMENDATION_ID
        ),
        all_outcomes as (
            select * from bar_outcomes
            union all
            select * from eod_outcomes
        )
        select
            ao.RECOMMENDATION_ID,
            ao.HORIZON_BARS,
            ao.ENTRY_TS,
            ao.EXIT_TS,
            ao.ENTRY_PRICE,
            ao.EXIT_PRICE,
            case
                when ao.ENTRY_PRICE is not null and ao.ENTRY_PRICE <> 0
                 and ao.EXIT_PRICE is not null and ao.EXIT_PRICE <> 0
                then (ao.EXIT_PRICE / ao.ENTRY_PRICE) - 1
                else null
            end as REALIZED_RETURN,
            'LONG' as DIRECTION,
            case
                when ao.ENTRY_PRICE is not null and ao.ENTRY_PRICE <> 0
                 and ao.EXIT_PRICE is not null and ao.EXIT_PRICE <> 0
                then ((ao.EXIT_PRICE / ao.ENTRY_PRICE) - 1) >= :v_thr
                else null
            end as HIT_FLAG,
            'THRESHOLD' as HIT_RULE,
            :v_thr as MIN_RETURN_THRESHOLD,
            case
                when ao.ENTRY_PRICE is null or ao.ENTRY_PRICE = 0 then 'FAILED_NO_ENTRY_BAR'
                when ao.EXIT_PRICE is null or ao.EXIT_PRICE = 0 then 'INSUFFICIENT_FUTURE_DATA'
                else 'SUCCESS'
            end as EVAL_STATUS,
            current_timestamp() as CALCULATED_AT
        from all_outcomes ao
    ) s
      on t.RECOMMENDATION_ID = s.RECOMMENDATION_ID
     and t.HORIZON_BARS = s.HORIZON_BARS
    when matched then update set
        t.ENTRY_TS = s.ENTRY_TS,
        t.EXIT_TS = s.EXIT_TS,
        t.ENTRY_PRICE = s.ENTRY_PRICE,
        t.EXIT_PRICE = s.EXIT_PRICE,
        t.REALIZED_RETURN = s.REALIZED_RETURN,
        t.DIRECTION = s.DIRECTION,
        t.HIT_FLAG = s.HIT_FLAG,
        t.HIT_RULE = s.HIT_RULE,
        t.MIN_RETURN_THRESHOLD = s.MIN_RETURN_THRESHOLD,
        t.EVAL_STATUS = s.EVAL_STATUS,
        t.CALCULATED_AT = s.CALCULATED_AT
    when not matched then insert (
        RECOMMENDATION_ID, HORIZON_BARS, ENTRY_TS, EXIT_TS, ENTRY_PRICE, EXIT_PRICE,
        REALIZED_RETURN, DIRECTION, HIT_FLAG, HIT_RULE, MIN_RETURN_THRESHOLD, EVAL_STATUS, CALCULATED_AT
    ) values (
        s.RECOMMENDATION_ID, s.HORIZON_BARS, s.ENTRY_TS, s.EXIT_TS, s.ENTRY_PRICE, s.EXIT_PRICE,
        s.REALIZED_RETURN, s.DIRECTION, s.HIT_FLAG, s.HIT_RULE, s.MIN_RETURN_THRESHOLD, s.EVAL_STATUS, s.CALCULATED_AT
    );

    select count(*)
      into :v_after
      from MIP.APP.RECOMMENDATION_OUTCOMES o
      join MIP.APP.RECOMMENDATION_LOG r
        on r.RECOMMENDATION_ID = o.RECOMMENDATION_ID
      join MIP.APP.INGEST_UNIVERSE iu
        on upper(iu.SYMBOL) = upper(r.SYMBOL)
       and upper(iu.MARKET_TYPE) = upper(r.MARKET_TYPE)
       and iu.INTERVAL_MINUTES = r.INTERVAL_MINUTES
     where upper(coalesce(iu.SYMBOL_COHORT, 'CORE')) = upper(:P_SYMBOL_COHORT)
       and upper(r.MARKET_TYPE) = upper(:P_MARKET_TYPE)
       and r.INTERVAL_MINUTES = :P_INTERVAL_MINUTES
       and r.TS >= :v_from_ts
       and r.TS <= :v_to_ts;

    v_delta := :v_after - :v_before;

    return object_construct(
        'status', 'SUCCESS',
        'symbol_cohort', :P_SYMBOL_COHORT,
        'market_type', :P_MARKET_TYPE,
        'interval_minutes', :P_INTERVAL_MINUTES,
        'from_ts', :v_from_ts,
        'to_ts', :v_to_ts,
        'rows_before', :v_before,
        'rows_after', :v_after,
        'outcomes_computed_count', :v_delta
    );
end;
$$;

