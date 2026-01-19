-- 175_sp_validate_sim_readiness.sql
-- Purpose: Validate data readiness for portfolio simulation

use role MIP_ADMIN_ROLE;
use database MIP;

create table if not exists MIP.APP.SIM_READINESS_AUDIT (
    AUDIT_TS   timestamp_ntz default current_timestamp(),
    RUN_ID     string        default uuid_string(),
    AS_OF_TS   timestamp_ntz,
    SIM_READY  boolean,
    REASONS    variant,
    DETAILS    variant
);

create or replace procedure MIP.APP.SP_VALIDATE_SIM_READINESS(
    P_AS_OF_DATE timestamp_ntz
)
returns variant
language sql
execute as owner
as
$$
declare
    v_as_of_ts timestamp_ntz := coalesce(:P_AS_OF_DATE, current_timestamp());
    v_min_sample_size number := 30;
    v_max_horizon number := 0;
    v_missing_outcomes number := 0;
    v_null_success_returns number := 0;
    v_horizon_mismatch number := 0;
    v_sample_shortfall number := 0;
    v_reasons array := array_construct();
    v_sim_ready boolean := true;
    v_details variant;
    v_run_id string := uuid_string();
begin
    select to_number(CONFIG_VALUE)
      into v_min_sample_size
      from MIP.APP.APP_CONFIG
     where CONFIG_KEY = 'SIM_MIN_SAMPLE_SIZE';

    v_min_sample_size := coalesce(v_min_sample_size, 30);

    select max(HORIZON_BARS)
      into v_max_horizon
      from (values (1), (3), (5), (10), (20)) v(HORIZON_BARS);

    select count(*)
      into v_missing_outcomes
      from (
        with horizons as (
            select column1::number as HORIZON_BARS
            from values (1), (3), (5), (10), (20)
        ),
        entry_bars as (
            select
                r.RECOMMENDATION_ID,
                r.SYMBOL,
                r.MARKET_TYPE,
                r.INTERVAL_MINUTES,
                r.TS as ENTRY_TS
            from MIP.APP.RECOMMENDATION_LOG r
            where r.TS <= :v_as_of_ts
        ),
        future_ranked as (
            select
                e.RECOMMENDATION_ID,
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
        ),
        max_future as (
            select
                RECOMMENDATION_ID,
                max(FUTURE_RN) as MAX_FUTURE_BARS
            from future_ranked
            group by RECOMMENDATION_ID
        ),
        eligible as (
            select RECOMMENDATION_ID
            from max_future
            where MAX_FUTURE_BARS >= :v_max_horizon
        ),
        expected as (
            select e.RECOMMENDATION_ID, h.HORIZON_BARS
            from eligible e
            cross join horizons h
        )
        select e.RECOMMENDATION_ID, e.HORIZON_BARS
        from expected e
        left join MIP.APP.RECOMMENDATION_OUTCOMES o
          on o.RECOMMENDATION_ID = e.RECOMMENDATION_ID
         and o.HORIZON_BARS = e.HORIZON_BARS
        where o.RECOMMENDATION_ID is null
           or o.EXIT_TS is null
    ) missing;

    select count(*)
      into v_null_success_returns
      from MIP.APP.RECOMMENDATION_OUTCOMES
     where EVAL_STATUS = 'SUCCESS'
       and REALIZED_RETURN is null;

    select count(*)
      into v_horizon_mismatch
      from (
        with horizons as (
            select column1::number as HORIZON_BARS
            from values (1), (3), (5), (10), (20)
        ),
        entry_bars as (
            select
                r.RECOMMENDATION_ID,
                r.SYMBOL,
                r.MARKET_TYPE,
                r.INTERVAL_MINUTES,
                r.TS as ENTRY_TS
            from MIP.APP.RECOMMENDATION_LOG r
            where r.TS <= :v_as_of_ts
        ),
        future_ranked as (
            select
                e.RECOMMENDATION_ID,
                b.TS as EXIT_TS,
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
        ),
        expected_exit as (
            select
                fr.RECOMMENDATION_ID,
                h.HORIZON_BARS,
                fr.EXIT_TS
            from future_ranked fr
            join horizons h
              on fr.FUTURE_RN = h.HORIZON_BARS
        )
        select o.RECOMMENDATION_ID
        from expected_exit e
        join MIP.APP.RECOMMENDATION_OUTCOMES o
          on o.RECOMMENDATION_ID = e.RECOMMENDATION_ID
         and o.HORIZON_BARS = e.HORIZON_BARS
        where o.EXIT_TS is not null
          and e.EXIT_TS <> o.EXIT_TS
    ) mismatch;

    select count(*)
      into v_sample_shortfall
      from (
        select
            r.PATTERN_ID,
            r.MARKET_TYPE,
            o.HORIZON_BARS,
            count(*) as SAMPLE_COUNT
        from MIP.APP.RECOMMENDATION_LOG r
        join MIP.APP.RECOMMENDATION_OUTCOMES o
          on o.RECOMMENDATION_ID = r.RECOMMENDATION_ID
        where r.TS <= :v_as_of_ts
          and o.EVAL_STATUS = 'SUCCESS'
        group by r.PATTERN_ID, r.MARKET_TYPE, o.HORIZON_BARS
        having count(*) < :v_min_sample_size
    ) shortfalls;

    if (v_missing_outcomes > 0) then
        v_reasons := array_append(v_reasons, 'Missing outcomes for recommendations older than max horizon');
        v_sim_ready := false;
    end if;

    if (v_null_success_returns > 0) then
        v_reasons := array_append(v_reasons, 'Null realized returns on SUCCESS outcomes');
        v_sim_ready := false;
    end if;

    if (v_horizon_mismatch > 0) then
        v_reasons := array_append(v_reasons, 'Outcome exit timestamps do not align with horizon bars');
        v_sim_ready := false;
    end if;

    if (v_sample_shortfall > 0) then
        v_reasons := array_append(v_reasons, 'Sample size below threshold for pattern/market type/horizon');
        v_sim_ready := false;
    end if;

    v_details := object_construct(
        'missing_outcomes', :v_missing_outcomes,
        'null_success_returns', :v_null_success_returns,
        'horizon_mismatch', :v_horizon_mismatch,
        'sample_shortfall_groups', :v_sample_shortfall,
        'min_sample_size', :v_min_sample_size,
        'max_horizon_bars', :v_max_horizon,
        'as_of_ts', :v_as_of_ts
    );

    insert into MIP.APP.SIM_READINESS_AUDIT (
        AUDIT_TS,
        RUN_ID,
        AS_OF_TS,
        SIM_READY,
        REASONS,
        DETAILS
    ) values (
        current_timestamp(),
        :v_run_id,
        :v_as_of_ts,
        :v_sim_ready,
        :v_reasons,
        :v_details
    );

    return object_construct(
        'SIM_READY', :v_sim_ready,
        'REASONS', :v_reasons,
        'DETAILS', :v_details,
        'RUN_ID', :v_run_id
    );
end;
$$;
