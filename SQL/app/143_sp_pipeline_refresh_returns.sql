-- 143_sp_pipeline_refresh_returns.sql
-- Purpose: Pipeline step to refresh MARKET_RETURNS view

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_PIPELINE_REFRESH_RETURNS(
    P_PARENT_RUN_ID string default null
)
returns variant
language sql
execute as caller
as
$$
declare
    v_run_id string := coalesce(nullif(current_query_tag(), ''), uuid_string());
    v_step_start timestamp_ntz := current_timestamp();
    v_step_end timestamp_ntz;
    v_latest_market_bars_ts timestamp_ntz;
    v_latest_returns_ts timestamp_ntz;
    v_market_bars_at_latest_ts number := 0;
    v_returns_at_latest_ts number := 0;
    v_effective_cap timestamp_ntz;
begin
    -- Use effective_to_ts override when present (replay/time-travel)
    select EFFECTIVE_TO_TS into :v_effective_cap
      from MIP.APP.RUN_SCOPE_OVERRIDE
     where RUN_ID = :v_run_id
     limit 1;

    select max(TS)
      into :v_latest_market_bars_ts
      from MIP.MART.MARKET_BARS
     where (:v_effective_cap is null or TS <= :v_effective_cap);

    if (v_latest_market_bars_ts is not null) then
        select count(*)
          into :v_market_bars_at_latest_ts
          from MIP.MART.MARKET_BARS
         where TS = :v_latest_market_bars_ts
           and (:v_effective_cap is null or TS <= :v_effective_cap);
    end if;

    begin
        create or replace view MIP.MART.MARKET_RETURNS as
        with bars_scoped as (
            select *
              from MIP.MART.MARKET_BARS
             where (
                   (select EFFECTIVE_TO_TS from MIP.APP.RUN_SCOPE_OVERRIDE where RUN_ID = current_query_tag() limit 1) is null
                   or TS <= (select EFFECTIVE_TO_TS from MIP.APP.RUN_SCOPE_OVERRIDE where RUN_ID = current_query_tag() limit 1)
             )
        ),
        deduped as (
            select
                TS,
                SYMBOL,
                SOURCE,
                MARKET_TYPE,
                INTERVAL_MINUTES,
                OPEN,
                HIGH,
                LOW,
                CLOSE,
                VOLUME,
                INGESTED_AT
            from (
                select
                    TS,
                    SYMBOL,
                    SOURCE,
                    MARKET_TYPE,
                    INTERVAL_MINUTES,
                    OPEN,
                    HIGH,
                    LOW,
                    CLOSE,
                    VOLUME,
                    INGESTED_AT,
                    row_number() over (
                        partition by MARKET_TYPE, SYMBOL, INTERVAL_MINUTES, TS
                        order by INGESTED_AT desc, SOURCE desc
                    ) as RN
                from bars_scoped
            )
            where RN = 1
        ),
        ordered as (
            select
                TS,
                SYMBOL,
                SOURCE,
                MARKET_TYPE,
                INTERVAL_MINUTES,
                OPEN,
                HIGH,
                LOW,
                CLOSE,
                VOLUME,
                INGESTED_AT,
                lag(CLOSE) over (
                    partition by SYMBOL, MARKET_TYPE, INTERVAL_MINUTES
                    order by TS
                ) as PREV_CLOSE
            from deduped
        )
        select
            TS,
            SYMBOL,
            SOURCE,
            MARKET_TYPE,
            INTERVAL_MINUTES,
            OPEN,
            HIGH,
            LOW,
            CLOSE,
            VOLUME,
            INGESTED_AT,
            PREV_CLOSE,
            case
                when PREV_CLOSE is not null and PREV_CLOSE <> 0
                then (CLOSE - PREV_CLOSE) / PREV_CLOSE
                else null
            end as RETURN_SIMPLE,
            case
                when PREV_CLOSE is not null and PREV_CLOSE > 0 and CLOSE > 0
                then ln(CLOSE / PREV_CLOSE)
                else null
            end as RETURN_LOG
        from ordered;

        select max(TS)
          into :v_latest_returns_ts
          from MIP.MART.MARKET_RETURNS;

        if (v_latest_returns_ts is not null) then
            select count(*)
              into :v_returns_at_latest_ts
              from MIP.MART.MARKET_RETURNS
             where TS = :v_latest_returns_ts;
        end if;

        v_step_end := current_timestamp();

        call MIP.APP.SP_AUDIT_LOG_STEP(
            :P_PARENT_RUN_ID,
            'RETURNS_REFRESH',
            'SUCCESS',
            null,
            object_construct(
                'step_name', 'returns_refresh',
                'scope', 'AGG',
                'scope_key', null,
                'started_at', :v_step_start,
                'completed_at', :v_step_end,
                'latest_market_bars_ts', :v_latest_market_bars_ts,
                'latest_market_returns_ts', :v_latest_returns_ts,
                'market_bars_at_latest_ts', :v_market_bars_at_latest_ts,
                'returns_at_latest_ts', :v_returns_at_latest_ts
            ),
            null
        );

        return object_construct(
            'status', 'SUCCESS',
            'latest_market_bars_ts', :v_latest_market_bars_ts,
            'latest_market_returns_ts', :v_latest_returns_ts,
            'market_bars_at_latest_ts', :v_market_bars_at_latest_ts,
            'returns_at_latest_ts', :v_returns_at_latest_ts,
            'started_at', :v_step_start,
            'completed_at', :v_step_end
        );
    exception
        when other then
            v_step_end := current_timestamp();
            call MIP.APP.SP_AUDIT_LOG_STEP(
                :P_PARENT_RUN_ID,
                'RETURNS_REFRESH',
                'FAIL',
                null,
                object_construct(
                    'step_name', 'returns_refresh',
                    'scope', 'AGG',
                    'scope_key', null,
                    'started_at', :v_step_start,
                    'completed_at', :v_step_end
                ),
                :sqlerrm
            );
            raise;
    end;
end;
$$;
