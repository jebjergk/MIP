-- 144_sp_pipeline_generate_recommendations.sql
-- Purpose: Pipeline step to generate recommendations for a market type + interval

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_PIPELINE_GENERATE_RECOMMENDATIONS(
    P_MARKET_TYPE string,
    P_INTERVAL_MINUTES number,
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
    v_rows_before number := 0;
    v_rows_after number := 0;
    v_inserted_count number := 0;
    v_latest_market_bars_ts timestamp_ntz;
    v_latest_returns_ts timestamp_ntz;
    v_existing_recs_at_latest_ts number := 0;
    v_pattern_count number := 0;
    v_min_return number := 0.0;
    v_skip_reason string;
begin
    select max(TS)
      into :v_latest_market_bars_ts
      from MIP.MART.MARKET_BARS
     where MARKET_TYPE = :P_MARKET_TYPE
       and INTERVAL_MINUTES = :P_INTERVAL_MINUTES;

    select max(TS)
      into :v_latest_returns_ts
      from MIP.MART.MARKET_RETURNS
     where MARKET_TYPE = :P_MARKET_TYPE
       and INTERVAL_MINUTES = :P_INTERVAL_MINUTES;

    if (v_latest_market_bars_ts is not null) then
        select count(*)
          into :v_existing_recs_at_latest_ts
          from MIP.APP.RECOMMENDATION_LOG
         where MARKET_TYPE = :P_MARKET_TYPE
           and INTERVAL_MINUTES = :P_INTERVAL_MINUTES
           and TS = :v_latest_market_bars_ts;
    end if;

    select count(*)
      into :v_pattern_count
      from MIP.APP.PATTERN_DEFINITION
     where coalesce(IS_ACTIVE, 'N') = 'Y'
       and coalesce(ENABLED, true)
       and upper(coalesce(PARAMS_JSON:market_type::string, 'STOCK')) = upper(:P_MARKET_TYPE)
       and coalesce(PARAMS_JSON:interval_minutes::number, 1440) = :P_INTERVAL_MINUTES;

    select count(*)
      into :v_rows_before
      from MIP.APP.RECOMMENDATION_LOG
     where MARKET_TYPE = :P_MARKET_TYPE
       and INTERVAL_MINUTES = :P_INTERVAL_MINUTES;

    begin
        call MIP.APP.SP_GENERATE_MOMENTUM_RECS(
            :v_min_return,
            :P_MARKET_TYPE,
            :P_INTERVAL_MINUTES,
            null,
            null
        );

        select count(*)
          into :v_rows_after
          from MIP.APP.RECOMMENDATION_LOG
         where MARKET_TYPE = :P_MARKET_TYPE
           and INTERVAL_MINUTES = :P_INTERVAL_MINUTES;

        v_inserted_count := v_rows_after - v_rows_before;
        v_step_end := current_timestamp();

        if (v_inserted_count = 0) then
            if (v_latest_market_bars_ts is null) then
                v_skip_reason := 'NO_MARKET_BARS';
            elseif (v_latest_returns_ts is null) then
                v_skip_reason := 'NO_RETURNS_AVAILABLE';
            elseif (v_existing_recs_at_latest_ts > 0) then
                v_skip_reason := 'ALREADY_GENERATED_FOR_TS';
            elseif (v_pattern_count = 0) then
                v_skip_reason := 'NO_ACTIVE_PATTERNS';
            else
                v_skip_reason := 'FILTERED_BY_THRESHOLD';
            end if;
        end if;

        insert into MIP.APP.MIP_AUDIT_LOG (
            EVENT_TS,
            RUN_ID,
            PARENT_RUN_ID,
            EVENT_TYPE,
            EVENT_NAME,
            STATUS,
            ROWS_AFFECTED,
            DETAILS,
            ERROR_MESSAGE
        )
        select
            current_timestamp(),
            :v_run_id,
            :P_PARENT_RUN_ID,
            'PIPELINE_STEP',
            'RECOMMENDATIONS',
            'SUCCESS',
            :v_inserted_count,
            object_construct(
                'step_name', 'recommendations',
                'market_type', :P_MARKET_TYPE,
                'interval_minutes', :P_INTERVAL_MINUTES,
                'started_at', :v_step_start,
                'completed_at', :v_step_end,
                'rows_before', :v_rows_before,
                'rows_after', :v_rows_after,
                'min_return', :v_min_return,
                'lookback_days', null,
                'min_zscore', null,
                'latest_market_bars_ts', :v_latest_market_bars_ts,
                'latest_returns_ts', :v_latest_returns_ts,
                'existing_recs_at_latest_ts', :v_existing_recs_at_latest_ts,
                'inserted_count', :v_inserted_count,
                'pattern_count', :v_pattern_count,
                'skip_reason', :v_skip_reason
            ),
            null;

        return object_construct(
            'status', 'SUCCESS',
            'market_type', :P_MARKET_TYPE,
            'interval_minutes', :P_INTERVAL_MINUTES,
            'rows_before', :v_rows_before,
            'rows_after', :v_rows_after,
            'inserted_count', :v_inserted_count,
            'latest_market_bars_ts', :v_latest_market_bars_ts,
            'latest_returns_ts', :v_latest_returns_ts,
            'existing_recs_at_latest_ts', :v_existing_recs_at_latest_ts,
            'pattern_count', :v_pattern_count,
            'skip_reason', :v_skip_reason,
            'started_at', :v_step_start,
            'completed_at', :v_step_end
        );
    exception
        when other then
            v_step_end := current_timestamp();
            insert into MIP.APP.MIP_AUDIT_LOG (
                EVENT_TS,
                RUN_ID,
                PARENT_RUN_ID,
                EVENT_TYPE,
                EVENT_NAME,
                STATUS,
                ROWS_AFFECTED,
                DETAILS,
                ERROR_MESSAGE
            )
            select
                current_timestamp(),
                :v_run_id,
                :P_PARENT_RUN_ID,
                'PIPELINE_STEP',
                'RECOMMENDATIONS',
                'FAIL',
                null,
                object_construct(
                    'step_name', 'recommendations',
                    'market_type', :P_MARKET_TYPE,
                    'interval_minutes', :P_INTERVAL_MINUTES,
                    'started_at', :v_step_start,
                    'completed_at', :v_step_end,
                    'min_return', :v_min_return
                ),
                :sqlerrm;
            raise;
    end;
end;
$$;
