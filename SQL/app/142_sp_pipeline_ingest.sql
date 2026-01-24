-- 142_sp_pipeline_ingest.sql
-- Purpose: Pipeline step wrapper for ingestion

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_PIPELINE_INGEST()
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
    v_rows_delta number := 0;
    v_ingest_result variant;
    v_ingest_status string;
    v_rate_limit_hit boolean := false;
    v_audit_status string;
begin
    select count(*)
      into :v_rows_before
      from MIP.MART.MARKET_BARS;

    begin
        v_ingest_result := (call MIP.APP.SP_INGEST_ALPHAVANTAGE_BARS());
        v_ingest_status := coalesce(:v_ingest_result:"status"::string, 'UNKNOWN');
        v_rate_limit_hit := coalesce(:v_ingest_result:"rate_limit_hit"::boolean, false);

        select count(*)
          into :v_rows_after
          from MIP.MART.MARKET_BARS;

        v_rows_delta := v_rows_after - v_rows_before;
        v_step_end := current_timestamp();

        v_audit_status := case
            when :v_rate_limit_hit then 'SKIP_RATE_LIMIT'
            when :v_ingest_status = 'SUCCESS_WITH_SKIPS' then 'SUCCESS_WITH_SKIPS'
            else 'SUCCESS'
        end;

        insert into MIP.APP.MIP_AUDIT_LOG (
            EVENT_TS,
            RUN_ID,
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
            'PIPELINE_STEP',
            'INGESTION',
            :v_audit_status,
            :v_rows_delta,
            object_construct(
                'step_name', 'ingestion',
                'started_at', :v_step_start,
                'completed_at', :v_step_end,
                'rows_before', :v_rows_before,
                'rows_after', :v_rows_after,
                'rows_delta', :v_rows_delta,
                'ingest_result', :v_ingest_result
            ),
            null;

        return object_construct(
            'status', :v_audit_status,
            'rows_before', :v_rows_before,
            'rows_after', :v_rows_after,
            'rows_delta', :v_rows_delta,
            'ingest_result', :v_ingest_result,
            'started_at', :v_step_start,
            'completed_at', :v_step_end
        );
    exception
        when other then
            v_step_end := current_timestamp();
            insert into MIP.APP.MIP_AUDIT_LOG (
                EVENT_TS,
                RUN_ID,
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
                'PIPELINE_STEP',
                'INGESTION',
                'FAIL',
                null,
                object_construct(
                    'step_name', 'ingestion',
                    'started_at', :v_step_start,
                    'completed_at', :v_step_end
                ),
                :sqlerrm;
            raise;
    end;
end;
$$;
