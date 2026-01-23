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
begin
    select count(*)
      into :v_rows_before
      from MIP.MART.MARKET_BARS;

    begin
        call MIP.APP.SP_INGEST_ALPHAVANTAGE_BARS();

        select count(*)
          into :v_rows_after
          from MIP.MART.MARKET_BARS;

        v_rows_delta := v_rows_after - v_rows_before;
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
            'SUCCESS',
            :v_rows_delta,
            object_construct(
                'step_name', 'ingestion',
                'started_at', :v_step_start,
                'completed_at', :v_step_end,
                'rows_before', :v_rows_before,
                'rows_after', :v_rows_after
            ),
            null;

        return object_construct(
            'status', 'SUCCESS',
            'rows_before', :v_rows_before,
            'rows_after', :v_rows_after,
            'rows_delta', :v_rows_delta,
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
