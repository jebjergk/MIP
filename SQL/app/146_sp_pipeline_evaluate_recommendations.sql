-- 146_sp_pipeline_evaluate_recommendations.sql
-- Purpose: Pipeline step wrapper for recommendation evaluation

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_PIPELINE_EVALUATE_RECOMMENDATIONS(
    P_FROM_TS timestamp_ntz,
    P_TO_TS timestamp_ntz,
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
    v_rows_delta number := 0;
begin
    select count(*)
      into :v_rows_before
      from MIP.APP.RECOMMENDATION_OUTCOMES;

    begin
        call MIP.APP.SP_EVALUATE_RECOMMENDATIONS(:P_FROM_TS, :P_TO_TS);

        select count(*)
          into :v_rows_after
          from MIP.APP.RECOMMENDATION_OUTCOMES;

        v_rows_delta := v_rows_after - v_rows_before;
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
            'EVALUATION',
            'SUCCESS',
            :v_rows_delta,
            object_construct(
                'step_name', 'evaluation',
                'started_at', :v_step_start,
                'completed_at', :v_step_end,
                'rows_before', :v_rows_before,
                'rows_after', :v_rows_after,
                'from_ts', :P_FROM_TS,
                'to_ts', :P_TO_TS
            ),
            null;

        return object_construct(
            'status', 'SUCCESS',
            'rows_before', :v_rows_before,
            'rows_after', :v_rows_after,
            'rows_delta', :v_rows_delta,
            'from_ts', :P_FROM_TS,
            'to_ts', :P_TO_TS,
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
                'EVALUATION',
                'FAIL',
                null,
                object_construct(
                    'step_name', 'evaluation',
                    'started_at', :v_step_start,
                    'completed_at', :v_step_end,
                    'from_ts', :P_FROM_TS,
                    'to_ts', :P_TO_TS
                ),
                :sqlerrm;
            raise;
    end;
end;
$$;
