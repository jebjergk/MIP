-- 186_sp_write_morning_brief.sql
-- Purpose: Persist latest morning brief snapshot into AGENT_OUT

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_WRITE_MORNING_BRIEF(
    P_PORTFOLIO_ID number,
    P_PIPELINE_RUN_ID string
)
returns variant
language sql
execute as caller
as
$$
begin
    merge into MIP.AGENT_OUT.MORNING_BRIEF as target
    using (
        select
            :P_PORTFOLIO_ID as portfolio_id,
            BRIEF:attribution:latest_run_id::string as run_id,
            AS_OF_TS as as_of_ts,
            BRIEF as brief,
            :P_PIPELINE_RUN_ID as pipeline_run_id
        from MIP.MART.V_MORNING_BRIEF_JSON
    ) as source
    on target.PORTFOLIO_ID = source.PORTFOLIO_ID
   and target.RUN_ID = source.RUN_ID
    when matched then update set
        target.BRIEF = source.BRIEF,
        target.AS_OF_TS = source.AS_OF_TS,
        target.PIPELINE_RUN_ID = source.PIPELINE_RUN_ID
    when not matched then insert (
        PORTFOLIO_ID,
        RUN_ID,
        BRIEF,
        AS_OF_TS,
        PIPELINE_RUN_ID
    ) values (
        source.PORTFOLIO_ID,
        source.RUN_ID,
        source.BRIEF,
        source.AS_OF_TS,
        source.PIPELINE_RUN_ID
    );

    return object_construct(
        'portfolio_id', :P_PORTFOLIO_ID,
        'pipeline_run_id', :P_PIPELINE_RUN_ID
    );
end;
$$;
