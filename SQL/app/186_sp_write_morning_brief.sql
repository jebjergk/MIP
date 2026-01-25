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
declare
    v_brief_proposed_count number;
    v_actual_proposed_count number;
    v_brief_executed_count number;
    v_actual_executed_proposals number;
    v_actual_executed_trades number;
    v_brief_risk_status string;
    v_gate_risk_status string;
    v_validation_warnings array := array_construct();
begin
    -- MED-003: Morning brief consistency validation
    -- Get counts from brief JSON (if available)
    select
        BRIEF:proposals:proposed_count::number,
        BRIEF:executions:total_count::number,
        BRIEF:risk:status::string
      into :v_brief_proposed_count,
           :v_brief_executed_count,
           :v_brief_risk_status
      from MIP.MART.V_MORNING_BRIEF_JSON
     limit 1;

    -- Get actual counts from tables
    -- Match proposals by portfolio and recent creation time (within last hour)
    -- since proposals don't have direct pipeline_run_id link
    select
        count(*) as proposed_count,
        count_if(STATUS = 'EXECUTED') as executed_proposals_count
      into :v_actual_proposed_count,
           :v_actual_executed_proposals
      from MIP.AGENT_OUT.ORDER_PROPOSALS
     where PORTFOLIO_ID = :P_PORTFOLIO_ID
       and PROPOSED_AT >= current_timestamp() - interval '1 hour';

    -- Also get actual executed trades count from PORTFOLIO_TRADES
    select count(*)
      into :v_actual_executed_trades
      from MIP.APP.PORTFOLIO_TRADES
     where PORTFOLIO_ID = :P_PORTFOLIO_ID
       and TRADE_TS >= current_timestamp() - interval '1 hour';

    -- Use trades count as the executed count (more accurate)
    v_actual_executed_proposals := coalesce(:v_actual_executed_trades, :v_actual_executed_proposals);

    -- Get risk status from gate view
    select RISK_STATUS
      into :v_gate_risk_status
      from MIP.MART.V_PORTFOLIO_RISK_GATE
     where PORTFOLIO_ID = :P_PORTFOLIO_ID;

    -- Validate and log warnings
    if (v_brief_proposed_count is not null and v_brief_proposed_count != v_actual_proposed_count) then
        v_validation_warnings := array_append(
            :v_validation_warnings,
            'PROPOSED_COUNT_MISMATCH: brief=' || :v_brief_proposed_count || ', actual=' || :v_actual_proposed_count
        );
    end if;

    if (v_brief_executed_count is not null and v_brief_executed_count != v_actual_executed_proposals) then
        v_validation_warnings := array_append(
            :v_validation_warnings,
            'EXECUTED_COUNT_MISMATCH: brief=' || :v_brief_executed_count || ', actual=' || :v_actual_executed_proposals
        );
    end if;

    if (v_brief_risk_status is not null and v_gate_risk_status is not null and v_brief_risk_status != v_gate_risk_status) then
        v_validation_warnings := array_append(
            :v_validation_warnings,
            'RISK_STATUS_MISMATCH: brief=' || :v_brief_risk_status || ', gate=' || :v_gate_risk_status
        );
    end if;

    -- Log warnings if any inconsistencies found
    if (array_size(:v_validation_warnings) > 0) then
        call MIP.APP.SP_LOG_EVENT(
            'VALIDATION',
            'SP_WRITE_MORNING_BRIEF',
            'WARN',
            null,
            object_construct(
                'portfolio_id', :P_PORTFOLIO_ID,
                'pipeline_run_id', :P_PIPELINE_RUN_ID,
                'warnings', :v_validation_warnings,
                'brief_proposed', :v_brief_proposed_count,
                'actual_proposed', :v_actual_proposed_count,
                'brief_executed', :v_brief_executed_count,
                'actual_executed_proposals', :v_actual_executed_proposals,
                'actual_executed_trades', :v_actual_executed_trades,
                'brief_risk_status', :v_brief_risk_status,
                'gate_risk_status', :v_gate_risk_status
            ),
            null,
            :P_PIPELINE_RUN_ID,
            null
        );
    end if;

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
        'pipeline_run_id', :P_PIPELINE_RUN_ID,
        'validation_warnings', :v_validation_warnings,
        'validation_status', case
            when array_size(:v_validation_warnings) > 0 then 'WARN'
            else 'OK'
        end
    );
end;
$$;
