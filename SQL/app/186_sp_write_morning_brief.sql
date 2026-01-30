-- 186_sp_write_morning_brief.sql
-- Purpose: Persist morning brief snapshot into AGENT_OUT. Deterministic + idempotent on (P_PORTFOLIO_ID, P_AS_OF_TS, P_RUN_ID, P_AGENT_NAME).
-- MERGE key uses only pipeline-passed params; view is content-only.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_WRITE_MORNING_BRIEF(
    P_PORTFOLIO_ID number,
    P_AS_OF_TS timestamp_ntz,
    P_RUN_ID varchar,
    P_AGENT_NAME varchar default 'MORNING_BRIEF'
)
returns variant
language sql
execute as caller
as
$$
declare
    v_brief variant;
    v_attr variant;
    v_brief_proposed_count number;
    v_actual_proposed_count number;
    v_brief_executed_count number;
    v_actual_executed_proposals number;
    v_actual_executed_trades number;
    v_brief_risk_status string;
    v_gate_risk_status string;
    v_validation_warnings array := array_construct();
    v_agent_name varchar := coalesce(nullif(trim(:P_AGENT_NAME), ''), 'MORNING_BRIEF');
begin
    -- Fetch BRIEF from view (content only); procedure overwrites attribution keys.
    select BRIEF
      into :v_brief
      from MIP.MART.V_MORNING_BRIEF_JSON
     where PORTFOLIO_ID = :P_PORTFOLIO_ID
     limit 1;
    if (v_brief is null) then
        call MIP.APP.SP_LOG_EVENT(
            'VALIDATION',
            'SP_WRITE_MORNING_BRIEF',
            'FAIL',
            null,
            object_construct('portfolio_id', :P_PORTFOLIO_ID, 'reason', 'NO_BRIEF_CONTENT'),
            'No brief content for portfolio',
            :P_RUN_ID,
            null
        );
        raise exc_no_brief;
    end if;

    -- Overwrite attribution + top-level pipeline_run_id so MERGE key uses pipeline params only.
    v_attr := coalesce(v_brief:attribution, object_construct());
    v_attr := object_insert(v_attr, 'pipeline_run_id', :P_RUN_ID);
    v_attr := object_insert(v_attr, 'as_of_ts', to_varchar(:P_AS_OF_TS));
    v_brief := object_insert(v_brief, 'attribution', v_attr);
    v_brief := object_insert(v_brief, 'pipeline_run_id', :P_RUN_ID);

    -- MED-003: Morning brief consistency validation (optional; use extracted counts from v_brief)
    begin
        v_brief_proposed_count := v_brief:proposals:proposed_count::number;
    exception
        when other then v_brief_proposed_count := null;
    end;
    begin
        v_brief_executed_count := v_brief:executions:total_count::number;
    exception
        when other then v_brief_executed_count := null;
    end;
    begin
        v_brief_risk_status := v_brief:risk:status::string;
    exception
        when other then v_brief_risk_status := null;
    end;

    select
        count(*) as proposed_count,
        count_if(STATUS = 'EXECUTED') as executed_proposals_count
      into :v_actual_proposed_count,
           :v_actual_executed_proposals
      from MIP.AGENT_OUT.ORDER_PROPOSALS
     where PORTFOLIO_ID = :P_PORTFOLIO_ID
       and RUN_ID_VARCHAR = :P_RUN_ID
       and PROPOSED_AT >= :P_AS_OF_TS - interval '1 day'
       and PROPOSED_AT <= :P_AS_OF_TS + interval '1 hour';

    select count(*)
      into :v_actual_executed_trades
      from MIP.APP.PORTFOLIO_TRADES
     where PORTFOLIO_ID = :P_PORTFOLIO_ID
       and TRADE_TS >= :P_AS_OF_TS - interval '1 day'
       and TRADE_TS <= :P_AS_OF_TS + interval '1 hour';

    v_actual_executed_proposals := coalesce(:v_actual_executed_trades, :v_actual_executed_proposals);

    begin
        select RISK_STATUS into :v_gate_risk_status
          from MIP.MART.V_PORTFOLIO_RISK_GATE
         where PORTFOLIO_ID = :P_PORTFOLIO_ID
         limit 1;
    exception
        when other then v_gate_risk_status := null;
    end;

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

    if (array_size(:v_validation_warnings) > 0) then
        call MIP.APP.SP_LOG_EVENT(
            'VALIDATION',
            'SP_WRITE_MORNING_BRIEF',
            'WARN',
            null,
            object_construct(
                'portfolio_id', :P_PORTFOLIO_ID,
                'pipeline_run_id', :P_RUN_ID,
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
            :P_RUN_ID,
            null
        );
    end if;

    merge into MIP.AGENT_OUT.MORNING_BRIEF as target
    using (
        select
            :P_PORTFOLIO_ID::number as portfolio_id,
            :P_AS_OF_TS::timestamp_ntz as as_of_ts,
            :P_RUN_ID::varchar as run_id,
            :v_agent_name::varchar(128) as agent_name,
            :v_brief::variant as brief,
            :P_RUN_ID::varchar as pipeline_run_id
    ) as source
    on target.PORTFOLIO_ID = source.portfolio_id
   and target.AS_OF_TS = source.as_of_ts
   and target.RUN_ID = source.run_id
   and coalesce(target.AGENT_NAME, '') = coalesce(source.agent_name, '')
    when matched then update set
        target.BRIEF = source.brief,
        target.PIPELINE_RUN_ID = source.pipeline_run_id,
        target.UPDATED_AT = current_timestamp()
    when not matched then insert (
        PORTFOLIO_ID,
        RUN_ID,
        AS_OF_TS,
        BRIEF,
        PIPELINE_RUN_ID,
        AGENT_NAME
    ) values (
        source.portfolio_id,
        source.run_id,
        source.as_of_ts,
        source.brief,
        source.pipeline_run_id,
        source.agent_name
    );

    return object_construct(
        'portfolio_id', :P_PORTFOLIO_ID,
        'as_of_ts', :P_AS_OF_TS,
        'run_id', :P_RUN_ID,
        'agent_name', :v_agent_name,
        'validation_warnings', :v_validation_warnings,
        'validation_status', case
            when array_size(:v_validation_warnings) > 0 then 'WARN'
            else 'OK'
        end
    );
end;
$$;
