-- 186_sp_write_morning_brief.sql
-- Purpose: Persist morning brief snapshot into AGENT_OUT. Deterministic + idempotent on (P_PORTFOLIO_ID, P_AS_OF_TS, P_RUN_ID, P_AGENT_NAME).
-- MERGE key uses only pipeline-passed params; view is content-only.
--
-- Attribution overwrite contract (final merged BRIEF must satisfy):
--   - BRIEF:"as_of_ts" is populated at root from P_AS_OF_TS.
--   - BRIEF:"attribution":"as_of_ts" is removed / not present.
--   - attribution contains only pipeline fields (pipeline_run_id).

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
    -- Copy parameters to local variables to avoid binding issues with exception handlers
    v_portfolio_id number := :P_PORTFOLIO_ID;
    v_as_of_ts timestamp_ntz := :P_AS_OF_TS;
    v_run_id varchar := :P_RUN_ID;
    exc_no_brief exception;
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
    -- Guard: do not write for invalid portfolio (e.g. test portfolio_id=0).
    if (:v_portfolio_id is null or :v_portfolio_id <= 0) then
        call MIP.APP.SP_LOG_EVENT(
            'VALIDATION',
            'SP_WRITE_MORNING_BRIEF',
            'SKIP_INVALID_PORTFOLIO',
            null,
            object_construct('portfolio_id', :v_portfolio_id, 'reason', 'PORTFOLIO_ID_LE_ZERO'),
            'Portfolio ID must be positive',
            :v_run_id,
            null
        );
        return object_construct('status', 'SKIP_INVALID_PORTFOLIO', 'portfolio_id', :v_portfolio_id);
    end if;

    -- Fetch BRIEF from view (content only); procedure overwrites attribution keys.
    -- If no content, write an empty brief with explanation (never skip brief write).
    begin
        select BRIEF
          into :v_brief
          from MIP.MART.V_MORNING_BRIEF_JSON
         where PORTFOLIO_ID = :v_portfolio_id
         limit 1;
    exception
        when other then
            v_brief := null;
    end;
    
    if (v_brief is null) then
        -- Build empty brief structure with explanation
        v_brief := object_construct(
            'portfolio_id', :v_portfolio_id,
            'as_of_ts', to_varchar(:v_as_of_ts),
            'pipeline_run_id', :v_run_id,
            'status', 'EMPTY',
            'summary', object_construct(
                'headline', 'No brief content available',
                'explanation', 'No opportunities or data available for this run. This may be due to: no new market bars, no eligible signals, or entry gate blocked.'
            ),
            'opportunities', array_construct(),
            'signals', object_construct(
                'trusted_now', array_construct(),
                'watch_negative', array_construct()
            ),
            'proposals', object_construct(
                'summary', object_construct('total', 0, 'proposed', 0, 'approved', 0, 'rejected', 0, 'executed', 0),
                'proposed_trades', array_construct(),
                'executed_trades', array_construct()
            ),
            'risk', object_construct(),
            'portfolio', object_construct(),
            'attribution', object_construct('pipeline_run_id', :v_run_id)
        );
        call MIP.APP.SP_LOG_EVENT(
            'VALIDATION',
            'SP_WRITE_MORNING_BRIEF',
            'INFO',
            null,
            object_construct('portfolio_id', :v_portfolio_id, 'reason', 'EMPTY_BRIEF_WRITTEN', 'run_id', :v_run_id),
            'Writing empty brief (no content available)',
            :v_run_id,
            null
        );
    end if;

    -- Attribution overwrite: attribution = pipeline fields only (pipeline_run_id); as_of_ts at BRIEF root only.
    v_attr := coalesce(v_brief:attribution, object_construct());
    v_attr := object_delete(v_attr, 'latest_run_id');
    v_attr := object_delete(v_attr, 'pipeline_run_id');
    v_attr := object_delete(v_attr, 'as_of_ts');   -- ensure attribution never contains as_of_ts
    v_attr := object_insert(v_attr, 'pipeline_run_id', :v_run_id);
    v_brief := object_delete(v_brief, 'attribution');
    v_brief := object_insert(v_brief, 'attribution', v_attr);
    v_brief := object_delete(v_brief, 'pipeline_run_id');
    v_brief := object_insert(v_brief, 'pipeline_run_id', :v_run_id);
    v_brief := object_delete(v_brief, 'as_of_ts');
    v_brief := object_insert(v_brief, 'as_of_ts', to_varchar(:v_as_of_ts));  -- canonical at root

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
     where PORTFOLIO_ID = :v_portfolio_id
       and RUN_ID_VARCHAR = :v_run_id
       and PROPOSED_AT >= :v_as_of_ts - interval '1 day'
       and PROPOSED_AT <= :v_as_of_ts + interval '1 hour';

    select count(*)
      into :v_actual_executed_trades
      from MIP.APP.PORTFOLIO_TRADES
     where PORTFOLIO_ID = :v_portfolio_id
       and TRADE_TS >= :v_as_of_ts - interval '1 day'
       and TRADE_TS <= :v_as_of_ts + interval '1 hour';

    v_actual_executed_proposals := coalesce(:v_actual_executed_trades, :v_actual_executed_proposals);

    begin
        select RISK_STATUS into :v_gate_risk_status
          from MIP.MART.V_PORTFOLIO_RISK_GATE
         where PORTFOLIO_ID = :v_portfolio_id
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
                'portfolio_id', :v_portfolio_id,
                'pipeline_run_id', :v_run_id,
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
            :v_run_id,
            null
        );
    end if;

    merge into MIP.AGENT_OUT.MORNING_BRIEF as target
    using (
        select
            :v_portfolio_id::number as portfolio_id,
            :v_as_of_ts::timestamp_ntz as as_of_ts,
            :v_run_id::varchar as run_id,
            :v_agent_name::varchar(128) as agent_name,
            :v_brief::variant as brief,
            :v_run_id::varchar as pipeline_run_id,
            current_timestamp() as created_at
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
        AGENT_NAME,
        CREATED_AT
    ) values (
        source.portfolio_id,
        source.run_id,
        source.as_of_ts,
        source.brief,
        source.pipeline_run_id,
        source.agent_name,
        source.created_at
    );

    return object_construct(
        'portfolio_id', :v_portfolio_id,
        'as_of_ts', :v_as_of_ts,
        'run_id', :v_run_id,
        'agent_name', :v_agent_name,
        'validation_warnings', :v_validation_warnings,
        'validation_status', case
            when array_size(:v_validation_warnings) > 0 then 'WARN'
            else 'OK'
        end
    );
end;
$$;
