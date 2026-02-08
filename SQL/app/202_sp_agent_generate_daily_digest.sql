-- 202_sp_agent_generate_daily_digest.sql
-- Purpose: Generate the Daily Intelligence Digest per portfolio.
--   Step 1: Compute deterministic snapshot from V_DAILY_DIGEST_SNAPSHOT.
--   Step 2: MERGE snapshot into DAILY_DIGEST_SNAPSHOT.
--   Step 3: Call Snowflake Cortex COMPLETE() to produce narrative from snapshot + prior snapshot.
--   Step 4: MERGE narrative into DAILY_DIGEST_NARRATIVE.
--   Step 5: Audit log event with counts and hashes.
--
-- Inputs:  :P_RUN_ID, :P_AS_OF_TS, :P_PORTFOLIO_ID (null = all active portfolios).
-- Uses:    :P_RUN_ID everywhere; does NOT use signal_run_id.
-- Cortex:  snowflake.cortex.complete('mistral-large2', prompt) for narrative.
-- Fallback: If Cortex fails, writes deterministic fallback narrative from detectors.
-- MERGE semantics: idempotent; reruns update the same keys (no duplicates).
--
-- Note: Uses RESULTSET + FOR loop pattern per SNOWFLAKE_SQL_LIMITATIONS.md.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_AGENT_GENERATE_DAILY_DIGEST(
    P_RUN_ID        varchar,
    P_AS_OF_TS      timestamp_ntz,
    P_PORTFOLIO_ID  number default null  -- null = all active portfolios
)
returns variant
language sql
execute as caller
as
$$
declare
    v_run_id            varchar := :P_RUN_ID;
    v_as_of_ts          timestamp_ntz := :P_AS_OF_TS;
    v_agent_name        varchar := 'DAILY_DIGEST';
    v_model_name        varchar := 'mistral-large2';
    v_portfolios        resultset;
    v_portfolio_id      number;
    v_snapshot          variant;
    v_prior_snapshot    variant;
    v_facts_hash        varchar;
    v_narrative_text    string;
    v_narrative_json    variant;
    v_cortex_prompt     string;
    v_cortex_succeeded  boolean := false;
    v_results           array := array_construct();
    v_portfolio_count   number := 0;
    v_snapshot_count    number := 0;
    v_narrative_count   number := 0;
    v_fired_detectors   variant;
    v_fallback_bullets  array;
begin
    -- Step 0: Determine portfolio scope
    if (:P_PORTFOLIO_ID is not null and :P_PORTFOLIO_ID > 0) then
        v_portfolios := (
            select PORTFOLIO_ID
            from MIP.APP.PORTFOLIO
            where PORTFOLIO_ID = :P_PORTFOLIO_ID
              and STATUS = 'ACTIVE'
        );
    else
        v_portfolios := (
            select PORTFOLIO_ID
            from MIP.APP.PORTFOLIO
            where STATUS = 'ACTIVE'
            order by PORTFOLIO_ID
        );
    end if;

    for rec in v_portfolios do
        v_portfolio_id := rec.PORTFOLIO_ID;
        v_portfolio_count := :v_portfolio_count + 1;
        v_cortex_succeeded := false;

        -- Step 1: Compute deterministic snapshot from view
        begin
            v_snapshot := (
                select SNAPSHOT_JSON
                from MIP.MART.V_DAILY_DIGEST_SNAPSHOT
                where PORTFOLIO_ID = :v_portfolio_id
                limit 1
            );
        exception
            when other then
                v_snapshot := object_construct(
                    'error', 'SNAPSHOT_VIEW_FAILED',
                    'message', :sqlerrm,
                    'portfolio_id', :v_portfolio_id
                );
        end;

        v_facts_hash := (select sha2(to_varchar(:v_snapshot), 256));

        -- Step 2: MERGE snapshot (idempotent)
        merge into MIP.AGENT_OUT.DAILY_DIGEST_SNAPSHOT as target
        using (
            select
                :v_portfolio_id::number       as portfolio_id,
                :v_as_of_ts::timestamp_ntz    as as_of_ts,
                :v_run_id::varchar            as run_id,
                :v_snapshot::variant           as snapshot_json,
                :v_facts_hash::varchar         as source_facts_hash
        ) as source
        on  target.PORTFOLIO_ID = source.portfolio_id
        and target.AS_OF_TS     = source.as_of_ts
        and target.RUN_ID       = source.run_id
        when matched then update set
            target.SNAPSHOT_JSON     = source.snapshot_json,
            target.SOURCE_FACTS_HASH = source.source_facts_hash
        when not matched then insert (
            PORTFOLIO_ID, AS_OF_TS, RUN_ID, SNAPSHOT_JSON, SOURCE_FACTS_HASH, CREATED_AT
        ) values (
            source.portfolio_id, source.as_of_ts, source.run_id,
            source.snapshot_json, source.source_facts_hash, current_timestamp()
        );

        v_snapshot_count := :v_snapshot_count + 1;

        -- Step 3: Get prior snapshot for narrative context
        begin
            v_prior_snapshot := (
                select SNAPSHOT_JSON
                from MIP.AGENT_OUT.DAILY_DIGEST_SNAPSHOT
                where PORTFOLIO_ID = :v_portfolio_id
                  and (AS_OF_TS < :v_as_of_ts or (AS_OF_TS = :v_as_of_ts and RUN_ID != :v_run_id))
                order by AS_OF_TS desc
                limit 1
            );
        exception
            when other then
                v_prior_snapshot := null;
        end;

        -- Step 4: Build Cortex prompt and call COMPLETE()
        -- Extract fired detectors for priority ordering
        v_fired_detectors := (
            select array_agg(value)
            from table(flatten(input => :v_snapshot:detectors))
            where value:fired::boolean = true
        );
        v_fired_detectors := coalesce(:v_fired_detectors, array_construct());

        v_cortex_prompt :=
'You are a portfolio intelligence analyst for MIP (Market Intelligence Platform). ' ||
'You write concise, fact-grounded daily digests. You MUST only reference numbers and facts present in the snapshot data below. ' ||
'Do NOT invent facts, propose trades, or suggest parameter changes. ' ||
'
CURRENT SNAPSHOT (portfolio ' || :v_portfolio_id::string || '):
' || to_varchar(:v_snapshot) || '

PRIOR SNAPSHOT:
' || coalesce(to_varchar(:v_prior_snapshot), 'No prior snapshot available (first run).') || '

FIRED INTEREST DETECTORS (prioritise these):
' || to_varchar(:v_fired_detectors) || '

Produce a JSON object with exactly these keys:
{
  "headline": "One sentence summary of what matters most today",
  "what_changed": ["bullet 1", "bullet 2", ...],
  "what_matters": ["bullet 1", "bullet 2", ...],
  "waiting_for": ["bullet 1", "bullet 2", ...],
  "where_to_look": [{"label": "page name", "route": "/path"}, ...]
}

Rules:
- headline: 1 sentence, reference concrete numbers from snapshot.
- what_changed: 3-5 bullets about changes since prior snapshot. If nothing changed, say so and explain why.
- what_matters: 2-4 bullets about the most important current state facts.
- waiting_for: 2-3 bullets about upcoming catalysts or thresholds approaching.
- where_to_look: 2-4 links. Valid routes: /signals, /training, /portfolios/' || :v_portfolio_id::string || ', /brief, /market-timeline, /suggestions
- Every number you mention MUST appear in the snapshot data.
- Return ONLY the JSON object, no markdown fences, no explanation.';

        begin
            v_narrative_text := (
                select snowflake.cortex.complete(:v_model_name, :v_cortex_prompt)
            );
            v_cortex_succeeded := true;
        exception
            when other then
                v_cortex_succeeded := false;
                -- Log Cortex failure
                call MIP.APP.SP_LOG_EVENT(
                    'AGENT',
                    'SP_AGENT_GENERATE_DAILY_DIGEST',
                    'WARN_CORTEX_FAILED',
                    null,
                    object_construct(
                        'portfolio_id', :v_portfolio_id,
                        'run_id', :v_run_id,
                        'error', :sqlerrm
                    ),
                    :sqlerrm,
                    :v_run_id,
                    null
                );
        end;

        -- Step 5: Parse narrative or build deterministic fallback
        if (:v_cortex_succeeded and :v_narrative_text is not null) then
            begin
                v_narrative_json := parse_json(:v_narrative_text);
            exception
                when other then
                    -- Cortex returned non-JSON; wrap as plain text
                    v_narrative_json := object_construct(
                        'headline', 'Daily digest generated but could not parse structured response.',
                        'what_changed', array_construct(:v_narrative_text),
                        'what_matters', array_construct(),
                        'waiting_for', array_construct(),
                        'where_to_look', array_construct()
                    );
            end;
        else
            -- Deterministic fallback: build from snapshot data directly
            v_fallback_bullets := array_construct();

            -- Gate status
            v_fallback_bullets := array_append(:v_fallback_bullets,
                'Gate status: ' || coalesce(:v_snapshot:gate:risk_status::string, 'OK') ||
                iff(:v_snapshot:gate:entries_blocked::boolean, ' (entries BLOCKED)', ' (entries allowed)')
            );

            -- Capacity
            v_fallback_bullets := array_append(:v_fallback_bullets,
                'Portfolio capacity: ' || coalesce(:v_snapshot:capacity:open_positions::string, '0') ||
                '/' || coalesce(:v_snapshot:capacity:max_positions::string, '?') ||
                ' positions (' || coalesce(:v_snapshot:capacity:remaining_capacity::string, '?') || ' remaining)'
            );

            -- Signals
            v_fallback_bullets := array_append(:v_fallback_bullets,
                'Signals today: ' || coalesce(:v_snapshot:signals:total_signals::string, '0') ||
                ' total, ' || coalesce(:v_snapshot:signals:total_eligible::string, '0') || ' eligible'
            );

            -- Proposals
            v_fallback_bullets := array_append(:v_fallback_bullets,
                'Proposals: ' || coalesce(:v_snapshot:proposals:proposed_count::string, '0') ||
                ' proposed, ' || coalesce(:v_snapshot:proposals:executed_count::string, '0') ||
                ' executed, ' || coalesce(:v_snapshot:proposals:rejected_count::string, '0') || ' rejected'
            );

            -- Training
            v_fallback_bullets := array_append(:v_fallback_bullets,
                'Training: ' || coalesce(:v_snapshot:training:trusted_count::string, '0') ||
                ' trusted / ' || coalesce(:v_snapshot:training:watch_count::string, '0') ||
                ' watch / ' || coalesce(:v_snapshot:training:untrusted_count::string, '0') || ' untrusted'
            );

            v_narrative_text := 'No AI narrative available; showing deterministic summary.';
            v_narrative_json := object_construct(
                'headline', 'Daily digest for portfolio ' || :v_portfolio_id::string ||
                    ' — ' || coalesce(:v_snapshot:gate:risk_status::string, 'OK') ||
                    ', ' || coalesce(:v_snapshot:capacity:remaining_capacity::string, '?') || ' slots remaining',
                'what_changed', :v_fallback_bullets,
                'what_matters', array_construct(
                    'Return: ' || coalesce(round(:v_snapshot:kpis:total_return::float * 100, 2)::string, '?') || '%',
                    'Drawdown: ' || coalesce(round(:v_snapshot:kpis:max_drawdown::float * 100, 2)::string, '?') || '%'
                ),
                'waiting_for', array_construct(
                    'Next pipeline run for fresh signals',
                    coalesce(:v_snapshot:training:watch_count::string, '0') || ' patterns in WATCH status approaching trust threshold'
                ),
                'where_to_look', array_construct(
                    object_construct('label', 'Signals Explorer', 'route', '/signals'),
                    object_construct('label', 'Training Status', 'route', '/training'),
                    object_construct('label', 'Portfolio', 'route', '/portfolios/' || :v_portfolio_id::string)
                )
            );
            v_model_name := 'DETERMINISTIC_FALLBACK';
        end if;

        -- Step 6: MERGE narrative (idempotent)
        merge into MIP.AGENT_OUT.DAILY_DIGEST_NARRATIVE as target
        using (
            select
                :v_portfolio_id::number          as portfolio_id,
                :v_as_of_ts::timestamp_ntz       as as_of_ts,
                :v_run_id::varchar               as run_id,
                :v_agent_name::varchar           as agent_name,
                :v_narrative_text::string         as narrative_text,
                :v_narrative_json::variant        as narrative_json,
                :v_model_name::varchar            as model_info,
                :v_facts_hash::varchar            as source_facts_hash
        ) as source
        on  target.PORTFOLIO_ID = source.portfolio_id
        and target.AS_OF_TS     = source.as_of_ts
        and target.RUN_ID       = source.run_id
        and target.AGENT_NAME   = source.agent_name
        when matched then update set
            target.NARRATIVE_TEXT    = source.narrative_text,
            target.NARRATIVE_JSON   = source.narrative_json,
            target.MODEL_INFO       = source.model_info,
            target.SOURCE_FACTS_HASH = source.source_facts_hash
        when not matched then insert (
            PORTFOLIO_ID, AS_OF_TS, RUN_ID, AGENT_NAME,
            NARRATIVE_TEXT, NARRATIVE_JSON, MODEL_INFO, SOURCE_FACTS_HASH, CREATED_AT
        ) values (
            source.portfolio_id, source.as_of_ts, source.run_id, source.agent_name,
            source.narrative_text, source.narrative_json, source.model_info,
            source.source_facts_hash, current_timestamp()
        );

        v_narrative_count := :v_narrative_count + 1;

        v_results := array_append(:v_results, object_construct(
            'portfolio_id', :v_portfolio_id,
            'facts_hash', :v_facts_hash,
            'cortex_succeeded', :v_cortex_succeeded,
            'model_info', :v_model_name
        ));
    end for;

    -- Compute top-level Cortex summary for audit visibility
    let v_cortex_success_count number := 0;
    let v_cortex_fallback_count number := 0;
    let v_narrative_mode string := 'UNKNOWN';
    begin
        select
            count_if(value:cortex_succeeded::boolean = true),
            count_if(value:cortex_succeeded::boolean = false or value:cortex_succeeded is null)
          into :v_cortex_success_count, :v_cortex_fallback_count
          from table(flatten(input => :v_results));
    exception
        when other then null;
    end;
    v_narrative_mode := case
        when :v_cortex_success_count > 0 and :v_cortex_fallback_count = 0 then 'CORTEX_AI'
        when :v_cortex_success_count = 0 then 'DETERMINISTIC_FALLBACK'
        else 'MIXED'
    end;

    -- Step 7: Audit log
    call MIP.APP.SP_LOG_EVENT(
        'AGENT',
        'SP_AGENT_GENERATE_DAILY_DIGEST',
        'SUCCESS',
        :v_portfolio_count,
        object_construct(
            'run_id', :v_run_id,
            'as_of_ts', :v_as_of_ts,
            'portfolio_count', :v_portfolio_count,
            'snapshot_count', :v_snapshot_count,
            'narrative_count', :v_narrative_count,
            'narrative_mode', :v_narrative_mode,
            'cortex_success_count', :v_cortex_success_count,
            'cortex_fallback_count', :v_cortex_fallback_count,
            'results', :v_results
        ),
        null,
        :v_run_id,
        null
    );

    return object_construct(
        'status', 'SUCCESS',
        'portfolio_count', :v_portfolio_count,
        'snapshot_count', :v_snapshot_count,
        'narrative_count', :v_narrative_count,
        'narrative_mode', :v_narrative_mode,
        'cortex_success_count', :v_cortex_success_count,
        'cortex_fallback_count', :v_cortex_fallback_count,
        'results', :v_results
    );
exception
    when other then
        call MIP.APP.SP_LOG_EVENT(
            'AGENT',
            'SP_AGENT_GENERATE_DAILY_DIGEST',
            'FAIL',
            :v_portfolio_count,
            object_construct(
                'run_id', :v_run_id,
                'as_of_ts', :v_as_of_ts,
                'portfolio_count', :v_portfolio_count,
                'snapshot_count', :v_snapshot_count,
                'narrative_count', :v_narrative_count
            ),
            :sqlerrm,
            :v_run_id,
            null
        );
        return object_construct(
            'status', 'ERROR',
            'error_message', :sqlerrm,
            'portfolio_count', :v_portfolio_count,
            'snapshot_count', :v_snapshot_count,
            'narrative_count', :v_narrative_count
        );
end;
$$;
