-- 245_sp_generate_pw_narrative.sql
-- Purpose: Generate Parallel Worlds narrative for each active portfolio using Cortex.
--   Step 1: Compute deterministic snapshot from V_PARALLEL_WORLD_SNAPSHOT.
--   Step 2: MERGE snapshot into PARALLEL_WORLD_SNAPSHOT.
--   Step 3: Call Snowflake Cortex COMPLETE() to produce narrative from snapshot.
--   Step 4: MERGE narrative into PARALLEL_WORLD_NARRATIVE.
--   Fallback: If Cortex fails, writes deterministic fallback narrative.
--   MERGE semantics: idempotent; reruns update the same keys (no duplicates).

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_GENERATE_PW_NARRATIVE(
    P_RUN_ID        varchar,
    P_AS_OF_TS      timestamp_ntz,
    P_PORTFOLIO_ID  number default null
)
returns variant
language sql
execute as caller
as
$$
declare
    v_run_id            varchar := :P_RUN_ID;
    v_as_of_ts          timestamp_ntz := :P_AS_OF_TS;
    v_agent_name        varchar := 'PARALLEL_WORLDS';
    v_model_name        varchar := 'mistral-large2';
    v_portfolios        resultset;
    v_portfolio_id      number;
    v_snapshot          variant;
    v_facts_hash        varchar;
    v_narrative_text    string;
    v_narrative_json    variant;
    v_cortex_prompt     string;
    v_cortex_succeeded  boolean := false;
    v_portfolio_count   number := 0;
    v_snapshot_count    number := 0;
    v_narrative_count   number := 0;
begin
    -- Portfolio scope
    if (:P_PORTFOLIO_ID is not null and :P_PORTFOLIO_ID > 0) then
        v_portfolios := (select PORTFOLIO_ID from MIP.APP.PORTFOLIO where PORTFOLIO_ID = :P_PORTFOLIO_ID and STATUS = 'ACTIVE');
    else
        v_portfolios := (select PORTFOLIO_ID from MIP.APP.PORTFOLIO where STATUS = 'ACTIVE' order by PORTFOLIO_ID);
    end if;

    for rec in v_portfolios do
        v_portfolio_id := rec.PORTFOLIO_ID;
        v_portfolio_count := :v_portfolio_count + 1;
        v_cortex_succeeded := false;

        -- Step 1: Compute snapshot
        begin
            v_snapshot := (
                select SNAPSHOT_JSON
                from MIP.MART.V_PARALLEL_WORLD_SNAPSHOT
                where PORTFOLIO_ID = :v_portfolio_id
                  and AS_OF_TS::date = :v_as_of_ts::date
                limit 1
            );
        exception when other then
            v_snapshot := object_construct('error', 'SNAPSHOT_VIEW_FAILED', 'message', :sqlerrm, 'portfolio_id', :v_portfolio_id);
        end;

        if (:v_snapshot is null) then
            continue;
        end if;

        v_facts_hash := (select sha2(to_varchar(:v_snapshot), 256));

        -- Step 2: MERGE snapshot
        merge into MIP.AGENT_OUT.PARALLEL_WORLD_SNAPSHOT as target
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

        -- Step 3: Build Cortex prompt
        v_cortex_prompt :=
'You are a Parallel Worlds analyst for MIP (Market Intelligence Platform). ' ||
'You write clear, insightful counterfactual analysis that helps a portfolio manager understand what alternatives existed and why they were not taken. ' ||
'You MUST only reference numbers and facts present in the snapshot data below. ' ||
'Do NOT invent facts, propose trades, or suggest parameter changes. ' ||
'
PARALLEL WORLDS SNAPSHOT (portfolio ' || :v_portfolio_id::string || '):
' || to_varchar(:v_snapshot) || '

IMPORTANT CONTEXT — What Parallel Worlds shows:
- ACTUAL world: what the portfolio actually did today (trades, PnL, equity, positions).
- COUNTERFACTUAL worlds: what would have happened under alternative rules (different thresholds, sizing, timing, or doing nothing).
- Each scenario has a PNL_DELTA (positive = scenario would have done better than actual, negative = worse).
- Decision traces show which gates (risk, capacity, trust, threshold) caused the divergence.
- Regret metrics show whether a scenario CONSISTENTLY outperforms actual over rolling 20-day windows.

Produce a JSON object with exactly these keys:
{
  "headline": "One sentence: what is the key takeaway from comparing actual vs alternatives today?",
  "best_scenario": {"name": "scenario_name", "pnl_delta": 123.45, "reason": "Why this scenario outperformed"},
  "worst_scenario": {"name": "scenario_name", "pnl_delta": -67.89, "reason": "Why this scenario underperformed"},
  "gate_analysis": "Which gates caused the biggest divergence between actual and counterfactual? Reference specific gate values.",
  "regret_trend": "Are any scenarios consistently outperforming actual? Reference rolling regret numbers if available.",
  "what_if_summary": ["bullet 1: key insight about a specific scenario", "bullet 2: another key insight"],
  "recommendation": "Based on consistent regret patterns, what should the manager consider? (Only if regret data suggests a pattern; otherwise say No action suggested.)"
}

CRITICAL FORMATTING RULES:
- ALWAYS use percentages for returns and drawdowns (multiply by 100). 0.005 → 0.50%, not 0.005.
- ALWAYS use $ for dollar amounts. Round to 2 decimal places.
- Every number you mention MUST appear in the snapshot data.
- Return ONLY the raw JSON object. Start with { and end with }.';

        begin
            v_narrative_text := (select snowflake.cortex.complete(:v_model_name, :v_cortex_prompt));
            v_cortex_succeeded := true;
        exception when other then
            v_cortex_succeeded := false;
        end;

        -- Step 4: Parse or fallback
        if (:v_cortex_succeeded) then
            begin
                v_narrative_json := (select try_parse_json(:v_narrative_text));
                if (:v_narrative_json is null) then
                    v_narrative_json := object_construct('raw_text', :v_narrative_text);
                end if;
            exception when other then
                v_narrative_json := object_construct('raw_text', :v_narrative_text);
            end;
        else
            -- Deterministic fallback
            v_narrative_text := 'Parallel Worlds analysis for portfolio ' || :v_portfolio_id::string || ' on ' || :v_as_of_ts::string || '.';
            v_narrative_json := object_construct(
                'headline', 'Parallel Worlds comparison completed — ' ||
                    coalesce(:v_snapshot:summary:scenarios_outperformed::string, '0') ||
                    ' of ' || coalesce(:v_snapshot:summary:total_scenarios::string, '8') ||
                    ' scenarios outperformed actual.',
                'best_scenario', object_construct(
                    'name', coalesce(:v_snapshot:summary:best_scenario::string, 'N/A'),
                    'pnl_delta', coalesce(:v_snapshot:summary:best_pnl_delta::number, 0)
                ),
                'worst_scenario', object_construct(
                    'name', coalesce(:v_snapshot:summary:worst_scenario::string, 'N/A'),
                    'pnl_delta', coalesce(:v_snapshot:summary:worst_pnl_delta::number, 0)
                ),
                'gate_analysis', 'Deterministic fallback — Cortex unavailable.',
                'regret_trend', 'Deterministic fallback — see regret data in snapshot.',
                'what_if_summary', array_construct('See scenario comparison table for details.'),
                'recommendation', 'No action suggested (fallback mode).'
            );
        end if;

        -- Step 5: MERGE narrative
        merge into MIP.AGENT_OUT.PARALLEL_WORLD_NARRATIVE as target
        using (
            select
                :v_portfolio_id::number       as portfolio_id,
                :v_as_of_ts::timestamp_ntz    as as_of_ts,
                :v_run_id::varchar            as run_id,
                :v_agent_name::varchar        as agent_name,
                :v_narrative_text::string      as narrative_text,
                :v_narrative_json::variant     as narrative_json,
                iff(:v_cortex_succeeded, :v_model_name, 'DETERMINISTIC_FALLBACK')::varchar as model_info,
                :v_facts_hash::varchar         as source_facts_hash
        ) as source
        on  target.PORTFOLIO_ID = source.portfolio_id
        and target.AS_OF_TS     = source.as_of_ts
        and target.RUN_ID       = source.run_id
        and target.AGENT_NAME   = source.agent_name
        when matched then update set
            target.NARRATIVE_TEXT     = source.narrative_text,
            target.NARRATIVE_JSON    = source.narrative_json,
            target.MODEL_INFO        = source.model_info,
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
    end for;

    return object_construct(
        'status', 'COMPLETED',
        'portfolio_count', :v_portfolio_count,
        'snapshot_count', :v_snapshot_count,
        'narrative_count', :v_narrative_count
    );
end;
$$;
