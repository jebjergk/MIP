-- 202_sp_agent_generate_daily_digest.sql
-- Purpose: Generate the Daily Intelligence Digest per portfolio AND global scope.
--   Step 1: Compute deterministic snapshot from V_DAILY_DIGEST_SNAPSHOT (per portfolio).
--   Step 2: MERGE snapshot into DAILY_DIGEST_SNAPSHOT.
--   Step 3: Call Snowflake Cortex COMPLETE() to produce narrative from snapshot + prior snapshot.
--   Step 4: MERGE narrative into DAILY_DIGEST_NARRATIVE.
--   Step 5: Repeat for GLOBAL scope using V_DAILY_DIGEST_SNAPSHOT_GLOBAL.
--   Step 6: Audit log event with counts and hashes.
--
-- Inputs:  :P_RUN_ID, :P_AS_OF_TS, :P_PORTFOLIO_ID (null = all active portfolios).
-- Uses:    :P_RUN_ID everywhere; does NOT use signal_run_id.
-- Cortex:  snowflake.cortex.complete('mistral-large2', prompt) for narrative.
-- Fallback: If Cortex fails, writes deterministic fallback narrative from detectors.
-- MERGE semantics: idempotent; reruns update the same keys (no duplicates).
-- SCOPE: 'PORTFOLIO' for per-portfolio rows, 'GLOBAL' for system-wide row.
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
                'PORTFOLIO'::varchar          as scope,
                :v_portfolio_id::number       as portfolio_id,
                :v_as_of_ts::timestamp_ntz    as as_of_ts,
                :v_run_id::varchar            as run_id,
                :v_snapshot::variant           as snapshot_json,
                :v_facts_hash::varchar         as source_facts_hash
        ) as source
        on  target.SCOPE        = source.scope
        and target.PORTFOLIO_ID = source.portfolio_id
        and target.AS_OF_TS     = source.as_of_ts
        and target.RUN_ID       = source.run_id
        when matched then update set
            target.SNAPSHOT_JSON     = source.snapshot_json,
            target.SOURCE_FACTS_HASH = source.source_facts_hash
        when not matched then insert (
            SCOPE, PORTFOLIO_ID, AS_OF_TS, RUN_ID, SNAPSHOT_JSON, SOURCE_FACTS_HASH, CREATED_AT
        ) values (
            source.scope, source.portfolio_id, source.as_of_ts, source.run_id,
            source.snapshot_json, source.source_facts_hash, current_timestamp()
        );

        v_snapshot_count := :v_snapshot_count + 1;

        -- Step 3: Get prior snapshot for narrative context
        begin
            v_prior_snapshot := (
                select SNAPSHOT_JSON
                from MIP.AGENT_OUT.DAILY_DIGEST_SNAPSHOT
                where SCOPE = 'PORTFOLIO'
                  and PORTFOLIO_ID = :v_portfolio_id
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
'You write clear, insightful daily digests that help a portfolio manager understand what is happening and what to do about it. ' ||
'You MUST only reference numbers and facts present in the snapshot data below. ' ||
'Do NOT invent facts, propose trades, or suggest parameter changes. ' ||
'
CURRENT SNAPSHOT (portfolio ' || :v_portfolio_id::string || '):
' || to_varchar(:v_snapshot) || '

PRIOR SNAPSHOT:
' || coalesce(to_varchar(:v_prior_snapshot), 'No prior snapshot available (first run).') || '

FIRED INTEREST DETECTORS (prioritise these):
' || to_varchar(:v_fired_detectors) || '

IMPORTANT CONTEXT — How this portfolio works:
MIP manages portfolios that automatically trade based on AI signals. The key concepts users need to understand:
- RISK GATE: Controls whether the portfolio can enter new trades. SAFE = open for business. CAUTION = partially restricted. STOPPED = no new entries allowed. A gate change is significant — explain what triggered it.
- CAPACITY (saturation): max_positions vs open_positions. When saturated (e.g. 5/5 slots used), no new trades can enter even if great signals appear. When near-full, every remaining slot is precious.
- SIGNALS: AI-generated trade recommendations. Not all signals lead to trades — they must pass through trust, eligibility, and portfolio rules first.
- PROPOSAL FUNNEL: signals → eligible → trusted → proposed → executed. Each step filters. Explain WHERE the funnel narrows and WHY.
- TRUST: Patterns must earn trust through training (enough outcomes, good hit rate, positive returns). TRUSTED signals can become proposals; UNTRUSTED cannot.
- TRAINING vs TRADING: Training must be CONFIDENT AND pattern must be TRUSTED for trades to happen. If training is stuck or trust is lost, no trades will flow.
- DRAWDOWN: How far the portfolio has fallen from its peak. A rising drawdown means losing money. Important for risk context.

CRITICAL RULE — CONSISTENCY ON TRUST AND ELIGIBILITY:
- The snapshot includes training trusted/watch/untrusted counts. These are the DEFINITIVE counts from the trust policy engine.
- Training gate thresholds (min signals, min hit rate, min avg return) are a SEPARATE system from the trust policy. They may not perfectly align.
- If the snapshot says N patterns are TRUSTED, those N patterns ARE trusted and CAN generate trade proposals. Do NOT then say "no patterns are eligible" or "patterns still need to earn trust" in another bullet.
- NEVER contradict yourself: if one sentence says X patterns are trusted, every other sentence must be consistent with that.
- When discussing why trades did or did not happen, check the proposal funnel data — not just trust counts.

PATTERN CONTEXT — How signals are generated:
The snapshot contains "patterns" (signal strategy definitions) and "pattern_observations" (what each pattern produced today).
- Each pattern has params: fast_window (recent behavior window in bars), slow_window (baseline window in bars), lookback_days (history depth for statistics), min_return (minimum observed return to generate a signal), min_zscore (minimum z-score threshold for signal strength).
- pattern_observations show per-pattern counts: signals_generated (how many raw signals the pattern produced), signals_trusted/signals_watch/signals_untrusted (how many passed each trust level), signals_eligible (how many can actually become trade proposals).
- observed_score_range shows the min/max/avg observed return (score) of signals that DID fire. This tells you how strong today''s signals were.
- near_miss_examples show WATCH signals (close to trust) with their observed scores and trust policy metrics.
- thresholds show the pattern''s own gating parameters.
- explain fields describe what each parameter means in plain language.
- training_thresholds show system-level trust gating: min_signals, min_hit_rate, min_avg_return needed for a pattern to earn TRUSTED status.

Pattern parameter explanation rules (must follow):
- You may ONLY explain a parameter (e.g., min_zscore, min_return, fast_window, slow_window, lookback_days) if you also cite an observation from "pattern_observations" that shows it mattered today (a signal passed/failed/was near-miss).
- When mentioning z-score, you MUST state its basis using snapshot fields (e.g., "relative to this symbol''s own return distribution over N lookback days of MARKET_TYPE bars").
- When mentioning fast_window / slow_window, you MUST describe them as two time-perspectives AND tie them to an observation (e.g., "the fast window (last N bars) reacted but the baseline (last M bars) did not").
- Never list raw PARAMS_JSON keys in isolation. Always pair parameter names with observed values and their practical meaning.
- If pattern_observations is empty (no signals today), do NOT explain pattern parameters — there is nothing to explain.

Produce a JSON object with exactly these keys:
{
  "headline": "One sentence summary of what matters most today",
  "what_changed": ["bullet 1", "bullet 2", ...],
  "what_matters": ["bullet 1", "bullet 2", ...],
  "waiting_for": ["bullet 1", "bullet 2", ...],
  "where_to_look": [{"label": "page name", "route": "/path"}, ...]
}

Narrative quality rules (must follow):
- Write as if explaining to a portfolio manager, not a data engineer. Answer "what does this mean for my portfolio?"
- Every bullet should be 2-3 sentences: state the fact, explain what it means, and state the consequence.
- Whenever you mention a metric (saturation, capacity, trust, drawdown, freshness, proposal funnel, gate), you MUST:
  (a) explain what it means in plain language,
  (b) show the numbers behind it (X of Y, percentages, deltas),
  (c) state the practical impact: "This means..." or "As a result..." or "So..."
  Example: "The portfolio is at 80% capacity — 4 of 5 allowed position slots are occupied. This means only 1 slot remains for new entries. If a strong signal appears, it can still be acted on, but the portfolio is nearly full."
- When explaining the proposal funnel, walk through each stage: how many signals were generated, how many passed eligibility, how many were trusted, how many became proposals, how many executed. Explain WHERE the biggest drop happens and WHY.
- For "no change" situations, always explain both WHY nothing changed AND what it means: "No new trades today because no new market bars arrived (the market was closed or data was delayed). The portfolio remains in its current state — existing positions continue to be held."
- Do NOT just name detectors. Explain what they mean for the user.
- waiting_for bullets must explain what happens when the condition is met: "Waiting for new market bars to arrive. Once they do, the signal engine will run and may identify new trading opportunities."
- Prefer substance over brevity. Longer, clearer bullets are better than short, cryptic ones.

Rules:
- headline: 1-2 sentences. State the key fact AND its meaning for the portfolio.
- what_changed: 3-5 bullets. Each explains a change, its cause, and its consequence. If nothing changed, explain why thoroughly (no new bars, market closed, data stale) and what the implication is.
- what_matters: 3-5 bullets. what_matters[0] MUST unpack one key pattern gating outcome using observed vs required numbers from pattern_observations, if any signals exist. Example: "Momentum signals were filtered mainly by trust gates: for FX_MOMENTUM_DAILY, 12 signals generated but only 2 were trusted (10 blocked). The pattern requires a min z-score of 1.0 against 60-day lookback, and observed returns ranged from 0.001 to 0.004." If no signals, focus on portfolio state instead. Include at least one bullet about the signal-to-trade pipeline (are signals flowing? are they converting to trades? if not, why?). Include at least one bullet explaining what the current drawdown/performance means.
- waiting_for: 2-4 bullets about what needs to happen next. Each must explain what the user can expect when the condition is met.
- where_to_look: 2-4 links. Valid routes: /signals, /training, /portfolios/' || :v_portfolio_id::string || ', /market-timeline, /suggestions, /cockpit
- Every number you mention MUST appear in the snapshot data.
- Return ONLY the raw JSON object. Do NOT wrap it in markdown fences or any extra text. Start your response with { and end with }.';

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
            -- Strip markdown code fences that Cortex sometimes wraps around JSON
            -- Handles: ```json\n{...}\n``` and ```\n{...}\n```
            v_narrative_text := trim(:v_narrative_text);
            if (left(:v_narrative_text, 7) = '```json') then
                v_narrative_text := trim(substr(:v_narrative_text, 8));
            elseif (left(:v_narrative_text, 3) = '```') then
                v_narrative_text := trim(substr(:v_narrative_text, 4));
            end if;
            if (right(:v_narrative_text, 3) = '```') then
                v_narrative_text := trim(substr(:v_narrative_text, 1, length(:v_narrative_text) - 3));
            end if;
            v_narrative_text := trim(:v_narrative_text);

            begin
                v_narrative_json := parse_json(:v_narrative_text);
            exception
                when other then
                    -- Cortex returned non-JSON even after stripping; wrap as plain text
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
                'PORTFOLIO'::varchar             as scope,
                :v_portfolio_id::number          as portfolio_id,
                :v_as_of_ts::timestamp_ntz       as as_of_ts,
                :v_run_id::varchar               as run_id,
                :v_agent_name::varchar           as agent_name,
                :v_narrative_text::string         as narrative_text,
                :v_narrative_json::variant        as narrative_json,
                :v_model_name::varchar            as model_info,
                :v_facts_hash::varchar            as source_facts_hash
        ) as source
        on  target.SCOPE        = source.scope
        and target.PORTFOLIO_ID = source.portfolio_id
        and target.AS_OF_TS     = source.as_of_ts
        and target.RUN_ID       = source.run_id
        and target.AGENT_NAME   = source.agent_name
        when matched then update set
            target.NARRATIVE_TEXT    = source.narrative_text,
            target.NARRATIVE_JSON   = source.narrative_json,
            target.MODEL_INFO       = source.model_info,
            target.SOURCE_FACTS_HASH = source.source_facts_hash
        when not matched then insert (
            SCOPE, PORTFOLIO_ID, AS_OF_TS, RUN_ID, AGENT_NAME,
            NARRATIVE_TEXT, NARRATIVE_JSON, MODEL_INFO, SOURCE_FACTS_HASH, CREATED_AT
        ) values (
            source.scope, source.portfolio_id, source.as_of_ts, source.run_id, source.agent_name,
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

    -- ════════════════════════════════════════════════════════════
    -- GLOBAL SCOPE: system-wide snapshot + narrative
    -- ════════════════════════════════════════════════════════════
    begin
        -- Global snapshot from the global view
        v_snapshot := (
            select SNAPSHOT_JSON
            from MIP.MART.V_DAILY_DIGEST_SNAPSHOT_GLOBAL
            limit 1
        );
        v_facts_hash := (select sha2(to_varchar(:v_snapshot), 256));

        -- MERGE global snapshot
        merge into MIP.AGENT_OUT.DAILY_DIGEST_SNAPSHOT as target
        using (
            select
                'GLOBAL'::varchar             as scope,
                null::number                  as portfolio_id,
                :v_as_of_ts::timestamp_ntz    as as_of_ts,
                :v_run_id::varchar            as run_id,
                :v_snapshot::variant           as snapshot_json,
                :v_facts_hash::varchar         as source_facts_hash
        ) as source
        on  target.SCOPE        = source.scope
        and target.PORTFOLIO_ID is null
        and source.portfolio_id is null
        and target.AS_OF_TS     = source.as_of_ts
        and target.RUN_ID       = source.run_id
        when matched then update set
            target.SNAPSHOT_JSON     = source.snapshot_json,
            target.SOURCE_FACTS_HASH = source.source_facts_hash
        when not matched then insert (
            SCOPE, PORTFOLIO_ID, AS_OF_TS, RUN_ID, SNAPSHOT_JSON, SOURCE_FACTS_HASH, CREATED_AT
        ) values (
            source.scope, null, source.as_of_ts, source.run_id,
            source.snapshot_json, source.source_facts_hash, current_timestamp()
        );

        v_snapshot_count := :v_snapshot_count + 1;

        -- Get prior global snapshot for context
        begin
            v_prior_snapshot := (
                select SNAPSHOT_JSON
                from MIP.AGENT_OUT.DAILY_DIGEST_SNAPSHOT
                where SCOPE = 'GLOBAL'
                  and PORTFOLIO_ID is null
                  and (AS_OF_TS < :v_as_of_ts or (AS_OF_TS = :v_as_of_ts and RUN_ID != :v_run_id))
                order by AS_OF_TS desc
                limit 1
            );
        exception
            when other then
                v_prior_snapshot := null;
        end;

        -- Fired detectors for global
        v_fired_detectors := (
            select array_agg(value)
            from table(flatten(input => :v_snapshot:detectors))
            where value:fired::boolean = true
        );
        v_fired_detectors := coalesce(:v_fired_detectors, array_construct());

        -- Build global Cortex prompt
        v_cortex_prompt :=
'You are a system-wide intelligence analyst for MIP (Market Intelligence Platform). ' ||
'You write clear, insightful daily digests that help a portfolio manager understand the big picture across ALL portfolios and the entire trading universe. ' ||
'You MUST only reference numbers and facts present in the snapshot data below. ' ||
'Do NOT invent facts, propose trades, or suggest parameter changes. ' ||
'
CURRENT GLOBAL SNAPSHOT:
' || to_varchar(:v_snapshot) || '

PRIOR GLOBAL SNAPSHOT:
' || coalesce(to_varchar(:v_prior_snapshot), 'No prior snapshot available (first run).') || '

FIRED INTEREST DETECTORS (prioritise these):
' || to_varchar(:v_fired_detectors) || '

IMPORTANT CONTEXT — The MIP system explained:
MIP is an automated market intelligence platform that manages multiple portfolios. Each portfolio trades autonomously based on AI signals that pass through training, trust, and risk rules. Here is what the user needs to understand:
- PORTFOLIOS: Each has its own risk gate (SAFE/CAUTION/STOPPED), capacity (position slots), and trade history. A gate change affects ALL trades for that portfolio.
- SIGNAL PIPELINE: Market bars arrive → signals are generated → filtered by eligibility → checked against trust rules → proposed as trades → executed. If ANY stage is empty or blocked, no trades flow.
- CAPACITY: Total slots across all portfolios. When the system is saturated (all slots full), no new trades can enter anywhere, no matter how good the signals are.
- TRUST: Patterns earn trust through training. The system tracks trusted/watch/untrusted counts. Only TRUSTED patterns can generate trade proposals.
- RISK: Drawdown, gate status, and health indicators determine whether portfolios are safe to trade. If risk is elevated, the system protects capital by restricting entries.
- "NOTHING HAPPENED" days are common and normal. Explain WHY: no new market data? market closed? all slots full? no trusted signals? This is the most important thing to explain clearly.

CRITICAL RULE — CONSISTENCY ON TRUST AND ELIGIBILITY:
- The snapshot includes system-wide trusted/watch/untrusted counts. These are DEFINITIVE — computed by the trust policy engine.
- Training thresholds (signals count, hit rate, avg return) are a SEPARATE scoring system. They may disagree with the trust label for individual symbols.
- If the snapshot says N patterns are TRUSTED, do NOT then say "no patterns are eligible" or "patterns cannot trade." Be consistent.
- If a specific symbol appears in the snapshot as TRUSTED, describe it as eligible for trading. If UNTRUSTED or WATCH, describe it as not eligible.
- NEVER write one sentence saying trading is possible and another saying it is not, for the same symbol or the same trust group.

PATTERN CONTEXT — How signals are generated:
The snapshot contains "patterns" (signal strategy definitions) and "pattern_observations" (what each pattern produced today).
- Each pattern has params: fast_window (recent behavior window in bars), slow_window (baseline window in bars), lookback_days (history depth for statistics), min_return (minimum observed return to generate a signal), min_zscore (minimum z-score threshold for signal strength).
- pattern_observations show per-pattern counts: signals_generated, signals_trusted/signals_watch/signals_untrusted, signals_eligible.
- observed_score_range shows the min/max/avg observed return (score) of signals that DID fire.
- near_miss_examples show WATCH signals (close to trust) with their observed scores and trust policy metrics.
- thresholds show the pattern''s own gating parameters.
- explain fields describe what each parameter means in plain language.
- training_thresholds show system-level trust gating: min_signals, min_hit_rate, min_avg_return needed for a pattern to earn TRUSTED status.

Pattern parameter explanation rules (must follow):
- You may ONLY explain a parameter (e.g., min_zscore, min_return, fast_window, slow_window, lookback_days) if you also cite an observation from "pattern_observations" that shows it mattered today (a signal passed/failed/was near-miss).
- When mentioning z-score, you MUST state its basis using snapshot fields (e.g., "relative to each symbol''s own return distribution over N lookback days").
- When mentioning fast_window / slow_window, you MUST describe them as two time-perspectives AND tie them to an observation.
- Never list raw PARAMS_JSON keys in isolation. Always pair parameter names with observed values and their practical meaning.
- If pattern_observations is empty (no signals today), do NOT explain pattern parameters — there is nothing to explain.

Produce a JSON object with exactly these keys:
{
  "headline": "One sentence summary of the system-wide state today",
  "what_changed": ["bullet 1", "bullet 2", ...],
  "what_matters": ["bullet 1", "bullet 2", ...],
  "waiting_for": ["bullet 1", "bullet 2", ...],
  "where_to_look": [{"label": "page name", "route": "/path"}, ...]
}

Narrative quality rules (must follow):
- Write as if briefing a portfolio manager who wants to know: "Is everything running? Are we making money? Is anything stuck? What should I watch?"
- Every bullet should be 2-3 sentences: state the fact, explain what it means, and state the consequence for the user.
- Whenever you mention a metric (capacity, saturation, trust, drawdown, freshness, proposal funnel, gate), you MUST:
  (a) explain what it means in plain language,
  (b) show the numbers (X of Y, percentages, deltas across portfolios),
  (c) state the practical impact.
  Example: "The system is using 6 of 10 total position slots across 2 portfolios (60% saturated). This means 4 slots are available for new trades — the system has room to act on new opportunities when they appear."
- For the proposal funnel, walk through the full pipeline: signals generated → eligible → trusted → proposed → executed. Show where the funnel narrows most and explain why: "The biggest drop is from eligible to trusted (X → Y), because most signals come from patterns still in training. As more patterns earn trust, this funnel will widen."
- When nothing changed, give a thorough explanation: "No new trades or signals today because no new market bars arrived. This typically happens on weekends/holidays or when the data provider has a delay. All existing positions continue to be held. The system will resume signal generation when fresh market data appears."
- Do NOT just name detectors. Explain their meaning and impact.
- Prefer substance and reasoning over brevity.

Rules:
- headline: 1-2 sentences. State the system state AND its meaning. Example: "All 2 portfolios running safely with 4 of 10 slots available — system is ready for opportunities but no new market data arrived today."
- what_changed: 3-5 bullets. Each explains a change, names portfolios where relevant, and states the consequence. If nothing changed, explain why thoroughly and reassure the user.
- what_matters: 3-5 bullets. what_matters[0] MUST unpack one key pattern gating outcome using observed vs required numbers from pattern_observations, if any signals exist. Example: "Across all patterns, 24 momentum signals were generated today — 14 from FX_MOMENTUM_DAILY and 10 from STOCK_MOMENTUM_FAST. Only 4 were eligible for trading because most patterns are still in WATCH trust status (they need min_hit_rate >= 0.55 but current rates range from 0.38 to 0.51)." If no signals today, focus on system health instead. Include at least one bullet about the signal pipeline (flowing or blocked, and why). Include at least one bullet comparing system state to prior (is it improving, stable, or degrading?).
- waiting_for: 2-4 bullets. Each explains what needs to happen and what the user can expect when it does.
- where_to_look: 2-4 links. Valid routes: /signals, /training, /market-timeline, /suggestions, /portfolios, /cockpit
- Every number you mention MUST appear in the snapshot data.
- Return ONLY the raw JSON object. Do NOT wrap it in markdown fences or any extra text. Start your response with { and end with }.';

        v_model_name := 'mistral-large2';
        v_cortex_succeeded := false;

        begin
            v_narrative_text := (
                select snowflake.cortex.complete(:v_model_name, :v_cortex_prompt)
            );
            v_cortex_succeeded := true;
        exception
            when other then
                v_cortex_succeeded := false;
                call MIP.APP.SP_LOG_EVENT(
                    'AGENT',
                    'SP_AGENT_GENERATE_DAILY_DIGEST',
                    'WARN_CORTEX_FAILED_GLOBAL',
                    null,
                    object_construct(
                        'scope', 'GLOBAL',
                        'run_id', :v_run_id,
                        'error', :sqlerrm
                    ),
                    :sqlerrm,
                    :v_run_id,
                    null
                );
        end;

        -- Parse or fallback (same logic as portfolio)
        if (:v_cortex_succeeded and :v_narrative_text is not null) then
            v_narrative_text := trim(:v_narrative_text);
            if (left(:v_narrative_text, 7) = '```json') then
                v_narrative_text := trim(substr(:v_narrative_text, 8));
            elseif (left(:v_narrative_text, 3) = '```') then
                v_narrative_text := trim(substr(:v_narrative_text, 4));
            end if;
            if (right(:v_narrative_text, 3) = '```') then
                v_narrative_text := trim(substr(:v_narrative_text, 1, length(:v_narrative_text) - 3));
            end if;
            v_narrative_text := trim(:v_narrative_text);

            begin
                v_narrative_json := parse_json(:v_narrative_text);
            exception
                when other then
                    v_narrative_json := object_construct(
                        'headline', 'Global digest generated but could not parse structured response.',
                        'what_changed', array_construct(:v_narrative_text),
                        'what_matters', array_construct(),
                        'waiting_for', array_construct(),
                        'where_to_look', array_construct()
                    );
            end;
        else
            -- Deterministic fallback for global scope
            v_fallback_bullets := array_construct();

            v_fallback_bullets := array_append(:v_fallback_bullets,
                'Active portfolios: ' || coalesce(:v_snapshot:system:active_portfolios::string, '0')
            );
            v_fallback_bullets := array_append(:v_fallback_bullets,
                'Gate status: ' || coalesce(:v_snapshot:gates:ok_count::string, '0') || ' OK, ' ||
                coalesce(:v_snapshot:gates:warn_count::string, '0') || ' WARN, ' ||
                coalesce(:v_snapshot:gates:blocked_count::string, '0') || ' blocked'
            );
            v_fallback_bullets := array_append(:v_fallback_bullets,
                'Signals today: ' || coalesce(:v_snapshot:signals:total_signals::string, '0') ||
                ' total, ' || coalesce(:v_snapshot:signals:total_eligible::string, '0') || ' eligible'
            );
            v_fallback_bullets := array_append(:v_fallback_bullets,
                'Proposals: ' || coalesce(:v_snapshot:proposals:total_proposed::string, '0') ||
                ' proposed, ' || coalesce(:v_snapshot:proposals:total_executed::string, '0') ||
                ' executed, ' || coalesce(:v_snapshot:proposals:total_rejected::string, '0') || ' rejected'
            );
            v_fallback_bullets := array_append(:v_fallback_bullets,
                'Training: ' || coalesce(:v_snapshot:training:trusted_count::string, '0') ||
                ' trusted / ' || coalesce(:v_snapshot:training:watch_count::string, '0') ||
                ' watch / ' || coalesce(:v_snapshot:training:untrusted_count::string, '0') || ' untrusted'
            );

            v_narrative_text := 'No AI narrative available; showing deterministic summary.';
            v_narrative_json := object_construct(
                'headline', 'Global digest — ' ||
                    coalesce(:v_snapshot:system:active_portfolios::string, '0') || ' active portfolios, ' ||
                    coalesce(:v_snapshot:signals:total_signals::string, '0') || ' signals today',
                'what_changed', :v_fallback_bullets,
                'what_matters', array_construct(
                    'Total open positions: ' || coalesce(:v_snapshot:capacity:total_open::string, '0') ||
                    ' / ' || coalesce(:v_snapshot:capacity:total_max_positions::string, '?') ||
                    ' (' || coalesce(:v_snapshot:capacity:total_remaining::string, '?') || ' remaining)',
                    'System saturation: ' || coalesce(:v_snapshot:capacity:saturation_pct::string, '0') || '%'
                ),
                'waiting_for', array_construct(
                    'Next pipeline run for fresh signals',
                    coalesce(:v_snapshot:training:watch_count::string, '0') || ' patterns in WATCH status approaching trust threshold'
                ),
                'where_to_look', array_construct(
                    object_construct('label', 'Signals Explorer', 'route', '/signals'),
                    object_construct('label', 'Training Status', 'route', '/training'),
                    object_construct('label', 'Cockpit', 'route', '/cockpit')
                )
            );
            v_model_name := 'DETERMINISTIC_FALLBACK';
        end if;

        -- MERGE global narrative
        merge into MIP.AGENT_OUT.DAILY_DIGEST_NARRATIVE as target
        using (
            select
                'GLOBAL'::varchar                as scope,
                null::number                     as portfolio_id,
                :v_as_of_ts::timestamp_ntz       as as_of_ts,
                :v_run_id::varchar               as run_id,
                :v_agent_name::varchar           as agent_name,
                :v_narrative_text::string         as narrative_text,
                :v_narrative_json::variant        as narrative_json,
                :v_model_name::varchar            as model_info,
                :v_facts_hash::varchar            as source_facts_hash
        ) as source
        on  target.SCOPE        = source.scope
        and target.PORTFOLIO_ID is null
        and source.portfolio_id is null
        and target.AS_OF_TS     = source.as_of_ts
        and target.RUN_ID       = source.run_id
        and target.AGENT_NAME   = source.agent_name
        when matched then update set
            target.NARRATIVE_TEXT    = source.narrative_text,
            target.NARRATIVE_JSON   = source.narrative_json,
            target.MODEL_INFO       = source.model_info,
            target.SOURCE_FACTS_HASH = source.source_facts_hash
        when not matched then insert (
            SCOPE, PORTFOLIO_ID, AS_OF_TS, RUN_ID, AGENT_NAME,
            NARRATIVE_TEXT, NARRATIVE_JSON, MODEL_INFO, SOURCE_FACTS_HASH, CREATED_AT
        ) values (
            source.scope, null, source.as_of_ts, source.run_id, source.agent_name,
            source.narrative_text, source.narrative_json, source.model_info,
            source.source_facts_hash, current_timestamp()
        );

        v_narrative_count := :v_narrative_count + 1;

        v_results := array_append(:v_results, object_construct(
            'scope', 'GLOBAL',
            'portfolio_id', null,
            'facts_hash', :v_facts_hash,
            'cortex_succeeded', :v_cortex_succeeded,
            'model_info', :v_model_name
        ));
    exception
        when other then
            -- Global digest failure is non-fatal; log and continue
            call MIP.APP.SP_LOG_EVENT(
                'AGENT',
                'SP_AGENT_GENERATE_DAILY_DIGEST',
                'WARN_GLOBAL_DIGEST_FAILED',
                null,
                object_construct(
                    'scope', 'GLOBAL',
                    'run_id', :v_run_id,
                    'error', :sqlerrm
                ),
                :sqlerrm,
                :v_run_id,
                null
            );
            v_results := array_append(:v_results, object_construct(
                'scope', 'GLOBAL',
                'portfolio_id', null,
                'cortex_succeeded', false,
                'error', :sqlerrm
            ));
    end;

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
