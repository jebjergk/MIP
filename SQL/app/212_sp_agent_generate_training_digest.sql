-- 212_sp_agent_generate_training_digest.sql
-- Purpose: Generate the Training Journey Digest (global + per-symbol).
--   Step 1: Compute global training snapshot from V_TRAINING_DIGEST_SNAPSHOT_GLOBAL.
--   Step 2: MERGE snapshot + call Cortex for global narrative.
--   Step 3: Loop top symbols, compute per-symbol snapshot from V_TRAINING_DIGEST_SNAPSHOT_SYMBOL.
--   Step 4: MERGE snapshot + call Cortex for per-symbol narrative.
--   Step 5: Audit log.
--
-- Inputs:  :P_RUN_ID, :P_AS_OF_TS, :P_SYMBOL (null = global + top symbols).
-- Cortex:  snowflake.cortex.complete('mistral-large2', prompt) for narrative.
-- Fallback: If Cortex fails, writes deterministic fallback narrative.
-- MERGE semantics: idempotent; reruns update the same keys (no duplicates).

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_AGENT_GENERATE_TRAINING_DIGEST(
    P_RUN_ID        varchar,
    P_AS_OF_TS      timestamp_ntz,
    P_SYMBOL        varchar default null,  -- null = global + top symbols
    P_MARKET_TYPE   varchar default null
)
returns variant
language sql
execute as caller
as
$$
declare
    v_run_id            varchar := :P_RUN_ID;
    v_as_of_ts          timestamp_ntz := :P_AS_OF_TS;
    v_agent_name        varchar := 'TRAINING_DIGEST';
    v_model_name        varchar := 'mistral-large2';
    v_snapshot          variant;
    v_prior_snapshot    variant;
    v_facts_hash        varchar;
    v_narrative_text    string;
    v_narrative_json    variant;
    v_cortex_prompt     string;
    v_cortex_succeeded  boolean := false;
    v_results           array := array_construct();
    v_snapshot_count    number := 0;
    v_narrative_count   number := 0;
    v_fired_detectors   variant;
    v_fallback_bullets  array;
    v_symbols           resultset;
    v_symbol            varchar;
    v_market_type       varchar;
begin
    -- ════════════════════════════════════════════════════════════
    -- GLOBAL TRAINING DIGEST
    -- ════════════════════════════════════════════════════════════
    if (:P_SYMBOL is null) then
    begin
        v_snapshot := (
            select SNAPSHOT_JSON
            from MIP.MART.V_TRAINING_DIGEST_SNAPSHOT_GLOBAL
            limit 1
        );
        v_facts_hash := (select sha2(to_varchar(:v_snapshot), 256));

        -- MERGE global snapshot
        merge into MIP.AGENT_OUT.TRAINING_DIGEST_SNAPSHOT as target
        using (
            select
                'GLOBAL_TRAINING'::varchar    as scope,
                null::varchar                 as symbol,
                null::varchar                 as market_type,
                :v_as_of_ts::timestamp_ntz    as as_of_ts,
                :v_run_id::varchar            as run_id,
                :v_snapshot::variant           as snapshot_json,
                :v_facts_hash::varchar         as source_facts_hash
        ) as source
        on  target.SCOPE        = source.scope
        and target.SYMBOL is null and source.symbol is null
        and target.MARKET_TYPE is null and source.market_type is null
        and target.AS_OF_TS     = source.as_of_ts
        and target.RUN_ID       = source.run_id
        when matched then update set
            target.SNAPSHOT_JSON     = source.snapshot_json,
            target.SOURCE_FACTS_HASH = source.source_facts_hash
        when not matched then insert (
            SCOPE, SYMBOL, MARKET_TYPE, AS_OF_TS, RUN_ID, SNAPSHOT_JSON, SOURCE_FACTS_HASH, CREATED_AT
        ) values (
            source.scope, null, null, source.as_of_ts, source.run_id,
            source.snapshot_json, source.source_facts_hash, current_timestamp()
        );
        v_snapshot_count := :v_snapshot_count + 1;

        -- Prior global snapshot
        begin
            v_prior_snapshot := (
                select SNAPSHOT_JSON
                from MIP.AGENT_OUT.TRAINING_DIGEST_SNAPSHOT
                where SCOPE = 'GLOBAL_TRAINING'
                  and SYMBOL is null
                  and (AS_OF_TS < :v_as_of_ts or (AS_OF_TS = :v_as_of_ts and RUN_ID != :v_run_id))
                order by AS_OF_TS desc
                limit 1
            );
        exception when other then v_prior_snapshot := null;
        end;

        -- Fired detectors
        v_fired_detectors := (
            select array_agg(value)
            from table(flatten(input => :v_snapshot:detectors))
            where value:fired::boolean = true
        );
        v_fired_detectors := coalesce(:v_fired_detectors, array_construct());

        -- Build global training Cortex prompt
        v_cortex_prompt :=
'You are a training coach and analyst for MIP (Market Intelligence Platform). ' ||
'You explain the training journey in plain language — where we are, what changed, what matters, what we are waiting for. ' ||
'You MUST only reference numbers and facts present in the snapshot data below. ' ||
'Do NOT invent facts, propose trades, or suggest training parameter changes. ' ||
'
CURRENT GLOBAL TRAINING SNAPSHOT:
' || to_varchar(:v_snapshot) || '

PRIOR GLOBAL TRAINING SNAPSHOT:
' || coalesce(to_varchar(:v_prior_snapshot), 'No prior snapshot available (first run).') || '

FIRED INTEREST DETECTORS (prioritise these):
' || to_varchar(:v_fired_detectors) || '

Produce a JSON object with exactly these keys:
{
  "headline": "One sentence summary of the training state across all symbols",
  "what_changed": ["bullet 1", "bullet 2", ...],
  "what_matters": ["bullet 1", "bullet 2", ...],
  "waiting_for": ["bullet 1", "bullet 2", ...],
  "where_to_look": [{"label": "page name", "route": "/path"}, ...],
  "journey": ["Collecting evidence", "Evaluating outcomes", "Earning trust", "Becoming trade-eligible"]
}

Narrative quality rules (must follow):
- Explain metrics like you are talking to a non-expert.
- Whenever you mention a metric or label (e.g. maturity score, coverage, hit rate, confidence, stage), you MUST unpack it using snapshot values:
  (a) what it means in plain language,
  (b) what numbers it is made of (e.g. X of Y, counts, deltas),
  (c) the operational implication ("so what").
  Example: "12 of 25 symbols are still in INSUFFICIENT stage (maturity < 25) — they need more signal recommendations before outcomes can be evaluated."
- Avoid vague bullets. Prefer: "Because <fact>, <implication>. Today: <number>."
- Do not say "no change" unless you also state the most likely reason from the snapshot (e.g. no new outcomes, no new recommendations, stalled evaluation).
- waiting_for bullets must be specific thresholds when available: "Waiting for <threshold> (today: A, target: B)."
- The journey field should list the 4 training stages as short steps with a marker showing current system position.

Rules:
- headline: 1 sentence, reference concrete numbers from snapshot. This covers ALL symbols.
- what_changed: 3-5 bullets about training changes since prior snapshot. Name specific symbols when they advanced or regressed.
- what_matters: 2-4 bullets. what_matters[0] MUST unpack the headline metric(s) using snapshot components (stage counts, totals, near-misses, etc.).
- waiting_for: 2-3 bullets about thresholds symbols are approaching. Be specific with gap numbers.
- where_to_look: 2-4 links. Valid routes: /training, /signals, /market-timeline, /digest, /brief
- journey: MUST be exactly 4 items: "Collecting evidence (N symbols)", "Evaluating outcomes (N symbols)", "Earning trust (N symbols)", "Trade-eligible (N symbols)" using stage counts from snapshot.
- Every number you mention MUST appear in the snapshot data.
- Return ONLY the raw JSON object. Do NOT wrap it in markdown fences or any extra text. Start your response with { and end with }.';

        v_model_name := 'mistral-large2';
        v_cortex_succeeded := false;

        begin
            v_narrative_text := (select snowflake.cortex.complete(:v_model_name, :v_cortex_prompt));
            v_cortex_succeeded := true;
        exception
            when other then
                v_cortex_succeeded := false;
                call MIP.APP.SP_LOG_EVENT('AGENT', 'SP_AGENT_GENERATE_TRAINING_DIGEST',
                    'WARN_CORTEX_FAILED_GLOBAL', null,
                    object_construct('scope', 'GLOBAL_TRAINING', 'run_id', :v_run_id, 'error', :sqlerrm),
                    :sqlerrm, :v_run_id, null);
        end;

        -- Parse or fallback
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
            exception when other then
                v_narrative_json := object_construct(
                    'headline', 'Training digest generated but could not parse structured response.',
                    'what_changed', array_construct(:v_narrative_text),
                    'what_matters', array_construct(),
                    'waiting_for', array_construct(),
                    'where_to_look', array_construct(),
                    'journey', array_construct()
                );
            end;
        else
            -- Deterministic fallback
            v_fallback_bullets := array_construct();
            v_fallback_bullets := array_append(:v_fallback_bullets,
                'Training universe: ' || coalesce(:v_snapshot:stages:total_symbols::string, '0') || ' symbols tracked');
            v_fallback_bullets := array_append(:v_fallback_bullets,
                'Stage distribution: ' ||
                coalesce(:v_snapshot:stages:insufficient_count::string, '0') || ' INSUFFICIENT, ' ||
                coalesce(:v_snapshot:stages:warming_up_count::string, '0') || ' WARMING_UP, ' ||
                coalesce(:v_snapshot:stages:learning_count::string, '0') || ' LEARNING, ' ||
                coalesce(:v_snapshot:stages:confident_count::string, '0') || ' CONFIDENT');
            v_fallback_bullets := array_append(:v_fallback_bullets,
                'Trust distribution: ' ||
                coalesce(:v_snapshot:trust:trusted_count::string, '0') || ' trusted / ' ||
                coalesce(:v_snapshot:trust:watch_count::string, '0') || ' watch / ' ||
                coalesce(:v_snapshot:trust:untrusted_count::string, '0') || ' untrusted');
            v_fallback_bullets := array_append(:v_fallback_bullets,
                'Total outcomes evaluated: ' || coalesce(:v_snapshot:stages:total_outcomes::string, '0') ||
                ' (avg coverage: ' || coalesce(:v_snapshot:stages:avg_coverage_ratio::string, '0') || ')');

            v_narrative_text := 'No AI narrative available; showing deterministic training summary.';
            v_narrative_json := object_construct(
                'headline', 'Training overview — ' ||
                    coalesce(:v_snapshot:stages:total_symbols::string, '0') || ' symbols, ' ||
                    coalesce(:v_snapshot:stages:confident_count::string, '0') || ' CONFIDENT',
                'what_changed', :v_fallback_bullets,
                'what_matters', array_construct(
                    'Average maturity: ' || coalesce(:v_snapshot:stages:avg_maturity_score::string, '0') ||
                    '/100 across ' || coalesce(:v_snapshot:stages:total_symbols::string, '0') || ' symbols'
                ),
                'waiting_for', array_construct(
                    'More outcome evaluations needed to advance training stages'
                ),
                'where_to_look', array_construct(
                    object_construct('label', 'Training Status', 'route', '/training'),
                    object_construct('label', 'Signals Explorer', 'route', '/signals')
                ),
                'journey', array_construct(
                    'Collecting evidence (' || coalesce(:v_snapshot:stages:insufficient_count::string, '0') || ' symbols)',
                    'Evaluating outcomes (' || coalesce(:v_snapshot:stages:warming_up_count::string, '0') || ' symbols)',
                    'Earning trust (' || coalesce(:v_snapshot:stages:learning_count::string, '0') || ' symbols)',
                    'Trade-eligible (' || coalesce(:v_snapshot:stages:confident_count::string, '0') || ' symbols)'
                )
            );
            v_model_name := 'DETERMINISTIC_FALLBACK';
        end if;

        -- MERGE global narrative
        merge into MIP.AGENT_OUT.TRAINING_DIGEST_NARRATIVE as target
        using (
            select
                'GLOBAL_TRAINING'::varchar       as scope,
                null::varchar                    as symbol,
                null::varchar                    as market_type,
                :v_as_of_ts::timestamp_ntz       as as_of_ts,
                :v_run_id::varchar               as run_id,
                :v_agent_name::varchar           as agent_name,
                :v_narrative_text::string         as narrative_text,
                :v_narrative_json::variant        as narrative_json,
                :v_model_name::varchar            as model_info,
                :v_facts_hash::varchar            as source_facts_hash
        ) as source
        on  target.SCOPE        = source.scope
        and target.SYMBOL is null and source.symbol is null
        and target.MARKET_TYPE is null and source.market_type is null
        and target.AS_OF_TS     = source.as_of_ts
        and target.RUN_ID       = source.run_id
        and target.AGENT_NAME   = source.agent_name
        when matched then update set
            target.NARRATIVE_TEXT     = source.narrative_text,
            target.NARRATIVE_JSON    = source.narrative_json,
            target.MODEL_INFO        = source.model_info,
            target.SOURCE_FACTS_HASH = source.source_facts_hash
        when not matched then insert (
            SCOPE, SYMBOL, MARKET_TYPE, AS_OF_TS, RUN_ID, AGENT_NAME,
            NARRATIVE_TEXT, NARRATIVE_JSON, MODEL_INFO, SOURCE_FACTS_HASH, CREATED_AT
        ) values (
            source.scope, null, null, source.as_of_ts, source.run_id, source.agent_name,
            source.narrative_text, source.narrative_json, source.model_info,
            source.source_facts_hash, current_timestamp()
        );
        v_narrative_count := :v_narrative_count + 1;

        v_results := array_append(:v_results, object_construct(
            'scope', 'GLOBAL_TRAINING',
            'facts_hash', :v_facts_hash,
            'cortex_succeeded', :v_cortex_succeeded,
            'model_info', :v_model_name
        ));
    exception
        when other then
            call MIP.APP.SP_LOG_EVENT('AGENT', 'SP_AGENT_GENERATE_TRAINING_DIGEST',
                'WARN_GLOBAL_FAILED', null,
                object_construct('scope', 'GLOBAL_TRAINING', 'run_id', :v_run_id, 'error', :sqlerrm),
                :sqlerrm, :v_run_id, null);
            v_results := array_append(:v_results, object_construct(
                'scope', 'GLOBAL_TRAINING', 'cortex_succeeded', false, 'error', :sqlerrm));
    end;
    end if;

    -- ════════════════════════════════════════════════════════════
    -- PER-SYMBOL TRAINING DIGESTS
    -- ════════════════════════════════════════════════════════════
    -- If P_SYMBOL is specified, do just that one; otherwise top 10 by maturity score
    if (:P_SYMBOL is not null) then
        v_symbols := (
            select SYMBOL, MARKET_TYPE
            from MIP.MART.V_TRAINING_DIGEST_SNAPSHOT_SYMBOL
            where SYMBOL = :P_SYMBOL
              and (:P_MARKET_TYPE is null or MARKET_TYPE = :P_MARKET_TYPE)
            limit 1
        );
    else
        v_symbols := (
            select SYMBOL, MARKET_TYPE
            from MIP.MART.V_TRAINING_DIGEST_SNAPSHOT_SYMBOL
            order by SNAPSHOT_JSON:maturity:score::float desc
            limit 10
        );
    end if;

    for rec in v_symbols do
        v_symbol := rec.SYMBOL;
        v_market_type := rec.MARKET_TYPE;
        v_cortex_succeeded := false;

        begin
            v_snapshot := (
                select SNAPSHOT_JSON
                from MIP.MART.V_TRAINING_DIGEST_SNAPSHOT_SYMBOL
                where SYMBOL = :v_symbol and MARKET_TYPE = :v_market_type
                limit 1
            );
        exception when other then
            v_snapshot := object_construct(
                'error', 'SNAPSHOT_VIEW_FAILED', 'message', :sqlerrm,
                'symbol', :v_symbol, 'market_type', :v_market_type
            );
        end;

        v_facts_hash := (select sha2(to_varchar(:v_snapshot), 256));

        -- MERGE symbol snapshot
        merge into MIP.AGENT_OUT.TRAINING_DIGEST_SNAPSHOT as target
        using (
            select
                'SYMBOL_TRAINING'::varchar    as scope,
                :v_symbol::varchar            as symbol,
                :v_market_type::varchar       as market_type,
                :v_as_of_ts::timestamp_ntz    as as_of_ts,
                :v_run_id::varchar            as run_id,
                :v_snapshot::variant           as snapshot_json,
                :v_facts_hash::varchar         as source_facts_hash
        ) as source
        on  target.SCOPE        = source.scope
        and target.SYMBOL       = source.symbol
        and target.MARKET_TYPE  = source.market_type
        and target.AS_OF_TS     = source.as_of_ts
        and target.RUN_ID       = source.run_id
        when matched then update set
            target.SNAPSHOT_JSON     = source.snapshot_json,
            target.SOURCE_FACTS_HASH = source.source_facts_hash
        when not matched then insert (
            SCOPE, SYMBOL, MARKET_TYPE, AS_OF_TS, RUN_ID, SNAPSHOT_JSON, SOURCE_FACTS_HASH, CREATED_AT
        ) values (
            source.scope, source.symbol, source.market_type, source.as_of_ts, source.run_id,
            source.snapshot_json, source.source_facts_hash, current_timestamp()
        );
        v_snapshot_count := :v_snapshot_count + 1;

        -- Prior symbol snapshot
        begin
            v_prior_snapshot := (
                select SNAPSHOT_JSON
                from MIP.AGENT_OUT.TRAINING_DIGEST_SNAPSHOT
                where SCOPE = 'SYMBOL_TRAINING'
                  and SYMBOL = :v_symbol and MARKET_TYPE = :v_market_type
                  and (AS_OF_TS < :v_as_of_ts or (AS_OF_TS = :v_as_of_ts and RUN_ID != :v_run_id))
                order by AS_OF_TS desc limit 1
            );
        exception when other then v_prior_snapshot := null;
        end;

        -- Build per-symbol Cortex prompt
        v_cortex_prompt :=
'You are a training coach for MIP (Market Intelligence Platform), analysing the training journey of symbol ' || :v_symbol || ' (' || :v_market_type || '). ' ||
'Explain in plain language where this symbol is on the path from "no data" to "trade-eligible". ' ||
'You MUST only reference numbers and facts present in the snapshot data below. ' ||
'Do NOT invent facts, propose trades, or suggest training parameter changes. ' ||
'
CURRENT SYMBOL TRAINING SNAPSHOT:
' || to_varchar(:v_snapshot) || '

PRIOR SYMBOL SNAPSHOT:
' || coalesce(to_varchar(:v_prior_snapshot), 'No prior snapshot available (first run).') || '

Produce a JSON object with exactly these keys:
{
  "headline": "One sentence: where is ' || :v_symbol || ' on the training journey today",
  "what_changed": ["bullet 1", "bullet 2", ...],
  "what_matters": ["bullet 1", "bullet 2", ...],
  "waiting_for": ["bullet 1", "bullet 2", ...],
  "where_to_look": [{"label": "page name", "route": "/path"}, ...],
  "journey": ["step 1", "step 2", "step 3", "step 4"]
}

Narrative quality rules (must follow):
- Explain metrics like you are talking to a non-expert.
- Whenever you mention a metric (maturity, coverage, hit rate, signals gap, avg return), UNPACK it:
  (a) what it means,
  (b) what numbers behind it (e.g. X of Y, today vs threshold),
  (c) the "so what" implication.
  Example: "Hit rate is 0.52 (52% of evaluated outcomes were profitable) — just below the 0.55 threshold needed for trust. Gap: 0.03."
- Avoid vague bullets. Prefer: "Because <fact>, <implication>. Today: <number>."
- waiting_for bullets must cite specific thresholds: "Waiting for hit_rate >= 0.55 (today: 0.52, gap: 0.03)."
- Use the threshold_gaps section to show exactly what is met and what is not.

Rules:
- headline: 1 sentence about this specific symbol. Reference its maturity stage and score.
- what_changed: 2-4 bullets about changes since prior snapshot. If first run, explain current state.
- what_matters: 2-3 bullets. what_matters[0] MUST explain "why not confident yet" or "why confident" using threshold_gaps.
- waiting_for: 1-3 bullets with specific threshold gaps from snapshot.
- where_to_look: 2-3 links. Valid routes: /training, /signals, /training?symbol=' || :v_symbol || '&market_type=' || :v_market_type || ', /market-timeline
- journey: MUST be exactly 4 items: "Collecting evidence", "Evaluating outcomes", "Earning trust", "Trade-eligible". Mark the CURRENT stage with ">>" prefix. Example: ["Collecting evidence", ">> Evaluating outcomes", "Earning trust", "Trade-eligible"]
- Every number you mention MUST appear in the snapshot data.
- Return ONLY the raw JSON object. Do NOT wrap it in markdown fences or any extra text. Start your response with { and end with }.';

        begin
            v_narrative_text := (select snowflake.cortex.complete(:v_model_name, :v_cortex_prompt));
            v_cortex_succeeded := true;
        exception when other then
            v_cortex_succeeded := false;
        end;

        -- Parse or fallback
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
            exception when other then
                v_narrative_json := object_construct(
                    'headline', :v_symbol || ' training digest — could not parse AI response.',
                    'what_changed', array_construct(:v_narrative_text),
                    'what_matters', array_construct(),
                    'waiting_for', array_construct(),
                    'where_to_look', array_construct(),
                    'journey', array_construct()
                );
            end;
        else
            -- Deterministic fallback for symbol
            v_narrative_text := 'No AI narrative available; showing deterministic training summary.';
            v_narrative_json := object_construct(
                'headline', :v_symbol || ' — ' || coalesce(:v_snapshot:maturity:stage::string, '?') ||
                    ' (score ' || coalesce(:v_snapshot:maturity:score::string, '?') || '/100)',
                'what_changed', array_construct(
                    'Recommendations: ' || coalesce(:v_snapshot:evidence:recs_total::string, '0') ||
                    ', Outcomes: ' || coalesce(:v_snapshot:evidence:outcomes_total::string, '0') ||
                    ', Coverage: ' || coalesce(:v_snapshot:evidence:coverage_ratio::string, '0')
                ),
                'what_matters', array_construct(
                    'Signals threshold: ' || iff(:v_snapshot:threshold_gaps:signals_met::boolean,
                        'MET (' || :v_snapshot:evidence:recs_total::string || ' >= ' || :v_snapshot:threshold_gaps:min_signals::string || ')',
                        'NOT MET (need ' || :v_snapshot:threshold_gaps:signals_gap::string || ' more)')
                ),
                'waiting_for', array_construct(
                    iff(:v_snapshot:threshold_gaps:signals_met::boolean,
                        'Waiting for hit_rate >= ' || :v_snapshot:threshold_gaps:min_hit_rate::string ||
                        ' (today: ' || coalesce(:v_snapshot:evidence:hit_rate::string, '?') || ')',
                        'Waiting for ' || :v_snapshot:threshold_gaps:signals_gap::string || ' more signal recommendations')
                ),
                'where_to_look', array_construct(
                    object_construct('label', 'Training Status', 'route', '/training'),
                    object_construct('label', 'View ' || :v_symbol, 'route',
                        '/training?symbol=' || :v_symbol || '&market_type=' || :v_market_type)
                ),
                'journey', array_construct(
                    iff(:v_snapshot:maturity:stage::string = 'INSUFFICIENT', '>> Collecting evidence', 'Collecting evidence'),
                    iff(:v_snapshot:maturity:stage::string = 'WARMING_UP', '>> Evaluating outcomes', 'Evaluating outcomes'),
                    iff(:v_snapshot:maturity:stage::string = 'LEARNING', '>> Earning trust', 'Earning trust'),
                    iff(:v_snapshot:maturity:stage::string = 'CONFIDENT', '>> Trade-eligible', 'Trade-eligible')
                )
            );
            v_model_name := 'DETERMINISTIC_FALLBACK';
        end if;

        -- MERGE symbol narrative
        merge into MIP.AGENT_OUT.TRAINING_DIGEST_NARRATIVE as target
        using (
            select
                'SYMBOL_TRAINING'::varchar       as scope,
                :v_symbol::varchar               as symbol,
                :v_market_type::varchar          as market_type,
                :v_as_of_ts::timestamp_ntz       as as_of_ts,
                :v_run_id::varchar               as run_id,
                :v_agent_name::varchar           as agent_name,
                :v_narrative_text::string         as narrative_text,
                :v_narrative_json::variant        as narrative_json,
                :v_model_name::varchar            as model_info,
                :v_facts_hash::varchar            as source_facts_hash
        ) as source
        on  target.SCOPE        = source.scope
        and target.SYMBOL       = source.symbol
        and target.MARKET_TYPE  = source.market_type
        and target.AS_OF_TS     = source.as_of_ts
        and target.RUN_ID       = source.run_id
        and target.AGENT_NAME   = source.agent_name
        when matched then update set
            target.NARRATIVE_TEXT     = source.narrative_text,
            target.NARRATIVE_JSON    = source.narrative_json,
            target.MODEL_INFO        = source.model_info,
            target.SOURCE_FACTS_HASH = source.source_facts_hash
        when not matched then insert (
            SCOPE, SYMBOL, MARKET_TYPE, AS_OF_TS, RUN_ID, AGENT_NAME,
            NARRATIVE_TEXT, NARRATIVE_JSON, MODEL_INFO, SOURCE_FACTS_HASH, CREATED_AT
        ) values (
            source.scope, source.symbol, source.market_type, source.as_of_ts, source.run_id,
            source.agent_name, source.narrative_text, source.narrative_json, source.model_info,
            source.source_facts_hash, current_timestamp()
        );
        v_narrative_count := :v_narrative_count + 1;
        v_model_name := 'mistral-large2';  -- Reset for next iteration

        v_results := array_append(:v_results, object_construct(
            'scope', 'SYMBOL_TRAINING',
            'symbol', :v_symbol,
            'market_type', :v_market_type,
            'facts_hash', :v_facts_hash,
            'cortex_succeeded', :v_cortex_succeeded,
            'model_info', :v_model_name
        ));
    end for;

    -- ════════════════════════════════════════════════════════════
    -- AUDIT LOG
    -- ════════════════════════════════════════════════════════════
    call MIP.APP.SP_LOG_EVENT(
        'AGENT',
        'SP_AGENT_GENERATE_TRAINING_DIGEST',
        'SUCCESS',
        :v_snapshot_count,
        object_construct(
            'run_id', :v_run_id,
            'as_of_ts', :v_as_of_ts,
            'snapshot_count', :v_snapshot_count,
            'narrative_count', :v_narrative_count,
            'results', :v_results
        ),
        null,
        :v_run_id,
        null
    );

    return object_construct(
        'status', 'SUCCESS',
        'snapshot_count', :v_snapshot_count,
        'narrative_count', :v_narrative_count,
        'results', :v_results
    );
exception
    when other then
        call MIP.APP.SP_LOG_EVENT(
            'AGENT', 'SP_AGENT_GENERATE_TRAINING_DIGEST', 'FAIL',
            :v_snapshot_count,
            object_construct('run_id', :v_run_id, 'as_of_ts', :v_as_of_ts,
                'snapshot_count', :v_snapshot_count, 'narrative_count', :v_narrative_count),
            :sqlerrm, :v_run_id, null
        );
        return object_construct(
            'status', 'ERROR', 'error_message', :sqlerrm,
            'snapshot_count', :v_snapshot_count, 'narrative_count', :v_narrative_count
        );
end;
$$;
