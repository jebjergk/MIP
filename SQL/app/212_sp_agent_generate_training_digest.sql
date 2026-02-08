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

IMPORTANT CONTEXT — The training journey explained:
MIP tracks symbols through a training journey before they become eligible for real trading. The stages are:
1. INSUFFICIENT (score < 25): Not enough signal data yet. The system is collecting initial evidence. No trading is possible. User implication: "We are still gathering data — nothing to act on yet."
2. WARMING_UP (25-49): Some data exists but outcomes (did the signal predict correctly?) have not been fully evaluated. User implication: "Early days — we can see signals but cannot yet judge if they are good."
3. LEARNING (50-74): Enough data to start judging quality. The system is comparing hit rates, returns, and coverage against thresholds. This is where symbols get CLOSE to being trade-eligible but may fail on one or more criteria. User implication: "We have evidence. The question is: is it good enough? Check the threshold gaps."
4. CONFIDENT (75+): Strong evidence — the symbol passes training thresholds and CAN be traded. But CONFIDENT does not automatically mean it IS being traded: it must also be TRUSTED by the trust policy AND the portfolio must have capacity (open slots). User implication: "This symbol has proven itself in training. If it is also TRUSTED and the portfolio has room, it will generate trade proposals."

KEY DISTINCTION you MUST explain to users:
- CONFIDENT (training maturity) means "enough quality evidence" — it is about data completeness and outcomes.
- TRUSTED (trust policy) means "the pattern passes return/coverage/hit-rate rules" — it is about performance quality.
- A symbol can be CONFIDENT but NOT TRUSTED (good sample, poor returns). It can be TRUSTED but NOT CONFIDENT (good returns on thin data — risky).
- Only symbols that are BOTH CONFIDENT AND TRUSTED become eligible for trade proposals.
- Even then, the portfolio must have capacity (remaining position slots) for a trade to actually happen.

CRITICAL RULE — NEVER CONTRADICT TRUST LABELS:
- The trust_label for each symbol (TRUSTED / WATCH / UNTRUSTED) is computed by the trust policy engine and is the DEFINITIVE answer on whether a symbol is trusted.
- Training gate thresholds (signals count, hit rate, avg return targets) are a SEPARATE system from the trust policy. They can disagree.
- If the snapshot says a symbol is TRUSTED, it IS trusted — do NOT say "not eligible" or "still needs to earn trust" even if some training thresholds appear unmet. Those gaps are about training completeness, not about the trust verdict.
- If a symbol is listed as UNTRUSTED or WATCH, it is NOT trusted — even if individual metrics look good.
- When naming specific symbols (from near_miss_symbols, top_confident_symbols), always state their trust label consistently. NEVER say "eligible" and "not eligible" about the same symbol.
- Example: If a symbol is CONFIDENT (score 86) and TRUSTED: say "eligible for trading — the trust policy has approved this symbol despite training signals still building up."

Narrative quality rules (must follow):
- Write as if talking to a portfolio manager who is NOT a data scientist. Explain what things MEAN, not just what the numbers are.
- Every bullet should answer "so what does this mean for me?" — connect facts to user impact.
- Whenever you mention a metric or label (e.g. maturity score, coverage, hit rate, stage, TRUSTED/WATCH/UNTRUSTED), you MUST:
  (a) explain what it means in plain language,
  (b) show the numbers behind it (e.g. X of Y, counts, deltas),
  (c) state the practical consequence for trading ("so what").
  Example: "12 of 25 symbols are still INSUFFICIENT (maturity < 25), meaning the system does not have enough signal history to judge them yet — no trading decisions can be based on these symbols until more data arrives."
- When a detector fires, do NOT just name the detector. Explain what it means: "Training has stalled (0 new outcomes since last snapshot). This means the system is not learning anything new — either no new market bars arrived, or the evaluation pipeline has not run. Until new outcomes appear, no symbol will advance in training."
- For "no change" situations, always explain WHY nothing changed AND what the consequence is: "Nothing changed because no new recommendations or outcomes were generated. This means the training picture is frozen — no symbol can advance stages or earn trust until fresh data flows in."
- waiting_for bullets must be specific AND explain what happens when the threshold is reached: "Waiting for 50 more signal recommendations (today: 232, need: 282 to reach confident threshold) — once reached, these symbols will have enough evidence to be scored for trust eligibility."
- Prefer longer, explanatory bullets over short telegraphic ones. 2-3 sentences per bullet is fine.

Rules:
- headline: 1-2 sentences. Reference concrete numbers AND state the practical meaning. Example: "34 symbols in training — 12 are actively learning but none advanced today because no new outcomes arrived."
- what_changed: 3-5 bullets. Each bullet MUST explain the change AND its consequence for the user. Name specific symbols from near_miss_symbols or top_confident_symbols. If nothing changed, explain why (stalled evaluation, no new bars, weekend) and what this means.
- what_matters: 3-5 bullets. what_matters[0] MUST explain the overall training-to-trading pipeline using stage counts: how many symbols at each stage, how many are TRUSTED, and what this means for the user in terms of trade readiness. Include at least one bullet about the gap between CONFIDENT and TRUSTED counts (if they differ, explain why). Include at least one bullet about near-miss symbols (who is close and what they need).
- waiting_for: 2-4 bullets. Each must cite a specific threshold AND explain what happens when it is reached ("Once X reaches Y, it becomes eligible for...").
- where_to_look: 2-4 links. Valid routes: /training, /signals, /market-timeline, /digest, /brief
- journey: MUST be exactly 4 items using stage counts: "Collecting evidence (N symbols)", "Evaluating outcomes (N symbols)", "Earning trust (N symbols)", "Trade-eligible (N symbols)".
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
    -- If P_SYMBOL is specified, do just that one; otherwise ALL symbols in training
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
            select distinct SYMBOL, MARKET_TYPE
            from MIP.MART.V_TRAINING_DIGEST_SNAPSHOT_SYMBOL
            order by SYMBOL
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
  "headline": "One sentence: where is ' || :v_symbol || ' on the training journey today and what it means for trading",
  "what_changed": ["bullet 1", "bullet 2", ...],
  "what_matters": ["bullet 1", "bullet 2", ...],
  "waiting_for": ["bullet 1", "bullet 2", ...],
  "where_to_look": [{"label": "page name", "route": "/path"}, ...],
  "journey": ["step 1", "step 2", "step 3", "step 4"]
}

IMPORTANT CONTEXT — What the training stages mean for THIS symbol:
The training journey determines whether ' || :v_symbol || ' can generate real trade proposals. The stages are:
1. INSUFFICIENT (score < 25): Not enough signal data. The system needs more recommendations before it can even begin evaluating. User impact: "We cannot judge this symbol yet — it is too early."
2. WARMING_UP (25-49): Some data exists but outcome evaluations are incomplete. User impact: "We have started collecting evidence but do not have enough evaluated outcomes to judge quality."
3. LEARNING (50-74): Enough data to measure quality. The system is checking hit rate (% of correct predictions), average return, and coverage against thresholds. User impact: "We are actively judging this symbol. Check the threshold gaps to see what is still missing."
4. CONFIDENT (75+): Training data is strong. BUT this does NOT automatically mean the symbol IS being traded. To actually trade, it must ALSO be TRUSTED (pass performance thresholds: hit_rate >= min_hit_rate, avg_return >= min_avg_return, enough coverage) AND the portfolio must have available capacity (open position slots).

KEY DISTINCTION you MUST explain clearly:
- CONFIDENT = "enough quality evidence" (data completeness). Check maturity score.
- TRUSTED = "pattern passes performance rules" (return quality). Check trust.trust_label.
- ' || :v_symbol || ' can ONLY generate trade proposals if it is BOTH CONFIDENT AND TRUSTED.
- If trust_label is WATCH: the symbol is being monitored but is NOT eligible for trading yet.
- If trust_label is UNTRUSTED: the symbol fails trust criteria — no trading.
- If trust_label is TRUSTED: the symbol passes performance rules and CAN trade (if portfolio has capacity).

CRITICAL RULE — NEVER CONTRADICT THE trust_label FIELD:
- The trust.trust_label value is the DEFINITIVE answer to "is this symbol trusted?". It is computed by the trust policy engine.
- The threshold_gaps section shows training gate thresholds which are a SEPARATE system from the trust policy. They may disagree.
- If trust_label is TRUSTED, the symbol IS trusted — do NOT say "not trusted" or "not eligible" even if some threshold_gaps show unmet criteria. Those gaps relate to training maturity, not to the trust decision.
- If trust_label is UNTRUSTED or WATCH, the symbol is NOT trusted — even if individual metrics look good.
- In your headline and bullets: READ trust.trust_label FIRST, then describe threshold_gaps as additional context about training completeness.
- Example: If trust_label=TRUSTED but signals_met=false, say: "AUD/USD is TRUSTED and eligible for trading. While the training signals count (22) has not yet reached the full training target (40), the trust policy has already approved this symbol based on its strong hit rate and returns."
- NEVER write one sentence saying "eligible" and another saying "not eligible" for the same symbol. Pick the truth from trust_label and be consistent.

Narrative quality rules (must follow):
- Write as if talking to a portfolio manager who wants to know: "Can I trade this symbol? If not, why not? What needs to happen?"
- Every bullet should answer "so what does this mean for me?" — connect facts to trading readiness.
- Whenever you mention a metric (maturity, coverage, hit rate, signals gap, avg return, trust label), UNPACK it:
  (a) what it means in plain language,
  (b) the actual numbers (today vs threshold, gap),
  (c) the practical consequence for trading.
  Example: "Hit rate is 0.52 (52% of evaluated outcomes were profitable). The system requires at least 0.55 (55%) to trust this symbol for trading. Gap: 0.03 — roughly 2 more successful outcomes out of the next 10 would close this gap and make ' || :v_symbol || ' eligible for trade proposals."
- Do NOT just list detector names. Explain what they mean for the user.
- FIRST read trust.trust_label from the snapshot. This is the definitive answer. Then:
  - If trust_label is TRUSTED: "' || :v_symbol || ' is TRUSTED and eligible for trade proposals. It has passed the performance checks. Whether trades actually happen depends on portfolio capacity and signal strength." Then optionally mention any remaining threshold_gaps as training completeness context (not as blockers).
  - If trust_label is WATCH: "' || :v_symbol || ' is on the WATCH list — being monitored but NOT yet eligible for trading. Specifically: [cite the unmet threshold_gaps]. Once these are met, it may earn trust."
  - If trust_label is UNTRUSTED: "' || :v_symbol || ' is UNTRUSTED — it does not pass performance rules. Specifically: [cite unmet gaps]. No trade proposals will be generated until trust is earned."
- NEVER contradict yourself. If trust_label says TRUSTED, every sentence must be consistent with "this symbol CAN trade."
- Prefer longer, explanatory bullets (2-3 sentences each). Users need reasoning, not just numbers.
- waiting_for bullets must state the threshold, today''s value, the gap, AND what happens when met.
  - If the symbol is NOT trusted: "Waiting for hit_rate to reach 0.55 (today: 0.52, gap: 0.03). Once met, ' || :v_symbol || ' will be eligible for trust status, unlocking trade proposals."
  - If the symbol IS trusted: waiting_for should focus on remaining training milestones (e.g. signals count towards full maturity) or portfolio capacity, NOT on earning trust (since trust is already granted). Example: "Waiting for signals to reach 40 (today: 22, gap: 18). This is a training completeness target — ' || :v_symbol || ' is already TRUSTED and can trade while this builds up."

Rules:
- headline: 1-2 sentences. State the maturity stage, score, trust_label, and what it means for trading. Base the trading eligibility statement ONLY on trust.trust_label — never guess from threshold_gaps. Examples: If TRUSTED: "' || :v_symbol || ' is CONFIDENT (score 86/100) and TRUSTED — eligible for trade proposals." If WATCH: "' || :v_symbol || ' is LEARNING (score 62/100) and on WATCH — building evidence but not yet eligible for trading."
- what_changed: 2-4 bullets. Each explains a change AND its consequence. If first run, explain current state thoroughly. If nothing changed, explain why AND what it means.
- what_matters: 3-4 bullets. what_matters[0] MUST START with the trust_label verdict and be consistent with it throughout. If TRUSTED: "' || :v_symbol || ' is TRUSTED and eligible for trading. Here is where each metric stands: [list each threshold]. All required performance checks passed." If NOT trusted: "' || :v_symbol || ' is [trust_label] and NOT yet eligible. Here is where each metric stands: [list thresholds, highlighting unmet ones]." Include at least one bullet explaining WHAT needs to happen next.
- waiting_for: 2-3 bullets. Each cites threshold, gap, AND what happens when reached.
- where_to_look: 2-3 links. Valid routes: /training, /signals, /training?symbol=' || :v_symbol || '&market_type=' || :v_market_type || ', /market-timeline
- journey: MUST be exactly 4 items: "Collecting evidence", "Evaluating outcomes", "Earning trust", "Trade-eligible". Mark the CURRENT stage with ">>" prefix.
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
