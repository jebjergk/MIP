-- 193_sp_agent_generate_morning_brief.sql
-- Purpose: Read-only agent morning brief: build BRIEF_JSON from MART views, write to AGENT_OUT.AGENT_MORNING_BRIEF.
-- Deterministic for given as_of_ts + signal_run_id; no external APIs; one audit event; upsert by (as_of_ts, signal_run_id, agent_name).
-- Note: Avoid SELECT...INTO in Snowflake procedures; use := (SELECT ...) or RESULTSET + FOR loop. See MIP/docs/SNOWFLAKE_SQL_LIMITATIONS.md.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_AGENT_GENERATE_MORNING_BRIEF(
    P_AS_OF_TS      timestamp_ntz,
    P_SIGNAL_RUN_ID number
)
returns variant
language sql
execute as caller
as
$$
declare
    v_agent_name           string := 'AGENT_V0_MORNING_BRIEF';
    v_status               string := 'SUCCESS';
    v_min_n_signals        number := 20;
    v_top_n_patterns       number := 5;
    v_top_n_candidates     number := 5;
    v_ranking_formula      string := 'HIT_RATE_SUCCESS * AVG_RETURN_SUCCESS';
    v_ranking_formula_type string := 'HIT_RATE_AVG_RETURN';
    v_enabled              boolean := true;
    v_has_new_bars         boolean := false;
    v_latest_market_ts     timestamp_ntz;
    v_entries_allowed      boolean := true;
    v_stop_reason          string := null;
    v_training_top5        variant;
    v_candidate_top5       variant;
    v_candidate_reason     string := null;
    v_training_fallback    boolean := false;
    v_candidate_diagnostics variant := null;
    v_header               variant;
    v_system_status        variant;
    v_training_summary     variant;
    v_candidate_summary    variant;
    v_assumptions          variant;
    v_rationale_templates  array := array_construct();
    v_interpretation_bullets array := array_construct();
    v_data_lineage         variant;
    v_brief_json           variant;
    v_brief_id             number;
    v_generated_at         timestamp_ntz := current_timestamp();
    v_training_rs          resultset;
    v_candidate_rs         resultset;
    v_config_rs            resultset;
    v_training_count       number := 0;
    v_candidate_count      number := 0;
    v_effective_min_signals number;
    v_run_id                string;
    v_audit_details         variant;
    v_inputs_json           variant;
    v_outputs_json          variant;
begin
    -- Load config from MIP.APP.AGENT_CONFIG for AGENT_V0_MORNING_BRIEF
    v_config_rs := (select MIN_N_SIGNALS, TOP_N_PATTERNS, TOP_N_CANDIDATES, RANKING_FORMULA, RANKING_FORMULA_TYPE, ENABLED from MIP.APP.AGENT_CONFIG where AGENT_NAME = :v_agent_name limit 1);
    for rec in v_config_rs do
        v_min_n_signals := rec.MIN_N_SIGNALS;
        v_top_n_patterns := rec.TOP_N_PATTERNS;
        v_top_n_candidates := rec.TOP_N_CANDIDATES;
        v_ranking_formula := rec.RANKING_FORMULA;
        v_ranking_formula_type := rec.RANKING_FORMULA_TYPE;
        v_enabled := rec.ENABLED;
        break;
    end for;
    if (:v_min_n_signals is null) then v_min_n_signals := 20; end if;
    if (:v_top_n_patterns is null) then v_top_n_patterns := 5; end if;
    if (:v_top_n_candidates is null) then v_top_n_candidates := 5; end if;
    if (:v_ranking_formula is null) then v_ranking_formula := 'HIT_RATE_SUCCESS * AVG_RETURN_SUCCESS'; end if;
    if (:v_ranking_formula_type is null) then v_ranking_formula_type := 'HIT_RATE_AVG_RETURN'; end if;
    if (:v_enabled is null) then v_enabled := true; end if;

    -- System status: latest market bars, entries allowed (aggregate from risk state)
    v_latest_market_ts := (select max(TS) from MIP.MART.MARKET_BARS);
    v_entries_allowed := (select coalesce(min(iff(not coalesce(ENTRIES_BLOCKED, false), true, false)), true) from MIP.MART.V_PORTFOLIO_RISK_STATE);
    v_stop_reason := (select max(STOP_REASON) from MIP.MART.V_PORTFOLIO_RISK_STATE);
    v_has_new_bars := (:v_latest_market_ts is not null);

    v_header := object_construct(
        'as_of_ts', :P_AS_OF_TS,
        'signal_run_id', :P_SIGNAL_RUN_ID,
        'generated_at', :v_generated_at
    );

    -- Training summary: top N from V_TRAINING_LEADERBOARD. If 0 rows, retry with relaxed min (1) and set fallback_used.
    v_effective_min_signals := :v_min_n_signals;
    v_training_rs := (
        select array_agg(obj) within group (order by rn) as agg
        from (
            select
                row_number() over (
                    order by
                        iff(:v_ranking_formula_type = 'SHARPE_LIKE', SHARPE_LIKE_SUCCESS, HIT_RATE_SUCCESS) desc nulls last,
                        iff(:v_ranking_formula_type = 'SHARPE_LIKE', HIT_RATE_SUCCESS, AVG_RETURN_SUCCESS) desc nulls last
                ) as rn,
                object_construct(
                    'pattern_id', PATTERN_ID,
                    'market_type', MARKET_TYPE,
                    'interval_minutes', INTERVAL_MINUTES,
                    'horizon_bars', HORIZON_BARS,
                    'n_signals', N_SIGNALS,
                    'n_success', N_SUCCESS,
                    'hit_rate_success', HIT_RATE_SUCCESS,
                    'avg_return_success', AVG_RETURN_SUCCESS,
                    'sharpe_like_success', SHARPE_LIKE_SUCCESS
                ) as obj
            from MIP.MART.V_TRAINING_LEADERBOARD
            where N_SIGNALS >= :v_effective_min_signals
            qualify rn <= :v_top_n_patterns
        ) t
    );
    for rec in v_training_rs do
        v_training_top5 := rec.agg;
        break;
    end for;
    v_training_summary := coalesce(:v_training_top5, array_construct());
    if (array_size(:v_training_summary) = 0) then
        v_effective_min_signals := 1;
        v_training_fallback := true;
        v_training_rs := (
            select array_agg(obj) within group (order by rn) as agg
            from (
                select
                    row_number() over (
                        order by
                            iff(:v_ranking_formula_type = 'SHARPE_LIKE', SHARPE_LIKE_SUCCESS, HIT_RATE_SUCCESS) desc nulls last,
                            iff(:v_ranking_formula_type = 'SHARPE_LIKE', HIT_RATE_SUCCESS, AVG_RETURN_SUCCESS) desc nulls last
                    ) as rn,
                    object_construct(
                        'pattern_id', PATTERN_ID,
                        'market_type', MARKET_TYPE,
                        'interval_minutes', INTERVAL_MINUTES,
                        'horizon_bars', HORIZON_BARS,
                        'n_signals', N_SIGNALS,
                        'n_success', N_SUCCESS,
                        'hit_rate_success', HIT_RATE_SUCCESS,
                        'avg_return_success', AVG_RETURN_SUCCESS,
                        'sharpe_like_success', SHARPE_LIKE_SUCCESS
                    ) as obj
                from MIP.MART.V_TRAINING_LEADERBOARD
                where N_SIGNALS >= :v_effective_min_signals
                qualify rn <= :v_top_n_patterns
            ) t
        );
        for rec in v_training_rs do
            v_training_top5 := rec.agg;
            break;
        end for;
        v_training_summary := coalesce(:v_training_top5, array_construct());
    end if;
    v_training_count := array_size(:v_training_summary);

    v_system_status := object_construct(
        'as_of_ts', :P_AS_OF_TS,
        'signal_run_id', :P_SIGNAL_RUN_ID,
        'generated_at', :v_generated_at,
        'has_new_bars', :v_has_new_bars,
        'latest_market_bars_ts', :v_latest_market_ts,
        'entries_allowed', :v_entries_allowed,
        'stop_reason', :v_stop_reason,
        'source_views', array_construct('MIP.MART.V_TRAINING_LEADERBOARD', 'MIP.MART.V_TRUSTED_SIGNALS_LATEST_TS', 'MIP.MART.V_PORTFOLIO_RISK_STATE', 'MIP.MART.MARKET_BARS'),
        'filters', object_construct('min_n_signals', :v_min_n_signals, 'top_n_patterns', :v_top_n_patterns, 'top_n_candidates', :v_top_n_candidates),
        'determinism_key', object_construct('as_of_ts', :P_AS_OF_TS, 'signal_run_id', :P_SIGNAL_RUN_ID, 'agent_name', :v_agent_name),
        'fallback_used', :v_training_fallback
    );

    -- Candidate summary: top N from V_TRUSTED_SIGNALS_LATEST_TS (this run). Rank by formula type; if 0, emit empty list + diagnostics.
    v_candidate_rs := (
        select array_agg(obj) within group (order by rn) as agg
        from (
            select
                rn,
                object_construct(
                    'recommendation_id', RECOMMENDATION_ID,
                    'pattern_id', PATTERN_ID,
                    'symbol', SYMBOL,
                    'market_type', MARKET_TYPE,
                    'interval_minutes', INTERVAL_MINUTES,
                    'horizon_bars', HORIZON_BARS,
                    'score', SCORE,
                    'n_signals', N_SIGNALS,
                    'hit_rate_success', HIT_RATE_SUCCESS,
                    'avg_return_success', AVG_RETURN_SUCCESS,
                    'sharpe_like_success', SHARPE_LIKE_SUCCESS,
                    'ranking_score', RANKING_SCORE
                ) as obj
            from (
                select
                    RECOMMENDATION_ID,
                    PATTERN_ID,
                    SYMBOL,
                    MARKET_TYPE,
                    INTERVAL_MINUTES,
                    HORIZON_BARS,
                    SCORE,
                    N_SIGNALS,
                    HIT_RATE_SUCCESS,
                    AVG_RETURN_SUCCESS,
                    SHARPE_LIKE_SUCCESS,
                    iff(:v_ranking_formula_type = 'SHARPE_LIKE',
                        coalesce(SHARPE_LIKE_SUCCESS, -999),
                        coalesce(HIT_RATE_SUCCESS, 0) * coalesce(AVG_RETURN_SUCCESS, 0)) as RANKING_SCORE,
                    row_number() over (
                        order by
                            iff(:v_ranking_formula_type = 'SHARPE_LIKE', SHARPE_LIKE_SUCCESS, (coalesce(HIT_RATE_SUCCESS, 0) * coalesce(AVG_RETURN_SUCCESS, 0))) desc nulls last,
                            SCORE desc nulls last,
                            RECOMMENDATION_ID
                    ) as rn
                from MIP.MART.V_TRUSTED_SIGNALS_LATEST_TS
                where (try_to_number(replace(to_varchar(RUN_ID), 'T', '')) = :P_SIGNAL_RUN_ID
                       or to_varchar(P_SIGNAL_RUN_ID) = RUN_ID)
                  and coalesce(N_SIGNALS, 0) >= :v_min_n_signals
            ) ranked
            where rn <= :v_top_n_candidates
        ) sub
    );
    for rec in v_candidate_rs do
        v_candidate_top5 := rec.agg;
        break;
    end for;
    v_candidate_top5 := coalesce(:v_candidate_top5, array_construct());
    v_candidate_count := array_size(:v_candidate_top5);
    if (v_candidate_count = 0) then
        v_candidate_reason := (
            select case
                when not exists (
                    select 1 from MIP.MART.V_TRUSTED_SIGNALS_LATEST_TS
                    where (try_to_number(replace(to_varchar(RUN_ID), 'T', '')) = :P_SIGNAL_RUN_ID or to_varchar(P_SIGNAL_RUN_ID) = RUN_ID)
                ) then 'no_trusted_signals_at_latest_ts'
                else 'no_candidates_with_min_n_signals'
            end
        );
        v_candidate_diagnostics := object_construct(
            'as_of_ts', :P_AS_OF_TS,
            'signal_run_id', :P_SIGNAL_RUN_ID,
            'view_raw_count', (select count(*) from MIP.MART.V_TRUSTED_SIGNALS_LATEST_TS where try_to_number(replace(to_varchar(RUN_ID), 'T', '')) = :P_SIGNAL_RUN_ID or to_varchar(P_SIGNAL_RUN_ID) = RUN_ID),
            'reason', :v_candidate_reason
        );
    end if;
    -- Contract: always same keys (candidates array, reason, fallback_used, diagnostics)
    v_candidate_summary := object_construct(
        'candidates', :v_candidate_top5,
        'reason', :v_candidate_reason,
        'fallback_used', false,
        'diagnostics', :v_candidate_diagnostics
    );

    -- BONUS: Explainability; assumptions reflect formula type
    v_assumptions := object_construct(
        'min_n_signals', :v_min_n_signals,
        'ranking_formula', :v_ranking_formula,
        'ranking_formula_type', :v_ranking_formula_type,
        'horizons_considered', 'from V_TRUSTED_PATTERN_HORIZONS (training gate passed)'
    );
    v_rationale_templates := array_construct(
        'Top pattern/horizon by Sharpe-like (success-only).',
        'Paper candidates ranked by hit_rate_success * avg_return_success, min_n_signals guard.',
        'Entries allowed when no portfolio has entries_blocked.'
    );
    -- P4: Deterministic interpretation bullets (no LLM)
    v_interpretation_bullets := array_construct(
        'Training leaderboard: top ' || :v_top_n_patterns || ' patterns by ' || iff(:v_ranking_formula_type = 'SHARPE_LIKE', 'Sharpe-like', 'hit_rate * avg_return') || '.',
        'Candidates: up to ' || :v_top_n_candidates || ' paper candidates from trusted signals at latest TS, min_n_signals=' || :v_min_n_signals || '.',
        'Entries allowed when no portfolio has entries_blocked.'
    );
    v_data_lineage := object_construct(
        'source_views', array_construct(
            'MIP.MART.V_TRAINING_LEADERBOARD',
            'MIP.MART.V_TRUSTED_SIGNALS_LATEST_TS',
            'MIP.MART.V_PORTFOLIO_RISK_STATE',
            'MIP.MART.MARKET_BARS'
        ),
        'filters_applied', object_construct(
            'training_summary', 'top 5 by sharpe_like_success, hit_rate_success, avg_return_success',
            'candidate_summary', 'signal_run_id match, n_signals >= min_n_signals, top 5 by ranking_score'
        )
    );

    v_brief_json := object_construct(
        'header', :v_header,
        'system_status', :v_system_status,
        'training_summary', :v_training_summary,
        'candidate_summary', :v_candidate_summary,
        'assumptions', :v_assumptions,
        'rationale_templates', :v_rationale_templates,
        'interpretation_bullets', :v_interpretation_bullets,
        'data_lineage', :v_data_lineage
    );

    -- Upsert: MERGE by (AS_OF_TS, SIGNAL_RUN_ID, AGENT_NAME); match then update BRIEF_JSON/STATUS/CREATED_AT, else insert
    merge into MIP.AGENT_OUT.AGENT_MORNING_BRIEF t
    using (
        select
            :P_AS_OF_TS as AS_OF_TS,
            :P_SIGNAL_RUN_ID as SIGNAL_RUN_ID,
            :v_agent_name as AGENT_NAME,
            :v_status as STATUS,
            :v_brief_json as BRIEF_JSON,
            :v_generated_at as CREATED_AT
    ) s
    on t.AS_OF_TS = s.AS_OF_TS and t.SIGNAL_RUN_ID = s.SIGNAL_RUN_ID and t.AGENT_NAME = s.AGENT_NAME
    when matched then
        update set t.BRIEF_JSON = s.BRIEF_JSON, t.STATUS = s.STATUS, t.CREATED_AT = s.CREATED_AT
    when not matched then
        insert (AS_OF_TS, SIGNAL_RUN_ID, AGENT_NAME, STATUS, BRIEF_JSON, CREATED_AT)
        values (s.AS_OF_TS, s.SIGNAL_RUN_ID, s.AGENT_NAME, s.STATUS, s.BRIEF_JSON, s.CREATED_AT);

    v_brief_id := (
        select BRIEF_ID
        from MIP.AGENT_OUT.AGENT_MORNING_BRIEF
        where AS_OF_TS = :P_AS_OF_TS
          and SIGNAL_RUN_ID = :P_SIGNAL_RUN_ID
          and AGENT_NAME = :v_agent_name
        limit 1
    );

    -- One audit event (EVENT_TYPE='AGENT', EVENT_NAME='SP_AGENT_GENERATE_MORNING_BRIEF')
    v_run_id := (select uuid_string());
    v_audit_details := object_construct(
        'as_of_ts', :P_AS_OF_TS,
        'signal_run_id', :P_SIGNAL_RUN_ID,
        'agent_name', :v_agent_name,
        'brief_id', :v_brief_id
    );
    insert into MIP.APP.MIP_AUDIT_LOG (
        EVENT_TS,
        RUN_ID,
        PARENT_RUN_ID,
        EVENT_TYPE,
        EVENT_NAME,
        STATUS,
        ROWS_AFFECTED,
        DETAILS
    )
    values (
        current_timestamp(),
        :v_run_id,
        null,
        'AGENT',
        'SP_AGENT_GENERATE_MORNING_BRIEF',
        :v_status,
        1,
        :v_audit_details
    );

    -- P1: AGENT_RUN_LOG with rowcounts for observability (success path)
    begin
        v_run_id := (select uuid_string());
        v_inputs_json := object_construct('as_of_ts', :P_AS_OF_TS, 'signal_run_id', :P_SIGNAL_RUN_ID);
        v_outputs_json := object_construct('training_count', :v_training_count, 'candidate_count', :v_candidate_count, 'brief_id', :v_brief_id);
        insert into MIP.AGENT_OUT.AGENT_RUN_LOG (
            RUN_ID, AGENT_NAME, AS_OF_TS, SIGNAL_RUN_ID, STATUS, INPUTS_JSON, OUTPUTS_JSON, CREATED_AT
        )
        values (
            :v_run_id,
            :v_agent_name,
            :P_AS_OF_TS,
            :P_SIGNAL_RUN_ID,
            'SUCCESS',
            :v_inputs_json,
            :v_outputs_json,
            current_timestamp()
        );
    exception
        when other then
            null;
    end;

    -- Return BRIEF_JSON (variant) per spec
    return :v_brief_json;
exception
    when other then
        v_status := 'ERROR';
        v_run_id := (select uuid_string());
        v_audit_details := object_construct('as_of_ts', :P_AS_OF_TS, 'signal_run_id', :P_SIGNAL_RUN_ID, 'agent_name', :v_agent_name);
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
        values (
            current_timestamp(),
            :v_run_id,
            null,
            'AGENT',
            'SP_AGENT_GENERATE_MORNING_BRIEF',
            :v_status,
            0,
            :v_audit_details,
            :sqlerrm
        );
        -- Optionally write AGENT_RUN_LOG if table exists (run in same proc; ignore errors on insert)
        begin
            v_run_id := (select uuid_string());
            v_inputs_json := object_construct('as_of_ts', :P_AS_OF_TS, 'signal_run_id', :P_SIGNAL_RUN_ID);
            insert into MIP.AGENT_OUT.AGENT_RUN_LOG (
                RUN_ID, AGENT_NAME, AS_OF_TS, SIGNAL_RUN_ID, STATUS, INPUTS_JSON, OUTPUTS_JSON, ERROR_MESSAGE, CREATED_AT
            )
            values (
                :v_run_id,
                :v_agent_name,
                :P_AS_OF_TS,
                :P_SIGNAL_RUN_ID,
                'ERROR',
                :v_inputs_json,
                null,
                :sqlerrm,
                current_timestamp()
            );
        exception
            when other then
                null;
        end;
        return object_construct('status', 'ERROR', 'error_message', :sqlerrm, 'as_of_ts', :P_AS_OF_TS, 'signal_run_id', :P_SIGNAL_RUN_ID);
end;
$$;
