-- 193_sp_agent_generate_morning_brief.sql
-- Purpose: Read-only agent morning brief: build BRIEF_JSON from MART views, write to AGENT_OUT.AGENT_MORNING_BRIEF.
-- Deterministic for given as_of_ts + signal_run_id; no external APIs; one audit event; upsert by (as_of_ts, signal_run_id, agent_name).

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
    v_agent_name        string := 'AGENT_V0_MORNING_BRIEF';
    v_status           string := 'SUCCESS';
    v_min_n_signals     number := 20;
    v_has_new_bars      boolean := false;
    v_latest_market_ts  timestamp_ntz;
    v_entries_allowed   boolean := true;
    v_stop_reason       string := null;
    v_training_top5     variant;
    v_candidate_top5    variant;
    v_candidate_reason  string := null;
    v_header            variant;
    v_system_status     variant;
    v_training_summary  variant;
    v_candidate_summary variant;
    v_assumptions       variant;
    v_rationale_templates array := array_construct();
    v_data_lineage      variant;
    v_brief_json        variant;
    v_brief_id          number;
    v_generated_at      timestamp_ntz := current_timestamp();
begin
    -- Configurable min_n_signals for candidate_summary (default 20)
    select coalesce(try_to_number(CONFIG_VALUE), 20)
      into :v_min_n_signals
      from MIP.APP.APP_CONFIG
     where CONFIG_KEY = 'AGENT_BRIEF_MIN_N_SIGNALS'
     limit 1;
    if (:v_min_n_signals is null) then
        v_min_n_signals := 20;
    end if;

    -- System status: latest market bars, entries allowed (aggregate from risk state)
    select max(TS) into :v_latest_market_ts from MIP.MART.MARKET_BARS;
    select
        coalesce(min(iff(not coalesce(ENTRIES_BLOCKED, false), true, false)), true),
        max(STOP_REASON)
      into :v_entries_allowed,
           :v_stop_reason
      from MIP.MART.V_PORTFOLIO_RISK_STATE;
    v_has_new_bars := (:v_latest_market_ts is not null);

    v_header := object_construct(
        'as_of_ts', :P_AS_OF_TS,
        'signal_run_id', :P_SIGNAL_RUN_ID,
        'generated_at', :v_generated_at
    );

    v_system_status := object_construct(
        'has_new_bars', :v_has_new_bars,
        'latest_market_bars_ts', :v_latest_market_ts,
        'entries_allowed', :v_entries_allowed,
        'stop_reason', :v_stop_reason
    );

    -- Training summary: top 5 from V_TRAINING_LEADERBOARD
    select array_agg(obj) within group (order by rn)
      into :v_training_top5
      from (
        select
            row_number() over (order by SHARPE_LIKE_SUCCESS desc nulls last, HIT_RATE_SUCCESS desc nulls last, AVG_RETURN_SUCCESS desc nulls last) as rn,
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
        qualify rn <= 5
      ) t;
    v_training_summary := coalesce(:v_training_top5, array_construct());

    -- Candidate summary: top 5 "paper candidates" from V_TRUSTED_SIGNALS_LATEST_TS (this run)
    -- Rank by (hit_rate_success * avg_return_success), guard n_signals >= v_min_n_signals
    -- Subquery required: Snowflake rejects SELECT...INTO when statement starts with WITH
    select array_agg(obj) within group (order by rn)
      into :v_candidate_top5
      from (
        with eligible as (
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
                (coalesce(HIT_RATE_SUCCESS, 0) * coalesce(AVG_RETURN_SUCCESS, 0)) as RANKING_SCORE
            from MIP.MART.V_TRUSTED_SIGNALS_LATEST_TS
            where (try_to_number(replace(to_varchar(RUN_ID), 'T', '')) = :P_SIGNAL_RUN_ID
                   or to_varchar(P_SIGNAL_RUN_ID) = RUN_ID)
              and coalesce(N_SIGNALS, 0) >= :v_min_n_signals
        ),
        ranked as (
            select
                e.*,
                row_number() over (
                    order by e.RANKING_SCORE desc nulls last,
                             e.SCORE desc nulls last,
                             e.RECOMMENDATION_ID
                ) as rn
            from eligible e
        )
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
                'ranking_score', RANKING_SCORE
            ) as obj
        from ranked
        where rn <= 5
      ) sub;
    v_candidate_top5 := coalesce(:v_candidate_top5, array_construct());
    if (array_size(:v_candidate_top5) = 0) then
        select
            case
                when not exists (
                    select 1 from MIP.MART.V_TRUSTED_SIGNALS_LATEST_TS
                    where (try_to_number(replace(to_varchar(RUN_ID), 'T', '')) = :P_SIGNAL_RUN_ID or to_varchar(P_SIGNAL_RUN_ID) = RUN_ID)
                ) then 'no_trusted_signals_at_latest_ts'
                else 'no_candidates_with_min_n_signals'
            end
          into :v_candidate_reason;
    end if;
    v_candidate_summary := object_construct(
        'candidates', :v_candidate_top5,
        'reason', :v_candidate_reason
    );

    -- BONUS: Explainability
    v_assumptions := object_construct(
        'min_n_signals', :v_min_n_signals,
        'ranking_formula', 'hit_rate_success * avg_return_success',
        'horizons_considered', 'from V_TRUSTED_PATTERN_HORIZONS (training gate passed)'
    );
    v_rationale_templates := array_construct(
        'Top pattern/horizon by Sharpe-like (success-only).',
        'Paper candidates ranked by hit_rate_success * avg_return_success, min_n_signals guard.',
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
        'data_lineage', :v_data_lineage
    );

    -- Upsert: delete existing row for (as_of_ts, signal_run_id, agent_name) then insert
    delete from MIP.AGENT_OUT.AGENT_MORNING_BRIEF
     where AS_OF_TS = :P_AS_OF_TS
       and SIGNAL_RUN_ID = :P_SIGNAL_RUN_ID
       and AGENT_NAME = :v_agent_name;

    insert into MIP.AGENT_OUT.AGENT_MORNING_BRIEF (
        AS_OF_TS,
        SIGNAL_RUN_ID,
        AGENT_NAME,
        STATUS,
        BRIEF_JSON,
        CREATED_AT
    )
    values (
        :P_AS_OF_TS,
        :P_SIGNAL_RUN_ID,
        :v_agent_name,
        :v_status,
        :v_brief_json,
        :v_generated_at
    );

    select BRIEF_ID into :v_brief_id
      from MIP.AGENT_OUT.AGENT_MORNING_BRIEF
     where AS_OF_TS = :P_AS_OF_TS
       and SIGNAL_RUN_ID = :P_SIGNAL_RUN_ID
       and AGENT_NAME = :v_agent_name
     limit 1;

    -- One audit event (EVENT_TYPE='AGENT', EVENT_NAME='SP_AGENT_GENERATE_MORNING_BRIEF')
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
        uuid_string(),
        null,
        'AGENT',
        'SP_AGENT_GENERATE_MORNING_BRIEF',
        :v_status,
        1,
        object_construct(
            'as_of_ts', :P_AS_OF_TS,
            'signal_run_id', :P_SIGNAL_RUN_ID,
            'agent_name', :v_agent_name,
            'brief_id', :v_brief_id
        )
    );

    return object_construct('brief_json', :v_brief_json, 'brief_id', :v_brief_id);
exception
    when other then
        v_status := 'FAIL';
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
            uuid_string(),
            null,
            'AGENT',
            'SP_AGENT_GENERATE_MORNING_BRIEF',
            :v_status,
            0,
            object_construct('as_of_ts', :P_AS_OF_TS, 'signal_run_id', :P_SIGNAL_RUN_ID),
            sqlerrm
        );
        raise;
end;
$$;
