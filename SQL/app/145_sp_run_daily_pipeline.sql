-- 145_sp_run_daily_pipeline.sql
-- Purpose: Orchestrate daily ingestion + recommendation pipeline

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_RUN_DAILY_PIPELINE()
returns variant
language sql
execute as caller
as
$$
declare
    v_from_ts timestamp_ntz := dateadd(day, -90, current_timestamp()::timestamp_ntz);
    v_requested_to_ts timestamp_ntz := current_timestamp()::timestamp_ntz;
    v_effective_to_ts timestamp_ntz := :v_requested_to_ts;
    v_latest_market_bars_ts timestamp_ntz;
    v_latest_market_bars_ts_before timestamp_ntz;
    v_latest_market_bars_ts_after timestamp_ntz;
    v_has_new_bars boolean := false;
    v_skip_downstream boolean := false;
    v_any_step_skipped_or_degraded boolean := false;
    v_pipeline_status_reason string;
    v_pipeline_root_status string := 'SUCCESS';
    v_run_id string := uuid_string();
    v_market_types resultset;
    v_market_type string;
    v_market_type_count number := 0;
    v_portfolios resultset;
    v_portfolio_id number;
    v_portfolio_results array := array_construct();
    v_portfolio_count number := 0;
    v_portfolio_run_result variant;
    v_ingest_result variant;
    v_ingest_status string;
    v_ingest_rate_limit boolean := false;
    v_returns_result variant;
    v_eval_result variant;
    v_portfolio_result variant;
    v_brief_result variant;
    v_brief_results array := array_construct();
    v_brief_count number := 0;
    v_brief_run_result variant;
    v_signal_run_id string;   -- pipeline run id (= recommendations DETAILS:run_id); passed to agent for deterministic tie-back
    v_eligible_signal_count number := 0;
    v_proposed_count number := 0;
    v_approved_count number := 0;
    v_rejected_count number := 0;
    v_executed_count number := 0;
    v_recommendation_results array := array_construct();
    v_recommendation_result variant;
    v_summary variant;
    v_interval_minutes number := 1440;
    v_step_start timestamp_ntz;
    v_step_end timestamp_ntz;
    v_rows_before number;
    v_rows_after number;
    v_rows_delta number;
    v_trusted_signal_proc string;
    v_trusted_signal_status string;
    v_trusted_signal_rows_before number := 0;
    v_trusted_signal_rows_after number := 0;
    v_trusted_signal_rows_delta number := 0;
    v_trusted_candidates number := 0;
    v_trusted_patterns number := 0;
    v_gate_param_set string := null;
    v_proposer_start timestamp_ntz;
    v_proposer_end timestamp_ntz;
    v_executor_start timestamp_ntz;
    v_executor_end timestamp_ntz;
    v_brief_start timestamp_ntz;
    v_brief_end timestamp_ntz;
    v_proposals_before number := 0;
    v_proposals_after number := 0;
    v_proposals_delta number := 0;
    v_executed_before number := 0;
    v_executed_after number := 0;
    v_executed_delta number := 0;
    v_trades_before number := 0;
    v_trades_after number := 0;
    v_trades_delta number := 0;
    v_portfolio_trades number := 0;
    v_portfolio_entries_blocked number := 0;
    v_portfolio_stop_reasons array := array_construct();
    v_portfolio_run_ids array := array_construct();
    v_brief_rows_before number := 0;
    v_brief_rows_after number := 0;
    v_brief_rows_delta number := 0;
    v_brief_ids array := array_construct();
    v_agent_brief_result variant;
    v_agent_brief_id number;
    v_agent_brief_status string;
    v_agent_brief_start timestamp_ntz;
    v_agent_brief_end timestamp_ntz;
begin
    execute immediate 'alter session set query_tag = ''' || v_run_id || '''';

    call MIP.APP.SP_LOG_EVENT(
        'PIPELINE',
        'SP_RUN_DAILY_PIPELINE',
        'START',
        null,
        object_construct('from_ts', :v_from_ts, 'requested_to_ts', :v_requested_to_ts),
        null,
        :v_run_id,
        null
    );

    select max(ts)
      into :v_latest_market_bars_ts_before
      from MIP.MART.MARKET_BARS;

    v_step_start := current_timestamp();
    begin
        v_ingest_result := (call MIP.APP.SP_PIPELINE_INGEST(:v_run_id));
    exception
        when other then
            v_step_end := current_timestamp();
            call MIP.APP.SP_AUDIT_LOG_STEP(
                :v_run_id,
                'INGESTION',
                'FAIL',
                null,
                object_construct(
                    'step_name', 'ingestion',
                    'scope', 'AGG',
                    'scope_key', null,
                    'started_at', :v_step_start,
                    'completed_at', :v_step_end,
                    'requested_to_ts', :v_requested_to_ts
                ),
                :sqlerrm
            );
            call MIP.APP.SP_LOG_EVENT(
                'PIPELINE',
                'SP_RUN_DAILY_PIPELINE',
                'FAIL',
                null,
                object_construct('from_ts', :v_from_ts, 'requested_to_ts', :v_requested_to_ts),
                :sqlerrm,
                :v_run_id,
                null
            );
            raise;
    end;

    v_step_end := current_timestamp();
    v_ingest_status := coalesce(:v_ingest_result:"status"::string, 'UNKNOWN');
    v_ingest_rate_limit := coalesce(
        :v_ingest_result:"ingest_result":"rate_limit_hit"::boolean,
        :v_ingest_result:"rate_limit_hit"::boolean,
        false
    );
    v_rows_before := :v_ingest_result:"rows_before"::number;
    v_rows_after := :v_ingest_result:"rows_after"::number;
    v_rows_delta := coalesce(:v_ingest_result:"rows_delta"::number, :v_rows_after - :v_rows_before);

    select max(ts)
      into :v_latest_market_bars_ts_after
      from MIP.MART.MARKET_BARS;
    v_latest_market_bars_ts := :v_latest_market_bars_ts_after;
    v_has_new_bars := (
        :v_latest_market_bars_ts_after is not null
        and :v_latest_market_bars_ts_before is not null
        and :v_latest_market_bars_ts_after > :v_latest_market_bars_ts_before
    );
    v_skip_downstream := (
        not :v_has_new_bars
        and :v_ingest_status in ('SKIP_RATE_LIMIT', 'SUCCESS_WITH_SKIPS')
    );
    if (:v_ingest_status in ('SKIP_RATE_LIMIT', 'SUCCESS_WITH_SKIPS')) then
        v_any_step_skipped_or_degraded := true;
    end if;

    call MIP.APP.SP_AUDIT_LOG_STEP(
        :v_run_id,
        'INGESTION',
        :v_ingest_status,
        :v_rows_delta,
        object_construct(
            'step_name', 'ingestion',
            'scope', 'AGG',
            'scope_key', null,
            'started_at', :v_step_start,
            'completed_at', :v_step_end,
            'rows_before', :v_rows_before,
            'rows_after', :v_rows_after,
            'rows_delta', :v_rows_delta,
            'ingest_status', :v_ingest_status,
            'rate_limit_hit', :v_ingest_rate_limit,
            'requested_to_ts', :v_requested_to_ts,
            'ingest_result', :v_ingest_result
        ),
        null
    );

    if (v_ingest_rate_limit or v_ingest_status in ('SUCCESS_WITH_SKIPS', 'SKIP_RATE_LIMIT')) then
        call MIP.APP.SP_AUDIT_LOG_STEP(
            :v_run_id,
            'INGESTION',
            'SKIP_RATE_LIMIT',
            null,
            object_construct(
                'step_name', 'ingestion',
                'scope', 'AGG',
                'scope_key', null,
                'requested_to_ts', :v_requested_to_ts,
                'ingest_status', :v_ingest_status,
                'ingest_result', :v_ingest_result
            ),
            null
        );
    end if;

    v_effective_to_ts := coalesce(
        least(:v_requested_to_ts, :v_latest_market_bars_ts),
        :v_latest_market_bars_ts,
        :v_requested_to_ts
    );

    v_step_start := current_timestamp();
    begin
        v_returns_result := (call MIP.APP.SP_PIPELINE_REFRESH_RETURNS(:v_run_id));
    exception
        when other then
            v_step_end := current_timestamp();
            call MIP.APP.SP_AUDIT_LOG_STEP(
                :v_run_id,
                'RETURNS_REFRESH',
                'FAIL',
                null,
                object_construct(
                    'step_name', 'returns_refresh',
                    'scope', 'AGG',
                    'scope_key', null,
                    'started_at', :v_step_start,
                    'completed_at', :v_step_end
                ),
                :sqlerrm
            );
            raise;
    end;
    v_step_end := current_timestamp();

    call MIP.APP.SP_AUDIT_LOG_STEP(
        :v_run_id,
        'RETURNS_REFRESH',
        'SUCCESS',
        :v_returns_result:"returns_at_latest_ts"::number,
        object_construct(
            'step_name', 'returns_refresh',
            'scope', 'AGG',
            'scope_key', null,
            'started_at', :v_step_start,
            'completed_at', :v_step_end,
            'latest_market_bars_ts', :v_returns_result:"latest_market_bars_ts"::timestamp_ntz,
            'latest_market_returns_ts', :v_returns_result:"latest_market_returns_ts"::timestamp_ntz,
            'market_bars_at_latest_ts', :v_returns_result:"market_bars_at_latest_ts"::number,
            'returns_at_latest_ts', :v_returns_result:"returns_at_latest_ts"::number
        ),
        null
    );

    if (v_skip_downstream) then
        v_step_start := current_timestamp();
        v_step_end := current_timestamp();
        call MIP.APP.SP_AUDIT_LOG_STEP(
            :v_run_id,
            'RECOMMENDATIONS',
            'SKIPPED_NO_NEW_BARS',
            null,
            object_construct(
                'step_name', 'recommendations',
                'scope', 'AGG',
                'scope_key', null,
                'started_at', :v_step_start,
                'completed_at', :v_step_end,
                'reason', 'NO_NEW_BARS'
            ),
            null
        );
        call MIP.APP.SP_AUDIT_LOG_STEP(
            :v_run_id,
            'EVALUATION',
            'SKIPPED_NO_NEW_BARS',
            null,
            object_construct(
                'step_name', 'evaluation',
                'scope', 'AGG',
                'scope_key', null,
                'started_at', :v_step_start,
                'completed_at', :v_step_end,
                'reason', 'NO_NEW_BARS'
            ),
            null
        );
        call MIP.APP.SP_AUDIT_LOG_STEP(
            :v_run_id,
            'PORTFOLIO_SIMULATION',
            'SKIPPED_NO_NEW_BARS',
            null,
            object_construct(
                'step_name', 'portfolio_simulation',
                'scope', 'AGG',
                'scope_key', null,
                'started_at', :v_step_start,
                'completed_at', :v_step_end,
                'reason', 'NO_NEW_BARS'
            ),
            null
        );
        call MIP.APP.SP_AUDIT_LOG_STEP(
            :v_run_id,
            'TRUSTED_SIGNAL_REFRESH',
            'SKIPPED_NO_NEW_BARS',
            null,
            object_construct(
                'step_name', 'trusted_signal_refresh',
                'scope', 'AGG',
                'scope_key', null,
                'started_at', :v_step_start,
                'completed_at', :v_step_end,
                'reason', 'NO_NEW_BARS'
            ),
            null
        );
        call MIP.APP.SP_AUDIT_LOG_STEP(
            :v_run_id,
            'MORNING_BRIEF',
            'SKIPPED_NO_NEW_BARS',
            null,
            object_construct(
                'step_name', 'morning_brief',
                'scope', 'AGG',
                'scope_key', null,
                'started_at', :v_step_start,
                'completed_at', :v_step_end,
                'reason', 'NO_NEW_BARS'
            ),
            null
        );
        call MIP.APP.SP_AUDIT_LOG_STEP(
            :v_run_id,
            'PROPOSER',
            'SKIPPED_NO_NEW_BARS',
            null,
            object_construct(
                'step_name', 'proposer',
                'scope', 'AGG',
                'scope_key', null,
                'started_at', :v_step_start,
                'completed_at', :v_step_end,
                'reason', 'NO_NEW_BARS'
            ),
            null
        );
        call MIP.APP.SP_AUDIT_LOG_STEP(
            :v_run_id,
            'EXECUTOR',
            'SKIPPED_NO_NEW_BARS',
            null,
            object_construct(
                'step_name', 'executor',
                'scope', 'AGG',
                'scope_key', null,
                'started_at', :v_step_start,
                'completed_at', :v_step_end,
                'reason', 'NO_NEW_BARS'
            ),
            null
        );
        call MIP.APP.SP_AUDIT_LOG_STEP(
            :v_run_id,
            'AGENT',
            'SKIPPED_NO_NEW_BARS',
            null,
            object_construct(
                'step_name', 'agent_run_all',
                'scope', 'AGG',
                'scope_key', null,
                'started_at', :v_step_start,
                'completed_at', :v_step_end,
                'reason', 'NO_NEW_BARS'
            ),
            null
        );
        v_pipeline_status_reason := 'RATE_LIMIT';
        v_pipeline_root_status := 'SUCCESS_WITH_SKIPS';
        v_summary := object_construct(
            'run_id', :v_run_id,
            'from_ts', :v_from_ts,
            'requested_to_ts', :v_requested_to_ts,
            'effective_to_ts', :v_effective_to_ts,
            'latest_market_bars_ts', :v_latest_market_bars_ts,
            'has_new_bars', :v_has_new_bars,
            'latest_market_bars_ts_before', :v_latest_market_bars_ts_before,
            'latest_market_bars_ts_after', :v_latest_market_bars_ts_after,
            'pipeline_status_reason', :v_pipeline_status_reason,
            'ingestion', :v_ingest_result,
            'returns_refresh', :v_returns_result,
            'recommendations', object_construct('status', 'SKIPPED_NO_NEW_BARS', 'reason', 'NO_NEW_BARS'),
            'evaluation', object_construct('status', 'SKIPPED_NO_NEW_BARS', 'reason', 'NO_NEW_BARS'),
            'portfolio_simulation', object_construct('status', 'SKIPPED_NO_NEW_BARS', 'reason', 'NO_NEW_BARS'),
            'morning_brief', object_construct('status', 'SKIPPED_NO_NEW_BARS', 'reason', 'NO_NEW_BARS'),
            'agent_generate_morning_brief', object_construct('status', 'SKIPPED_NO_NEW_BARS', 'reason', 'NO_NEW_BARS'),
            'signal_run_id', null
        );
        call MIP.APP.SP_LOG_EVENT(
            'PIPELINE',
            'SP_RUN_DAILY_PIPELINE',
            :v_pipeline_root_status,
            null,
            :v_summary,
            null,
            :v_run_id,
            null
        );
        return :v_summary;
    end if;

    create or replace temporary table MIP.APP.TMP_PIPELINE_MARKET_TYPES (MARKET_TYPE string);
    insert into MIP.APP.TMP_PIPELINE_MARKET_TYPES (MARKET_TYPE)
    select distinct MARKET_TYPE
      from MIP.APP.INGEST_UNIVERSE
     where coalesce(IS_ENABLED, true);

    select count(*)
      into :v_market_type_count
      from MIP.APP.TMP_PIPELINE_MARKET_TYPES;

    if (v_market_type_count = 0) then
        insert into MIP.APP.TMP_PIPELINE_MARKET_TYPES (MARKET_TYPE)
        select distinct MARKET_TYPE
          from MIP.MART.MARKET_BARS
         where TS >= dateadd(day, -7, current_timestamp()::timestamp_ntz);
    end if;

    v_market_types := (
        select MARKET_TYPE
          from MIP.APP.TMP_PIPELINE_MARKET_TYPES
         order by MARKET_TYPE
    );

    v_step_start := current_timestamp();
    begin
        for rec in v_market_types do
            v_market_type := rec.MARKET_TYPE;
            v_recommendation_result := (call MIP.APP.SP_PIPELINE_GENERATE_RECOMMENDATIONS(
                :v_market_type,
                :v_interval_minutes,
                :v_run_id
            ));
            v_recommendation_results := array_append(:v_recommendation_results, :v_recommendation_result);
        end for;
    exception
        when other then
            v_step_end := current_timestamp();
            call MIP.APP.SP_AUDIT_LOG_STEP(
                :v_run_id,
                'RECOMMENDATIONS',
                'FAIL',
                null,
                object_construct(
                    'step_name', 'recommendations',
                    'scope', 'AGG',
                    'scope_key', null,
                    'started_at', :v_step_start,
                    'completed_at', :v_step_end,
                    'interval_minutes', :v_interval_minutes,
                    'market_type_count', :v_market_type_count
                ),
                :sqlerrm
            );
            raise;
    end;
    v_step_end := current_timestamp();

    select
        coalesce(sum(try_to_number(value:rows_before::string)), 0),
        coalesce(sum(try_to_number(value:rows_after::string)), 0),
        coalesce(sum(try_to_number(value:inserted_count::string)), 0)
      into :v_rows_before,
           :v_rows_after,
           :v_rows_delta
      from table(flatten(input => :v_recommendation_results));

    call MIP.APP.SP_AUDIT_LOG_STEP(
        :v_run_id,
        'RECOMMENDATIONS',
        'SUCCESS',
        :v_rows_delta,
        object_construct(
            'step_name', 'recommendations',
            'scope', 'AGG',
            'scope_key', null,
            'started_at', :v_step_start,
            'completed_at', :v_step_end,
            'rows_before', :v_rows_before,
            'rows_after', :v_rows_after,
            'rows_delta', :v_rows_delta,
            'interval_minutes', :v_interval_minutes,
            'market_type_count', :v_market_type_count
        ),
        null
    );

    v_step_start := current_timestamp();
    begin
        v_eval_result := (call MIP.APP.SP_PIPELINE_EVALUATE_RECOMMENDATIONS(:v_from_ts, :v_effective_to_ts, :v_run_id));
    exception
        when other then
            v_step_end := current_timestamp();
            call MIP.APP.SP_AUDIT_LOG_STEP(
                :v_run_id,
                'EVALUATION',
                'FAIL',
                null,
                object_construct(
                    'step_name', 'evaluation',
                    'scope', 'AGG',
                    'scope_key', null,
                    'started_at', :v_step_start,
                    'completed_at', :v_step_end,
                    'from_ts', :v_from_ts,
                    'to_ts', :v_effective_to_ts
                ),
                :sqlerrm
            );
            raise;
    end;
    v_step_end := current_timestamp();

    call MIP.APP.SP_AUDIT_LOG_STEP(
        :v_run_id,
        'EVALUATION',
        'SUCCESS',
        :v_eval_result:"rows_delta"::number,
        object_construct(
            'step_name', 'evaluation',
            'scope', 'AGG',
            'scope_key', null,
            'started_at', :v_step_start,
            'completed_at', :v_step_end,
            'rows_before', :v_eval_result:"rows_before"::number,
            'rows_after', :v_eval_result:"rows_after"::number,
            'rows_delta', :v_eval_result:"rows_delta"::number,
            'from_ts', :v_from_ts,
            'to_ts', :v_effective_to_ts
        ),
        null
    );

    create or replace temporary table MIP.APP.TMP_PIPELINE_PORTFOLIOS (PORTFOLIO_ID number);
    insert into MIP.APP.TMP_PIPELINE_PORTFOLIOS (PORTFOLIO_ID)
    select PORTFOLIO_ID
      from MIP.APP.PORTFOLIO
     where STATUS = 'ACTIVE';

    v_step_start := current_timestamp();
    begin
        v_portfolio_results := array_construct();
        v_portfolio_count := 0;
        v_portfolios := (
            select PORTFOLIO_ID
              from MIP.APP.TMP_PIPELINE_PORTFOLIOS
             order by PORTFOLIO_ID
        );

        for rec in v_portfolios do
            v_portfolio_id := rec.PORTFOLIO_ID;
            v_portfolio_count := v_portfolio_count + 1;
            v_portfolio_run_result := (call MIP.APP.SP_PIPELINE_RUN_PORTFOLIO(
                :v_portfolio_id,
                :v_from_ts,
                :v_effective_to_ts,
                :v_run_id,
                :v_run_id
            ));
            v_portfolio_results := array_append(:v_portfolio_results, :v_portfolio_run_result);
        end for;

        v_portfolio_result := object_construct(
            'status', 'SUCCESS',
            'portfolio_count', :v_portfolio_count,
            'results', :v_portfolio_results
        );
    exception
        when other then
            v_step_end := current_timestamp();
            call MIP.APP.SP_AUDIT_LOG_STEP(
                :v_run_id,
                'PORTFOLIO_SIMULATION',
                'FAIL',
                null,
                object_construct(
                    'step_name', 'portfolio_simulation',
                    'scope', 'AGG',
                    'scope_key', null,
                    'started_at', :v_step_start,
                    'completed_at', :v_step_end,
                    'from_ts', :v_from_ts,
                    'to_ts', :v_effective_to_ts
                ),
                :sqlerrm
            );
            raise;
    end;
    v_step_end := current_timestamp();

    select
        coalesce(sum(try_to_number(value:trades::string)), 0),
        coalesce(sum(iff(value:entries_blocked::boolean, 1, 0)), 0),
        array_agg(distinct value:block_reason::string),
        array_agg(distinct value:run_id::string)
      into :v_portfolio_trades,
           :v_portfolio_entries_blocked,
           :v_portfolio_stop_reasons,
           :v_portfolio_run_ids
      from table(flatten(input => :v_portfolio_result:results));

    v_portfolio_stop_reasons := coalesce(:v_portfolio_stop_reasons, array_construct());
    v_portfolio_run_ids := coalesce(:v_portfolio_run_ids, array_construct());

    call MIP.APP.SP_AUDIT_LOG_STEP(
        :v_run_id,
        'PORTFOLIO_SIMULATION',
        'SUCCESS',
        :v_portfolio_trades,
        object_construct(
            'step_name', 'portfolio_simulation',
            'scope', 'AGG',
            'scope_key', null,
            'started_at', :v_step_start,
            'completed_at', :v_step_end,
            'portfolio_count', :v_portfolio_result:"portfolio_count"::number,
            'from_ts', :v_from_ts,
            'to_ts', :v_effective_to_ts,
            'simulation_run_ids', :v_portfolio_run_ids,
            'trades', :v_portfolio_trades,
            'entries_blocked', :v_portfolio_entries_blocked,
            'stop_reason', :v_portfolio_stop_reasons
        ),
        null
    );

    v_step_start := current_timestamp();
    begin
        select count(*)
          into :v_trusted_signal_rows_before
          from MIP.MART.V_TRUSTED_SIGNALS;

        select
            coalesce(
                max(iff(PROCEDURE_SCHEMA = 'APP', 'MIP.APP.SP_REFRESH_TRUSTED_SIGNALS', null)),
                max(iff(PROCEDURE_SCHEMA = 'MART', 'MIP.MART.SP_REFRESH_TRUSTED_SIGNALS', null))
            )
          into :v_trusted_signal_proc
          from MIP.INFORMATION_SCHEMA.PROCEDURES
         where PROCEDURE_SCHEMA in ('APP', 'MART')
           and PROCEDURE_NAME = 'SP_REFRESH_TRUSTED_SIGNALS'
           and ARGUMENT_SIGNATURE = '()';

        if (v_trusted_signal_proc is not null) then
            execute immediate 'call ' || v_trusted_signal_proc || '()';
            v_trusted_signal_status := 'SUCCESS';
        else
            v_trusted_signal_status := 'SKIPPED_NOT_FOUND';
            v_any_step_skipped_or_degraded := true;
        end if;

        select count(*)
          into :v_trusted_signal_rows_after
          from MIP.MART.V_TRUSTED_SIGNALS;

        v_trusted_signal_rows_delta := :v_trusted_signal_rows_after - :v_trusted_signal_rows_before;
    exception
        when other then
            v_step_end := current_timestamp();
            call MIP.APP.SP_AUDIT_LOG_STEP(
                :v_run_id,
                'TRUSTED_SIGNAL_REFRESH',
                'FAIL',
                null,
                object_construct(
                    'step_name', 'trusted_signal_refresh',
                    'scope', 'AGG',
                    'scope_key', null,
                    'started_at', :v_step_start,
                    'completed_at', :v_step_end,
                    'procedure_name', :v_trusted_signal_proc
                ),
                :sqlerrm
            );
            raise;
    end;
    v_step_end := current_timestamp();

    select count(*) into :v_trusted_candidates from MIP.MART.V_TRUSTED_SIGNALS_LATEST_TS;
    select count(*) into :v_trusted_patterns from MIP.MART.V_TRUSTED_PATTERN_HORIZONS;
    select PARAM_SET into :v_gate_param_set
      from MIP.APP.TRAINING_GATE_PARAMS
     where IS_ACTIVE
     qualify row_number() over (order by PARAM_SET) = 1;

    call MIP.APP.SP_AUDIT_LOG_STEP(
        :v_run_id,
        'TRUSTED_SIGNAL_REFRESH',
        :v_trusted_signal_status,
        :v_trusted_signal_rows_delta,
        object_construct(
            'step_name', 'trusted_signal_refresh',
            'scope', 'AGG',
            'scope_key', null,
            'started_at', :v_step_start,
            'completed_at', :v_step_end,
            'rows_before', :v_trusted_signal_rows_before,
            'rows_after', :v_trusted_signal_rows_after,
            'rows_delta', :v_trusted_signal_rows_delta,
            'procedure_name', :v_trusted_signal_proc,
            'procedure_status', :v_trusted_signal_status,
            'trusted_candidates', :v_trusted_candidates,
            'trusted_patterns', :v_trusted_patterns,
            'gate_param_set', :v_gate_param_set
        ),
        null
    );

    -- Use current pipeline run id as signal_run_id so agent ties back to recommendations we just generated (DETAILS:run_id = v_run_id).
    v_signal_run_id := :v_run_id;

    -- [A4] Agent step: call SP_AGENT_RUN_ALL with pipeline run id so brief filters by RUN_ID deterministically
    v_agent_brief_start := current_timestamp();
    if (v_signal_run_id is not null) then
        begin
            v_agent_brief_result := (call MIP.APP.SP_AGENT_RUN_ALL(:v_effective_to_ts, :v_signal_run_id));
            v_agent_brief_status := coalesce(v_agent_brief_result:agent_results[0]:status::string, 'SUCCESS');
            v_agent_brief_id := v_agent_brief_result:agent_results[0]:brief_id::number;
        exception
            when other then
                v_agent_brief_status := 'FAIL';
                v_agent_brief_id := null;
                call MIP.APP.SP_AUDIT_LOG_STEP(
                    :v_run_id,
                    'AGENT',
                    'FAIL',
                    null,
                    object_construct(
                        'step_name', 'agent_run_all',
                        'scope', 'AGG',
                        'scope_key', null,
                        'started_at', :v_agent_brief_start,
                        'completed_at', current_timestamp(),
                        'signal_run_id', :v_signal_run_id
                    ),
                    sqlerrm
                );
                raise;
        end;
    else
        v_agent_brief_status := 'SKIPPED_NO_SIGNAL_RUN_ID';
        v_agent_brief_id := null;
    end if;
    v_agent_brief_end := current_timestamp();

    if (v_signal_run_id is not null and v_agent_brief_status = 'SUCCESS') then
        call MIP.APP.SP_AUDIT_LOG_STEP(
            :v_run_id,
            'AGENT',
            :v_agent_brief_status,
            1,
            object_construct(
                'step_name', 'agent_run_all',
                'scope', 'AGG',
                'scope_key', null,
                'started_at', :v_agent_brief_start,
                'completed_at', :v_agent_brief_end,
                'brief_id', :v_agent_brief_id,
                'signal_run_id', :v_signal_run_id,
                'agent_results', :v_agent_brief_result
            ),
            null
        );
    elseif (v_agent_brief_status = 'SKIPPED_NO_SIGNAL_RUN_ID') then
        call MIP.APP.SP_AUDIT_LOG_STEP(
            :v_run_id,
            'AGENT',
            :v_agent_brief_status,
            0,
            object_construct(
                'step_name', 'agent_run_all',
                'scope', 'AGG',
                'scope_key', null,
                'started_at', :v_agent_brief_start,
                'completed_at', :v_agent_brief_end,
                'reason', 'NO_SIGNAL_RUN_ID'
            ),
            null
        );
    end if;

    v_brief_start := current_timestamp();
    select count(*)
      into :v_brief_rows_before
      from MIP.AGENT_OUT.MORNING_BRIEF
     where PIPELINE_RUN_ID = :v_run_id;

    v_proposer_start := :v_brief_start;
    v_executor_start := :v_brief_start;

    if (v_signal_run_id is not null) then
        select count(*)
          into :v_proposals_before
          from MIP.AGENT_OUT.ORDER_PROPOSALS
         where RUN_ID = :v_signal_run_id;

        select count(*)
          into :v_executed_before
          from MIP.AGENT_OUT.ORDER_PROPOSALS
         where RUN_ID = :v_signal_run_id
           and STATUS = 'EXECUTED';

        select count(*)
          into :v_trades_before
          from MIP.APP.PORTFOLIO_TRADES
         where RUN_ID = to_varchar(:v_signal_run_id);

        select count(*)
          into :v_eligible_signal_count
          from MIP.APP.V_SIGNALS_ELIGIBLE_TODAY
         where IS_ELIGIBLE
           and RUN_ID = :v_signal_run_id;
    end if;

    v_brief_results := array_construct();
    v_brief_count := 0;
    v_portfolios := (
        select PORTFOLIO_ID
          from MIP.APP.TMP_PIPELINE_PORTFOLIOS
         order by PORTFOLIO_ID
    );

    for rec in v_portfolios do
        v_portfolio_id := rec.PORTFOLIO_ID;
        v_brief_count := v_brief_count + 1;
        v_brief_run_result := (call MIP.APP.SP_PIPELINE_WRITE_MORNING_BRIEF(
            :v_portfolio_id,
            :v_run_id,
            :v_signal_run_id,
            :v_run_id
        ));
        v_brief_results := array_append(:v_brief_results, :v_brief_run_result);
    end for;

    v_brief_result := object_construct(
        'status', 'SUCCESS',
        'portfolio_count', :v_brief_count,
        'results', :v_brief_results
    );

    if (v_signal_run_id is not null) then
        -- HIGH-002: Improved proposal count query with better run ID scoping
        -- Use both RUN_ID (number) and SIGNAL_RUN_ID (string) for robust matching
        select
            count(*) as proposed_count,
            count_if(STATUS in ('APPROVED', 'EXECUTED')) as approved_count,
            count_if(STATUS = 'REJECTED') as rejected_count,
            count_if(STATUS = 'EXECUTED') as executed_count
          into :v_proposed_count,
               :v_approved_count,
               :v_rejected_count,
               :v_executed_count
          from MIP.AGENT_OUT.ORDER_PROPOSALS
         where RUN_ID = :v_signal_run_id
            or (SIGNAL_RUN_ID is not null and SIGNAL_RUN_ID = :v_signal_run_id);
    end if;

    v_brief_end := current_timestamp();
    v_proposer_end := :v_brief_end;
    v_executor_end := :v_brief_end;

    select count(*)
      into :v_brief_rows_after
      from MIP.AGENT_OUT.MORNING_BRIEF
     where PIPELINE_RUN_ID = :v_run_id;

    v_brief_rows_delta := :v_brief_rows_after - :v_brief_rows_before;

    select array_agg(BRIEF_ID) within group (order by BRIEF_ID)
      into :v_brief_ids
      from MIP.AGENT_OUT.MORNING_BRIEF
     where PIPELINE_RUN_ID = :v_run_id;

    v_brief_ids := coalesce(:v_brief_ids, array_construct());

    call MIP.APP.SP_AUDIT_LOG_STEP(
        :v_run_id,
        'MORNING_BRIEF',
        'SUCCESS',
        :v_brief_rows_delta,
        object_construct(
            'step_name', 'morning_brief',
            'scope', 'AGG',
            'scope_key', null,
            'started_at', :v_brief_start,
            'completed_at', :v_brief_end,
            'rows_before', :v_brief_rows_before,
            'rows_after', :v_brief_rows_after,
            'rows_delta', :v_brief_rows_delta,
            'brief_count', :v_brief_rows_after,
            'brief_ids', :v_brief_ids,
            'signal_run_id', :v_signal_run_id
        ),
        null
    );

    if (v_signal_run_id is not null) then
        select count(*)
          into :v_proposals_after
          from MIP.AGENT_OUT.ORDER_PROPOSALS
         where RUN_ID = :v_signal_run_id;

        select count(*)
          into :v_executed_after
          from MIP.AGENT_OUT.ORDER_PROPOSALS
         where RUN_ID = :v_signal_run_id
           and STATUS = 'EXECUTED';

        select count(*)
          into :v_trades_after
          from MIP.APP.PORTFOLIO_TRADES
         where RUN_ID = to_varchar(:v_signal_run_id);

        v_proposals_delta := :v_proposals_after - :v_proposals_before;
        v_executed_delta := :v_executed_after - :v_executed_before;
        v_trades_delta := :v_trades_after - :v_trades_before;

        call MIP.APP.SP_AUDIT_LOG_STEP(
            :v_run_id,
            'PROPOSER',
            'SUCCESS',
            :v_proposals_delta,
            object_construct(
                'step_name', 'proposer',
                'scope', 'AGG',
                'scope_key', null,
                'started_at', :v_proposer_start,
                'completed_at', :v_proposer_end,
                'rows_before', :v_proposals_before,
                'rows_after', :v_proposals_after,
                'rows_delta', :v_proposals_delta,
                'signal_run_id', :v_signal_run_id,
                'proposed_count', :v_proposed_count,
                'approved_count', :v_approved_count,
                'rejected_count', :v_rejected_count
            ),
            null
        );

        call MIP.APP.SP_AUDIT_LOG_STEP(
            :v_run_id,
            'EXECUTOR',
            'SUCCESS',
            :v_executed_delta,
            object_construct(
                'step_name', 'executor',
                'scope', 'AGG',
                'scope_key', null,
                'started_at', :v_executor_start,
                'completed_at', :v_executor_end,
                'rows_before', :v_executed_before,
                'rows_after', :v_executed_after,
                'rows_delta', :v_executed_delta,
                'signal_run_id', :v_signal_run_id,
                'executed_count', :v_executed_count,
                'approved_count', :v_approved_count,
                'rejected_count', :v_rejected_count,
                'trade_rows_before', :v_trades_before,
                'trade_rows_after', :v_trades_after,
                'trade_rows_delta', :v_trades_delta
            ),
            null
        );
    else
        call MIP.APP.SP_AUDIT_LOG_STEP(
            :v_run_id,
            'PROPOSER',
            'SKIPPED_NO_SIGNAL_RUN_ID',
            0,
            object_construct(
                'step_name', 'proposer',
                'scope', 'AGG',
                'scope_key', null,
                'started_at', :v_proposer_start,
                'completed_at', :v_proposer_end,
                'signal_run_id', :v_signal_run_id,
                'reason', 'NO_SIGNAL_RUN_ID'
            ),
            null
        );

        call MIP.APP.SP_AUDIT_LOG_STEP(
            :v_run_id,
            'EXECUTOR',
            'SKIPPED_NO_SIGNAL_RUN_ID',
            0,
            object_construct(
                'step_name', 'executor',
                'scope', 'AGG',
                'scope_key', null,
                'started_at', :v_executor_start,
                'completed_at', :v_executor_end,
                'signal_run_id', :v_signal_run_id,
                'reason', 'NO_SIGNAL_RUN_ID'
            ),
            null
        );
        v_any_step_skipped_or_degraded := true;
    end if;

    v_pipeline_root_status := iff(:v_any_step_skipped_or_degraded, 'SUCCESS_WITH_SKIPS', 'SUCCESS');
    v_pipeline_status_reason := iff(:v_ingest_status in ('SKIP_RATE_LIMIT', 'SUCCESS_WITH_SKIPS'), 'RATE_LIMIT', null);

    v_summary := object_construct(
        'run_id', :v_run_id,
        'from_ts', :v_from_ts,
        'requested_to_ts', :v_requested_to_ts,
        'effective_to_ts', :v_effective_to_ts,
        'latest_market_bars_ts', :v_latest_market_bars_ts,
        'has_new_bars', :v_has_new_bars,
        'latest_market_bars_ts_before', :v_latest_market_bars_ts_before,
        'latest_market_bars_ts_after', :v_latest_market_bars_ts_after,
        'pipeline_status_reason', :v_pipeline_status_reason,
        'ingestion', :v_ingest_result,
        'returns_refresh', :v_returns_result,
        'recommendations', :v_recommendation_results,
        'evaluation', :v_eval_result,
        'portfolio_simulation', :v_portfolio_result,
        'agent_generate_morning_brief', object_construct(
            'status', iff(:v_agent_brief_status = 'SUCCESS', 'SUCCESS', :v_agent_brief_status),
            'brief_id', :v_agent_brief_id
        ),
        'morning_brief', :v_brief_result,
        'signal_run_id', :v_signal_run_id,
        'eligible_signals', :v_eligible_signal_count,
        'proposals_proposed', :v_proposed_count,
        'proposals_approved', :v_approved_count,
        'proposals_rejected', :v_rejected_count,
        'proposals_executed', :v_executed_count
    );

    call MIP.APP.SP_LOG_EVENT(
        'PIPELINE',
        'SP_RUN_DAILY_PIPELINE',
        :v_pipeline_root_status,
        null,
        :v_summary,
        null,
        :v_run_id,
        null
    );

    return :v_summary;
exception
    when other then
        call MIP.APP.SP_LOG_EVENT(
            'PIPELINE',
            'SP_RUN_DAILY_PIPELINE',
            'FAIL',
            null,
            object_construct('from_ts', :v_from_ts, 'requested_to_ts', :v_requested_to_ts),
            :sqlerrm,
            :v_run_id,
            null
        );
        raise;
end;
$$;
