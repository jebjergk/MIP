-- 320_sp_run_intraday_pipeline.sql
-- Purpose: Orchestrator for the independent intraday learning pipeline.
-- Flow: check feature flag → ingest intraday bars → detect patterns → evaluate outcomes → log run.
-- Non-fatal per step: each step is wrapped so failures don't kill the pipeline.
-- Does NOT touch daily pipeline tables/procs except shared RECOMMENDATION_LOG and RECOMMENDATION_OUTCOMES.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_RUN_INTRADAY_PIPELINE()
returns variant
language sql
execute as caller
as
$$
declare
    v_run_id              string := uuid_string();
    v_started_at          timestamp_ntz := current_timestamp();
    v_completed_at        timestamp_ntz;
    v_interval_minutes    number := 60;
    v_is_enabled          boolean := false;
    v_use_daily_context   boolean := false;

    v_ingest_result       variant;
    v_ingest_status       string := 'PENDING';
    v_bars_ingested       number := 0;

    v_signal_result       variant;
    v_signal_status       string := 'PENDING';
    v_signals_generated   number := 0;

    v_eval_result         variant;
    v_eval_status         string := 'PENDING';
    v_outcomes_evaluated  number := 0;

    v_symbols_processed   number := 0;
    v_pipeline_status     string := 'SUCCESS';
    v_compute_start       timestamp_ntz;
    v_compute_seconds     float := 0;

    v_from_ts             timestamp_ntz := dateadd(day, -30, current_timestamp()::timestamp_ntz);
    v_to_ts               timestamp_ntz := current_timestamp()::timestamp_ntz;
begin
    execute immediate 'alter session set query_tag = ''' || v_run_id || '''';

    -- Read config (one value per SELECT to avoid subquery-in-INTO restriction)
    begin
        v_is_enabled := coalesce(try_to_boolean(
            (select CONFIG_VALUE from MIP.APP.APP_CONFIG where CONFIG_KEY = 'INTRADAY_ENABLED')
        ), false);
        v_interval_minutes := coalesce(try_to_number(
            (select CONFIG_VALUE from MIP.APP.APP_CONFIG where CONFIG_KEY = 'INTRADAY_INTERVAL_MINUTES')
        ), 60);
        v_use_daily_context := coalesce(try_to_boolean(
            (select CONFIG_VALUE from MIP.APP.APP_CONFIG where CONFIG_KEY = 'INTRADAY_USE_DAILY_CONTEXT')
        ), false);
    exception
        when other then
            v_is_enabled := false;
    end;

    if (not :v_is_enabled) then
        insert into MIP.APP.INTRADAY_PIPELINE_RUN_LOG (
            RUN_ID, INTERVAL_MINUTES, STARTED_AT, COMPLETED_AT,
            STATUS, BARS_INGESTED, SIGNALS_GENERATED, OUTCOMES_EVALUATED,
            SYMBOLS_PROCESSED, DAILY_CONTEXT_USED, COMPUTE_SECONDS,
            DETAILS
        )
        select
            :v_run_id, :v_interval_minutes, :v_started_at, current_timestamp(),
            'SKIPPED_DISABLED', 0, 0, 0,
            0, false, 0,
            object_construct('reason', 'INTRADAY_ENABLED is false');

        return object_construct(
            'status', 'SKIPPED_DISABLED',
            'run_id', :v_run_id,
            'reason', 'INTRADAY_ENABLED is false'
        );
    end if;

    v_compute_start := current_timestamp();

    -- ── STEP 1: Ingest intraday bars ──────────────────────────────────
    begin
        v_ingest_result := (call MIP.APP.SP_INGEST_ALPHAVANTAGE_BARS(:v_interval_minutes));
        v_ingest_status := coalesce(:v_ingest_result:status::string, 'UNKNOWN');
        v_bars_ingested := coalesce(:v_ingest_result:rows_inserted::number, 0);
        v_symbols_processed := coalesce(:v_ingest_result:symbols_processed::number, 0);
    exception
        when other then
            v_ingest_status := 'FAIL';
            v_ingest_result := object_construct('status', 'FAIL', 'error', sqlerrm);
            v_pipeline_status := 'PARTIAL';
    end;

    -- ── STEP 2: Generate intraday signals ─────────────────────────────
    begin
        v_signal_result := (call MIP.APP.SP_INTRADAY_GENERATE_SIGNALS(
            :v_interval_minutes, :v_run_id
        ));
        v_signal_status := coalesce(:v_signal_result:status::string, 'UNKNOWN');
        v_signals_generated := coalesce(:v_signal_result:total_signals::number, 0);
    exception
        when other then
            v_signal_status := 'FAIL';
            v_signal_result := object_construct('status', 'FAIL', 'error', sqlerrm);
            if (:v_pipeline_status = 'SUCCESS') then
                v_pipeline_status := 'PARTIAL';
            end if;
    end;

    -- ── STEP 3: Evaluate outcomes for intraday recommendations ────────
    begin
        v_eval_result := (call MIP.APP.SP_PIPELINE_EVALUATE_RECOMMENDATIONS(
            :v_from_ts, :v_to_ts, :v_run_id
        ));
        v_eval_status := coalesce(:v_eval_result:status::string, 'UNKNOWN');
        v_outcomes_evaluated := coalesce(:v_eval_result:rows_delta::number, 0);
    exception
        when other then
            v_eval_status := 'FAIL';
            v_eval_result := object_construct('status', 'FAIL', 'error', sqlerrm);
            if (:v_pipeline_status = 'SUCCESS') then
                v_pipeline_status := 'PARTIAL';
            end if;
    end;

    -- ── Finalize ──────────────────────────────────────────────────────
    v_completed_at := current_timestamp();
    v_compute_seconds := timestampdiff(millisecond, :v_compute_start, :v_completed_at) / 1000.0;

    if (:v_ingest_status = 'FAIL' and :v_signal_status = 'FAIL' and :v_eval_status = 'FAIL') then
        v_pipeline_status := 'FAIL';
    end if;

    insert into MIP.APP.INTRADAY_PIPELINE_RUN_LOG (
        RUN_ID, INTERVAL_MINUTES, STARTED_AT, COMPLETED_AT,
        STATUS, BARS_INGESTED, SIGNALS_GENERATED, OUTCOMES_EVALUATED,
        SYMBOLS_PROCESSED, DAILY_CONTEXT_USED, COMPUTE_SECONDS,
        DETAILS
    )
    select
        :v_run_id, :v_interval_minutes, :v_started_at, :v_completed_at,
        :v_pipeline_status, :v_bars_ingested, :v_signals_generated, :v_outcomes_evaluated,
        :v_symbols_processed, :v_use_daily_context, :v_compute_seconds,
        object_construct(
            'ingestion', :v_ingest_result,
            'signal_generation', :v_signal_result,
            'evaluation', :v_eval_result,
            'config', object_construct(
                'interval_minutes', :v_interval_minutes,
                'use_daily_context', :v_use_daily_context
            )
        );

    call MIP.APP.SP_LOG_EVENT(
        'INTRADAY_PIPELINE',
        'SP_RUN_INTRADAY_PIPELINE',
        :v_pipeline_status,
        :v_signals_generated,
        object_construct(
            'run_id', :v_run_id,
            'interval_minutes', :v_interval_minutes,
            'bars_ingested', :v_bars_ingested,
            'signals_generated', :v_signals_generated,
            'outcomes_evaluated', :v_outcomes_evaluated,
            'symbols_processed', :v_symbols_processed,
            'compute_seconds', :v_compute_seconds,
            'step_statuses', object_construct(
                'ingestion', :v_ingest_status,
                'signal_generation', :v_signal_status,
                'evaluation', :v_eval_status
            )
        ),
        null,
        :v_run_id,
        null
    );

    return object_construct(
        'status', :v_pipeline_status,
        'run_id', :v_run_id,
        'interval_minutes', :v_interval_minutes,
        'bars_ingested', :v_bars_ingested,
        'signals_generated', :v_signals_generated,
        'outcomes_evaluated', :v_outcomes_evaluated,
        'symbols_processed', :v_symbols_processed,
        'compute_seconds', :v_compute_seconds,
        'steps', object_construct(
            'ingestion', object_construct('status', :v_ingest_status),
            'signal_generation', object_construct('status', :v_signal_status),
            'evaluation', object_construct('status', :v_eval_status)
        )
    );
exception
    when other then
        begin
            insert into MIP.APP.INTRADAY_PIPELINE_RUN_LOG (
                RUN_ID, INTERVAL_MINUTES, STARTED_AT, COMPLETED_AT,
                STATUS, COMPUTE_SECONDS, DETAILS
            )
            select
                :v_run_id, :v_interval_minutes, :v_started_at, current_timestamp(),
                'FAIL', 0,
                object_construct('error', sqlerrm, 'sqlstate', sqlstate);
        exception when other then null;
        end;
        raise;
end;
$$;
