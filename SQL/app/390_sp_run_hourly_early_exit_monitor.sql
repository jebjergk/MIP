-- 390_sp_run_hourly_early_exit_monitor.sql
-- Purpose: Hourly monitor for daily-position early exits.
-- Flow: ingest bars at EARLY_EXIT_INTERVAL_MINUTES -> evaluate early exits.
-- This is intentionally decoupled from SP_RUN_INTRADAY_PIPELINE so intraday
-- trading/training can remain parked.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_RUN_HOURLY_EARLY_EXIT_MONITOR()
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
    v_enabled             boolean := false;

    v_ingest_result       variant;
    v_ingest_status       string := 'PENDING';
    v_bars_ingested       number := 0;
    v_symbols_processed   number := 0;

    v_early_exit_result   variant;
    v_early_exit_status   string := 'PENDING';
    v_positions_evaluated number := 0;
    v_exit_signals        number := 0;
    v_exits_executed      number := 0;

    v_pipeline_status     string := 'SUCCESS';
begin
    execute immediate 'alter session set query_tag = ''' || v_run_id || '''';

    begin
        v_enabled := coalesce(try_to_boolean(
            (select CONFIG_VALUE from MIP.APP.APP_CONFIG where CONFIG_KEY = 'EARLY_EXIT_ENABLED')
        ), false);
        v_interval_minutes := coalesce(try_to_number(
            (select CONFIG_VALUE from MIP.APP.APP_CONFIG where CONFIG_KEY = 'EARLY_EXIT_INTERVAL_MINUTES')
        ), 60);
    exception
        when other then
            v_enabled := false;
    end;

    if (not :v_enabled) then
        call MIP.APP.SP_LOG_EVENT(
            'EARLY_EXIT_PIPELINE',
            'SP_RUN_HOURLY_EARLY_EXIT_MONITOR',
            'SKIPPED',
            0,
            object_construct(
                'run_id', :v_run_id,
                'interval_minutes', :v_interval_minutes,
                'reason', 'EARLY_EXIT_ENABLED is false'
            ),
            null,
            :v_run_id,
            null
        );

        return object_construct(
            'status', 'SKIPPED_DISABLED',
            'run_id', :v_run_id,
            'interval_minutes', :v_interval_minutes,
            'reason', 'EARLY_EXIT_ENABLED is false'
        );
    end if;

    -- Step 1: ingest bars for configured early-exit interval.
    begin
        v_ingest_result := (call MIP.APP.SP_INGEST_ALPHAVANTAGE_BARS(:v_interval_minutes));
        v_ingest_status := coalesce(:v_ingest_result:status::string, 'UNKNOWN');
        v_bars_ingested := coalesce(:v_ingest_result:rows_inserted::number, 0);
        v_symbols_processed := coalesce(:v_ingest_result:symbols_processed::number, 0);
    exception
        when other then
            v_ingest_status := 'FAIL';
            v_ingest_result := object_construct('status', 'FAIL', 'error', :sqlerrm);
            v_pipeline_status := 'PARTIAL';
    end;

    -- Step 2: evaluate early exits on open daily positions.
    begin
        v_early_exit_result := (call MIP.APP.SP_EVALUATE_EARLY_EXITS(:v_run_id));
        v_early_exit_status := coalesce(:v_early_exit_result:status::string, 'UNKNOWN');
        v_positions_evaluated := coalesce(:v_early_exit_result:positions_evaluated::number, 0);
        v_exit_signals := coalesce(:v_early_exit_result:exit_signals::number, 0);
        v_exits_executed := coalesce(:v_early_exit_result:exits_executed::number, 0);
    exception
        when other then
            v_early_exit_status := 'FAIL';
            v_early_exit_result := object_construct('status', 'FAIL', 'error', :sqlerrm);
            if (:v_pipeline_status = 'SUCCESS') then
                v_pipeline_status := 'PARTIAL';
            end if;
    end;

    if (:v_ingest_status = 'FAIL' and :v_early_exit_status = 'FAIL') then
        v_pipeline_status := 'FAIL';
    end if;

    v_completed_at := current_timestamp();

    call MIP.APP.SP_LOG_EVENT(
        'EARLY_EXIT_PIPELINE',
        'SP_RUN_HOURLY_EARLY_EXIT_MONITOR',
        :v_pipeline_status,
        :v_exit_signals,
        object_construct(
            'run_id', :v_run_id,
            'started_at', :v_started_at,
            'completed_at', :v_completed_at,
            'interval_minutes', :v_interval_minutes,
            'bars_ingested', :v_bars_ingested,
            'symbols_processed', :v_symbols_processed,
            'positions_evaluated', :v_positions_evaluated,
            'exit_signals', :v_exit_signals,
            'exits_executed', :v_exits_executed,
            'steps', object_construct(
                'ingestion', :v_ingest_result,
                'early_exit', :v_early_exit_result
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
        'symbols_processed', :v_symbols_processed,
        'positions_evaluated', :v_positions_evaluated,
        'exit_signals', :v_exit_signals,
        'exits_executed', :v_exits_executed,
        'steps', object_construct(
            'ingestion', object_construct('status', :v_ingest_status),
            'early_exit', object_construct('status', :v_early_exit_status)
        )
    );
exception
    when other then
        call MIP.APP.SP_LOG_EVENT(
            'EARLY_EXIT_PIPELINE',
            'SP_RUN_HOURLY_EARLY_EXIT_MONITOR',
            'FAIL',
            0,
            object_construct('run_id', :v_run_id),
            :sqlerrm,
            :v_run_id,
            null
        );
        raise;
end;
$$;
