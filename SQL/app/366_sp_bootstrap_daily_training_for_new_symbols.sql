-- 366_sp_bootstrap_daily_training_for_new_symbols.sql
-- Purpose: Additive day-by-day bootstrap for new symbol cohorts.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_BOOTSTRAP_DAILY_TRAINING_FOR_NEW_SYMBOLS(
    P_RUN_ID string default null,
    P_START_DATE date default '2025-09-01',
    P_END_DATE date default current_date(),
    P_SYMBOL_COHORT string default 'VOL_EXP',
    P_MARKET_TYPE string default 'STOCK',
    P_WAREHOUSE_OVERRIDE string default null
)
returns variant
language sql
execute as caller
as
$$
declare
    v_run_id string := coalesce(:P_RUN_ID, uuid_string());
    v_start_date date := coalesce(:P_START_DATE, to_date('2025-09-01'));
    v_end_date date := coalesce(:P_END_DATE, current_date());
    v_symbol_cohort string := coalesce(:P_SYMBOL_COHORT, 'VOL_EXP');
    v_market_type_filter string := :P_MARKET_TYPE;
    v_d date;
    v_effective_to_ts timestamp_ntz;
    v_day_run_id string;
    v_backfill_result variant;
    v_returns_result variant;
    v_gen_result variant;
    v_eval_result variant;
    v_market_types resultset;
    v_market_type string;
    v_signals_created_total number := 0;
    v_outcomes_total number := 0;
    v_trust_total number := 0;
    v_bars_loaded_total number := 0;
    v_days_processed number := 0;
    v_failures array := array_construct();
    v_status string := 'SUCCESS';
    v_step_fail string;
    v_trust_rows_day number := 0;
begin
    if (v_start_date > v_end_date) then
        return object_construct(
            'status', 'FAIL',
            'error', 'start_date_after_end_date',
            'run_id', :v_run_id
        );
    end if;

    if (P_WAREHOUSE_OVERRIDE is not null) then
        execute immediate 'use warehouse ' || identifier(:P_WAREHOUSE_OVERRIDE);
    end if;

    merge into MIP.APP.VOL_EXP_BOOTSTRAP_RUN_LOG t
    using (select :v_run_id as RUN_ID, 'TRAINING_REPLAY' as STEP_NAME) s
    on t.RUN_ID = s.RUN_ID and t.STEP_NAME = s.STEP_NAME
    when matched then update set
        t.SYMBOL_COHORT = :v_symbol_cohort,
        t.MARKET_TYPE = :v_market_type_filter,
        t.START_DATE = :v_start_date,
        t.END_DATE = :v_end_date,
        t.STARTED_AT = current_timestamp(),
        t.FINISHED_AT = null,
        t.STATUS = 'RUNNING',
        t.FAILURES = null,
        t.DETAILS = object_construct('mode', 'bootstrap_training')
    when not matched then insert (
        RUN_ID, STEP_NAME, SYMBOL_COHORT, MARKET_TYPE, START_DATE, END_DATE, STARTED_AT, STATUS, DETAILS
    ) values (
        :v_run_id, 'TRAINING_REPLAY', :v_symbol_cohort, :v_market_type_filter, :v_start_date, :v_end_date,
        current_timestamp(), 'RUNNING', object_construct('mode', 'bootstrap_training')
    );

    -- (A) Ensure daily bars exist first for the cohort/range.
    v_backfill_result := (
        call MIP.APP.SP_BACKFILL_DAILY_ALPHAVANTAGE_COHORT(
            :v_run_id,
            :v_start_date,
            :v_end_date,
            :v_symbol_cohort,
            :v_market_type_filter,
            :P_WAREHOUSE_OVERRIDE
        )
    );
    v_bars_loaded_total := coalesce(:v_backfill_result:"bars_loaded_count"::number, 0);

    v_d := :v_start_date;
    while (v_d <= v_end_date) do
        v_day_run_id := uuid_string();
        v_effective_to_ts := dateadd(second, -1, dateadd(day, 1, to_timestamp_ntz(:v_d)));
        execute immediate 'alter session set query_tag = ''' || :v_day_run_id || '''';
        call MIP.APP.SP_ENFORCE_RUN_SCOPING(:v_day_run_id, null, :v_effective_to_ts);

        v_step_fail := null;
        begin
            v_returns_result := (call MIP.APP.SP_PIPELINE_REFRESH_RETURNS(:v_day_run_id));

            v_market_types := (
                select distinct MARKET_TYPE
                from MIP.APP.INGEST_UNIVERSE
                where coalesce(IS_ENABLED, true)
                  and INTERVAL_MINUTES = 1440
                  and upper(coalesce(SYMBOL_COHORT, 'CORE')) = upper(:v_symbol_cohort)
                  and (:v_market_type_filter is null or upper(MARKET_TYPE) = upper(:v_market_type_filter))
                order by MARKET_TYPE
            );

            for rec in v_market_types do
                v_market_type := rec.MARKET_TYPE;

                v_gen_result := (
                    call MIP.APP.SP_BOOTSTRAP_GENERATE_RECOMMENDATIONS_COHORT(
                        :v_effective_to_ts,
                        :v_symbol_cohort,
                        :v_market_type,
                        1440,
                        :v_day_run_id
                    )
                );
                v_signals_created_total := :v_signals_created_total + coalesce(:v_gen_result:"signals_created_count"::number, 0);

                v_eval_result := (
                    call MIP.APP.SP_BOOTSTRAP_EVALUATE_RECOMMENDATIONS_COHORT(
                        dateadd(day, -90, :v_effective_to_ts),
                        :v_effective_to_ts,
                        :v_symbol_cohort,
                        :v_market_type,
                        1440,
                        0.0
                    )
                );
                v_outcomes_total := :v_outcomes_total + coalesce(:v_eval_result:"outcomes_computed_count"::number, 0);
            end for;

            select count(*)
              into :v_trust_rows_day
              from MIP.APP.V_TRUSTED_SIGNAL_CLASSIFICATION c
              join MIP.APP.INGEST_UNIVERSE iu
                on upper(iu.SYMBOL) = upper(c.SYMBOL)
               and upper(iu.MARKET_TYPE) = upper(c.MARKET_TYPE)
               and iu.INTERVAL_MINUTES = c.INTERVAL_MINUTES
             where upper(coalesce(iu.SYMBOL_COHORT, 'CORE')) = upper(:v_symbol_cohort)
               and c.INTERVAL_MINUTES = 1440
               and c.TS::date = :v_d
               and (:v_market_type_filter is null or upper(c.MARKET_TYPE) = upper(:v_market_type_filter));

            v_trust_total := :v_trust_total + coalesce(:v_trust_rows_day, 0);
            v_days_processed := :v_days_processed + 1;
        exception
            when other then
                v_step_fail := :sqlerrm;
        end;

        if (v_step_fail is not null) then
            v_failures := array_append(
                :v_failures,
                object_construct(
                    'day', :v_d,
                    'run_id', :v_day_run_id,
                    'error', :v_step_fail
                )
            );
        end if;

        delete from MIP.APP.RUN_SCOPE_OVERRIDE where RUN_ID = :v_day_run_id;
        v_d := dateadd(day, 1, v_d);
    end while;

    if (array_size(:v_failures) > 0) then
        v_status := 'SUCCESS_WITH_SKIPS';
    end if;

    update MIP.APP.VOL_EXP_BOOTSTRAP_RUN_LOG
       set FINISHED_AT = current_timestamp(),
           STATUS = :v_status,
           SYMBOLS_PROCESSED = (
               select count(distinct SYMBOL)
               from MIP.APP.INGEST_UNIVERSE
               where coalesce(IS_ENABLED, true)
                 and INTERVAL_MINUTES = 1440
                 and upper(coalesce(SYMBOL_COHORT, 'CORE')) = upper(:v_symbol_cohort)
                 and (:v_market_type_filter is null or upper(MARKET_TYPE) = upper(:v_market_type_filter))
           ),
           BARS_LOADED_COUNT = :v_bars_loaded_total,
           SIGNALS_CREATED_COUNT = :v_signals_created_total,
           OUTCOMES_COMPUTED_COUNT = :v_outcomes_total,
           TRUST_ROWS_UPDATED_COUNT = :v_trust_total,
           FAILURES = to_variant(:v_failures),
           DETAILS = object_construct(
               'days_processed', :v_days_processed,
               'start_date', :v_start_date,
               'end_date', :v_end_date,
               'backfill_result', :v_backfill_result
           )
     where RUN_ID = :v_run_id
       and STEP_NAME = 'TRAINING_REPLAY';

    return object_construct(
        'status', :v_status,
        'run_id', :v_run_id,
        'symbol_cohort', :v_symbol_cohort,
        'market_type', :v_market_type_filter,
        'start_date', :v_start_date,
        'end_date', :v_end_date,
        'days_processed', :v_days_processed,
        'bars_loaded_count', :v_bars_loaded_total,
        'signals_created_count', :v_signals_created_total,
        'outcomes_computed_count', :v_outcomes_total,
        'trust_rows_updated_count', :v_trust_total,
        'failures', :v_failures
    );
exception
    when other then
        update MIP.APP.VOL_EXP_BOOTSTRAP_RUN_LOG
           set FINISHED_AT = current_timestamp(),
               STATUS = 'FAIL',
               FAILURES = array_construct(object_construct('error', :sqlerrm))
         where RUN_ID = :v_run_id
           and STEP_NAME = 'TRAINING_REPLAY';
        raise;
end;
$$;

