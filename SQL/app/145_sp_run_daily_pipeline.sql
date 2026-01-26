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
    v_signal_run_id number;
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

    v_step_start := current_timestamp();
    begin
        v_ingest_result := (call MIP.APP.SP_PIPELINE_INGEST());
    exception
        when other then
            v_step_end := current_timestamp();
            insert into MIP.APP.MIP_AUDIT_LOG (
                EVENT_TS,
                RUN_ID,
                EVENT_TYPE,
                EVENT_NAME,
                STATUS,
                ROWS_AFFECTED,
                DETAILS,
                ERROR_MESSAGE
            )
            select
                current_timestamp(),
                :v_run_id,
                'PIPELINE_STEP',
                'INGESTION',
                'FAIL',
                null,
                object_construct(
                    'step_name', 'ingestion',
                    'started_at', :v_step_start,
                    'completed_at', :v_step_end,
                    'requested_to_ts', :v_requested_to_ts
                ),
                :sqlerrm;
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

    insert into MIP.APP.MIP_AUDIT_LOG (
        EVENT_TS,
        RUN_ID,
        EVENT_TYPE,
        EVENT_NAME,
        STATUS,
        ROWS_AFFECTED,
        DETAILS,
        ERROR_MESSAGE
    )
    select
        current_timestamp(),
        :v_run_id,
        'PIPELINE_STEP',
        'INGESTION',
        :v_ingest_status,
        :v_rows_delta,
        object_construct(
            'step_name', 'ingestion',
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
        null;

    if (v_ingest_rate_limit or v_ingest_status in ('SUCCESS_WITH_SKIPS', 'SKIP_RATE_LIMIT')) then
        insert into MIP.APP.MIP_AUDIT_LOG (
            EVENT_TS,
            RUN_ID,
            EVENT_TYPE,
            EVENT_NAME,
            STATUS,
            ROWS_AFFECTED,
            DETAILS,
            ERROR_MESSAGE
        )
        select
            current_timestamp(),
            :v_run_id,
            'INGESTION',
            'INGESTION',
            'SKIP_RATE_LIMIT',
            null,
            object_construct(
                'requested_to_ts', :v_requested_to_ts,
                'ingest_status', :v_ingest_status,
                'ingest_result', :v_ingest_result
            ),
            null;
    end if;

    select max(ts)
      into :v_latest_market_bars_ts
      from MIP.MART.MARKET_BARS;

    v_effective_to_ts := coalesce(
        least(:v_requested_to_ts, :v_latest_market_bars_ts),
        :v_latest_market_bars_ts,
        :v_requested_to_ts
    );

    v_step_start := current_timestamp();
    begin
        v_returns_result := (call MIP.APP.SP_PIPELINE_REFRESH_RETURNS());
    exception
        when other then
            v_step_end := current_timestamp();
            insert into MIP.APP.MIP_AUDIT_LOG (
                EVENT_TS,
                RUN_ID,
                EVENT_TYPE,
                EVENT_NAME,
                STATUS,
                ROWS_AFFECTED,
                DETAILS,
                ERROR_MESSAGE
            )
            select
                current_timestamp(),
                :v_run_id,
                'PIPELINE_STEP',
                'RETURNS_REFRESH',
                'FAIL',
                null,
                object_construct(
                    'step_name', 'returns_refresh',
                    'started_at', :v_step_start,
                    'completed_at', :v_step_end
                ),
                :sqlerrm;
            raise;
    end;
    v_step_end := current_timestamp();

    insert into MIP.APP.MIP_AUDIT_LOG (
        EVENT_TS,
        RUN_ID,
        EVENT_TYPE,
        EVENT_NAME,
        STATUS,
        ROWS_AFFECTED,
        DETAILS,
        ERROR_MESSAGE
    )
    select
        current_timestamp(),
        :v_run_id,
        'PIPELINE_STEP',
        'RETURNS_REFRESH',
        'SUCCESS',
        :v_returns_result:"returns_at_latest_ts"::number,
        object_construct(
            'step_name', 'returns_refresh',
            'started_at', :v_step_start,
            'completed_at', :v_step_end,
            'latest_market_bars_ts', :v_returns_result:"latest_market_bars_ts"::timestamp_ntz,
            'latest_market_returns_ts', :v_returns_result:"latest_market_returns_ts"::timestamp_ntz,
            'market_bars_at_latest_ts', :v_returns_result:"market_bars_at_latest_ts"::number,
            'returns_at_latest_ts', :v_returns_result:"returns_at_latest_ts"::number
        ),
        null;

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
                :v_interval_minutes
            ));
            v_recommendation_results := array_append(:v_recommendation_results, :v_recommendation_result);
        end for;
    exception
        when other then
            v_step_end := current_timestamp();
            insert into MIP.APP.MIP_AUDIT_LOG (
                EVENT_TS,
                RUN_ID,
                EVENT_TYPE,
                EVENT_NAME,
                STATUS,
                ROWS_AFFECTED,
                DETAILS,
                ERROR_MESSAGE
            )
            select
                current_timestamp(),
                :v_run_id,
                'PIPELINE_STEP',
                'RECOMMENDATIONS',
                'FAIL',
                null,
                object_construct(
                    'step_name', 'recommendations',
                    'started_at', :v_step_start,
                    'completed_at', :v_step_end,
                    'interval_minutes', :v_interval_minutes,
                    'market_type_count', :v_market_type_count
                ),
                :sqlerrm;
            raise;
    end;
    v_step_end := current_timestamp();

    select
        coalesce(sum(try_to_number(value:rows_before)), 0),
        coalesce(sum(try_to_number(value:rows_after)), 0),
        coalesce(sum(try_to_number(value:inserted_count)), 0)
      into :v_rows_before,
           :v_rows_after,
           :v_rows_delta
      from table(flatten(input => :v_recommendation_results));

    insert into MIP.APP.MIP_AUDIT_LOG (
        EVENT_TS,
        RUN_ID,
        EVENT_TYPE,
        EVENT_NAME,
        STATUS,
        ROWS_AFFECTED,
        DETAILS,
        ERROR_MESSAGE
    )
    select
        current_timestamp(),
        :v_run_id,
        'PIPELINE_STEP',
        'RECOMMENDATIONS',
        'SUCCESS',
        :v_rows_delta,
        object_construct(
            'step_name', 'recommendations',
            'started_at', :v_step_start,
            'completed_at', :v_step_end,
            'rows_before', :v_rows_before,
            'rows_after', :v_rows_after,
            'rows_delta', :v_rows_delta,
            'interval_minutes', :v_interval_minutes,
            'market_type_count', :v_market_type_count
        ),
        null;

    v_step_start := current_timestamp();
    begin
        v_eval_result := (call MIP.APP.SP_PIPELINE_EVALUATE_RECOMMENDATIONS(:v_from_ts, :v_effective_to_ts));
    exception
        when other then
            v_step_end := current_timestamp();
            insert into MIP.APP.MIP_AUDIT_LOG (
                EVENT_TS,
                RUN_ID,
                EVENT_TYPE,
                EVENT_NAME,
                STATUS,
                ROWS_AFFECTED,
                DETAILS,
                ERROR_MESSAGE
            )
            select
                current_timestamp(),
                :v_run_id,
                'PIPELINE_STEP',
                'EVALUATION',
                'FAIL',
                null,
                object_construct(
                    'step_name', 'evaluation',
                    'started_at', :v_step_start,
                    'completed_at', :v_step_end,
                    'from_ts', :v_from_ts,
                    'to_ts', :v_effective_to_ts
                ),
                :sqlerrm;
            raise;
    end;
    v_step_end := current_timestamp();

    insert into MIP.APP.MIP_AUDIT_LOG (
        EVENT_TS,
        RUN_ID,
        EVENT_TYPE,
        EVENT_NAME,
        STATUS,
        ROWS_AFFECTED,
        DETAILS,
        ERROR_MESSAGE
    )
    select
        current_timestamp(),
        :v_run_id,
        'PIPELINE_STEP',
        'EVALUATION',
        'SUCCESS',
        :v_eval_result:"rows_delta"::number,
        object_construct(
            'step_name', 'evaluation',
            'started_at', :v_step_start,
            'completed_at', :v_step_end,
            'rows_before', :v_eval_result:"rows_before"::number,
            'rows_after', :v_eval_result:"rows_after"::number,
            'rows_delta', :v_eval_result:"rows_delta"::number,
            'from_ts', :v_from_ts,
            'to_ts', :v_effective_to_ts
        ),
        null;

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
            insert into MIP.APP.MIP_AUDIT_LOG (
                EVENT_TS,
                RUN_ID,
                EVENT_TYPE,
                EVENT_NAME,
                STATUS,
                ROWS_AFFECTED,
                DETAILS,
                ERROR_MESSAGE
            )
            select
                current_timestamp(),
                :v_run_id,
                'PIPELINE_STEP',
                'PORTFOLIO_SIMULATION',
                'FAIL',
                null,
                object_construct(
                    'step_name', 'portfolio_simulation',
                    'started_at', :v_step_start,
                    'completed_at', :v_step_end,
                    'from_ts', :v_from_ts,
                    'to_ts', :v_effective_to_ts
                ),
                :sqlerrm;
            raise;
    end;
    v_step_end := current_timestamp();

    select
        coalesce(sum(try_to_number(value:trades)), 0),
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

    insert into MIP.APP.MIP_AUDIT_LOG (
        EVENT_TS,
        RUN_ID,
        EVENT_TYPE,
        EVENT_NAME,
        STATUS,
        ROWS_AFFECTED,
        DETAILS,
        ERROR_MESSAGE
    )
    select
        current_timestamp(),
        :v_run_id,
        'PIPELINE_STEP',
        'PORTFOLIO_SIMULATION',
        'SUCCESS',
        :v_portfolio_trades,
        object_construct(
            'step_name', 'portfolio_simulation',
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
        null;

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
        end if;

        select count(*)
          into :v_trusted_signal_rows_after
          from MIP.MART.V_TRUSTED_SIGNALS;

        v_trusted_signal_rows_delta := :v_trusted_signal_rows_after - :v_trusted_signal_rows_before;
    exception
        when other then
            v_step_end := current_timestamp();
            insert into MIP.APP.MIP_AUDIT_LOG (
                EVENT_TS,
                RUN_ID,
                EVENT_TYPE,
                EVENT_NAME,
                STATUS,
                ROWS_AFFECTED,
                DETAILS,
                ERROR_MESSAGE
            )
            select
                current_timestamp(),
                :v_run_id,
                'PIPELINE_STEP',
                'TRUSTED_SIGNAL_REFRESH',
                'FAIL',
                null,
                object_construct(
                    'step_name', 'trusted_signal_refresh',
                    'started_at', :v_step_start,
                    'completed_at', :v_step_end,
                    'procedure_name', :v_trusted_signal_proc
                ),
                :sqlerrm;
            raise;
    end;
    v_step_end := current_timestamp();

    insert into MIP.APP.MIP_AUDIT_LOG (
        EVENT_TS,
        RUN_ID,
        EVENT_TYPE,
        EVENT_NAME,
        STATUS,
        ROWS_AFFECTED,
        DETAILS,
        ERROR_MESSAGE
    )
    select
        current_timestamp(),
        :v_run_id,
        'PIPELINE_STEP',
        'TRUSTED_SIGNAL_REFRESH',
        :v_trusted_signal_status,
        :v_trusted_signal_rows_delta,
        object_construct(
            'step_name', 'trusted_signal_refresh',
            'started_at', :v_step_start,
            'completed_at', :v_step_end,
            'rows_before', :v_trusted_signal_rows_before,
            'rows_after', :v_trusted_signal_rows_after,
            'rows_delta', :v_trusted_signal_rows_delta,
            'procedure_name', :v_trusted_signal_proc,
            'procedure_status', :v_trusted_signal_status
        ),
        null;

    select max(try_to_number(replace(RUN_ID, 'T', '')))
      into :v_signal_run_id
      from MIP.APP.V_SIGNALS_ELIGIBLE_TODAY;

    if (v_signal_run_id is null) then
        select max(try_to_number(replace(RUN_ID, 'T', '')))
          into :v_signal_run_id
          from MIP.APP.V_SIGNALS_ELIGIBLE_TODAY;
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
           and try_to_number(replace(RUN_ID, 'T', '')) = :v_signal_run_id;

        v_brief_result := (call MIP.APP.SP_PIPELINE_WRITE_MORNING_BRIEFS(
            :v_run_id,
            :v_signal_run_id
        ));

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
            or (
                SIGNAL_RUN_ID is not null
                and (
                    SIGNAL_RUN_ID = to_varchar(:v_signal_run_id)
                    or try_to_number(replace(SIGNAL_RUN_ID, 'T', '')) = :v_signal_run_id
                )
            );
    else
        v_brief_result := (call MIP.APP.SP_PIPELINE_WRITE_MORNING_BRIEFS(:v_run_id, null));
    end if;

    v_brief_end := current_timestamp();
    v_proposer_end := :v_brief_end;
    v_executor_end := :v_brief_end;

    select count(*)
      into :v_brief_rows_after
      from MIP.AGENT_OUT.MORNING_BRIEF
     where PIPELINE_RUN_ID = :v_run_id;

    v_brief_rows_delta := :v_brief_rows_after - :v_brief_rows_before;

    select array_agg(BRIEF_ID order by BRIEF_ID)
      into :v_brief_ids
      from MIP.AGENT_OUT.MORNING_BRIEF
     where PIPELINE_RUN_ID = :v_run_id;

    v_brief_ids := coalesce(:v_brief_ids, array_construct());

    insert into MIP.APP.MIP_AUDIT_LOG (
        EVENT_TS,
        RUN_ID,
        EVENT_TYPE,
        EVENT_NAME,
        STATUS,
        ROWS_AFFECTED,
        DETAILS,
        ERROR_MESSAGE
    )
    select
        current_timestamp(),
        :v_run_id,
        'PIPELINE_STEP',
        'MORNING_BRIEF',
        'SUCCESS',
        :v_brief_rows_delta,
        object_construct(
            'step_name', 'morning_brief',
            'started_at', :v_brief_start,
            'completed_at', :v_brief_end,
            'rows_before', :v_brief_rows_before,
            'rows_after', :v_brief_rows_after,
            'rows_delta', :v_brief_rows_delta,
            'brief_count', :v_brief_rows_after,
            'brief_ids', :v_brief_ids,
            'signal_run_id', :v_signal_run_id
        ),
        null;

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

        insert into MIP.APP.MIP_AUDIT_LOG (
            EVENT_TS,
            RUN_ID,
            EVENT_TYPE,
            EVENT_NAME,
            STATUS,
            ROWS_AFFECTED,
            DETAILS,
            ERROR_MESSAGE
        )
        select
            current_timestamp(),
            :v_run_id,
            'PIPELINE_STEP',
            'PROPOSER',
            'SUCCESS',
            :v_proposals_delta,
            object_construct(
                'step_name', 'proposer',
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
            null;

        insert into MIP.APP.MIP_AUDIT_LOG (
            EVENT_TS,
            RUN_ID,
            EVENT_TYPE,
            EVENT_NAME,
            STATUS,
            ROWS_AFFECTED,
            DETAILS,
            ERROR_MESSAGE
        )
        select
            current_timestamp(),
            :v_run_id,
            'PIPELINE_STEP',
            'EXECUTOR',
            'SUCCESS',
            :v_executed_delta,
            object_construct(
                'step_name', 'executor',
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
            null;
    else
        insert into MIP.APP.MIP_AUDIT_LOG (
            EVENT_TS,
            RUN_ID,
            EVENT_TYPE,
            EVENT_NAME,
            STATUS,
            ROWS_AFFECTED,
            DETAILS,
            ERROR_MESSAGE
        )
        select
            current_timestamp(),
            :v_run_id,
            'PIPELINE_STEP',
            'PROPOSER',
            'SKIPPED_NO_SIGNAL_RUN_ID',
            0,
            object_construct(
                'step_name', 'proposer',
                'started_at', :v_proposer_start,
                'completed_at', :v_proposer_end,
                'signal_run_id', :v_signal_run_id,
                'reason', 'NO_SIGNAL_RUN_ID'
            ),
            null;

        insert into MIP.APP.MIP_AUDIT_LOG (
            EVENT_TS,
            RUN_ID,
            EVENT_TYPE,
            EVENT_NAME,
            STATUS,
            ROWS_AFFECTED,
            DETAILS,
            ERROR_MESSAGE
        )
        select
            current_timestamp(),
            :v_run_id,
            'PIPELINE_STEP',
            'EXECUTOR',
            'SKIPPED_NO_SIGNAL_RUN_ID',
            0,
            object_construct(
                'step_name', 'executor',
                'started_at', :v_executor_start,
                'completed_at', :v_executor_end,
                'signal_run_id', :v_signal_run_id,
                'reason', 'NO_SIGNAL_RUN_ID'
            ),
            null;
    end if;

    v_summary := object_construct(
        'run_id', :v_run_id,
        'from_ts', :v_from_ts,
        'requested_to_ts', :v_requested_to_ts,
        'effective_to_ts', :v_effective_to_ts,
        'latest_market_bars_ts', :v_latest_market_bars_ts,
        'ingestion', :v_ingest_result,
        'returns_refresh', :v_returns_result,
        'recommendations', :v_recommendation_results,
        'evaluation', :v_eval_result,
        'portfolio_simulation', :v_portfolio_result,
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
        'SUCCESS',
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
