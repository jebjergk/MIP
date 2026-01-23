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
    v_to_ts timestamp_ntz := current_timestamp()::timestamp_ntz;
    v_run_id string := uuid_string();
    v_market_types resultset;
    v_market_type string;
    v_market_type_count number := 0;
    v_ingest_result variant;
    v_returns_result variant;
    v_eval_result variant;
    v_portfolio_result variant;
    v_brief_result variant;
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
begin
    execute immediate 'alter session set query_tag = ''' || v_run_id || '''';

    call MIP.APP.SP_LOG_EVENT(
        'PIPELINE',
        'SP_RUN_DAILY_PIPELINE',
        'START',
        null,
        object_construct('from_ts', :v_from_ts, 'to_ts', :v_to_ts),
        null,
        :v_run_id,
        null
    );

    v_ingest_result := (call MIP.APP.SP_PIPELINE_INGEST());
    v_returns_result := (call MIP.APP.SP_PIPELINE_REFRESH_RETURNS());

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

    for rec in v_market_types do
        v_market_type := rec.MARKET_TYPE;
        v_recommendation_result := (call MIP.APP.SP_PIPELINE_GENERATE_RECOMMENDATIONS(
            :v_market_type,
            :v_interval_minutes
        ));
        v_recommendation_results := array_append(:v_recommendation_results, :v_recommendation_result);
    end for;

    v_eval_result := (call MIP.APP.SP_PIPELINE_EVALUATE_RECOMMENDATIONS(:v_from_ts, :v_to_ts));
    v_portfolio_result := (call MIP.APP.SP_PIPELINE_RUN_PORTFOLIOS(:v_from_ts, :v_to_ts, :v_run_id));

    select max(try_to_number(replace(RUN_ID, 'T', '')))
      into :v_signal_run_id
      from MIP.APP.V_SIGNALS_ELIGIBLE_TODAY;

    if (v_signal_run_id is null) then
        select max(try_to_number(replace(RUN_ID, 'T', '')))
          into :v_signal_run_id
          from MIP.APP.V_SIGNALS_ELIGIBLE_TODAY;
    end if;

    if (v_signal_run_id is not null) then
        select count(*)
          into :v_eligible_signal_count
          from MIP.APP.V_SIGNALS_ELIGIBLE_TODAY
         where IS_ELIGIBLE
           and try_to_number(replace(RUN_ID, 'T', '')) = :v_signal_run_id;

        v_brief_result := (call MIP.APP.SP_PIPELINE_WRITE_MORNING_BRIEFS(
            :v_run_id,
            :v_signal_run_id
        ));

        select count(*)
          into :v_proposed_count
          from MIP.AGENT_OUT.ORDER_PROPOSALS
         where RUN_ID = :v_signal_run_id;

        select count(*)
          into :v_approved_count
          from MIP.AGENT_OUT.ORDER_PROPOSALS
         where RUN_ID = :v_signal_run_id
           and STATUS in ('APPROVED', 'EXECUTED');

        select count(*)
          into :v_rejected_count
          from MIP.AGENT_OUT.ORDER_PROPOSALS
         where RUN_ID = :v_signal_run_id
           and STATUS = 'REJECTED';

        select count(*)
          into :v_executed_count
          from MIP.AGENT_OUT.ORDER_PROPOSALS
         where RUN_ID = :v_signal_run_id
           and STATUS = 'EXECUTED';
    else
        v_brief_result := (call MIP.APP.SP_PIPELINE_WRITE_MORNING_BRIEFS(:v_run_id, null));
    end if;

    v_summary := object_construct(
        'run_id', :v_run_id,
        'from_ts', :v_from_ts,
        'to_ts', :v_to_ts,
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
            object_construct('from_ts', :v_from_ts, 'to_ts', :v_to_ts),
            :sqlerrm,
            :v_run_id,
            null
        );
        raise;
end;
$$;
