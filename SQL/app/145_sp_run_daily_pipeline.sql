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
    v_from_ts          timestamp_ntz := dateadd(day, -90, current_timestamp()::timestamp_ntz);
    v_to_ts            timestamp_ntz := current_timestamp()::timestamp_ntz;
    v_msg_ingest       string;
    v_msg_returns      string;
    v_msg_signals      string;
    v_msg_eval         string;
    v_return_rows      number := 0;
    v_run_id           string := uuid_string();
    v_step_start       timestamp_ntz;
    v_step_end         timestamp_ntz;
    v_rows_before      number := 0;
    v_rows_after       number := 0;
    v_rows_delta       number := 0;
    v_market_type      string;
    v_market_types     resultset;
    v_market_type_count number := 0;
    v_pattern_count    number := 0;
    v_min_return       number := 0.0;
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

    v_step_start := MIP.APP.F_NOW_BERLIN_NTZ();
    select count(*)
      into :v_rows_before
      from MIP.MART.MARKET_BARS;
    begin
        call MIP.APP.SP_INGEST_ALPHAVANTAGE_BARS();
        select count(*)
          into :v_rows_after
          from MIP.MART.MARKET_BARS;
        v_rows_delta := v_rows_after - v_rows_before;
        v_step_end := MIP.APP.F_NOW_BERLIN_NTZ();
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
            MIP.APP.F_NOW_BERLIN_NTZ(),
            :v_run_id,
            'PIPELINE_STEP',
            'INGESTION',
            'SUCCESS',
            :v_rows_delta,
            object_construct(
                'step_name', 'ingestion',
                'market_type', null,
                'interval_minutes', null,
                'started_at', :v_step_start,
                'completed_at', :v_step_end,
                'rows_before', :v_rows_before,
                'rows_after', :v_rows_after
            ),
            null;
    exception
        when other then
            v_step_end := MIP.APP.F_NOW_BERLIN_NTZ();
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
                MIP.APP.F_NOW_BERLIN_NTZ(),
                :v_run_id,
                'PIPELINE_STEP',
                'INGESTION',
                'FAIL',
                null,
                object_construct(
                    'step_name', 'ingestion',
                    'market_type', null,
                    'interval_minutes', null,
                    'started_at', :v_step_start,
                    'completed_at', :v_step_end
                ),
                :sqlerrm;
            raise;
    end;
    v_msg_ingest := 'Ingestion completed for enabled universe.';

    create or replace temporary table MIP.APP.TMP_MARKET_TYPES (MARKET_TYPE string);
    insert into MIP.APP.TMP_MARKET_TYPES (MARKET_TYPE)
    select distinct MARKET_TYPE
      from MIP.APP.INGEST_UNIVERSE
     where coalesce(IS_ENABLED, true);

    select count(*)
      into :v_market_type_count
      from MIP.APP.TMP_MARKET_TYPES;

    if (v_market_type_count = 0) then
        insert into MIP.APP.TMP_MARKET_TYPES (MARKET_TYPE)
        select distinct MARKET_TYPE
          from MIP.MART.MARKET_BARS
         where TS >= dateadd(day, -7, current_timestamp()::timestamp_ntz);
    end if;

    v_step_start := MIP.APP.F_NOW_BERLIN_NTZ();
    select count(*)
      into :v_rows_before
      from MIP.MART.MARKET_RETURNS;
    begin
        create or replace view MIP.MART.MARKET_RETURNS as
        with deduped as (
            select
                TS,
                SYMBOL,
                SOURCE,
                MARKET_TYPE,
                INTERVAL_MINUTES,
                OPEN,
                HIGH,
                LOW,
                CLOSE,
                VOLUME,
                INGESTED_AT
            from (
                select
                    TS,
                    SYMBOL,
                    SOURCE,
                    MARKET_TYPE,
                    INTERVAL_MINUTES,
                    OPEN,
                    HIGH,
                    LOW,
                    CLOSE,
                    VOLUME,
                    INGESTED_AT,
                    row_number() over (
                        partition by MARKET_TYPE, SYMBOL, INTERVAL_MINUTES, TS
                        order by INGESTED_AT desc, SOURCE desc
                    ) as RN
                from MIP.MART.MARKET_BARS
            )
            where RN = 1
        ),
        ordered as (
            select
                TS,
                SYMBOL,
                SOURCE,
                MARKET_TYPE,
                INTERVAL_MINUTES,
                OPEN,
                HIGH,
                LOW,
                CLOSE,
                VOLUME,
                INGESTED_AT,
                lag(CLOSE) over (
                    partition by SYMBOL, MARKET_TYPE, INTERVAL_MINUTES
                    order by TS
                ) as PREV_CLOSE
            from deduped
        )
        select
            TS,
            SYMBOL,
            SOURCE,
            MARKET_TYPE,
            INTERVAL_MINUTES,
            OPEN,
            HIGH,
            LOW,
            CLOSE,
            VOLUME,
            INGESTED_AT,
            PREV_CLOSE,
            case
                when PREV_CLOSE is not null and PREV_CLOSE <> 0
                then (CLOSE - PREV_CLOSE) / PREV_CLOSE
                else null
            end as RETURN_SIMPLE,
            case
                when PREV_CLOSE is not null and PREV_CLOSE > 0 and CLOSE > 0
                then ln(CLOSE / PREV_CLOSE)
                else null
            end as RETURN_LOG
        from ordered;

        select count(*)
          into :v_rows_after
          from MIP.MART.MARKET_RETURNS;
        v_rows_delta := v_rows_after - v_rows_before;
        v_step_end := MIP.APP.F_NOW_BERLIN_NTZ();
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
            MIP.APP.F_NOW_BERLIN_NTZ(),
            :v_run_id,
            'PIPELINE_STEP',
            'RETURNS_REFRESH',
            'SUCCESS',
            :v_rows_delta,
            object_construct(
                'step_name', 'returns_refresh',
                'market_type', null,
                'interval_minutes', null,
                'started_at', :v_step_start,
                'completed_at', :v_step_end,
                'rows_before', :v_rows_before,
                'rows_after', :v_rows_after
            ),
            null;
    exception
        when other then
            v_step_end := MIP.APP.F_NOW_BERLIN_NTZ();
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
                MIP.APP.F_NOW_BERLIN_NTZ(),
                :v_run_id,
                'PIPELINE_STEP',
                'RETURNS_REFRESH',
                'FAIL',
                null,
                object_construct(
                    'step_name', 'returns_refresh',
                    'market_type', null,
                    'interval_minutes', null,
                    'started_at', :v_step_start,
                    'completed_at', :v_step_end
                ),
                :sqlerrm;
            raise;
    end;

    select count(*)
      into :v_return_rows
      from MIP.MART.MARKET_RETURNS;
    v_msg_returns := 'Market returns refreshed (' || v_return_rows || ' rows).';

    v_market_types := (
        select MARKET_TYPE
          from MIP.APP.TMP_MARKET_TYPES
         order by MARKET_TYPE
    );

    for rec in v_market_types do
        v_market_type := rec.MARKET_TYPE;
        v_min_return := 0.0;

        select count(*)
          into :v_pattern_count
          from MIP.APP.PATTERN_DEFINITION
         where coalesce(IS_ACTIVE, 'N') = 'Y'
           and coalesce(ENABLED, true)
           and upper(coalesce(PARAMS_JSON:market_type::string, 'STOCK')) = upper(:v_market_type);

        if (v_pattern_count = 0) then
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
                MIP.APP.F_NOW_BERLIN_NTZ(),
                :v_run_id,
                'PIPELINE_STEP',
                'PATTERN_CHECK',
                'WARN',
                null,
                object_construct(
                    'step_name', 'pattern_check',
                    'market_type', :v_market_type,
                    'interval_minutes', null,
                    'started_at', MIP.APP.F_NOW_BERLIN_NTZ(),
                    'completed_at', MIP.APP.F_NOW_BERLIN_NTZ(),
                    'message', 'No active patterns matched market type.'
                ),
                null;
        end if;

        v_step_start := MIP.APP.F_NOW_BERLIN_NTZ();
        select count(*)
          into :v_rows_before
          from MIP.APP.RECOMMENDATION_LOG
         where MARKET_TYPE = :v_market_type;
        begin
            -- Do not pass NULL min_return; null comparisons can suppress inserts.
            call MIP.APP.SP_GENERATE_MOMENTUM_RECS(:v_min_return, :v_market_type, null, null, null);

            select count(*)
              into :v_rows_after
              from MIP.APP.RECOMMENDATION_LOG
             where MARKET_TYPE = :v_market_type;
            v_rows_delta := v_rows_after - v_rows_before;
            v_step_end := MIP.APP.F_NOW_BERLIN_NTZ();
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
                MIP.APP.F_NOW_BERLIN_NTZ(),
                :v_run_id,
                'PIPELINE_STEP',
                'RECOMMENDATIONS',
                'SUCCESS',
                :v_rows_delta,
                object_construct(
                    'step_name', 'recommendations',
                    'market_type', :v_market_type,
                    'interval_minutes', null,
                    'started_at', :v_step_start,
                    'completed_at', :v_step_end,
                    'rows_before', :v_rows_before,
                    'rows_after', :v_rows_after,
                    'min_return', :v_min_return,
                    'interval_minutes_param', null,
                    'lookback_days', null,
                    'min_zscore', null
                ),
                null;
        exception
            when other then
                v_step_end := MIP.APP.F_NOW_BERLIN_NTZ();
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
                    MIP.APP.F_NOW_BERLIN_NTZ(),
                    :v_run_id,
                    'PIPELINE_STEP',
                    'RECOMMENDATIONS',
                    'FAIL',
                    null,
                    object_construct(
                        'step_name', 'recommendations',
                        'market_type', :v_market_type,
                        'interval_minutes', null,
                        'started_at', :v_step_start,
                        'completed_at', :v_step_end,
                        'min_return', :v_min_return,
                        'interval_minutes_param', null,
                        'lookback_days', null,
                        'min_zscore', null
                    ),
                    :sqlerrm;
                raise;
        end;
    end for;
    v_msg_signals := 'Momentum recommendations generated for active daily patterns.';

    v_step_start := MIP.APP.F_NOW_BERLIN_NTZ();
    select count(*)
      into :v_rows_before
      from MIP.APP.RECOMMENDATION_OUTCOMES;
    begin
        call MIP.APP.SP_EVALUATE_RECOMMENDATIONS(:v_from_ts, :v_to_ts);
        select count(*)
          into :v_rows_after
          from MIP.APP.RECOMMENDATION_OUTCOMES;
        v_rows_delta := v_rows_after - v_rows_before;
        v_step_end := MIP.APP.F_NOW_BERLIN_NTZ();
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
            MIP.APP.F_NOW_BERLIN_NTZ(),
            :v_run_id,
            'PIPELINE_STEP',
            'EVALUATION',
            'SUCCESS',
            :v_rows_delta,
            object_construct(
                'step_name', 'evaluation',
                'market_type', null,
                'interval_minutes', null,
                'started_at', :v_step_start,
                'completed_at', :v_step_end,
                'rows_before', :v_rows_before,
                'rows_after', :v_rows_after
            ),
            null;
    exception
        when other then
            v_step_end := MIP.APP.F_NOW_BERLIN_NTZ();
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
                MIP.APP.F_NOW_BERLIN_NTZ(),
                :v_run_id,
                'PIPELINE_STEP',
                'EVALUATION',
                'FAIL',
                null,
                object_construct(
                    'step_name', 'evaluation',
                    'market_type', null,
                    'interval_minutes', null,
                    'started_at', :v_step_start,
                    'completed_at', :v_step_end
                ),
                :sqlerrm;
            raise;
    end;
    v_msg_eval := 'Recommendation outcomes evaluated for last 90 days.';

    call MIP.APP.SP_LOG_EVENT(
        'PIPELINE',
        'SP_RUN_DAILY_PIPELINE',
        'SUCCESS',
        :v_return_rows,
        object_construct(
            'from_ts', :v_from_ts,
            'to_ts', :v_to_ts,
            'market_returns_rows', :v_return_rows,
            'msg_ingest', :v_msg_ingest,
            'msg_returns', :v_msg_returns,
            'msg_signals', :v_msg_signals,
            'msg_evaluate', :v_msg_eval
        ),
        null,
        :v_run_id,
        null
    );

    return object_construct(
        'from_ts', :v_from_ts,
        'to_ts', :v_to_ts,
        'market_returns_rows', :v_return_rows,
        'msg_ingest', :v_msg_ingest,
        'msg_returns', :v_msg_returns,
        'msg_signals', :v_msg_signals,
        'msg_evaluate', :v_msg_eval
    );
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
