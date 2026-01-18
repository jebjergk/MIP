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
    v_from_ts          timestamp_ntz := dateadd(day, -90, current_date());
    v_to_ts            timestamp_ntz := current_timestamp();
    v_msg_ingest       string;
    v_msg_returns      string;
    v_msg_signals      string;
    v_msg_eval         string;
    v_return_rows      number := 0;
    v_run_id           string := uuid_string();
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

    call MIP.APP.SP_INGEST_ALPHAVANTAGE_BARS();
    v_msg_ingest := 'Ingestion completed for enabled universe.';

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
      into :v_return_rows
      from MIP.MART.MARKET_RETURNS;
    v_msg_returns := 'Market returns refreshed (' || v_return_rows || ' rows).';

    call MIP.APP.SP_GENERATE_MOMENTUM_RECS(null, 'STOCK', null, null, null);
    call MIP.APP.SP_GENERATE_MOMENTUM_RECS(null, 'FX', null, null, null);
    call MIP.APP.SP_GENERATE_MOMENTUM_RECS(null, 'ETF', null, null, null);
    v_msg_signals := 'Momentum recommendations generated for active daily patterns.';

    call MIP.APP.SP_EVALUATE_RECOMMENDATIONS(:v_from_ts, :v_to_ts);
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
