-- 149_sp_replay_time_travel.sql
-- Purpose: One-off historical replay (time travel) excluding ingestion.
-- Loops day-by-day from P_FROM_DATE to P_TO_DATE, sets effective_to_ts per day,
-- runs returns refresh, recommendations, evaluation, and optionally portfolio + briefs.
-- Does NOT call ingestion. Logs REPLAY events via SP_LOG_EVENT.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_REPLAY_TIME_TRAVEL(
    P_FROM_DATE        date,
    P_TO_DATE          date,
    P_RUN_PORTFOLIOS   boolean default false,
    P_RUN_BRIEFS       boolean default false
)
returns variant
language sql
execute as caller
as
$$
declare
    v_from_ts          timestamp_ntz;
    v_to_ts            timestamp_ntz;
    v_effective_to_ts  timestamp_ntz;
    v_run_id           string;
    v_d                date := to_date(:P_FROM_DATE);
    v_end              date := to_date(:P_TO_DATE);
    v_interval_minutes number := 1440;
    v_market_types     resultset;
    v_market_type      string;
    v_portfolios       resultset;
    v_portfolio_id     number;
    v_returns_result   variant;
    v_eval_result      variant;
    v_summary          variant := object_construct();
    v_day_count        number := 0;
    v_replay_run_id    string := uuid_string();
begin
    if (v_d > v_end) then
        return object_construct('status', 'SKIP', 'reason', 'from_date > to_date', 'day_count', 0);
    end if;

    call MIP.APP.SP_LOG_EVENT(
        'REPLAY',
        'SP_REPLAY_TIME_TRAVEL',
        'START',
        null,
        object_construct(
            'from_date', :P_FROM_DATE,
            'to_date', :P_TO_DATE,
            'run_portfolios', :P_RUN_PORTFOLIOS,
            'run_briefs', :P_RUN_BRIEFS,
            'replay_batch_id', :v_replay_run_id
        ),
        null,
        :v_replay_run_id,
        null
    );

    while (v_d <= v_end) do
        v_run_id := uuid_string();
        v_effective_to_ts := dateadd(second, -1, dateadd(day, 1, to_timestamp_ntz(:v_d)));

        execute immediate 'alter session set query_tag = ''' || :v_run_id || '''';

        call MIP.APP.SP_LOG_EVENT(
            'REPLAY',
            'REPLAY_DAY',
            'START',
            null,
            object_construct('effective_to_ts', :v_effective_to_ts, 'run_id', :v_run_id, 'day', :v_d),
            null,
            :v_run_id,
            :v_replay_run_id
        );

        call MIP.APP.SP_ENFORCE_RUN_SCOPING(:v_run_id, null, :v_effective_to_ts);

        v_returns_result := (call MIP.APP.SP_PIPELINE_REFRESH_RETURNS(:v_run_id));

        create or replace temporary table MIP.APP.TMP_PIPELINE_MARKET_TYPES (MARKET_TYPE string);
        insert into MIP.APP.TMP_PIPELINE_MARKET_TYPES (MARKET_TYPE)
        select distinct MARKET_TYPE
          from MIP.APP.INGEST_UNIVERSE
         where coalesce(IS_ENABLED, true);
        insert into MIP.APP.TMP_PIPELINE_MARKET_TYPES (MARKET_TYPE)
        select distinct b.MARKET_TYPE
          from MIP.MART.MARKET_BARS b
         where b.TS >= dateadd(day, -7, :v_effective_to_ts)
           and b.TS <= :v_effective_to_ts
           and b.MARKET_TYPE not in (select MARKET_TYPE from MIP.APP.TMP_PIPELINE_MARKET_TYPES);

        v_market_types := (select MARKET_TYPE from MIP.APP.TMP_PIPELINE_MARKET_TYPES order by MARKET_TYPE);
        for rec in v_market_types do
            v_market_type := rec.MARKET_TYPE;
            call MIP.APP.SP_PIPELINE_GENERATE_RECOMMENDATIONS(:v_market_type, :v_interval_minutes, :v_run_id);
        end for;

        v_from_ts := dateadd(day, -90, :v_effective_to_ts);
        v_eval_result := (call MIP.APP.SP_PIPELINE_EVALUATE_RECOMMENDATIONS(:v_from_ts, :v_effective_to_ts, :v_run_id));

        if (:P_RUN_PORTFOLIOS) then
            v_portfolios := (
                select PORTFOLIO_ID
                  from MIP.APP.PORTFOLIO
                 where STATUS = 'ACTIVE'
                 order by PORTFOLIO_ID
            );
            for rec in v_portfolios do
                v_portfolio_id := rec.PORTFOLIO_ID;
                call MIP.APP.SP_PIPELINE_RUN_PORTFOLIO(
                    :v_portfolio_id,
                    :v_from_ts,
                    :v_effective_to_ts,
                    :v_run_id,
                    :v_run_id
                );
            end for;
        end if;

        if (:P_RUN_BRIEFS) then
            v_portfolios := (
                select PORTFOLIO_ID
                  from MIP.APP.PORTFOLIO
                 where STATUS = 'ACTIVE'
                 order by PORTFOLIO_ID
            );
            for rec in v_portfolios do
                v_portfolio_id := rec.PORTFOLIO_ID;
                call MIP.APP.SP_PIPELINE_WRITE_MORNING_BRIEF(
                    :v_portfolio_id,
                    :v_effective_to_ts,
                    :v_run_id,
                    :v_run_id
                );
            end for;
        end if;

        call MIP.APP.SP_LOG_EVENT(
            'REPLAY',
            'REPLAY_DAY',
            'SUCCESS',
            null,
            object_construct(
                'effective_to_ts', :v_effective_to_ts,
                'run_id', :v_run_id,
                'day', :v_d,
                'returns_result', :v_returns_result,
                'evaluation_result', :v_eval_result
            ),
            null,
            :v_run_id,
            :v_replay_run_id
        );

        delete from MIP.APP.RUN_SCOPE_OVERRIDE where RUN_ID = :v_run_id;

        v_day_count := v_day_count + 1;
        v_d := dateadd(day, 1, v_d);
    end while;

    v_summary := object_construct(
        'status', 'SUCCESS',
        'from_date', :P_FROM_DATE,
        'to_date', :P_TO_DATE,
        'day_count', :v_day_count,
        'run_portfolios', :P_RUN_PORTFOLIOS,
        'run_briefs', :P_RUN_BRIEFS,
        'replay_batch_id', :v_replay_run_id
    );

    call MIP.APP.SP_LOG_EVENT(
        'REPLAY',
        'SP_REPLAY_TIME_TRAVEL',
        'SUCCESS',
        :v_day_count,
        :v_summary,
        null,
        :v_replay_run_id,
        null
    );

    return :v_summary;
exception
    when other then
        call MIP.APP.SP_LOG_EVENT(
            'REPLAY',
            'SP_REPLAY_TIME_TRAVEL',
            'FAIL',
            null,
            object_construct(
                'from_date', :P_FROM_DATE,
                'to_date', :P_TO_DATE,
                'day_count', :v_day_count,
                'replay_batch_id', :v_replay_run_id
            ),
            :sqlerrm,
            :v_replay_run_id,
            null
        );
        raise;
end;
$$;
