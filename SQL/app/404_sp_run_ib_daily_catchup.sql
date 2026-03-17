-- 404_sp_run_ib_daily_catchup.sql
-- Purpose: IB-only daily catch-up orchestration for missed days (research/training flow).

use role MIP_ADMIN_ROLE;
use database MIP;

create table if not exists MIP.APP.IB_DAILY_CATCHUP_LOG (
    BATCH_ID        string,
    TARGET_DATE     date,
    DAY_DATE        date,
    STATUS          string,         -- PLANNED | SUCCESS | FAIL
    MESSAGE         string,
    DETAILS         variant,
    CREATED_AT      timestamp_ntz default current_timestamp()
);

create or replace procedure MIP.APP.SP_RUN_IB_DAILY_CATCHUP(
    P_TARGET_DATE date default current_date(),
    P_DRY_RUN boolean default true
)
returns variant
language sql
execute as caller
as
$$
declare
    v_target_date date := coalesce(:P_TARGET_DATE, to_date(convert_timezone('America/New_York', current_timestamp())));
    v_last_success_date date;
    v_from_date date;
    v_target_symbol_count number := 0;
    v_missing_days number := 0;
    v_processed_days number := 0;
    v_missing_from date;
    v_missing_to date;
    v_batch_id string := uuid_string();
    v_days array := array_construct();
    v_replay_result variant;
    v_error_message string;
begin
    -- Last successfully replayed day from catch-up log; fallback to latest daily pipeline success.
    select max(DAY_DATE)
      into :v_last_success_date
      from (
          select max(DAY_DATE) as DAY_DATE
          from MIP.APP.IB_DAILY_CATCHUP_LOG
          where STATUS = 'SUCCESS'
          union all
          select max(to_date(DETAILS:effective_to_ts::timestamp_ntz)) as DAY_DATE
          from MIP.APP.MIP_AUDIT_LOG
          where EVENT_TYPE = 'PIPELINE'
            and EVENT_NAME = 'SP_RUN_DAILY_PIPELINE'
            and STATUS in ('SUCCESS', 'SUCCESS_WITH_SKIPS')
      ) s;

    v_from_date := iff(:v_last_success_date is null, :v_target_date, dateadd(day, 1, :v_last_success_date));
    if (v_from_date > v_target_date) then
        return object_construct(
            'status', 'SUCCESS',
            'batch_id', :v_batch_id,
            'dry_run', :P_DRY_RUN,
            'message', 'Already up to date.',
            'target_date', :v_target_date,
            'from_date', :v_from_date,
            'missing_days', 0,
            'days', array_construct()
        );
    end if;

    select count(*)
      into :v_target_symbol_count
      from (
            select distinct upper(SYMBOL) as SYMBOL, upper(MARKET_TYPE) as MARKET_TYPE
            from MIP.APP.INGEST_UNIVERSE
            where coalesce(IS_ENABLED, true)
              and INTERVAL_MINUTES = 1440
      );

    -- Missing days = dates in range where full IBKR daily-bar universe exists and day not yet marked SUCCESS.
    with day_series as (
        select dateadd(day, g.N, :v_from_date) as DAY_DATE
        from (
            select seq4() as N
            from table(generator(rowcount => 4000))
        ) g
        where dateadd(day, g.N, :v_from_date) <= :v_target_date
    ),
    universe as (
        select distinct upper(SYMBOL) as SYMBOL, upper(MARKET_TYPE) as MARKET_TYPE
        from MIP.APP.INGEST_UNIVERSE
        where coalesce(IS_ENABLED, true)
          and INTERVAL_MINUTES = 1440
    ),
    coverage as (
        select
            mb.TS::date as DAY_DATE,
            count(distinct upper(mb.SYMBOL) || '|' || upper(mb.MARKET_TYPE)) as COVERED
        from MIP.MART.MARKET_BARS mb
        join universe u
          on u.SYMBOL = upper(mb.SYMBOL)
         and u.MARKET_TYPE = upper(mb.MARKET_TYPE)
        where mb.INTERVAL_MINUTES = 1440
          and upper(coalesce(mb.SOURCE, '')) = 'IBKR'
          and mb.TS::date between :v_from_date and :v_target_date
        group by mb.TS::date
    ),
    eligible_days as (
        select d.DAY_DATE
        from day_series d
        join coverage c
          on c.DAY_DATE = d.DAY_DATE
        where c.COVERED = :v_target_symbol_count
    ),
    missing as (
        select e.DAY_DATE
        from eligible_days e
        left join MIP.APP.IB_DAILY_CATCHUP_LOG l
          on l.DAY_DATE = e.DAY_DATE
         and l.STATUS = 'SUCCESS'
        where l.DAY_DATE is null
        order by e.DAY_DATE
    )
    select count(*), array_agg(DAY_DATE), min(DAY_DATE), max(DAY_DATE)
      into :v_missing_days, :v_days, :v_missing_from, :v_missing_to
      from missing;

    v_days := coalesce(:v_days, array_construct());

    if (:P_DRY_RUN) then
        return object_construct(
            'status', 'DRY_RUN',
            'batch_id', :v_batch_id,
            'target_date', :v_target_date,
            'from_date', :v_from_date,
            'last_success_date', :v_last_success_date,
            'target_symbol_count', :v_target_symbol_count,
            'missing_days', :v_missing_days,
            'days', :v_days
        );
    end if;

    if (:v_missing_days = 0 or :v_missing_from is null or :v_missing_to is null) then
        return object_construct(
            'status', 'SUCCESS',
            'batch_id', :v_batch_id,
            'target_date', :v_target_date,
            'last_success_date', :v_last_success_date,
            'target_symbol_count', :v_target_symbol_count,
            'missing_days', :v_missing_days,
            'processed_days', 0,
            'days', :v_days
        );
    end if;

    begin
        insert into MIP.APP.IB_DAILY_CATCHUP_LOG (BATCH_ID, TARGET_DATE, DAY_DATE, STATUS, MESSAGE, DETAILS)
        values (
            :v_batch_id,
            :v_target_date,
            :v_missing_from,
            'PLANNED',
            'Starting replay range',
            object_construct('dry_run', false, 'from_date', :v_missing_from, 'to_date', :v_missing_to)
        );

        v_replay_result := (call MIP.APP.SP_REPLAY_TIME_TRAVEL(:v_missing_from, :v_missing_to, false, true));
        v_processed_days := :v_missing_days;

        insert into MIP.APP.IB_DAILY_CATCHUP_LOG (BATCH_ID, TARGET_DATE, DAY_DATE, STATUS, MESSAGE, DETAILS)
        values (
            :v_batch_id,
            :v_target_date,
            :v_missing_to,
            'SUCCESS',
            'Replay range completed',
            :v_replay_result
        );
    exception
        when other then
            v_error_message := sqlerrm;
            insert into MIP.APP.IB_DAILY_CATCHUP_LOG (BATCH_ID, TARGET_DATE, DAY_DATE, STATUS, MESSAGE, DETAILS)
            values (
                :v_batch_id,
                :v_target_date,
                :v_missing_from,
                'FAIL',
                :v_error_message,
                object_construct('sqlstate', sqlstate, 'from_date', :v_missing_from, 'to_date', :v_missing_to)
            );
            return object_construct(
                'status', 'FAIL',
                'batch_id', :v_batch_id,
                'target_date', :v_target_date,
                'processed_days', 0,
                'failed_from', :v_missing_from,
                'failed_to', :v_missing_to,
                'error', :v_error_message
            );
    end;

    return object_construct(
        'status', 'SUCCESS',
        'batch_id', :v_batch_id,
        'target_date', :v_target_date,
        'last_success_date', :v_last_success_date,
        'target_symbol_count', :v_target_symbol_count,
        'missing_days', :v_missing_days,
        'processed_days', :v_processed_days,
        'days', :v_days
    );
end;
$$;
