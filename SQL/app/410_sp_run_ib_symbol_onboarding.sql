-- 410_sp_run_ib_symbol_onboarding.sql
-- Purpose: Batch onboarding for IB symbols with immediate daily bootstrap training.

use role MIP_ADMIN_ROLE;
use database MIP;

alter table MIP.APP.INGEST_UNIVERSE
    add column if not exists SYMBOL_COHORT string;

create table if not exists MIP.APP.IB_SYMBOL_ONBOARDING_RUN_LOG (
    RUN_ID                    string        not null,
    SYMBOL_COHORT             string        not null,
    MARKET_TYPE               string        not null,
    START_DATE                date          not null,
    END_DATE                  date          not null,
    AUTO_ACTIVATE_IF_TRUSTED  boolean       not null,
    STATUS                    string,
    STARTED_AT                timestamp_ntz default current_timestamp(),
    FINISHED_AT               timestamp_ntz,
    SYMBOLS_REQUESTED         number,
    SYMBOLS_READY             number,
    SYMBOLS_ACTIVATED         number,
    DETAILS                   variant,
    FAILURES                  variant,
    constraint PK_IB_SYMBOL_ONBOARDING_RUN_LOG primary key (RUN_ID)
);

create table if not exists MIP.APP.IB_SYMBOL_ONBOARDING_SYMBOL_LOG (
    RUN_ID             string        not null,
    SYMBOL             string        not null,
    MARKET_TYPE        string        not null,
    SYMBOL_COHORT      string        not null,
    STATUS             string,
    READY_FLAG         boolean,
    TRUSTED_LEVEL      string,
    ACTIVATED_FLAG     boolean,
    REASON             string,
    FIRST_BAR_DATE     date,
    LAST_BAR_DATE      date,
    BAR_COUNT          number,
    DETAILS            variant,
    UPDATED_AT         timestamp_ntz default current_timestamp(),
    constraint PK_IB_SYMBOL_ONBOARDING_SYMBOL_LOG primary key (RUN_ID, SYMBOL, MARKET_TYPE)
);

create table if not exists MIP.APP.IB_SYMBOL_TRADE_ACTIVATION (
    SYMBOL               string        not null,
    MARKET_TYPE          string        not null,
    IS_ACTIVE_FOR_TRADE  boolean       not null,
    ACTIVATED_AT         timestamp_ntz,
    LAST_RUN_ID          string,
    SOURCE               string,
    REASON               string,
    UPDATED_AT           timestamp_ntz default current_timestamp(),
    constraint PK_IB_SYMBOL_TRADE_ACTIVATION primary key (SYMBOL, MARKET_TYPE)
);

create or replace procedure MIP.APP.SP_BOOTSTRAP_DAILY_TRAINING_FOR_NEW_SYMBOLS_IB(
    P_RUN_ID string default null,
    P_START_DATE date default '2025-08-01',
    P_END_DATE date default current_date(),
    P_SYMBOL_COHORT string default 'IB_ONBOARDING',
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
    v_start_date date := coalesce(:P_START_DATE, to_date('2025-08-01'));
    v_end_date date := coalesce(:P_END_DATE, current_date());
    v_symbol_cohort string := coalesce(:P_SYMBOL_COHORT, 'IB_ONBOARDING');
    v_market_type_filter string := :P_MARKET_TYPE;
    v_warehouse_override string := :P_WAREHOUSE_OVERRIDE;
    v_d date;
    v_effective_to_ts timestamp_ntz;
    v_day_run_id string;
    v_returns_result variant;
    v_gen_result variant;
    v_eval_result variant;
    v_market_types resultset;
    v_market_type string;
    v_signals_created_total number := 0;
    v_outcomes_total number := 0;
    v_trust_total number := 0;
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

    if (v_warehouse_override is not null) then
        execute immediate 'use warehouse ' || :v_warehouse_override;
    end if;

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

    return object_construct(
        'status', :v_status,
        'run_id', :v_run_id,
        'symbol_cohort', :v_symbol_cohort,
        'market_type', :v_market_type_filter,
        'start_date', :v_start_date,
        'end_date', :v_end_date,
        'days_processed', :v_days_processed,
        'signals_created_count', :v_signals_created_total,
        'outcomes_computed_count', :v_outcomes_total,
        'trust_rows_updated_count', :v_trust_total,
        'failures', :v_failures
    );
end;
$$;

create or replace procedure MIP.APP.SP_RUN_IB_SYMBOL_ONBOARDING(
    P_SYMBOLS variant,
    P_MARKET_TYPE string default 'STOCK',
    P_START_DATE date default '2025-08-01',
    P_END_DATE date default current_date(),
    P_AUTO_ACTIVATE_IF_TRUSTED boolean default true,
    P_RUN_ID string default null,
    P_SYMBOL_COHORT string default null,
    P_PRIORITY number default 50
)
returns variant
language sql
execute as caller
as
$$
declare
    v_run_id string := coalesce(:P_RUN_ID, uuid_string());
    v_market_type string := upper(coalesce(:P_MARKET_TYPE, 'STOCK'));
    v_start_date date := coalesce(:P_START_DATE, to_date('2025-08-01'));
    v_end_date date := coalesce(:P_END_DATE, current_date());
    v_auto_activate boolean := coalesce(:P_AUTO_ACTIVATE_IF_TRUSTED, true);
    v_priority number := coalesce(:P_PRIORITY, 50);
    v_symbol_cohort string := coalesce(:P_SYMBOL_COHORT, 'IB_ONBOARD_' || substr(replace(:v_run_id, '-', ''), 1, 8));
    v_requested number := 0;
    v_ready number := 0;
    v_activated number := 0;
    v_bootstrap_result variant;
    v_status string := 'SUCCESS';
begin
    if (v_start_date > v_end_date) then
        return object_construct(
            'status', 'FAIL',
            'error', 'start_date_after_end_date',
            'run_id', :v_run_id
        );
    end if;

    create or replace temporary table TMP_IB_ONBOARD_SYMBOLS as
    select distinct upper(trim(value::string)) as SYMBOL
    from table(flatten(input => :P_SYMBOLS))
    where value is not null
      and trim(value::string) <> '';

    select count(*) into :v_requested from TMP_IB_ONBOARD_SYMBOLS;

    if (v_requested = 0) then
        return object_construct(
            'status', 'FAIL',
            'error', 'symbols_required',
            'run_id', :v_run_id
        );
    end if;

    merge into MIP.APP.IB_SYMBOL_ONBOARDING_RUN_LOG t
    using (
        select
            :v_run_id as RUN_ID,
            :v_symbol_cohort as SYMBOL_COHORT,
            :v_market_type as MARKET_TYPE,
            :v_start_date as START_DATE,
            :v_end_date as END_DATE,
            :v_auto_activate as AUTO_ACTIVATE_IF_TRUSTED
    ) s
    on t.RUN_ID = s.RUN_ID
    when matched then update set
        t.SYMBOL_COHORT = s.SYMBOL_COHORT,
        t.MARKET_TYPE = s.MARKET_TYPE,
        t.START_DATE = s.START_DATE,
        t.END_DATE = s.END_DATE,
        t.AUTO_ACTIVATE_IF_TRUSTED = s.AUTO_ACTIVATE_IF_TRUSTED,
        t.STATUS = 'RUNNING',
        t.STARTED_AT = current_timestamp(),
        t.FINISHED_AT = null,
        t.FAILURES = null,
        t.DETAILS = object_construct('mode', 'IB_SYMBOL_ONBOARDING')
    when not matched then insert (
        RUN_ID, SYMBOL_COHORT, MARKET_TYPE, START_DATE, END_DATE, AUTO_ACTIVATE_IF_TRUSTED, STATUS, STARTED_AT, DETAILS
    ) values (
        s.RUN_ID, s.SYMBOL_COHORT, s.MARKET_TYPE, s.START_DATE, s.END_DATE, s.AUTO_ACTIVATE_IF_TRUSTED,
        'RUNNING', current_timestamp(), object_construct('mode', 'IB_SYMBOL_ONBOARDING')
    );

    merge into MIP.APP.INGEST_UNIVERSE t
    using (
        select
            s.SYMBOL as SYMBOL,
            :v_market_type as MARKET_TYPE,
            1440 as INTERVAL_MINUTES,
            :v_priority as PRIORITY,
            :v_symbol_cohort as SYMBOL_COHORT,
            'IB onboarding ' || :v_run_id as NOTES
        from TMP_IB_ONBOARD_SYMBOLS s
    ) s
    on upper(t.SYMBOL) = upper(s.SYMBOL)
   and upper(t.MARKET_TYPE) = upper(s.MARKET_TYPE)
   and t.INTERVAL_MINUTES = s.INTERVAL_MINUTES
    when matched then update set
        t.IS_ENABLED = true,
        t.PRIORITY = greatest(coalesce(t.PRIORITY, 0), s.PRIORITY),
        t.SYMBOL_COHORT = s.SYMBOL_COHORT,
        t.NOTES = coalesce(t.NOTES, s.NOTES)
    when not matched then insert (
        SYMBOL, MARKET_TYPE, INTERVAL_MINUTES, IS_ENABLED, PRIORITY, SYMBOL_COHORT, NOTES
    ) values (
        s.SYMBOL, s.MARKET_TYPE, s.INTERVAL_MINUTES, true, s.PRIORITY, s.SYMBOL_COHORT, s.NOTES
    );

    merge into MIP.APP.IB_SYMBOL_ONBOARDING_SYMBOL_LOG t
    using (
        select
            :v_run_id as RUN_ID,
            SYMBOL,
            :v_market_type as MARKET_TYPE,
            :v_symbol_cohort as SYMBOL_COHORT,
            'REGISTERED' as STATUS
        from TMP_IB_ONBOARD_SYMBOLS
    ) s
    on t.RUN_ID = s.RUN_ID and t.SYMBOL = s.SYMBOL and t.MARKET_TYPE = s.MARKET_TYPE
    when matched then update set
        t.SYMBOL_COHORT = s.SYMBOL_COHORT,
        t.STATUS = s.STATUS,
        t.UPDATED_AT = current_timestamp()
    when not matched then insert (
        RUN_ID, SYMBOL, MARKET_TYPE, SYMBOL_COHORT, STATUS, UPDATED_AT
    ) values (
        s.RUN_ID, s.SYMBOL, s.MARKET_TYPE, s.SYMBOL_COHORT, s.STATUS, current_timestamp()
    );

    v_bootstrap_result := (
        call MIP.APP.SP_BOOTSTRAP_DAILY_TRAINING_FOR_NEW_SYMBOLS_IB(
            :v_run_id,
            :v_start_date,
            :v_end_date,
            :v_symbol_cohort,
            :v_market_type,
            null
        )
    );

    merge into MIP.APP.IB_SYMBOL_ONBOARDING_SYMBOL_LOG t
    using (
        with bars as (
            select
                upper(b.SYMBOL) as SYMBOL_KEY,
                upper(b.MARKET_TYPE) as MARKET_TYPE_KEY,
                min(b.TS::date) as FIRST_BAR_DATE,
                max(b.TS::date) as LAST_BAR_DATE,
                count(*) as BAR_COUNT
            from MIP.MART.MARKET_BARS b
            join TMP_IB_ONBOARD_SYMBOLS s
              on upper(s.SYMBOL) = upper(b.SYMBOL)
            where b.INTERVAL_MINUTES = 1440
              and upper(b.MARKET_TYPE) = :v_market_type
              and b.TS::date between :v_start_date and :v_end_date
              and upper(coalesce(b.SOURCE, '')) = 'IBKR'
            group by upper(b.SYMBOL), upper(b.MARKET_TYPE)
        ),
        readiness as (
            select
                upper(SYMBOL) as SYMBOL_KEY,
                coalesce(READY_FLAG, false) as READY_FLAG,
                TRUSTED_LEVEL,
                REASON
            from MIP.MART.V_SYMBOL_TRAINING_READINESS
            where upper(COHORT) = upper(:v_symbol_cohort)
        ),
        trust_latest as (
            select
                upper(c.SYMBOL) as SYMBOL_KEY,
                upper(c.MARKET_TYPE) as MARKET_TYPE_KEY,
                c.TRUST_LABEL,
                c.RECOMMENDED_ACTION,
                c.GATING_REASON,
                row_number() over (
                    partition by upper(c.SYMBOL), upper(c.MARKET_TYPE)
                    order by c.TS desc
                ) as RN
            from MIP.APP.V_TRUSTED_SIGNAL_CLASSIFICATION c
            join TMP_IB_ONBOARD_SYMBOLS s
              on upper(s.SYMBOL) = upper(c.SYMBOL)
            where c.INTERVAL_MINUTES = 1440
              and upper(c.MARKET_TYPE) = :v_market_type
              and c.TS::date between :v_start_date and :v_end_date
        )
        select
            :v_run_id as RUN_ID,
            s.SYMBOL as SYMBOL,
            :v_market_type as MARKET_TYPE,
            coalesce(r.READY_FLAG, false) as READY_FLAG,
            coalesce(tl.TRUST_LABEL, 'UNTRUSTED') as TRUSTED_LEVEL,
            iff(
                coalesce(r.READY_FLAG, false)
                and upper(coalesce(tl.TRUST_LABEL, 'UNTRUSTED')) = 'TRUSTED'
                and upper(coalesce(tl.RECOMMENDED_ACTION, 'DISABLE')) = 'ENABLE',
                true,
                false
            ) as TRADE_READY_FLAG,
            iff(
                :v_auto_activate
                and coalesce(r.READY_FLAG, false)
                and upper(coalesce(tl.TRUST_LABEL, 'UNTRUSTED')) = 'TRUSTED'
                and upper(coalesce(tl.RECOMMENDED_ACTION, 'DISABLE')) = 'ENABLE',
                true,
                false
            ) as ACTIVATED_FLAG,
            coalesce(r.REASON, tl.GATING_REASON::string, 'NOT_READY') as REASON,
            b.FIRST_BAR_DATE,
            b.LAST_BAR_DATE,
            coalesce(b.BAR_COUNT, 0) as BAR_COUNT
        from TMP_IB_ONBOARD_SYMBOLS s
        left join bars b
          on b.SYMBOL_KEY = upper(s.SYMBOL)
         and b.MARKET_TYPE_KEY = :v_market_type
        left join readiness r
          on r.SYMBOL_KEY = upper(s.SYMBOL)
        left join trust_latest tl
          on tl.SYMBOL_KEY = upper(s.SYMBOL)
         and tl.MARKET_TYPE_KEY = :v_market_type
         and tl.RN = 1
    ) s
    on t.RUN_ID = s.RUN_ID and t.SYMBOL = s.SYMBOL and t.MARKET_TYPE = s.MARKET_TYPE
    when matched then update set
        t.STATUS = iff(s.TRADE_READY_FLAG, 'READY', 'TRAINED_NOT_READY'),
        t.READY_FLAG = s.READY_FLAG,
        t.TRUSTED_LEVEL = s.TRUSTED_LEVEL,
        t.ACTIVATED_FLAG = s.ACTIVATED_FLAG,
        t.REASON = s.REASON,
        t.FIRST_BAR_DATE = s.FIRST_BAR_DATE,
        t.LAST_BAR_DATE = s.LAST_BAR_DATE,
        t.BAR_COUNT = s.BAR_COUNT,
        t.DETAILS = object_construct(
            'trade_ready', s.TRADE_READY_FLAG,
            'auto_activate_requested', :v_auto_activate
        ),
        t.UPDATED_AT = current_timestamp();

    merge into MIP.APP.IB_SYMBOL_TRADE_ACTIVATION t
    using (
        select
            SYMBOL,
            MARKET_TYPE,
            coalesce(ACTIVATED_FLAG, false) as IS_ACTIVE_FOR_TRADE,
            REASON
        from MIP.APP.IB_SYMBOL_ONBOARDING_SYMBOL_LOG
        where RUN_ID = :v_run_id
    ) s
    on upper(t.SYMBOL) = upper(s.SYMBOL) and upper(t.MARKET_TYPE) = upper(s.MARKET_TYPE)
    when matched then update set
        t.IS_ACTIVE_FOR_TRADE = s.IS_ACTIVE_FOR_TRADE,
        t.ACTIVATED_AT = iff(s.IS_ACTIVE_FOR_TRADE, current_timestamp(), null),
        t.LAST_RUN_ID = :v_run_id,
        t.SOURCE = 'SP_RUN_IB_SYMBOL_ONBOARDING',
        t.REASON = s.REASON,
        t.UPDATED_AT = current_timestamp()
    when not matched then insert (
        SYMBOL, MARKET_TYPE, IS_ACTIVE_FOR_TRADE, ACTIVATED_AT, LAST_RUN_ID, SOURCE, REASON, UPDATED_AT
    ) values (
        s.SYMBOL, s.MARKET_TYPE, s.IS_ACTIVE_FOR_TRADE,
        iff(s.IS_ACTIVE_FOR_TRADE, current_timestamp(), null),
        :v_run_id, 'SP_RUN_IB_SYMBOL_ONBOARDING', s.REASON, current_timestamp()
    );

    select
        count(*),
        count_if(coalesce(READY_FLAG, false)),
        count_if(coalesce(ACTIVATED_FLAG, false))
      into :v_requested, :v_ready, :v_activated
      from MIP.APP.IB_SYMBOL_ONBOARDING_SYMBOL_LOG
     where RUN_ID = :v_run_id;

    if (coalesce(:v_bootstrap_result:"status"::string, 'FAIL') in ('SUCCESS', 'SUCCESS_WITH_SKIPS')) then
        v_status := coalesce(:v_bootstrap_result:"status"::string, 'SUCCESS');
    else
        v_status := 'FAIL';
    end if;

    update MIP.APP.IB_SYMBOL_ONBOARDING_RUN_LOG
       set STATUS = :v_status,
           FINISHED_AT = current_timestamp(),
           SYMBOLS_REQUESTED = :v_requested,
           SYMBOLS_READY = :v_ready,
           SYMBOLS_ACTIVATED = :v_activated,
           DETAILS = object_construct(
               'bootstrap_result', :v_bootstrap_result,
               'symbol_cohort', :v_symbol_cohort,
               'market_type', :v_market_type
           )
     where RUN_ID = :v_run_id;

    return object_construct(
        'status', :v_status,
        'run_id', :v_run_id,
        'symbol_cohort', :v_symbol_cohort,
        'market_type', :v_market_type,
        'start_date', :v_start_date,
        'end_date', :v_end_date,
        'symbols_requested', :v_requested,
        'symbols_ready', :v_ready,
        'symbols_activated', :v_activated,
        'bootstrap_result', :v_bootstrap_result
    );
exception
    when other then
        update MIP.APP.IB_SYMBOL_ONBOARDING_RUN_LOG
           set STATUS = 'FAIL',
               FINISHED_AT = current_timestamp(),
               FAILURES = array_construct(object_construct('error', :sqlerrm))
         where RUN_ID = :v_run_id;
        raise;
end;
$$;
