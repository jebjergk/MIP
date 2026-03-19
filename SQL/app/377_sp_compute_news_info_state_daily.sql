-- 377_sp_compute_news_info_state_daily.sql
-- Purpose: Phase 4 deterministic daily info-state computation.
-- Time contract:
--   - P_AS_OF_TS is interpreted in UTC.
--   - AS_OF_DATE is derived from UTC snapshot_ts.
-- Architecture contract:
--   - Context layer only; no signal/training/trust dependencies.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.NEWS.SP_COMPUTE_INFO_STATE_DAILY(
    P_AS_OF_TS timestamp_tz default current_timestamp(),
    P_RUN_ID string default null
)
returns variant
language sql
execute as caller
as
$$
declare
    v_snapshot_ts timestamp_ntz;
    v_as_of_date date;
    v_hot_z float := 1.5;
    v_run_id string := coalesce(:P_RUN_ID, replace(uuid_string(), '-', ''));
    v_rows_written number := 0;
begin
    execute immediate 'use schema MIP.NEWS';

    v_snapshot_ts := convert_timezone('UTC', :P_AS_OF_TS)::timestamp_ntz;
    v_as_of_date := :v_snapshot_ts::date;

    select coalesce(max(try_to_double(CONFIG_VALUE)), 1.5)
      into :v_hot_z
      from MIP.APP.APP_CONFIG
     where CONFIG_KEY = 'NEWS_BURST_Z_HOT';

    create or replace temporary table TMP_NEWS_BASE as
    select
        m.NEWS_ID,
        m.SYMBOL,
        m.MARKET_TYPE,
        r.SOURCE_ID,
        r.PUBLISHED_AT,
        r.INGESTED_AT,
        r.TITLE,
        r.SUMMARY,
        r.URL,
        coalesce(d.CLUSTER_SIZE, 1) as CLUSTER_SIZE
    from (
        select
            NEWS_ID,
            upper(SYMBOL) as SYMBOL,
            upper(MARKET_TYPE) as MARKET_TYPE
        from MIP.NEWS.NEWS_SYMBOL_MAP
        qualify row_number() over (
            partition by NEWS_ID, upper(SYMBOL), upper(MARKET_TYPE)
            order by MATCH_CONFIDENCE desc, MATCH_METHOD
        ) = 1
    ) m
    join MIP.NEWS.NEWS_RAW r
      on r.NEWS_ID = m.NEWS_ID
    left join MIP.NEWS.NEWS_DEDUP d
      on d.DEDUP_CLUSTER_ID = r.DEDUP_CLUSTER_ID
    where r.PUBLISHED_AT::date between dateadd(day, -14, :v_as_of_date) and :v_as_of_date
      and r.URL is not null
      -- Snowflake REGEXP_LIKE is full-string match; require scheme plus remaining path.
      and regexp_like(lower(r.URL), '^https?://.+')
      and not regexp_like(lower(r.URL), 'mock-item-|/rss/|\\.xml$');

    create or replace temporary table TMP_NEWS_DAILY_COUNTS as
    select
        PUBLISHED_AT::date as AS_OF_DATE,
        SYMBOL,
        MARKET_TYPE,
        count(*) as NEWS_COUNT,
        count(distinct SOURCE_ID) as UNIQUE_SOURCES,
        count_if(CLUSTER_SIZE > 1) as DEDUP_COUNT,
        max(PUBLISHED_AT) as LAST_NEWS_PUBLISHED_AT,
        max(INGESTED_AT) as LAST_INGESTED_AT,
        iff(
            count_if(regexp_like(upper(coalesce(TITLE, '') || ' ' || coalesce(SUMMARY, '')), '\\b(UPGRADE|BEAT|RAISE|GAIN|SURGE|RECORD HIGH)\\b')) > 0
            and count_if(regexp_like(upper(coalesce(TITLE, '') || ' ' || coalesce(SUMMARY, '')), '\\b(DOWNGRADE|MISS|CUT|DROP|PLUNGE|RECORD LOW)\\b')) > 0,
            true,
            false
        ) as UNCERTAINTY_FLAG
    from TMP_NEWS_BASE
    group by 1, 2, 3;

    create or replace temporary table TMP_NEWS_TOP_HEADLINES as
    with ranked as (
        select
            b.PUBLISHED_AT::date as AS_OF_DATE,
            b.SYMBOL,
            b.MARKET_TYPE,
            b.TITLE,
            b.URL,
            b.PUBLISHED_AT,
            row_number() over (
                partition by b.PUBLISHED_AT::date, b.SYMBOL, b.MARKET_TYPE
                order by b.CLUSTER_SIZE desc, b.PUBLISHED_AT desc, b.NEWS_ID
            ) as RN
        from TMP_NEWS_BASE b
    )
    select
        AS_OF_DATE,
        SYMBOL,
        MARKET_TYPE,
        array_agg(
            object_construct('title', TITLE, 'url', URL)
        ) within group (order by PUBLISHED_AT desc) as TOP_HEADLINES
    from ranked
    where RN <= 3
    group by AS_OF_DATE, SYMBOL, MARKET_TYPE;

    create or replace temporary table TMP_NEWS_UNIVERSE as
    select distinct
        upper(SYMBOL) as SYMBOL,
        upper(MARKET_TYPE) as MARKET_TYPE
    from MIP.APP.INGEST_UNIVERSE
    where coalesce(IS_ENABLED, true)
      and INTERVAL_MINUTES = 1440;

    create or replace temporary table TMP_NEWS_TARGET as
    select
        :v_as_of_date as AS_OF_DATE,
        u.SYMBOL,
        u.MARKET_TYPE,
        coalesce(dc.NEWS_COUNT, 0) as NEWS_COUNT,
        coalesce(dc.UNIQUE_SOURCES, 0) as UNIQUE_SOURCES,
        coalesce(dc.DEDUP_COUNT, 0) as DEDUP_COUNT,
        coalesce(dc.UNCERTAINTY_FLAG, false) as UNCERTAINTY_FLAG,
        dc.LAST_NEWS_PUBLISHED_AT,
        dc.LAST_INGESTED_AT
    from TMP_NEWS_UNIVERSE u
    left join TMP_NEWS_DAILY_COUNTS dc
      on dc.AS_OF_DATE = :v_as_of_date
     and dc.SYMBOL = u.SYMBOL
     and dc.MARKET_TYPE = u.MARKET_TYPE;

    create or replace temporary table TMP_NEWS_TRAILING as
    select
        t.SYMBOL,
        t.MARKET_TYPE,
        avg(h.NEWS_COUNT) as TRAILING_AVG_7D,
        stddev_samp(h.NEWS_COUNT) as TRAILING_STD_7D
    from TMP_NEWS_TARGET t
    left join TMP_NEWS_DAILY_COUNTS h
      on h.SYMBOL = t.SYMBOL
     and h.MARKET_TYPE = t.MARKET_TYPE
     and h.AS_OF_DATE between dateadd(day, -7, :v_as_of_date) and dateadd(day, -1, :v_as_of_date)
    group by t.SYMBOL, t.MARKET_TYPE;

    merge into MIP.NEWS.NEWS_INFO_STATE_DAILY tgt
    using (
        select
            t.AS_OF_DATE,
            t.SYMBOL,
            t.MARKET_TYPE,
            t.NEWS_COUNT,
            t.UNIQUE_SOURCES,
            t.DEDUP_COUNT,
            iff(t.NEWS_COUNT > 0, 1 - (t.DEDUP_COUNT::float / t.NEWS_COUNT::float), null) as NOVELTY_SCORE,
            tr.TRAILING_AVG_7D,
            tr.TRAILING_STD_7D,
            iff(
                tr.TRAILING_STD_7D is null or tr.TRAILING_STD_7D = 0,
                null,
                (t.NEWS_COUNT - tr.TRAILING_AVG_7D) / tr.TRAILING_STD_7D
            ) as Z_SCORE,
            iff(
                tr.TRAILING_STD_7D is null or tr.TRAILING_STD_7D = 0,
                null,
                (t.NEWS_COUNT - tr.TRAILING_AVG_7D) / tr.TRAILING_STD_7D
            ) as BURST_SCORE,
            t.UNCERTAINTY_FLAG,
            th.TOP_HEADLINES,
            case
                when t.NEWS_COUNT = 0 then 'NONE'
                when tr.TRAILING_STD_7D is not null and tr.TRAILING_STD_7D != 0
                     and ((t.NEWS_COUNT - tr.TRAILING_AVG_7D) / tr.TRAILING_STD_7D) >= :v_hot_z
                    then 'HOT'
                else 'NORMAL'
            end as NEWS_CONTEXT_BADGE,
            t.LAST_NEWS_PUBLISHED_AT,
            t.LAST_INGESTED_AT,
            :v_snapshot_ts as SNAPSHOT_TS,
            current_timestamp() as CREATED_AT,
            :v_run_id as RUN_ID
        from TMP_NEWS_TARGET t
        left join TMP_NEWS_TRAILING tr
          on tr.SYMBOL = t.SYMBOL
         and tr.MARKET_TYPE = t.MARKET_TYPE
        left join TMP_NEWS_TOP_HEADLINES th
          on th.AS_OF_DATE = t.AS_OF_DATE
         and th.SYMBOL = t.SYMBOL
         and th.MARKET_TYPE = t.MARKET_TYPE
    ) src
       on tgt.AS_OF_DATE = src.AS_OF_DATE
      and tgt.SYMBOL = src.SYMBOL
      and tgt.MARKET_TYPE = src.MARKET_TYPE
      and tgt.SNAPSHOT_TS = src.SNAPSHOT_TS
    when matched then update set
        tgt.NEWS_COUNT = src.NEWS_COUNT,
        tgt.UNIQUE_SOURCES = src.UNIQUE_SOURCES,
        tgt.DEDUP_COUNT = src.DEDUP_COUNT,
        tgt.NOVELTY_SCORE = src.NOVELTY_SCORE,
        tgt.BURST_SCORE = src.BURST_SCORE,
        tgt.UNCERTAINTY_FLAG = src.UNCERTAINTY_FLAG,
        tgt.TOP_HEADLINES = src.TOP_HEADLINES,
        tgt.TRAILING_AVG_7D = src.TRAILING_AVG_7D,
        tgt.TRAILING_STD_7D = src.TRAILING_STD_7D,
        tgt.Z_SCORE = src.Z_SCORE,
        tgt.NEWS_CONTEXT_BADGE = src.NEWS_CONTEXT_BADGE,
        tgt.LAST_NEWS_PUBLISHED_AT = src.LAST_NEWS_PUBLISHED_AT,
        tgt.LAST_INGESTED_AT = src.LAST_INGESTED_AT,
        tgt.CREATED_AT = src.CREATED_AT,
        tgt.RUN_ID = src.RUN_ID
    when not matched then insert (
        AS_OF_DATE, SYMBOL, MARKET_TYPE,
        NEWS_COUNT, UNIQUE_SOURCES, DEDUP_COUNT,
        NOVELTY_SCORE, BURST_SCORE, UNCERTAINTY_FLAG, TOP_HEADLINES,
        TRAILING_AVG_7D, TRAILING_STD_7D, Z_SCORE, NEWS_CONTEXT_BADGE,
        LAST_NEWS_PUBLISHED_AT, LAST_INGESTED_AT,
        SNAPSHOT_TS, CREATED_AT, RUN_ID
    ) values (
        src.AS_OF_DATE, src.SYMBOL, src.MARKET_TYPE,
        src.NEWS_COUNT, src.UNIQUE_SOURCES, src.DEDUP_COUNT,
        src.NOVELTY_SCORE, src.BURST_SCORE, src.UNCERTAINTY_FLAG, src.TOP_HEADLINES,
        src.TRAILING_AVG_7D, src.TRAILING_STD_7D, src.Z_SCORE, src.NEWS_CONTEXT_BADGE,
        src.LAST_NEWS_PUBLISHED_AT, src.LAST_INGESTED_AT,
        src.SNAPSHOT_TS, src.CREATED_AT, src.RUN_ID
    );

    select count(*)
      into :v_rows_written
      from MIP.NEWS.NEWS_INFO_STATE_DAILY
     where AS_OF_DATE = :v_as_of_date
       and SNAPSHOT_TS = :v_snapshot_ts;

    drop table if exists TMP_NEWS_BASE;
    drop table if exists TMP_NEWS_DAILY_COUNTS;
    drop table if exists TMP_NEWS_TOP_HEADLINES;
    drop table if exists TMP_NEWS_UNIVERSE;
    drop table if exists TMP_NEWS_TARGET;
    drop table if exists TMP_NEWS_TRAILING;

    return object_construct(
        'status', 'SUCCESS',
        'as_of_date', :v_as_of_date,
        'snapshot_ts_utc', :v_snapshot_ts,
        'rows_written', :v_rows_written,
        'run_id', :v_run_id,
        'hot_z_threshold', :v_hot_z
    );
end;
$$;
