-- 390_sp_aggregate_news_events.sql
-- Purpose: Deterministic hourly aggregation of mapped news for proposal-time usage.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.NEWS.SP_AGGREGATE_NEWS_EVENTS(
    P_AS_OF_TS timestamp_ntz default current_timestamp(),
    P_RUN_ID string default null
)
returns variant
language sql
execute as caller
as
$$
declare
    v_as_of_ts timestamp_ntz := coalesce(:P_AS_OF_TS, current_timestamp());
    v_run_id string := coalesce(:P_RUN_ID, replace(uuid_string(), '-', ''));
    v_tau_hours float := 24.0;
    v_hot_threshold float := 2.5;
    v_rows_written number := 0;
begin
    select
        coalesce(max(try_to_double(iff(CONFIG_KEY = 'NEWS_AGG_TAU_HOURS', CONFIG_VALUE, null))), :v_tau_hours),
        coalesce(max(try_to_double(iff(CONFIG_KEY = 'NEWS_AGG_BADGE_HOT_THRESHOLD', CONFIG_VALUE, null))), :v_hot_threshold)
      into :v_tau_hours, :v_hot_threshold
      from MIP.APP.APP_CONFIG
     where CONFIG_KEY in ('NEWS_AGG_TAU_HOURS', 'NEWS_AGG_BADGE_HOT_THRESHOLD');

    create or replace temporary table TMP_NEWS_AGG_BASE as
    select
        date_trunc('hour', r.PUBLISHED_AT) as AS_OF_TS_BUCKET,
        m.SYMBOL,
        m.MARKET_TYPE,
        r.NEWS_ID,
        r.SOURCE_ID,
        r.DEDUP_CLUSTER_ID,
        r.TITLE,
        r.URL,
        r.PUBLISHED_AT,
        r.INGESTED_AT,
        coalesce(d.CLUSTER_SIZE, 1) as CLUSTER_SIZE
    from MIP.NEWS.NEWS_SYMBOL_MAP m
    join MIP.NEWS.NEWS_RAW r
      on r.NEWS_ID = m.NEWS_ID
    left join MIP.NEWS.NEWS_DEDUP d
      on d.DEDUP_CLUSTER_ID = r.DEDUP_CLUSTER_ID
    where r.PUBLISHED_AT <= :v_as_of_ts
      and r.PUBLISHED_AT >= dateadd(day, -7, :v_as_of_ts)
    qualify row_number() over (
        partition by m.NEWS_ID, m.SYMBOL, m.MARKET_TYPE
        order by m.CREATED_AT desc
    ) = 1;

    create or replace temporary table TMP_NEWS_AGG_CLUSTER as
    select
        AS_OF_TS_BUCKET,
        SYMBOL,
        MARKET_TYPE,
        DEDUP_CLUSTER_ID,
        count(*) as ITEMS_IN_CLUSTER,
        count(distinct SOURCE_ID) as SOURCES_IN_CLUSTER,
        min(PUBLISHED_AT) as CLUSTER_FIRST_PUBLISHED_AT,
        max(PUBLISHED_AT) as CLUSTER_LAST_PUBLISHED_AT,
        max(INGESTED_AT) as CLUSTER_LAST_INGESTED_AT,
        min(TITLE) as HEADLINE,
        min(URL) as URL,
        min(SOURCE_ID) as SOURCE_ID,
        max(CLUSTER_SIZE) as CLUSTER_SIZE
    from TMP_NEWS_AGG_BASE
    group by AS_OF_TS_BUCKET, SYMBOL, MARKET_TYPE, DEDUP_CLUSTER_ID;

    create or replace temporary table TMP_NEWS_AGG_TOP as
    select
        c.*,
        row_number() over (
            partition by c.AS_OF_TS_BUCKET, c.SYMBOL, c.MARKET_TYPE
            order by c.SOURCES_IN_CLUSTER desc, c.CLUSTER_SIZE desc, c.CLUSTER_LAST_PUBLISHED_AT desc, c.DEDUP_CLUSTER_ID
        ) as RN
    from TMP_NEWS_AGG_CLUSTER c;

    create or replace temporary table TMP_NEWS_AGG_FINAL as
    with bucket_rollup as (
        select
            b.AS_OF_TS_BUCKET,
            b.SYMBOL,
            b.MARKET_TYPE,
            count(*) as ITEMS_TOTAL,
            count(distinct b.SOURCE_ID) as SOURCES_TOTAL,
            count(distinct b.DEDUP_CLUSTER_ID) as DEDUP_CLUSTERS_TOTAL,
            max(b.PUBLISHED_AT) as LAST_PUBLISHED_AT,
            max(b.INGESTED_AT) as LAST_INGESTED_AT
        from TMP_NEWS_AGG_BASE b
        group by b.AS_OF_TS_BUCKET, b.SYMBOL, b.MARKET_TYPE
    ),
    conflict_calc as (
        select
            b.AS_OF_TS_BUCKET,
            b.SYMBOL,
            b.MARKET_TYPE,
            sum(iff(regexp_like(upper(coalesce(b.TITLE, '')), '\\b(BEAT|UPGRADE|RAISE|OUTPERFORM|BUY|SURGE|GROWTH)\\b'), 1, 0)) as POS_HITS,
            sum(iff(regexp_like(upper(coalesce(b.TITLE, '')), '\\b(MISS|DOWNGRADE|CUT|UNDERPERFORM|SELL|PLUNGE|RISK)\\b'), 1, 0)) as NEG_HITS
        from TMP_NEWS_AGG_BASE b
        group by b.AS_OF_TS_BUCKET, b.SYMBOL, b.MARKET_TYPE
    ),
    pressure_calc as (
        select
            t.AS_OF_TS_BUCKET,
            t.SYMBOL,
            t.MARKET_TYPE,
            sum(
                exp(
                    -greatest(datediff('minute', t.CLUSTER_LAST_PUBLISHED_AT, :v_as_of_ts), 0)
                    / 60.0
                    / nullif(:v_tau_hours, 0)
                ) * least(greatest(t.CLUSTER_SIZE, 1), 5)
            ) as INFO_PRESSURE
        from TMP_NEWS_AGG_TOP t
        group by t.AS_OF_TS_BUCKET, t.SYMBOL, t.MARKET_TYPE
    ),
    top_clusters as (
        select
            t.AS_OF_TS_BUCKET,
            t.SYMBOL,
            t.MARKET_TYPE,
            array_agg(
                object_construct(
                    'headline', t.HEADLINE,
                    'url', t.URL,
                    'source_id', t.SOURCE_ID,
                    'published_at', t.CLUSTER_LAST_PUBLISHED_AT,
                    'items_in_cluster', t.ITEMS_IN_CLUSTER,
                    'sources_in_cluster', t.SOURCES_IN_CLUSTER
                )
            ) within group (order by t.RN) as TOP_CLUSTERS
        from TMP_NEWS_AGG_TOP t
        where t.RN <= 5
        group by t.AS_OF_TS_BUCKET, t.SYMBOL, t.MARKET_TYPE
    )
    select
        r.AS_OF_TS_BUCKET,
        r.SYMBOL,
        r.MARKET_TYPE,
        r.ITEMS_TOTAL,
        r.SOURCES_TOTAL,
        r.DEDUP_CLUSTERS_TOTAL,
        tc.TOP_CLUSTERS,
        object_construct('deterministic_v1', r.ITEMS_TOTAL) as EVENT_TYPE_MIX,
        coalesce(p.INFO_PRESSURE, 0.0) as INFO_PRESSURE,
        iff(r.ITEMS_TOTAL > 0, 1.0 - (r.DEDUP_CLUSTERS_TOTAL::float / r.ITEMS_TOTAL::float), 0.0) as NOVELTY,
        iff(
            coalesce(c.POS_HITS, 0) + coalesce(c.NEG_HITS, 0) = 0,
            0.0,
            least(coalesce(c.POS_HITS, 0), coalesce(c.NEG_HITS, 0))::float
            / greatest(coalesce(c.POS_HITS, 0), coalesce(c.NEG_HITS, 0))::float
        ) as CONFLICT,
        case
            when coalesce(p.INFO_PRESSURE, 0.0) >= :v_hot_threshold then 'HOT'
            when coalesce(p.INFO_PRESSURE, 0.0) < 0.50 then 'QUIET'
            else 'NORMAL'
        end as BADGE,
        r.LAST_PUBLISHED_AT,
        r.LAST_INGESTED_AT
    from bucket_rollup r
    left join pressure_calc p
      on p.AS_OF_TS_BUCKET = r.AS_OF_TS_BUCKET
     and p.SYMBOL = r.SYMBOL
     and p.MARKET_TYPE = r.MARKET_TYPE
    left join conflict_calc c
      on c.AS_OF_TS_BUCKET = r.AS_OF_TS_BUCKET
     and c.SYMBOL = r.SYMBOL
     and c.MARKET_TYPE = r.MARKET_TYPE
    left join top_clusters tc
      on tc.AS_OF_TS_BUCKET = r.AS_OF_TS_BUCKET
     and tc.SYMBOL = r.SYMBOL
     and tc.MARKET_TYPE = r.MARKET_TYPE;

    merge into MIP.NEWS.NEWS_AGGREGATED_EVENTS tgt
    using (
        select
            AS_OF_TS_BUCKET, SYMBOL, MARKET_TYPE,
            ITEMS_TOTAL, SOURCES_TOTAL, DEDUP_CLUSTERS_TOTAL,
            TOP_CLUSTERS, EVENT_TYPE_MIX, INFO_PRESSURE, NOVELTY, CONFLICT, BADGE,
            LAST_PUBLISHED_AT, LAST_INGESTED_AT
        from TMP_NEWS_AGG_FINAL
    ) src
    on tgt.AS_OF_TS_BUCKET = src.AS_OF_TS_BUCKET
   and tgt.SYMBOL = src.SYMBOL
   and tgt.MARKET_TYPE = src.MARKET_TYPE
    when matched then update set
        tgt.ITEMS_TOTAL = src.ITEMS_TOTAL,
        tgt.SOURCES_TOTAL = src.SOURCES_TOTAL,
        tgt.DEDUP_CLUSTERS_TOTAL = src.DEDUP_CLUSTERS_TOTAL,
        tgt.TOP_CLUSTERS = src.TOP_CLUSTERS,
        tgt.EVENT_TYPE_MIX = src.EVENT_TYPE_MIX,
        tgt.INFO_PRESSURE = src.INFO_PRESSURE,
        tgt.NOVELTY = src.NOVELTY,
        tgt.CONFLICT = src.CONFLICT,
        tgt.BADGE = src.BADGE,
        tgt.LAST_PUBLISHED_AT = src.LAST_PUBLISHED_AT,
        tgt.LAST_INGESTED_AT = src.LAST_INGESTED_AT,
        tgt.SNAPSHOT_TS = :v_as_of_ts,
        tgt.RUN_ID = :v_run_id,
        tgt.UPDATED_AT = current_timestamp()
    when not matched then insert (
        AS_OF_TS_BUCKET, SYMBOL, MARKET_TYPE,
        ITEMS_TOTAL, SOURCES_TOTAL, DEDUP_CLUSTERS_TOTAL,
        TOP_CLUSTERS, EVENT_TYPE_MIX, INFO_PRESSURE, NOVELTY, CONFLICT, BADGE,
        LAST_PUBLISHED_AT, LAST_INGESTED_AT, SNAPSHOT_TS, RUN_ID, CREATED_AT, UPDATED_AT
    ) values (
        src.AS_OF_TS_BUCKET, src.SYMBOL, src.MARKET_TYPE,
        src.ITEMS_TOTAL, src.SOURCES_TOTAL, src.DEDUP_CLUSTERS_TOTAL,
        src.TOP_CLUSTERS, src.EVENT_TYPE_MIX, src.INFO_PRESSURE, src.NOVELTY, src.CONFLICT, src.BADGE,
        src.LAST_PUBLISHED_AT, src.LAST_INGESTED_AT, :v_as_of_ts, :v_run_id, current_timestamp(), current_timestamp()
    );

    select count(*) into :v_rows_written from TMP_NEWS_AGG_FINAL;

    return object_construct(
        'status', 'SUCCESS',
        'run_id', :v_run_id,
        'as_of_ts', :v_as_of_ts,
        'rows_written', :v_rows_written
    );
end;
$$;
