-- 384_sp_build_news_features_snapshot.sql
-- Purpose: Phase B feature vector builder from NEWS_EVENT_EXTRACTED.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.NEWS.SP_BUILD_NEWS_FEATURES_SNAPSHOT(
    P_AS_OF_TS timestamp_tz default current_timestamp(),
    P_RUN_ID string default null
)
returns variant
language sql
execute as caller
as
$$
declare
    v_as_of_ts timestamp_ntz;
    v_run_id string := coalesce(:P_RUN_ID, replace(uuid_string(), '-', ''));
    v_decay_tau_hours float := 24;
    v_lookback_hours float := 72;
    v_stale_minutes number := 180;
    v_rows_written number := 0;
begin
    execute immediate 'use schema MIP.NEWS';

    v_as_of_ts := convert_timezone('UTC', :P_AS_OF_TS)::timestamp_ntz;

    select
        coalesce(max(try_to_double(iff(CONFIG_KEY = 'NEWS_DECAY_TAU_HOURS', CONFIG_VALUE, null))), 24),
        coalesce(max(try_to_double(iff(CONFIG_KEY = 'NEWS_FEATURE_LOOKBACK_HOURS', CONFIG_VALUE, null))), 72),
        coalesce(max(try_to_number(iff(CONFIG_KEY = 'NEWS_STALENESS_THRESHOLD_MINUTES', CONFIG_VALUE, null))), 180)
      into :v_decay_tau_hours, :v_lookback_hours, :v_stale_minutes
      from MIP.APP.APP_CONFIG
     where CONFIG_KEY in ('NEWS_DECAY_TAU_HOURS', 'NEWS_FEATURE_LOOKBACK_HOURS', 'NEWS_STALENESS_THRESHOLD_MINUTES');

    create or replace temporary table TMP_NEWS_EVENT_BASE as
    with dedup_extract as (
        select
            e.*,
            row_number() over (
                partition by e.EXTRACT_ID
                order by e.UPDATED_AT desc, e.EXTRACTED_AT desc
            ) as RN
        from MIP.NEWS.NEWS_EVENT_EXTRACTED e
        where e.EVENT_TS <= :v_as_of_ts
          and e.EVENT_TS >= dateadd(hour, -:v_lookback_hours, :v_as_of_ts)
    )
    select
        e.EXTRACT_ID,
        e.NEWS_ID,
        upper(e.SYMBOL) as SYMBOL,
        upper(e.MARKET_TYPE) as MARKET_TYPE,
        e.EVENT_TS,
        lower(e.EVENT_TYPE) as EVENT_TYPE,
        lower(e.DIRECTION) as DIRECTION,
        greatest(least(coalesce(e.CONFIDENCE, 0), 1), 0) as CONFIDENCE,
        lower(e.RELEVANCE_SCOPE) as RELEVANCE_SCOPE,
        greatest(least(coalesce(e.EVENT_RISK_SCORE, 0), 1), 0) as EVENT_RISK_SCORE,
        e.EVENT_SUMMARY,
        e.LLM_USED,
        r.PUBLISHED_AT,
        r.INGESTED_AT,
        greatest(datediff('second', e.EVENT_TS, :v_as_of_ts) / 3600.0, 0) as AGE_HOURS,
        exp(-greatest(datediff('second', e.EVENT_TS, :v_as_of_ts) / 3600.0, 0) / nullif(:v_decay_tau_hours, 0)) as DECAY_WEIGHT,
        case lower(e.DIRECTION)
            when 'positive' then 1.0
            when 'negative' then -1.0
            else 0.0
        end as DIRECTION_SIGN,
        case lower(e.EVENT_TYPE)
            when 'regulatory_legal' then 1.30
            when 'macro_policy' then 1.20
            when 'mna' then 1.10
            when 'earnings' then 1.00
            when 'product_business' then 0.80
            else 0.60
        end as EVENT_TYPE_WEIGHT
    from dedup_extract e
    join MIP.NEWS.NEWS_RAW r
      on r.NEWS_ID = e.NEWS_ID
    where e.RN = 1;

    create or replace temporary table TMP_NEWS_FEATURE_AGG as
    select
        :v_as_of_ts as AS_OF_TS,
        SYMBOL,
        MARKET_TYPE,
        count(*) as EVENT_COUNT,
        count_if(DIRECTION = 'positive') as POSITIVE_EVENT_COUNT,
        count_if(DIRECTION = 'negative') as NEGATIVE_EVENT_COUNT,
        count_if(DIRECTION = 'neutral') as NEUTRAL_EVENT_COUNT,
        least(
            greatest(
                coalesce(
                    sum(DECAY_WEIGHT * CONFIDENCE * EVENT_RISK_SCORE * EVENT_TYPE_WEIGHT)
                    / nullif(sum(DECAY_WEIGHT * greatest(CONFIDENCE, 0.05)), 0),
                    0
                ),
                0
            ),
            1
        ) as NEWS_PRESSURE,
        greatest(
            least(
                coalesce(
                    sum((DECAY_WEIGHT * greatest(CONFIDENCE, 0.05) * DIRECTION_SIGN) * EVENT_TYPE_WEIGHT)
                    / nullif(sum(abs(DECAY_WEIGHT * greatest(CONFIDENCE, 0.05) * EVENT_TYPE_WEIGHT)), 0),
                    0
                ),
                1
            ),
            -1
        ) as NEWS_SENTIMENT,
        least(
            greatest(
                coalesce(
                    sum(
                        DECAY_WEIGHT * greatest(CONFIDENCE, 0.05)
                        * least(1.0, greatest(0.0,
                            (1 - CONFIDENCE)
                            + iff(DIRECTION = 'neutral', 0.35, 0.0)
                            + iff(EVENT_TYPE in ('macro_policy', 'regulatory_legal'), 0.20, 0.0)
                        ))
                    ) / nullif(sum(DECAY_WEIGHT * greatest(CONFIDENCE, 0.05)), 0),
                    0
                ),
                0
            ),
            1
        ) as UNCERTAINTY_SCORE,
        least(
            greatest(
                coalesce(
                    sum(DECAY_WEIGHT * greatest(CONFIDENCE, 0.05) * EVENT_RISK_SCORE)
                    / nullif(sum(DECAY_WEIGHT * greatest(CONFIDENCE, 0.05)), 0),
                    0
                ),
                0
            ),
            1
        ) as EVENT_RISK_SCORE,
        least(
            greatest(
                coalesce(
                    sum(
                        DECAY_WEIGHT * greatest(CONFIDENCE, 0.05)
                        * iff(RELEVANCE_SCOPE = 'macro' or EVENT_TYPE = 'macro_policy', EVENT_RISK_SCORE, 0)
                    ) / nullif(sum(DECAY_WEIGHT * greatest(CONFIDENCE, 0.05)), 0),
                    0
                ),
                0
            ),
            1
        ) as MACRO_HEAT,
        max(EVENT_TS) as LAST_EVENT_TS,
        max(PUBLISHED_AT) as LAST_NEWS_PUBLISHED_AT,
        max(INGESTED_AT) as LAST_INGESTED_AT,
        :v_as_of_ts as SNAPSHOT_TS,
        datediff('minute', max(PUBLISHED_AT), :v_as_of_ts) as NEWS_SNAPSHOT_AGE_MINUTES,
        iff(datediff('minute', max(PUBLISHED_AT), :v_as_of_ts) > :v_stale_minutes, true, false) as NEWS_IS_STALE,
        :v_decay_tau_hours as DECAY_TAU_HOURS,
        :v_lookback_hours as LOOKBACK_HOURS,
        current_timestamp() as CREATED_AT,
        :v_run_id as RUN_ID
    from TMP_NEWS_EVENT_BASE
    group by SYMBOL, MARKET_TYPE;

    create or replace temporary table TMP_NEWS_TOP_EVENTS as
    with ranked as (
        select
            b.SYMBOL,
            b.MARKET_TYPE,
            b.EVENT_TS,
            b.EVENT_TYPE,
            b.DIRECTION,
            b.CONFIDENCE,
            b.EVENT_RISK_SCORE,
            b.RELEVANCE_SCOPE,
            b.EVENT_SUMMARY,
            b.LLM_USED,
            abs(
                b.DECAY_WEIGHT * greatest(b.CONFIDENCE, 0.05) * b.EVENT_RISK_SCORE
                * b.DIRECTION_SIGN * b.EVENT_TYPE_WEIGHT
            ) as IMPACT_ABS,
            row_number() over (
                partition by b.SYMBOL, b.MARKET_TYPE
                order by
                    abs(
                        b.DECAY_WEIGHT * greatest(b.CONFIDENCE, 0.05) * b.EVENT_RISK_SCORE
                        * b.DIRECTION_SIGN * b.EVENT_TYPE_WEIGHT
                    ) desc,
                    b.EVENT_TS desc,
                    b.EXTRACT_ID
            ) as RN
        from TMP_NEWS_EVENT_BASE b
    )
    select
        SYMBOL,
        MARKET_TYPE,
        array_agg(
            object_construct(
                'event_ts', EVENT_TS,
                'event_type', EVENT_TYPE,
                'direction', DIRECTION,
                'confidence', CONFIDENCE,
                'event_risk_score', EVENT_RISK_SCORE,
                'relevance_scope', RELEVANCE_SCOPE,
                'impact_abs', IMPACT_ABS,
                'event_summary', EVENT_SUMMARY,
                'llm_used', LLM_USED
            )
        ) within group (order by IMPACT_ABS desc, EVENT_TS desc) as TOP_EVENTS
    from ranked
    where RN <= 3
    group by SYMBOL, MARKET_TYPE;

    merge into MIP.NEWS.NEWS_FEATURES_SNAPSHOT tgt
    using (
        select
            f.AS_OF_TS,
            f.SYMBOL,
            f.MARKET_TYPE,
            f.EVENT_COUNT,
            f.POSITIVE_EVENT_COUNT,
            f.NEGATIVE_EVENT_COUNT,
            f.NEUTRAL_EVENT_COUNT,
            f.NEWS_PRESSURE,
            f.NEWS_SENTIMENT,
            f.UNCERTAINTY_SCORE,
            f.EVENT_RISK_SCORE,
            f.MACRO_HEAT,
            t.TOP_EVENTS,
            f.LAST_EVENT_TS,
            f.LAST_NEWS_PUBLISHED_AT,
            f.LAST_INGESTED_AT,
            f.SNAPSHOT_TS,
            f.NEWS_SNAPSHOT_AGE_MINUTES,
            f.NEWS_IS_STALE,
            f.DECAY_TAU_HOURS,
            f.LOOKBACK_HOURS,
            f.CREATED_AT,
            f.RUN_ID
        from TMP_NEWS_FEATURE_AGG f
        left join TMP_NEWS_TOP_EVENTS t
          on t.SYMBOL = f.SYMBOL
         and t.MARKET_TYPE = f.MARKET_TYPE
    ) src
       on tgt.AS_OF_TS = src.AS_OF_TS
      and tgt.SYMBOL = src.SYMBOL
      and tgt.MARKET_TYPE = src.MARKET_TYPE
      and tgt.SNAPSHOT_TS = src.SNAPSHOT_TS
    when matched then update set
        tgt.EVENT_COUNT = src.EVENT_COUNT,
        tgt.POSITIVE_EVENT_COUNT = src.POSITIVE_EVENT_COUNT,
        tgt.NEGATIVE_EVENT_COUNT = src.NEGATIVE_EVENT_COUNT,
        tgt.NEUTRAL_EVENT_COUNT = src.NEUTRAL_EVENT_COUNT,
        tgt.NEWS_PRESSURE = src.NEWS_PRESSURE,
        tgt.NEWS_SENTIMENT = src.NEWS_SENTIMENT,
        tgt.UNCERTAINTY_SCORE = src.UNCERTAINTY_SCORE,
        tgt.EVENT_RISK_SCORE = src.EVENT_RISK_SCORE,
        tgt.MACRO_HEAT = src.MACRO_HEAT,
        tgt.TOP_EVENTS = src.TOP_EVENTS,
        tgt.LAST_EVENT_TS = src.LAST_EVENT_TS,
        tgt.LAST_NEWS_PUBLISHED_AT = src.LAST_NEWS_PUBLISHED_AT,
        tgt.LAST_INGESTED_AT = src.LAST_INGESTED_AT,
        tgt.NEWS_SNAPSHOT_AGE_MINUTES = src.NEWS_SNAPSHOT_AGE_MINUTES,
        tgt.NEWS_IS_STALE = src.NEWS_IS_STALE,
        tgt.DECAY_TAU_HOURS = src.DECAY_TAU_HOURS,
        tgt.LOOKBACK_HOURS = src.LOOKBACK_HOURS,
        tgt.CREATED_AT = src.CREATED_AT,
        tgt.RUN_ID = src.RUN_ID
    when not matched then insert (
        AS_OF_TS, SYMBOL, MARKET_TYPE,
        EVENT_COUNT, POSITIVE_EVENT_COUNT, NEGATIVE_EVENT_COUNT, NEUTRAL_EVENT_COUNT,
        NEWS_PRESSURE, NEWS_SENTIMENT, UNCERTAINTY_SCORE, EVENT_RISK_SCORE, MACRO_HEAT,
        TOP_EVENTS,
        LAST_EVENT_TS, LAST_NEWS_PUBLISHED_AT, LAST_INGESTED_AT,
        SNAPSHOT_TS, NEWS_SNAPSHOT_AGE_MINUTES, NEWS_IS_STALE,
        DECAY_TAU_HOURS, LOOKBACK_HOURS,
        CREATED_AT, RUN_ID
    ) values (
        src.AS_OF_TS, src.SYMBOL, src.MARKET_TYPE,
        src.EVENT_COUNT, src.POSITIVE_EVENT_COUNT, src.NEGATIVE_EVENT_COUNT, src.NEUTRAL_EVENT_COUNT,
        src.NEWS_PRESSURE, src.NEWS_SENTIMENT, src.UNCERTAINTY_SCORE, src.EVENT_RISK_SCORE, src.MACRO_HEAT,
        src.TOP_EVENTS,
        src.LAST_EVENT_TS, src.LAST_NEWS_PUBLISHED_AT, src.LAST_INGESTED_AT,
        src.SNAPSHOT_TS, src.NEWS_SNAPSHOT_AGE_MINUTES, src.NEWS_IS_STALE,
        src.DECAY_TAU_HOURS, src.LOOKBACK_HOURS,
        src.CREATED_AT, src.RUN_ID
    );

    select count(*)
      into :v_rows_written
      from MIP.NEWS.NEWS_FEATURES_SNAPSHOT
     where AS_OF_TS = :v_as_of_ts
       and SNAPSHOT_TS = :v_as_of_ts;

    drop table if exists TMP_NEWS_EVENT_BASE;
    drop table if exists TMP_NEWS_FEATURE_AGG;
    drop table if exists TMP_NEWS_TOP_EVENTS;

    return object_construct(
        'status', 'SUCCESS',
        'as_of_ts_utc', :v_as_of_ts,
        'rows_written', :v_rows_written,
        'run_id', :v_run_id,
        'decay_tau_hours', :v_decay_tau_hours,
        'lookback_hours', :v_lookback_hours,
        'stale_minutes', :v_stale_minutes
    );
end;
$$;
