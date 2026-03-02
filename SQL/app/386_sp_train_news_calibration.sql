-- 386_sp_train_news_calibration.sql
-- Purpose: Train news-conditioned calibration multipliers from outcomes.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_TRAIN_NEWS_CALIBRATION(
    P_RUN_ID string,
    P_TRAINING_VERSION string,
    P_START_DATE date default '2025-09-01'::date,
    P_END_DATE date default current_date(),
    P_MARKET_TYPE string default null
)
returns variant
language sql
execute as caller
as
$$
declare
    v_run_id string := coalesce(:P_RUN_ID, uuid_string());
    v_training_version string := coalesce(:P_TRAINING_VERSION, 'NEWS_CAL_V1');
    v_start_date date := coalesce(:P_START_DATE, '2025-09-01'::date);
    v_end_date date := coalesce(:P_END_DATE, current_date());
    v_market_type string := :P_MARKET_TYPE;

    v_min_n number := 80;
    v_shrink_k float := 150;
    v_cap_lo float := 0.85;
    v_cap_hi float := 1.15;

    v_rows_enriched number := 0;
    v_total_buckets number := 0;
    v_eligible_buckets number := 0;
begin
    execute immediate 'use schema MIP.APP';

    select
        coalesce(max(case when CONFIG_KEY = 'DAILY_NEWS_CAL_MIN_N' then try_to_number(CONFIG_VALUE) end), :v_min_n),
        coalesce(max(case when CONFIG_KEY = 'DAILY_NEWS_CAL_SHRINK_K' then try_to_double(CONFIG_VALUE) end), :v_shrink_k),
        coalesce(max(case when CONFIG_KEY = 'DAILY_NEWS_CAL_MULT_CAP_LO' then try_to_double(CONFIG_VALUE) end), :v_cap_lo),
        coalesce(max(case when CONFIG_KEY = 'DAILY_NEWS_CAL_MULT_CAP_HI' then try_to_double(CONFIG_VALUE) end), :v_cap_hi)
      into :v_min_n, :v_shrink_k, :v_cap_lo, :v_cap_hi
      from MIP.APP.APP_CONFIG
     where CONFIG_KEY in ('DAILY_NEWS_CAL_MIN_N', 'DAILY_NEWS_CAL_SHRINK_K', 'DAILY_NEWS_CAL_MULT_CAP_LO', 'DAILY_NEWS_CAL_MULT_CAP_HI');

    -- 1) Enrich outcomes with feature buckets at entry_ts.
    create or replace temporary table TMP_NEWS_OUTCOME_ENRICH as
    with outcome_scope as (
        select
            o.RECOMMENDATION_ID,
            o.HORIZON_BARS,
            o.ENTRY_TS,
            r.SYMBOL,
            r.MARKET_TYPE
        from MIP.APP.RECOMMENDATION_OUTCOMES o
        join MIP.APP.RECOMMENDATION_LOG r
          on r.RECOMMENDATION_ID = o.RECOMMENDATION_ID
        where o.EVAL_STATUS = 'SUCCESS'
          and o.ENTRY_TS::date between :v_start_date and :v_end_date
          and (:v_market_type is null or r.MARKET_TYPE = :v_market_type)
    ),
    feature_candidates as (
        select
            s.RECOMMENDATION_ID,
            s.HORIZON_BARS,
            f.AS_OF_TS,
            f.SNAPSHOT_TS,
            f.NEWS_PRESSURE,
            f.NEWS_SENTIMENT,
            f.UNCERTAINTY_SCORE,
            f.EVENT_RISK_SCORE,
            f.MACRO_HEAT,
            f.TOP_EVENTS,
            row_number() over (
                partition by s.RECOMMENDATION_ID, s.HORIZON_BARS
                order by f.AS_OF_TS desc, f.SNAPSHOT_TS desc
            ) as RN
        from outcome_scope s
        left join MIP.MART.V_NEWS_FEATURES_BY_TS f
          on f.SYMBOL = s.SYMBOL
         and f.MARKET_TYPE = s.MARKET_TYPE
         and f.AS_OF_TS <= s.ENTRY_TS
    )
    select
        s.RECOMMENDATION_ID,
        s.HORIZON_BARS,
        c.SNAPSHOT_TS as NEWS_FEATURE_SNAPSHOT_TS,
        iff(c.SNAPSHOT_TS is null, null, datediff('minute', c.SNAPSHOT_TS, s.ENTRY_TS)) as NEWS_FEATURE_AGE_MINUTES,
        case
            when c.NEWS_PRESSURE is null then 'UNKNOWN'
            when c.NEWS_PRESSURE >= 0.80 then 'P4_VERY_HIGH'
            when c.NEWS_PRESSURE >= 0.60 then 'P3_HIGH'
            when c.NEWS_PRESSURE >= 0.30 then 'P2_MED'
            else 'P1_LOW'
        end as NEWS_PRESSURE_BUCKET,
        case
            when c.NEWS_SENTIMENT is null then 'UNKNOWN'
            when c.NEWS_SENTIMENT >= 0.25 then 'POS'
            when c.NEWS_SENTIMENT <= -0.25 then 'NEG'
            else 'NEU'
        end as NEWS_SENTIMENT_BUCKET,
        case
            when c.UNCERTAINTY_SCORE is null then 'UNKNOWN'
            when c.UNCERTAINTY_SCORE >= 0.60 then 'HIGH'
            when c.UNCERTAINTY_SCORE >= 0.30 then 'MED'
            else 'LOW'
        end as NEWS_UNCERTAINTY_BUCKET,
        case
            when c.EVENT_RISK_SCORE is null then 'UNKNOWN'
            when c.EVENT_RISK_SCORE >= 0.70 or coalesce(c.MACRO_HEAT, 0) >= 0.60 then 'HIGH'
            when c.EVENT_RISK_SCORE >= 0.35 then 'MED'
            else 'LOW'
        end as NEWS_EVENT_RISK_BUCKET,
        object_construct(
            'news_pressure', c.NEWS_PRESSURE,
            'news_sentiment', c.NEWS_SENTIMENT,
            'uncertainty_score', c.UNCERTAINTY_SCORE,
            'event_risk_score', c.EVENT_RISK_SCORE,
            'macro_heat', c.MACRO_HEAT,
            'top_events', c.TOP_EVENTS
        ) as NEWS_FEATURES_JSON
    from outcome_scope s
    left join feature_candidates c
      on c.RECOMMENDATION_ID = s.RECOMMENDATION_ID
     and c.HORIZON_BARS = s.HORIZON_BARS
     and c.RN = 1;

    merge into MIP.APP.RECOMMENDATION_OUTCOMES t
    using TMP_NEWS_OUTCOME_ENRICH s
      on t.RECOMMENDATION_ID = s.RECOMMENDATION_ID
     and t.HORIZON_BARS = s.HORIZON_BARS
    when matched then update set
        t.NEWS_PRESSURE_BUCKET = s.NEWS_PRESSURE_BUCKET,
        t.NEWS_SENTIMENT_BUCKET = s.NEWS_SENTIMENT_BUCKET,
        t.NEWS_UNCERTAINTY_BUCKET = s.NEWS_UNCERTAINTY_BUCKET,
        t.NEWS_EVENT_RISK_BUCKET = s.NEWS_EVENT_RISK_BUCKET,
        t.NEWS_FEATURE_SNAPSHOT_TS = s.NEWS_FEATURE_SNAPSHOT_TS,
        t.NEWS_FEATURE_AGE_MINUTES = s.NEWS_FEATURE_AGE_MINUTES,
        t.NEWS_FEATURES_JSON = s.NEWS_FEATURES_JSON;

    select count(*) into :v_rows_enriched from TMP_NEWS_OUTCOME_ENRICH;

    -- 2) Train bucket multipliers vs baseline by market_type + horizon.
    create or replace temporary table TMP_NEWS_CAL_SOURCE as
    select
        r.MARKET_TYPE,
        o.HORIZON_BARS,
        coalesce(o.NEWS_PRESSURE_BUCKET, 'UNKNOWN') as NEWS_PRESSURE_BUCKET,
        coalesce(o.NEWS_SENTIMENT_BUCKET, 'UNKNOWN') as NEWS_SENTIMENT_BUCKET,
        coalesce(o.NEWS_UNCERTAINTY_BUCKET, 'UNKNOWN') as NEWS_UNCERTAINTY_BUCKET,
        coalesce(o.NEWS_EVENT_RISK_BUCKET, 'UNKNOWN') as NEWS_EVENT_RISK_BUCKET,
        o.REALIZED_RETURN,
        iff(o.HIT_FLAG, 1.0, 0.0) as HIT_FLOAT
    from MIP.APP.RECOMMENDATION_OUTCOMES o
    join MIP.APP.RECOMMENDATION_LOG r
      on r.RECOMMENDATION_ID = o.RECOMMENDATION_ID
    where o.EVAL_STATUS = 'SUCCESS'
      and o.REALIZED_RETURN is not null
      and o.ENTRY_TS::date between :v_start_date and :v_end_date
      and (:v_market_type is null or r.MARKET_TYPE = :v_market_type);

    create or replace temporary table TMP_NEWS_CAL_BASELINE as
    select
        MARKET_TYPE,
        HORIZON_BARS,
        count(*) as BASELINE_N,
        avg(REALIZED_RETURN) as BASELINE_AVG_RETURN,
        avg(HIT_FLOAT) as BASELINE_WIN_RATE
    from TMP_NEWS_CAL_SOURCE
    group by MARKET_TYPE, HORIZON_BARS;

    create or replace temporary table TMP_NEWS_CAL_BUCKETS as
    select
        s.MARKET_TYPE,
        s.HORIZON_BARS,
        s.NEWS_PRESSURE_BUCKET,
        s.NEWS_SENTIMENT_BUCKET,
        s.NEWS_UNCERTAINTY_BUCKET,
        s.NEWS_EVENT_RISK_BUCKET,
        count(*) as N_OUTCOMES,
        avg(s.REALIZED_RETURN) as BUCKET_AVG_RETURN,
        avg(s.HIT_FLOAT) as BUCKET_WIN_RATE
    from TMP_NEWS_CAL_SOURCE s
    group by
        s.MARKET_TYPE,
        s.HORIZON_BARS,
        s.NEWS_PRESSURE_BUCKET,
        s.NEWS_SENTIMENT_BUCKET,
        s.NEWS_UNCERTAINTY_BUCKET,
        s.NEWS_EVENT_RISK_BUCKET;

    merge into MIP.APP.DAILY_NEWS_CALIBRATION_TRAINED t
    using (
        select
            :v_training_version as TRAINING_VERSION,
            b.MARKET_TYPE,
            b.HORIZON_BARS,
            b.NEWS_PRESSURE_BUCKET,
            b.NEWS_SENTIMENT_BUCKET,
            b.NEWS_UNCERTAINTY_BUCKET,
            b.NEWS_EVENT_RISK_BUCKET,
            b.N_OUTCOMES,
            b.BUCKET_AVG_RETURN,
            b.BUCKET_WIN_RATE,
            l.BASELINE_AVG_RETURN,
            l.BASELINE_WIN_RATE,
            iff(l.BASELINE_WIN_RATE is not null and l.BASELINE_WIN_RATE > 0, b.BUCKET_WIN_RATE / l.BASELINE_WIN_RATE, 1.0) as RAW_MULTIPLIER,
            iff(b.N_OUTCOMES > 0, b.N_OUTCOMES / (b.N_OUTCOMES + :v_shrink_k), 0.0) as SHRINK_FACTOR,
            1.0 + iff(b.N_OUTCOMES > 0, b.N_OUTCOMES / (b.N_OUTCOMES + :v_shrink_k), 0.0)
                * (iff(l.BASELINE_WIN_RATE is not null and l.BASELINE_WIN_RATE > 0, b.BUCKET_WIN_RATE / l.BASELINE_WIN_RATE, 1.0) - 1.0) as SHRUNK_MULTIPLIER,
            least(
                :v_cap_hi,
                greatest(
                    :v_cap_lo,
                    1.0 + iff(b.N_OUTCOMES > 0, b.N_OUTCOMES / (b.N_OUTCOMES + :v_shrink_k), 0.0)
                        * (iff(l.BASELINE_WIN_RATE is not null and l.BASELINE_WIN_RATE > 0, b.BUCKET_WIN_RATE / l.BASELINE_WIN_RATE, 1.0) - 1.0)
                )
            ) as MULTIPLIER_CAPPED,
            iff(b.N_OUTCOMES >= :v_min_n, true, false) as ELIGIBLE_FLAG,
            iff(b.N_OUTCOMES >= :v_min_n, 'ELIGIBLE', 'INSUFFICIENT_N') as REASON,
            :v_run_id as RUN_ID,
            current_timestamp() as CALCULATED_AT
        from TMP_NEWS_CAL_BUCKETS b
        join TMP_NEWS_CAL_BASELINE l
          on l.MARKET_TYPE = b.MARKET_TYPE
         and l.HORIZON_BARS = b.HORIZON_BARS
    ) src
    on t.TRAINING_VERSION = src.TRAINING_VERSION
   and t.MARKET_TYPE = src.MARKET_TYPE
   and t.HORIZON_BARS = src.HORIZON_BARS
   and t.NEWS_PRESSURE_BUCKET = src.NEWS_PRESSURE_BUCKET
   and t.NEWS_SENTIMENT_BUCKET = src.NEWS_SENTIMENT_BUCKET
   and t.NEWS_UNCERTAINTY_BUCKET = src.NEWS_UNCERTAINTY_BUCKET
   and t.NEWS_EVENT_RISK_BUCKET = src.NEWS_EVENT_RISK_BUCKET
    when matched then update set
        t.N_OUTCOMES = src.N_OUTCOMES,
        t.BUCKET_AVG_RETURN = src.BUCKET_AVG_RETURN,
        t.BUCKET_WIN_RATE = src.BUCKET_WIN_RATE,
        t.BASELINE_AVG_RETURN = src.BASELINE_AVG_RETURN,
        t.BASELINE_WIN_RATE = src.BASELINE_WIN_RATE,
        t.RAW_MULTIPLIER = src.RAW_MULTIPLIER,
        t.SHRINK_FACTOR = src.SHRINK_FACTOR,
        t.SHRUNK_MULTIPLIER = src.SHRUNK_MULTIPLIER,
        t.MULTIPLIER_CAPPED = src.MULTIPLIER_CAPPED,
        t.ELIGIBLE_FLAG = src.ELIGIBLE_FLAG,
        t.REASON = src.REASON,
        t.RUN_ID = src.RUN_ID,
        t.CALCULATED_AT = src.CALCULATED_AT
    when not matched then insert (
        TRAINING_VERSION, MARKET_TYPE, HORIZON_BARS,
        NEWS_PRESSURE_BUCKET, NEWS_SENTIMENT_BUCKET, NEWS_UNCERTAINTY_BUCKET, NEWS_EVENT_RISK_BUCKET,
        N_OUTCOMES, BUCKET_AVG_RETURN, BUCKET_WIN_RATE, BASELINE_AVG_RETURN, BASELINE_WIN_RATE,
        RAW_MULTIPLIER, SHRINK_FACTOR, SHRUNK_MULTIPLIER, MULTIPLIER_CAPPED,
        ELIGIBLE_FLAG, REASON, RUN_ID, CALCULATED_AT
    ) values (
        src.TRAINING_VERSION, src.MARKET_TYPE, src.HORIZON_BARS,
        src.NEWS_PRESSURE_BUCKET, src.NEWS_SENTIMENT_BUCKET, src.NEWS_UNCERTAINTY_BUCKET, src.NEWS_EVENT_RISK_BUCKET,
        src.N_OUTCOMES, src.BUCKET_AVG_RETURN, src.BUCKET_WIN_RATE, src.BASELINE_AVG_RETURN, src.BASELINE_WIN_RATE,
        src.RAW_MULTIPLIER, src.SHRINK_FACTOR, src.SHRUNK_MULTIPLIER, src.MULTIPLIER_CAPPED,
        src.ELIGIBLE_FLAG, src.REASON, src.RUN_ID, src.CALCULATED_AT
    );

    select
        count(*) as TOTAL_BUCKETS,
        coalesce(count_if(ELIGIBLE_FLAG), 0) as ELIGIBLE_BUCKETS
      into :v_total_buckets, :v_eligible_buckets
      from MIP.APP.DAILY_NEWS_CALIBRATION_TRAINED
     where TRAINING_VERSION = :v_training_version
       and (:v_market_type is null or MARKET_TYPE = :v_market_type);

    return object_construct(
        'status', 'SUCCESS',
        'run_id', :v_run_id,
        'training_version', :v_training_version,
        'start_date', :v_start_date,
        'end_date', :v_end_date,
        'market_type', :v_market_type,
        'rows_enriched', :v_rows_enriched,
        'total_buckets', :v_total_buckets,
        'eligible_buckets', :v_eligible_buckets,
        'min_n', :v_min_n
    );
end;
$$;
