-- 385_news_calibration_policy.sql
-- Purpose: Phase D policy scaffolding for news-conditioned calibration.

use role MIP_ADMIN_ROLE;
use database MIP;

merge into MIP.APP.APP_CONFIG t
using (
    select 'ENABLE_DAILY_NEWS_CALIBRATION' as CONFIG_KEY, 'false' as CONFIG_VALUE,
           'Feature flag for daily news-conditioned calibration multipliers' as DESCRIPTION
    union all
    select 'DAILY_NEWS_CAL_ACTIVE_TRAINING_VERSION', 'CURRENT',
           'Active training version for news calibration apply step'
    union all
    select 'DAILY_NEWS_CAL_MIN_N', '80',
           'Minimum outcomes per bucket to mark bucket-eligible'
    union all
    select 'DAILY_NEWS_CAL_SHRINK_K', '150',
           'Shrinkage parameter k for n/(n+k) shrink-to-1.0'
    union all
    select 'DAILY_NEWS_CAL_MULT_CAP_LO', '0.85',
           'Lower cap for news calibration multiplier'
    union all
    select 'DAILY_NEWS_CAL_MULT_CAP_HI', '1.15',
           'Upper cap for news calibration multiplier'
    union all
    select 'DAILY_NEWS_CAL_DEFAULT_MULT', '1.00',
           'Fallback multiplier when no eligible bucket exists'
) s
on t.CONFIG_KEY = s.CONFIG_KEY
when matched then update set
    t.CONFIG_VALUE = s.CONFIG_VALUE,
    t.DESCRIPTION = s.DESCRIPTION,
    t.UPDATED_AT = current_timestamp()
when not matched then insert (CONFIG_KEY, CONFIG_VALUE, DESCRIPTION, UPDATED_AT)
values (s.CONFIG_KEY, s.CONFIG_VALUE, s.DESCRIPTION, current_timestamp());

create table if not exists MIP.APP.DAILY_NEWS_CALIBRATION_TRAINED (
    TRAINING_VERSION               string        not null,
    MARKET_TYPE                    string        not null,
    HORIZON_BARS                   number        not null,
    NEWS_PRESSURE_BUCKET           string        not null,
    NEWS_SENTIMENT_BUCKET          string        not null,
    NEWS_UNCERTAINTY_BUCKET        string        not null,
    NEWS_EVENT_RISK_BUCKET         string        not null,
    N_OUTCOMES                     number,
    BUCKET_AVG_RETURN              float,
    BUCKET_WIN_RATE                float,
    BASELINE_AVG_RETURN            float,
    BASELINE_WIN_RATE              float,
    RAW_MULTIPLIER                 float,
    SHRINK_FACTOR                  float,
    SHRUNK_MULTIPLIER              float,
    MULTIPLIER_CAPPED              float,
    ELIGIBLE_FLAG                  boolean,
    REASON                         string,
    RUN_ID                         string,
    CALCULATED_AT                  timestamp_ntz default current_timestamp(),
    constraint PK_DAILY_NEWS_CALIBRATION_TRAINED primary key (
        TRAINING_VERSION, MARKET_TYPE, HORIZON_BARS,
        NEWS_PRESSURE_BUCKET, NEWS_SENTIMENT_BUCKET, NEWS_UNCERTAINTY_BUCKET, NEWS_EVENT_RISK_BUCKET
    )
);

create table if not exists MIP.APP.DAILY_NEWS_CALIBRATION_APPLY_LOG (
    RUN_ID                         string        not null,
    TRAINING_VERSION               string        not null,
    APPLIED_AT                     timestamp_ntz not null default current_timestamp(),
    TARGET_RUN_ID                  string,
    PORTFOLIO_ID                   number,
    PROPOSALS_TOUCHED              number,
    PROPOSALS_WITH_ELIGIBLE_MULT   number,
    AVG_MULTIPLIER                 float,
    DETAILS                        variant,
    constraint PK_DAILY_NEWS_CALIBRATION_APPLY_LOG primary key (RUN_ID, TRAINING_VERSION)
);

-- Persist news feature buckets on outcomes for learning.
alter table if exists MIP.APP.RECOMMENDATION_OUTCOMES
    add column if not exists NEWS_PRESSURE_BUCKET string;
alter table if exists MIP.APP.RECOMMENDATION_OUTCOMES
    add column if not exists NEWS_SENTIMENT_BUCKET string;
alter table if exists MIP.APP.RECOMMENDATION_OUTCOMES
    add column if not exists NEWS_UNCERTAINTY_BUCKET string;
alter table if exists MIP.APP.RECOMMENDATION_OUTCOMES
    add column if not exists NEWS_EVENT_RISK_BUCKET string;
alter table if exists MIP.APP.RECOMMENDATION_OUTCOMES
    add column if not exists NEWS_FEATURE_SNAPSHOT_TS timestamp_ntz;
alter table if exists MIP.APP.RECOMMENDATION_OUTCOMES
    add column if not exists NEWS_FEATURE_AGE_MINUTES number;
alter table if exists MIP.APP.RECOMMENDATION_OUTCOMES
    add column if not exists NEWS_FEATURES_JSON variant;
