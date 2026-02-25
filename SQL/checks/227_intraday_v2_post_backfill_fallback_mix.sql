-- 227_intraday_v2_post_backfill_fallback_mix.sql
-- Purpose: Post-backfill diagnostic for fallback mix by pattern+horizon.

use role MIP_ADMIN_ROLE;
use database MIP;

-- 1) Fallback mix by pattern+horizon.
with cfg as (
    select
        'v1_1'::string as METRIC_VERSION,
        'v1'::string as BUCKET_VERSION,
        'BASELINE_FIXED20'::string as TRUST_CONFIG_VERSION,
        90::number as WINDOW_DAYS,
        20::number as MIN_SAMPLE,
        sha2(
            concat('v1_1', '|', 'v1', '|', to_varchar(90), '|', to_varchar(20), '|', 'BASELINE_FIXED20', '|', 'exact->regime_only->global'),
            256
        ) as TRUST_VERSION
),
as_of_cte as (
    select max(t.CALCULATED_AT) as AS_OF_TS
    from MIP.APP.INTRA_TRUST_STATS t
    join cfg c
      on t.METRIC_VERSION = c.METRIC_VERSION
     and t.BUCKET_VERSION = c.BUCKET_VERSION
     and t.TRUST_VERSION = c.TRUST_VERSION
)
select
    PATTERN_ID,
    MARKET_TYPE,
    INTERVAL_MINUTES,
    HORIZON_BARS,
    sum(iff(FALLBACK_LEVEL = 'EXACT', 1, 0)) as EXACT_ROWS,
    sum(iff(FALLBACK_LEVEL = 'REGIME_ONLY', 1, 0)) as REGIME_ONLY_ROWS,
    sum(iff(FALLBACK_LEVEL = 'GLOBAL', 1, 0)) as GLOBAL_ROWS,
    count(*) as TOTAL_ROWS,
    round(100.0 * sum(iff(FALLBACK_LEVEL = 'EXACT', 1, 0)) / nullif(count(*), 0), 2) as EXACT_PCT,
    round(100.0 * sum(iff(FALLBACK_LEVEL = 'REGIME_ONLY', 1, 0)) / nullif(count(*), 0), 2) as REGIME_ONLY_PCT,
    round(100.0 * sum(iff(FALLBACK_LEVEL = 'GLOBAL', 1, 0)) / nullif(count(*), 0), 2) as GLOBAL_PCT
from MIP.APP.INTRA_TRUST_STATS
where CALCULATED_AT = (select AS_OF_TS from as_of_cte)
  and METRIC_VERSION = (select METRIC_VERSION from cfg)
  and BUCKET_VERSION = (select BUCKET_VERSION from cfg)
  and TRUST_VERSION = (select TRUST_VERSION from cfg)
group by 1,2,3,4
order by GLOBAL_PCT desc, PATTERN_ID, HORIZON_BARS;

-- 2) Effective min-sample config rows used by current trust_config_version.
select
    TRUST_CONFIG_VERSION,
    PATTERN_ID,
    HORIZON_BARS,
    MIN_SAMPLE,
    IS_ACTIVE,
    VALID_FROM_TS,
    VALID_TO_TS
from MIP.APP.INTRA_TRUST_MIN_SAMPLE_CONFIG
where TRUST_CONFIG_VERSION = 'BASELINE_FIXED20'
  and IS_ACTIVE = true
order by PATTERN_ID, HORIZON_BARS, VALID_FROM_TS desc;
