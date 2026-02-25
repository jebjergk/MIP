-- 230_intraday_v2_override_delta_diagnostics.sql
-- Purpose: Compare fallback mix baseline vs override trust configs and show current terrain distribution.

use role MIP_ADMIN_ROLE;
use database MIP;

with versions as (
    select
        'BASELINE_FIXED20' as CFG_NAME,
        sha2(concat('v1_1','|','v1','|',to_varchar(90),'|',to_varchar(20),'|','BASELINE_FIXED20','|','exact->regime_only->global'),256) as TRUST_VERSION
    union all
    select
        'OVR_H04H08_V1',
        sha2(concat('v1_1','|','v1','|',to_varchar(90),'|',to_varchar(20),'|','OVR_H04H08_V1','|','exact->regime_only->global'),256)
),
as_of_tbl as (
    select
        v.CFG_NAME,
        max(t.CALCULATED_AT) as AS_OF_TS
    from versions v
    join MIP.APP.INTRA_TRUST_STATS t
      on t.TRUST_VERSION = v.TRUST_VERSION
     and t.METRIC_VERSION = 'v1_1'
     and t.BUCKET_VERSION = 'v1'
    group by 1
),
mix as (
    select
        aot.CFG_NAME,
        t.PATTERN_ID,
        t.HORIZON_BARS,
        sum(iff(t.FALLBACK_LEVEL = 'EXACT', 1, 0)) as EXACT_ROWS,
        sum(iff(t.FALLBACK_LEVEL = 'REGIME_ONLY', 1, 0)) as REGIME_ONLY_ROWS,
        sum(iff(t.FALLBACK_LEVEL = 'GLOBAL', 1, 0)) as GLOBAL_ROWS,
        count(*) as TOTAL_ROWS
    from as_of_tbl aot
    join versions v on v.CFG_NAME = aot.CFG_NAME
    join MIP.APP.INTRA_TRUST_STATS t
      on t.CALCULATED_AT = aot.AS_OF_TS
     and t.TRUST_VERSION = v.TRUST_VERSION
     and t.METRIC_VERSION = 'v1_1'
     and t.BUCKET_VERSION = 'v1'
    group by 1,2,3
),
base as (
    select * from mix where CFG_NAME = 'BASELINE_FIXED20'
),
ovr as (
    select * from mix where CFG_NAME = 'OVR_H04H08_V1'
)
select
    coalesce(b.PATTERN_ID, o.PATTERN_ID) as PATTERN_ID,
    coalesce(b.HORIZON_BARS, o.HORIZON_BARS) as HORIZON_BARS,
    coalesce(b.EXACT_ROWS, 0) as BASE_EXACT_ROWS,
    coalesce(o.EXACT_ROWS, 0) as OVR_EXACT_ROWS,
    coalesce(o.EXACT_ROWS, 0) - coalesce(b.EXACT_ROWS, 0) as DELTA_EXACT_ROWS,
    coalesce(b.REGIME_ONLY_ROWS, 0) as BASE_REGIME_ROWS,
    coalesce(o.REGIME_ONLY_ROWS, 0) as OVR_REGIME_ROWS,
    coalesce(o.REGIME_ONLY_ROWS, 0) - coalesce(b.REGIME_ONLY_ROWS, 0) as DELTA_REGIME_ROWS,
    coalesce(b.GLOBAL_ROWS, 0) as BASE_GLOBAL_ROWS,
    coalesce(o.GLOBAL_ROWS, 0) as OVR_GLOBAL_ROWS,
    coalesce(o.GLOBAL_ROWS, 0) - coalesce(b.GLOBAL_ROWS, 0) as DELTA_GLOBAL_ROWS
from base b
full outer join ovr o
  on o.PATTERN_ID = b.PATTERN_ID
 and o.HORIZON_BARS = b.HORIZON_BARS
order by PATTERN_ID, HORIZON_BARS;

-- Terrain score distribution summary for the currently materialized terrain rows.
select
    min(TERRAIN_SCORE) as MIN_SCORE,
    max(TERRAIN_SCORE) as MAX_SCORE,
    stddev_samp(TERRAIN_SCORE) as STD_SCORE,
    count(distinct TERRAIN_SCORE) as DISTINCT_SCORE_COUNT
from MIP.APP.OPPORTUNITY_TERRAIN_15M
where METRIC_VERSION = 'v1_1'
  and BUCKET_VERSION = 'v1'
  and TERRAIN_VERSION = 'v1';
