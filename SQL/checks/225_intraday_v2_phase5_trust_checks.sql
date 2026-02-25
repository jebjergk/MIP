-- 225_intraday_v2_phase5_trust_checks.sql
-- Purpose: Phase 5 verification checks for INTRA_TRUST_STATS snapshots.

use role MIP_ADMIN_ROLE;
use database MIP;

-- 1) Snapshot footprint + fallback mix.
with params as (
    select
        'v1_1'::string as METRIC_VERSION,
        'v1'::string as BUCKET_VERSION,
        90::number as WINDOW_DAYS,
        20::number as MIN_SAMPLE,
        'exact->regime_only->global'::string as FALLBACK_RULES
),
trust_params as (
    select
        p.*,
        sha2(
            concat(p.METRIC_VERSION, '|', p.BUCKET_VERSION, '|', to_varchar(p.WINDOW_DAYS), '|', to_varchar(p.MIN_SAMPLE), '|', p.FALLBACK_RULES),
            256
        ) as TRUST_VERSION
    from params p
),
as_of_cte as (
    select max(t.CALCULATED_AT) as AS_OF_TS
    from MIP.APP.INTRA_TRUST_STATS t
    join trust_params tp
      on t.METRIC_VERSION = tp.METRIC_VERSION
     and t.BUCKET_VERSION = tp.BUCKET_VERSION
     and t.TRUST_VERSION = tp.TRUST_VERSION
)
select
    t.CALCULATED_AT,
    count(*) as SNAPSHOT_ROWS,
    count(distinct concat(PATTERN_ID, '|', MARKET_TYPE, '|', INTERVAL_MINUTES, '|', HORIZON_BARS, '|', STATE_BUCKET_ID)) as DISTINCT_GRAIN_ROWS,
    sum(iff(FALLBACK_LEVEL = 'EXACT', 1, 0)) as EXACT_ROWS,
    sum(iff(FALLBACK_LEVEL = 'REGIME_ONLY', 1, 0)) as REGIME_ONLY_ROWS,
    sum(iff(FALLBACK_LEVEL = 'GLOBAL', 1, 0)) as GLOBAL_ROWS
from MIP.APP.INTRA_TRUST_STATS t
join trust_params tp
  on t.METRIC_VERSION = tp.METRIC_VERSION
 and t.BUCKET_VERSION = tp.BUCKET_VERSION
 and t.TRUST_VERSION = tp.TRUST_VERSION
join as_of_cte x
  on t.CALCULATED_AT = x.AS_OF_TS
group by 1;

-- 2) Deterministic runtime selection check (tie-break rule).
with ranked as (
    select
        t.*,
        row_number() over (
            partition by PATTERN_ID, MARKET_TYPE, INTERVAL_MINUTES, HORIZON_BARS, STATE_BUCKET_ID
            order by CALCULATED_AT desc, TRAIN_WINDOW_END desc
        ) as RN
    from MIP.APP.INTRA_TRUST_STATS t
    join (
        select
            'v1_1'::string as METRIC_VERSION,
            'v1'::string as BUCKET_VERSION,
            sha2(concat('v1_1', '|', 'v1', '|', to_varchar(90), '|', to_varchar(20), '|', 'BASELINE_FIXED20', '|', 'exact->regime_only->global'), 256) as TRUST_VERSION
    ) p
      on t.METRIC_VERSION = p.METRIC_VERSION
     and t.BUCKET_VERSION = p.BUCKET_VERSION
     and t.TRUST_VERSION = p.TRUST_VERSION
    where t.CALCULATED_AT <= (
        select max(CALCULATED_AT)
        from MIP.APP.INTRA_TRUST_STATS
        where METRIC_VERSION = p.METRIC_VERSION
          and BUCKET_VERSION = p.BUCKET_VERSION
          and TRUST_VERSION = p.TRUST_VERSION
    )
)
select
    count(*) as SELECTED_ROWS,
    count(distinct concat(PATTERN_ID, '|', MARKET_TYPE, '|', INTERVAL_MINUTES, '|', HORIZON_BARS, '|', STATE_BUCKET_ID)) as DISTINCT_GRAIN_ROWS
from ranked
where RN = 1;

-- 3) Sample threshold + fallback correctness.
select
    sum(iff(N_SIGNALS < 20 and FALLBACK_LEVEL = 'EXACT', 1, 0)) as INVALID_EXACT_BELOW_MIN_SAMPLE,
    sum(iff(N_SIGNALS >= 20 and FALLBACK_LEVEL = 'GLOBAL', 1, 0)) as POTENTIAL_UNNEEDED_GLOBAL_ROWS
from MIP.APP.INTRA_TRUST_STATS
where CALCULATED_AT = (
    select max(CALCULATED_AT)
    from MIP.APP.INTRA_TRUST_STATS
    where METRIC_VERSION = 'v1_1'
      and BUCKET_VERSION = 'v1'
      and TRUST_VERSION = sha2(concat('v1_1', '|', 'v1', '|', to_varchar(90), '|', to_varchar(20), '|', 'BASELINE_FIXED20', '|', 'exact->regime_only->global'), 256)
)
  and METRIC_VERSION = 'v1_1'
  and BUCKET_VERSION = 'v1'
  and TRUST_VERSION = sha2(concat('v1_1', '|', 'v1', '|', to_varchar(90), '|', to_varchar(20), '|', 'BASELINE_FIXED20', '|', 'exact->regime_only->global'), 256);

-- 4) Value sanity.
select
    min(HIT_RATE) as MIN_HIT_RATE,
    max(HIT_RATE) as MAX_HIT_RATE,
    sum(iff(HIT_RATE < 0 or HIT_RATE > 1, 1, 0)) as OUT_OF_RANGE_HIT_RATE_ROWS,
    min(CI_WIDTH) as MIN_CI_WIDTH,
    max(CI_WIDTH) as MAX_CI_WIDTH
from MIP.APP.INTRA_TRUST_STATS
where CALCULATED_AT = (
    select max(CALCULATED_AT)
    from MIP.APP.INTRA_TRUST_STATS
    where METRIC_VERSION = 'v1_1'
      and BUCKET_VERSION = 'v1'
      and TRUST_VERSION = sha2(concat('v1_1', '|', 'v1', '|', to_varchar(90), '|', to_varchar(20), '|', 'BASELINE_FIXED20', '|', 'exact->regime_only->global'), 256)
)
  and METRIC_VERSION = 'v1_1'
  and BUCKET_VERSION = 'v1'
  and TRUST_VERSION = sha2(concat('v1_1', '|', 'v1', '|', to_varchar(90), '|', to_varchar(20), '|', 'BASELINE_FIXED20', '|', 'exact->regime_only->global'), 256);

-- 5) CI width by sample-size band (should generally shrink as sample grows).
with binned as (
    select
        case
            when N_SIGNALS < 20 then '<20'
            when N_SIGNALS < 50 then '20-49'
            when N_SIGNALS < 100 then '50-99'
            else '100+'
        end as SAMPLE_BAND,
        CI_WIDTH
    from MIP.APP.INTRA_TRUST_STATS
    where CALCULATED_AT = (
        select max(CALCULATED_AT)
        from MIP.APP.INTRA_TRUST_STATS
        where METRIC_VERSION = 'v1_1'
          and BUCKET_VERSION = 'v1'
          and TRUST_VERSION = sha2(concat('v1_1', '|', 'v1', '|', to_varchar(90), '|', to_varchar(20), '|', 'BASELINE_FIXED20', '|', 'exact->regime_only->global'), 256)
    )
      and METRIC_VERSION = 'v1_1'
      and BUCKET_VERSION = 'v1'
      and TRUST_VERSION = sha2(concat('v1_1', '|', 'v1', '|', to_varchar(90), '|', to_varchar(20), '|', 'BASELINE_FIXED20', '|', 'exact->regime_only->global'), 256)
      and CI_WIDTH is not null
)
select
    SAMPLE_BAND,
    count(*) as ROWS_IN_BAND,
    avg(CI_WIDTH) as AVG_CI_WIDTH
from binned
group by 1
order by
    case SAMPLE_BAND
        when '<20' then 1
        when '20-49' then 2
        when '50-99' then 3
        else 4
    end;
