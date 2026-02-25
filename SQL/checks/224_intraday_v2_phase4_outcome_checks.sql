-- 224_intraday_v2_phase4_outcome_checks.sql
-- Purpose: Phase 4 verification checks for INTRA_OUTCOMES.

use role MIP_ADMIN_ROLE;
use database MIP;

set start_ts = dateadd(day, -30, current_timestamp());
set end_ts = current_timestamp();
set metric_version = 'v1_1';
set bucket_version = 'v1';

-- Freeze to latest bridged legacy signal timestamp for deterministic rerun comparisons.
set end_ts = (
    select max(SIGNAL_TS)
    from MIP.APP.INTRA_SIGNALS
    where INTERVAL_MINUTES = 15
      and SOURCE_MODE = 'LEGACY_PATTERN'
      and METRIC_VERSION = $metric_version
      and BUCKET_VERSION = $bucket_version
);
set start_ts = dateadd(day, -30, $end_ts);

-- 1) Referential integrity: outcomes must map to signals by id + nk hash.
select
    count(*) as OUTCOME_ROWS,
    sum(
        iff(s.SIGNAL_ID is null, 1, 0)
    ) as MISSING_SIGNAL_ID_ROWS,
    sum(
        iff(s.SIGNAL_ID is not null and o.SIGNAL_NK_HASH <> s.SIGNAL_NK_HASH, 1, 0)
    ) as NK_HASH_MISMATCH_ROWS
from MIP.APP.INTRA_OUTCOMES o
left join MIP.APP.INTRA_SIGNALS s
  on s.SIGNAL_ID = o.SIGNAL_ID
where o.ENTRY_TS between $start_ts and $end_ts
  and o.METRIC_VERSION = $metric_version
  and o.BUCKET_VERSION = $bucket_version;

-- 2) Completeness by active horizon.
with sig as (
    select count(*) as SIGNAL_ROWS
    from MIP.APP.INTRA_SIGNALS
    where SIGNAL_TS between $start_ts and $end_ts
      and INTERVAL_MINUTES = 15
      and METRIC_VERSION = $metric_version
      and BUCKET_VERSION = $bucket_version
      and SOURCE_MODE = 'LEGACY_PATTERN'
),
hor as (
    select HORIZON_BARS
    from MIP.APP.INTRA_HORIZON_DEF
    where IS_ACTIVE = true
)
select
    h.HORIZON_BARS,
    s.SIGNAL_ROWS as TOTAL_SIGNALS,
    count(o.SIGNAL_ID) as OUTCOME_ROWS,
    sum(iff(o.EVAL_STATUS = 'SUCCESS', 1, 0)) as SUCCESS_ROWS,
    sum(iff(o.EVAL_STATUS = 'INSUFFICIENT_FUTURE_DATA', 1, 0)) as INSUFFICIENT_ROWS,
    round(100.0 * count(o.SIGNAL_ID) / nullif(s.SIGNAL_ROWS, 0), 4) as OUTCOME_COVERAGE_PCT
from hor h
cross join sig s
left join MIP.APP.INTRA_OUTCOMES o
  on o.HORIZON_BARS = h.HORIZON_BARS
 and o.ENTRY_TS between $start_ts and $end_ts
 and o.METRIC_VERSION = $metric_version
 and o.BUCKET_VERSION = $bucket_version
group by 1, 2
order by 1;

-- 3) Idempotency fingerprint.
select
    count(*) as ROW_COUNT,
    count(distinct concat(SIGNAL_NK_HASH, '|', HORIZON_BARS)) as DISTINCT_KEY_COUNT,
    min(ENTRY_TS) as MIN_ENTRY_TS,
    max(ENTRY_TS) as MAX_ENTRY_TS,
    sum(abs(hash(concat(SIGNAL_NK_HASH, '|', HORIZON_BARS, '|', coalesce(to_varchar(RETURN_NET), 'NULL'))))) as OUTCOME_FINGERPRINT
from MIP.APP.INTRA_OUTCOMES
where ENTRY_TS between $start_ts and $end_ts
  and METRIC_VERSION = $metric_version
  and BUCKET_VERSION = $bucket_version;

-- 4) Return sanity checks.
select
    min(RETURN_NET) as MIN_RETURN_NET,
    max(RETURN_NET) as MAX_RETURN_NET,
    avg(RETURN_NET) as AVG_RETURN_NET,
    sum(iff(abs(RETURN_NET) > 0.5, 1, 0)) as EXTREME_ABS_GT_50PCT_ROWS,
    sum(iff(RETURN_NET = 0, 1, 0)) as EXACT_ZERO_RETURN_ROWS
from MIP.APP.INTRA_OUTCOMES
where ENTRY_TS between $start_ts and $end_ts
  and METRIC_VERSION = $metric_version
  and BUCKET_VERSION = $bucket_version
  and EVAL_STATUS = 'SUCCESS';
