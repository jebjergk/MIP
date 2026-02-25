-- 223_intraday_v2_phase3_signal_bridge_checks.sql
-- Purpose: Phase 3 verification checks for legacy -> INTRA_SIGNALS bridge.

use role MIP_ADMIN_ROLE;
use database MIP;

set start_ts = dateadd(day, -30, current_timestamp());
set end_ts = current_timestamp();
set metric_version = 'v1_1';
set bucket_version = 'v1';

-- 1) Source-vs-target count equivalence for eligible rows.
with source_eligible as (
    select count(*) as SRC_ROWS
    from MIP.APP.RECOMMENDATION_LOG rl
    join MIP.APP.PATTERN_DEFINITION pd
      on pd.PATTERN_ID = rl.PATTERN_ID
    join MIP.APP.STATE_SNAPSHOT_15M ss
      on ss.MARKET_TYPE = rl.MARKET_TYPE
     and ss.SYMBOL = rl.SYMBOL
     and ss.INTERVAL_MINUTES = rl.INTERVAL_MINUTES
     and ss.TS = rl.TS
     and ss.METRIC_VERSION = $metric_version
     and ss.BUCKET_VERSION = $bucket_version
    where rl.INTERVAL_MINUTES = 15
      and rl.TS between $start_ts and $end_ts
      and pd.PATTERN_TYPE in ('ORB', 'PULLBACK_CONTINUATION', 'MEAN_REVERSION')
),
target as (
    select count(*) as TGT_ROWS
    from MIP.APP.INTRA_SIGNALS
    where INTERVAL_MINUTES = 15
      and SIGNAL_TS between $start_ts and $end_ts
      and METRIC_VERSION = $metric_version
      and BUCKET_VERSION = $bucket_version
      and SOURCE_MODE = 'LEGACY_PATTERN'
)
select
    s.SRC_ROWS as SOURCE_ELIGIBLE_ROWS,
    t.TGT_ROWS as TARGET_ROWS,
    t.TGT_ROWS - s.SRC_ROWS as DELTA_ROWS
from source_eligible s
cross join target t;

-- 2) Duplicate protection by SIGNAL_NK_HASH.
select
    SIGNAL_NK_HASH,
    count(*) as DUP_COUNT
from MIP.APP.INTRA_SIGNALS
where INTERVAL_MINUTES = 15
  and SIGNAL_TS between $start_ts and $end_ts
group by 1
having count(*) > 1
order by DUP_COUNT desc
limit 50;

-- 3) State attachment coverage.
select
    count(*) as TOTAL_ROWS,
    sum(iff(STATE_BUCKET_ID is null, 1, 0)) as MISSING_BUCKET_ROWS,
    100.0 * avg(iff(STATE_BUCKET_ID is null, 1, 0)) as MISSING_BUCKET_PCT
from MIP.APP.INTRA_SIGNALS
where INTERVAL_MINUTES = 15
  and SIGNAL_TS between $start_ts and $end_ts
  and METRIC_VERSION = $metric_version
  and BUCKET_VERSION = $bucket_version
  and SOURCE_MODE = 'LEGACY_PATTERN';

-- 4) Idempotency fingerprint for rerun comparisons.
select
    count(*) as ROW_COUNT,
    count(distinct SIGNAL_NK_HASH) as DISTINCT_HASH_COUNT,
    min(SIGNAL_TS) as MIN_SIGNAL_TS,
    max(SIGNAL_TS) as MAX_SIGNAL_TS,
    sum(abs(hash(SIGNAL_NK_HASH))) as HASH_FINGERPRINT
from MIP.APP.INTRA_SIGNALS
where INTERVAL_MINUTES = 15
  and SIGNAL_TS between $start_ts and $end_ts
  and METRIC_VERSION = $metric_version
  and BUCKET_VERSION = $bucket_version
  and SOURCE_MODE = 'LEGACY_PATTERN';

-- 5) Pattern-level reconciliation.
with src as (
    select
        rl.PATTERN_ID,
        count(*) as SRC_ROWS
    from MIP.APP.RECOMMENDATION_LOG rl
    join MIP.APP.PATTERN_DEFINITION pd
      on pd.PATTERN_ID = rl.PATTERN_ID
    join MIP.APP.STATE_SNAPSHOT_15M ss
      on ss.MARKET_TYPE = rl.MARKET_TYPE
     and ss.SYMBOL = rl.SYMBOL
     and ss.INTERVAL_MINUTES = rl.INTERVAL_MINUTES
     and ss.TS = rl.TS
     and ss.METRIC_VERSION = $metric_version
     and ss.BUCKET_VERSION = $bucket_version
    where rl.INTERVAL_MINUTES = 15
      and rl.TS between $start_ts and $end_ts
      and pd.PATTERN_TYPE in ('ORB', 'PULLBACK_CONTINUATION', 'MEAN_REVERSION')
    group by 1
),
tgt as (
    select
        PATTERN_ID,
        count(*) as TGT_ROWS
    from MIP.APP.INTRA_SIGNALS
    where INTERVAL_MINUTES = 15
      and SIGNAL_TS between $start_ts and $end_ts
      and METRIC_VERSION = $metric_version
      and BUCKET_VERSION = $bucket_version
      and SOURCE_MODE = 'LEGACY_PATTERN'
    group by 1
)
select
    coalesce(s.PATTERN_ID, t.PATTERN_ID) as PATTERN_ID,
    coalesce(s.SRC_ROWS, 0) as SRC_ROWS,
    coalesce(t.TGT_ROWS, 0) as TGT_ROWS,
    coalesce(t.TGT_ROWS, 0) - coalesce(s.SRC_ROWS, 0) as DELTA_ROWS
from src s
full outer join tgt t
  on t.PATTERN_ID = s.PATTERN_ID
order by abs(coalesce(t.TGT_ROWS, 0) - coalesce(s.SRC_ROWS, 0)) desc, PATTERN_ID;
