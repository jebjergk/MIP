-- 226_intraday_v2_phase6_terrain_checks.sql
-- Purpose: Phase 6 verification checks for OPPORTUNITY_TERRAIN_15M.

use role MIP_ADMIN_ROLE;
use database MIP;

set start_ts = dateadd(day, -30, current_timestamp());
set end_ts = (
    select max(SIGNAL_TS)
    from MIP.APP.INTRA_SIGNALS
    where INTERVAL_MINUTES = 15
      and SOURCE_MODE = 'LEGACY_PATTERN'
      and METRIC_VERSION = 'v1_1'
      and BUCKET_VERSION = 'v1'
);

-- 1) Cardinality guardrail: terrain should align with signal/transition candidates, not all bars.
with signal_ts as (
    select distinct MARKET_TYPE, SYMBOL, INTERVAL_MINUTES, SIGNAL_TS as TS
    from MIP.APP.INTRA_SIGNALS
    where INTERVAL_MINUTES = 15
      and SIGNAL_TS between $start_ts and $end_ts
      and METRIC_VERSION = 'v1_1'
      and BUCKET_VERSION = 'v1'
),
transition_ts as (
    select distinct MARKET_TYPE, SYMBOL, INTERVAL_MINUTES, TS_TO as TS
    from MIP.APP.STATE_TRANSITIONS
    where INTERVAL_MINUTES = 15
      and TS_TO between $start_ts and $end_ts
      and METRIC_VERSION = 'v1_1'
      and BUCKET_VERSION = 'v1'
),
candidate_ts as (
    select * from signal_ts
    union
    select * from transition_ts
),
all_bar_ts as (
    select count(*) as ALL_BAR_ROWS
    from MIP.APP.STATE_SNAPSHOT_15M
    where INTERVAL_MINUTES = 15
      and TS between $start_ts and $end_ts
      and METRIC_VERSION = 'v1_1'
      and BUCKET_VERSION = 'v1'
),
terrain_rows as (
    select count(*) as TERRAIN_ROWS
    from MIP.APP.OPPORTUNITY_TERRAIN_15M
    where INTERVAL_MINUTES = 15
      and TS between $start_ts and $end_ts
      and METRIC_VERSION = 'v1_1'
      and BUCKET_VERSION = 'v1'
      and TERRAIN_VERSION = 'v1'
),
candidate_rows as (
    select count(*) as CANDIDATE_TS_ROWS
    from candidate_ts
)
select
    tr.TERRAIN_ROWS,
    cr.CANDIDATE_TS_ROWS,
    ab.ALL_BAR_ROWS
from terrain_rows tr
cross join candidate_rows cr
cross join all_bar_ts ab;

-- 2) Score sanity.
select
    count(*) as ROW_COUNT,
    count(distinct TERRAIN_SCORE) as DISTINCT_TERRAIN_SCORES,
    min(TERRAIN_SCORE) as MIN_TERRAIN_SCORE,
    max(TERRAIN_SCORE) as MAX_TERRAIN_SCORE,
    avg(TERRAIN_SCORE) as AVG_TERRAIN_SCORE,
    stddev_samp(TERRAIN_SCORE) as STDDEV_TERRAIN_SCORE
from MIP.APP.OPPORTUNITY_TERRAIN_15M
where INTERVAL_MINUTES = 15
  and TS between $start_ts and $end_ts
  and METRIC_VERSION = 'v1_1'
  and BUCKET_VERSION = 'v1'
  and TERRAIN_VERSION = 'v1';

-- 3) Suitability shrinkage behavior.
with small_n as (
    select avg(abs(SUITABILITY)) as AVG_ABS_SUITABILITY_SMALL_N
    from MIP.APP.OPPORTUNITY_TERRAIN_15M
    where INTERVAL_MINUTES = 15
      and TS between $start_ts and $end_ts
      and METRIC_VERSION = 'v1_1'
      and BUCKET_VERSION = 'v1'
      and TERRAIN_VERSION = 'v1'
      and N_SIGNALS < 20
),
large_n as (
    select avg(abs(SUITABILITY)) as AVG_ABS_SUITABILITY_LARGE_N
    from MIP.APP.OPPORTUNITY_TERRAIN_15M
    where INTERVAL_MINUTES = 15
      and TS between $start_ts and $end_ts
      and METRIC_VERSION = 'v1_1'
      and BUCKET_VERSION = 'v1'
      and TERRAIN_VERSION = 'v1'
      and N_SIGNALS >= 20
)
select
    s.AVG_ABS_SUITABILITY_SMALL_N,
    l.AVG_ABS_SUITABILITY_LARGE_N
from small_n s
cross join large_n l;

-- 4) Candidate-source split.
select
    CANDIDATE_SOURCE,
    count(*) as ROW_COUNT
from MIP.APP.OPPORTUNITY_TERRAIN_15M
where INTERVAL_MINUTES = 15
  and TS between $start_ts and $end_ts
  and METRIC_VERSION = 'v1_1'
  and BUCKET_VERSION = 'v1'
  and TERRAIN_VERSION = 'v1'
group by 1
order by 2 desc;

-- 5) Reproducibility fingerprint.
select
    count(*) as ROW_COUNT,
    sum(abs(hash(concat(
        coalesce(to_varchar(PATTERN_ID), ''), '|',
        coalesce(MARKET_TYPE, ''), '|',
        coalesce(SYMBOL, ''), '|',
        coalesce(to_varchar(INTERVAL_MINUTES), ''), '|',
        coalesce(to_varchar(TS, 'YYYY-MM-DD HH24:MI:SS.FF3'), ''), '|',
        coalesce(to_varchar(HORIZON_BARS), ''), '|',
        coalesce(STATE_BUCKET_ID, ''), '|',
        coalesce(to_varchar(TERRAIN_SCORE), 'NULL')
    )))) as TERRAIN_FINGERPRINT
from MIP.APP.OPPORTUNITY_TERRAIN_15M
where INTERVAL_MINUTES = 15
  and TS between $start_ts and $end_ts
  and METRIC_VERSION = 'v1_1'
  and BUCKET_VERSION = 'v1'
  and TERRAIN_VERSION = 'v1';
