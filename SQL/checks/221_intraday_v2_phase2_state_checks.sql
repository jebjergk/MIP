-- 221_intraday_v2_phase2_state_checks.sql
-- Purpose: Phase 2 verification checks for STATE_SNAPSHOT_15M.
-- Uses last 30 days by default.

use role MIP_ADMIN_ROLE;
use database MIP;

-- Set reporting window.
set start_ts = dateadd(day, -30, current_timestamp());
set end_ts = current_timestamp();

-- 1) Coverage check: bars vs snapshots by day/market.
with bars as (
    select MARKET_TYPE, TS::date as BAR_DATE, count(*) as BAR_ROWS
    from MIP.MART.MARKET_BARS
    where INTERVAL_MINUTES = 15
      and TS between $start_ts and $end_ts
    group by 1, 2
),
snaps as (
    select MARKET_TYPE, TS::date as BAR_DATE, count(*) as SNAP_ROWS
    from MIP.APP.STATE_SNAPSHOT_15M
    where INTERVAL_MINUTES = 15
      and TS between $start_ts and $end_ts
    group by 1, 2
)
select
    coalesce(b.MARKET_TYPE, s.MARKET_TYPE) as MARKET_TYPE,
    coalesce(b.BAR_DATE, s.BAR_DATE) as BAR_DATE,
    coalesce(b.BAR_ROWS, 0) as BAR_ROWS,
    coalesce(s.SNAP_ROWS, 0) as SNAP_ROWS,
    coalesce(s.SNAP_ROWS, 0) - coalesce(b.BAR_ROWS, 0) as DELTA_ROWS
from bars b
full outer join snaps s
    on b.MARKET_TYPE = s.MARKET_TYPE
   and b.BAR_DATE = s.BAR_DATE
order by 2, 1;

-- 2) Null-rate check on core metrics.
select
    count(*) as TOTAL_ROWS,
    100.0 * avg(iff(BELIEF_DIRECTION is null, 1, 0)) as NULL_PCT_BELIEF_DIRECTION,
    100.0 * avg(iff(BELIEF_STRENGTH is null, 1, 0)) as NULL_PCT_BELIEF_STRENGTH,
    100.0 * avg(iff(BELIEF_STABILITY is null, 1, 0)) as NULL_PCT_BELIEF_STABILITY,
    100.0 * avg(iff(REACTION_SPEED is null, 1, 0)) as NULL_PCT_REACTION_SPEED,
    100.0 * avg(iff(DRIFT_VS_IMPULSE is null, 1, 0)) as NULL_PCT_DRIFT_VS_IMPULSE,
    100.0 * avg(iff(RECOVERY_TIME is null, 1, 0)) as NULL_PCT_RECOVERY_TIME,
    100.0 * avg(iff(MTF_ALIGNMENT is null, 1, 0)) as NULL_PCT_MTF_ALIGNMENT,
    100.0 * avg(iff(CHOP_INDEX is null, 1, 0)) as NULL_PCT_CHOP_INDEX,
    100.0 * avg(iff(VOL_DIRECTION_ALIGNMENT is null, 1, 0)) as NULL_PCT_VOL_DIRECTION_ALIGNMENT,
    100.0 * avg(iff(STATE_BUCKET_ID is null, 1, 0)) as NULL_PCT_STATE_BUCKET
from MIP.APP.STATE_SNAPSHOT_15M
where INTERVAL_MINUTES = 15
  and TS between $start_ts and $end_ts;

-- 3) Value sanity/range check.
select
    min(BELIEF_DIRECTION) as MIN_BELIEF_DIRECTION,
    max(BELIEF_DIRECTION) as MAX_BELIEF_DIRECTION,
    min(BELIEF_STRENGTH) as MIN_BELIEF_STRENGTH,
    max(BELIEF_STRENGTH) as MAX_BELIEF_STRENGTH,
    min(BELIEF_STABILITY) as MIN_BELIEF_STABILITY,
    max(BELIEF_STABILITY) as MAX_BELIEF_STABILITY,
    min(REACTION_SPEED) as MIN_REACTION_SPEED,
    max(REACTION_SPEED) as MAX_REACTION_SPEED,
    min(DRIFT_VS_IMPULSE) as MIN_DRIFT_VS_IMPULSE,
    max(DRIFT_VS_IMPULSE) as MAX_DRIFT_VS_IMPULSE,
    min(RECOVERY_TIME) as MIN_RECOVERY_TIME,
    max(RECOVERY_TIME) as MAX_RECOVERY_TIME,
    min(MTF_ALIGNMENT) as MIN_MTF_ALIGNMENT,
    max(MTF_ALIGNMENT) as MAX_MTF_ALIGNMENT,
    min(CHOP_INDEX) as MIN_CHOP_INDEX,
    max(CHOP_INDEX) as MAX_CHOP_INDEX,
    min(VOL_DIRECTION_ALIGNMENT) as MIN_VOL_DIRECTION_ALIGNMENT,
    max(VOL_DIRECTION_ALIGNMENT) as MAX_VOL_DIRECTION_ALIGNMENT
from MIP.APP.STATE_SNAPSHOT_15M
where INTERVAL_MINUTES = 15
  and TS between $start_ts and $end_ts;

-- 4) Bucket cardinality and distribution.
with dist as (
    select
        STATE_BUCKET_ID,
        count(*) as ROWS_IN_BUCKET
    from MIP.APP.STATE_SNAPSHOT_15M
    where INTERVAL_MINUTES = 15
      and TS between $start_ts and $end_ts
    group by 1
),
tot as (
    select sum(ROWS_IN_BUCKET) as TOTAL_ROWS
    from dist
)
select
    d.STATE_BUCKET_ID,
    d.ROWS_IN_BUCKET,
    round(100.0 * d.ROWS_IN_BUCKET / nullif(t.TOTAL_ROWS, 0), 4) as PCT_ROWS
from dist d
cross join tot t
order by d.ROWS_IN_BUCKET desc, d.STATE_BUCKET_ID;

-- 4a) Cardinality summary.
select
    count(distinct STATE_BUCKET_ID) as BUCKET_CARDINALITY
from MIP.APP.STATE_SNAPSHOT_15M
where INTERVAL_MINUTES = 15
  and TS between $start_ts and $end_ts;

-- 5) Stability sanity: average daily state transitions by symbol (lower is more stable).
with seq as (
    select
        MARKET_TYPE,
        SYMBOL,
        TS::date as BAR_DATE,
        STATE_BUCKET_ID,
        lag(STATE_BUCKET_ID) over (
            partition by MARKET_TYPE, SYMBOL, TS::date
            order by TS
        ) as PREV_BUCKET
    from MIP.APP.STATE_SNAPSHOT_15M
    where INTERVAL_MINUTES = 15
      and TS between $start_ts and $end_ts
),
daily_switches as (
    select
        MARKET_TYPE,
        SYMBOL,
        BAR_DATE,
        sum(iff(PREV_BUCKET is not null and PREV_BUCKET <> STATE_BUCKET_ID, 1, 0)) as SWITCHES
    from seq
    group by 1, 2, 3
),
symbol_avg as (
    select
        MARKET_TYPE,
        SYMBOL,
        avg(SWITCHES) as AVG_SWITCHES_PER_DAY
    from daily_switches
    group by 1, 2
)
select
    MARKET_TYPE,
    SYMBOL,
    round(AVG_SWITCHES_PER_DAY, 4) as AVG_SWITCHES_PER_DAY
from symbol_avg
order by AVG_SWITCHES_PER_DAY desc
limit 20;
