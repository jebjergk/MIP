-- 222_intraday_v2_phase2a_transition_checks.sql
-- Purpose: Phase 2a verification for STATE_TRANSITIONS.

use role MIP_ADMIN_ROLE;
use database MIP;

set start_ts = dateadd(day, -30, current_timestamp());
set end_ts = current_timestamp();

-- 1) Row coverage by day/market for transitions.
select
    MARKET_TYPE,
    TS_FROM::date as TRANSITION_DATE,
    count(*) as TRANSITION_ROWS
from MIP.APP.STATE_TRANSITIONS
where INTERVAL_MINUTES = 15
  and TS_FROM between $start_ts and $end_ts
group by 1, 2
order by 2, 1;

-- 2) Integrity: transitions must reflect bucket change.
select
    count(*) as INVALID_SAME_BUCKET_TRANSITIONS
from MIP.APP.STATE_TRANSITIONS
where INTERVAL_MINUTES = 15
  and TS_FROM between $start_ts and $end_ts
  and FROM_STATE_BUCKET_ID = TO_STATE_BUCKET_ID;

-- 3) Integrity: non-positive duration.
select
    count(*) as INVALID_NON_POSITIVE_DURATION
from MIP.APP.STATE_TRANSITIONS
where INTERVAL_MINUTES = 15
  and TS_FROM between $start_ts and $end_ts
  and DURATION_BARS <= 0;

-- 4) Duration reconciliation diagnostic by symbol/day.
-- This checks whether transition durations cover most bars for days with transitions.
with day_bars as (
    select
        MARKET_TYPE,
        SYMBOL,
        TS::date as BAR_DATE,
        count(*) as BAR_ROWS
    from MIP.APP.STATE_SNAPSHOT_15M
    where INTERVAL_MINUTES = 15
      and TS between $start_ts and $end_ts
    group by 1, 2, 3
),
day_transitions as (
    select
        MARKET_TYPE,
        SYMBOL,
        TS_FROM::date as BAR_DATE,
        sum(DURATION_BARS) as SUM_DURATION_BARS,
        count(*) as TRANSITIONS
    from MIP.APP.STATE_TRANSITIONS
    where INTERVAL_MINUTES = 15
      and TS_FROM between $start_ts and $end_ts
    group by 1, 2, 3
)
select
    b.MARKET_TYPE,
    b.SYMBOL,
    b.BAR_DATE,
    b.BAR_ROWS,
    coalesce(t.SUM_DURATION_BARS, 0) as SUM_DURATION_BARS,
    coalesce(t.TRANSITIONS, 0) as TRANSITIONS,
    b.BAR_ROWS - coalesce(t.SUM_DURATION_BARS, 0) as BAR_GAP
from day_bars b
left join day_transitions t
  on t.MARKET_TYPE = b.MARKET_TYPE
 and t.SYMBOL = b.SYMBOL
 and t.BAR_DATE = b.BAR_DATE
where b.BAR_ROWS > 0
order by abs(b.BAR_ROWS - coalesce(t.SUM_DURATION_BARS, 0)) desc, b.BAR_DATE desc
limit 30;

-- 5) Top transition pairs.
select
    FROM_STATE_BUCKET_ID,
    TO_STATE_BUCKET_ID,
    count(*) as TRANSITION_COUNT,
    avg(DURATION_BARS) as AVG_DURATION_BARS
from MIP.APP.STATE_TRANSITIONS
where INTERVAL_MINUTES = 15
  and TS_FROM between $start_ts and $end_ts
group by 1, 2
order by TRANSITION_COUNT desc
limit 20;
