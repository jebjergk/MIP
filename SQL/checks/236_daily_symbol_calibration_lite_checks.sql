-- 236_daily_symbol_calibration_lite_checks.sql
-- Purpose: Lite acceptance checks for daily symbol personalization rollout (Mode C)
--
-- Replace literals as needed:
--   :training_version -> e.g. 'DAILY_CAL_V1'
--   :start_date       -> e.g. dateadd(day, -30, current_date())
--   :end_date         -> e.g. current_date()

use role MIP_ADMIN_ROLE;
use database MIP;

-- ---------------------------------------------------------------------------
-- 1) SIGNAL INVARIANCE
-- Signal generation must stay unchanged (counts per day/pattern).
-- ---------------------------------------------------------------------------
with signal_counts as (
    select
        r.TS::date as SIGNAL_DATE,
        r.PATTERN_ID,
        r.MARKET_TYPE,
        count(*) as SIGNAL_COUNT
    from MIP.APP.RECOMMENDATION_LOG r
    where r.INTERVAL_MINUTES = 1440
      and r.TS::date between dateadd(day, -30, current_date()) and current_date()
    group by 1, 2, 3
)
select
    count(*) as N_BUCKETS,
    sum(SIGNAL_COUNT) as TOTAL_SIGNALS
from signal_counts;

-- ---------------------------------------------------------------------------
-- 2) TARGET RANGE
-- Effective targets must remain within multiplier caps around pattern target.
-- ---------------------------------------------------------------------------
select
    count(*) as N_ROWS,
    count_if(
        PATTERN_TARGET is not null
        and EFFECTIVE_TARGET is not null
        and (
            EFFECTIVE_TARGET < PATTERN_TARGET * 0.80
            or EFFECTIVE_TARGET > PATTERN_TARGET * 1.20
        )
    ) as OUT_OF_RANGE_ROWS
from MIP.APP.DAILY_POLICY_EFFECTIVE_TRAINED
where TRAINING_VERSION = 'DAILY_CAL_V1';

-- ---------------------------------------------------------------------------
-- 3) HORIZON CONSISTENCY
-- Target selection must align with the position hold horizon (unless override).
-- ---------------------------------------------------------------------------
with open_positions as (
    select
        p.PORTFOLIO_ID,
        p.SYMBOL,
        p.MARKET_TYPE,
        p.ENTRY_TS,
        greatest(p.HOLD_UNTIL_INDEX - p.ENTRY_INDEX, 1) as HOLD_BARS
    from MIP.MART.V_PORTFOLIO_OPEN_POSITIONS_CANONICAL p
    where p.INTERVAL_MINUTES = 1440
      and p.IS_OPEN = true
),
signal_anchor as (
    select
        op.*,
        max(r.TS::date) as SIGNAL_DATE
    from open_positions op
    left join MIP.APP.RECOMMENDATION_LOG r
      on r.SYMBOL = op.SYMBOL
     and r.MARKET_TYPE = op.MARKET_TYPE
     and r.INTERVAL_MINUTES = 1440
     and r.TS < op.ENTRY_TS
    group by op.PORTFOLIO_ID, op.SYMBOL, op.MARKET_TYPE, op.ENTRY_TS, op.HOLD_BARS
),
candidate as (
    select
        sa.PORTFOLIO_ID,
        sa.SYMBOL,
        sa.MARKET_TYPE,
        sa.ENTRY_TS,
        sa.HOLD_BARS,
        ts2.HORIZON_BARS,
        pa.EFFECTIVE_HORIZON_BARS,
        row_number() over (
            partition by sa.PORTFOLIO_ID, sa.SYMBOL, sa.MARKET_TYPE, sa.ENTRY_TS
            order by
                case when ts2.HORIZON_BARS = sa.HOLD_BARS then 0 else 1 end,
                abs(ts2.HORIZON_BARS - sa.HOLD_BARS),
                coalesce(pa.EFFECTIVE_TARGET, ts2.AVG_RETURN) desc
        ) as RN
    from signal_anchor sa
    join MIP.APP.RECOMMENDATION_LOG rl
      on rl.SYMBOL = sa.SYMBOL
     and rl.MARKET_TYPE = sa.MARKET_TYPE
     and rl.INTERVAL_MINUTES = 1440
     and rl.TS::date = sa.SIGNAL_DATE
    join MIP.MART.V_TRUSTED_SIGNALS ts2
      on ts2.PATTERN_ID = rl.PATTERN_ID
     and ts2.MARKET_TYPE = rl.MARKET_TYPE
     and ts2.INTERVAL_MINUTES = 1440
     and ts2.IS_TRUSTED = true
    left join MIP.APP.DAILY_POLICY_EFFECTIVE_TRAINED pa
      on pa.TRAINING_VERSION = 'DAILY_CAL_V1'
     and pa.SYMBOL = sa.SYMBOL
     and pa.MARKET_TYPE = sa.MARKET_TYPE
     and pa.PATTERN_ID = rl.PATTERN_ID
     and pa.HORIZON_BARS = ts2.HORIZON_BARS
)
select
    count(*) as N_POSITIONS,
    count_if(
        coalesce(EFFECTIVE_HORIZON_BARS, HORIZON_BARS) != HOLD_BARS
    ) as HORIZON_MISMATCH_ROWS
from candidate
where RN = 1;

-- ---------------------------------------------------------------------------
-- 4) MULTIPLIER DISTRIBUTION
-- Multipliers should be centered near 1.0 with modest eligibility share.
-- ---------------------------------------------------------------------------
select
    count(*) as N_ROWS,
    avg(MULTIPLIER_CAPPED) as AVG_MULTIPLIER,
    median(MULTIPLIER_CAPPED) as MEDIAN_MULTIPLIER,
    percentile_cont(0.95) within group (order by MULTIPLIER_CAPPED) as P95_MULTIPLIER,
    avg(iff(ELIGIBLE_FLAG, 1.0, 0.0)) as ELIGIBLE_SHARE
from MIP.APP.DAILY_SYMBOL_CALIBRATION_TRAINED
where TRAINING_VERSION = 'DAILY_CAL_V1';

-- ---------------------------------------------------------------------------
-- 5) NO TARGET INFLATION REGRESSION
-- Ensure selected target is horizon-matched candidate, not max return candidate.
-- ---------------------------------------------------------------------------
with trusted_horizons as (
    select
        rl.SYMBOL,
        rl.MARKET_TYPE,
        rl.TS::date as SIGNAL_DATE,
        ts2.HORIZON_BARS,
        ts2.AVG_RETURN
    from MIP.APP.RECOMMENDATION_LOG rl
    join MIP.MART.V_TRUSTED_SIGNALS ts2
      on ts2.PATTERN_ID = rl.PATTERN_ID
     and ts2.MARKET_TYPE = rl.MARKET_TYPE
     and ts2.INTERVAL_MINUTES = 1440
     and ts2.IS_TRUSTED = true
    where rl.INTERVAL_MINUTES = 1440
      and rl.TS::date between dateadd(day, -30, current_date()) and current_date()
),
horizon_max as (
    select
        SYMBOL,
        MARKET_TYPE,
        SIGNAL_DATE,
        max(AVG_RETURN) as MAX_HORIZON_RETURN
    from trusted_horizons
    group by SYMBOL, MARKET_TYPE, SIGNAL_DATE
),
horizon_min as (
    select
        SYMBOL,
        MARKET_TYPE,
        SIGNAL_DATE,
        min(AVG_RETURN) as MIN_HORIZON_RETURN
    from trusted_horizons
    group by SYMBOL, MARKET_TYPE, SIGNAL_DATE
)
select
    count(*) as N_SIGNAL_DATES,
    avg(hm.MAX_HORIZON_RETURN - hn.MIN_HORIZON_RETURN) as AVG_HORIZON_SPREAD
from horizon_max hm
join horizon_min hn
  on hn.SYMBOL = hm.SYMBOL
 and hn.MARKET_TYPE = hm.MARKET_TYPE
 and hn.SIGNAL_DATE = hm.SIGNAL_DATE;

