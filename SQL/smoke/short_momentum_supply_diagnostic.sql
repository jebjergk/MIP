-- SHORT momentum supply / constraint diagnostic (STOCK, 1440)
-- Date window: 2025-08-01 .. 2026-04-06 (inclusive signal TS::date)
-- Read-only analytics; mirrors SP_GENERATE_MOMENTUM_RECS SHORT STOCK logic (070).

use role MIP_ADMIN_ROLE;
use database MIP;

-- ---------------------------------------------------------------------------
-- 1) Monthly signal supply by pattern
-- ---------------------------------------------------------------------------
with logf as (
    select
        r.PATTERN_ID,
        date_trunc('month', r.TS::date) as MO,
        r.SYMBOL,
        r.TS::date as D
    from MIP.APP.RECOMMENDATION_LOG r
    where r.PATTERN_ID in (1101, 1102)
      and coalesce(r.SIGNAL_DIRECTION, '') = 'SHORT'
      and upper(r.MARKET_TYPE) = 'STOCK'
      and r.INTERVAL_MINUTES = 1440
      and r.TS::date between '2025-08-01' and '2026-04-06'
)
select
    PATTERN_ID,
    to_char(MO, 'YYYY-MM') as MONTH,
    count(*) as TOTAL_SHORT_SIGNALS,
    count(distinct SYMBOL) as DISTINCT_SYMBOLS,
    count(distinct D) as DISTINCT_SIGNAL_DAYS,
    div0(TOTAL_SHORT_SIGNALS, nullif(DISTINCT_SIGNAL_DAYS, 0)) as AVG_SIGNALS_PER_ACTIVE_SIGNAL_DAY
from logf
group by PATTERN_ID, MO
order by PATTERN_ID, MO;

-- ---------------------------------------------------------------------------
-- 2) Pattern quality summary (by horizon) + last signal
-- ---------------------------------------------------------------------------
with base as (
    select
        r.PATTERN_ID,
        r.RECOMMENDATION_ID,
        r.TS,
        o.HORIZON_BARS,
        o.EVAL_STATUS,
        o.HIT_FLAG,
        o.REALIZED_RETURN
    from MIP.APP.RECOMMENDATION_LOG r
    join MIP.APP.RECOMMENDATION_OUTCOMES o
      on o.RECOMMENDATION_ID = r.RECOMMENDATION_ID
    where r.PATTERN_ID in (1101, 1102)
      and coalesce(r.SIGNAL_DIRECTION, '') = 'SHORT'
      and upper(r.MARKET_TYPE) = 'STOCK'
      and r.INTERVAL_MINUTES = 1440
      and r.TS::date between '2025-08-01' and '2026-04-06'
)
select
    PATTERN_ID,
    HORIZON_BARS,
    count(*) as N_OUTCOME_ROWS,
    count_if(EVAL_STATUS = 'SUCCESS') as N_SUCCESS_EVAL,
    count_if(EVAL_STATUS = 'SUCCESS' and HIT_FLAG is not null) as N_LABELED,
    div0(count_if(EVAL_STATUS = 'SUCCESS' and HIT_FLAG), nullif(count_if(EVAL_STATUS = 'SUCCESS' and HIT_FLAG is not null), 0)) as HIT_RATE_AMONG_LABELED,
    avg(iff(EVAL_STATUS = 'SUCCESS', REALIZED_RETURN, null)) as AVG_REALIZED_RETURN,
    median(iff(EVAL_STATUS = 'SUCCESS', REALIZED_RETURN, null)) as MEDIAN_REALIZED_RETURN
from base
group by PATTERN_ID, HORIZON_BARS
order by PATTERN_ID, HORIZON_BARS;

select
    PATTERN_ID,
    count(distinct RECOMMENDATION_ID) as N_DISTINCT_SIGNALS,
    count(distinct SYMBOL) as DISTINCT_SYMBOLS_IN_LOG
from (
    select r.PATTERN_ID, r.RECOMMENDATION_ID, r.SYMBOL
    from MIP.APP.RECOMMENDATION_LOG r
    where r.PATTERN_ID in (1101, 1102)
      and coalesce(r.SIGNAL_DIRECTION, '') = 'SHORT'
      and upper(r.MARKET_TYPE) = 'STOCK'
      and r.INTERVAL_MINUTES = 1440
      and r.TS::date between '2025-08-01' and '2026-04-06'
) x
group by PATTERN_ID
order by PATTERN_ID;

select PATTERN_ID, max(TS) as LAST_SIGNAL_TS
from MIP.APP.RECOMMENDATION_LOG r
where r.PATTERN_ID in (1101, 1102)
  and coalesce(r.SIGNAL_DIRECTION, '') = 'SHORT'
  and upper(r.MARKET_TYPE) = 'STOCK'
  and r.INTERVAL_MINUTES = 1440
  and r.TS::date between '2025-08-01' and '2026-04-06'
group by PATTERN_ID
order by PATTERN_ID;

-- ---------------------------------------------------------------------------
-- 4) Symbol concentration (top 15 + top5/top10 share)
-- ---------------------------------------------------------------------------
with c as (
    select PATTERN_ID, SYMBOL, count(*) as N
    from MIP.APP.RECOMMENDATION_LOG r
    where r.PATTERN_ID in (1101, 1102)
      and coalesce(r.SIGNAL_DIRECTION, '') = 'SHORT'
      and upper(r.MARKET_TYPE) = 'STOCK'
      and r.INTERVAL_MINUTES = 1440
      and r.TS::date between '2025-08-01' and '2026-04-06'
    group by PATTERN_ID, SYMBOL
),
rk as (
    select
        *,
        row_number() over (partition by PATTERN_ID order by N desc, SYMBOL) as RN,
        sum(N) over (partition by PATTERN_ID) as TOT
    from c
)
select PATTERN_ID, SYMBOL, N, RN
from rk
where RN <= 15
order by PATTERN_ID, RN;

with c as (
    select PATTERN_ID, SYMBOL, count(*) as N
    from MIP.APP.RECOMMENDATION_LOG r
    where r.PATTERN_ID in (1101, 1102)
      and coalesce(r.SIGNAL_DIRECTION, '') = 'SHORT'
      and upper(r.MARKET_TYPE) = 'STOCK'
      and r.INTERVAL_MINUTES = 1440
      and r.TS::date between '2025-08-01' and '2026-04-06'
    group by PATTERN_ID, SYMBOL
),
rk as (
    select
        PATTERN_ID,
        N,
        row_number() over (partition by PATTERN_ID order by N desc, SYMBOL) as RN,
        sum(N) over (partition by PATTERN_ID) as TOT
    from c
),
agg as (
    select
        PATTERN_ID,
        max(TOT) as TOT,
        sum(iff(RN <= 5, N, 0)) as TOP5,
        sum(iff(RN <= 10, N, 0)) as TOP10
    from rk
    group by PATTERN_ID
)
select
    PATTERN_ID,
    TOT,
    TOP5,
    TOP10,
    div0(TOP5, TOT) as SHARE_TOP5,
    div0(TOP10, TOT) as SHARE_TOP10
from agg
order by PATTERN_ID;

-- ---------------------------------------------------------------------------
-- 5) Regime clustering (monthly totals per pattern)
-- ---------------------------------------------------------------------------
with logf as (
    select
        r.PATTERN_ID,
        date_trunc('month', r.TS::date) as MO
    from MIP.APP.RECOMMENDATION_LOG r
    where r.PATTERN_ID in (1101, 1102)
      and coalesce(r.SIGNAL_DIRECTION, '') = 'SHORT'
      and upper(r.MARKET_TYPE) = 'STOCK'
      and r.INTERVAL_MINUTES = 1440
      and r.TS::date between '2025-08-01' and '2026-04-06'
),
m as (
    select PATTERN_ID, MO, count(*) as CNT
    from logf
    group by PATTERN_ID, MO
),
s as (
    select
        PATTERN_ID,
        sum(CNT) as TOTAL,
        count(*) as N_MONTHS_NONZERO,
        max(CNT) as MAX_MONTH_CNT,
        stddev_pop(CNT) as STDDEV_MONTH,
        avg(CNT) as AVG_MONTH_CNT,
        div0(MAX_MONTH_CNT, nullif(TOTAL, 0)) as MAX_MONTH_SHARE_OF_TOTAL
    from m
    group by PATTERN_ID
)
select
    PATTERN_ID,
    TOTAL,
    N_MONTHS_NONZERO,
    MAX_MONTH_CNT,
    MAX_MONTH_SHARE_OF_TOTAL,
    STDDEV_MONTH,
    AVG_MONTH_CNT,
    div0(STDDEV_MONTH, nullif(AVG_MONTH_CNT, 0)) as CV_MONTH_CNT
from s
order by PATTERN_ID;

-- ---------------------------------------------------------------------------
-- 3) Detector funnel (STOCK daily) — mirrors 070 SHORT STOCK branch; window frames match slow/fast.
--    s1 = universe + non-null return + min volume (same as returns_filtered)
-- ---------------------------------------------------------------------------

-- Pattern 1101: fast=20, slow=3, hist=max(lookback,30)=30, min_return=0.002, min_zscore=1 (P_MIN_ZSCORE null → z waived if stddev null/zero)
with cfg as (
    select coalesce(
        (select try_to_number(CONFIG_VALUE) from MIP.APP.APP_CONFIG where CONFIG_KEY = 'MIN_VOLUME' limit 1),
        1000
    ) as min_vol
),
b as (
    select '2025-08-01'::date as d0, '2026-04-06'::date as d1, 30 as hist_days
),
rf as (
    select
        r.SYMBOL,
        r.TS,
        r.TS::date as bar_d,
        r.RETURN_SIMPLE,
        r.CLOSE,
        row_number() over (partition by r.SYMBOL order by r.TS) as rn
    from MIP.MART.MARKET_RETURNS r
    cross join b
    where r.MARKET_TYPE = 'STOCK'
      and r.INTERVAL_MINUTES = 1440
      and r.RETURN_SIMPLE is not null
      and r.VOLUME >= (select min_vol from cfg)
      and r.TS::date >= dateadd(day, -b.hist_days, b.d0)
      and r.TS::date <= b.d1
      and exists (
          select 1
          from MIP.APP.INGEST_UNIVERSE iu
          where upper(replace(iu.SYMBOL, chr(47), '')) = upper(replace(r.SYMBOL, chr(47), ''))
            and upper(iu.MARKET_TYPE) = 'STOCK'
            and iu.INTERVAL_MINUTES = 1440
            and coalesce(iu.IS_ENABLED, true)
      )
),
w as (
    select
        rf.*,
        sum(iff(RETURN_SIMPLE < 0, 1, 0)) over (
            partition by SYMBOL order by TS rows between 3 preceding and 1 preceding
        ) as neg_lag_ct,
        min(CLOSE) over (
            partition by SYMBOL order by TS rows between 20 preceding and 1 preceding
        ) as min_lag_close,
        stddev_samp(RETURN_SIMPLE) over (
            partition by SYMBOL order by TS rows between 19 preceding and current row
        ) as stddev_w
    from rf
),
eval as (
    select * from w
    where bar_d between '2025-08-01' and '2026-04-06'
)
select
    1101 as PATTERN_ID,
    'STOCK_MOMENTUM_FAST_SHORT' as PATTERN_NAME,
    count(*) as s1_univ_tradable_symbol_days,
    count_if(RETURN_SIMPLE < 0) as s2_negative_daily_return,
    count_if(RETURN_SIMPLE <= -0.002) as s3_min_return_threshold,
    count_if(RETURN_SIMPLE <= -0.002 and neg_lag_ct >= 3) as s4_down_day_streak,
    count_if(
        RETURN_SIMPLE <= -0.002 and neg_lag_ct >= 3
        and (min_lag_close is null or CLOSE <= min_lag_close)
    ) as s5_close_vs_prior_window_low,
    count_if(
        RETURN_SIMPLE <= -0.002 and neg_lag_ct >= 3
        and (min_lag_close is null or CLOSE <= min_lag_close)
        and (
            stddev_w is null
            or stddev_w <= 0
            or (stddev_w > 0 and RETURN_SIMPLE / stddev_w <= -1.0)
        )
    ) as s6_zscore_pass,
    (
        select count(*)
        from MIP.APP.RECOMMENDATION_LOG l
        where l.PATTERN_ID = 1101
          and coalesce(l.SIGNAL_DIRECTION, '') = 'SHORT'
          and upper(l.MARKET_TYPE) = 'STOCK'
          and l.INTERVAL_MINUTES = 1440
          and l.TS::date between '2025-08-01' and '2026-04-06'
    ) as s7_inserted_short_signals
from eval;

-- Pattern 1102: fast=30, slow=2, hist=60, min_return=0.001, min_zscore=0.75
with cfg as (
    select coalesce(
        (select try_to_number(CONFIG_VALUE) from MIP.APP.APP_CONFIG where CONFIG_KEY = 'MIN_VOLUME' limit 1),
        1000
    ) as min_vol
),
b as (
    select '2025-08-01'::date as d0, '2026-04-06'::date as d1, 60 as hist_days
),
rf as (
    select
        r.SYMBOL,
        r.TS,
        r.TS::date as bar_d,
        r.RETURN_SIMPLE,
        r.CLOSE,
        row_number() over (partition by r.SYMBOL order by r.TS) as rn
    from MIP.MART.MARKET_RETURNS r
    cross join b
    where r.MARKET_TYPE = 'STOCK'
      and r.INTERVAL_MINUTES = 1440
      and r.RETURN_SIMPLE is not null
      and r.VOLUME >= (select min_vol from cfg)
      and r.TS::date >= dateadd(day, -b.hist_days, b.d0)
      and r.TS::date <= b.d1
      and exists (
          select 1
          from MIP.APP.INGEST_UNIVERSE iu
          where upper(replace(iu.SYMBOL, chr(47), '')) = upper(replace(r.SYMBOL, chr(47), ''))
            and upper(iu.MARKET_TYPE) = 'STOCK'
            and iu.INTERVAL_MINUTES = 1440
            and coalesce(iu.IS_ENABLED, true)
      )
),
w as (
    select
        rf.*,
        sum(iff(RETURN_SIMPLE < 0, 1, 0)) over (
            partition by SYMBOL order by TS rows between 2 preceding and 1 preceding
        ) as neg_lag_ct,
        min(CLOSE) over (
            partition by SYMBOL order by TS rows between 30 preceding and 1 preceding
        ) as min_lag_close,
        stddev_samp(RETURN_SIMPLE) over (
            partition by SYMBOL order by TS rows between 29 preceding and current row
        ) as stddev_w
    from rf
),
eval as (
    select * from w
    where bar_d between '2025-08-01' and '2026-04-06'
)
select
    1102 as PATTERN_ID,
    'STOCK_MOMENTUM_SLOW_SHORT' as PATTERN_NAME,
    count(*) as s1_univ_tradable_symbol_days,
    count_if(RETURN_SIMPLE < 0) as s2_negative_daily_return,
    count_if(RETURN_SIMPLE <= -0.001) as s3_min_return_threshold,
    count_if(RETURN_SIMPLE <= -0.001 and neg_lag_ct >= 2) as s4_down_day_streak,
    count_if(
        RETURN_SIMPLE <= -0.001 and neg_lag_ct >= 2
        and (min_lag_close is null or CLOSE <= min_lag_close)
    ) as s5_close_vs_prior_window_low,
    count_if(
        RETURN_SIMPLE <= -0.001 and neg_lag_ct >= 2
        and (min_lag_close is null or CLOSE <= min_lag_close)
        and (
            stddev_w is null
            or stddev_w <= 0
            or (stddev_w > 0 and RETURN_SIMPLE / stddev_w <= -0.75)
        )
    ) as s6_zscore_pass,
    (
        select count(*)
        from MIP.APP.RECOMMENDATION_LOG l
        where l.PATTERN_ID = 1102
          and coalesce(l.SIGNAL_DIRECTION, '') = 'SHORT'
          and upper(l.MARKET_TYPE) = 'STOCK'
          and l.INTERVAL_MINUTES = 1440
          and l.TS::date between '2025-08-01' and '2026-04-06'
    ) as s7_inserted_short_signals
from eval;
