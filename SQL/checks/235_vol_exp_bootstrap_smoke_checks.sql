-- 235_vol_exp_bootstrap_smoke_checks.sql
-- Purpose: Smoke checks for VOL_EXP cohort bootstrap.

use role MIP_ADMIN_ROLE;
use database MIP;

-- 1) Cohort symbols are configured and enabled for daily.
select
    SYMBOL,
    MARKET_TYPE,
    INTERVAL_MINUTES,
    IS_ENABLED,
    SYMBOL_COHORT
from MIP.APP.INGEST_UNIVERSE
where upper(coalesce(SYMBOL_COHORT, 'CORE')) = 'VOL_EXP'
  and INTERVAL_MINUTES = 1440
order by MARKET_TYPE, SYMBOL;

-- 2) Bars are present from requested backfill start.
select
    SYMBOL,
    MARKET_TYPE,
    min(TS::date) as first_bar_date,
    max(TS::date) as last_bar_date,
    count(*) as bar_count
from MIP.MART.MARKET_BARS b
join MIP.APP.INGEST_UNIVERSE u
  on upper(u.SYMBOL) = upper(b.SYMBOL)
 and upper(u.MARKET_TYPE) = upper(b.MARKET_TYPE)
 and u.INTERVAL_MINUTES = b.INTERVAL_MINUTES
where upper(coalesce(u.SYMBOL_COHORT, 'CORE')) = 'VOL_EXP'
  and b.INTERVAL_MINUTES = 1440
  and b.TS::date >= to_date('2025-09-01')
group by SYMBOL, MARKET_TYPE
order by MARKET_TYPE, SYMBOL;

-- 3) Bootstrap recommendation writes are cohort-only (no CORE contamination with bootstrap marker).
select
    count(*) as core_bootstrap_recommendation_rows
from MIP.APP.RECOMMENDATION_LOG r
join MIP.APP.INGEST_UNIVERSE u
  on upper(u.SYMBOL) = upper(r.SYMBOL)
 and upper(u.MARKET_TYPE) = upper(r.MARKET_TYPE)
 and u.INTERVAL_MINUTES = r.INTERVAL_MINUTES
where r.DETAILS:bootstrap_mode::boolean = true
  and upper(coalesce(u.SYMBOL_COHORT, 'CORE')) <> 'VOL_EXP';

-- 4) Readiness and diagnostics views are populated.
select * from MIP.MART.V_SYMBOL_TRAINING_READINESS where COHORT = 'VOL_EXP' order by SYMBOL;
select * from MIP.MART.V_VOL_EXP_BOOTSTRAP_DIAGNOSTICS order by MARKET_TYPE, SYMBOL;

