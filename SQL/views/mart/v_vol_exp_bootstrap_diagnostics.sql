-- v_vol_exp_bootstrap_diagnostics.sql
-- Purpose: Compact per-symbol diagnostics for VOL_EXP bootstrap evidence.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_VOL_EXP_BOOTSTRAP_DIAGNOSTICS as
with cohort_symbols as (
    select distinct
        iu.SYMBOL,
        upper(iu.MARKET_TYPE) as MARKET_TYPE,
        iu.INTERVAL_MINUTES,
        upper(coalesce(iu.SYMBOL_COHORT, 'CORE')) as COHORT
    from MIP.APP.INGEST_UNIVERSE iu
    where iu.INTERVAL_MINUTES = 1440
      and coalesce(iu.IS_ENABLED, true)
),
bars_cov as (
    select
        b.SYMBOL,
        upper(b.MARKET_TYPE) as MARKET_TYPE,
        b.INTERVAL_MINUTES,
        min(b.TS::date) as FIRST_BAR_DATE,
        max(b.TS::date) as LAST_BAR_DATE,
        count(*) as BAR_COUNT_SINCE_2025_09_01
    from MIP.MART.MARKET_BARS b
    where b.INTERVAL_MINUTES = 1440
      and b.TS::date >= to_date('2025-09-01')
    group by b.SYMBOL, upper(b.MARKET_TYPE), b.INTERVAL_MINUTES
),
signals_cov as (
    select
        r.SYMBOL,
        upper(r.MARKET_TYPE) as MARKET_TYPE,
        r.INTERVAL_MINUTES,
        count(*) as SIGNAL_COUNT_SINCE_2025_09_01
    from MIP.APP.RECOMMENDATION_LOG r
    where r.INTERVAL_MINUTES = 1440
      and r.TS::date >= to_date('2025-09-01')
    group by r.SYMBOL, upper(r.MARKET_TYPE), r.INTERVAL_MINUTES
),
outcomes_cov as (
    select
        r.SYMBOL,
        upper(r.MARKET_TYPE) as MARKET_TYPE,
        r.INTERVAL_MINUTES,
        count_if(o.EVAL_STATUS = 'SUCCESS') as OUTCOME_SUCCESS_COUNT
    from MIP.APP.RECOMMENDATION_OUTCOMES o
    join MIP.APP.RECOMMENDATION_LOG r
      on r.RECOMMENDATION_ID = o.RECOMMENDATION_ID
    where r.INTERVAL_MINUTES = 1440
      and r.TS::date >= to_date('2025-09-01')
    group by r.SYMBOL, upper(r.MARKET_TYPE), r.INTERVAL_MINUTES
),
trust_dist as (
    select
        c.SYMBOL,
        upper(c.MARKET_TYPE) as MARKET_TYPE,
        c.INTERVAL_MINUTES,
        count_if(c.TRUST_LABEL = 'TRUSTED') as TRUSTED_ROWS,
        count_if(c.TRUST_LABEL = 'WATCH') as WATCH_ROWS,
        count_if(c.TRUST_LABEL = 'UNTRUSTED') as UNTRUSTED_ROWS
    from MIP.APP.V_TRUSTED_SIGNAL_CLASSIFICATION c
    where c.INTERVAL_MINUTES = 1440
      and c.TS::date >= to_date('2025-09-01')
    group by c.SYMBOL, upper(c.MARKET_TYPE), c.INTERVAL_MINUTES
)
select
    cs.SYMBOL,
    cs.MARKET_TYPE,
    cs.COHORT,
    coalesce(bc.FIRST_BAR_DATE, null) as FIRST_BAR_DATE,
    coalesce(bc.LAST_BAR_DATE, null) as LAST_BAR_DATE,
    coalesce(bc.BAR_COUNT_SINCE_2025_09_01, 0) as BAR_COUNT_SINCE_2025_09_01,
    coalesce(sc.SIGNAL_COUNT_SINCE_2025_09_01, 0) as SIGNAL_COUNT_SINCE_2025_09_01,
    coalesce(oc.OUTCOME_SUCCESS_COUNT, 0) as OUTCOME_SUCCESS_COUNT,
    coalesce(td.TRUSTED_ROWS, 0) as TRUSTED_ROWS,
    coalesce(td.WATCH_ROWS, 0) as WATCH_ROWS,
    coalesce(td.UNTRUSTED_ROWS, 0) as UNTRUSTED_ROWS,
    sr.READY_FLAG,
    sr.REASON as READINESS_REASON
from cohort_symbols cs
left join bars_cov bc
  on bc.SYMBOL = cs.SYMBOL
 and bc.MARKET_TYPE = cs.MARKET_TYPE
 and bc.INTERVAL_MINUTES = cs.INTERVAL_MINUTES
left join signals_cov sc
  on sc.SYMBOL = cs.SYMBOL
 and sc.MARKET_TYPE = cs.MARKET_TYPE
 and sc.INTERVAL_MINUTES = cs.INTERVAL_MINUTES
left join outcomes_cov oc
  on oc.SYMBOL = cs.SYMBOL
 and oc.MARKET_TYPE = cs.MARKET_TYPE
 and oc.INTERVAL_MINUTES = cs.INTERVAL_MINUTES
left join trust_dist td
  on td.SYMBOL = cs.SYMBOL
 and td.MARKET_TYPE = cs.MARKET_TYPE
 and td.INTERVAL_MINUTES = cs.INTERVAL_MINUTES
left join MIP.MART.V_SYMBOL_TRAINING_READINESS sr
  on sr.SYMBOL = cs.SYMBOL
where cs.COHORT = 'VOL_EXP'
order by cs.MARKET_TYPE, cs.SYMBOL;

