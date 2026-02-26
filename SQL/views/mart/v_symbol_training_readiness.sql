-- v_symbol_training_readiness.sql
-- Purpose: Cohort readiness for normal trust-gated actionability.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_SYMBOL_TRAINING_READINESS as
with cfg as (
    select
        coalesce(max(iff(CONFIG_KEY = 'BOOTSTRAP_MIN_OUTCOMES_N', try_to_number(CONFIG_VALUE), null)), 20) as MIN_OUTCOMES_N,
        coalesce(max(iff(CONFIG_KEY = 'BOOTSTRAP_MAX_CI_WIDTH', try_to_double(CONFIG_VALUE), null)), 0.35) as MAX_CI_WIDTH,
        coalesce(max(iff(CONFIG_KEY = 'BOOTSTRAP_MIN_TRUSTED_SHARE', try_to_double(CONFIG_VALUE), null)), 0.50) as MIN_TRUSTED_SHARE,
        coalesce(max(iff(CONFIG_KEY = 'BOOTSTRAP_MAX_FALLBACK_SHARE', try_to_double(CONFIG_VALUE), null)), 0.60) as MAX_FALLBACK_SHARE
    from MIP.APP.APP_CONFIG
),
cohort_symbols as (
    select distinct
        upper(iu.SYMBOL) as SYMBOL_KEY,
        iu.SYMBOL,
        upper(iu.MARKET_TYPE) as MARKET_TYPE,
        iu.INTERVAL_MINUTES,
        upper(coalesce(iu.SYMBOL_COHORT, 'CORE')) as COHORT
    from MIP.APP.INGEST_UNIVERSE iu
    where iu.INTERVAL_MINUTES = 1440
      and coalesce(iu.IS_ENABLED, true)
),
outcome_stats as (
    select
        upper(r.SYMBOL) as SYMBOL_KEY,
        upper(r.MARKET_TYPE) as MARKET_TYPE,
        r.INTERVAL_MINUTES,
        count_if(o.EVAL_STATUS = 'SUCCESS' and o.HIT_FLAG is not null) as OUTCOMES_N,
        avg(iff(o.EVAL_STATUS = 'SUCCESS' and o.HIT_FLAG is not null, iff(o.HIT_FLAG, 1.0, 0.0), null)) as HIT_RATE
    from MIP.APP.RECOMMENDATION_OUTCOMES o
    join MIP.APP.RECOMMENDATION_LOG r
      on r.RECOMMENDATION_ID = o.RECOMMENDATION_ID
    where r.INTERVAL_MINUTES = 1440
    group by upper(r.SYMBOL), upper(r.MARKET_TYPE), r.INTERVAL_MINUTES
),
classification_30d as (
    select
        upper(c.SYMBOL) as SYMBOL_KEY,
        upper(c.MARKET_TYPE) as MARKET_TYPE,
        c.INTERVAL_MINUTES,
        count(*) as SIGNALS_30D,
        count_if(c.TRUST_LABEL = 'TRUSTED' and c.RECOMMENDED_ACTION = 'ENABLE') as TRUSTED_ENABLE_COUNT_30D,
        avg(iff(c.GATING_REASON:policy_source::string = 'SCORE_FALLBACK', 1.0, 0.0)) as FALLBACK_SHARE
    from MIP.APP.V_TRUSTED_SIGNAL_CLASSIFICATION c
    where c.INTERVAL_MINUTES = 1440
      and c.TS >= dateadd(day, -30, current_timestamp()::timestamp_ntz)
    group by upper(c.SYMBOL), upper(c.MARKET_TYPE), c.INTERVAL_MINUTES
)
select
    cs.SYMBOL as SYMBOL,
    cs.COHORT as COHORT,
    coalesce(os.OUTCOMES_N, 0) as OUTCOMES_N,
    case
        when coalesce(cl.SIGNALS_30D, 0) = 0 then 'UNTRUSTED'
        when cl.TRUSTED_ENABLE_COUNT_30D > 0 then 'TRUSTED'
        else 'WATCH'
    end as TRUSTED_LEVEL,
    coalesce(cl.FALLBACK_SHARE, 1.0) as FALLBACK_SHARE,
    case
        when coalesce(os.OUTCOMES_N, 0) > 0 and os.HIT_RATE is not null
        then 2 * 1.96 * sqrt((os.HIT_RATE * (1 - os.HIT_RATE)) / nullif(os.OUTCOMES_N, 0))
        else null
    end as CI_WIDTH,
    iff(
        coalesce(os.OUTCOMES_N, 0) >= cfg.MIN_OUTCOMES_N
        and coalesce(
            case
                when coalesce(os.OUTCOMES_N, 0) > 0 and os.HIT_RATE is not null
                then 2 * 1.96 * sqrt((os.HIT_RATE * (1 - os.HIT_RATE)) / nullif(os.OUTCOMES_N, 0))
                else 999.0
            end,
            999.0
        ) <= cfg.MAX_CI_WIDTH
        and coalesce(cl.TRUSTED_ENABLE_COUNT_30D / nullif(cl.SIGNALS_30D, 0), 0.0) >= cfg.MIN_TRUSTED_SHARE
        and coalesce(cl.FALLBACK_SHARE, 1.0) <= cfg.MAX_FALLBACK_SHARE,
        true,
        false
    ) as READY_FLAG,
    case
        when coalesce(os.OUTCOMES_N, 0) < cfg.MIN_OUTCOMES_N then 'MIN_OUTCOMES_NOT_MET'
        when coalesce(cl.FALLBACK_SHARE, 1.0) > cfg.MAX_FALLBACK_SHARE then 'TOO_MUCH_FALLBACK'
        when coalesce(cl.TRUSTED_ENABLE_COUNT_30D / nullif(cl.SIGNALS_30D, 0), 0.0) < cfg.MIN_TRUSTED_SHARE then 'TRUST_SHARE_TOO_LOW'
        when coalesce(
            case
                when coalesce(os.OUTCOMES_N, 0) > 0 and os.HIT_RATE is not null
                then 2 * 1.96 * sqrt((os.HIT_RATE * (1 - os.HIT_RATE)) / nullif(os.OUTCOMES_N, 0))
                else 999.0
            end,
            999.0
        ) > cfg.MAX_CI_WIDTH then 'CI_TOO_WIDE'
        else 'READY'
    end as REASON
from cohort_symbols cs
cross join cfg
left join outcome_stats os
  on os.SYMBOL_KEY = cs.SYMBOL_KEY
 and os.MARKET_TYPE = cs.MARKET_TYPE
 and os.INTERVAL_MINUTES = cs.INTERVAL_MINUTES
left join classification_30d cl
  on cl.SYMBOL_KEY = cs.SYMBOL_KEY
 and cl.MARKET_TYPE = cs.MARKET_TYPE
 and cl.INTERVAL_MINUTES = cs.INTERVAL_MINUTES;

