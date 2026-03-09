-- 165_signals_eligible_today.sql
-- Purpose: Canonical control-plane view for eligible signals (today + history)
-- Fix: run_id filtering is now per-interval so daily signals are not excluded
--      when intraday signals (which carry run_ids) are present.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.APP.V_SIGNALS_ELIGIBLE_TODAY as
with recs as (
    select
        r.RECOMMENDATION_ID,
        r.PATTERN_ID,
        r.SYMBOL,
        r.MARKET_TYPE,
        r.INTERVAL_MINUTES,
        r.TS,
        r.GENERATED_AT,
        r.SCORE,
        r.DETAILS,
        r.DETAILS:run_id::string as LOG_RUN_ID,
        min(r.GENERATED_AT) over (
            partition by r.TS, r.MARKET_TYPE, r.INTERVAL_MINUTES
        ) as RUN_GENERATED_AT
    from MIP.APP.RECOMMENDATION_LOG r
),
latest_interval_day as (
    select
        INTERVAL_MINUTES,
        max(GENERATED_AT::date) as LATEST_GENERATED_DATE
    from recs
    group by INTERVAL_MINUTES
),
interval_flags as (
    select
        r.INTERVAL_MINUTES,
        count_if(
            r.LOG_RUN_ID is not null
            and r.GENERATED_AT::date = d.LATEST_GENERATED_DATE
        ) > 0 as HAS_RUN_ID
    from recs r
    join latest_interval_day d
      on d.INTERVAL_MINUTES = r.INTERVAL_MINUTES
    group by r.INTERVAL_MINUTES
)
select
    coalesce(r.LOG_RUN_ID, to_varchar(r.RUN_GENERATED_AT, 'YYYYMMDD"T"HH24MISS')) as RUN_ID,
    r.RECOMMENDATION_ID,
    r.TS,
    r.SYMBOL,
    r.MARKET_TYPE,
    r.INTERVAL_MINUTES,
    r.PATTERN_ID,
    r.SCORE,
    r.DETAILS,
    c.TRAINING_VERSION,
    c.TRUST_LABEL,
    c.RECOMMENDED_ACTION,
    iff(
        c.TRUST_LABEL = 'TRUSTED'
        and c.RECOMMENDED_ACTION = 'ENABLE',
        true,
        false
    ) as IS_ELIGIBLE,
    c.GATING_REASON
from recs r
join latest_interval_day d
  on d.INTERVAL_MINUTES = r.INTERVAL_MINUTES
join interval_flags f
  on f.INTERVAL_MINUTES = r.INTERVAL_MINUTES
left join MIP.APP.V_TRUSTED_SIGNAL_CLASSIFICATION c
  on c.SYMBOL = r.SYMBOL
 and c.MARKET_TYPE = r.MARKET_TYPE
 and c.INTERVAL_MINUTES = r.INTERVAL_MINUTES
 and c.TS = r.TS
 and c.PATTERN_ID = r.PATTERN_ID
where r.GENERATED_AT::date = d.LATEST_GENERATED_DATE
  and (f.HAS_RUN_ID = false or r.LOG_RUN_ID is not null);
