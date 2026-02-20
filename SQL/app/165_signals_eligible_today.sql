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
interval_flags as (
    select
        INTERVAL_MINUTES,
        count_if(LOG_RUN_ID is not null and GENERATED_AT::date = current_date()) > 0 as HAS_RUN_ID
    from recs
    group by INTERVAL_MINUTES
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
join interval_flags f
  on f.INTERVAL_MINUTES = r.INTERVAL_MINUTES
left join MIP.APP.V_TRUSTED_SIGNAL_CLASSIFICATION c
  on c.SYMBOL = r.SYMBOL
 and c.MARKET_TYPE = r.MARKET_TYPE
 and c.INTERVAL_MINUTES = r.INTERVAL_MINUTES
 and c.TS = r.TS
 and c.PATTERN_ID = r.PATTERN_ID
where r.GENERATED_AT::date = current_date()
  and (f.HAS_RUN_ID = false or r.LOG_RUN_ID is not null);
