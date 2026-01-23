-- 165_signals_eligible_today.sql
-- Purpose: Canonical control-plane view for eligible signals (today + history)

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
        min(r.GENERATED_AT) over (
            partition by r.TS, r.MARKET_TYPE, r.INTERVAL_MINUTES
        ) as RUN_GENERATED_AT
    from MIP.APP.RECOMMENDATION_LOG r
),
policy_scored as (
    select
        p.PATTERN_ID,
        p.MARKET_TYPE,
        p.INTERVAL_MINUTES,
        p.HORIZON_BARS,
        p.TRUST_LABEL,
        p.RECOMMENDED_ACTION,
        p.REASON,
        case p.TRUST_LABEL
            when 'TRUSTED' then 3
            when 'WATCH' then 2
            else 1
        end as TRUST_RANK
    from MIP.MART.V_TRUSTED_SIGNAL_POLICY p
),
policy_ranked as (
    select
        p.*,
        row_number() over (
            partition by p.PATTERN_ID, p.MARKET_TYPE, p.INTERVAL_MINUTES
            order by p.TRUST_RANK desc, p.HORIZON_BARS desc
        ) as POLICY_RN
    from policy_scored p
)
select
    to_varchar(r.RUN_GENERATED_AT, 'YYYYMMDD\"T\"HH24MISS') as RUN_ID,
    r.TS,
    r.SYMBOL,
    r.MARKET_TYPE,
    r.INTERVAL_MINUTES,
    r.PATTERN_ID,
    r.SCORE,
    r.DETAILS,
    coalesce(p.TRUST_LABEL, 'UNTRUSTED') as TRUST_LABEL,
    coalesce(p.RECOMMENDED_ACTION, 'DISABLE') as RECOMMENDED_ACTION,
    iff(
        coalesce(p.TRUST_LABEL, 'UNTRUSTED') = 'TRUSTED'
        and coalesce(p.RECOMMENDED_ACTION, 'DISABLE') = 'ENABLE',
        true,
        false
    ) as IS_ELIGIBLE,
    object_construct(
        'trust_label', coalesce(p.TRUST_LABEL, 'UNTRUSTED'),
        'recommended_action', coalesce(p.RECOMMENDED_ACTION, 'DISABLE'),
        'policy_source', 'MIP.MART.V_TRUSTED_SIGNAL_POLICY',
        'policy_version', 'v1',
        'horizon_bars', p.HORIZON_BARS,
        'reason', p.REASON,
        'note', iff(p.PATTERN_ID is null, 'NO_POLICY_MATCH', null)
    ) as GATING_REASON
from recs r
left join policy_ranked p
  on p.PATTERN_ID = r.PATTERN_ID
 and p.MARKET_TYPE = r.MARKET_TYPE
 and p.INTERVAL_MINUTES = r.INTERVAL_MINUTES
 and p.POLICY_RN = 1;
