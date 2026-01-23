-- 164_trusted_signal_classification.sql
-- Purpose: Canonical trust/gating classification for signals

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.APP.V_TRUSTED_SIGNAL_CLASSIFICATION as
with recs as (
    select
        r.SYMBOL,
        r.MARKET_TYPE,
        r.INTERVAL_MINUTES,
        r.TS,
        r.PATTERN_ID,
        r.SCORE,
        r.GENERATED_AT,
        r.DETAILS
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
    r.SYMBOL,
    r.MARKET_TYPE,
    r.INTERVAL_MINUTES,
    r.TS,
    r.PATTERN_ID,
    coalesce(
        p.TRUST_LABEL,
        case
            when r.SCORE >= 0.7 then 'TRUSTED'
            when r.SCORE >= 0.4 then 'WATCH'
            else 'UNTRUSTED'
        end
    ) as TRUST_LABEL,
    coalesce(
        p.RECOMMENDED_ACTION,
        case
            when r.SCORE >= 0.7 then 'ENABLE'
            when r.SCORE >= 0.4 then 'MONITOR'
            else 'DISABLE'
        end
    ) as RECOMMENDED_ACTION,
    object_construct(
        'policy_source', iff(p.PATTERN_ID is null, 'SCORE_FALLBACK', 'MIP.MART.V_TRUSTED_SIGNAL_POLICY'),
        'policy_version', 'v1',
        'horizon_bars', p.HORIZON_BARS,
        'policy_reason', p.REASON,
        'score', r.SCORE,
        'score_thresholds', object_construct(
            'trusted_min', 0.7,
            'watch_min', 0.4
        ),
        'generated_at', r.GENERATED_AT
    ) as GATING_REASON
from recs r
left join policy_ranked p
  on p.PATTERN_ID = r.PATTERN_ID
 and p.MARKET_TYPE = r.MARKET_TYPE
 and p.INTERVAL_MINUTES = r.INTERVAL_MINUTES
 and p.POLICY_RN = 1;
