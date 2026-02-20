-- v_trusted_signal_policy.sql
-- Purpose: Policy view for trusted signal recommendations
-- Combines all-time statistical significance with 90-day recency check
-- so trust naturally degrades when recent performance drops.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_TRUSTED_SIGNAL_POLICY as
with recent_kpis as (
    select
        r.PATTERN_ID,
        r.MARKET_TYPE,
        r.INTERVAL_MINUTES,
        o.HORIZON_BARS,
        count_if(o.EVAL_STATUS = 'SUCCESS' and o.REALIZED_RETURN is not null) as RECENT_N,
        avg(case
            when o.EVAL_STATUS = 'SUCCESS' and o.REALIZED_RETURN is not null
            then o.REALIZED_RETURN
        end) as RECENT_AVG_RETURN,
        median(case
            when o.EVAL_STATUS = 'SUCCESS' and o.REALIZED_RETURN is not null
            then o.REALIZED_RETURN
        end) as RECENT_MEDIAN_RETURN,
        avg(case
            when o.EVAL_STATUS = 'SUCCESS' and o.REALIZED_RETURN is not null and o.HIT_FLAG is not null
            then iff(o.HIT_FLAG, 1, 0)
        end) as RECENT_HIT_RATE
    from MIP.APP.RECOMMENDATION_OUTCOMES o
    join MIP.APP.RECOMMENDATION_LOG r
      on r.RECOMMENDATION_ID = o.RECOMMENDATION_ID
    where o.ENTRY_TS >= dateadd(day, -90, current_date())
    group by r.PATTERN_ID, r.MARKET_TYPE, r.INTERVAL_MINUTES, o.HORIZON_BARS
)
select
    s.PATTERN_ID,
    s.MARKET_TYPE,
    s.INTERVAL_MINUTES,
    s.HORIZON_BARS,
    case
        when s.N_SUCCESS >= 30
         and s.COVERAGE_RATE >= 0.8
         and (s.AVG_RETURN > 0 or s.MEDIAN_RETURN > 0)
         and coalesce(rk.RECENT_N, 0) >= 10
         and coalesce(rk.RECENT_HIT_RATE, 0) >= 0.50
         and (coalesce(rk.RECENT_AVG_RETURN, 0) > 0 or coalesce(rk.RECENT_MEDIAN_RETURN, 0) > 0)
        then 'TRUSTED'
        when s.N_SUCCESS >= 30
         and s.COVERAGE_RATE >= 0.8
        then 'WATCH'
        else 'UNTRUSTED'
    end as TRUST_LABEL,
    case
        when s.N_SUCCESS >= 30
         and s.COVERAGE_RATE >= 0.8
         and (s.AVG_RETURN > 0 or s.MEDIAN_RETURN > 0)
         and coalesce(rk.RECENT_N, 0) >= 10
         and coalesce(rk.RECENT_HIT_RATE, 0) >= 0.50
         and (coalesce(rk.RECENT_AVG_RETURN, 0) > 0 or coalesce(rk.RECENT_MEDIAN_RETURN, 0) > 0)
        then 'ENABLE'
        when s.N_SUCCESS >= 30
         and s.COVERAGE_RATE >= 0.8
        then 'MONITOR'
        else 'DISABLE'
    end as RECOMMENDED_ACTION,
    object_construct(
        'n_success', s.N_SUCCESS,
        'coverage_rate', s.COVERAGE_RATE,
        'avg_return', s.AVG_RETURN,
        'median_return', s.MEDIAN_RETURN,
        'hit_rate', s.HIT_RATE,
        'score_return_corr', s.SCORE_RETURN_CORR,
        'recent_n', rk.RECENT_N,
        'recent_avg_return', rk.RECENT_AVG_RETURN,
        'recent_median_return', rk.RECENT_MEDIAN_RETURN,
        'recent_hit_rate', rk.RECENT_HIT_RATE
    ) as REASON,
    current_timestamp() as AS_OF_TS
from MIP.MART.V_SIGNAL_OUTCOME_KPIS s
left join recent_kpis rk
  on rk.PATTERN_ID = s.PATTERN_ID
 and rk.MARKET_TYPE = s.MARKET_TYPE
 and rk.INTERVAL_MINUTES = s.INTERVAL_MINUTES
 and rk.HORIZON_BARS = s.HORIZON_BARS;
