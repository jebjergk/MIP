-- v_trusted_signal_policy.sql
-- Purpose: Policy view for trusted signal recommendations

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_TRUSTED_SIGNAL_POLICY as
select
    s.PATTERN_ID,
    s.MARKET_TYPE,
    s.INTERVAL_MINUTES,
    s.HORIZON_BARS,
    case
        when s.N_SUCCESS >= 30
         and s.COVERAGE_RATE >= 0.8
         and (s.AVG_RETURN > 0 or s.MEDIAN_RETURN > 0)
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
        'score_return_corr', s.SCORE_RETURN_CORR
    ) as REASON,
    current_timestamp() as AS_OF_TS
from MIP.MART.V_SIGNAL_OUTCOME_KPIS s;
