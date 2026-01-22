-- v_agent_signal_health_brief.sql
-- Purpose: Agent-ready signal health snapshot with trust/calibration flags

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_AGENT_SIGNAL_HEALTH_BRIEF as
with calibration as (
    select
        PATTERN_ID,
        MARKET_TYPE,
        INTERVAL_MINUTES,
        HORIZON_BARS,
        count(distinct SCORE_DECILE) as CALIBRATION_DECILES,
        sum(N) as CALIBRATION_SAMPLE_N
    from MIP.MART.V_SCORE_CALIBRATION
    group by
        PATTERN_ID,
        MARKET_TYPE,
        INTERVAL_MINUTES,
        HORIZON_BARS
)
select
    s.PATTERN_ID,
    s.MARKET_TYPE,
    s.INTERVAL_MINUTES,
    s.HORIZON_BARS,
    s.N_TOTAL,
    s.N_SUCCESS,
    s.N_PENDING,
    s.COVERAGE_RATE,
    s.AVG_RETURN,
    s.MEDIAN_RETURN,
    s.STDDEV_RETURN,
    s.MIN_RETURN,
    s.MAX_RETURN,
    s.HIT_RATE,
    s.AVG_WIN,
    s.AVG_LOSS,
    s.SCORE_RETURN_CORR,
    s.OLDEST_NOT_READY_ENTRY_TS,
    s.NEWEST_NOT_READY_ENTRY_TS,
    s.LATEST_MATURED_ENTRY_TS,
    case
        when s.N_SUCCESS >= 30
         and s.COVERAGE_RATE >= 0.8
         and (s.AVG_RETURN > 0 or s.MEDIAN_RETURN > 0)
        then 'TRUSTED'
        when s.N_SUCCESS >= 30
         and s.COVERAGE_RATE >= 0.8
        then 'WATCH'
        else 'UNTRUSTED'
    end as TRUST_STATUS,
    c.CALIBRATION_DECILES,
    c.CALIBRATION_SAMPLE_N,
    case
        when s.N_SUCCESS >= 30
         and s.COVERAGE_RATE >= 0.8
         and (s.AVG_RETURN > 0 or s.MEDIAN_RETURN > 0)
        then 'OK'
        else 'WARN'
    end as STATUS,
    current_timestamp() as AS_OF_TS
from MIP.MART.V_SIGNAL_OUTCOME_KPIS s
left join calibration c
  on c.PATTERN_ID = s.PATTERN_ID
 and c.MARKET_TYPE = s.MARKET_TYPE
 and c.INTERVAL_MINUTES = s.INTERVAL_MINUTES
 and c.HORIZON_BARS = s.HORIZON_BARS;
