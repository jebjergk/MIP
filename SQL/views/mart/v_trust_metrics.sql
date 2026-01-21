-- v_trust_metrics.sql
-- Purpose: Consumer-ready signal KPIs with minimum sample size filters

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_TRUST_METRICS as
select
    PATTERN_ID,
    MARKET_TYPE,
    INTERVAL_MINUTES,
    HORIZON_BARS,
    N_TOTAL,
    N_SUCCESS,
    N_PENDING,
    COVERAGE_RATE,
    AVG_RETURN,
    MEDIAN_RETURN,
    STDDEV_RETURN,
    MIN_RETURN,
    MAX_RETURN,
    HIT_RATE,
    AVG_WIN,
    AVG_LOSS,
    SCORE_RETURN_CORR,
    OLDEST_NOT_READY_ENTRY_TS,
    NEWEST_NOT_READY_ENTRY_TS,
    LATEST_MATURED_ENTRY_TS
from MIP.MART.V_SIGNAL_OUTCOME_KPIS
where N_SUCCESS >= 30
  and COVERAGE_RATE >= 0.8;
