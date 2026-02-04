-- triage_trusted_signals_pipeline.sql
-- Purpose: Triage notebook to see exactly where the trusted-signals pipeline dries up.
-- Run each section to compare latest timestamps and counts along the chain.
-- Use this to decide gating thresholds and bootstrap mode.

use role MIP_ADMIN_ROLE;
use database MIP;

-- =============================================================================
-- 1. Latest timestamp in MARKET_BARS
-- =============================================================================
select
    'MARKET_BARS' as source,
    max(TS) as latest_ts,
    count(*) as row_count,
    count(distinct date_trunc('day', TS)) as distinct_days
from MIP.MART.MARKET_BARS
where INTERVAL_MINUTES = 1440;

-- =============================================================================
-- 2. Latest timestamp in RECOMMENDATION_LOG
-- =============================================================================
select
    'RECOMMENDATION_LOG' as source,
    max(TS) as latest_ts,
    max(GENERATED_AT) as latest_generated_at,
    count(*) as row_count,
    count(distinct coalesce(DETAILS:run_id::string, to_varchar(GENERATED_AT, 'YYYYMMDD"T"HH24MISS'))) as distinct_run_ids
from MIP.APP.RECOMMENDATION_LOG
where INTERVAL_MINUTES = 1440;

-- =============================================================================
-- 3. Latest timestamp in V_SIGNAL_OUTCOMES_BASE
-- =============================================================================
select
    'V_SIGNAL_OUTCOMES_BASE' as source,
    max(SIGNAL_TS) as latest_signal_ts,
    max(CALCULATED_AT) as latest_calculated_at,
    count(*) as row_count,
    count(distinct RECOMMENDATION_ID) as distinct_recommendations
from MIP.MART.V_SIGNAL_OUTCOMES_BASE;

-- =============================================================================
-- 4. Latest timestamp + count in V_TRAINING_KPIS
-- =============================================================================
select
    'V_TRAINING_KPIS' as source,
    max(LAST_SIGNAL_TS) as latest_signal_ts,
    count(*) as row_count,
    sum(N_SIGNALS) as total_n_signals,
    sum(N_SUCCESS) as total_n_success,
    count_if(N_SUCCESS >= 30) as leaderboard_eligible_count
from MIP.MART.V_TRAINING_KPIS;

-- =============================================================================
-- 5. Count in V_TRUSTED_SIGNALS_LATEST_TS (and V_TRUSTED_PATTERN_HORIZONS)
-- =============================================================================
select
    'V_TRUSTED_PATTERN_HORIZONS' as source,
    count(*) as trusted_pattern_count
from MIP.MART.V_TRUSTED_PATTERN_HORIZONS;

select
    'V_TRUSTED_SIGNALS_LATEST_TS' as source,
    count(*) as trusted_signals_count,
    count(distinct RECOMMENDATION_ID) as distinct_recommendations,
    max(SIGNAL_TS) as latest_signal_ts
from MIP.MART.V_TRUSTED_SIGNALS_LATEST_TS;

-- =============================================================================
-- 6. One-row summary: where does the pipe dry up?
-- =============================================================================
select
    (select max(TS) from MIP.MART.MARKET_BARS where INTERVAL_MINUTES = 1440) as market_bars_latest_ts,
    (select max(TS) from MIP.APP.RECOMMENDATION_LOG where INTERVAL_MINUTES = 1440) as rec_log_latest_ts,
    (select max(SIGNAL_TS) from MIP.MART.V_SIGNAL_OUTCOMES_BASE) as outcomes_base_latest_ts,
    (select max(LAST_SIGNAL_TS) from MIP.MART.V_TRAINING_KPIS) as training_kpis_latest_ts,
    (select count(*) from MIP.MART.V_TRAINING_KPIS) as training_kpis_row_count,
    (select count(*) from MIP.MART.V_TRUSTED_PATTERN_HORIZONS) as trusted_pattern_count,
    (select count(*) from MIP.MART.V_TRUSTED_SIGNALS_LATEST_TS) as trusted_signals_count;
