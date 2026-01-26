-- smoke_tests.sql
-- Purpose: Basic smoke tests for core MIP components

use role MIP_ADMIN_ROLE;
use database MIP;

-- Test 1: Basic connectivity and table existence
select
    'TABLE_EXISTENCE' as test_name,
    count(*) as tables_found
from information_schema.tables
where table_schema in ('MIP.APP', 'MIP.MART', 'MIP.AGENT_OUT', 'MIP.RAW_EXT')
  and table_type = 'BASE TABLE';

-- Test 2: View compilation validation
select
    'VIEW_COMPILATION' as test_name,
    count(*) as views_found,
    count_if(table_type = 'VIEW') as compiled_views
from information_schema.tables
where table_schema in ('MIP.APP', 'MIP.MART', 'MIP.AGENT_OUT')
  and table_type = 'VIEW';

-- Test 3: Procedure signature validation
select
    'PROCEDURE_SIGNATURES' as test_name,
    count(*) as procedures_found
from information_schema.procedures
where procedure_schema = 'MIP.APP'
  and procedure_name like 'SP_%';

-- Test 4: Core table non-empty checks (where expected)
select
    'DATA_INTEGRITY' as test_name,
    (select count(*) from MIP.APP.PORTFOLIO) as portfolio_count,
    (select count(*) from MIP.APP.PORTFOLIO_PROFILE) as profile_count,
    (select count(*) from MIP.APP.PATTERN_DEFINITION) as pattern_count,
    (select count(*) from MIP.APP.INGEST_UNIVERSE) as ingest_universe_count,
    (select count(*) from MIP.MART.MARKET_BARS) as market_bars_count,
    (select count(*) from MIP.APP.RECOMMENDATION_LOG) as recommendation_count,
    (select count(*) from MIP.APP.MIP_AUDIT_LOG) as audit_log_count;

-- Test 5: Key view accessibility
select
    'VIEW_ACCESSIBILITY' as test_name,
    (select count(*) from MIP.MART.V_PORTFOLIO_RISK_GATE) as risk_gate_rows,
    (select count(*) from MIP.APP.V_SIGNALS_ELIGIBLE_TODAY) as eligible_signals_rows,
    (select count(*) from MIP.MART.V_PORTFOLIO_OPEN_POSITIONS) as open_positions_rows,
    (select count(*) from MIP.MART.V_PORTFOLIO_RUN_KPIS) as portfolio_kpis_rows;

-- Test 6: Audit log recent entries
select
    'AUDIT_LOG_RECENT' as test_name,
    count(*) as recent_entries,
    min(EVENT_TS) as earliest_event,
    max(EVENT_TS) as latest_event
from MIP.APP.MIP_AUDIT_LOG
where EVENT_TS >= current_timestamp() - interval '7 days';

-- Test 7: Portfolio simulation readiness
select
    'PORTFOLIO_SIMULATION_READINESS' as test_name,
    count(distinct p.PORTFOLIO_ID) as active_portfolios,
    count(distinct pd.PORTFOLIO_ID) as portfolios_with_daily_records,
    count(distinct pt.PORTFOLIO_ID) as portfolios_with_trades
from MIP.APP.PORTFOLIO p
left join MIP.APP.PORTFOLIO_DAILY pd
  on pd.PORTFOLIO_ID = p.PORTFOLIO_ID
left join MIP.APP.PORTFOLIO_TRADES pt
  on pt.PORTFOLIO_ID = p.PORTFOLIO_ID
where p.STATUS = 'ACTIVE';

-- Test 8: Morning brief existence
select
    'MORNING_BRIEF_EXISTENCE' as test_name,
    count(*) as brief_count,
    count(distinct PIPELINE_RUN_ID) as distinct_runs,
    count(distinct PORTFOLIO_ID) as distinct_portfolios,
    min(CREATED_AT) as earliest_brief,
    max(CREATED_AT) as latest_brief
from MIP.AGENT_OUT.MORNING_BRIEF;

-- Test 9: Order proposals status distribution
select
    'ORDER_PROPOSALS_STATUS' as test_name,
    count(*) as total_proposals,
    count_if(STATUS = 'PROPOSED') as proposed_count,
    count_if(STATUS = 'APPROVED') as approved_count,
    count_if(STATUS = 'REJECTED') as rejected_count,
    count_if(STATUS = 'EXECUTED') as executed_count
from MIP.AGENT_OUT.ORDER_PROPOSALS;

-- Test 10: Data freshness check
select
    'DATA_FRESHNESS' as test_name,
    max(mb.TS) as latest_market_bar,
    max(mr.TS) as latest_return,
    max(rl.GENERATED_AT) as latest_recommendation,
    datediff('hour', max(mb.TS), current_timestamp()) as hours_since_latest_bar,
    datediff('hour', max(mr.TS), current_timestamp()) as hours_since_latest_return,
    datediff('hour', max(rl.GENERATED_AT), current_timestamp()) as hours_since_latest_recommendation
from MIP.MART.MARKET_BARS mb
cross join (select max(TS) as TS from MIP.MART.MARKET_RETURNS) mr
cross join (select max(GENERATED_AT) as GENERATED_AT from MIP.APP.RECOMMENDATION_LOG) rl;

-- Summary: Overall smoke test status
select
    'SMOKE_TEST_SUMMARY' as test_name,
    current_timestamp() as test_timestamp,
    'All smoke tests completed' as status;
