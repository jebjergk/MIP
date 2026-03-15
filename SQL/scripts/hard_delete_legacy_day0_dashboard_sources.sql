-- hard_delete_legacy_day0_dashboard_sources.sql
-- Purpose:
--   Hard delete legacy rows (older than PERFORMANCE_DASHBOARD_DAY0_TS)
--   from performance-dashboard source tables so no pre-day0 trace remains.

use role MIP_ADMIN_ROLE;
use database MIP;

set v_day0_ts = (
  select try_to_timestamp_ntz(CONFIG_VALUE)
  from MIP.APP.APP_CONFIG
  where CONFIG_KEY = 'PERFORMANCE_DASHBOARD_DAY0_TS'
);

-- Safety: verify baseline timestamp exists.
select $v_day0_ts as DAY0_TS;

delete from MIP.APP.RECOMMENDATION_OUTCOMES
where CALCULATED_AT < $v_day0_ts;
select 'MIP.APP.RECOMMENDATION_OUTCOMES' as TABLE_NAME, "number of rows deleted" as ROWS_DELETED
from table(result_scan(last_query_id()));

delete from MIP.APP.RECOMMENDATION_LOG
where TS < $v_day0_ts;
select 'MIP.APP.RECOMMENDATION_LOG' as TABLE_NAME, "number of rows deleted" as ROWS_DELETED
from table(result_scan(last_query_id()));

delete from MIP.AGENT_OUT.ORDER_PROPOSALS
where PROPOSED_AT < $v_day0_ts;
select 'MIP.AGENT_OUT.ORDER_PROPOSALS' as TABLE_NAME, "number of rows deleted" as ROWS_DELETED
from table(result_scan(last_query_id()));

delete from MIP.APP.MIP_AUDIT_LOG
where EVENT_TS < $v_day0_ts;
select 'MIP.APP.MIP_AUDIT_LOG' as TABLE_NAME, "number of rows deleted" as ROWS_DELETED
from table(result_scan(last_query_id()));

delete from MIP.AGENT_OUT.LEARNING_DECISION_LEDGER
where EVENT_TS < $v_day0_ts;
select 'MIP.AGENT_OUT.LEARNING_DECISION_LEDGER' as TABLE_NAME, "number of rows deleted" as ROWS_DELETED
from table(result_scan(last_query_id()));

delete from MIP.AGENT_OUT.TRAINING_DIGEST_SNAPSHOT
where CREATED_AT < $v_day0_ts;
select 'MIP.AGENT_OUT.TRAINING_DIGEST_SNAPSHOT' as TABLE_NAME, "number of rows deleted" as ROWS_DELETED
from table(result_scan(last_query_id()));

delete from MIP.APP.EARLY_EXIT_LOG
where DECISION_TS < $v_day0_ts;
select 'MIP.APP.EARLY_EXIT_LOG' as TABLE_NAME, "number of rows deleted" as ROWS_DELETED
from table(result_scan(last_query_id()));

delete from MIP.LIVE.LIVE_ACTIONS
where CREATED_AT < $v_day0_ts;
select 'MIP.LIVE.LIVE_ACTIONS' as TABLE_NAME, "number of rows deleted" as ROWS_DELETED
from table(result_scan(last_query_id()));

delete from MIP.LIVE.LIVE_ORDERS
where coalesce(LAST_UPDATED_AT, CREATED_AT) < $v_day0_ts;
select 'MIP.LIVE.LIVE_ORDERS' as TABLE_NAME, "number of rows deleted" as ROWS_DELETED
from table(result_scan(last_query_id()));

delete from MIP.LIVE.BROKER_SNAPSHOTS
where SNAPSHOT_TS < $v_day0_ts;
select 'MIP.LIVE.BROKER_SNAPSHOTS' as TABLE_NAME, "number of rows deleted" as ROWS_DELETED
from table(result_scan(last_query_id()));

delete from MIP.LIVE.DRAWDOWN_LOG
where LOG_TS < $v_day0_ts;
select 'MIP.LIVE.DRAWDOWN_LOG' as TABLE_NAME, "number of rows deleted" as ROWS_DELETED
from table(result_scan(last_query_id()));
