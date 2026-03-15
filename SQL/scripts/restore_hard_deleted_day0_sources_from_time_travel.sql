-- restore_hard_deleted_day0_sources_from_time_travel.sql
-- Purpose:
--   Restore rows deleted by hard_delete_legacy_day0_dashboard_sources.sql
--   using Snowflake Time Travel snapshot from just before delete execution.

use role MIP_ADMIN_ROLE;
use database MIP;

-- First destructive delete started at 2026-03-15 07:48:07.874 +01:00.
-- Use a safety snapshot point just before that.
set v_restore_ts = to_timestamp_tz('2026-03-15 07:48:07.500 +01:00');

select $v_restore_ts as RESTORE_TS;

insert into MIP.APP.RECOMMENDATION_OUTCOMES
select * from MIP.APP.RECOMMENDATION_OUTCOMES at(timestamp => $v_restore_ts)
minus
select * from MIP.APP.RECOMMENDATION_OUTCOMES;
select 'MIP.APP.RECOMMENDATION_OUTCOMES' as TABLE_NAME, "number of rows inserted" as ROWS_RESTORED
from table(result_scan(last_query_id()));

insert into MIP.APP.RECOMMENDATION_LOG
select * from MIP.APP.RECOMMENDATION_LOG at(timestamp => $v_restore_ts)
minus
select * from MIP.APP.RECOMMENDATION_LOG;
select 'MIP.APP.RECOMMENDATION_LOG' as TABLE_NAME, "number of rows inserted" as ROWS_RESTORED
from table(result_scan(last_query_id()));

insert into MIP.AGENT_OUT.ORDER_PROPOSALS
select * from MIP.AGENT_OUT.ORDER_PROPOSALS at(timestamp => $v_restore_ts)
minus
select * from MIP.AGENT_OUT.ORDER_PROPOSALS;
select 'MIP.AGENT_OUT.ORDER_PROPOSALS' as TABLE_NAME, "number of rows inserted" as ROWS_RESTORED
from table(result_scan(last_query_id()));

insert into MIP.APP.MIP_AUDIT_LOG
select * from MIP.APP.MIP_AUDIT_LOG at(timestamp => $v_restore_ts)
minus
select * from MIP.APP.MIP_AUDIT_LOG;
select 'MIP.APP.MIP_AUDIT_LOG' as TABLE_NAME, "number of rows inserted" as ROWS_RESTORED
from table(result_scan(last_query_id()));

insert into MIP.AGENT_OUT.LEARNING_DECISION_LEDGER
select * from MIP.AGENT_OUT.LEARNING_DECISION_LEDGER at(timestamp => $v_restore_ts)
minus
select * from MIP.AGENT_OUT.LEARNING_DECISION_LEDGER;
select 'MIP.AGENT_OUT.LEARNING_DECISION_LEDGER' as TABLE_NAME, "number of rows inserted" as ROWS_RESTORED
from table(result_scan(last_query_id()));

insert into MIP.AGENT_OUT.TRAINING_DIGEST_SNAPSHOT
select * from MIP.AGENT_OUT.TRAINING_DIGEST_SNAPSHOT at(timestamp => $v_restore_ts)
minus
select * from MIP.AGENT_OUT.TRAINING_DIGEST_SNAPSHOT;
select 'MIP.AGENT_OUT.TRAINING_DIGEST_SNAPSHOT' as TABLE_NAME, "number of rows inserted" as ROWS_RESTORED
from table(result_scan(last_query_id()));

insert into MIP.APP.EARLY_EXIT_LOG
select * from MIP.APP.EARLY_EXIT_LOG at(timestamp => $v_restore_ts)
minus
select * from MIP.APP.EARLY_EXIT_LOG;
select 'MIP.APP.EARLY_EXIT_LOG' as TABLE_NAME, "number of rows inserted" as ROWS_RESTORED
from table(result_scan(last_query_id()));

insert into MIP.LIVE.LIVE_ACTIONS
select * from MIP.LIVE.LIVE_ACTIONS at(timestamp => $v_restore_ts)
minus
select * from MIP.LIVE.LIVE_ACTIONS;
select 'MIP.LIVE.LIVE_ACTIONS' as TABLE_NAME, "number of rows inserted" as ROWS_RESTORED
from table(result_scan(last_query_id()));

insert into MIP.LIVE.LIVE_ORDERS
select * from MIP.LIVE.LIVE_ORDERS at(timestamp => $v_restore_ts)
minus
select * from MIP.LIVE.LIVE_ORDERS;
select 'MIP.LIVE.LIVE_ORDERS' as TABLE_NAME, "number of rows inserted" as ROWS_RESTORED
from table(result_scan(last_query_id()));

insert into MIP.LIVE.BROKER_SNAPSHOTS
select * from MIP.LIVE.BROKER_SNAPSHOTS at(timestamp => $v_restore_ts)
minus
select * from MIP.LIVE.BROKER_SNAPSHOTS;
select 'MIP.LIVE.BROKER_SNAPSHOTS' as TABLE_NAME, "number of rows inserted" as ROWS_RESTORED
from table(result_scan(last_query_id()));

insert into MIP.LIVE.DRAWDOWN_LOG
select * from MIP.LIVE.DRAWDOWN_LOG at(timestamp => $v_restore_ts)
minus
select * from MIP.LIVE.DRAWDOWN_LOG;
select 'MIP.LIVE.DRAWDOWN_LOG' as TABLE_NAME, "number of rows inserted" as ROWS_RESTORED
from table(result_scan(last_query_id()));
