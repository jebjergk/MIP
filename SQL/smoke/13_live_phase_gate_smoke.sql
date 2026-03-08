use role MIP_ADMIN_ROLE;
use database MIP;

-- 1) Core object existence checks.
select 'LEARNING_DECISION_LEDGER_EXISTS' as check_name, count(*) as cnt
from MIP.INFORMATION_SCHEMA.TABLES
where TABLE_SCHEMA = 'AGENT_OUT'
  and TABLE_NAME = 'LEARNING_DECISION_LEDGER';

select 'LIVE_ACTIONS_EXISTS' as check_name, count(*) as cnt
from MIP.INFORMATION_SCHEMA.TABLES
where TABLE_SCHEMA = 'LIVE'
  and TABLE_NAME = 'LIVE_ACTIONS';

select 'LIVE_ORDERS_EXISTS' as check_name, count(*) as cnt
from MIP.INFORMATION_SCHEMA.TABLES
where TABLE_SCHEMA = 'LIVE'
  and TABLE_NAME = 'LIVE_ORDERS';

select 'BROKER_EVENT_LEDGER_EXISTS' as check_name, count(*) as cnt
from MIP.INFORMATION_SCHEMA.TABLES
where TABLE_SCHEMA = 'LIVE'
  and TABLE_NAME = 'BROKER_EVENT_LEDGER';

-- 2) Procedure existence checks.
show procedures like 'SP_LEDGER_APPEND_EVENT' in schema MIP.APP;
show procedures like 'SP_RUN_HOURLY_EARLY_EXIT_MONITOR' in schema MIP.APP;

-- 3) Data-path sanity checks (non-destructive).
select 'AUDIT_RECENT_7D' as check_name, count(*) as cnt
from MIP.APP.MIP_AUDIT_LOG
where EVENT_TS >= dateadd(day, -7, current_timestamp());

select 'LEDGER_RECENT_30D' as check_name, count(*) as cnt
from MIP.AGENT_OUT.LEARNING_DECISION_LEDGER
where EVENT_TS >= dateadd(day, -30, current_timestamp());

select 'EARLY_EXIT_AUDIT_RECENT_30D' as check_name, count(*) as cnt
from MIP.APP.MIP_AUDIT_LOG
where EVENT_TYPE = 'EARLY_EXIT_PIPELINE'
  and EVENT_NAME = 'SP_RUN_HOURLY_EARLY_EXIT_MONITOR'
  and EVENT_TS >= dateadd(day, -30, current_timestamp());

select 'LIVE_ACTIONS_COUNT' as check_name, count(*) as cnt
from MIP.LIVE.LIVE_ACTIONS;

select 'LIVE_ORDERS_COUNT' as check_name, count(*) as cnt
from MIP.LIVE.LIVE_ORDERS;
