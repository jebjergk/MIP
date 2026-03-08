use role MIP_ADMIN_ROLE;
use database MIP;

select 'LEARNING_DECISION_LEDGER_EXISTS' as check_name, count(*) as cnt
from MIP.INFORMATION_SCHEMA.TABLES
where TABLE_SCHEMA = 'AGENT_OUT'
  and TABLE_NAME = 'LEARNING_DECISION_LEDGER';

select 'LIVE_EVENT_COUNT_30D' as check_name, count(*) as cnt
from MIP.AGENT_OUT.LEARNING_DECISION_LEDGER
where EVENT_TS >= dateadd(day, -30, current_timestamp())
  and EVENT_NAME like 'LIVE_%';

select
    'CHAINABLE_LIVE_ACTION_KEYS_30D' as check_name,
    count(distinct LIVE_ACTION_ID) as cnt
from MIP.AGENT_OUT.LEARNING_DECISION_LEDGER
where EVENT_TS >= dateadd(day, -30, current_timestamp())
  and LIVE_ACTION_ID is not null;

select
    'RECENT_CHAIN_SAMPLE' as check_name,
    LIVE_ACTION_ID,
    count(*) as chain_event_count,
    min(EVENT_TS) as first_ts,
    max(EVENT_TS) as last_ts
from MIP.AGENT_OUT.LEARNING_DECISION_LEDGER
where EVENT_TS >= dateadd(day, -30, current_timestamp())
  and LIVE_ACTION_ID is not null
group by LIVE_ACTION_ID
order by chain_event_count desc, last_ts desc
limit 10;

