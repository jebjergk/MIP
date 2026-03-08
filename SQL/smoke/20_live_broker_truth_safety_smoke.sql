use role MIP_ADMIN_ROLE;
use database MIP;

select 'DRIFT_LOG_EXISTS' as check_name, count(*) as cnt
from MIP.INFORMATION_SCHEMA.TABLES
where TABLE_SCHEMA = 'LIVE'
  and TABLE_NAME = 'DRIFT_LOG';

select 'ORPHAN_EXECUTION_ACTIONS' as check_name, count(*) as cnt
from MIP.LIVE.LIVE_ACTIONS a
where a.STATUS in ('EXECUTION_REQUESTED', 'EXECUTION_PARTIAL')
  and not exists (
    select 1
    from MIP.LIVE.LIVE_ORDERS o
    where o.ACTION_ID = a.ACTION_ID
  );

select 'DUPLICATE_ACTIVE_ORDERS_PER_ACTION' as check_name, count(*) as cnt
from (
    select ACTION_ID
    from MIP.LIVE.LIVE_ORDERS
    where STATUS in ('SUBMITTED', 'ACKNOWLEDGED', 'PARTIAL_FILL')
    group by ACTION_ID
    having count(*) > 1
) d;

select 'DUPLICATE_IDEMPOTENCY_KEYS' as check_name, count(*) as cnt
from (
    select IDEMPOTENCY_KEY
    from MIP.LIVE.LIVE_ORDERS
    where IDEMPOTENCY_KEY is not null
    group by IDEMPOTENCY_KEY
    having count(*) > 1
) d;

