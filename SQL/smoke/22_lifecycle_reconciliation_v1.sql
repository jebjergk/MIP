-- Smoke: lifecycle reconciliation v1 DDL + read path
-- Expect: tables exist; optional empty SELECT.

select 'LIFECYCLE_RECONCILIATION_STATE' as obj, count(*) as row_count
from MIP.LIVE.LIFECYCLE_RECONCILIATION_STATE;

select 'LIFECYCLE_RECONCILIATION_EVENT' as obj, count(*) as row_count
from MIP.LIVE.LIFECYCLE_RECONCILIATION_EVENT;

select PORTFOLIO_ID, SYMBOL, RECONCILIATION_CLASS, RULE_VERSION, UPDATED_TS
from MIP.LIVE.LIFECYCLE_RECONCILIATION_STATE
order by UPDATED_TS desc nulls last
limit 10;
