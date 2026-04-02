-- Phase 4: TRADE_CLOSEOUT hardened columns + reconstruction view + ALIGN_RULE_V1 shape checks.
use role MIP_ADMIN_ROLE;
use database MIP;
use schema LIVE;

-- 10.1 Schema / shape
select 'P4_TC_COL_PROPOSAL_ID' as check_name, count(*) as cnt
from MIP.INFORMATION_SCHEMA.COLUMNS
where TABLE_SCHEMA = 'LIVE' and TABLE_NAME = 'TRADE_CLOSEOUT' and COLUMN_NAME = 'PROPOSAL_ID';

select 'P4_TC_COL_FROZEN' as check_name, count(*) as cnt
from MIP.INFORMATION_SCHEMA.COLUMNS
where TABLE_SCHEMA = 'LIVE' and TABLE_NAME = 'TRADE_CLOSEOUT' and COLUMN_NAME = 'FROZEN_ENTRY_EXPECTATION';

select 'P4_TC_COL_EXIT_ACTION' as check_name, count(*) as cnt
from MIP.INFORMATION_SCHEMA.COLUMNS
where TABLE_SCHEMA = 'LIVE' and TABLE_NAME = 'TRADE_CLOSEOUT' and COLUMN_NAME = 'EXIT_ACTION_ID';

select 'P4_VIEW_RECONSTRUCTION' as check_name, count(*) as cnt
from MIP.INFORMATION_SCHEMA.VIEWS
where TABLE_SCHEMA = 'LIVE' and TABLE_NAME = 'V_ENTRY_INTEL_LIFECYCLE_RECONSTRUCTION';

-- 10.2 / 10.4: When closeouts exist, ALIGN_RULE_V1 must appear (deterministic writer)
select 'P4_CLOSEOUT_ALIGN_RULE_SAMPLE' as check_name,
       coalesce(max(ALIGNMENT_JSON:comparison_rule_version::varchar), 'NO_ROWS') as rule_version,
       count(*) as row_cnt
from MIP.LIVE.TRADE_CLOSEOUT;

-- 10.4 Real-flow placeholder (IDs filled in validation note when available)
select 'P4_LINKED_CLOSEOUT_CHAIN' as check_name,
       tc.ENTRY_ACTION_ID,
       tc.SNAPSHOT_ID,
       tc.CLOSEOUT_ID,
       tc.ALIGNMENT_JSON:alignment_class::varchar as alignment_class
from MIP.LIVE.TRADE_CLOSEOUT tc
inner join MIP.LIVE.ENTRY_INTEL_ACTION_LINK l on l.ENTRY_ACTION_ID = tc.ENTRY_ACTION_ID
limit 5;
