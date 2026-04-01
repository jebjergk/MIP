-- 18_entry_intel_lifecycle_smoke.sql
-- Phase 1: EIS immutability (append role), proposal→snapshot, link, closeout chain checks.
-- Run as MIP_ADMIN_ROLE except where noted. Review last statements for expected errors.

use role MIP_ADMIN_ROLE;
use database MIP;

-- Prerequisites
select 'EIS_TABLES' as check_label, count(*) as cnt
from information_schema.tables
where table_schema = 'LIVE'
  and table_name in ('ENTRY_INTEL_SNAPSHOT', 'ENTRY_INTEL_ACTION_LINK', 'TRADE_CLOSEOUT');

-- ---------------------------------------------------------------------------
-- Test 1 (manual): UPDATE must fail as MIP_EIS_APPEND_ONLY
-- Run separately:
--   use role MIP_EIS_APPEND_ONLY;
--   update MIP.LIVE.ENTRY_INTEL_SNAPSHOT set EIS_NOTE = 'x' where 1=0;
-- Expect: SQL compilation error or authorization error (no UPDATE privilege).
-- ---------------------------------------------------------------------------

-- Pick a real proposal (latest)
set smoke_proposal_id = (select max(PROPOSAL_ID) from MIP.AGENT_OUT.ORDER_PROPOSALS);

-- Ensure EIS row exists (idempotent)
call MIP.APP.SP_ENSURE_ENTRY_INTEL_FOR_PROPOSAL($smoke_proposal_id);

-- Test 2: exactly >=1 snapshot for that proposal after ensure
select 'EIS_ROWS_FOR_PROPOSAL' as check_label, count(*) as cnt
from MIP.LIVE.ENTRY_INTEL_SNAPSHOT
where PROPOSAL_ID = $smoke_proposal_id;

-- Test 5-style reconstruction: snapshot id
set smoke_snapshot_id = (
    select SNAPSHOT_ID
    from MIP.LIVE.ENTRY_INTEL_SNAPSHOT
    where PROPOSAL_ID = $smoke_proposal_id
    order by EIS_VERSION desc
    limit 1
);

select 'SNAPSHOT_ID_RESOLVED' as check_label, $smoke_snapshot_id as snapshot_id;

-- Optional: link + closeout dry-run requires a LIVE_ACTIONS row — skip if none for proposal
-- (Integration tests use API import + order FILLED for full chain.)
