-- backfill_morning_brief_created_at.sql
-- Purpose: One-time migration to backfill NULL CREATED_AT values in MORNING_BRIEF.
-- Run ONCE after deploying the schema fix (185_agent_out_morning_brief.sql).
--
-- Strategy:
--   1. Try to match PIPELINE_RUN_ID to MIP_AUDIT_LOG to get actual run timestamp
--   2. Fallback: Use AS_OF_TS as approximate timestamp (documented as approximate)
--   3. For deterministic ordering within same AS_OF_TS, add a small offset based on BRIEF_ID

use role MIP_ADMIN_ROLE;
use database MIP;

-- Step 1: Check how many NULL CREATED_AT values exist
select 
    count(*) as total_rows,
    count_if(CREATED_AT is null) as null_created_at,
    count_if(CREATED_AT is not null) as has_created_at
from MIP.AGENT_OUT.MORNING_BRIEF;

-- Step 2: Backfill from audit log where possible
-- Match PIPELINE_RUN_ID to audit log event timestamp
update MIP.AGENT_OUT.MORNING_BRIEF mb
   set CREATED_AT = audit.event_ts
  from (
      select 
          RUN_ID,
          min(EVENT_TS) as event_ts
      from MIP.APP.MIP_AUDIT_LOG
      where EVENT_TYPE = 'PIPELINE'
        and EVENT_NAME in ('SP_RUN_DAILY_PIPELINE', 'MORNING_BRIEF')
      group by RUN_ID
  ) audit
 where mb.PIPELINE_RUN_ID = audit.RUN_ID
   and mb.CREATED_AT is null;

-- Step 3: For remaining NULLs, use AS_OF_TS as fallback with deterministic offset
-- Add small millisecond offset based on BRIEF_ID for ordering determinism
update MIP.AGENT_OUT.MORNING_BRIEF
   set CREATED_AT = dateadd(
       millisecond, 
       mod(BRIEF_ID, 1000),  -- deterministic offset 0-999ms based on BRIEF_ID
       AS_OF_TS
   )
 where CREATED_AT is null;

-- Step 4: Verify no NULLs remain
select 
    count(*) as total_rows,
    count_if(CREATED_AT is null) as null_created_at,
    count_if(CREATED_AT is not null) as has_created_at,
    min(CREATED_AT) as min_created_at,
    max(CREATED_AT) as max_created_at
from MIP.AGENT_OUT.MORNING_BRIEF;

-- Step 5: Add comment documenting the backfill
comment on column MIP.AGENT_OUT.MORNING_BRIEF.CREATED_AT is 
    'Timestamp when brief was generated. Use this (not AS_OF_TS) for "latest brief" selection. '
    'Historical rows backfilled from audit log or AS_OF_TS (approximate) on 2026-02-04.';
