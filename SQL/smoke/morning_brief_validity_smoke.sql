-- morning_brief_validity_smoke.sql
-- Assert no bad brief rows for a pipeline run: portfolio_id > 0, pipeline_run_id and as_of_ts present in BRIEF.
-- Also assert attribution overwrite contract: as_of_ts at root only, not in attribution.
-- Assert no portfolio_id <= 0 or AGENT_V0_MORNING_BRIEF artifacts.

use role MIP_ADMIN_ROLE;
use database MIP;

-- 0) No portfolio_id <= 0 or agent brief artifacts (AGENT_V0_MORNING_BRIEF removed). Fail if bad_count > 0.
select count(*) as bad_count
from MIP.AGENT_OUT.MORNING_BRIEF
where portfolio_id <= 0;
-- Expect bad_count = 0; smoke fails if bad_count > 0.

-- Set run_id to the pipeline run to check, e.g. from MIP_AUDIT_LOG or last SP_RUN_DAILY_PIPELINE result.
-- Example: set run_id = 'your-run-id';

-- 1) General validity: portfolio > 0, pipeline_run_id and as_of_ts present.
select count(*) as bad
from MIP.AGENT_OUT.MORNING_BRIEF
where run_id = $run_id
  and (
    portfolio_id <= 0
    or brief:"pipeline_run_id" is null
    or coalesce(brief:"as_of_ts", brief:"attribution":"as_of_ts") is null
  );
-- Expect bad = 0 for real pipeline runs.

-- 2) Attribution overwrite contract: BRIEF:as_of_ts at root, BRIEF:attribution:as_of_ts absent. Fail if bad_rows > 0.
select count(*) as bad_rows
from MIP.AGENT_OUT.MORNING_BRIEF
where run_id = $run_id
  and agent_name = 'MORNING_BRIEF'
  and (
    brief:"as_of_ts" is null
    or brief:"attribution":"as_of_ts" is not null
  );
-- Expect bad_rows = 0; smoke fails if bad_rows > 0.
