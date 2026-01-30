-- morning_brief_validity_smoke.sql
-- Assert no bad brief rows for a pipeline run: portfolio_id > 0, pipeline_run_id and as_of_ts present in BRIEF.

use role MIP_ADMIN_ROLE;
use database MIP;

-- Set run_id to the pipeline run to check, e.g. from MIP_AUDIT_LOG or last SP_RUN_DAILY_PIPELINE result.
-- Example: set run_id = 'your-run-id';
select count(*) as bad
from MIP.AGENT_OUT.MORNING_BRIEF
where run_id = $run_id
  and (
    portfolio_id <= 0
    or brief:"pipeline_run_id" is null
    or coalesce(brief:"as_of_ts", brief:"attribution":"as_of_ts") is null
  );
-- Expect bad = 0 for real pipeline runs.
