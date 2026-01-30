-- morning_brief_attribution_smoke.sql
-- Assert that newly written briefs do not have attribution:latest_run_id (null or absent).

use role MIP_ADMIN_ROLE;
use database MIP;

-- For a given run_id (set after SP_WRITE_MORNING_BRIEF), expect 0 rows where attribution has latest_run_id.
-- Example: set run_id = 'your-run-id'; then run the query below.
-- Or use recent rows: briefs written in the last hour.
select
    'NO_LATEST_RUN_ID_IN_ATTRIBUTION' as smoke_check,
    count(*) as rows_with_latest_run_id
from MIP.AGENT_OUT.MORNING_BRIEF
where (run_id = $run_id or ($run_id is null and created_at >= dateadd('hour', -1, current_timestamp())))
  and brief:attribution:latest_run_id is not null;
-- Expect rows_with_latest_run_id = 0 for new briefs.
