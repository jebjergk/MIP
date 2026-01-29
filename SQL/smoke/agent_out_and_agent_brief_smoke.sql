-- agent_out_and_agent_brief_smoke.sql
-- [A1] SHOW TABLES in MIP.AGENT_OUT returns AGENT_MORNING_BRIEF, AGENT_RUN_LOG.
-- [A2] Call SP_AGENT_GENERATE_MORNING_BRIEF; verify one row and JSON shape.
-- [P1] Determinism: run twice for same (as_of_ts, signal_run_id); assert same row key (updated not duplicated), JSON stable.

use role MIP_ADMIN_ROLE;
use database MIP;

-- A1: SHOW TABLES returns expected tables
show tables in schema MIP.AGENT_OUT;

-- A2: Call procedure with known as_of_ts and signal_run_id; verify return and one row
call MIP.APP.SP_AGENT_GENERATE_MORNING_BRIEF(current_timestamp()::timestamp_ntz, 0);

-- Verify latest row: STATUS, header, system_status, training_summary, candidate_summary, contract fields
select
    BRIEF_ID,
    AS_OF_TS,
    SIGNAL_RUN_ID,
    AGENT_NAME,
    STATUS,
    BRIEF_JSON:header:as_of_ts::timestamp_ntz as header_as_of_ts,
    BRIEF_JSON:system_status:has_new_bars::boolean as has_new_bars,
    BRIEF_JSON:system_status:fallback_used::boolean as fallback_used,
    array_size(BRIEF_JSON:training_summary::array) as training_count,
    BRIEF_JSON:candidate_summary:reason::string as candidate_reason,
    BRIEF_JSON:candidate_summary:diagnostics as candidate_diagnostics,
    BRIEF_JSON:assumptions:min_n_signals::number as min_n_signals,
    BRIEF_JSON:interpretation_bullets::array as interpretation_bullets,
    BRIEF_JSON:data_lineage:source_views::array as source_views,
    CREATED_AT
from MIP.AGENT_OUT.AGENT_MORNING_BRIEF
order by CREATED_AT desc
limit 1;

-- P1 Determinism: run SP twice for same (as_of_ts, signal_run_id); assert same row key, updated not duplicated, JSON stable
set smoke_ts = '2020-01-01 00:00:00'::timestamp_ntz;
set smoke_run_id = 999;

-- First run
call MIP.APP.SP_AGENT_GENERATE_MORNING_BRIEF($smoke_ts, $smoke_run_id);

-- Capture stable parts of BRIEF_JSON for later comparison (exclude generated_at)
create or replace temporary table MIP.APP.SMOKE_FIRST_BRIEF as
select
    b.BRIEF_JSON:training_summary as training_summary,
    b.BRIEF_JSON:candidate_summary as candidate_summary,
    hash(b.BRIEF_JSON:training_summary) as h_training,
    hash(b.BRIEF_JSON:candidate_summary) as h_candidate
from MIP.AGENT_OUT.AGENT_MORNING_BRIEF b
where b.AS_OF_TS = $smoke_ts and b.SIGNAL_RUN_ID = $smoke_run_id and b.AGENT_NAME = 'AGENT_V0_MORNING_BRIEF';

-- Second run: same key -> update same row (no duplicate)
call MIP.APP.SP_AGENT_GENERATE_MORNING_BRIEF($smoke_ts, $smoke_run_id);

-- Assert: exactly one row for key (updated not duplicated)
select count(*) as row_count_after_upsert
from MIP.AGENT_OUT.AGENT_MORNING_BRIEF
where AS_OF_TS = $smoke_ts and SIGNAL_RUN_ID = $smoke_run_id and AGENT_NAME = 'AGENT_V0_MORNING_BRIEF';
-- Expect row_count_after_upsert = 1

-- Assert: JSON stable (same training_summary and candidate_summary hash across reruns)
select 'PASS' as determinism_json_stable
from MIP.AGENT_OUT.AGENT_MORNING_BRIEF b
cross join MIP.APP.SMOKE_FIRST_BRIEF f
where b.AS_OF_TS = $smoke_ts and b.SIGNAL_RUN_ID = $smoke_run_id and b.AGENT_NAME = 'AGENT_V0_MORNING_BRIEF'
  and hash(b.BRIEF_JSON:training_summary) = f.h_training
  and hash(b.BRIEF_JSON:candidate_summary) = f.h_candidate;
-- Expect one row: PASS

-- P1: AGENT_RUN_LOG has success row with training_count, candidate_count
select RUN_ID, AGENT_NAME, STATUS, OUTPUTS_JSON:training_count::number as training_count, OUTPUTS_JSON:candidate_count::number as candidate_count
from MIP.AGENT_OUT.AGENT_RUN_LOG
where AS_OF_TS = $smoke_ts and SIGNAL_RUN_ID = $smoke_run_id and STATUS = 'SUCCESS'
order by CREATED_AT desc
limit 1;
