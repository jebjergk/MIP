-- agent_out_and_agent_brief_smoke.sql
-- [A1] SHOW TABLES in MIP.AGENT_OUT returns MORNING_BRIEF, AGENT_RUN_LOG.
-- [A2] Call SP_AGENT_GENERATE_MORNING_BRIEF; verify one row and JSON shape (agent briefs in MORNING_BRIEF with PORTFOLIO_ID=0).
-- [P1] Determinism: run twice for same (as_of_ts, signal_run_id); assert same row key (updated not duplicated), JSON stable.

use role MIP_ADMIN_ROLE;
use database MIP;

-- A1: SHOW TABLES returns expected tables
show tables in schema MIP.AGENT_OUT;

-- A2: Call procedure with known as_of_ts and signal_run_id; verify return and one row
call MIP.APP.SP_AGENT_GENERATE_MORNING_BRIEF(current_timestamp()::timestamp_ntz, 0);

-- Verify latest agent brief row (MORNING_BRIEF with PORTFOLIO_ID=0; BRIEF has { status, agent_name, brief })
select
    BRIEF_ID,
    AS_OF_TS,
    RUN_ID,
    BRIEF:status::string as status,
    BRIEF:agent_name::string as agent_name,
    BRIEF:brief:header:as_of_ts::timestamp_ntz as header_as_of_ts,
    BRIEF:brief:system_status:has_new_bars::boolean as has_new_bars,
    BRIEF:brief:system_status:fallback_used::boolean as fallback_used,
    array_size(BRIEF:brief:training_summary::array) as training_count,
    BRIEF:brief:candidate_summary:reason::string as candidate_reason,
    BRIEF:brief:candidate_summary:diagnostics as candidate_diagnostics,
    BRIEF:brief:assumptions:min_n_signals::number as min_n_signals,
    BRIEF:brief:interpretation_bullets::array as interpretation_bullets,
    BRIEF:brief:data_lineage:source_views::array as source_views
from MIP.AGENT_OUT.MORNING_BRIEF
where PORTFOLIO_ID = 0
order by AS_OF_TS desc
limit 1;

-- P1 Determinism: run SP twice for same (as_of_ts, signal_run_id); assert same row key, updated not duplicated, JSON stable
set smoke_ts = '2020-01-01 00:00:00'::timestamp_ntz;
set smoke_run_id = 999;
-- Key must match SP_AGENT_GENERATE_MORNING_BRIEF: agent_name || '_' || to_varchar(as_of_ts, 'YYYY-MM-DD"T"HH24:MI:SS.FF3') || '_' || to_varchar(signal_run_id)
set smoke_run_id_key = (select 'AGENT_V0_MORNING_BRIEF_' || to_varchar($smoke_ts, 'YYYY-MM-DD"T"HH24:MI:SS.FF3') || '_' || to_varchar($smoke_run_id));

-- First run
call MIP.APP.SP_AGENT_GENERATE_MORNING_BRIEF($smoke_ts, $smoke_run_id);

-- Capture stable parts of BRIEF.brief for later comparison (exclude generated_at)
create or replace temporary table MIP.APP.SMOKE_FIRST_BRIEF as
select
    b.BRIEF:brief:training_summary as training_summary,
    b.BRIEF:brief:candidate_summary as candidate_summary,
    hash(b.BRIEF:brief:training_summary) as h_training,
    hash(b.BRIEF:brief:candidate_summary) as h_candidate
from MIP.AGENT_OUT.MORNING_BRIEF b
where b.PORTFOLIO_ID = 0 and b.RUN_ID = $smoke_run_id_key;

-- Second run: same key -> update same row (no duplicate)
call MIP.APP.SP_AGENT_GENERATE_MORNING_BRIEF($smoke_ts, $smoke_run_id);

-- Assert: exactly one row for key (updated not duplicated)
select count(*) as row_count_after_upsert
from MIP.AGENT_OUT.MORNING_BRIEF
where PORTFOLIO_ID = 0 and RUN_ID = $smoke_run_id_key;
-- Expect row_count_after_upsert = 1

-- Assert: JSON stable (same training_summary and candidate_summary hash across reruns)
select 'PASS' as determinism_json_stable
from MIP.AGENT_OUT.MORNING_BRIEF b
cross join MIP.APP.SMOKE_FIRST_BRIEF f
where b.PORTFOLIO_ID = 0 and b.RUN_ID = $smoke_run_id_key
  and hash(b.BRIEF:brief:training_summary) = f.h_training
  and hash(b.BRIEF:brief:candidate_summary) = f.h_candidate;
-- Expect one row: PASS

-- P1: AGENT_RUN_LOG has success row with training_count, candidate_count
select RUN_ID, AGENT_NAME, STATUS, OUTPUTS_JSON:training_count::number as training_count, OUTPUTS_JSON:candidate_count::number as candidate_count
from MIP.AGENT_OUT.AGENT_RUN_LOG
where AS_OF_TS = $smoke_ts and SIGNAL_RUN_ID = $smoke_run_id and STATUS = 'SUCCESS'
order by CREATED_AT desc
limit 1;
