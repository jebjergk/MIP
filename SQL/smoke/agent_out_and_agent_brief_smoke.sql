-- agent_out_and_agent_brief_smoke.sql
-- [A1] SHOW TABLES in MIP.AGENT_OUT returns AGENT_MORNING_BRIEF, AGENT_RUN_LOG (and existing MORNING_BRIEF, ORDER_PROPOSALS).
-- [A2] Insert/select smoke: call SP_AGENT_GENERATE_MORNING_BRIEF, verify one row and JSON shape.

use role MIP_ADMIN_ROLE;
use database MIP;

-- A1: SHOW TABLES returns expected tables
show tables in schema MIP.AGENT_OUT;

-- A1/A2: Call procedure with known as_of_ts and signal_run_id; verify return and one row
-- Use a fixed signal_run_id (e.g. 0) so smoke works even with no recommendations
call MIP.APP.SP_AGENT_GENERATE_MORNING_BRIEF(current_timestamp()::timestamp_ntz, 0);

-- Verify latest row: STATUS, header, system_status, training_summary, candidate_summary, explainability
select
    BRIEF_ID,
    AS_OF_TS,
    SIGNAL_RUN_ID,
    AGENT_NAME,
    STATUS,
    BRIEF_JSON:header:as_of_ts::timestamp_ntz as header_as_of_ts,
    BRIEF_JSON:system_status:has_new_bars::boolean as has_new_bars,
    array_size(BRIEF_JSON:training_summary::array) as training_count,
    BRIEF_JSON:candidate_summary:reason::string as candidate_reason,
    BRIEF_JSON:assumptions:min_n_signals::number as min_n_signals,
    BRIEF_JSON:data_lineage:source_views::array as source_views,
    CREATED_AT
from MIP.AGENT_OUT.AGENT_MORNING_BRIEF
order by CREATED_AT desc
limit 1;

-- Upsert: run again for same (as_of_ts, signal_run_id) -> still one row (delete+insert)
-- Use a deterministic as_of_ts so second call matches
set smoke_ts = '2020-01-01 00:00:00'::timestamp_ntz;
call MIP.APP.SP_AGENT_GENERATE_MORNING_BRIEF($smoke_ts, 999);
call MIP.APP.SP_AGENT_GENERATE_MORNING_BRIEF($smoke_ts, 999);
select count(*) as row_count_after_upsert
from MIP.AGENT_OUT.AGENT_MORNING_BRIEF
where AS_OF_TS = $smoke_ts and SIGNAL_RUN_ID = 999 and AGENT_NAME = 'AGENT_V0_MORNING_BRIEF';
-- Expect row_count_after_upsert = 1
