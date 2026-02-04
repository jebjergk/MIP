-- agent_smoke.sql
-- [A5] One-liner usage: run in Snowflake worksheet to populate and inspect agent morning brief.
-- Usage: execute full script (determines as_of_ts and signal_run_id, calls proc, shows last 5 briefs).

use role MIP_ADMIN_ROLE;
use database MIP;

-- Determine latest as_of_ts from market bars (or use effective_to_ts from pipeline if preferred)
set as_of_ts = (select max(TS)::timestamp_ntz from MIP.MART.MARKET_BARS limit 1);

-- Determine a recent signal_run_id (or use a literal, e.g. 0)
set signal_run_id = (
    select coalesce(max(try_to_number(replace(to_varchar(RUN_ID), 'T', ''))), 0)
    from MIP.APP.V_SIGNALS_ELIGIBLE_TODAY
    limit 1
);

-- Call agent morning brief
call MIP.APP.SP_AGENT_GENERATE_MORNING_BRIEF($as_of_ts, $signal_run_id);

-- Show last 5 agent briefs (MORNING_BRIEF with PORTFOLIO_ID=0; BRIEF has { status, agent_name, brief })
select
    BRIEF_ID,
    AS_OF_TS,
    RUN_ID,
    BRIEF:agent_name::string as agent_name,
    BRIEF:status::string as status,
    BRIEF:brief:system_status:as_of_ts::timestamp_ntz as system_as_of_ts,
    array_size(BRIEF:brief:training_summary::array) as training_count,
    BRIEF:brief:candidate_summary:reason::string as candidate_reason
from MIP.AGENT_OUT.MORNING_BRIEF
where PORTFOLIO_ID = 0
order by AS_OF_TS desc
limit 5;
