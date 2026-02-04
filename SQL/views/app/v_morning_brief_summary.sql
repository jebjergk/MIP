-- v_morning_brief_summary.sql
-- Purpose: Summary view over MORNING_BRIEF for Streamlit/ops without inspecting JSON.
-- Note: Use CREATED_AT (not AS_OF_TS) for "latest brief" selection.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.AGENT_OUT.V_MORNING_BRIEF_SUMMARY (
    PORTFOLIO_ID,
    AS_OF_TS,
    CREATED_AT,
    RUN_ID,
    AGENT_NAME,
    PIPELINE_RUN_ID,
    ENTRIES_BLOCKED,
    RISK_STATUS,
    PROPOSALS_COUNT,
    SIGNALS_COUNT
) as
select
    mb.PORTFOLIO_ID,
    mb.AS_OF_TS,
    coalesce(mb.CREATED_AT, mb.AS_OF_TS) as CREATED_AT,
    mb.RUN_ID,
    coalesce(mb.AGENT_NAME, '') as AGENT_NAME,
    mb.BRIEF:pipeline_run_id::string as PIPELINE_RUN_ID,
    g.ENTRIES_BLOCKED,
    coalesce(mb.BRIEF:risk:latest:risk_status::string, g.RISK_STATUS) as RISK_STATUS,
    mb.BRIEF:proposals:summary:total::number as PROPOSALS_COUNT,
    coalesce(array_size(mb.BRIEF:signals:trusted_now), 0) + coalesce(array_size(mb.BRIEF:signals:watch_negative), 0) as SIGNALS_COUNT
from MIP.AGENT_OUT.MORNING_BRIEF mb
left join MIP.MART.V_PORTFOLIO_RISK_GATE g on g.PORTFOLIO_ID = mb.PORTFOLIO_ID;
