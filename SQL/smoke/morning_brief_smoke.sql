-- morning_brief_smoke.sql
-- Smoke tests for V_MORNING_BRIEF_JSON

select
    AS_OF_TS,
    BRIEF:signals:trusted_now[0] as TRUSTED_NOW_SAMPLE,
    BRIEF:risk:latest:risk_status as RISK_STATUS,
    BRIEF:attribution:by_market_type[0]:market_type as ATTRIBUTION_MARKET_TYPE
from MIP.MART.V_MORNING_BRIEF_JSON;

select count(*) as RUN_ID_MISMATCH_COUNT
from MIP.MART.V_MORNING_BRIEF_JSON
where BRIEF:risk:latest:run_id::string <> BRIEF:attribution:latest_run_id::string;

select count(*) as ROW_COUNT
from MIP.MART.V_MORNING_BRIEF_JSON;
