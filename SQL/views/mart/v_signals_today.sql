-- v_signals_today.sql
-- Purpose: MART wrapper view for dashboards consuming today's eligible signals

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_SIGNALS_TODAY as
select
    RUN_ID,
    TS as SIGNAL_TS,
    SYMBOL,
    MARKET_TYPE,
    INTERVAL_MINUTES,
    PATTERN_ID,
    SCORE,
    TRUST_LABEL,
    RECOMMENDED_ACTION,
    IS_ELIGIBLE,
    GATING_REASON
from MIP.APP.V_SIGNALS_ELIGIBLE_TODAY;
