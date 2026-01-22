-- v_agent_daily_risk_brief.sql
-- Purpose: Daily portfolio risk flags and drawdown stops

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_AGENT_DAILY_RISK_BRIEF as
with base as (
    select *
    from MIP.MART.V_AGENT_PORTFOLIO_RISK_BRIEF
)
select
    b.PORTFOLIO_ID,
    b.RUN_ID,
    b.FROM_TS,
    b.TO_TS,
    b.TRADING_DAYS,
    b.STARTING_CASH,
    b.FINAL_EQUITY,
    b.TOTAL_RETURN,
    b.MAX_DRAWDOWN,
    b.PEAK_EQUITY,
    b.MIN_EQUITY,
    b.DAILY_VOLATILITY,
    b.AVG_DAILY_RETURN,
    b.AVG_MARKET_RETURN,
    b.AVG_EQ_RETURN,
    b.MAX_MARKET_RETURN_RAW,
    b.MAX_MARKET_RETURN,
    b.AVG_RETURN_RECON_DIFF,
    b.WIN_DAYS,
    b.LOSS_DAYS,
    b.AVG_WIN_PNL,
    b.AVG_LOSS_PNL,
    b.AVG_OPEN_POSITIONS,
    b.TIME_IN_MARKET,
    b.DRAWDOWN_STOP_TS,
    b.FIRST_FLAT_NO_POSITIONS_TS,
    b.STOP_REASON,
    b.DRAWDOWN_STOP_PCT,
    b.RISK_STATUS,
    'RISK_STATUS' as BRIEF_CATEGORY,
    b.AS_OF_TS
from base b
where b.RISK_STATUS != 'OK'

union all

select
    b.PORTFOLIO_ID,
    b.RUN_ID,
    b.FROM_TS,
    b.TO_TS,
    b.TRADING_DAYS,
    b.STARTING_CASH,
    b.FINAL_EQUITY,
    b.TOTAL_RETURN,
    b.MAX_DRAWDOWN,
    b.PEAK_EQUITY,
    b.MIN_EQUITY,
    b.DAILY_VOLATILITY,
    b.AVG_DAILY_RETURN,
    b.AVG_MARKET_RETURN,
    b.AVG_EQ_RETURN,
    b.MAX_MARKET_RETURN_RAW,
    b.MAX_MARKET_RETURN,
    b.AVG_RETURN_RECON_DIFF,
    b.WIN_DAYS,
    b.LOSS_DAYS,
    b.AVG_WIN_PNL,
    b.AVG_LOSS_PNL,
    b.AVG_OPEN_POSITIONS,
    b.TIME_IN_MARKET,
    b.DRAWDOWN_STOP_TS,
    b.FIRST_FLAT_NO_POSITIONS_TS,
    b.STOP_REASON,
    b.DRAWDOWN_STOP_PCT,
    b.RISK_STATUS,
    'RECENT_DRAWDOWN_STOP' as BRIEF_CATEGORY,
    b.AS_OF_TS
from base b
where b.DRAWDOWN_STOP_TS >= dateadd(day, -1, current_timestamp());
