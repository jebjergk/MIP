-- v_agent_portfolio_risk_brief.sql
-- Purpose: Agent-ready portfolio risk summary with drawdown status

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_AGENT_PORTFOLIO_RISK_BRIEF as
select
    k.PORTFOLIO_ID,
    k.RUN_ID,
    k.FROM_TS,
    k.TO_TS,
    k.TRADING_DAYS,
    k.STARTING_CASH,
    k.FINAL_EQUITY,
    k.TOTAL_RETURN,
    k.MAX_DRAWDOWN,
    k.PEAK_EQUITY,
    k.MIN_EQUITY,
    k.DAILY_VOLATILITY,
    k.AVG_DAILY_RETURN,
    k.AVG_MARKET_RETURN,
    k.AVG_EQ_RETURN,
    k.MAX_MARKET_RETURN_RAW,
    k.MAX_MARKET_RETURN,
    k.AVG_RETURN_RECON_DIFF,
    k.WIN_DAYS,
    k.LOSS_DAYS,
    k.AVG_WIN_PNL,
    k.AVG_LOSS_PNL,
    k.AVG_OPEN_POSITIONS,
    k.TIME_IN_MARKET,
    k.DRAWDOWN_STOP_TS,
    e.FIRST_FLAT_NO_POSITIONS_TS,
    e.STOP_REASON,
    coalesce(prof.DRAWDOWN_STOP_PCT, 0.10) as DRAWDOWN_STOP_PCT,
    case
        when k.MAX_DRAWDOWN is null then 'WARN'
        when k.MAX_DRAWDOWN >= coalesce(prof.DRAWDOWN_STOP_PCT, 0.10) then 'WARN'
        else 'OK'
    end as RISK_STATUS,
    current_timestamp() as AS_OF_TS
from MIP.MART.V_PORTFOLIO_RUN_KPIS k
left join MIP.MART.V_PORTFOLIO_RUN_EVENTS e
  on e.PORTFOLIO_ID = k.PORTFOLIO_ID
 and e.RUN_ID = k.RUN_ID
left join MIP.APP.PORTFOLIO p
  on p.PORTFOLIO_ID = k.PORTFOLIO_ID
left join MIP.APP.PORTFOLIO_PROFILE prof
  on prof.PROFILE_ID = p.PROFILE_ID;
