-- v_portfolio_run_events.sql
-- Purpose: Run-level portfolio stop/event markers

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_PORTFOLIO_RUN_EVENTS as
with daily as (
    select
        d.PORTFOLIO_ID,
        d.RUN_ID,
        d.TS,
        d.OPEN_POSITIONS,
        d.DRAWDOWN,
        coalesce(prof.DRAWDOWN_STOP_PCT, 0.10) as DRAWDOWN_STOP_PCT
    from MIP.APP.PORTFOLIO_DAILY d
    join MIP.APP.PORTFOLIO p
      on p.PORTFOLIO_ID = d.PORTFOLIO_ID
    left join MIP.APP.V_PORTFOLIO_ACTIVE_EPISODE e
      on e.PORTFOLIO_ID = d.PORTFOLIO_ID
    left join MIP.APP.PORTFOLIO_PROFILE prof
      on prof.PROFILE_ID = p.PROFILE_ID
    where e.EPISODE_ID is null or d.TS >= e.START_TS
),
stop_events as (
    select
        PORTFOLIO_ID,
        RUN_ID,
        min(case when DRAWDOWN >= DRAWDOWN_STOP_PCT then TS end) as DRAWDOWN_STOP_TS
    from daily
    group by
        PORTFOLIO_ID,
        RUN_ID
)
select
    s.PORTFOLIO_ID,
    s.RUN_ID,
    s.DRAWDOWN_STOP_TS,
    min(case
        when s.DRAWDOWN_STOP_TS is not null
         and d.OPEN_POSITIONS = 0
         and d.TS > s.DRAWDOWN_STOP_TS
        then d.TS
    end) as FIRST_FLAT_NO_POSITIONS_TS,
    case
        when s.DRAWDOWN_STOP_TS is not null then 'DRAWDOWN_STOP'
        else null
    end as STOP_REASON
from stop_events s
left join daily d
  on d.PORTFOLIO_ID = s.PORTFOLIO_ID
 and d.RUN_ID = s.RUN_ID
group by
    s.PORTFOLIO_ID,
    s.RUN_ID,
    s.DRAWDOWN_STOP_TS;
