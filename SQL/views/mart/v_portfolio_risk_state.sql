-- v_portfolio_risk_state.sql
-- Purpose: Centralized portfolio risk gating state for entry permissions

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_PORTFOLIO_RISK_STATE as
with profile_actions as (
    select
        p.PORTFOLIO_ID,
        p.BUST_AT,
        coalesce(prof.BUST_ACTION, 'ALLOW_EXITS_ONLY') as BUST_ACTION
    from MIP.APP.PORTFOLIO p
    left join MIP.APP.PORTFOLIO_PROFILE prof
      on prof.PROFILE_ID = p.PROFILE_ID
)
select
    g.PORTFOLIO_ID,
    g.LATEST_RUN_ID as RUN_ID,
    g.AS_OF_TS,
    g.CURRENT_BAR_INDEX,
    g.OPEN_POSITIONS,
    g.DRAWDOWN_STOP_TS,
    g.FIRST_FLAT_NO_POSITIONS_TS,
    g.DRAWDOWN_STOP_PCT,
    g.MAX_DRAWDOWN,
    iff(
        coalesce(g.ENTRIES_BLOCKED, false)
        or (pa.BUST_AT is not null and pa.BUST_ACTION = 'ALLOW_EXITS_ONLY'),
        true,
        false
    ) as ENTRIES_BLOCKED,
    case
        when coalesce(g.ENTRIES_BLOCKED, false)
          or (pa.BUST_AT is not null and pa.BUST_ACTION = 'ALLOW_EXITS_ONLY')
        then 'ALLOW_EXITS_ONLY'
        else 'ALLOW_ENTRIES'
    end as ALLOWED_ACTIONS,
    case
        when coalesce(g.ENTRIES_BLOCKED, false) then g.BLOCK_REASON
        when pa.BUST_AT is not null and pa.BUST_ACTION = 'ALLOW_EXITS_ONLY' then 'ALLOW_EXITS_ONLY'
        else null
    end as STOP_REASON,
    g.BLOCK_REASON,
    g.RISK_STATUS
from MIP.MART.V_PORTFOLIO_RISK_GATE g
left join profile_actions pa
  on pa.PORTFOLIO_ID = g.PORTFOLIO_ID;
