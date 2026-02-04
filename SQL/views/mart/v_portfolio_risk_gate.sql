-- v_portfolio_risk_gate.sql
-- Purpose: Canonical entry gate based on latest portfolio simulation KPIs and open positions
--
-- Run anchor priority (robust fallback when PORTFOLIO_DAILY is empty after reset):
--   1. PORTFOLIO.LAST_SIMULATION_RUN_ID (if not null)
--   2. Latest RUN_ID from PORTFOLIO_DAILY by TS
--   3. Latest RUN_ID from PORTFOLIO_TRADES by TRADE_TS
--   4. Latest RUN_ID from PORTFOLIO_POSITIONS by ENTRY_TS

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_PORTFOLIO_RISK_GATE as
with run_anchor as (
    -- Robust run anchor: try multiple sources in priority order
    select
        p.PORTFOLIO_ID,
        coalesce(
            -- 1. Portfolio's last simulation run (most authoritative)
            p.LAST_SIMULATION_RUN_ID,
            -- 2. Latest from PORTFOLIO_DAILY
            (select RUN_ID from MIP.APP.PORTFOLIO_DAILY d 
             where d.PORTFOLIO_ID = p.PORTFOLIO_ID 
             order by d.TS desc limit 1),
            -- 3. Latest from PORTFOLIO_TRADES
            (select RUN_ID from MIP.APP.PORTFOLIO_TRADES t 
             where t.PORTFOLIO_ID = p.PORTFOLIO_ID 
             order by t.TRADE_TS desc limit 1),
            -- 4. Latest from PORTFOLIO_POSITIONS
            (select RUN_ID from MIP.APP.PORTFOLIO_POSITIONS pos 
             where pos.PORTFOLIO_ID = p.PORTFOLIO_ID 
             order by pos.ENTRY_TS desc limit 1)
        ) as LATEST_RUN_ID,
        coalesce(
            p.LAST_SIMULATED_AT,
            (select max(TS) from MIP.APP.PORTFOLIO_DAILY d where d.PORTFOLIO_ID = p.PORTFOLIO_ID),
            (select max(TRADE_TS) from MIP.APP.PORTFOLIO_TRADES t where t.PORTFOLIO_ID = p.PORTFOLIO_ID),
            (select max(ENTRY_TS) from MIP.APP.PORTFOLIO_POSITIONS pos where pos.PORTFOLIO_ID = p.PORTFOLIO_ID)
        ) as AS_OF_TS
    from MIP.APP.PORTFOLIO p
),
latest_kpis as (
    select
        k.PORTFOLIO_ID,
        k.RUN_ID,
        k.TO_TS,
        k.MAX_DRAWDOWN
    from MIP.MART.V_PORTFOLIO_RUN_KPIS k
    join run_anchor ra
      on ra.PORTFOLIO_ID = k.PORTFOLIO_ID
     and ra.LATEST_RUN_ID = k.RUN_ID
),
run_events as (
    select
        e.PORTFOLIO_ID,
        e.RUN_ID,
        e.DRAWDOWN_STOP_TS,
        e.FIRST_FLAT_NO_POSITIONS_TS
    from MIP.MART.V_PORTFOLIO_RUN_EVENTS e
    join run_anchor ra
      on ra.PORTFOLIO_ID = e.PORTFOLIO_ID
     and ra.LATEST_RUN_ID = e.RUN_ID
),
profiles as (
    select
        p.PORTFOLIO_ID,
        coalesce(prof.DRAWDOWN_STOP_PCT, 0.10) as DRAWDOWN_STOP_PCT
    from MIP.APP.PORTFOLIO p
    left join MIP.APP.PORTFOLIO_PROFILE prof
      on prof.PROFILE_ID = p.PROFILE_ID
),
open_positions as (
    select
        PORTFOLIO_ID,
        max(AS_OF_TS) as AS_OF_TS,
        max(CURRENT_BAR_INDEX) as CURRENT_BAR_INDEX,
        max(OPEN_POSITIONS) as OPEN_POSITIONS
    from MIP.MART.V_PORTFOLIO_OPEN_POSITIONS_CANONICAL
    group by PORTFOLIO_ID
)
select
    p.PORTFOLIO_ID,
    coalesce(ra.LATEST_RUN_ID, lk.RUN_ID) as LATEST_RUN_ID,
    coalesce(op.AS_OF_TS, ra.AS_OF_TS) as AS_OF_TS,
    op.CURRENT_BAR_INDEX,
    coalesce(op.OPEN_POSITIONS, 0) as OPEN_POSITIONS,
    re.DRAWDOWN_STOP_TS,
    re.FIRST_FLAT_NO_POSITIONS_TS,
    p.DRAWDOWN_STOP_PCT,
    lk.MAX_DRAWDOWN,
    (
        re.DRAWDOWN_STOP_TS is not null
        and coalesce(op.OPEN_POSITIONS, 0) > 0
        and (
            re.FIRST_FLAT_NO_POSITIONS_TS is null
            or re.FIRST_FLAT_NO_POSITIONS_TS > coalesce(op.AS_OF_TS, ra.AS_OF_TS)
        )
    ) as ENTRIES_BLOCKED,
    case
        when re.DRAWDOWN_STOP_TS is not null
         and coalesce(op.OPEN_POSITIONS, 0) > 0
         and (
             re.FIRST_FLAT_NO_POSITIONS_TS is null
             or re.FIRST_FLAT_NO_POSITIONS_TS > coalesce(op.AS_OF_TS, ra.AS_OF_TS)
         )
        then 'DRAWDOWN_STOP_ACTIVE'
        else null
    end as BLOCK_REASON,
    case
        when lk.MAX_DRAWDOWN is null then 'OK'   /* no run data (e.g. after reset) = no drawdown yet */
        when lk.MAX_DRAWDOWN >= p.DRAWDOWN_STOP_PCT then 'WARN'
        else 'OK'
    end as RISK_STATUS
from profiles p
left join run_anchor ra
  on ra.PORTFOLIO_ID = p.PORTFOLIO_ID
left join latest_kpis lk
  on lk.PORTFOLIO_ID = p.PORTFOLIO_ID
left join run_events re
  on re.PORTFOLIO_ID = p.PORTFOLIO_ID
 and re.RUN_ID = coalesce(ra.LATEST_RUN_ID, lk.RUN_ID)
left join open_positions op
  on op.PORTFOLIO_ID = p.PORTFOLIO_ID;

  grant select on view MIP.MART.V_PORTFOLIO_RISK_GATE to role MIP_UI_API_ROLE;

