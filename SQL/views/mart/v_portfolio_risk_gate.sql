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
with latest_daily as (
    -- Latest PORTFOLIO_DAILY row per portfolio (avoids correlated subquery)
    select PORTFOLIO_ID, RUN_ID, TS
    from MIP.APP.PORTFOLIO_DAILY
    qualify row_number() over (partition by PORTFOLIO_ID order by TS desc) = 1
),
latest_trade as (
    -- Latest PORTFOLIO_TRADES row per portfolio
    select PORTFOLIO_ID, RUN_ID, TRADE_TS
    from MIP.APP.PORTFOLIO_TRADES
    qualify row_number() over (partition by PORTFOLIO_ID order by TRADE_TS desc) = 1
),
latest_position as (
    -- Latest PORTFOLIO_POSITIONS row per portfolio
    select PORTFOLIO_ID, RUN_ID, ENTRY_TS
    from MIP.APP.PORTFOLIO_POSITIONS
    qualify row_number() over (partition by PORTFOLIO_ID order by ENTRY_TS desc) = 1
),
run_anchor as (
    -- Robust run anchor: try multiple sources in priority order via LEFT JOINs
    select
        p.PORTFOLIO_ID,
        coalesce(p.LAST_SIMULATION_RUN_ID, ld.RUN_ID, lt.RUN_ID, lp.RUN_ID) as LATEST_RUN_ID,
        coalesce(p.LAST_SIMULATED_AT, ld.TS, lt.TRADE_TS, lp.ENTRY_TS) as AS_OF_TS
    from MIP.APP.PORTFOLIO p
    left join latest_daily    ld on ld.PORTFOLIO_ID = p.PORTFOLIO_ID
    left join latest_trade    lt on lt.PORTFOLIO_ID = p.PORTFOLIO_ID
    left join latest_position lp on lp.PORTFOLIO_ID = p.PORTFOLIO_ID
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

