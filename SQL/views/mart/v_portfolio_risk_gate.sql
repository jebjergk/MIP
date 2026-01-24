-- v_portfolio_risk_gate.sql
-- Purpose: Canonical entry gate based on latest portfolio simulation KPIs and open positions

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_PORTFOLIO_RISK_GATE as
with latest_run as (
    select
        PORTFOLIO_ID,
        RUN_ID,
        TO_TS,
        row_number() over (
            partition by PORTFOLIO_ID
            order by TO_TS desc
        ) as RN
    from MIP.MART.V_PORTFOLIO_RUN_KPIS
),
latest_kpis as (
    select
        k.PORTFOLIO_ID,
        k.RUN_ID,
        k.TO_TS,
        k.MAX_DRAWDOWN
    from MIP.MART.V_PORTFOLIO_RUN_KPIS k
    join latest_run lr
      on lr.PORTFOLIO_ID = k.PORTFOLIO_ID
     and lr.RUN_ID = k.RUN_ID
    where lr.RN = 1
),
run_events as (
    select
        e.PORTFOLIO_ID,
        e.RUN_ID,
        e.DRAWDOWN_STOP_TS,
        e.FIRST_FLAT_NO_POSITIONS_TS
    from MIP.MART.V_PORTFOLIO_RUN_EVENTS e
    join latest_run lr
      on lr.PORTFOLIO_ID = e.PORTFOLIO_ID
     and lr.RUN_ID = e.RUN_ID
    where lr.RN = 1
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
    from MIP.MART.V_PORTFOLIO_OPEN_POSITIONS
    group by PORTFOLIO_ID
)
select
    p.PORTFOLIO_ID,
    lk.RUN_ID as LATEST_RUN_ID,
    op.AS_OF_TS,
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
            or re.FIRST_FLAT_NO_POSITIONS_TS > op.AS_OF_TS
        )
    ) as ENTRIES_BLOCKED,
    case
        when re.DRAWDOWN_STOP_TS is not null
         and coalesce(op.OPEN_POSITIONS, 0) > 0
         and (
             re.FIRST_FLAT_NO_POSITIONS_TS is null
             or re.FIRST_FLAT_NO_POSITIONS_TS > op.AS_OF_TS
         )
        then 'DRAWDOWN_STOP_ACTIVE'
        else null
    end as BLOCK_REASON,
    case
        when lk.MAX_DRAWDOWN is null then 'WARN'
        when lk.MAX_DRAWDOWN >= p.DRAWDOWN_STOP_PCT then 'WARN'
        else 'OK'
    end as RISK_STATUS
from profiles p
left join latest_kpis lk
  on lk.PORTFOLIO_ID = p.PORTFOLIO_ID
left join run_events re
  on re.PORTFOLIO_ID = p.PORTFOLIO_ID
 and re.RUN_ID = lk.RUN_ID
left join open_positions op
  on op.PORTFOLIO_ID = p.PORTFOLIO_ID;
