-- v_parallel_world_actual.sql
-- Purpose: Derives ACTUAL-world baseline metrics per portfolio-day from existing tables.
-- Produces the same metric shape as PARALLEL_WORLD_RESULT for join compatibility
-- with the diff view. Episode-scoped to the active episode.
--
-- Sources:
--   MIP.APP.PORTFOLIO_DAILY     — daily equity snapshots (cash, equity, drawdown)
--   MIP.APP.PORTFOLIO_TRADES    — trade counts and realized PnL
--   MIP.APP.PORTFOLIO           — starting cash and profile
--   MIP.APP.PORTFOLIO_EPISODE   — active episode scoping
--   MIP.APP.PORTFOLIO_PROFILE   — max positions, max position pct

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_PARALLEL_WORLD_ACTUAL (
    PORTFOLIO_ID,
    AS_OF_TS,
    EPISODE_ID,
    STARTING_CASH,
    TRADES_ACTUAL,
    BUY_COUNT,
    SELL_COUNT,
    REALIZED_PNL,
    TOTAL_EQUITY,
    CASH,
    EQUITY_VALUE,
    OPEN_POSITIONS,
    DAILY_PNL,
    DAILY_RETURN,
    PEAK_EQUITY,
    DRAWDOWN,
    MAX_POSITIONS,
    MAX_POSITION_PCT
) as
with
-- Active episode per portfolio
active_episode as (
    select
        PORTFOLIO_ID,
        EPISODE_ID,
        START_TS,
        START_EQUITY
    from MIP.APP.PORTFOLIO_EPISODE
    where STATUS = 'ACTIVE'
    qualify row_number() over (partition by PORTFOLIO_ID order by START_TS desc) = 1
),

-- Daily snapshots scoped to active episode
daily as (
    select
        d.PORTFOLIO_ID,
        d.TS as AS_OF_TS,
        d.EPISODE_ID,
        d.CASH,
        d.EQUITY_VALUE,
        d.TOTAL_EQUITY,
        d.OPEN_POSITIONS,
        d.DAILY_PNL,
        d.DAILY_RETURN,
        d.PEAK_EQUITY,
        d.DRAWDOWN
    from MIP.APP.PORTFOLIO_DAILY d
    left join active_episode ae
      on ae.PORTFOLIO_ID = d.PORTFOLIO_ID
    where (
        (d.EPISODE_ID is not null and d.EPISODE_ID = ae.EPISODE_ID)
        or (d.EPISODE_ID is null and (ae.EPISODE_ID is null or d.TS >= ae.START_TS))
    )
),

-- Trade counts per portfolio-day (scoped to active episode)
trades_per_day as (
    select
        t.PORTFOLIO_ID,
        t.TRADE_TS::date as TRADE_DATE,
        count(*)                                  as TRADE_COUNT,
        sum(case when t.SIDE = 'BUY' then 1 else 0 end)  as BUY_COUNT,
        sum(case when t.SIDE = 'SELL' then 1 else 0 end) as SELL_COUNT,
        sum(coalesce(t.REALIZED_PNL, 0))          as REALIZED_PNL
    from MIP.APP.PORTFOLIO_TRADES t
    left join active_episode ae
      on ae.PORTFOLIO_ID = t.PORTFOLIO_ID
    where (
        (t.EPISODE_ID is not null and t.EPISODE_ID = ae.EPISODE_ID)
        or (t.EPISODE_ID is null and (ae.EPISODE_ID is null or t.TRADE_TS >= ae.START_TS))
    )
    group by t.PORTFOLIO_ID, t.TRADE_TS::date
)

select
    d.PORTFOLIO_ID,
    d.AS_OF_TS,
    d.EPISODE_ID,
    coalesce(ae.START_EQUITY, p.STARTING_CASH) as STARTING_CASH,
    coalesce(tpd.TRADE_COUNT, 0) as TRADES_ACTUAL,
    coalesce(tpd.BUY_COUNT, 0)   as BUY_COUNT,
    coalesce(tpd.SELL_COUNT, 0)  as SELL_COUNT,
    coalesce(tpd.REALIZED_PNL, 0) as REALIZED_PNL,
    d.TOTAL_EQUITY,
    d.CASH,
    d.EQUITY_VALUE,
    d.OPEN_POSITIONS,
    d.DAILY_PNL,
    d.DAILY_RETURN,
    d.PEAK_EQUITY,
    d.DRAWDOWN,
    coalesce(pp.MAX_POSITIONS, 5)    as MAX_POSITIONS,
    coalesce(pp.MAX_POSITION_PCT, 0.05) as MAX_POSITION_PCT
from daily d
join MIP.APP.PORTFOLIO p
  on p.PORTFOLIO_ID = d.PORTFOLIO_ID
left join MIP.APP.PORTFOLIO_PROFILE pp
  on pp.PROFILE_ID = p.PROFILE_ID
left join active_episode ae
  on ae.PORTFOLIO_ID = d.PORTFOLIO_ID
left join trades_per_day tpd
  on tpd.PORTFOLIO_ID = d.PORTFOLIO_ID
 and tpd.TRADE_DATE = d.AS_OF_TS::date
where p.STATUS = 'ACTIVE';
