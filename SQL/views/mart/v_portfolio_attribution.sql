-- v_portfolio_attribution.sql
-- Purpose: Trade attribution KPIs by symbol and pattern rollups

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_PORTFOLIO_ATTRIBUTION as
with sell_trades as (
    select
        TRADE_ID,
        PORTFOLIO_ID,
        RUN_ID,
        SYMBOL,
        MARKET_TYPE,
        INTERVAL_MINUTES,
        TRADE_TS,
        REALIZED_PNL
    from MIP.APP.PORTFOLIO_TRADES
    where SIDE = 'SELL'
      and REALIZED_PNL is not null
),
run_totals as (
    select
        PORTFOLIO_ID,
        RUN_ID,
        sum(REALIZED_PNL) as TOTAL_REALIZED_PNL
    from sell_trades
    group by
        PORTFOLIO_ID,
        RUN_ID
),
by_symbol as (
    select
        PORTFOLIO_ID,
        RUN_ID,
        MARKET_TYPE,
        SYMBOL,
        sum(REALIZED_PNL) as TOTAL_REALIZED_PNL,
        count(*) as ROUNDTRIPS,
        sum(case when REALIZED_PNL > 0 then 1 else 0 end) / nullif(count(*), 0) as WIN_RATE
    from sell_trades
    group by
        PORTFOLIO_ID,
        RUN_ID,
        MARKET_TYPE,
        SYMBOL
)
select
    s.PORTFOLIO_ID,
    s.RUN_ID,
    s.MARKET_TYPE,
    s.SYMBOL,
    s.TOTAL_REALIZED_PNL,
    s.ROUNDTRIPS,
    s.TOTAL_REALIZED_PNL / nullif(s.ROUNDTRIPS, 0) as AVG_PNL_PER_TRADE,
    s.WIN_RATE,
    s.TOTAL_REALIZED_PNL / nullif(t.TOTAL_REALIZED_PNL, 0) as CONTRIBUTION_PCT
from by_symbol s
left join run_totals t
  on t.PORTFOLIO_ID = s.PORTFOLIO_ID
 and t.RUN_ID = s.RUN_ID;

create or replace view MIP.MART.V_PORTFOLIO_ATTRIBUTION_BY_PATTERN as
with sell_trades as (
    select
        TRADE_ID,
        PORTFOLIO_ID,
        RUN_ID,
        SYMBOL,
        MARKET_TYPE,
        INTERVAL_MINUTES,
        TRADE_TS,
        REALIZED_PNL
    from MIP.APP.PORTFOLIO_TRADES
    where SIDE = 'SELL'
      and REALIZED_PNL is not null
),
position_map as (
    -- Match each SELL trade to its corresponding BUY trade to get entry timestamp.
    -- Uses BUY trades (self-join on PORTFOLIO_TRADES) which is more robust than
    -- joining to PORTFOLIO_POSITIONS since positions may outlive their sell cycle.
    select
        t.TRADE_ID,
        t.PORTFOLIO_ID,
        t.RUN_ID,
        t.MARKET_TYPE,
        t.SYMBOL,
        t.INTERVAL_MINUTES,
        t.REALIZED_PNL,
        buy.TRADE_TS as ENTRY_TS
    from sell_trades t
    join MIP.APP.PORTFOLIO_TRADES buy
      on buy.PORTFOLIO_ID = t.PORTFOLIO_ID
     and buy.SYMBOL = t.SYMBOL
     and buy.MARKET_TYPE = t.MARKET_TYPE
     and buy.INTERVAL_MINUTES = t.INTERVAL_MINUTES
     and buy.SIDE = 'BUY'
     and buy.TRADE_TS <= t.TRADE_TS
    qualify row_number() over (
        partition by t.TRADE_ID
        order by buy.TRADE_TS desc
    ) = 1
),
rec_map as (
    select
        pm.TRADE_ID,
        pm.PORTFOLIO_ID,
        pm.RUN_ID,
        pm.MARKET_TYPE,
        pm.SYMBOL,
        pm.REALIZED_PNL,
        r.PATTERN_ID,
        o.HORIZON_BARS
    from position_map pm
    join MIP.APP.RECOMMENDATION_LOG r
      on r.SYMBOL = pm.SYMBOL
     and r.MARKET_TYPE = pm.MARKET_TYPE
     and r.INTERVAL_MINUTES = pm.INTERVAL_MINUTES
     and r.TS = pm.ENTRY_TS
    left join MIP.APP.RECOMMENDATION_OUTCOMES o
      on o.RECOMMENDATION_ID = r.RECOMMENDATION_ID
    qualify row_number() over (
        partition by pm.TRADE_ID
        order by o.HORIZON_BARS
    ) = 1
),
run_totals as (
    select
        PORTFOLIO_ID,
        RUN_ID,
        sum(REALIZED_PNL) as TOTAL_REALIZED_PNL
    from rec_map
    group by
        PORTFOLIO_ID,
        RUN_ID
)
select
    r.PORTFOLIO_ID,
    r.RUN_ID,
    r.PATTERN_ID,
    r.MARKET_TYPE,
    r.HORIZON_BARS,
    sum(r.REALIZED_PNL) as TOTAL_REALIZED_PNL,
    count(*) as ROUNDTRIPS,
    sum(r.REALIZED_PNL) / nullif(count(*), 0) as AVG_PNL_PER_TRADE,
    sum(case when r.REALIZED_PNL > 0 then 1 else 0 end) / nullif(count(*), 0) as WIN_RATE,
    sum(r.REALIZED_PNL) / nullif(t.TOTAL_REALIZED_PNL, 0) as CONTRIBUTION_PCT
from rec_map r
left join run_totals t
  on t.PORTFOLIO_ID = r.PORTFOLIO_ID
 and t.RUN_ID = r.RUN_ID
group by
    r.PORTFOLIO_ID,
    r.RUN_ID,
    r.PATTERN_ID,
    r.MARKET_TYPE,
    r.HORIZON_BARS,
    t.TOTAL_REALIZED_PNL;
