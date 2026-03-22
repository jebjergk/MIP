-- v_portfolio_fee_summary.sql
-- Purpose: Portfolio-level fee analytics for operator visibility.
-- Shows total fees by period, by symbol, fees as % of gross P&L,
-- and identifies trades where fees exceeded net P&L.

use role MIP_ADMIN_ROLE;
use database MIP;

-- 1) Fee summary by symbol (per portfolio/run)
create or replace view MIP.MART.V_PORTFOLIO_FEE_BY_SYMBOL as
select
    PORTFOLIO_ID,
    RUN_ID,
    SYMBOL,
    MARKET_TYPE,
    count(*) as TRADE_COUNT,
    sum(case when SIDE = 'BUY' then 1 else 0 end) as BUY_COUNT,
    sum(case when SIDE = 'SELL' then 1 else 0 end) as SELL_COUNT,
    sum(coalesce(COMMISSION, 0)) as TOTAL_COMMISSION,
    sum(coalesce(TOTAL_FEE, 0)) as TOTAL_FEES,
    sum(coalesce(NOTIONAL, 0)) as TOTAL_NOTIONAL,
    sum(coalesce(REALIZED_PNL, 0)) as TOTAL_REALIZED_PNL,
    case
        when sum(coalesce(NOTIONAL, 0)) > 0
        then sum(coalesce(TOTAL_FEE, 0)) / sum(coalesce(NOTIONAL, 0)) * 10000
        else 0
    end as FEES_BPS_OF_NOTIONAL,
    case
        when abs(sum(coalesce(REALIZED_PNL, 0))) > 0
        then sum(coalesce(TOTAL_FEE, 0)) / abs(sum(coalesce(REALIZED_PNL, 0)))
        else null
    end as FEES_AS_PCT_OF_PNL
from MIP.APP.PORTFOLIO_TRADES
group by PORTFOLIO_ID, RUN_ID, SYMBOL, MARKET_TYPE;

-- 2) Fee summary by period (day/week/month)
create or replace view MIP.MART.V_PORTFOLIO_FEE_BY_PERIOD as
select
    PORTFOLIO_ID,
    RUN_ID,
    date_trunc('day', TRADE_TS) as TRADE_DATE,
    date_trunc('week', TRADE_TS) as TRADE_WEEK,
    date_trunc('month', TRADE_TS) as TRADE_MONTH,
    count(*) as TRADE_COUNT,
    sum(coalesce(COMMISSION, 0)) as DAILY_COMMISSION,
    sum(coalesce(TOTAL_FEE, 0)) as DAILY_FEES,
    sum(coalesce(NOTIONAL, 0)) as DAILY_NOTIONAL,
    sum(coalesce(REALIZED_PNL, 0)) as DAILY_REALIZED_PNL
from MIP.APP.PORTFOLIO_TRADES
group by PORTFOLIO_ID, RUN_ID, date_trunc('day', TRADE_TS),
         date_trunc('week', TRADE_TS), date_trunc('month', TRADE_TS);

-- 3) Marginal trades: where fees exceeded net P&L
create or replace view MIP.MART.V_PORTFOLIO_MARGINAL_TRADES as
select
    sell.TRADE_ID,
    sell.PORTFOLIO_ID,
    sell.RUN_ID,
    sell.SYMBOL,
    sell.MARKET_TYPE,
    sell.TRADE_TS,
    sell.REALIZED_PNL as GROSS_PNL,
    coalesce(sell.TOTAL_FEE, 0) + coalesce(buy.TOTAL_FEE, 0) as ROUND_TRIP_FEE,
    sell.REALIZED_PNL - coalesce(sell.TOTAL_FEE, 0) - coalesce(buy.TOTAL_FEE, 0) as NET_PNL_AFTER_FEES,
    case
        when sell.REALIZED_PNL > 0
         and sell.REALIZED_PNL <= coalesce(sell.TOTAL_FEE, 0) + coalesce(buy.TOTAL_FEE, 0)
        then true
        else false
    end as FEE_EXCEEDED_PNL
from MIP.APP.PORTFOLIO_TRADES sell
left join (
    select
        PORTFOLIO_ID, RUN_ID, SYMBOL, MARKET_TYPE,
        TRADE_TS, TOTAL_FEE,
        row_number() over (
            partition by PORTFOLIO_ID, RUN_ID, SYMBOL, MARKET_TYPE
            order by TRADE_TS desc
        ) as rn
    from MIP.APP.PORTFOLIO_TRADES
    where SIDE = 'BUY'
) buy
  on buy.PORTFOLIO_ID = sell.PORTFOLIO_ID
 and buy.RUN_ID = sell.RUN_ID
 and buy.SYMBOL = sell.SYMBOL
 and buy.MARKET_TYPE = sell.MARKET_TYPE
 and buy.TRADE_TS <= sell.TRADE_TS
 and buy.rn = 1
where sell.SIDE = 'SELL'
  and sell.REALIZED_PNL is not null;

-- 4) Overall portfolio fee totals
create or replace view MIP.MART.V_PORTFOLIO_FEE_TOTALS as
select
    PORTFOLIO_ID,
    RUN_ID,
    count(*) as TOTAL_TRADES,
    sum(coalesce(COMMISSION, 0)) as TOTAL_COMMISSION,
    sum(coalesce(REGULATORY_FEE, 0)) as TOTAL_REGULATORY_FEE,
    sum(coalesce(FX_CONVERSION_COST, 0)) as TOTAL_FX_COST,
    sum(coalesce(TOTAL_FEE, 0)) as TOTAL_ALL_FEES,
    sum(coalesce(NOTIONAL, 0)) as TOTAL_NOTIONAL,
    sum(coalesce(REALIZED_PNL, 0)) as TOTAL_REALIZED_PNL,
    case
        when abs(sum(coalesce(REALIZED_PNL, 0))) > 0
        then sum(coalesce(TOTAL_FEE, 0)) / abs(sum(coalesce(REALIZED_PNL, 0)))
        else null
    end as FEES_AS_PCT_OF_PNL,
    case
        when sum(coalesce(NOTIONAL, 0)) > 0
        then sum(coalesce(TOTAL_FEE, 0)) / sum(coalesce(NOTIONAL, 0)) * 10000
        else 0
    end as FEES_BPS_OF_NOTIONAL,
    max(FEE_SOURCE) as FEE_SOURCE_MODE
from MIP.APP.PORTFOLIO_TRADES
group by PORTFOLIO_ID, RUN_ID;
