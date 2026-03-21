-- v_market_regime.sql
-- Purpose: Compute current market regime based on broad market ETF behavior.
-- Uses SPY/DIA/QQQ as regime proxies: 20-day returns, trend direction, and volatility.
-- Regime labels: BULL, BEAR, SIDEWAYS, VOLATILE_BULL, VOLATILE_BEAR

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_MARKET_REGIME as
with daily_bars as (
    select
        SYMBOL,
        TS,
        CLOSE,
        LAG(CLOSE, 1) over (partition by SYMBOL order by TS) as PREV_CLOSE_1,
        LAG(CLOSE, 5) over (partition by SYMBOL order by TS) as PREV_CLOSE_5,
        LAG(CLOSE, 20) over (partition by SYMBOL order by TS) as PREV_CLOSE_20,
        STDDEV(CLOSE) over (
            partition by SYMBOL order by TS
            rows between 19 preceding and current row
        ) / nullif(AVG(CLOSE) over (
            partition by SYMBOL order by TS
            rows between 19 preceding and current row
        ), 0) as VOLATILITY_20D
    from MIP.MART.MARKET_BARS
    where SYMBOL in ('SPY', 'QQQ', 'DIA')
      and INTERVAL_MINUTES = 1440
      and MARKET_TYPE = 'ETF'
),
regime_per_etf as (
    select
        SYMBOL,
        TS,
        CLOSE,
        (CLOSE - PREV_CLOSE_1) / nullif(PREV_CLOSE_1, 0) as RETURN_1D,
        (CLOSE - PREV_CLOSE_5) / nullif(PREV_CLOSE_5, 0) as RETURN_5D,
        (CLOSE - PREV_CLOSE_20) / nullif(PREV_CLOSE_20, 0) as RETURN_20D,
        VOLATILITY_20D,
        case
            when (CLOSE - PREV_CLOSE_20) / nullif(PREV_CLOSE_20, 0) > 0.03
                 and VOLATILITY_20D < 0.02
            then 'BULL'
            when (CLOSE - PREV_CLOSE_20) / nullif(PREV_CLOSE_20, 0) > 0.03
                 and VOLATILITY_20D >= 0.02
            then 'VOLATILE_BULL'
            when (CLOSE - PREV_CLOSE_20) / nullif(PREV_CLOSE_20, 0) < -0.03
                 and VOLATILITY_20D < 0.02
            then 'BEAR'
            when (CLOSE - PREV_CLOSE_20) / nullif(PREV_CLOSE_20, 0) < -0.03
                 and VOLATILITY_20D >= 0.02
            then 'VOLATILE_BEAR'
            else 'SIDEWAYS'
        end as ETF_REGIME
    from daily_bars
    where PREV_CLOSE_20 is not null
),
consensus as (
    select
        TS,
        MODE(ETF_REGIME) as REGIME,
        ROUND(AVG(RETURN_1D), 6) as AVG_RETURN_1D,
        ROUND(AVG(RETURN_5D), 6) as AVG_RETURN_5D,
        ROUND(AVG(RETURN_20D), 6) as AVG_RETURN_20D,
        ROUND(AVG(VOLATILITY_20D), 6) as AVG_VOLATILITY_20D,
        ARRAY_AGG(DISTINCT ETF_REGIME) as REGIME_VOTES,
        COUNT(DISTINCT ETF_REGIME) as REGIME_AGREEMENT
    from regime_per_etf
    group by TS
)
select
    TS as REGIME_DATE,
    REGIME,
    case
        when REGIME_AGREEMENT = 1 then 'HIGH'
        when REGIME_AGREEMENT = 2 then 'MEDIUM'
        else 'LOW'
    end as REGIME_CONFIDENCE,
    AVG_RETURN_1D,
    AVG_RETURN_5D,
    AVG_RETURN_20D,
    AVG_VOLATILITY_20D,
    REGIME_VOTES,
    REGIME_AGREEMENT
from consensus;
