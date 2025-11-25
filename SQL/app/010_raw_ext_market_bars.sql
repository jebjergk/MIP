-- 010_raw_ext_market_bars.sql
-- Purpose: Landing table for AlphaVantage OHLCV data (stocks + FX)

use role MIP_ADMIN_ROLE;
use database MIP;

create table if not exists MIP.RAW_EXT.MARKET_BARS_RAW (
    TS              TIMESTAMP_NTZ,  -- Bar timestamp
    SYMBOL          STRING,         -- e.g. 'AAPL' or 'EUR/USD'
    SOURCE          STRING,         -- e.g. 'ALPHAVANTAGE'
    MARKET_TYPE     STRING,         -- 'STOCK' or 'FX'
    INTERVAL_MINUTES NUMBER,        -- e.g. 1, 5, 15
    OPEN            NUMBER,
    HIGH            NUMBER,
    LOW             NUMBER,
    CLOSE           NUMBER,
    VOLUME          NUMBER,
    RAW             VARIANT,        -- full JSON payload for debugging/audit
    INGESTED_AT     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
