-- 376_seed_symbol_alias_dict.sql
-- Purpose: Phase 3 initial deterministic alias dictionary seed.
-- Precision-first policy:
--   - conservative aliases only in v1.
--   - expand after QA review to avoid false positives.

use role MIP_ADMIN_ROLE;
use database MIP;

-- 1) Baseline: symbol self-alias from enabled daily ingest universe.
merge into MIP.NEWS.SYMBOL_ALIAS_DICT t
using (
    select
        upper(SYMBOL) as SYMBOL,
        upper(MARKET_TYPE) as MARKET_TYPE,
        upper(SYMBOL) as ALIAS,
        'TICKER' as ALIAS_TYPE,
        true as IS_ACTIVE
    from MIP.APP.INGEST_UNIVERSE
    where coalesce(IS_ENABLED, true)
      and INTERVAL_MINUTES = 1440
) s
on t.SYMBOL = s.SYMBOL
and t.MARKET_TYPE = s.MARKET_TYPE
and t.ALIAS = s.ALIAS
when matched then update set
    t.ALIAS_TYPE = s.ALIAS_TYPE,
    t.IS_ACTIVE = s.IS_ACTIVE,
    t.UPDATED_AT = current_timestamp()
when not matched then insert (
    SYMBOL, MARKET_TYPE, ALIAS, ALIAS_TYPE, IS_ACTIVE, CREATED_AT, UPDATED_AT
) values (
    s.SYMBOL, s.MARKET_TYPE, s.ALIAS, s.ALIAS_TYPE, s.IS_ACTIVE, current_timestamp(), current_timestamp()
);

-- 2) Conservative human-readable aliases for current tracked universe.
merge into MIP.NEWS.SYMBOL_ALIAS_DICT t
using (
    select 'AAPL' as SYMBOL, 'STOCK' as MARKET_TYPE, 'APPLE' as ALIAS, 'COMPANY_NAME' as ALIAS_TYPE, true as IS_ACTIVE
    union all select 'MSFT', 'STOCK', 'MICROSOFT', 'COMPANY_NAME', true
    union all select 'AMZN', 'STOCK', 'AMAZON', 'COMPANY_NAME', true
    union all select 'NVDA', 'STOCK', 'NVIDIA', 'COMPANY_NAME', true
    union all select 'GOOGL', 'STOCK', 'ALPHABET', 'COMPANY_NAME', true
    union all select 'GOOGL', 'STOCK', 'GOOGLE', 'COMPANY_NAME', true
    union all select 'META', 'STOCK', 'META', 'COMPANY_NAME', true
    union all select 'META', 'STOCK', 'META PLATFORMS', 'COMPANY_NAME', true
    union all select 'TSLA', 'STOCK', 'TESLA', 'COMPANY_NAME', true
    union all select 'JPM', 'STOCK', 'JPMORGAN', 'COMPANY_NAME', true
    union all select 'XOM', 'STOCK', 'EXXON', 'COMPANY_NAME', true
    union all select 'JNJ', 'STOCK', 'JOHNSON AND JOHNSON', 'COMPANY_NAME', true
    union all select 'PG', 'STOCK', 'PROCTER AND GAMBLE', 'COMPANY_NAME', true
    union all select 'KO', 'STOCK', 'COCA COLA', 'COMPANY_NAME', true

    union all select 'SPY', 'ETF', 'S&P 500 ETF', 'ETF_NAME', true
    union all select 'QQQ', 'ETF', 'NASDAQ 100 ETF', 'ETF_NAME', true
    union all select 'IWM', 'ETF', 'RUSSELL 2000 ETF', 'ETF_NAME', true
    union all select 'DIA', 'ETF', 'DOW JONES ETF', 'ETF_NAME', true
    union all select 'XLK', 'ETF', 'TECHNOLOGY SELECT SECTOR', 'ETF_NAME', true
    union all select 'XLF', 'ETF', 'FINANCIAL SELECT SECTOR', 'ETF_NAME', true

    union all select 'EURUSD', 'FX', 'EUR/USD', 'FX_PAIR', true
    union all select 'GBPUSD', 'FX', 'GBP/USD', 'FX_PAIR', true
    union all select 'USDJPY', 'FX', 'USD/JPY', 'FX_PAIR', true
    union all select 'USDCHF', 'FX', 'USD/CHF', 'FX_PAIR', true
    union all select 'AUDUSD', 'FX', 'AUD/USD', 'FX_PAIR', true
    union all select 'USDCAD', 'FX', 'USD/CAD', 'FX_PAIR', true
) s
on t.SYMBOL = s.SYMBOL
and t.MARKET_TYPE = s.MARKET_TYPE
and t.ALIAS = s.ALIAS
when matched then update set
    t.ALIAS_TYPE = s.ALIAS_TYPE,
    t.IS_ACTIVE = s.IS_ACTIVE,
    t.UPDATED_AT = current_timestamp()
when not matched then insert (
    SYMBOL, MARKET_TYPE, ALIAS, ALIAS_TYPE, IS_ACTIVE, CREATED_AT, UPDATED_AT
) values (
    s.SYMBOL, s.MARKET_TYPE, s.ALIAS, s.ALIAS_TYPE, s.IS_ACTIVE, current_timestamp(), current_timestamp()
);

-- 3) QA sample table for precision/coverage reporting.
create table if not exists MIP.NEWS.NEWS_MAPPING_QA_SAMPLE (
    QA_ID                   number autoincrement,
    NEWS_ID                 string         not null,
    EXPECTED_SYMBOL         string         not null,
    EXPECTED_MARKET_TYPE    string         not null,
    IS_RELEVANT             boolean        not null,
    REVIEWED_BY             string,
    REVIEWED_AT             timestamp_ntz,
    NOTES                   string,
    CREATED_AT              timestamp_ntz  not null default current_timestamp(),
    constraint PK_NEWS_MAPPING_QA_SAMPLE primary key (QA_ID)
);
