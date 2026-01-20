-- 020_app_config.sql
-- Purpose: Application-wide configuration for MIP (including API keys, defaults)

use role MIP_ADMIN_ROLE;
use database MIP;

-----------------------
-- 1. APP_CONFIG table
-----------------------
create table if not exists MIP.APP.APP_CONFIG (
    CONFIG_KEY   STRING         not null,
    CONFIG_VALUE STRING,
    DESCRIPTION  STRING,
    UPDATED_AT   TIMESTAMP_NTZ  default MIP.APP.F_NOW_BERLIN_NTZ(),
    constraint PK_APP_CONFIG primary key (CONFIG_KEY)
);

-----------------------
-- 2. Seed basic config rows
-----------------------

-- Helper: upsert-like pattern using MERGE
merge into MIP.APP.APP_CONFIG t
using (
    select 'ALPHAVANTAGE_API_KEY' as CONFIG_KEY,
           '7UYJREVV2O8KJ1Q4'        as CONFIG_VALUE,
           'API key for AlphaVantage market data (set this manually)' as DESCRIPTION
    union all
    select 'DEFAULT_STOCK_SYMBOLS',
           'AAPL,MSFT',
           'Comma-separated list of stock symbols to ingest from AlphaVantage'
    union all
    select 'DEFAULT_FX_PAIRS',
           'EUR/USD,USD/JPY',
           'Comma-separated list of FX pairs to ingest from AlphaVantage'
    union all
    select 'PATTERN_MIN_TRADES',
           '30',
           'Minimum trade count required to activate a pattern'
    union all
    select 'PATTERN_MIN_HIT_RATE',
           '0.55',
           'Minimum hit rate required to activate a pattern'
    union all
    select 'PATTERN_MIN_CUM_RETURN',
           '0.0',
           'Minimum cumulative return required to activate a pattern'
    union all
    select 'SIM_MIN_SAMPLE_SIZE',
           '30',
           'Minimum sample size per pattern/market type/horizon before simulation readiness'
) s
on t.CONFIG_KEY = s.CONFIG_KEY
when matched then update set
    t.CONFIG_VALUE = s.CONFIG_VALUE,
    t.DESCRIPTION  = s.DESCRIPTION,
    t.UPDATED_AT   = MIP.APP.F_NOW_BERLIN_NTZ()
when not matched then insert (CONFIG_KEY, CONFIG_VALUE, DESCRIPTION, UPDATED_AT)
values (s.CONFIG_KEY, s.CONFIG_VALUE, s.DESCRIPTION, MIP.APP.F_NOW_BERLIN_NTZ());

select * from MIP.APP.APP_CONFIG;

-- IMPORTANT:
-- After running this script, update the ALPHAVANTAGE_API_KEY row with your real key:
update MIP.APP.APP_CONFIG
   set CONFIG_VALUE = '7UYJREVV2O8KJ1Q4'
   where CONFIG_KEY = 'ALPHAVANTAGE_API_KEY';
