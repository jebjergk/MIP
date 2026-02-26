-- 361_vol_exp_cohort_seed.sql
-- Purpose: Seed VOL_EXP symbol cohort for additive bootstrap training.

use role MIP_ADMIN_ROLE;
use database MIP;

alter table MIP.APP.INGEST_UNIVERSE
    add column if not exists SYMBOL_COHORT string default 'CORE';

update MIP.APP.INGEST_UNIVERSE
   set SYMBOL_COHORT = 'CORE'
 where SYMBOL_COHORT is null;

merge into MIP.APP.INGEST_UNIVERSE t
using (
    select 'AMD' as SYMBOL, 'STOCK' as MARKET_TYPE, 1440 as INTERVAL_MINUTES, true as IS_ENABLED, 120 as PRIORITY, 'VOL_EXP bootstrap symbol' as NOTES, 'VOL_EXP' as SYMBOL_COHORT
    union all select 'MU', 'STOCK', 1440, true, 120, 'VOL_EXP bootstrap symbol', 'VOL_EXP'
    union all select 'CAT', 'STOCK', 1440, true, 120, 'VOL_EXP bootstrap symbol', 'VOL_EXP'
    union all select 'BA', 'STOCK', 1440, true, 120, 'VOL_EXP bootstrap symbol', 'VOL_EXP'
    union all select 'SHOP', 'STOCK', 1440, true, 120, 'VOL_EXP bootstrap symbol', 'VOL_EXP'
    union all select 'SOXX', 'ETF', 1440, true, 115, 'VOL_EXP bootstrap symbol', 'VOL_EXP'
    union all select 'XLE', 'ETF', 1440, true, 115, 'VOL_EXP bootstrap symbol', 'VOL_EXP'
    union all select 'GBP/JPY', 'FX', 1440, true, 110, 'VOL_EXP bootstrap symbol', 'VOL_EXP'
) s
   on t.SYMBOL = s.SYMBOL
  and t.MARKET_TYPE = s.MARKET_TYPE
  and t.INTERVAL_MINUTES = s.INTERVAL_MINUTES
when matched then update set
    t.IS_ENABLED = s.IS_ENABLED,
    t.PRIORITY = s.PRIORITY,
    t.NOTES = s.NOTES,
    t.SYMBOL_COHORT = s.SYMBOL_COHORT
when not matched then insert (
    SYMBOL,
    MARKET_TYPE,
    INTERVAL_MINUTES,
    IS_ENABLED,
    PRIORITY,
    NOTES,
    SYMBOL_COHORT
) values (
    s.SYMBOL,
    s.MARKET_TYPE,
    s.INTERVAL_MINUTES,
    s.IS_ENABLED,
    s.PRIORITY,
    s.NOTES,
    s.SYMBOL_COHORT
);

