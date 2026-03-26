-- 20260325_disable_us_etfs_expand_universe.sql
-- EU retail: US-listed ETFs typically lack PRIIPs KID — disable ingest/UX, retain bars.
-- Adds liquid large-cap stocks + major FX crosses (see 050_app_core_tables.sql notes).

use role MIP_ADMIN_ROLE;
use database MIP;

-- Daily + any interval: turn off US ETF universe rows (data in MART tables is not deleted)
update MIP.APP.INGEST_UNIVERSE
set
    IS_ENABLED = false,
    NOTES = 'Disabled: EU retail — US ETF lacks PRIIPs KID, historical data retained',
    PRIORITY = coalesce(PRIORITY, 90)
where MARKET_TYPE = 'ETF'
  and SYMBOL in (
    'SPY', 'QQQ', 'IWM', 'DIA', 'XLK', 'XLF', 'SOXX', 'XLE',
    'GLD', 'TLT'
  );

-- New daily symbols (idempotent)
merge into MIP.APP.INGEST_UNIVERSE t
using (
    select 'CSCO' as SYMBOL, 'STOCK' as MARKET_TYPE, 1440 as INTERVAL_MINUTES,
           true as IS_ENABLED, 95 as PRIORITY,
           'Large-cap tech, lower nominal than mega-cap leaders' as NOTES
    union all select 'INTC', 'STOCK', 1440, true, 95, 'Large-cap semis, more affordable nominal than NVDA'
    union all select 'PFE', 'STOCK', 1440, true, 95, 'Large-cap pharma, liquid US listing'
    union all select 'WMT', 'STOCK', 1440, true, 95, 'Defensive large-cap consumer'
    union all select 'VZ', 'STOCK', 1440, true, 95, 'Large-cap telecom, typically moderate share price'
    union all select 'NZDUSD', 'FX', 1440, true, 80, 'Major FX, complements G10 suite'
    union all select 'EURGBP', 'FX', 1440, true, 80, 'Major cross, liquid on IDEALPRO'
    union all select 'EURJPY', 'FX', 1440, true, 80, 'Major cross, liquid on IDEALPRO'
) s
on t.SYMBOL = s.SYMBOL and t.MARKET_TYPE = s.MARKET_TYPE and t.INTERVAL_MINUTES = s.INTERVAL_MINUTES
when matched then update set
    t.IS_ENABLED = coalesce(t.IS_ENABLED, s.IS_ENABLED),
    t.PRIORITY = coalesce(t.PRIORITY, s.PRIORITY),
    t.NOTES = coalesce(t.NOTES, s.NOTES)
when not matched then insert (SYMBOL, MARKET_TYPE, INTERVAL_MINUTES, IS_ENABLED, PRIORITY, NOTES)
    values (s.SYMBOL, s.MARKET_TYPE, s.INTERVAL_MINUTES, s.IS_ENABLED, s.PRIORITY, s.NOTES);

-- Intraday / hourly ETF rows: align with daily (script 302 uses iff(ETF,false) on full re-seed)
update MIP.APP.INGEST_UNIVERSE
set IS_ENABLED = false,
    NOTES = 'Disabled: EU retail — US ETF lacks PRIIPs KID'
where MARKET_TYPE = 'ETF'
  and INTERVAL_MINUTES in (15, 60)
  and SYMBOL in (
    'SPY', 'QQQ', 'IWM', 'DIA', 'XLK', 'XLF', 'SOXX', 'XLE',
    'GLD', 'TLT'
  );
