-- 301_intraday_config.sql
-- Purpose: Feature flags and seed data for the intraday subsystem.
-- All flags default to disabled / conservative values.

use role MIP_ADMIN_ROLE;
use database MIP;

------------------------------
-- 1. APP_CONFIG feature flags
------------------------------
merge into MIP.APP.APP_CONFIG t
using (
    select 'INTRADAY_ENABLED' as CONFIG_KEY,
           'false' as CONFIG_VALUE,
           'Master kill switch for intraday subsystem' as DESCRIPTION
    union all
    select 'INTRADAY_INTERVAL_MINUTES', '60',
           'Bar interval for intraday pipeline (60 = hourly)'
    union all
    select 'INTRADAY_USE_DAILY_CONTEXT', 'false',
           'Whether intraday signal generation reads daily directional bias'
    union all
    select 'INTRADAY_MAX_SYMBOLS', '8',
           'Max symbols to ingest per intraday pipeline run (cost control)'
) s
on t.CONFIG_KEY = s.CONFIG_KEY
when not matched then insert (CONFIG_KEY, CONFIG_VALUE, DESCRIPTION)
    values (s.CONFIG_KEY, s.CONFIG_VALUE, s.DESCRIPTION);

------------------------------
-- 2. INTRADAY_FEE_CONFIG seed
------------------------------
merge into MIP.APP.INTRADAY_FEE_CONFIG t
using (
    select
        'DEFAULT' as FEE_PROFILE,
        1.0       as FEE_BPS,
        2.0       as SLIPPAGE_BPS,
        1.0       as SPREAD_BPS,
        0.0       as MIN_FEE_USD,
        true      as IS_ACTIVE,
        'Default intraday fee profile: 1bp fee + 2bp slippage + 1bp spread = ~5bp round-trip' as DESCRIPTION
) s
on t.FEE_PROFILE = s.FEE_PROFILE
when matched then update set
    t.FEE_BPS      = s.FEE_BPS,
    t.SLIPPAGE_BPS = s.SLIPPAGE_BPS,
    t.SPREAD_BPS   = s.SPREAD_BPS,
    t.MIN_FEE_USD  = s.MIN_FEE_USD,
    t.IS_ACTIVE    = s.IS_ACTIVE,
    t.DESCRIPTION  = s.DESCRIPTION,
    t.UPDATED_AT   = current_timestamp()
when not matched then insert (
    FEE_PROFILE, FEE_BPS, SLIPPAGE_BPS, SPREAD_BPS, MIN_FEE_USD, IS_ACTIVE, DESCRIPTION
) values (
    s.FEE_PROFILE, s.FEE_BPS, s.SLIPPAGE_BPS, s.SPREAD_BPS, s.MIN_FEE_USD, s.IS_ACTIVE, s.DESCRIPTION
);
