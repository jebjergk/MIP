-- 400_market_data_provider_config.sql
-- Purpose: Provider routing config for market-bar ingestion and revalidation.

use role MIP_ADMIN_ROLE;
use database MIP;

merge into MIP.APP.APP_CONFIG t
using (
    select
      'MARKET_DATA_PROVIDER_DEFAULT' as CONFIG_KEY,
      'IBKR' as CONFIG_VALUE,
      'Default market bar provider for SQL pipeline ingestion (ALPHAVANTAGE or IBKR).' as DESCRIPTION
    union all
    select
      'MARKET_DATA_PROVIDER_REVALIDATION',
      'IBKR',
      'Preferred market bar provider for live committee revalidation refresh.'
    union all
    select
      'MARKET_DATA_PROVIDER_BACKFILL',
      'ALPHAVANTAGE',
      'Preferred provider for deep historical backfill runs.'
) s
on t.CONFIG_KEY = s.CONFIG_KEY
when matched then update set
  t.CONFIG_VALUE = s.CONFIG_VALUE,
  t.DESCRIPTION = s.DESCRIPTION,
  t.UPDATED_AT = current_timestamp()
when not matched then insert (CONFIG_KEY, CONFIG_VALUE, DESCRIPTION, UPDATED_AT)
values (s.CONFIG_KEY, s.CONFIG_VALUE, s.DESCRIPTION, current_timestamp());
