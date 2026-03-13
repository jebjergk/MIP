-- 402_disable_alphavantage_now.sql
-- Purpose: Immediately disable AlphaVantage usage to avoid additional billing.

use role MIP_ADMIN_ROLE;
use database MIP;

merge into MIP.APP.APP_CONFIG t
using (
    select 'ALPHAVANTAGE_ENABLED' as CONFIG_KEY, 'false' as CONFIG_VALUE,
           'Feature flag for AlphaVantage ingestion. Keep false when IBKR is the sole provider.' as DESCRIPTION
    union all
    select 'ALPHAVANTAGE_API_KEY', '<DISABLED>',
           'AlphaVantage API key placeholder when provider is disabled.'
    union all
    select 'MARKET_DATA_PROVIDER_DEFAULT', 'IBKR',
           'Default market bar provider for SQL pipeline ingestion (ALPHAVANTAGE or IBKR).'
    union all
    select 'MARKET_DATA_PROVIDER_REVALIDATION', 'IBKR',
           'Preferred market bar provider for live committee revalidation refresh.'
) s
on t.CONFIG_KEY = s.CONFIG_KEY
when matched then update set
  t.CONFIG_VALUE = s.CONFIG_VALUE,
  t.DESCRIPTION = s.DESCRIPTION,
  t.UPDATED_AT = current_timestamp()
when not matched then insert (CONFIG_KEY, CONFIG_VALUE, DESCRIPTION, UPDATED_AT)
values (s.CONFIG_KEY, s.CONFIG_VALUE, s.DESCRIPTION, current_timestamp());
