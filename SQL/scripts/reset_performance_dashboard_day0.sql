-- reset_performance_dashboard_day0.sql
-- Purpose:
--   Reset performance dashboard baseline to "day 0" so legacy portfolio activity
--   is excluded from dashboard metrics.
--
-- Behavior:
--   - Stores current timestamp in APP_CONFIG key PERFORMANCE_DASHBOARD_DAY0_TS
--   - Dashboard API uses this as a lower-bound filter (in addition to lookback)
--   - No historical data is deleted

use role MIP_ADMIN_ROLE;
use database MIP;

merge into MIP.APP.APP_CONFIG t
using (
  select
    'PERFORMANCE_DASHBOARD_DAY0_TS' as CONFIG_KEY,
    to_varchar(current_timestamp()) as CONFIG_VALUE,
    'Dashboard baseline start timestamp - excludes legacy noise before this time.' as DESCRIPTION
) s
on t.CONFIG_KEY = s.CONFIG_KEY
when matched then update set
  t.CONFIG_VALUE = s.CONFIG_VALUE,
  t.DESCRIPTION = s.DESCRIPTION,
  t.UPDATED_AT = current_timestamp()
when not matched then insert (CONFIG_KEY, CONFIG_VALUE, DESCRIPTION, UPDATED_AT)
values (s.CONFIG_KEY, s.CONFIG_VALUE, s.DESCRIPTION, current_timestamp());

select CONFIG_KEY, CONFIG_VALUE, DESCRIPTION
from MIP.APP.APP_CONFIG
where CONFIG_KEY = 'PERFORMANCE_DASHBOARD_DAY0_TS';
