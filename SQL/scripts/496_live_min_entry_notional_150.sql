-- 496_live_min_entry_notional_150.sql
-- Raise default LIVE_MIN_ENTRY_NOTIONAL_EUR to 150 (controlled step; API default matches live.py).
-- Idempotent: updates existing row; merge inserts 150 for new installs.

use role MIP_ADMIN_ROLE;
use database MIP;

merge into MIP.APP.APP_CONFIG t
using (
    select 'LIVE_MIN_ENTRY_NOTIONAL_EUR' as CONFIG_KEY,
           '150' as CONFIG_VALUE,
           'Minimum entry notional (EUR) for live IB entries. Uplifts whole-share qty toward this floor within caps. Set to 0 to disable notional-floor uplift only.' as DESCRIPTION
) s
on t.CONFIG_KEY = s.CONFIG_KEY
when matched then
  update set
    CONFIG_VALUE = s.CONFIG_VALUE,
    DESCRIPTION = s.DESCRIPTION,
    UPDATED_AT = current_timestamp()
when not matched then
  insert (CONFIG_KEY, CONFIG_VALUE, DESCRIPTION, UPDATED_AT)
  values (s.CONFIG_KEY, s.CONFIG_VALUE, s.DESCRIPTION, current_timestamp());
