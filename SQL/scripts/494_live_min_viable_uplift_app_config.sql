-- 494_live_min_viable_uplift_app_config.sql
-- Optional APP_CONFIG keys for live post-committee min-viable sizing (API: live.py).
-- Insert-only for missing keys; does not overwrite existing values.

use role MIP_ADMIN_ROLE;
use database MIP;

merge into MIP.APP.APP_CONFIG t
using (
    select 'LIVE_MIN_VIABLE_UPLIFT_ENABLED' as CONFIG_KEY,
           'true' as CONFIG_VALUE,
           'When true, live committee completion may uplift whole-share qty toward min notional within caps.' as DESCRIPTION
    union all
    select 'LIVE_MIN_ENTRY_NOTIONAL_EUR',
           '150',
           'Minimum entry notional (EUR) for live IB entries. Uplifts whole-share qty toward this floor within caps. Set to 0 to disable notional-floor uplift only.'
    union all
    select 'LIVE_MIN_VIABLE_UPLIFT_MAX_MULT',
           '10',
           'Max uplift: proposed qty may not exceed committee-sized qty times this factor.'
) s
on t.CONFIG_KEY = s.CONFIG_KEY
when not matched then insert (CONFIG_KEY, CONFIG_VALUE, DESCRIPTION, UPDATED_AT)
values (s.CONFIG_KEY, s.CONFIG_VALUE, s.DESCRIPTION, current_timestamp());

-- Apply default notional 150 for existing installs (496 may also bump prior 100 rows).
update MIP.APP.APP_CONFIG
   set CONFIG_VALUE = '150',
       DESCRIPTION = 'Minimum entry notional (EUR) for live IB entries. Uplifts whole-share qty toward this floor within caps. Set to 0 to disable notional-floor uplift only.',
       UPDATED_AT = current_timestamp()
 where CONFIG_KEY = 'LIVE_MIN_ENTRY_NOTIONAL_EUR';
