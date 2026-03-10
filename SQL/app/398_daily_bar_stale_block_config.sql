-- 398_daily_bar_stale_block_config.sql
-- Purpose: Block new entry proposals when latest 1440 bar is stale.

use role MIP_ADMIN_ROLE;
use database MIP;

merge into MIP.APP.APP_CONFIG t
using (
    select column1 as CONFIG_KEY, column2 as CONFIG_VALUE, column3 as DESCRIPTION
    from values
        ('DAILY_BAR_STALE_BLOCK_ENABLED', 'true',
         'When true, SP_AGENT_PROPOSE_TRADES blocks new entries if latest daily bar age exceeds threshold'),
        ('DAILY_BAR_MAX_AGE_HOURS', '30',
         'Maximum allowed age (hours) of latest 1440 bar before new BUY proposals are blocked')
) s
on t.CONFIG_KEY = s.CONFIG_KEY
when matched then update set
    t.CONFIG_VALUE = s.CONFIG_VALUE,
    t.DESCRIPTION = s.DESCRIPTION,
    t.UPDATED_AT = current_timestamp()
when not matched then insert (CONFIG_KEY, CONFIG_VALUE, DESCRIPTION)
    values (s.CONFIG_KEY, s.CONFIG_VALUE, s.DESCRIPTION);
