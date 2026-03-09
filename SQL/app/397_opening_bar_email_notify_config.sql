-- 397_opening_bar_email_notify_config.sql
-- Purpose: Config keys for opening-bar pending-approval email notifications.

use role MIP_ADMIN_ROLE;
use database MIP;

merge into MIP.APP.APP_CONFIG t
using (
    select column1 as CONFIG_KEY, column2 as CONFIG_VALUE, column3 as DESCRIPTION
    from values
        ('OPENING_BAR_EMAIL_NOTIFY_ENABLED', 'true',
         'Master switch for opening-bar pending-approval email notifications'),
        ('OPENING_BAR_EMAIL_RECIPIENTS', 'kenneth.jebjerg@gmail.com',
         'Comma-separated recipients for opening-bar pending-approval emails'),
        ('OPENING_BAR_EMAIL_INTEGRATION', 'MIP_EMAIL_INT',
         'Snowflake EMAIL notification integration name used by SYSTEM$SEND_EMAIL')
) s
on t.CONFIG_KEY = s.CONFIG_KEY
when matched then update set
    t.CONFIG_VALUE = s.CONFIG_VALUE,
    t.DESCRIPTION = s.DESCRIPTION,
    t.UPDATED_AT = current_timestamp()
when not matched then insert (CONFIG_KEY, CONFIG_VALUE, DESCRIPTION)
values (s.CONFIG_KEY, s.CONFIG_VALUE, s.DESCRIPTION);
