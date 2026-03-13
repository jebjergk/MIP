-- 403_symbol_local_gate_config.sql
-- Purpose: Configure symbol-local health gate for live proposal safety.
-- Default is shadow mode (enforcement off) to avoid behavior changes.

use role MIP_ADMIN_ROLE;
use database MIP;

merge into MIP.APP.APP_CONFIG t
using (
    select column1 as CONFIG_KEY, column2 as CONFIG_VALUE, column3 as DESCRIPTION
    from values
        ('SYMBOL_LOCAL_GATE_ENABLED', 'true',
         'Master switch for symbol-local health diagnostics in SP_AGENT_PROPOSE_TRADES'),
        ('SYMBOL_LOCAL_GATE_ENFORCE', 'false',
         'When true, SP_AGENT_PROPOSE_TRADES blocks symbols failing local health checks'),
        ('SYMBOL_LOCAL_MIN_RECENT_HIT_RATE', '0.50',
         'Minimum recent symbol-level hit rate required for local gate pass'),
        ('SYMBOL_LOCAL_MIN_RECENT_AVG_RETURN', '0.00',
         'Minimum recent symbol-level average return required for local gate pass'),
        ('SYMBOL_LOCAL_MIN_RECS', '8',
         'Minimum symbol-level recommendation sample size required for local gate pass')
) s
on t.CONFIG_KEY = s.CONFIG_KEY
when matched then update set
    t.CONFIG_VALUE = s.CONFIG_VALUE,
    t.DESCRIPTION = s.DESCRIPTION,
    t.UPDATED_AT = current_timestamp()
when not matched then insert (CONFIG_KEY, CONFIG_VALUE, DESCRIPTION, UPDATED_AT)
values (s.CONFIG_KEY, s.CONFIG_VALUE, s.DESCRIPTION, current_timestamp());

select
    CONFIG_KEY,
    CONFIG_VALUE,
    DESCRIPTION
from MIP.APP.APP_CONFIG
where CONFIG_KEY like 'SYMBOL_LOCAL_%'
order by CONFIG_KEY;
