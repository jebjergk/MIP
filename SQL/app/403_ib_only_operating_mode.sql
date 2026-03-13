-- 403_ib_only_operating_mode.sql
-- Purpose: Enforce IB-only execution mode while preserving research/training.

use role MIP_ADMIN_ROLE;
use database MIP;

merge into MIP.APP.APP_CONFIG t
using (
    select 'SIM_EXECUTION_ENABLED' as CONFIG_KEY, 'false' as CONFIG_VALUE,
           'If false, all simulation execution writes are disabled (research remains enabled).' as DESCRIPTION
    union all
    select 'EARLY_EXIT_ENABLED', 'false',
           'Disable autonomous early-exit loop in IB-only/manual validation operating model.'
    union all
    select 'INTRADAY_ENABLED', 'false',
           'Disable intraday autonomous pipeline in IB-only/manual validation operating model.'
) s
on t.CONFIG_KEY = s.CONFIG_KEY
when matched then update set
  t.CONFIG_VALUE = s.CONFIG_VALUE,
  t.DESCRIPTION = s.DESCRIPTION,
  t.UPDATED_AT = current_timestamp()
when not matched then insert (CONFIG_KEY, CONFIG_VALUE, DESCRIPTION, UPDATED_AT)
values (s.CONFIG_KEY, s.CONFIG_VALUE, s.DESCRIPTION, current_timestamp());
