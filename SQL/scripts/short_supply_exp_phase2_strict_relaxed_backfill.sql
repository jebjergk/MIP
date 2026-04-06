-- Enable strict + relaxed SHORT (1101–1104), run backfill, restore all to inactive.

use role MIP_ADMIN_ROLE;
use database MIP;
use schema APP;

update MIP.APP.PATTERN_DEFINITION
   set IS_ACTIVE = 'Y', ENABLED = true, UPDATED_AT = current_timestamp()
 where PATTERN_ID in (1101, 1102, 1103, 1104);

call MIP.APP.SP_BACKFILL_SHORT_MOMENTUM(
    '2025-08-01'::date,
    current_date()::date,
    'STOCK',
    1440,
    null,
    null,
    null,
    7,
    false
);

update MIP.APP.PATTERN_DEFINITION
   set IS_ACTIVE = 'N', ENABLED = false, UPDATED_AT = current_timestamp()
 where PATTERN_ID in (1101, 1102, 1103, 1104);
