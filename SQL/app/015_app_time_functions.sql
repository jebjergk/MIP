-- 015_app_time_functions.sql
-- Purpose: Standardized timestamp helpers

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace function MIP.APP.F_NOW_BERLIN_NTZ()
returns timestamp_ntz
as
$$
    convert_timezone('UTC', 'Europe/Berlin', current_timestamp())::timestamp_ntz
$$;
