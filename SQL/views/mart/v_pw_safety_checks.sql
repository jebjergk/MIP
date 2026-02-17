-- v_pw_safety_checks.sql
-- Purpose: Explodes the SAFETY_DETAIL JSON array from recommendations
-- into individual check rows for UI display.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_PW_SAFETY_CHECKS (
    REC_ID,
    PORTFOLIO_ID,
    SWEEP_FAMILY,
    RECOMMENDATION_TYPE,
    CHECK_NAME,
    PASSED,
    THRESHOLD,
    ACTUAL_VALUE,
    EXPLANATION
) as
select
    r.REC_ID,
    r.PORTFOLIO_ID,
    r.SWEEP_FAMILY,
    r.RECOMMENDATION_TYPE,
    c.value:check::varchar     as CHECK_NAME,
    c.value:passed::boolean    as PASSED,
    c.value:threshold::varchar as THRESHOLD,
    c.value:actual::varchar    as ACTUAL_VALUE,
    c.value:explanation::varchar as EXPLANATION
from MIP.APP.PARALLEL_WORLD_RECOMMENDATION r,
     lateral flatten(input => r.SAFETY_DETAIL) c
where r.SAFETY_DETAIL is not null
  and r.APPROVAL_STATUS != 'STALE';
