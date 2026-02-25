-- 359_seed_intra_trust_min_sample_overrides_v1.sql
-- Purpose: Conservative trust min-sample overrides for high-signal patterns (H04/H08 only).

use role MIP_ADMIN_ROLE;
use database MIP;

merge into MIP.APP.INTRA_TRUST_MIN_SAMPLE_CONFIG t
using (
    select 'OVR_H04H08_V1' as TRUST_CONFIG_VERSION, 301 as PATTERN_ID, 4 as HORIZON_BARS, 15 as MIN_SAMPLE, true as IS_ACTIVE, 'Conservative override for high-signal pattern' as NOTES
    union all
    select 'OVR_H04H08_V1', 301, 8, 15, true, 'Conservative override for high-signal pattern'
    union all
    select 'OVR_H04H08_V1', 302, 4, 12, true, 'Conservative override for high-signal pattern'
    union all
    select 'OVR_H04H08_V1', 302, 8, 12, true, 'Conservative override for high-signal pattern'
    union all
    select 'OVR_H04H08_V1', 303, 4, 12, true, 'Conservative override for high-signal pattern'
    union all
    select 'OVR_H04H08_V1', 303, 8, 12, true, 'Conservative override for high-signal pattern'
) s
on t.TRUST_CONFIG_VERSION = s.TRUST_CONFIG_VERSION
and t.PATTERN_ID = s.PATTERN_ID
and t.HORIZON_BARS = s.HORIZON_BARS
and t.VALID_TO_TS is null
when matched then update set
    t.MIN_SAMPLE = s.MIN_SAMPLE,
    t.IS_ACTIVE = s.IS_ACTIVE,
    t.NOTES = s.NOTES,
    t.UPDATED_AT = current_timestamp()
when not matched then insert (
    TRUST_CONFIG_VERSION, PATTERN_ID, HORIZON_BARS, MIN_SAMPLE, IS_ACTIVE, NOTES
) values (
    s.TRUST_CONFIG_VERSION, s.PATTERN_ID, s.HORIZON_BARS, s.MIN_SAMPLE, s.IS_ACTIVE, s.NOTES
);
