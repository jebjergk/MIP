-- 357_intra_trust_min_sample_config.sql
-- Purpose: Versioned min-sample overrides for trust by pattern_id+horizon.
-- Baseline behavior remains fixed P_MIN_SAMPLE unless a matching active override exists.

use role MIP_ADMIN_ROLE;
use database MIP;

create table if not exists MIP.APP.INTRA_TRUST_MIN_SAMPLE_CONFIG (
    TRUST_CONFIG_VERSION   string        not null,
    PATTERN_ID             number        not null,
    HORIZON_BARS           number        not null,
    MIN_SAMPLE             number        not null,
    IS_ACTIVE              boolean       not null default true,
    VALID_FROM_TS          timestamp_ntz not null default current_timestamp(),
    VALID_TO_TS            timestamp_ntz,
    NOTES                  string,
    CREATED_AT             timestamp_ntz not null default current_timestamp(),
    UPDATED_AT             timestamp_ntz,
    constraint PK_INTRA_TRUST_MIN_SAMPLE_CONFIG primary key (
        TRUST_CONFIG_VERSION, PATTERN_ID, HORIZON_BARS, VALID_FROM_TS
    )
);
