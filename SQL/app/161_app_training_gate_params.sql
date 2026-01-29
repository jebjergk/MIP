-- 161_app_training_gate_params.sql
-- Purpose: Param-driven training gate for V_TRUSTED_PATTERN_HORIZONS (auditable thresholds).

use role MIP_ADMIN_ROLE;
use database MIP;

create table if not exists MIP.APP.TRAINING_GATE_PARAMS (
    PARAM_SET     string   not null,
    MIN_SIGNALS   number   not null,
    MIN_HIT_RATE  float    not null,
    MIN_AVG_RETURN float   not null,
    IS_ACTIVE     boolean  not null default true
);

-- One active row: default thresholds (40 signals, 55% hit rate, 0.05% min avg return)
merge into MIP.APP.TRAINING_GATE_PARAMS t
using (select 'DEFAULT' as param_set) s
on t.PARAM_SET = s.PARAM_SET
when not matched then
    insert (PARAM_SET, MIN_SIGNALS, MIN_HIT_RATE, MIN_AVG_RETURN, IS_ACTIVE)
    values ('DEFAULT', 40, 0.55, 0.0005, true);
