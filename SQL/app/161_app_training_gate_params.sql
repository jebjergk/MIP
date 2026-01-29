-- 161_app_training_gate_params.sql
-- Purpose: Param-driven training gate for V_TRUSTED_PATTERN_HORIZONS (auditable thresholds).

use role MIP_ADMIN_ROLE;
use database MIP;

create table if not exists MIP.APP.TRAINING_GATE_PARAMS (
    PARAM_SET            string   not null,
    MIN_SIGNALS          number   not null,
    MIN_SIGNALS_BOOTSTRAP number  null,   -- bootstrap: allow N_SIGNALS >= this (e.g. 5), mark confidence=LOW
    MIN_HIT_RATE         float    not null,
    MIN_AVG_RETURN       float    not null,
    IS_ACTIVE            boolean  not null default true
);

-- One active row: default thresholds (40 signals full gate; 5 bootstrap; 55% hit rate; 0.05% min avg return).
-- For existing deployments without MIN_SIGNALS_BOOTSTRAP, run 162_alter_training_gate_params_bootstrap.sql once.
merge into MIP.APP.TRAINING_GATE_PARAMS t
using (select 'DEFAULT' as param_set, 40 as min_signals, 5 as min_signals_bootstrap, 0.55 as min_hit_rate, 0.0005 as min_avg_return, true as is_active) s
on t.PARAM_SET = s.PARAM_SET
when not matched then
    insert (PARAM_SET, MIN_SIGNALS, MIN_SIGNALS_BOOTSTRAP, MIN_HIT_RATE, MIN_AVG_RETURN, IS_ACTIVE)
    values (s.PARAM_SET, s.MIN_SIGNALS, s.MIN_SIGNALS_BOOTSTRAP, s.MIN_HIT_RATE, s.MIN_AVG_RETURN, s.IS_ACTIVE);
-- When matched: run 162_alter_training_gate_params_bootstrap.sql to add/backfill MIN_SIGNALS_BOOTSTRAP on existing DBs.
