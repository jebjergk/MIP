-- 162_alter_training_gate_params_bootstrap.sql
-- Purpose: Add MIN_SIGNALS_BOOTSTRAP for bootstrap-mode trust gating (run once on existing DBs).

use role MIP_ADMIN_ROLE;
use database MIP;

-- Add column (fails harmlessly if column already exists; run once per deployment)
alter table MIP.APP.TRAINING_GATE_PARAMS add column MIN_SIGNALS_BOOTSTRAP number default 5;

update MIP.APP.TRAINING_GATE_PARAMS set MIN_SIGNALS_BOOTSTRAP = 5 where MIN_SIGNALS_BOOTSTRAP is null;
