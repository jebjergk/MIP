-- 166_trust_table_renames.sql
-- Purpose: Resolve table/view name collisions by renaming storage tables

use role MIP_ADMIN_ROLE;
use database MIP;

alter table if exists MIP.APP.V_TRUSTED_SIGNAL_CLASSIFICATION
    rename to MIP.APP.T_TRUSTED_SIGNAL_CLASSIFICATION;

alter table if exists MIP.MART.REC_PATTERN_TRUST_RANKING
    rename to MIP.MART.T_REC_PATTERN_TRUST_RANKING;

alter table if exists MIP.MART.V_TRUSTED_SIGNALS
    rename to MIP.MART.T_TRUSTED_SIGNALS;

alter table if exists MIP.MART.V_TRUSTED_SIGNAL_POLICY
    rename to MIP.MART.T_TRUSTED_SIGNAL_POLICY;

alter table if exists MIP.MART.V_TRUST_METRICS
    rename to MIP.MART.T_TRUST_METRICS;
