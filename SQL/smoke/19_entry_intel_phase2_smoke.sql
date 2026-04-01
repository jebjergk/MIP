-- 19_entry_intel_phase2_smoke.sql
-- Phase 2: WORLDS_SPEC / ALPHA_SPEC shape, determinism, reconstruction template.
-- Run as MIP_ADMIN_ROLE. Requires 410 deployed (F_BUILD + EIS_SCHEMA_V2 procs).

use role MIP_ADMIN_ROLE;
use database MIP;

-- Log + function exist
select 'EIS_FAILURE_LOG' as check_label, count(*) as cnt
from information_schema.tables
where table_schema = 'APP'
  and table_name = 'EIS_ENSURE_FAILURE_LOG';

select 'F_BUILD_EXISTS' as check_label, count(*) as cnt
from information_schema.functions
where function_schema = 'APP'
  and function_name = 'F_BUILD_ENTRY_INTEL_FOR_PROPOSAL';

-- Latest proposal for dynamic checks
set smoke_proposal_id = (select max(PROPOSAL_ID) from MIP.AGENT_OUT.ORDER_PROPOSALS);

-- Determinism: two evaluations same JSON (stable given unchanged HOD source data)
select 'DETERMINISM_F_BUILD' as check_label,
       to_json(MIP.APP.F_BUILD_ENTRY_INTEL_FOR_PROPOSAL($smoke_proposal_id))
       = to_json(MIP.APP.F_BUILD_ENTRY_INTEL_FOR_PROPOSAL($smoke_proposal_id)) as ok;

-- WORLDS_SPEC / ALPHA_SPEC required keys (on builder output, always non-null for existing proposal)
select 'WORLDS_SCHEMA' as check_label,
       MIP.APP.F_BUILD_ENTRY_INTEL_FOR_PROPOSAL($smoke_proposal_id):WORLDS_SPEC:schema_version::string as v;
select 'ALPHA_SCHEMA' as check_label,
       MIP.APP.F_BUILD_ENTRY_INTEL_FOR_PROPOSAL($smoke_proposal_id):ALPHA_SPEC:alpha_schema_version::string as v;
select 'ALPHA_ACTION' as check_label,
       MIP.APP.F_BUILD_ENTRY_INTEL_FOR_PROPOSAL($smoke_proposal_id):ALPHA_SPEC:recommended_action::string as v;
select 'ALPHA_SIZE_BAND' as check_label,
       MIP.APP.F_BUILD_ENTRY_INTEL_FOR_PROPOSAL($smoke_proposal_id):ALPHA_SPEC:recommended_size_band::string as v;
select 'HOD_SAMPLE' as check_label,
       MIP.APP.F_BUILD_ENTRY_INTEL_FOR_PROPOSAL($smoke_proposal_id):WORLDS_SPEC:historical_distribution:sample_size::int as n;

-- Probability sanity: either insufficient_sample or three probs sum ~ 1
select 'PROB_SUM' as check_label,
       coalesce(
           MIP.APP.F_BUILD_ENTRY_INTEL_FOR_PROPOSAL($smoke_proposal_id):WORLDS_SPEC:supporting:insufficient_sample::boolean,
           false
       ) as insufficient,
       (
           coalesce(MIP.APP.F_BUILD_ENTRY_INTEL_FOR_PROPOSAL($smoke_proposal_id):WORLDS_SPEC:historical_distribution:upside_probability::float, 0)
         + coalesce(MIP.APP.F_BUILD_ENTRY_INTEL_FOR_PROPOSAL($smoke_proposal_id):WORLDS_SPEC:historical_distribution:base_probability::float, 0)
         + coalesce(MIP.APP.F_BUILD_ENTRY_INTEL_FOR_PROPOSAL($smoke_proposal_id):WORLDS_SPEC:historical_distribution:downside_probability::float, 0)
       ) as prob_sum;

-- Persisted EIS_SCHEMA_V2 rows (may be 0 until new proposals are inserted post-deploy; existing v1 stubs are not upgraded)
select 'EIS_V2_ROWS' as check_label, count(*) as cnt
from MIP.LIVE.ENTRY_INTEL_SNAPSHOT
where SOURCE_VERSION = 'EIS_SCHEMA_V2';

-- Reconstruction template: set smoke_entry_action to a real ENTRY_ACTION_ID when you have one
-- select e.SNAPSHOT_ID, e.WORLDS_SPEC:schema_version, e.ALPHA_SPEC:recommended_action, tc.CLOSEOUT_ID
-- from MIP.LIVE.ENTRY_INTEL_ACTION_LINK l
-- join MIP.LIVE.ENTRY_INTEL_SNAPSHOT e on e.SNAPSHOT_ID = l.SNAPSHOT_ID
-- left join MIP.LIVE.TRADE_CLOSEOUT tc on tc.ENTRY_ACTION_ID = l.ENTRY_ACTION_ID
-- where l.ENTRY_ACTION_ID = $smoke_entry_action;
