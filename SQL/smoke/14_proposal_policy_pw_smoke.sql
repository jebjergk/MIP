-- 14_proposal_policy_pw_smoke.sql
-- Validates proposal policy manifest/rules, PW portfolio context view, and recent proposals
-- carry policy + PW enrichment columns after SP_AGENT_PROPOSE_TRADES deploy.

use role MIP_ADMIN_ROLE;
use database MIP;

-- 1) Policy manifest has exactly one default active row
select 'policy_manifest_default' as check_name, POLICY_VERSION, IS_DEFAULT, IS_ACTIVE
from MIP.APP.PROPOSAL_POLICY_MANIFEST
where coalesce(IS_DEFAULT, false)
  and coalesce(IS_ACTIVE, true);

-- 2) Rules exist for v1 families
select 'policy_rule_count' as check_name, count(*) as rule_rows
from MIP.APP.PROPOSAL_POLICY_RULE
where POLICY_VERSION = '2026_04_03_V1';

-- 3) PW context view compiles (may return 0 rows if no PW data)
select 'pw_portfolio_context_sample' as check_name, *
from MIP.MART.V_PROPOSAL_PW_PORTFOLIO_CONTEXT
limit 3;

-- 4) Forward MR/trusted signals outside enabled ingest universe must not appear as proposal candidates
--    (structural check: count mismatch diagnostic — run after daily propose)
with trading_days as (
  select distinct TS::date as d
  from MIP.MART.MARKET_BARS
  where MARKET_TYPE = 'STOCK' and INTERVAL_MINUTES = 1440
  order by d desc limit 5
),
iu as (
  select upper(trim(SYMBOL)) su, upper(trim(MARKET_TYPE)) mt
  from MIP.APP.INGEST_UNIVERSE
  where coalesce(IS_ENABLED, true) and INTERVAL_MINUTES = 1440
),
recent as (
  select op.*
  from MIP.AGENT_OUT.ORDER_PROPOSALS op
  where op.PROPOSED_AT::date in (select d from trading_days)
    and op.STATUS = 'PROPOSED'
    -- Post-deploy proposals only (policy version stamped by new SP)
    and op.PROPOSAL_POLICY_VERSION is not null
)
select
  'proposals_outside_enabled_universe_post_policy' as check_name,
  count(*) as violation_count
from recent r
where not exists (
  select 1 from iu
  where iu.su = upper(trim(r.SYMBOL))
    and iu.mt = upper(trim(r.MARKET_TYPE))
);

-- 5) Recent proposals should expose policy + diagnostics when populated
select
  'recent_proposal_audit_columns' as check_name,
  PROPOSAL_ID,
  PROPOSAL_POLICY_VERSION,
  PROPOSAL_DIAGNOSTICS:policy_eligible::boolean as policy_ok,
  PW_ENRICHMENT:pw_available::boolean as pw_ok
from MIP.AGENT_OUT.ORDER_PROPOSALS
where PROPOSED_AT >= dateadd(day, -14, current_date())
order by PROPOSED_AT desc
limit 10;
