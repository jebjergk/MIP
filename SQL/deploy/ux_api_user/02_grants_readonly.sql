-- 02_grants_readonly.sql
-- Purpose: Grant read-only access to the UX API role (canonical objects only).
-- Idempotent: safe to run multiple times.
--
-- Placeholders: replace :var with literal before running.
--   :ux_role         default MIP_UI_API_ROLE
--   :warehouse_name  example MIP_WH_XS
--
-- Run as MIP_ADMIN_ROLE or ACCOUNTADMIN.

use role MIP_ADMIN_ROLE;
use database MIP;

grant usage on warehouse MIP_WH_XS to role MIP_UI_API_ROLE;  -- :warehouse_name, :ux_role
grant usage on database MIP to role MIP_UI_API_ROLE;
grant usage on schema MIP.APP       to role MIP_UI_API_ROLE;
grant usage on schema MIP.MART      to role MIP_UI_API_ROLE;
grant usage on schema MIP.AGENT_OUT to role MIP_UI_API_ROLE;

-- Tables (MIP.APP)
grant select on table MIP.APP.PORTFOLIO             to role MIP_UI_API_ROLE;
grant select on table MIP.APP.PORTFOLIO_PROFILE     to role MIP_UI_API_ROLE;
grant select on table MIP.APP.PORTFOLIO_TRADES      to role MIP_UI_API_ROLE;
grant select on table MIP.APP.PORTFOLIO_POSITIONS   to role MIP_UI_API_ROLE;
grant select on table MIP.APP.PORTFOLIO_DAILY       to role MIP_UI_API_ROLE;
grant select on table MIP.APP.PORTFOLIO_EPISODE     to role MIP_UI_API_ROLE;
grant select on table MIP.APP.PORTFOLIO_EPISODE_RESULTS to role MIP_UI_API_ROLE;
-- If PORTFOLIO_RISK_OVERRIDE exists (from 161_app_portfolio_risk_override.sql), uncomment:
-- grant select on table MIP.APP.PORTFOLIO_RISK_OVERRIDE to role MIP_UI_API_ROLE;
grant select on table MIP.APP.MIP_AUDIT_LOG         to role MIP_UI_API_ROLE;
grant select on table MIP.APP.RECOMMENDATION_LOG    to role MIP_UI_API_ROLE;
grant select on table MIP.APP.RECOMMENDATION_OUTCOMES to role MIP_UI_API_ROLE;
grant select on table MIP.APP.INGEST_UNIVERSE       to role MIP_UI_API_ROLE;
grant select on table MIP.APP.PATTERN_DEFINITION    to role MIP_UI_API_ROLE;
grant select on table MIP.APP.TRAINING_GATE_PARAMS  to role MIP_UI_API_ROLE;

-- Tables (MIP.AGENT_OUT)
grant select on table MIP.AGENT_OUT.MORNING_BRIEF   to role MIP_UI_API_ROLE;
grant select on table MIP.AGENT_OUT.ORDER_PROPOSALS to role MIP_UI_API_ROLE;
grant select on table MIP.AGENT_OUT.AGENT_RUN_LOG   to role MIP_UI_API_ROLE;
grant select on table MIP.AGENT_OUT.DAILY_DIGEST_SNAPSHOT to role MIP_UI_API_ROLE;
grant select on table MIP.AGENT_OUT.DAILY_DIGEST_NARRATIVE to role MIP_UI_API_ROLE;
grant select on table MIP.AGENT_OUT.TRAINING_DIGEST_SNAPSHOT to role MIP_UI_API_ROLE;
grant select on table MIP.AGENT_OUT.TRAINING_DIGEST_NARRATIVE to role MIP_UI_API_ROLE;

-- Tables/Views (MIP.MART)
grant select on table MIP.MART.MARKET_BARS          to role MIP_UI_API_ROLE;
grant select on view  MIP.MART.MARKET_RETURNS       to role MIP_UI_API_ROLE;

-- Views
grant select on view MIP.AGENT_OUT.V_MORNING_BRIEF_SUMMARY  to role MIP_UI_API_ROLE;
grant select on view MIP.MART.V_MORNING_BRIEF_JSON          to role MIP_UI_API_ROLE;
grant select on view MIP.MART.V_PORTFOLIO_RISK_GATE         to role MIP_UI_API_ROLE;
grant select on view MIP.MART.V_PORTFOLIO_RISK_STATE        to role MIP_UI_API_ROLE;
grant select on view MIP.APP.V_SIGNALS_ELIGIBLE_TODAY       to role MIP_UI_API_ROLE;
grant select on view MIP.APP.V_PORTFOLIO_ACTIVE_EPISODE    to role MIP_UI_API_ROLE;
grant select on view MIP.APP.V_TRUSTED_SIGNAL_CLASSIFICATION to role MIP_UI_API_ROLE;
grant select on view MIP.MART.V_TRUSTED_SIGNAL_POLICY       to role MIP_UI_API_ROLE;
grant select on view MIP.MART.V_TRUSTED_SIGNALS             to role MIP_UI_API_ROLE;
grant select on view MIP.MART.V_TRUSTED_PATTERN_HORIZONS   to role MIP_UI_API_ROLE;
grant select on view MIP.MART.V_TRUSTED_SIGNALS_LATEST_TS   to role MIP_UI_API_ROLE;

grant select on view MIP.MART.V_PORTFOLIO_RUN_KPIS          to role MIP_UI_API_ROLE;
grant select on view MIP.MART.V_PORTFOLIO_RUN_EVENTS        to role MIP_UI_API_ROLE;
grant select on view MIP.MART.V_PORTFOLIO_EPISODE_RESULTS  to role MIP_UI_API_ROLE;
grant select on view MIP.MART.V_PORTFOLIO_OPEN_POSITIONS_CANONICAL to role MIP_UI_API_ROLE;
grant select on view MIP.MART.V_BAR_INDEX to role MIP_UI_API_ROLE;
grant select on view MIP.MART.V_DAILY_DIGEST_SNAPSHOT_GLOBAL to role MIP_UI_API_ROLE;
grant select on view MIP.MART.V_TRAINING_DIGEST_SNAPSHOT_GLOBAL to role MIP_UI_API_ROLE;
grant select on view MIP.MART.V_TRAINING_DIGEST_SNAPSHOT_SYMBOL to role MIP_UI_API_ROLE;

-- Portfolio Management (220/221): lifecycle events, narrative, views, stored procedures
grant select on table MIP.APP.PORTFOLIO_LIFECYCLE_EVENT          to role MIP_UI_API_ROLE;
grant select on table MIP.AGENT_OUT.PORTFOLIO_LIFECYCLE_NARRATIVE to role MIP_UI_API_ROLE;
grant select on view  MIP.MART.V_PORTFOLIO_LIFECYCLE_TIMELINE    to role MIP_UI_API_ROLE;
grant select on view  MIP.MART.V_PORTFOLIO_LIFECYCLE_SNAPSHOT    to role MIP_UI_API_ROLE;

-- Write stored procedures (EXECUTE AS OWNER — API role only needs USAGE, no direct table writes)
grant usage on procedure MIP.APP.SP_UPSERT_PORTFOLIO(number, varchar, varchar, number, number, varchar)
    to role MIP_UI_API_ROLE;
grant usage on procedure MIP.APP.SP_PORTFOLIO_CASH_EVENT(number, varchar, number, varchar)
    to role MIP_UI_API_ROLE;
grant usage on procedure MIP.APP.SP_ATTACH_PROFILE(number, number)
    to role MIP_UI_API_ROLE;
grant usage on procedure MIP.APP.SP_UPSERT_PORTFOLIO_PROFILE(number, varchar, number, number, number, varchar, number, boolean, number, varchar, number, number, varchar, varchar)
    to role MIP_UI_API_ROLE;
grant usage on procedure MIP.APP.SP_AGENT_GENERATE_PORTFOLIO_NARRATIVE(number, varchar, timestamp_ntz)
    to role MIP_UI_API_ROLE;

-- ═══════════════════════════════════════════════════════════════════════════════
-- FUTURE GRANTS: Auto-grant SELECT on all future tables/views in all MIP schemas
-- Run once as MIP_ADMIN_ROLE or ACCOUNTADMIN. Covers everything created after this.
-- ═══════════════════════════════════════════════════════════════════════════════

use role MIP_ADMIN_ROLE;

-- Schema: MIP.APP
grant select on future tables in schema MIP.APP       to role MIP_UI_API_ROLE;
grant select on future views  in schema MIP.APP       to role MIP_UI_API_ROLE;

-- Schema: MIP.MART
grant select on future tables in schema MIP.MART      to role MIP_UI_API_ROLE;
grant select on future views  in schema MIP.MART      to role MIP_UI_API_ROLE;

-- Schema: MIP.AGENT_OUT
grant select on future tables in schema MIP.AGENT_OUT to role MIP_UI_API_ROLE;
grant select on future views  in schema MIP.AGENT_OUT to role MIP_UI_API_ROLE;