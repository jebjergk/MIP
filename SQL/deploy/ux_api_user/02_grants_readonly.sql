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

-- Tables/Views (MIP.MART)
grant select on table MIP.MART.MARKET_BARS          to role MIP_UI_API_ROLE;
grant select on view  MIP.MART.MARKET_RETURNS       to role MIP_UI_API_ROLE;

-- Views
grant select on view MIP.AGENT_OUT.V_MORNING_BRIEF_SUMMARY  to role MIP_UI_API_ROLE;
grant select on view MIP.MART.V_MORNING_BRIEF_JSON          to role MIP_UI_API_ROLE;
grant select on view MIP.MART.V_PORTFOLIO_RISK_GATE         to role MIP_UI_API_ROLE;
grant select on view MIP.MART.V_PORTFOLIO_RISK_STATE        to role MIP_UI_API_ROLE;
grant select on view MIP.APP.V_SIGNALS_ELIGIBLE_TODAY       to role MIP_UI_API_ROLE;
grant select on view MIP.MART.V_TRUSTED_SIGNAL_POLICY       to role MIP_UI_API_ROLE;
grant select on view MIP.MART.V_TRUSTED_SIGNALS             to role MIP_UI_API_ROLE;
grant select on view MIP.MART.V_TRUSTED_SIGNALS_LATEST_TS   to role MIP_UI_API_ROLE;
grant select on view MIP.MART.V_PORTFOLIO_RUN_KPIS          to role MIP_UI_API_ROLE;
grant select on view MIP.MART.V_PORTFOLIO_RUN_EVENTS        to role MIP_UI_API_ROLE;
grant select on view MIP.MART.V_PORTFOLIO_OPEN_POSITIONS_CANONICAL to role MIP_UI_API_ROLE;
grant select on view MIP.MART.V_BAR_INDEX to role MIP_UI_API_ROLE;
