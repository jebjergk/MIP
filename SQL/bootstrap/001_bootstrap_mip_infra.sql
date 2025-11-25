-- 001_bootstrap_mip_infra.sql
-- Purpose: Create core infra for the Market Intelligence Platform (MIP)
-- Run as a powerful admin role (e.g. SECURITYADMIN / SYSADMIN)

-----------------------
-- 1. Roles
-----------------------
-- Use SECURITYADMIN for role management
use role SECURITYADMIN;

create role if not exists MIP_ADMIN_ROLE;
create role if not exists MIP_APP_ROLE;

-- Optionally: future agent read role (for AI/agents later)
create role if not exists MIP_AGENT_READ_ROLE;

-----------------------
-- 2. Warehouse
-----------------------
-- Use SYSADMIN (or equivalent) for warehouses and databases
use role SYSADMIN;

create warehouse if not exists MIP_WH_XS
  warehouse_size = 'XSMALL'
  auto_suspend = 60
  auto_resume = true
  initially_suspended = true
  comment = 'XS warehouse for MIP ingestion, analytics and Streamlit';

-----------------------
-- 3. Database & Schemas
-----------------------
create database if not exists MIP
  comment = 'Market Intelligence Platform database';

grant usage on database MIP to role SYSADMIN;
grant create schema on database MIP to role SYSADMIN;
 
  
create schema if not exists MIP.RAW_EXT
  comment = 'Raw external market data (API responses, staging)';

create schema if not exists MIP.MART
  comment = 'Analytic models and views for MIP';

create schema if not exists MIP.APP
  comment = 'Application config, stored procedures, support tables';

create schema if not exists MIP.AGENT_OUT
  comment = 'Future agentic AI / narrative outputs';

-----------------------
-- 4. Grants to roles
-----------------------
-- Admin role: full control over MIP objects and warehouse
grant usage on warehouse MIP_WH_XS to role MIP_ADMIN_ROLE;
grant all privileges on warehouse MIP_WH_XS to role MIP_ADMIN_ROLE;

grant ownership on database MIP to role MIP_ADMIN_ROLE revoke current grants;
grant all privileges on database MIP to role MIP_ADMIN_ROLE;

GRANT OWNERSHIP ON SCHEMA MIP.RAW_EXT TO ROLE MIP_ADMIN_ROLE REVOKE CURRENT GRANTS;

grant ownership on schema MIP.RAW_EXT to role MIP_ADMIN_ROLE revoke current grants;
grant ownership on schema MIP.MART     to role MIP_ADMIN_ROLE revoke current grants;
grant ownership on schema MIP.APP      to role MIP_ADMIN_ROLE revoke current grants;
grant ownership on schema MIP.AGENT_OUT to role MIP_ADMIN_ROLE revoke current grants;

-- App role: minimal rights for Streamlit runtime / app
grant usage on warehouse MIP_WH_XS to role MIP_APP_ROLE;

grant usage on database MIP to role MIP_APP_ROLE;

grant usage on schema MIP.MART      to role MIP_APP_ROLE;
grant usage on schema MIP.APP       to role MIP_APP_ROLE;
grant usage on schema MIP.AGENT_OUT to role MIP_APP_ROLE;
grant usage on schema MIP.RAW_EXT   to role MIP_APP_ROLE;  -- read-only access to raw if needed

-- App role: read MART and AGENT_OUT, execute APP
grant select on all tables in schema MIP.MART       to role MIP_APP_ROLE;
grant select on all views  in schema MIP.MART       to role MIP_APP_ROLE;
grant select on all tables in schema MIP.AGENT_OUT  to role MIP_APP_ROLE;
grant select on all views  in schema MIP.AGENT_OUT  to role MIP_APP_ROLE;

grant select on future tables in schema MIP.MART       to role MIP_APP_ROLE;
grant select on future views  in schema MIP.MART       to role MIP_APP_ROLE;
grant select on future tables in schema MIP.AGENT_OUT  to role MIP_APP_ROLE;
grant select on future views  in schema MIP.AGENT_OUT  to role MIP_APP_ROLE;

-- Allow app role to execute procs/functions in APP
grant usage   on schema MIP.APP to role MIP_APP_ROLE;
grant usage on all procedures in schema MIP.APP to role MIP_APP_ROLE;
grant usage on all functions  in schema MIP.APP to role MIP_APP_ROLE;
grant usage on future procedures in schema MIP.APP to role MIP_APP_ROLE;
grant usage on future functions  in schema MIP.APP to role MIP_APP_ROLE;

-----------------------
-- 5. Notes / handover
-----------------------
-- Grant roles to actual users manually (examples, adjust usernames/roles):
--   grant role MIP_ADMIN_ROLE to user <ADMIN_USER>;
--   grant role MIP_APP_ROLE   to user <APP_USER>;
--
-- Security model:
--   - MIP_ADMIN_ROLE: full control over MIP (used by dev/ops only).
--   - MIP_APP_ROLE: runtime role for Streamlit; read MART/AGENT_OUT, execute APP SPs.
--   - MIP_AGENT_READ_ROLE: reserved for future AI agents with read-only access.
