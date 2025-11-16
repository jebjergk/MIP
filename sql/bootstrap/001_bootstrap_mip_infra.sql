-- ============================================================================
-- Snowflake bootstrap script for MIP infrastructure.
-- This script is safe to rerun and can be executed by a high-level admin role.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- Warehouse creation
-- ---------------------------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS MIP_WH_XS
    WITH WAREHOUSE_SIZE = 'XSMALL'
         AUTO_SUSPEND = 60
         AUTO_RESUME = TRUE
         INITIALLY_SUSPENDED = TRUE;

-- ---------------------------------------------------------------------------
-- Role creation
-- ---------------------------------------------------------------------------
CREATE ROLE IF NOT EXISTS MIP_ADMIN_ROLE;
CREATE ROLE IF NOT EXISTS MIP_APP_ROLE;

-- ---------------------------------------------------------------------------
-- Database and schema creation
-- ---------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS MIP;
CREATE SCHEMA IF NOT EXISTS MIP.MART;
CREATE SCHEMA IF NOT EXISTS MIP.APP;
CREATE SCHEMA IF NOT EXISTS MIP.AGENT_OUT;

-- ---------------------------------------------------------------------------
-- Grants for MIP_ADMIN_ROLE
-- ---------------------------------------------------------------------------
GRANT OWNERSHIP ON DATABASE MIP TO ROLE MIP_ADMIN_ROLE REVOKE CURRENT GRANTS;
GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE MIP TO ROLE MIP_ADMIN_ROLE;
GRANT ALL PRIVILEGES ON WAREHOUSE MIP_WH_XS TO ROLE MIP_ADMIN_ROLE;

-- ---------------------------------------------------------------------------
-- Grants for MIP_APP_ROLE (database and schema usage)
-- ---------------------------------------------------------------------------
GRANT USAGE ON DATABASE MIP TO ROLE MIP_APP_ROLE;
GRANT USAGE ON SCHEMA MIP.MART TO ROLE MIP_APP_ROLE;
GRANT USAGE ON SCHEMA MIP.APP TO ROLE MIP_APP_ROLE;
GRANT USAGE ON SCHEMA MIP.AGENT_OUT TO ROLE MIP_APP_ROLE;
GRANT USAGE ON WAREHOUSE MIP_WH_XS TO ROLE MIP_APP_ROLE;

-- ---------------------------------------------------------------------------
-- Object-level grants for MIP_APP_ROLE (tables/views)
-- ---------------------------------------------------------------------------
GRANT SELECT ON ALL TABLES IN SCHEMA MIP.MART TO ROLE MIP_APP_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA MIP.AGENT_OUT TO ROLE MIP_APP_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA MIP.MART TO ROLE MIP_APP_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA MIP.AGENT_OUT TO ROLE MIP_APP_ROLE;
GRANT SELECT ON ALL VIEWS IN SCHEMA MIP.MART TO ROLE MIP_APP_ROLE;
GRANT SELECT ON ALL VIEWS IN SCHEMA MIP.AGENT_OUT TO ROLE MIP_APP_ROLE;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA MIP.MART TO ROLE MIP_APP_ROLE;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA MIP.AGENT_OUT TO ROLE MIP_APP_ROLE;

-- ---------------------------------------------------------------------------
-- Object-level grants for MIP_APP_ROLE (procedures/functions)
-- ---------------------------------------------------------------------------
GRANT USAGE, EXECUTE ON ALL FUNCTIONS IN SCHEMA MIP.APP TO ROLE MIP_APP_ROLE;
GRANT USAGE, EXECUTE ON FUTURE FUNCTIONS IN SCHEMA MIP.APP TO ROLE MIP_APP_ROLE;
GRANT USAGE, EXECUTE ON ALL PROCEDURES IN SCHEMA MIP.APP TO ROLE MIP_APP_ROLE;
GRANT USAGE, EXECUTE ON FUTURE PROCEDURES IN SCHEMA MIP.APP TO ROLE MIP_APP_ROLE;

-- ---------------------------------------------------------------------------
-- Security model summary
-- ---------------------------------------------------------------------------
-- MIP_ADMIN_ROLE owns the MIP database, schemas, and warehouse, retaining
-- full administrative privileges for managing infrastructure objects.
-- MIP_APP_ROLE has least-privileged access for application workloads: read
-- access to MART/AGENT_OUT data and execution rights on APP code artifacts.
