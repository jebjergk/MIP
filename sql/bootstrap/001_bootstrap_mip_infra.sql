-- ============================================================================
-- Bootstrap the core Snowflake infrastructure for the MIP project.
-- The statements below are idempotent and reflect the existing manual setup.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- Database and schema creation (includes fix for prior APP schema typo).
-- ---------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS MIP;

CREATE SCHEMA IF NOT EXISTS MIP.RAW_EXT;
CREATE SCHEMA IF NOT EXISTS MIP.MART;
CREATE SCHEMA IF NOT EXISTS MIP.APP;       -- previously mistyped as IP.RAW_EXT
CREATE SCHEMA IF NOT EXISTS MIP.AGENT_OUT;

-- ---------------------------------------------------------------------------
-- Warehouse creation.
-- ---------------------------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS MIP_WH_XS
    WITH WAREHOUSE_SIZE = 'XSMALL'
         AUTO_SUSPEND = 60
         AUTO_RESUME = TRUE
         INITIALLY_SUSPENDED = TRUE;

CREATE WAREHOUSE IF NOT EXISTS MIP_WH_S
    WITH WAREHOUSE_SIZE = 'SMALL'
         AUTO_SUSPEND = 60
         AUTO_RESUME = TRUE
         INITIALLY_SUSPENDED = TRUE;

-- ---------------------------------------------------------------------------
-- Role creation.
-- ---------------------------------------------------------------------------
CREATE ROLE IF NOT EXISTS MIP_ADMIN_ROLE;
CREATE ROLE IF NOT EXISTS MIP_APP_ROLE;
CREATE ROLE IF NOT EXISTS MIP_AGENT_READ_ROLE;

-- Example user grants (replace <USERNAME> and uncomment as needed):
-- GRANT ROLE MIP_ADMIN_ROLE TO USER <USERNAME>;
-- GRANT ROLE MIP_APP_ROLE TO USER <USERNAME>;
-- GRANT ROLE MIP_AGENT_READ_ROLE TO USER <USERNAME>;

-- ---------------------------------------------------------------------------
-- Grants for MIP_ADMIN_ROLE.
-- ---------------------------------------------------------------------------
GRANT USAGE ON DATABASE MIP TO ROLE MIP_ADMIN_ROLE;
GRANT USAGE ON ALL SCHEMAS IN DATABASE MIP TO ROLE MIP_ADMIN_ROLE;
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE MIP TO ROLE MIP_ADMIN_ROLE;

GRANT USAGE, ALL PRIVILEGES ON SCHEMA MIP.RAW_EXT TO ROLE MIP_ADMIN_ROLE;
GRANT USAGE, ALL PRIVILEGES ON SCHEMA MIP.MART TO ROLE MIP_ADMIN_ROLE;
GRANT USAGE, ALL PRIVILEGES ON SCHEMA MIP.APP TO ROLE MIP_ADMIN_ROLE;
GRANT USAGE, ALL PRIVILEGES ON SCHEMA MIP.AGENT_OUT TO ROLE MIP_ADMIN_ROLE;

GRANT USAGE ON WAREHOUSE MIP_WH_XS TO ROLE MIP_ADMIN_ROLE;
GRANT USAGE ON WAREHOUSE MIP_WH_S TO ROLE MIP_ADMIN_ROLE;

-- ---------------------------------------------------------------------------
-- Grants for MIP_APP_ROLE.
-- ---------------------------------------------------------------------------
GRANT USAGE ON DATABASE MIP TO ROLE MIP_APP_ROLE;
GRANT USAGE ON SCHEMA MIP.MART TO ROLE MIP_APP_ROLE;
GRANT USAGE ON SCHEMA MIP.APP TO ROLE MIP_APP_ROLE;
GRANT USAGE ON SCHEMA MIP.AGENT_OUT TO ROLE MIP_APP_ROLE;
GRANT CREATE STREAMLIT ON SCHEMA MIP.APP TO ROLE MIP_APP_ROLE;
GRANT USAGE ON WAREHOUSE MIP_WH_XS TO ROLE MIP_APP_ROLE;

-- ---------------------------------------------------------------------------
-- Imported data share privileges for all relevant roles.
-- ---------------------------------------------------------------------------
GRANT IMPORTED PRIVILEGES ON DATABASE PUBLIC_DOMAIN_DATA TO ROLE MIP_ADMIN_ROLE;
GRANT IMPORTED PRIVILEGES ON DATABASE PUBLIC_DOMAIN_DATA TO ROLE MIP_APP_ROLE;
GRANT IMPORTED PRIVILEGES ON DATABASE PUBLIC_DOMAIN_DATA TO ROLE MIP_AGENT_READ_ROLE;

GRANT IMPORTED PRIVILEGES ON DATABASE TRADERMADE_CURRENCY_EXCHANGE_RATES TO ROLE MIP_ADMIN_ROLE;
GRANT IMPORTED PRIVILEGES ON DATABASE TRADERMADE_CURRENCY_EXCHANGE_RATES TO ROLE MIP_APP_ROLE;
GRANT IMPORTED PRIVILEGES ON DATABASE TRADERMADE_CURRENCY_EXCHANGE_RATES TO ROLE MIP_AGENT_READ_ROLE;

-- ---------------------------------------------------------------------------
-- End of bootstrap script.
-- ---------------------------------------------------------------------------
