use role MIP_ADMIN_ROLE;
use database MIP;

-- App role needs to read APP tables for the UI
grant select on all tables in schema MIP.APP to role MIP_APP_ROLE;
grant select on future tables in schema MIP.APP to role MIP_APP_ROLE;
grant usage on database mip to role mip_app_role;
grant usage on all schemas in database mip to role mip_app_role;
grant create streamlit on schema mip.app to role mip_app_role;
grant usage on warehouse mip_wh_xs to role mip_app_role;
grant create git repository on schema mip.app to role mip_app_role;
GRANT USAGE ON INTEGRATION repo_jebjergk TO ROLE mip_app_role;
GRANT USAGE ON INTEGRATION MIP_ALPHA_EXTERNAL_ACCESS TO ROLE mip_app_role;
GRANT USAGE ON NETWORK RULE MIP.APP.MIP_ALPHA_NETWORK_RULE TO ROLE mip_app_role;

'MIP_ALPHA_NETWORK_RULE


-- App role should be able to execute all current and future APP procedures
grant usage on all procedures in schema MIP.APP to role MIP_APP_ROLE;
grant usage on future procedures in schema MIP.APP to role MIP_APP_ROLE;

CREATE OR REPLACE API INTEGRATION mip_repo
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/jebjergk/MIP')
  ENABLED = TRUE;

USE ROLE ACCOUNTADMIN;

USE ROLE myco_git_admin;

CREATE OR REPLACE GIT REPOSITORY mip_repo
  API_INTEGRATION = my_git_api_integration
  GIT_CREDENTIALS = my_git_secret
  ORIGIN = 'https://github.com/my-account/snowflake-extensions.git';  

SHOW GRANTS TO ROLE MIP_APP_ROLE;
