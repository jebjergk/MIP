-- 01_create_role_and_user.sql
-- Purpose: Create role and user for the MIP UX API (read-only external app).
-- Idempotent: safe to run multiple times.
--
-- Placeholders: replace :var with literal before running (Snowflake worksheets don't support bind vars).
--   :ux_user  default MIP_UI_API
--   :ux_role  default MIP_UI_API_ROLE
--
-- Run as SECURITYADMIN or role with CREATE ROLE / CREATE USER privileges.

use role SECURITYADMIN;

create role if not exists MIP_UI_API_ROLE;  -- replace :ux_role

create user if not exists MIP_UI_API  -- replace :ux_user
  default_role = MIP_UI_API_ROLE;

grant role MIP_UI_API_ROLE to user MIP_UI_API;
