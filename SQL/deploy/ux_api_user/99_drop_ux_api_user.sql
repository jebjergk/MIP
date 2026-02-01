-- 99_drop_ux_api_user.sql
-- Purpose: Rollback — revoke role from user, drop user and role.
-- Idempotent: safe to run multiple times (IF EXISTS).
--
-- Placeholders: replace :var with literal before running.
--   :ux_user  default MIP_UI_API
--   :ux_role  default MIP_UI_API_ROLE
--
-- Run as SECURITYADMIN or ACCOUNTADMIN.
-- Warning: Use only for rollback.
-- Note: Revoke before drop ensures clean teardown. DROP USER IF EXISTS also removes role grants.

use role SECURITYADMIN;

-- Revoke role from user (no-op if user or grant does not exist; may error on re-run)
revoke role MIP_UI_API_ROLE from user MIP_UI_API;  -- :ux_role, :ux_user

drop user if exists MIP_UI_API;       -- :ux_user
drop role if exists MIP_UI_API_ROLE;  -- :ux_role
