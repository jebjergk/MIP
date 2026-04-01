-- 411_entry_intel_append_only_role.sql
-- Role with INSERT + SELECT only on EIS lifecycle tables (UPDATE must fail for this role).
--
-- Requires a role that can CREATE ROLE on the account (typically ACCOUNTADMIN).
-- If this fails with "Insufficient privileges to operate on account", run as ACCOUNTADMIN
-- and record the immutability smoke result in MIP/docs/validation/phase2_entry_intel_validation.md.

use role MIP_ADMIN_ROLE;
use database MIP;

create role if not exists MIP_EIS_APPEND_ONLY;

grant usage on database MIP to role MIP_EIS_APPEND_ONLY;
grant usage on schema MIP.LIVE to role MIP_EIS_APPEND_ONLY;

grant select, insert on table MIP.LIVE.ENTRY_INTEL_SNAPSHOT to role MIP_EIS_APPEND_ONLY;
grant select, insert on table MIP.LIVE.ENTRY_INTEL_ACTION_LINK to role MIP_EIS_APPEND_ONLY;
grant select, insert on table MIP.LIVE.TRADE_CLOSEOUT to role MIP_EIS_APPEND_ONLY;

-- Explicit: no UPDATE/DELETE/TRUNCATE grants (default deny).
