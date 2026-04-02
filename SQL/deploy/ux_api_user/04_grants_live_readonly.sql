-- 04_grants_live_readonly.sql
-- Purpose: MIP.LIVE read access for MIP_UI_API_ROLE (symbol tracker, LIC bootstrap, entry intel, committee).
-- Run as MIP_ADMIN_ROLE. Idempotent.

use role MIP_ADMIN_ROLE;
use database MIP;

grant usage on schema MIP.LIVE to role MIP_UI_API_ROLE;

grant select on table MIP.LIVE.LIVE_PORTFOLIO_CONFIG to role MIP_UI_API_ROLE;
grant select on table MIP.LIVE.BROKER_SNAPSHOTS to role MIP_UI_API_ROLE;
grant select on table MIP.LIVE.LIVE_ORDERS to role MIP_UI_API_ROLE;
grant select on table MIP.LIVE.LIVE_ACTIONS to role MIP_UI_API_ROLE;
grant select on table MIP.LIVE.ENTRY_INTEL_SNAPSHOT to role MIP_UI_API_ROLE;
grant select on table MIP.LIVE.ENTRY_INTEL_ACTION_LINK to role MIP_UI_API_ROLE;
grant select on table MIP.LIVE.TRADE_CLOSEOUT to role MIP_UI_API_ROLE;
grant select on table MIP.LIVE.COMMITTEE_RUN to role MIP_UI_API_ROLE;
grant select on table MIP.LIVE.COMMITTEE_VERDICT to role MIP_UI_API_ROLE;
grant select on table MIP.LIVE.COMMITTEE_ROLE_OUTPUT to role MIP_UI_API_ROLE;

-- Lifecycle broker reconciliation (RECON_V1). INSERT/UPDATE granted in migration 20260401_lifecycle_reconciliation_v1.sql.
grant select on table MIP.LIVE.LIFECYCLE_RECONCILIATION_STATE to role MIP_UI_API_ROLE;
grant select on table MIP.LIVE.LIFECYCLE_RECONCILIATION_EVENT to role MIP_UI_API_ROLE;

-- Future grants may require schema owner / higher privilege; optional if you add new LIVE objects often:
-- grant select on future tables in schema MIP.LIVE to role MIP_UI_API_ROLE;
-- grant select on future views in schema MIP.LIVE to role MIP_UI_API_ROLE;
