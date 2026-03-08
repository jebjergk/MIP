use role MIP_ADMIN_ROLE;
use database MIP;

alter table MIP.LIVE.LIVE_ACTIONS
    add column if not exists REVALIDATION_OUTCOME string;

alter table MIP.LIVE.LIVE_ACTIONS
    add column if not exists REVALIDATION_POLICY_VERSION string;

alter table MIP.LIVE.LIVE_ACTIONS
    add column if not exists REVALIDATION_DATA_SOURCE string;

