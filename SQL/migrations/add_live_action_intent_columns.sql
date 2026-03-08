use role MIP_ADMIN_ROLE;
use database MIP;

alter table MIP.LIVE.LIVE_ACTIONS
    add column if not exists INTENT_SUBMITTED_BY string;

alter table MIP.LIVE.LIVE_ACTIONS
    add column if not exists INTENT_SUBMITTED_TS timestamp_ntz;

alter table MIP.LIVE.LIVE_ACTIONS
    add column if not exists INTENT_APPROVED_BY string;

alter table MIP.LIVE.LIVE_ACTIONS
    add column if not exists INTENT_APPROVED_TS timestamp_ntz;

alter table MIP.LIVE.LIVE_ACTIONS
    add column if not exists INTENT_REFERENCE_ID string;

