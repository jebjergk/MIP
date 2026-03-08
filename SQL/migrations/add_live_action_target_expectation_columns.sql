use role MIP_ADMIN_ROLE;
use database MIP;

alter table MIP.LIVE.LIVE_ACTIONS
    add column if not exists TARGET_EXPECTATION_SNAPSHOT variant;

alter table MIP.LIVE.LIVE_ACTIONS
    add column if not exists TARGET_OPEN_CONDITION_FACTOR number(9,6);

alter table MIP.LIVE.LIVE_ACTIONS
    add column if not exists TARGET_EXPECTATION_POLICY_VERSION string;

