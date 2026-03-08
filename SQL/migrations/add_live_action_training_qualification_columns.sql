use role MIP_ADMIN_ROLE;
use database MIP;

alter table MIP.LIVE.LIVE_ACTIONS
    add column if not exists TRAINING_QUALIFICATION_SNAPSHOT variant;

alter table MIP.LIVE.LIVE_ACTIONS
    add column if not exists TRAINING_LIVE_ELIGIBLE boolean;

alter table MIP.LIVE.LIVE_ACTIONS
    add column if not exists TRAINING_RANK_IMPACT string;

alter table MIP.LIVE.LIVE_ACTIONS
    add column if not exists TRAINING_SIZE_CAP_FACTOR number(9,6);

