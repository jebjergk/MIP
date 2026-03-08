use role MIP_ADMIN_ROLE;
use database MIP;

alter table MIP.LIVE.LIVE_ACTIONS
    add column if not exists NEWS_CONTEXT_SNAPSHOT variant;

alter table MIP.LIVE.LIVE_ACTIONS
    add column if not exists NEWS_CONTEXT_STATE string;

alter table MIP.LIVE.LIVE_ACTIONS
    add column if not exists NEWS_EVENT_SHOCK_FLAG boolean;

alter table MIP.LIVE.LIVE_ACTIONS
    add column if not exists NEWS_FRESHNESS_BUCKET string;

alter table MIP.LIVE.LIVE_ACTIONS
    add column if not exists NEWS_CONTEXT_POLICY_VERSION string;

