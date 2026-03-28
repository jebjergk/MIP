-- Ask MIP 2.0 / v3 telemetry — additive columns on ASK_QUERY_EVENT.
-- Run once per account (ignore errors if columns already exist).

use role MIP_ADMIN_ROLE;
use database MIP;

alter table MIP.AGENT_OUT.ASK_QUERY_EVENT add column if not exists PAGE_ID varchar(256);
alter table MIP.AGENT_OUT.ASK_QUERY_EVENT add column if not exists SNOWFLAKE_FACT_LOOKUP boolean default false;
alter table MIP.AGENT_OUT.ASK_QUERY_EVENT add column if not exists RETRIEVAL_SOURCE_GROUPS variant;
