-- alter_order_proposals_signal_linkage.sql
-- Purpose: Add signal linkage columns to agent order proposals

use role MIP_ADMIN_ROLE;
use database MIP;

alter table MIP.AGENT_OUT.ORDER_PROPOSALS
    add column if not exists RECOMMENDATION_ID number(38,0);

alter table MIP.AGENT_OUT.ORDER_PROPOSALS
    add column if not exists SIGNAL_TS timestamp_ntz;

alter table MIP.AGENT_OUT.ORDER_PROPOSALS
    add column if not exists SIGNAL_PATTERN_ID number(38,0);

alter table MIP.AGENT_OUT.ORDER_PROPOSALS
    add column if not exists SIGNAL_INTERVAL_MINUTES number(38,0);

alter table MIP.AGENT_OUT.ORDER_PROPOSALS
    add column if not exists SIGNAL_RUN_ID varchar;

alter table MIP.AGENT_OUT.ORDER_PROPOSALS
    add column if not exists SIGNAL_SNAPSHOT variant;
