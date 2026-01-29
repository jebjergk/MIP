-- order_proposals_run_id_varchar_phase_a.sql
-- Phase A: Add RUN_ID_VARCHAR, backfill, use for filters. Keep RUN_ID (numeric) and SIGNAL_RUN_ID temporarily.

use role MIP_ADMIN_ROLE;
use database MIP;

alter table MIP.AGENT_OUT.ORDER_PROPOSALS
    add column if not exists RUN_ID_VARCHAR varchar(64);

update MIP.AGENT_OUT.ORDER_PROPOSALS
   set RUN_ID_VARCHAR = coalesce(nullif(trim(SIGNAL_RUN_ID), ''), to_varchar(RUN_ID))
 where RUN_ID_VARCHAR is null
   and (SIGNAL_RUN_ID is not null or RUN_ID is not null);
