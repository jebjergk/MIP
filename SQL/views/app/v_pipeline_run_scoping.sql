-- v_pipeline_run_scoping.sql
-- Purpose: Helper view to normalize run ID scoping across pipeline and signal runs

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.APP.V_PIPELINE_RUN_SCOPING as
with pipeline_runs as (
    select distinct
        RUN_ID as PIPELINE_RUN_ID,
        EVENT_TS as PIPELINE_START_TS
    from MIP.APP.MIP_AUDIT_LOG
    where EVENT_TYPE = 'PIPELINE'
      and EVENT_NAME = 'SP_RUN_DAILY_PIPELINE'
),
signal_runs as (
    select distinct
        RUN_ID as SIGNAL_RUN_ID_STRING,
        try_to_number(replace(RUN_ID, 'T', '')) as SIGNAL_RUN_ID_NUMBER,
        max(GENERATED_AT) as SIGNAL_GENERATED_AT
    from MIP.APP.RECOMMENDATION_LOG
    group by RUN_ID
),
proposal_runs as (
    select distinct
        RUN_ID as PROPOSAL_RUN_ID,
        SIGNAL_RUN_ID,
        PIPELINE_RUN_ID
    from MIP.AGENT_OUT.ORDER_PROPOSALS
    where RUN_ID is not null
)
select
    pr.PIPELINE_RUN_ID,
    sr.SIGNAL_RUN_ID_STRING,
    sr.SIGNAL_RUN_ID_NUMBER,
    pr.PROPOSAL_RUN_ID,
    pr.SIGNAL_RUN_ID as PROPOSAL_SIGNAL_RUN_ID,
    pr.PIPELINE_RUN_ID as PROPOSAL_PIPELINE_RUN_ID,
    sr.SIGNAL_GENERATED_AT,
    pr.PIPELINE_START_TS
from proposal_runs pr
left join signal_runs sr
  on sr.SIGNAL_RUN_ID_NUMBER = pr.PROPOSAL_RUN_ID
  or sr.SIGNAL_RUN_ID_STRING = pr.SIGNAL_RUN_ID
left join pipeline_runs pr2
  on pr2.PIPELINE_RUN_ID = pr.PIPELINE_RUN_ID;
