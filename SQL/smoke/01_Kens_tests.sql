use role MIP_ADMIN_ROLE;
use database MIP;

select * from mip.app.mip_audit_log order by event_ts desc;
desc table mip.app.mip_audit_log;

-- Run the pipeline once before executing this smoke SQL.

with last_pipeline as (
  select run_id
  from MIP.APP.MIP_AUDIT_LOG
  where event_type = 'PIPELINE' and event_name = 'SP_RUN_DAILY_PIPELINE' and status = 'SUCCESS'
  qualify row_number() over (order by event_ts desc) = 1
)
select
  sum(case when a.event_type <> 'PIPELINE' and a.parent_run_id = p.run_id then 1 else 0 end) as child_rows_linked,
  sum(case when a.event_type <> 'PIPELINE' and a.parent_run_id is null then 1 else 0 end) as child_rows_missing_parent
from MIP.APP.MIP_AUDIT_LOG a
cross join last_pipeline p
where a.event_ts >= dateadd('hour', -2, current_timestamp());

-- Expect: child_rows_missing_parent = 0

select distinct symbol, market_type from mip.app.ingest_universe;

select * from agent_out.order_proposals;
desc table agent_out.order_proposals;

call MIP.APP.SP_RUN_DAILY_PIPELINE();

select * from mip.mart.v_morning_brief_json limit 1;
desc table mip.app.portfolio;

desc view MIP.MART.V_PORTFOLIO_OPEN_POSITIONS_CANONICAL;
desc view MIP.app.PORTFOLIO_POSITIONS;
desc view MIP.MART.V_PORTFOLIO_RISK_GATE;

desc table mip.agent_out.morning_brief;
select
    PORTFOLIO_ID,
    SYMBOL,
    MARKET_TYPE,
    INTERVAL_MINUTES,
    SIDE,
    count(*) as proposal_count
from MIP.AGENT_OUT.ORDER_PROPOSALS
group by PORTFOLIO_ID, SYMBOL, MARKET_TYPE, INTERVAL_MINUTES, SIDE
having count(*) > 1
order by proposal_count desc, PORTFOLIO_ID, SYMBOL;

--delete from MIP.AGENT_OUT.ORDER_PROPOSALS
where PROPOSAL_ID in (
    select PROPOSAL_ID
    from (
        select
            PROPOSAL_ID,
            row_number() over (
                partition by PORTFOLIO_ID, SYMBOL, MARKET_TYPE, INTERVAL_MINUTES, SIDE
                order by PROPOSAL_ID desc
            ) as proposal_rank
        from MIP.AGENT_OUT.ORDER_PROPOSALS
    )
    where proposal_rank > 1
);

call MIP.APP.SP_RUN_DAILY_PIPELINE();
