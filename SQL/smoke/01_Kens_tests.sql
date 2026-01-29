use role MIP_ADMIN_ROLE;
use database MIP;

ALTER GIT REPOSITORY MIP.APP.MIP FETCH;


select * from mip.app.mip_audit_log order by event_ts desc;
desc table MIP.MART.V_TRUSTED_SIGNALS_LATEST_TS;

--drop table mip.agent_out.agent_morning_brief;

select RUN_ID, count(*) from MIP.MART.V_TRUSTED_SIGNALS_LATEST_TS group by 1 order by 2 desc;

-- Run the pipeline once before executing this smoke SQL.
select distinct RUN_ID from MIP.MART.V_TRUSTED_SIGNALS_LATEST_TS;
select * from MIP.MART.V_TRUSTED_SIGNALS_LATEST_TS;
select * from MIP.MART.V_TRAINING_KPIS limit 20;
select * from MIP.MART.V_TRAINING_LEADERBOARD limit 50;
-- should be > 0 if outcomes exist
select count(*) from MIP.MART.V_SIGNAL_OUTCOMES_BASE;

select distinct symbol, market_type from mip.app.ingest_universe;

select * from agent_out.order_proposals;
desc table agent_out.order_proposals;

call MIP.APP.SP_RUN_DAILY_PIPELINE();

select * from mip.mart.v_morning_brief_json limit 1;
desc table mip.app.portfolio;

desc view MIP.MART.V_PORTFOLIO_OPEN_POSITIONS_CANONICAL;
desc view MIP.app.PORTFOLIO_POSITIONS;
desc view MIP.MART.V_PORTFOLIO_RISK_GATE;

desc view MIP.MART.V_TRAINING_LEADERBOARD; 
desc view MIP.MART.V_SIGNAL_OUTCOMES_BASE;

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

-- D) Smoke: MORNING_BRIEF (RUN_ID = pipeline UUID). Run pipeline once first.
select *
from MIP.AGENT_OUT.MORNING_BRIEF
order by created_at desc
limit 5;

-- Validate candidates exist when trusted signals exist for that run (use a run_id from above):
-- set run_id = '<paste RUN_ID from MORNING_BRIEF>';
-- select count(*) from MIP.MART.V_TRUSTED_SIGNALS_LATEST_TS where run_id = :run_id;
-- Then check BRIEF_JSON: candidate_summary.candidates is non-empty when count > 0.
