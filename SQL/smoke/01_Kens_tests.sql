use role MIP_ADMIN_ROLE;
use database MIP;

select distinct symbol, market_type from mip.app.ingest_universe;

select * from agent_out.order_proposals;
desc table agent_out.order_proposals;

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
