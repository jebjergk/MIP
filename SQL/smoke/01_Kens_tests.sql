use role MIP_ADMIN_ROLE;
use database MIP;

-- =============================================================================
-- DIAGNOSTIC: Where did the $44 go? (Portfolio 1 lost money with no trades)
-- =============================================================================


ALTER GIT REPOSITORY MIP.APP.MIP FETCH;
select * from mip.app.mip_audit_log order by event_ts desc;

select * from mip.app.mip_audit_log where event_ts::date = '2026-02-01' order by event_ts desc;

call MIP.APP.SP_RUN_DAILY_PIPELINE();



-- Check for duplicate proposals today
select SYMBOL, PORTFOLIO_ID, RECOMMENDATION_ID, count(*) as cnt
from MIP.AGENT_OUT.ORDER_PROPOSALS
where PROPOSED_AT::date = current_date()
group by SYMBOL, PORTFOLIO_ID, RECOMMENDATION_ID
having count(*) > 1;

-- Delete the older duplicates (keep only latest per portfolio+recommendation)
delete from MIP.AGENT_OUT.ORDER_PROPOSALS
where PROPOSAL_ID in (
    select PROPOSAL_ID from (
        select PROPOSAL_ID,
               row_number() over (
                   partition by PORTFOLIO_ID, RECOMMENDATION_ID
                   order by PROPOSED_AT desc
               ) as rn
        from MIP.AGENT_OUT.ORDER_PROPOSALS
        where PROPOSED_AT::date = current_date()
    )
    where rn > 1
);