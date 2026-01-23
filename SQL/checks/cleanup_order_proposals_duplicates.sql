-- cleanup_order_proposals_duplicates.sql
-- Purpose: Remove duplicate proposals by (portfolio_id, run_id, recommendation_id)

use role MIP_ADMIN_ROLE;
use database MIP;

delete from MIP.AGENT_OUT.ORDER_PROPOSALS
where PROPOSAL_ID in (
    select PROPOSAL_ID
    from (
        select
            PROPOSAL_ID,
            row_number() over (
                partition by PORTFOLIO_ID, RUN_ID, RECOMMENDATION_ID
                order by PROPOSED_AT desc, PROPOSAL_ID desc
            ) as proposal_rank
        from MIP.AGENT_OUT.ORDER_PROPOSALS
        where RECOMMENDATION_ID is not null
    )
    where proposal_rank > 1
);
