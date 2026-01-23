-- order_proposals_signal_linkage_check.sql
-- Purpose: Validate signal linkage on order proposals

use role MIP_ADMIN_ROLE;
use database MIP;

set v_run_id = null;
set v_portfolio_id = null;

-- Expect 0: approved/executed proposals tied to ineligible signals
select
    count(*) as invalid_approved_or_executed
from MIP.AGENT_OUT.ORDER_PROPOSALS p
left join MIP.APP.V_SIGNALS_ELIGIBLE_TODAY s
  on s.RECOMMENDATION_ID = p.RECOMMENDATION_ID
where p.STATUS in ('APPROVED', 'EXECUTED')
  and (:v_run_id is null or p.RUN_ID = :v_run_id)
  and (:v_portfolio_id is null or p.PORTFOLIO_ID = :v_portfolio_id)
  and (s.RECOMMENDATION_ID is null or s.IS_ELIGIBLE = false);

-- Expect 0: proposed proposals missing recommendation linkage
select
    count(*) as proposed_missing_recommendation_id
from MIP.AGENT_OUT.ORDER_PROPOSALS p
where p.STATUS = 'PROPOSED'
  and p.RECOMMENDATION_ID is null
  and (:v_run_id is null or p.RUN_ID = :v_run_id)
  and (:v_portfolio_id is null or p.PORTFOLIO_ID = :v_portfolio_id);
