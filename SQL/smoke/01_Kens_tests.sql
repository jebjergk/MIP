use role MIP_ADMIN_ROLE;
use database MIP;

-- =============================================================================
-- DIAGNOSTIC: Where did the $44 go? (Portfolio 1 lost money with no trades)
-- =============================================================================


ALTER GIT REPOSITORY MIP.APP.MIP FETCH;
select * from mip.app.mip_audit_log order by event_ts desc;

call MIP.APP.SP_RUN_DAILY_PIPELINE();


-- Check if any per-symbol training snapshots exist
select SCOPE, count(*) as cnt
from MIP.AGENT_OUT.TRAINING_DIGEST_SNAPSHOT
group by SCOPE;

-- Check the audit log for errors
select EVENT_STATUS, EVENT_DETAIL
from MIP.APP.MIP_AUDIT_LOG
where EVENT_NAME = 'SP_AGENT_GENERATE_TRAINING_DIGEST'
order by CREATED_AT desc
limit 1;