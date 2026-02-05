use role MIP_ADMIN_ROLE;
use database MIP;

-- =============================================================================
-- DIAGNOSTIC: Where did the $44 go? (Portfolio 1 lost money with no trades)
-- =============================================================================


ALTER GIT REPOSITORY MIP.APP.MIP FETCH;
select * from mip.app.mip_audit_log order by event_ts desc;

select * from mip.app.mip_audit_log where event_ts::date = '2026-02-01' order by event_ts desc;

call MIP.APP.SP_RUN_DAILY_PIPELINE();


-- 1. Check if there's anything in REPLAY_CONTEXT
select * from MIP.APP.REPLAY_CONTEXT;

-- 2. Look at the latest audit events to see what's happening
select RUN_ID, EVENT_TYPE, EVENT_NAME, STATUS, EVENT_TS, 
       DETAILS:mode::string as MODE,
       DETAILS:replay_batch_id::string as REPLAY_BATCH_ID
from MIP.APP.MIP_AUDIT_LOG
order by EVENT_TS desc
limit 10;

-- 3. Check the most recent pipeline run specifically
select RUN_ID, EVENT_TYPE, EVENT_NAME, STATUS, EVENT_TS
from MIP.APP.MIP_AUDIT_LOG
where EVENT_NAME = 'SP_RUN_DAILY_PIPELINE'
order by EVENT_TS desc
limit 5;