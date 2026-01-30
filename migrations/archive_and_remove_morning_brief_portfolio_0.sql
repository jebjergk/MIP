-- archive_and_remove_morning_brief_portfolio_0.sql
-- Purpose: Remove test portfolio_id=0 and AGENT_V0_MORNING_BRIEF artifacts from MORNING_BRIEF. Archive first.

use role MIP_ADMIN_ROLE;
use database MIP;

-- Ensure archive table exists (same structure as MORNING_BRIEF).
create table if not exists MIP.AGENT_OUT.MORNING_BRIEF_LEGACY_ARCHIVE as
select *
from MIP.AGENT_OUT.MORNING_BRIEF
where 1 = 0;

-- Archive rows to remove: portfolio_id <= 0 or agent_name = 'AGENT_V0_MORNING_BRIEF'.
insert into MIP.AGENT_OUT.MORNING_BRIEF_LEGACY_ARCHIVE
select *
from MIP.AGENT_OUT.MORNING_BRIEF
where PORTFOLIO_ID <= 0
   or AGENT_NAME = 'AGENT_V0_MORNING_BRIEF';

-- Remove from MORNING_BRIEF: portfolio_id <= 0 or AGENT_V0_MORNING_BRIEF.
delete from MIP.AGENT_OUT.MORNING_BRIEF
where PORTFOLIO_ID <= 0
   or AGENT_NAME = 'AGENT_V0_MORNING_BRIEF';
