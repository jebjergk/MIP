-- archive_and_remove_morning_brief_portfolio_0.sql
-- Purpose: Remove test portfolio_id=0 artifacts from MORNING_BRIEF. Archive first if archive exists.

use role MIP_ADMIN_ROLE;
use database MIP;

-- Ensure archive table exists (same structure as MORNING_BRIEF).
create table if not exists MIP.AGENT_OUT.MORNING_BRIEF_LEGACY_ARCHIVE as
select *
from MIP.AGENT_OUT.MORNING_BRIEF
where 1 = 0;

-- Archive portfolio_id=0 rows.
insert into MIP.AGENT_OUT.MORNING_BRIEF_LEGACY_ARCHIVE
select *
from MIP.AGENT_OUT.MORNING_BRIEF
where PORTFOLIO_ID = 0;

-- Remove portfolio_id=0 from MORNING_BRIEF.
delete from MIP.AGENT_OUT.MORNING_BRIEF
where PORTFOLIO_ID = 0;
