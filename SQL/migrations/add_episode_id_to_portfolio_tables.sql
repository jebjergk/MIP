-- add_episode_id_to_portfolio_tables.sql
-- Purpose: Add EPISODE_ID column to portfolio tables and clean up pre-episode data.
-- Run AFTER deploying updated DDL (160_app_portfolio_tables.sql) and BEFORE next pipeline run.

use role MIP_ADMIN_ROLE;
use database MIP;

-- =============================================================================
-- STEP 1: Verify columns were added by DDL deployment
-- =============================================================================
select 'PORTFOLIO_POSITIONS' as TABLE_NAME, count(*) as HAS_EPISODE_ID
from INFORMATION_SCHEMA.COLUMNS
where TABLE_SCHEMA = 'APP' and TABLE_NAME = 'PORTFOLIO_POSITIONS' and COLUMN_NAME = 'EPISODE_ID'
union all
select 'PORTFOLIO_TRADES', count(*)
from INFORMATION_SCHEMA.COLUMNS
where TABLE_SCHEMA = 'APP' and TABLE_NAME = 'PORTFOLIO_TRADES' and COLUMN_NAME = 'EPISODE_ID'
union all
select 'PORTFOLIO_DAILY', count(*)
from INFORMATION_SCHEMA.COLUMNS
where TABLE_SCHEMA = 'APP' and TABLE_NAME = 'PORTFOLIO_DAILY' and COLUMN_NAME = 'EPISODE_ID';

-- =============================================================================
-- STEP 2: Preview data to be cleaned (rows from BEFORE active episode)
-- =============================================================================
select 'PORTFOLIO_DAILY' as TABLE_NAME, d.PORTFOLIO_ID, count(*) as ROWS_TO_DELETE
from MIP.APP.PORTFOLIO_DAILY d
join MIP.APP.PORTFOLIO_EPISODE e 
  on e.PORTFOLIO_ID = d.PORTFOLIO_ID and e.STATUS = 'ACTIVE'
where d.TS < e.START_TS
group by d.PORTFOLIO_ID
union all
select 'PORTFOLIO_TRADES', t.PORTFOLIO_ID, count(*)
from MIP.APP.PORTFOLIO_TRADES t
join MIP.APP.PORTFOLIO_EPISODE e 
  on e.PORTFOLIO_ID = t.PORTFOLIO_ID and e.STATUS = 'ACTIVE'
where t.TRADE_TS < e.START_TS
group by t.PORTFOLIO_ID
union all
select 'PORTFOLIO_POSITIONS', p.PORTFOLIO_ID, count(*)
from MIP.APP.PORTFOLIO_POSITIONS p
join MIP.APP.PORTFOLIO_EPISODE e 
  on e.PORTFOLIO_ID = p.PORTFOLIO_ID and e.STATUS = 'ACTIVE'
where p.ENTRY_TS < e.START_TS
group by p.PORTFOLIO_ID;

-- =============================================================================
-- STEP 3: Delete rows from BEFORE the active episode (for all portfolios)
-- These are historical artifacts that should not affect current episode metrics.
-- =============================================================================

-- Delete PORTFOLIO_DAILY rows before episode start
delete from MIP.APP.PORTFOLIO_DAILY d
using MIP.APP.PORTFOLIO_EPISODE e
where d.PORTFOLIO_ID = e.PORTFOLIO_ID
  and e.STATUS = 'ACTIVE'
  and d.TS < e.START_TS;

-- Delete PORTFOLIO_TRADES rows before episode start
delete from MIP.APP.PORTFOLIO_TRADES t
using MIP.APP.PORTFOLIO_EPISODE e
where t.PORTFOLIO_ID = e.PORTFOLIO_ID
  and e.STATUS = 'ACTIVE'
  and t.TRADE_TS < e.START_TS;

-- Delete PORTFOLIO_POSITIONS rows before episode start
delete from MIP.APP.PORTFOLIO_POSITIONS p
using MIP.APP.PORTFOLIO_EPISODE e
where p.PORTFOLIO_ID = e.PORTFOLIO_ID
  and e.STATUS = 'ACTIVE'
  and p.ENTRY_TS < e.START_TS;

-- =============================================================================
-- STEP 4: Backfill EPISODE_ID for remaining rows (current episode data)
-- =============================================================================

-- Backfill PORTFOLIO_DAILY
update MIP.APP.PORTFOLIO_DAILY d
   set EPISODE_ID = e.EPISODE_ID
  from MIP.APP.PORTFOLIO_EPISODE e
 where d.PORTFOLIO_ID = e.PORTFOLIO_ID
   and e.STATUS = 'ACTIVE'
   and d.TS >= e.START_TS
   and d.EPISODE_ID is null;

-- Backfill PORTFOLIO_TRADES
update MIP.APP.PORTFOLIO_TRADES t
   set EPISODE_ID = e.EPISODE_ID
  from MIP.APP.PORTFOLIO_EPISODE e
 where t.PORTFOLIO_ID = e.PORTFOLIO_ID
   and e.STATUS = 'ACTIVE'
   and t.TRADE_TS >= e.START_TS
   and t.EPISODE_ID is null;

-- Backfill PORTFOLIO_POSITIONS
update MIP.APP.PORTFOLIO_POSITIONS p
   set EPISODE_ID = e.EPISODE_ID
  from MIP.APP.PORTFOLIO_EPISODE e
 where p.PORTFOLIO_ID = e.PORTFOLIO_ID
   and e.STATUS = 'ACTIVE'
   and p.ENTRY_TS >= e.START_TS
   and p.EPISODE_ID is null;

-- =============================================================================
-- STEP 5: Verify cleanup and backfill
-- =============================================================================
select 'PORTFOLIO_DAILY' as TABLE_NAME, 
       count(*) as TOTAL_ROWS,
       count_if(EPISODE_ID is not null) as WITH_EPISODE_ID,
       count_if(EPISODE_ID is null) as WITHOUT_EPISODE_ID
from MIP.APP.PORTFOLIO_DAILY
where PORTFOLIO_ID in (select PORTFOLIO_ID from MIP.APP.PORTFOLIO_EPISODE where STATUS = 'ACTIVE')
union all
select 'PORTFOLIO_TRADES',
       count(*),
       count_if(EPISODE_ID is not null),
       count_if(EPISODE_ID is null)
from MIP.APP.PORTFOLIO_TRADES
where PORTFOLIO_ID in (select PORTFOLIO_ID from MIP.APP.PORTFOLIO_EPISODE where STATUS = 'ACTIVE')
union all
select 'PORTFOLIO_POSITIONS',
       count(*),
       count_if(EPISODE_ID is not null),
       count_if(EPISODE_ID is null)
from MIP.APP.PORTFOLIO_POSITIONS
where PORTFOLIO_ID in (select PORTFOLIO_ID from MIP.APP.PORTFOLIO_EPISODE where STATUS = 'ACTIVE');

-- =============================================================================
-- STEP 6: After cleanup, run the pipeline to get fresh metrics
-- The simulation will now populate EPISODE_ID automatically.
-- =============================================================================
-- call MIP.APP.SP_RUN_DAILY_PIPELINE();
