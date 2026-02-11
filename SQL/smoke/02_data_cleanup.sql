use role MIP_ADMIN_ROLE;
use database MIP;

-- =============================================================================
-- DATA CLEANUP: Remove duplicate PORTFOLIO_DAILY rows
-- =============================================================================
-- The simulation previously used INSERT (not MERGE) for PORTFOLIO_DAILY.
-- Every pipeline re-run added duplicate rows, causing cumulative P&L to inflate.
-- This script deduplicates by keeping only the LATEST row per (PORTFOLIO_ID, TS, EPISODE_ID).
-- =============================================================================

-- Step 1: Check how many duplicates exist
select
    PORTFOLIO_ID,
    EPISODE_ID,
    TS,
    count(*) as ROW_COUNT
from MIP.APP.PORTFOLIO_DAILY
group by PORTFOLIO_ID, EPISODE_ID, TS
having count(*) > 1
order by PORTFOLIO_ID, EPISODE_ID, TS;

-- Step 2: Preview what will be deleted (run this first to verify)
-- Keeps only the row with the latest RUN_ID per (PORTFOLIO_ID, TS, EPISODE_ID)
select count(*) as ROWS_TO_DELETE
from MIP.APP.PORTFOLIO_DAILY d
where exists (
    select 1
    from MIP.APP.PORTFOLIO_DAILY d2
    where d2.PORTFOLIO_ID = d.PORTFOLIO_ID
      and d2.TS = d.TS
      and coalesce(d2.EPISODE_ID, -1) = coalesce(d.EPISODE_ID, -1)
      and d2.RUN_ID > d.RUN_ID
);

-- Step 3: Delete duplicates (keeping the latest RUN_ID per day)
-- UNCOMMENT THE LINES BELOW WHEN READY TO EXECUTE

delete from MIP.APP.PORTFOLIO_DAILY d
where exists (
    select 1
    from MIP.APP.PORTFOLIO_DAILY d2
    where d2.PORTFOLIO_ID = d.PORTFOLIO_ID
      and d2.TS = d.TS
      and coalesce(d2.EPISODE_ID, -1) = coalesce(d.EPISODE_ID, -1)
      and d2.RUN_ID > d.RUN_ID
);


-- Step 4: Verify no more duplicates
select
    PORTFOLIO_ID,
    EPISODE_ID,
    count(*) as TOTAL_ROWS,
    count(distinct TS) as DISTINCT_DAYS,
    iff(count(*) = count(distinct TS), 'CLEAN', 'STILL HAS DUPLICATES') as STATUS
from MIP.APP.PORTFOLIO_DAILY
group by PORTFOLIO_ID, EPISODE_ID
order by PORTFOLIO_ID, EPISODE_ID;
