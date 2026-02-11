use role MIP_ADMIN_ROLE;
use database MIP;

-- =============================================================================
-- DATA CLEANUP: Remove duplicate PORTFOLIO_DAILY rows
-- =============================================================================
-- ROOT CAUSE: The simulation used INSERT (not MERGE) for PORTFOLIO_DAILY.
-- Every pipeline re-run added duplicate rows for the same (PORTFOLIO_ID, TS).
-- This inflated ALL cumulative metrics: P&L, equity, paid out, drawdown, etc.
--
-- This has been fixed in 180_sp_run_portfolio_simulation.sql (DELETE before INSERT).
-- Deploy that fix FIRST, then run this cleanup.
-- =============================================================================

-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 1: Diagnosis — see how many duplicates exist per portfolio/episode
-- ─────────────────────────────────────────────────────────────────────────────
select
    PORTFOLIO_ID,
    EPISODE_ID,
    count(*) as TOTAL_ROWS,
    count(distinct TS) as UNIQUE_DAYS,
    count(*) - count(distinct TS) as DUPLICATE_ROWS,
    round(count(*) / nullif(count(distinct TS), 0), 1) as AVG_COPIES_PER_DAY
from MIP.APP.PORTFOLIO_DAILY
group by PORTFOLIO_ID, EPISODE_ID
order by PORTFOLIO_ID, EPISODE_ID;


-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 2: Preview — count rows that will be deleted
-- ─────────────────────────────────────────────────────────────────────────────
-- Strategy: for each (PORTFOLIO_ID, TS, EPISODE_ID) keep the row with the
-- HIGHEST RUN_ID (most recent run). Delete all earlier copies.
select count(*) as ROWS_TO_DELETE
from (
    select
        PORTFOLIO_ID, TS, EPISODE_ID, RUN_ID,
        row_number() over (
            partition by PORTFOLIO_ID, TS, coalesce(EPISODE_ID, -1)
            order by RUN_ID desc
        ) as RN
    from MIP.APP.PORTFOLIO_DAILY
)
where RN > 1;


-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 3: DELETE duplicates — keeping only the latest RUN_ID per day
-- ─────────────────────────────────────────────────────────────────────────────
-- >>> UNCOMMENT AND RUN WHEN READY <<<
/*
delete from MIP.APP.PORTFOLIO_DAILY
where (PORTFOLIO_ID, TS, coalesce(EPISODE_ID, -1), RUN_ID) in (
    select PORTFOLIO_ID, TS, coalesce(EPISODE_ID, -1), RUN_ID
    from (
        select
            PORTFOLIO_ID, TS, EPISODE_ID, RUN_ID,
            row_number() over (
                partition by PORTFOLIO_ID, TS, coalesce(EPISODE_ID, -1)
                order by RUN_ID desc
            ) as RN
        from MIP.APP.PORTFOLIO_DAILY
    )
    where RN > 1
);
*/


-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 4: Verify — confirm clean state
-- ─────────────────────────────────────────────────────────────────────────────
select
    PORTFOLIO_ID,
    EPISODE_ID,
    count(*) as TOTAL_ROWS,
    count(distinct TS) as UNIQUE_DAYS,
    iff(count(*) = count(distinct TS), 'CLEAN', 'STILL_DIRTY') as STATUS
from MIP.APP.PORTFOLIO_DAILY
group by PORTFOLIO_ID, EPISODE_ID
order by PORTFOLIO_ID, EPISODE_ID;


-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 5: Quick health check — what the portfolios look like after cleanup
-- ─────────────────────────────────────────────────────────────────────────────
select
    p.PORTFOLIO_ID,
    p.NAME,
    p.STATUS,
    p.STARTING_CASH,
    p.FINAL_EQUITY,
    e.EPISODE_ID,
    e.STATUS as EP_STATUS,
    e.START_TS as EP_START,
    (select max(TS) from MIP.APP.PORTFOLIO_DAILY d
     where d.PORTFOLIO_ID = p.PORTFOLIO_ID
       and d.EPISODE_ID = e.EPISODE_ID) as LATEST_DAILY_TS,
    (select count(*) from MIP.APP.PORTFOLIO_DAILY d
     where d.PORTFOLIO_ID = p.PORTFOLIO_ID
       and d.EPISODE_ID = e.EPISODE_ID) as DAILY_ROW_COUNT,
    (select count(*) from MIP.APP.PORTFOLIO_POSITIONS pos
     where pos.PORTFOLIO_ID = p.PORTFOLIO_ID
       and pos.EPISODE_ID = e.EPISODE_ID) as POSITION_COUNT,
    (select count(*) from MIP.APP.PORTFOLIO_TRADES t
     where t.PORTFOLIO_ID = p.PORTFOLIO_ID
       and t.EPISODE_ID = e.EPISODE_ID) as TRADE_COUNT,
    (select max(TS) from MIP.MART.V_BAR_INDEX
     where INTERVAL_MINUTES = 1440) as LATEST_MARKET_BAR
from MIP.APP.PORTFOLIO p
left join MIP.APP.PORTFOLIO_EPISODE e
  on e.PORTFOLIO_ID = p.PORTFOLIO_ID
 and e.STATUS = 'ACTIVE'
where p.STATUS = 'ACTIVE'
order by p.PORTFOLIO_ID;
