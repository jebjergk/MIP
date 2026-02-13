-- cleanup_duplicate_trades_and_backfill_episode.sql
-- Purpose: One-time cleanup after deploying the MERGE dedup + EPISODE_ID fixes.
--
-- TWO issues to fix:
--   1. Portfolio 1 (and any others): duplicate simulation trades accumulated because
--      the MERGE dedup key used PRICE+QUANTITY which drift between re-runs.
--   2. Portfolio 2 (and any others): agent-created trades missing EPISODE_ID,
--      making them invisible to episode-scoped queries.
--
-- RUN THIS AFTER deploying 180_sp_run_portfolio_simulation.sql and
-- 189_sp_validate_and_execute_proposals.sql.
--
-- SAFE TO RUN MULTIPLE TIMES (idempotent).

use role MIP_ADMIN_ROLE;
use database MIP;

-- =============================================================================
-- STEP 1: Preview duplicate simulation trades
-- Duplicates = same (PORTFOLIO_ID, EPISODE_ID, SYMBOL, SIDE, TRADE_DAY)
-- but with PROPOSAL_ID IS NULL (simulation-generated, not agent-generated).
-- =============================================================================
select
    PORTFOLIO_ID,
    EPISODE_ID,
    SYMBOL,
    SIDE,
    date_trunc('day', TRADE_TS) as TRADE_DAY,
    count(*) as dupe_count,
    min(TRADE_ID) as keep_trade_id,
    array_agg(TRADE_ID) as all_trade_ids
from MIP.APP.PORTFOLIO_TRADES
where PROPOSAL_ID is null
group by PORTFOLIO_ID, EPISODE_ID, SYMBOL, SIDE, date_trunc('day', TRADE_TS)
having count(*) > 1
order by PORTFOLIO_ID, TRADE_DAY, SYMBOL, SIDE;

-- =============================================================================
-- STEP 2: Delete duplicate simulation trades (keep the EARLIEST TRADE_ID per group)
-- This preserves the original trade from the first pipeline run and removes
-- all subsequent duplicates created by re-runs.
-- =============================================================================
delete from MIP.APP.PORTFOLIO_TRADES
where PROPOSAL_ID is null
  and TRADE_ID not in (
      select min(TRADE_ID)
      from MIP.APP.PORTFOLIO_TRADES
      where PROPOSAL_ID is null
      group by PORTFOLIO_ID, EPISODE_ID, SYMBOL, SIDE, date_trunc('day', TRADE_TS)
  );

-- Show how many were deleted
select 'Duplicate trades deleted' as action, $1 as rows_affected;

-- =============================================================================
-- STEP 3a: Backfill EPISODE_ID on ALL trades (agent AND simulation) missing it.
-- Match trades to the episode whose window (START_TS..END_TS) contains the
-- trade's TRADE_TS. For ACTIVE episodes END_TS is null, so any trade after
-- START_TS matches.
-- =============================================================================
update MIP.APP.PORTFOLIO_TRADES t
   set EPISODE_ID = e.EPISODE_ID
  from (
      select
          t2.TRADE_ID,
          ep.EPISODE_ID
      from MIP.APP.PORTFOLIO_TRADES t2
      join MIP.APP.PORTFOLIO_EPISODE ep
        on ep.PORTFOLIO_ID = t2.PORTFOLIO_ID
       and t2.TRADE_TS >= ep.START_TS
       and (ep.END_TS is null or t2.TRADE_TS <= ep.END_TS)
      where t2.EPISODE_ID is null
  ) e
where t.TRADE_ID = e.TRADE_ID
  and t.EPISODE_ID is null;

select 'Trades backfilled with EPISODE_ID (by timestamp window)' as action, $1 as rows_affected;

-- =============================================================================
-- STEP 3b: REMOVED — Previously assigned all remaining orphan trades to the
-- earliest episode per portfolio. This is dangerous: trades outside any episode
-- window should remain orphaned (EPISODE_ID = NULL) rather than be forced into
-- a wrong episode, which corrupts cash recovery and equity calculations.
-- Any remaining orphans after Step 3a need manual inspection.
-- =============================================================================
-- (no-op)

-- =============================================================================
-- STEP 4: Verify — no more orphaned trades without EPISODE_ID
-- (Should return 0 rows after backfill, unless there are trades with no matching episode)
-- =============================================================================
select
    PORTFOLIO_ID,
    count(*) as orphan_count
from MIP.APP.PORTFOLIO_TRADES
where EPISODE_ID is null
group by PORTFOLIO_ID;

-- =============================================================================
-- STEP 5: Verify — no more duplicate simulation trades
-- (Should return 0 rows)
-- =============================================================================
select
    PORTFOLIO_ID,
    EPISODE_ID,
    SYMBOL,
    SIDE,
    date_trunc('day', TRADE_TS) as TRADE_DAY,
    count(*) as dupe_count
from MIP.APP.PORTFOLIO_TRADES
where PROPOSAL_ID is null
group by PORTFOLIO_ID, EPISODE_ID, SYMBOL, SIDE, date_trunc('day', TRADE_TS)
having count(*) > 1;

-- =============================================================================
-- STEP 6: Diagnose bogus crystallization episodes
-- Episodes that ran for < 1 day with 0 trades and high returns are phantom
-- crystallizations caused by the simulation re-trading historical signals.
-- Preview them before deleting.
-- =============================================================================
select
    e.PORTFOLIO_ID,
    e.EPISODE_ID,
    e.START_TS,
    e.END_TS,
    e.STATUS,
    e.END_REASON,
    r.RETURN_PCT,
    r.TRADES_COUNT,
    r.DISTRIBUTION_AMOUNT,
    timestampdiff(hour, e.START_TS, e.END_TS) as duration_hours
from MIP.APP.PORTFOLIO_EPISODE e
left join MIP.APP.PORTFOLIO_EPISODE_RESULTS r
  on r.PORTFOLIO_ID = e.PORTFOLIO_ID and r.EPISODE_ID = e.EPISODE_ID
where e.STATUS = 'ENDED'
  and e.END_REASON = 'PROFIT_TARGET_HIT'
  and timestampdiff(hour, e.START_TS, coalesce(e.END_TS, current_timestamp())) < 24
  and coalesce(r.TRADES_COUNT, 0) = 0
order by e.PORTFOLIO_ID, e.START_TS;

-- =============================================================================
-- STEP 7: Delete bogus crystallization episodes and their results
-- These are phantom episodes: ran < 1 day, 0 trades, triggered by re-trading
-- historical signals. Safe to delete because they have no real data.
--
-- NOTE: This also deletes PORTFOLIO_LIFECYCLE_EVENT rows for these episodes
-- and resets COOLDOWN_UNTIL_TS on affected portfolios.
-- =============================================================================

-- 7a: Delete results for phantom episodes
delete from MIP.APP.PORTFOLIO_EPISODE_RESULTS
where (PORTFOLIO_ID, EPISODE_ID) in (
    select e.PORTFOLIO_ID, e.EPISODE_ID
    from MIP.APP.PORTFOLIO_EPISODE e
    left join MIP.APP.PORTFOLIO_EPISODE_RESULTS r
      on r.PORTFOLIO_ID = e.PORTFOLIO_ID and r.EPISODE_ID = e.EPISODE_ID
    where e.STATUS = 'ENDED'
      and e.END_REASON = 'PROFIT_TARGET_HIT'
      and timestampdiff(hour, e.START_TS, coalesce(e.END_TS, current_timestamp())) < 24
      and coalesce(r.TRADES_COUNT, 0) = 0
);
select 'Phantom episode results deleted' as action, $1 as rows_affected;

-- 7b: Delete PORTFOLIO_DAILY rows for phantom episodes
delete from MIP.APP.PORTFOLIO_DAILY
where (PORTFOLIO_ID, EPISODE_ID) in (
    select e.PORTFOLIO_ID, e.EPISODE_ID
    from MIP.APP.PORTFOLIO_EPISODE e
    left join MIP.APP.PORTFOLIO_EPISODE_RESULTS r
      on r.PORTFOLIO_ID = e.PORTFOLIO_ID and r.EPISODE_ID = e.EPISODE_ID
    where e.STATUS = 'ENDED'
      and e.END_REASON = 'PROFIT_TARGET_HIT'
      and timestampdiff(hour, e.START_TS, coalesce(e.END_TS, current_timestamp())) < 24
      and coalesce(r.TRADES_COUNT, 0) = 0
);
select 'Phantom episode daily rows deleted' as action, $1 as rows_affected;

-- 7c: Delete lifecycle events for phantom episodes
delete from MIP.APP.PORTFOLIO_LIFECYCLE_EVENT
where EVENT_TYPE = 'CRYSTALLIZE'
  and (PORTFOLIO_ID, EPISODE_ID) in (
    select e.PORTFOLIO_ID, e.EPISODE_ID
    from MIP.APP.PORTFOLIO_EPISODE e
    left join MIP.APP.PORTFOLIO_EPISODE_RESULTS r
      on r.PORTFOLIO_ID = e.PORTFOLIO_ID and r.EPISODE_ID = e.EPISODE_ID
    where e.STATUS = 'ENDED'
      and e.END_REASON = 'PROFIT_TARGET_HIT'
      and timestampdiff(hour, e.START_TS, coalesce(e.END_TS, current_timestamp())) < 24
      and coalesce(r.TRADES_COUNT, 0) = 0
);
select 'Phantom crystallize lifecycle events deleted' as action, $1 as rows_affected;

-- 7d: Reset LAST_SIMULATION_RUN_ID on portfolios that had phantom episodes.
-- MUST run BEFORE 7e (episode deletion) because the subquery needs the
-- phantom episodes to still exist in the table.
-- DO NOT use COOLDOWN_UNTIL_TS IS NULL — that matches nearly ALL portfolios
-- and would clobber legitimate run IDs, causing simulations to miss bars.
update MIP.APP.PORTFOLIO
   set LAST_SIMULATION_RUN_ID = null,
       UPDATED_AT = current_timestamp()
 where PORTFOLIO_ID in (
     select distinct e.PORTFOLIO_ID
     from MIP.APP.PORTFOLIO_EPISODE e
     left join MIP.APP.PORTFOLIO_EPISODE_RESULTS r
       on r.PORTFOLIO_ID = e.PORTFOLIO_ID and r.EPISODE_ID = e.EPISODE_ID
     where e.STATUS = 'ENDED'
       and e.END_REASON = 'PROFIT_TARGET_HIT'
       and timestampdiff(hour, e.START_TS, coalesce(e.END_TS, current_timestamp())) < 24
       and coalesce(r.TRADES_COUNT, 0) = 0
 );
select 'LAST_SIMULATION_RUN_ID reset on phantom-affected portfolios' as action, $1 as rows_affected;

-- 7e: Clear stale COOLDOWN_UNTIL_TS on affected portfolios
-- (may have been set by a phantom crystallization)
update MIP.APP.PORTFOLIO
   set COOLDOWN_UNTIL_TS = null,
       UPDATED_AT = current_timestamp()
 where COOLDOWN_UNTIL_TS is not null
   and COOLDOWN_UNTIL_TS > current_timestamp();
select 'Stale cooldowns cleared' as action, $1 as rows_affected;

-- 7f: Delete the phantom episodes themselves
-- MUST run AFTER 7d (run-ID reset) since 7d needs the episodes to exist.
delete from MIP.APP.PORTFOLIO_EPISODE
where STATUS = 'ENDED'
  and END_REASON = 'PROFIT_TARGET_HIT'
  and EPISODE_ID in (
    select e.EPISODE_ID
    from MIP.APP.PORTFOLIO_EPISODE e
    left join MIP.APP.PORTFOLIO_EPISODE_RESULTS r
      on r.PORTFOLIO_ID = e.PORTFOLIO_ID and r.EPISODE_ID = e.EPISODE_ID
    where e.STATUS = 'ENDED'
      and e.END_REASON = 'PROFIT_TARGET_HIT'
      and timestampdiff(hour, e.START_TS, coalesce(e.END_TS, current_timestamp())) < 24
      and coalesce(r.TRADES_COUNT, 0) = 0
  );
select 'Phantom episodes deleted' as action, $1 as rows_affected;

-- =============================================================================
-- STEP 8: Verify — remaining episodes should look legitimate
-- =============================================================================
select
    e.PORTFOLIO_ID,
    e.EPISODE_ID,
    e.START_TS,
    e.END_TS,
    e.STATUS,
    e.END_REASON,
    r.RETURN_PCT,
    r.TRADES_COUNT,
    r.DISTRIBUTION_AMOUNT
from MIP.APP.PORTFOLIO_EPISODE e
left join MIP.APP.PORTFOLIO_EPISODE_RESULTS r
  on r.PORTFOLIO_ID = e.PORTFOLIO_ID and r.EPISODE_ID = e.EPISODE_ID
order by e.PORTFOLIO_ID, e.START_TS;
