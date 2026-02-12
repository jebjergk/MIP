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
-- STEP 3b: Fallback — if trades still have NULL EPISODE_ID (e.g. TRADE_TS is
-- before the earliest episode START_TS), assign them to the EARLIEST episode
-- for that portfolio. These are legacy trades from before episodes existed.
-- =============================================================================
update MIP.APP.PORTFOLIO_TRADES t
   set EPISODE_ID = e.EPISODE_ID
  from (
      select
          t2.TRADE_ID,
          first_ep.EPISODE_ID
      from MIP.APP.PORTFOLIO_TRADES t2
      join (
          select PORTFOLIO_ID, EPISODE_ID
          from MIP.APP.PORTFOLIO_EPISODE
          qualify row_number() over (partition by PORTFOLIO_ID order by START_TS asc) = 1
      ) first_ep
        on first_ep.PORTFOLIO_ID = t2.PORTFOLIO_ID
      where t2.EPISODE_ID is null
  ) e
where t.TRADE_ID = e.TRADE_ID
  and t.EPISODE_ID is null;

select 'Remaining orphan trades assigned to earliest episode (fallback)' as action, $1 as rows_affected;

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
