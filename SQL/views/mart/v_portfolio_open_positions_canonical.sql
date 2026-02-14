-- v_portfolio_open_positions_canonical.sql
-- Purpose: Canonical single source of truth for open positions used by all procedures.
-- Only positions belonging to the active episode are considered "open".
-- Uses EPISODE_ID column if populated, otherwise falls back to timestamp comparison.
--
-- CRITICAL: IS_OPEN is determined by FIFO sell matching, NOT by
-- HOLD_UNTIL_INDEX >= CURRENT_BAR_INDEX. The old BAR_INDEX comparison had a
-- timing gap: when a new bar arrives, CURRENT_BAR_INDEX jumps and positions
-- become IS_OPEN=false BEFORE the simulation creates SELL trades for them.
-- This caused "no open positions" in the UI while cash was still tied up.
--
-- FIFO logic: rank each position per symbol by ENTRY_TS. Count cumulative
-- SELL trades per symbol. A position is open if its rank > sell count.
-- This means a position stays open until a SELL trade actually exists,
-- regardless of whether HOLD_UNTIL_INDEX has been passed.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_PORTFOLIO_OPEN_POSITIONS_CANONICAL as
with latest_bar as (
    select
        TS as AS_OF_TS,
        BAR_INDEX as CURRENT_BAR_INDEX
    from MIP.MART.V_BAR_INDEX
    where INTERVAL_MINUTES = 1440
    qualify row_number() over (
        order by TS desc, BAR_INDEX desc
    ) = 1
),
-- All positions scoped to the active episode
episode_positions as (
    select
        p.PORTFOLIO_ID,
        p.RUN_ID,
        p.EPISODE_ID,
        p.SYMBOL,
        p.MARKET_TYPE,
        p.INTERVAL_MINUTES,
        p.ENTRY_TS,
        p.ENTRY_PRICE,
        p.QUANTITY,
        p.COST_BASIS,
        p.ENTRY_SCORE,
        p.ENTRY_INDEX,
        p.HOLD_UNTIL_INDEX,
        e.EPISODE_ID as ACTIVE_EPISODE_ID,
        e.START_TS as EPISODE_START_TS,
        row_number() over (
            partition by p.PORTFOLIO_ID, p.SYMBOL, p.MARKET_TYPE
            order by p.ENTRY_TS
        ) as POS_RANK
    from MIP.APP.PORTFOLIO_POSITIONS p
    left join MIP.APP.V_PORTFOLIO_ACTIVE_EPISODE e
      on e.PORTFOLIO_ID = p.PORTFOLIO_ID
    where
        -- Prefer EPISODE_ID match if column is populated
        (p.EPISODE_ID is not null and p.EPISODE_ID = e.EPISODE_ID)
        -- Fallback to timestamp comparison for legacy data
        or (p.EPISODE_ID is null and (e.START_TS is null or p.ENTRY_TS >= e.START_TS))
),
-- Count SELL trades per symbol in the active episode (FIFO sell matching)
sell_counts as (
    select
        t.PORTFOLIO_ID,
        t.SYMBOL,
        t.MARKET_TYPE,
        count(*) as SELL_COUNT
    from MIP.APP.PORTFOLIO_TRADES t
    left join MIP.APP.V_PORTFOLIO_ACTIVE_EPISODE e
      on e.PORTFOLIO_ID = t.PORTFOLIO_ID
    where t.SIDE = 'SELL'
      and (
          (t.EPISODE_ID is not null and t.EPISODE_ID = e.EPISODE_ID)
          or (t.EPISODE_ID is null and (e.START_TS is null or t.TRADE_TS >= e.START_TS))
      )
    group by t.PORTFOLIO_ID, t.SYMBOL, t.MARKET_TYPE
),
-- A position is OPEN if its FIFO rank > cumulative sells for that symbol.
-- This avoids the timing gap where HOLD_UNTIL_INDEX < CURRENT_BAR_INDEX
-- but no SELL trade has been created yet.
open_positions as (
    select
        ep.PORTFOLIO_ID,
        ep.RUN_ID,
        ep.EPISODE_ID,
        ep.SYMBOL,
        ep.MARKET_TYPE,
        ep.INTERVAL_MINUTES,
        ep.ENTRY_TS,
        ep.ENTRY_PRICE,
        ep.QUANTITY,
        ep.COST_BASIS,
        ep.ENTRY_SCORE,
        ep.ENTRY_INDEX,
        ep.HOLD_UNTIL_INDEX,
        b.AS_OF_TS,
        b.CURRENT_BAR_INDEX,
        true as IS_OPEN
    from episode_positions ep
    cross join latest_bar b
    left join sell_counts sc
      on sc.PORTFOLIO_ID = ep.PORTFOLIO_ID
     and sc.SYMBOL = ep.SYMBOL
     and sc.MARKET_TYPE = ep.MARKET_TYPE
    where ep.POS_RANK > coalesce(sc.SELL_COUNT, 0)
)
select
    PORTFOLIO_ID,
    RUN_ID,
    EPISODE_ID,
    SYMBOL,
    MARKET_TYPE,
    INTERVAL_MINUTES,
    ENTRY_TS,
    ENTRY_PRICE,
    QUANTITY,
    COST_BASIS,
    ENTRY_SCORE,
    ENTRY_INDEX,
    HOLD_UNTIL_INDEX,
    AS_OF_TS,
    CURRENT_BAR_INDEX,
    IS_OPEN,
    count(*) over (partition by PORTFOLIO_ID) as OPEN_POSITIONS
from open_positions;
