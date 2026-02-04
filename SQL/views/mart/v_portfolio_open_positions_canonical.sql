-- v_portfolio_open_positions_canonical.sql
-- Purpose: Canonical single source of truth for open positions used by all procedures.
-- Only positions belonging to the active episode are considered "open".
-- Uses EPISODE_ID column if populated, otherwise falls back to timestamp comparison.

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
positions_with_state as (
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
        b.AS_OF_TS,
        b.CURRENT_BAR_INDEX,
        p.HOLD_UNTIL_INDEX >= b.CURRENT_BAR_INDEX as IS_OPEN,
        e.EPISODE_ID as ACTIVE_EPISODE_ID,
        e.START_TS as EPISODE_START_TS
    from MIP.APP.PORTFOLIO_POSITIONS p
    cross join latest_bar b
    left join MIP.APP.V_PORTFOLIO_ACTIVE_EPISODE e
      on e.PORTFOLIO_ID = p.PORTFOLIO_ID
),
open_in_window as (
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
        IS_OPEN
    from positions_with_state
    where IS_OPEN
      and (
          -- Prefer EPISODE_ID match if column is populated
          (EPISODE_ID is not null and EPISODE_ID = ACTIVE_EPISODE_ID)
          -- Fallback to timestamp comparison for legacy data
          or (EPISODE_ID is null and (EPISODE_START_TS is null or ENTRY_TS >= EPISODE_START_TS))
      )
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
    count_if(IS_OPEN) over (partition by PORTFOLIO_ID) as OPEN_POSITIONS
from open_in_window;
