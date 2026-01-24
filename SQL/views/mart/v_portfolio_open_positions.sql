-- v_portfolio_open_positions.sql
-- Purpose: Canonical open-position state based on portfolio positions and latest bar index

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_PORTFOLIO_OPEN_POSITIONS as
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
        p.HOLD_UNTIL_INDEX >= b.CURRENT_BAR_INDEX as IS_OPEN
    from MIP.APP.PORTFOLIO_POSITIONS p
    cross join latest_bar b
)
select
    PORTFOLIO_ID,
    RUN_ID,
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
from positions_with_state
where IS_OPEN;
