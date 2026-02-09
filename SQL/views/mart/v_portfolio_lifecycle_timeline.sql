-- v_portfolio_lifecycle_timeline.sql
-- Purpose: UI-ready timeline of all portfolio lifecycle events.
-- Joins lifecycle events with profile names and episode context.
-- Ordered newest-first for display; the UI can reverse for charts.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_PORTFOLIO_LIFECYCLE_TIMELINE as
select
    le.EVENT_ID,
    le.PORTFOLIO_ID,
    p.NAME                          as PORTFOLIO_NAME,
    le.EVENT_TS,
    le.EVENT_TYPE,
    le.AMOUNT,
    le.CASH_BEFORE,
    le.CASH_AFTER,
    le.EQUITY_BEFORE,
    le.EQUITY_AFTER,
    le.CUMULATIVE_DEPOSITED,
    le.CUMULATIVE_WITHDRAWN,
    le.CUMULATIVE_PNL,
    le.EPISODE_ID,
    le.PROFILE_ID,
    pp.NAME                         as PROFILE_NAME,
    le.NOTES,
    le.CREATED_BY,
    -- Derived: net cash contributed = deposited - withdrawn
    (le.CUMULATIVE_DEPOSITED - le.CUMULATIVE_WITHDRAWN) as NET_CONTRIBUTED,
    -- Derived: lifetime return % = PnL / net contributed (avoid div/0)
    case
        when (le.CUMULATIVE_DEPOSITED - le.CUMULATIVE_WITHDRAWN) > 0
        then le.CUMULATIVE_PNL / (le.CUMULATIVE_DEPOSITED - le.CUMULATIVE_WITHDRAWN)
        else null
    end                             as LIFETIME_RETURN_PCT,
    -- Derived: event label for UI display
    case le.EVENT_TYPE
        when 'CREATE'          then 'Portfolio created'
        when 'DEPOSIT'         then 'Cash deposit +$' || to_varchar(le.AMOUNT, '999,999,999.00')
        when 'WITHDRAW'        then 'Cash withdrawal -$' || to_varchar(le.AMOUNT, '999,999,999.00')
        when 'CRYSTALLIZE'     then 'Profits crystallized $' || to_varchar(le.AMOUNT, '999,999,999.00')
        when 'PROFILE_CHANGE'  then 'Profile changed to ' || coalesce(pp.NAME, 'unknown')
        when 'EPISODE_START'   then 'New episode started'
        when 'EPISODE_END'     then 'Episode ended'
        when 'BUST'            then 'Portfolio bust'
        else le.EVENT_TYPE
    end                             as EVENT_LABEL
from MIP.APP.PORTFOLIO_LIFECYCLE_EVENT le
join MIP.APP.PORTFOLIO p
  on p.PORTFOLIO_ID = le.PORTFOLIO_ID
left join MIP.APP.PORTFOLIO_PROFILE pp
  on pp.PROFILE_ID = le.PROFILE_ID
order by le.EVENT_TS desc, le.EVENT_ID desc;
