-- 040_mart_portfolio_views.sql
-- Purpose: Portfolio simulation helper views

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_BAR_INDEX as
select
    SYMBOL,
    MARKET_TYPE,
    INTERVAL_MINUTES,
    TS,
    CLOSE,
    row_number() over (
        partition by SYMBOL, MARKET_TYPE, INTERVAL_MINUTES
        order by TS
    ) as BAR_INDEX
from MIP.MART.MARKET_BARS
where INTERVAL_MINUTES = 1440;
