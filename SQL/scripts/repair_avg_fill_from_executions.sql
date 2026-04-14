-- repair_avg_fill_from_executions.sql
-- One-shot: set LIVE_ORDERS.AVG_FILL_PRICE from latest BROKER_SNAPSHOTS EXECUTION payload
-- when STATUS = FILLED and AVG_FILL_PRICE is null. Uses PAYLOAD:avg_price then PAYLOAD:price.
--
-- Preconditions: run SELECT-only join first (see MIP/docs/avg_fill_price_repair_plan.md).
-- Edit PORTFOLIO_ID / lookback if needed.

use role MIP_ADMIN_ROLE;
use database MIP;

-- Preview (optional)
-- with ex as ( ... same subquery ... )
-- select o.ORDER_ID, o.SYMBOL, o.BROKER_ORDER_ID, o.AVG_FILL_PRICE as before_avg, coalesce(x.avgp, x.lastp) as proposed_avg
-- from MIP.LIVE.LIVE_ORDERS o
-- inner join ex x on trim(o.BROKER_ORDER_ID) = trim(x.broker_key) and x.rn = 1
-- where o.PORTFOLIO_ID = 1 and o.STATUS = 'FILLED' and o.AVG_FILL_PRICE is null and coalesce(x.avgp, x.lastp) is not null;

update MIP.LIVE.LIVE_ORDERS o
set
  o.AVG_FILL_PRICE = coalesce(x.avgp, x.lastp),
  o.LAST_UPDATED_AT = current_timestamp()
from (
  select
    coalesce(
      OPEN_ORDER_ID::string,
      PAYLOAD:perm_id::string,
      PAYLOAD:orderId::string,
      PAYLOAD:order_id::string
    ) as broker_key,
    try_to_double(PAYLOAD:avg_price::varchar) as avgp,
    try_to_double(PAYLOAD:price::varchar) as lastp,
    row_number() over (
      partition by coalesce(
        OPEN_ORDER_ID::string,
        PAYLOAD:perm_id::string,
        PAYLOAD:orderId::string,
        PAYLOAD:order_id::string
      )
      order by SNAPSHOT_TS desc
    ) as rn
  from MIP.LIVE.BROKER_SNAPSHOTS
  where SNAPSHOT_TYPE = 'EXECUTION'
    and IBKR_ACCOUNT_ID = (select IBKR_ACCOUNT_ID from MIP.LIVE.LIVE_PORTFOLIO_CONFIG where PORTFOLIO_ID = 1 limit 1)
    and SNAPSHOT_TS >= dateadd(day, -30, current_timestamp())
) x
where x.rn = 1
  and trim(o.BROKER_ORDER_ID) = trim(x.broker_key)
  and o.PORTFOLIO_ID = 1
  and o.STATUS = 'FILLED'
  and o.AVG_FILL_PRICE is null
  and coalesce(x.avgp, x.lastp) is not null;
