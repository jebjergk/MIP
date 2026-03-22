-- Backfill: Extract commission from historical BROKER_SNAPSHOTS EXECUTION payloads
-- and populate BROKER_EVENT_LEDGER.COMMISSION and LIVE_ORDERS.TOTAL_COMMISSION.
--
-- Safe to re-run: uses MERGE on BROKER_EXEC_ID for ledger, conditional update for orders.

use role MIP_ADMIN_ROLE;
use database MIP;

-- 1) Backfill BROKER_EVENT_LEDGER from BROKER_SNAPSHOTS EXECUTION rows
merge into MIP.LIVE.BROKER_EVENT_LEDGER t
using (
    select
        SNAPSHOT_ID || '-' || row_number() over (order by SNAPSHOT_TS, SNAPSHOT_ROW_ID) as EVENT_ID,
        SNAPSHOT_TS as EVENT_TS,
        'SNAPSHOT_FILL_BACKFILL' as EVENT_TYPE,
        PORTFOLIO_ID,
        OPEN_ORDER_ID as BROKER_ORDER_ID,
        PAYLOAD:exec_id::string as BROKER_EXEC_ID,
        SYMBOL,
        PAYLOAD:side::string as SIDE,
        POSITION_QTY as QTY,
        AVG_COST as PRICE,
        PAYLOAD:commission::float as COMMISSION,
        CURRENCY,
        PAYLOAD
    from MIP.LIVE.BROKER_SNAPSHOTS
    where SNAPSHOT_TYPE = 'EXECUTION'
      and PAYLOAD:commission is not null
      and PAYLOAD:commission::float != 0
      and PAYLOAD:exec_id is not null
) s
on t.BROKER_EXEC_ID = s.BROKER_EXEC_ID
when matched and (t.COMMISSION is null or t.COMMISSION = 0) then update set
    COMMISSION = s.COMMISSION
when not matched then insert (
    EVENT_ID, EVENT_TS, EVENT_TYPE, PORTFOLIO_ID,
    BROKER_ORDER_ID, BROKER_EXEC_ID, SYMBOL, SIDE,
    QTY, PRICE, COMMISSION, CURRENCY, PAYLOAD
) values (
    s.EVENT_ID, s.EVENT_TS, s.EVENT_TYPE, s.PORTFOLIO_ID,
    s.BROKER_ORDER_ID, s.BROKER_EXEC_ID, s.SYMBOL, s.SIDE,
    s.QTY, s.PRICE, s.COMMISSION, s.CURRENCY, s.PAYLOAD
);

-- 2) Roll up commissions to LIVE_ORDERS from BROKER_EVENT_LEDGER
update MIP.LIVE.LIVE_ORDERS lo
   set TOTAL_COMMISSION = agg.TOTAL_COMM,
       LAST_UPDATED_AT = current_timestamp()
  from (
    select BROKER_ORDER_ID, sum(COMMISSION) as TOTAL_COMM
      from MIP.LIVE.BROKER_EVENT_LEDGER
     where COMMISSION is not null and COMMISSION != 0
       and BROKER_ORDER_ID is not null
     group by BROKER_ORDER_ID
  ) agg
 where lo.BROKER_ORDER_ID = agg.BROKER_ORDER_ID
   and (lo.TOTAL_COMMISSION is null or lo.TOTAL_COMMISSION = 0);
