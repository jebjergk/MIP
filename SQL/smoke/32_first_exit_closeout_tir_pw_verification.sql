-- 32_first_exit_closeout_tir_pw_verification.sql
-- Run after the first broker-real EXIT or PROTECTIVE (TP/SL) fill is applied in MIP.
-- Edit all literals in the CONFIG section, then execute statements 1..7 in order.

use role MIP_ADMIN_ROLE;
use database MIP;

-- ========= CONFIG (edit) =========
-- Parent entry action (UUID string):
--   ENTRY_ACTION_ID = 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx'
-- Portfolio id:
--   PORTFOLIO_ID = 1
-- Closing order row (exit action order OR protective TP/SL child):
--   CLOSING_ORDER_ID = 'yyyyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy'
-- Optional: wall-clock window for "just happened" filters
--   WINDOW_HOURS = 48
-- =================================

-- 1) Baseline: entry action + existing closeout (should be empty before first proof)
select 'BASELINE_ENTRY_AND_CLOSEOUT' as step,
       la.ACTION_ID,
       la.ACTION_INTENT,
       la.STATUS as action_status,
       la.SYMBOL,
       tc.CLOSEOUT_ID,
       tc.CREATED_TS as closeout_created_ts,
       tc.EXIT_TYPE
from MIP.LIVE.LIVE_ACTIONS la
left join MIP.LIVE.TRADE_CLOSEOUT tc on tc.ENTRY_ACTION_ID = la.ACTION_ID
where la.ACTION_ID = 'REPLACE_ENTRY_ACTION_ID'
  and la.PORTFOLIO_ID = 1;

-- 2) Optional: recent EXECUTION snapshots for symbol/account (prove broker ingested)
--    Join LIVE_PORTFOLIO_CONFIG.IBKR_ACCOUNT_ID in your session if needed.
select 'RECENT_EXECUTIONS_SYMBOL' as step,
       SNAPSHOT_TS,
       SYMBOL,
       OPEN_ORDER_ID,
       PAYLOAD:perm_id::string as perm_id,
       PAYLOAD:exec_id::string as exec_id
from MIP.LIVE.BROKER_SNAPSHOTS
where SNAPSHOT_TYPE = 'EXECUTION'
  and IBKR_ACCOUNT_ID = (select IBKR_ACCOUNT_ID from MIP.LIVE.LIVE_PORTFOLIO_CONFIG where PORTFOLIO_ID = 1 limit 1)
  and upper(SYMBOL) = (select upper(SYMBOL) from MIP.LIVE.LIVE_ACTIONS where ACTION_ID = 'REPLACE_ENTRY_ACTION_ID' limit 1)
  and SNAPSHOT_TS >= dateadd(hour, -48, current_timestamp())
order by SNAPSHOT_TS desc
limit 20;

-- 3) V1: closing order FILLED + prices (protective path requires AVG_FILL_PRICE)
select 'V1_CLOSING_ORDER' as step,
       ORDER_ID,
       ACTION_ID,
       IDEMPOTENCY_KEY,
       STATUS,
       QTY_ORDERED,
       QTY_FILLED,
       AVG_FILL_PRICE,
       FILLED_AT,
       BROKER_ORDER_ID
from MIP.LIVE.LIVE_ORDERS
where ORDER_ID = 'REPLACE_CLOSING_ORDER_ID'
  and PORTFOLIO_ID = 1;

-- 3b) All orders on parent ENTRY_ACTION_ID (entry + :TP/:SL legs share this on path A)
select 'V1_ORDERS_ON_ENTRY_ACTION' as step,
       ORDER_ID,
       ACTION_ID,
       IDEMPOTENCY_KEY,
       STATUS,
       QTY_FILLED,
       AVG_FILL_PRICE,
       FILLED_AT
from MIP.LIVE.LIVE_ORDERS
where ACTION_ID = 'REPLACE_ENTRY_ACTION_ID'
  and PORTFOLIO_ID = 1
order by FILLED_AT nulls last, ORDER_ID;

-- Path B: closing leg may use ACTION_ID <> ENTRY_ACTION_ID; V1 above still targets CLOSING_ORDER_ID.

-- 4) V2: ledger fill event (match ACTION_ID + BROKER_ORDER_ID from closing order)
select 'V2_ORDER_FILLED_LEDGER' as step,
       EVENT_ID,
       EVENT_TS,
       EVENT_TYPE,
       ACTION_ID,
       BROKER_ORDER_ID,
       QTY,
       PRICE
from MIP.LIVE.BROKER_EVENT_LEDGER
where PORTFOLIO_ID = 1
  and EVENT_TYPE in ('ORDER_FILLED', 'ORDER_PARTIAL_FILL')
  and ACTION_ID = (select ACTION_ID from MIP.LIVE.LIVE_ORDERS where ORDER_ID = 'REPLACE_CLOSING_ORDER_ID' limit 1)
  and BROKER_ORDER_ID = (select BROKER_ORDER_ID from MIP.LIVE.LIVE_ORDERS where ORDER_ID = 'REPLACE_CLOSING_ORDER_ID' limit 1)
  and EVENT_TS >= dateadd(hour, -48, current_timestamp())
order by EVENT_TS desc
limit 10;

-- 5) V3: TRADE_CLOSEOUT for this entry
select 'V3_TRADE_CLOSEOUT' as step,
       CLOSEOUT_ID,
       ENTRY_ACTION_ID,
       EXIT_ACTION_ID,
       SYMBOL,
       EXIT_TYPE,
       REALIZED_RETURN_PCT,
       REALIZED_PNL,
       ENTRY_TS,
       EXIT_TS,
       CREATED_TS
from MIP.LIVE.TRADE_CLOSEOUT
where ENTRY_ACTION_ID = 'REPLACE_ENTRY_ACTION_ID'
order by CREATED_TS desc;

-- 6) V4: TIR row (view)
select 'V4_TIR' as step,
       PORTFOLIO_ID,
       CLOSEOUT_ID,
       ENTRY_ACTION_ID,
       SYMBOL,
       ENTRY_TS,
       EXIT_TS,
       HAS_EIS,
       REALIZED_RETURN,
       ALIGNMENT_CLASS,
       OUTCOME_CLASS,
       BEST_PW_SCENARIO_NAME,
       BEST_PW_SCENARIO_RETURN,
       BEST_PW_VS_ACTUAL_DELTA,
       PW_REGRET_AMOUNT,
       PW_REGRET_DRIVER
from MIP.MART.V_TRADE_INTELLIGENCE
where PORTFOLIO_ID = 1
  and ENTRY_ACTION_ID = 'REPLACE_ENTRY_ACTION_ID'
order by EXIT_TS desc nulls last;

-- 7a) V5: PW diff rows for exit calendar day (substitute EXIT_TS from V3 when known)
select 'V5A_PW_DIFF_DAY' as step,
       d.PORTFOLIO_ID,
       d.AS_OF_TS::date as as_of_day,
       count(*) as n_diff_rows,
       count_if(coalesce(s.IS_ACTIVE, false)) as n_active_scenario_rows
from MIP.MART.V_PARALLEL_WORLD_DIFF d
inner join MIP.APP.PARALLEL_WORLD_SCENARIO s on s.SCENARIO_ID = d.SCENARIO_ID
where d.PORTFOLIO_ID = 1
  and d.AS_OF_TS::date = coalesce(
        (select EXIT_TS::date from MIP.LIVE.TRADE_CLOSEOUT where ENTRY_ACTION_ID = 'REPLACE_ENTRY_ACTION_ID' order by CREATED_TS desc limit 1),
        (select ENTRY_TS::date from MIP.LIVE.TRADE_CLOSEOUT where ENTRY_ACTION_ID = 'REPLACE_ENTRY_ACTION_ID' order by CREATED_TS desc limit 1)
      )
group by 1, 2;

-- 7b) Repeat TIR PW columns only (quick read after 7a)
select 'V5B_TIR_PW_ONLY' as step,
       CLOSEOUT_ID,
       EXIT_TS::date as exit_day,
       BEST_PW_SCENARIO_NAME,
       PW_REGRET_AMOUNT,
       PW_REGRET_DRIVER
from MIP.MART.V_TRADE_INTELLIGENCE
where PORTFOLIO_ID = 1
  and ENTRY_ACTION_ID = 'REPLACE_ENTRY_ACTION_ID';
