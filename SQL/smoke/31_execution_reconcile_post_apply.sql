-- 31_execution_reconcile_post_apply.sql
-- Post-apply verification for broker execution → LIVE_ORDERS reconcile (Track A).
-- Replace :PORTFOLIO_ID and optional :ORDER_ID_LIST before run, or edit literals below.

use role MIP_ADMIN_ROLE;
use database MIP;

-- CONFIG (edit)
-- Portfolio under test:
--   PORTFOLIO_ID = 1

-- 1) Recent EXECUTION_RECONCILE_APPLY ledger rows
select 'LEDGER_EXECUTION_RECONCILE_APPLY' as check_name,
       EVENT_TS,
       EVENT_TYPE,
       PORTFOLIO_ID,
       ACTION_ID,
       PAYLOAD:reconcile_run_id::string as reconcile_run_id,
       PAYLOAD:exec_key::string as exec_key,
       PAYLOAD:order_id::string as order_id
from MIP.LIVE.BROKER_EVENT_LEDGER
where PORTFOLIO_ID = 1
  and EVENT_TYPE = 'EXECUTION_RECONCILE_APPLY'
  and EVENT_TS >= dateadd(hour, -72, current_timestamp())
order by EVENT_TS desc
limit 50;

-- 2) LIVE_ORDERS recently FILLED / partial (same portfolio)
select 'LIVE_ORDERS_FILLED_LIKE' as check_name,
       ORDER_ID,
       ACTION_ID,
       STATUS,
       QTY_FILLED,
       AVG_FILL_PRICE,
       FILLED_AT,
       LAST_UPDATED_AT,
       BROKER_ORDER_ID
from MIP.LIVE.LIVE_ORDERS
where PORTFOLIO_ID = 1
  and upper(coalesce(STATUS, '')) in ('FILLED', 'PARTIAL_FILL')
  and coalesce(LAST_UPDATED_AT, CREATED_AT) >= dateadd(hour, -72, current_timestamp())
order by LAST_UPDATED_AT desc nulls last
limit 50;

-- 3) TRADE_CLOSEOUT created recently (portfolio via entry action)
select 'TRADE_CLOSEOUT_RECENT' as check_name,
       tc.CLOSEOUT_ID,
       tc.ENTRY_ACTION_ID,
       tc.EXIT_ACTION_ID,
       tc.SYMBOL,
       tc.EXIT_TYPE,
       tc.CREATED_TS,
       la.PORTFOLIO_ID
from MIP.LIVE.TRADE_CLOSEOUT tc
inner join MIP.LIVE.LIVE_ACTIONS la on la.ACTION_ID = tc.ENTRY_ACTION_ID
where la.PORTFOLIO_ID = 1
  and tc.CREATED_TS >= dateadd(hour, -72, current_timestamp())
order by tc.CREATED_TS desc
limit 50;

-- 4) TIR rows for same closeouts (view = one row per CLOSEOUT_ID)
select 'TIR_FOR_RECENT_CLOSEOUTS' as check_name,
       t.CLOSEOUT_ID,
       t.ENTRY_ACTION_ID,
       t.SYMBOL,
       t.EXIT_TS,
       t.HAS_EIS,
       t.REALIZED_RETURN,
       t.BEST_PW_SCENARIO_NAME,
       t.PW_REGRET_AMOUNT
from MIP.MART.V_TRADE_INTELLIGENCE t
where t.PORTFOLIO_ID = 1
  and t.CLOSEOUT_ID in (
    select tc.CLOSEOUT_ID
    from MIP.LIVE.TRADE_CLOSEOUT tc
    inner join MIP.LIVE.LIVE_ACTIONS la on la.ACTION_ID = tc.ENTRY_ACTION_ID
    where la.PORTFOLIO_ID = 1
      and tc.CREATED_TS >= dateadd(hour, -72, current_timestamp())
  )
order by t.EXIT_TS desc nulls last;

-- 5) PW diff availability for those TIR exit days (regret-capable when true)
select 'PW_DIFF_FOR_TIR_DAYS' as check_name,
       t.CLOSEOUT_ID,
       coalesce(t.EXIT_TS::date, t.ENTRY_TS::date) as pw_calendar_date,
       count(d.SCENARIO_ID) as n_diff_rows
from MIP.MART.V_TRADE_INTELLIGENCE t
left join MIP.MART.V_PARALLEL_WORLD_DIFF d
  on d.PORTFOLIO_ID = t.PORTFOLIO_ID
 and d.AS_OF_TS::date = coalesce(t.EXIT_TS::date, t.ENTRY_TS::date)
where t.PORTFOLIO_ID = 1
  and t.CLOSEOUT_ID in (
    select tc.CLOSEOUT_ID
    from MIP.LIVE.TRADE_CLOSEOUT tc
    inner join MIP.LIVE.LIVE_ACTIONS la on la.ACTION_ID = tc.ENTRY_ACTION_ID
    where la.PORTFOLIO_ID = 1
      and tc.CREATED_TS >= dateadd(hour, -72, current_timestamp())
  )
group by t.CLOSEOUT_ID, pw_calendar_date
order by pw_calendar_date desc;
