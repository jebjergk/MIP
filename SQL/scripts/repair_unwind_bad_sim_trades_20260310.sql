-- repair_unwind_bad_sim_trades_20260310.sql
-- Purpose: Unwind 15 bad simulation BUY trades from 2026-03-10 open-session run,
-- restore cash chain, and mark proposals as manually rejected.

use role MIP_ADMIN_ROLE;
use database MIP;
use schema APP;

create or replace temporary table TMP_BAD_TRADES as
select
    t.TRADE_ID,
    t.PORTFOLIO_ID,
    t.SYMBOL,
    t.TRADE_TS,
    t.PROPOSAL_ID,
    t.NOTIONAL,
    t.RUN_ID
from MIP.APP.PORTFOLIO_TRADES t
where t.RUN_ID = '8cdf8a7d-7f34-4376-a1aa-5b1803f9ec0b'
  and t.PROPOSAL_ID between 4901 and 4915
  and t.SIDE = 'BUY'
  and t.TRADE_TS between '2026-03-10 17:35:00'::timestamp_ntz and '2026-03-10 17:37:00'::timestamp_ntz;

select
    count(*) as BAD_TRADE_COUNT,
    min(TRADE_TS) as MIN_TS,
    max(TRADE_TS) as MAX_TS,
    sum(NOTIONAL) as TOTAL_NOTIONAL
from TMP_BAD_TRADES;

begin;

delete from MIP.APP.EARLY_EXIT_POSITION_STATE eps
using TMP_BAD_TRADES b
where eps.PORTFOLIO_ID = b.PORTFOLIO_ID
  and eps.SYMBOL = b.SYMBOL
  and eps.ENTRY_TS = b.TRADE_TS;

delete from MIP.APP.PORTFOLIO_POSITIONS p
using TMP_BAD_TRADES b
where p.PORTFOLIO_ID = b.PORTFOLIO_ID
  and p.SYMBOL = b.SYMBOL
  and p.ENTRY_TS = b.TRADE_TS;

delete from MIP.APP.PORTFOLIO_TRADES t
using TMP_BAD_TRADES b
where t.TRADE_ID = b.TRADE_ID;

update MIP.AGENT_OUT.ORDER_PROPOSALS p
   set STATUS = 'REJECTED',
       APPROVED_AT = null,
       VALIDATION_ERRORS = array_distinct(
           array_cat(
               coalesce(p.VALIDATION_ERRORS, array_construct()),
               array_construct('MANUAL_UNWIND_COMMITTEE_BLOCKED')
           )
       ),
       RATIONALE = object_insert(
           coalesce(p.RATIONALE, object_construct()),
           'manual_unwind',
           object_construct(
               'ts', current_timestamp(),
               'reason', 'Committee fallback batch unwind requested',
               'source_script', 'repair_unwind_bad_sim_trades_20260310.sql'
           ),
           true
       )
where p.PROPOSAL_ID in (select PROPOSAL_ID from TMP_BAD_TRADES);

update MIP.APP.PORTFOLIO_TRADES t
   set CASH_AFTER = rc.CORRECT_CASH_AFTER
  from (
      select
          t2.TRADE_ID,
          round(
              p.STARTING_CASH
              + sum(
                  case
                      when upper(t2.SIDE) = 'BUY' then -t2.NOTIONAL
                      when upper(t2.SIDE) = 'SELL' then  t2.NOTIONAL
                      else 0
                  end
              ) over (
                  partition by t2.PORTFOLIO_ID
                  order by t2.TRADE_TS, t2.TRADE_ID
                  rows between unbounded preceding and current row
              ),
              2
          ) as CORRECT_CASH_AFTER
      from MIP.APP.PORTFOLIO_TRADES t2
      join MIP.APP.PORTFOLIO p
        on p.PORTFOLIO_ID = t2.PORTFOLIO_ID
      where t2.PORTFOLIO_ID in (select distinct PORTFOLIO_ID from TMP_BAD_TRADES)
  ) rc
 where t.TRADE_ID = rc.TRADE_ID
   and t.CASH_AFTER != rc.CORRECT_CASH_AFTER;

commit;

select count(*) as REMAINING_BAD_TRADES
from MIP.APP.PORTFOLIO_TRADES
where RUN_ID = '8cdf8a7d-7f34-4376-a1aa-5b1803f9ec0b'
  and PROPOSAL_ID between 4901 and 4915
  and SIDE = 'BUY'
  and TRADE_TS between '2026-03-10 17:35:00'::timestamp_ntz and '2026-03-10 17:37:00'::timestamp_ntz;
