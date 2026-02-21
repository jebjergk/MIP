-- v_early_exit_summary.sql
-- Purpose: Analytics views for the intraday early-exit layer.
-- Provides decision-first summary, per-symbol/pattern breakdown,
-- and "hit after" time distribution for the UI.

use role MIP_ADMIN_ROLE;
use database MIP;

-- ═══════════════════════════════════════════════════════════════════
-- 1. V_EARLY_EXIT_OVERVIEW — headline KPIs per portfolio
-- ═══════════════════════════════════════════════════════════════════
create or replace view MIP.MART.V_EARLY_EXIT_OVERVIEW as
select
    el.PORTFOLIO_ID,
    p.NAME as PORTFOLIO_NAME,
    el.MODE,
    count(*)                                              as EVALUATIONS,
    count(distinct el.SYMBOL || el.ENTRY_TS)              as POSITIONS_TRACKED,
    sum(iff(el.PAYOFF_REACHED, 1, 0))                     as PAYOFF_REACHED_COUNT,
    round(sum(iff(el.PAYOFF_REACHED, 1, 0)) * 100.0
        / nullif(count(distinct el.SYMBOL || el.ENTRY_TS), 0), 1)
                                                          as PAYOFF_REACHED_PCT,
    sum(iff(el.EXIT_SIGNAL, 1, 0))                        as EXIT_SIGNALS,
    sum(iff(el.EXECUTION_STATUS = 'EXECUTED', 1, 0))      as EXITS_EXECUTED,
    round(sum(el.EARLY_EXIT_PNL), 2)                      as TOTAL_EARLY_EXIT_PNL,
    round(sum(el.HOLD_TO_END_PNL), 2)                     as TOTAL_HOLD_PNL,
    round(sum(el.PNL_DELTA), 2)                           as TOTAL_PNL_DELTA,
    round(avg(case when el.EXIT_SIGNAL then el.PNL_DELTA end), 2)
                                                          as AVG_PNL_DELTA_PER_EXIT,
    round(avg(case when el.PAYOFF_REACHED
              then el.UNREALIZED_RETURN * 100 end), 2)    as AVG_RETURN_AT_PAYOFF_PCT,
    round(avg(case when el.PAYOFF_REACHED
              then el.MFE_RETURN * 100 end), 2)           as AVG_MFE_PCT,
    round(avg(case when el.PAYOFF_REACHED
              then el.GIVEBACK_PCT * 100 end), 2)         as AVG_GIVEBACK_PCT,
    min(el.BAR_CLOSE_TS)                                  as FIRST_EVALUATION,
    max(el.BAR_CLOSE_TS)                                  as LAST_EVALUATION
from MIP.APP.EARLY_EXIT_LOG el
join MIP.APP.PORTFOLIO p on p.PORTFOLIO_ID = el.PORTFOLIO_ID
group by el.PORTFOLIO_ID, p.NAME, el.MODE;


-- ═══════════════════════════════════════════════════════════════════
-- 2. V_EARLY_EXIT_BY_SYMBOL — breakdown per symbol + market type
-- ═══════════════════════════════════════════════════════════════════
create or replace view MIP.MART.V_EARLY_EXIT_BY_SYMBOL as
select
    el.PORTFOLIO_ID,
    el.SYMBOL,
    el.MARKET_TYPE,
    el.MODE,
    count(distinct el.ENTRY_TS)                           as POSITIONS,
    sum(iff(el.PAYOFF_REACHED, 1, 0))                     as PAYOFF_REACHED,
    round(sum(iff(el.PAYOFF_REACHED, 1, 0)) * 100.0
        / nullif(count(distinct el.ENTRY_TS), 0), 1)     as PAYOFF_REACHED_PCT,
    sum(iff(el.EXIT_SIGNAL, 1, 0))                        as EXIT_SIGNALS,
    round(sum(el.EARLY_EXIT_PNL), 2)                      as EARLY_EXIT_PNL,
    round(sum(el.HOLD_TO_END_PNL), 2)                     as HOLD_PNL,
    round(sum(el.PNL_DELTA), 2)                           as PNL_DELTA,
    round(avg(case when el.PAYOFF_REACHED
              then el.PAYOFF_HIT_AFTER_MINS end), 0)      as AVG_HIT_AFTER_MINS,
    round(avg(case when el.PAYOFF_REACHED
              then el.MFE_RETURN * 100 end), 2)           as AVG_MFE_PCT,
    round(avg(case when el.EXIT_SIGNAL
              then el.GIVEBACK_PCT * 100 end), 2)         as AVG_GIVEBACK_PCT
from MIP.APP.EARLY_EXIT_LOG el
group by el.PORTFOLIO_ID, el.SYMBOL, el.MARKET_TYPE, el.MODE;


-- ═══════════════════════════════════════════════════════════════════
-- 3. V_EARLY_EXIT_HIT_DISTRIBUTION — how quickly payoff is achieved
-- ═══════════════════════════════════════════════════════════════════
create or replace view MIP.MART.V_EARLY_EXIT_HIT_DISTRIBUTION as
select
    el.PORTFOLIO_ID,
    el.MARKET_TYPE,
    case
        when el.PAYOFF_HIT_AFTER_MINS <=  15 then '0-15 min'
        when el.PAYOFF_HIT_AFTER_MINS <=  30 then '15-30 min'
        when el.PAYOFF_HIT_AFTER_MINS <=  60 then '30-60 min'
        when el.PAYOFF_HIT_AFTER_MINS <= 120 then '1-2 hrs'
        when el.PAYOFF_HIT_AFTER_MINS <= 240 then '2-4 hrs'
        else '4+ hrs'
    end                                                   as HIT_AFTER_BUCKET,
    case
        when el.PAYOFF_HIT_AFTER_MINS <=  15 then 1
        when el.PAYOFF_HIT_AFTER_MINS <=  30 then 2
        when el.PAYOFF_HIT_AFTER_MINS <=  60 then 3
        when el.PAYOFF_HIT_AFTER_MINS <= 120 then 4
        when el.PAYOFF_HIT_AFTER_MINS <= 240 then 5
        else 6
    end                                                   as BUCKET_ORDER,
    count(*)                                              as POSITION_COUNT,
    round(avg(el.UNREALIZED_RETURN * 100), 2)             as AVG_RETURN_AT_HIT_PCT,
    round(avg(el.MFE_RETURN * 100), 2)                    as AVG_MFE_PCT,
    sum(iff(el.EXIT_SIGNAL, 1, 0))                        as EXIT_SIGNALS_IN_BUCKET,
    round(sum(el.PNL_DELTA), 2)                           as PNL_DELTA_IN_BUCKET
from MIP.APP.EARLY_EXIT_LOG el
where el.PAYOFF_REACHED = true
group by el.PORTFOLIO_ID, el.MARKET_TYPE,
         HIT_AFTER_BUCKET, BUCKET_ORDER;


-- ═══════════════════════════════════════════════════════════════════
-- 4. V_EARLY_EXIT_DETAIL — full log for the Decision Explorer
-- ═══════════════════════════════════════════════════════════════════
create or replace view MIP.MART.V_EARLY_EXIT_DETAIL as
select
    el.LOG_ID,
    el.RUN_ID,
    el.PORTFOLIO_ID,
    p.NAME as PORTFOLIO_NAME,
    el.SYMBOL,
    el.MARKET_TYPE,
    el.ENTRY_TS,
    el.ENTRY_PRICE,
    el.QUANTITY,
    el.COST_BASIS,
    el.HOLD_UNTIL_INDEX,
    el.BAR_CLOSE_TS,
    el.DECISION_TS,
    el.TARGET_RETURN,
    el.PAYOFF_MULTIPLIER,
    el.EFFECTIVE_TARGET,
    el.CURRENT_PRICE,
    round(el.UNREALIZED_RETURN * 100, 4)                  as UNREALIZED_RETURN_PCT,
    round(el.MFE_RETURN * 100, 4)                         as MFE_RETURN_PCT,
    el.MFE_TS,
    el.PAYOFF_REACHED,
    el.PAYOFF_FIRST_HIT_TS,
    el.PAYOFF_HIT_AFTER_MINS,
    round(el.GIVEBACK_FROM_PEAK * 100, 4)                 as GIVEBACK_FROM_PEAK_PCT,
    round(el.GIVEBACK_PCT * 100, 2)                       as GIVEBACK_PCT,
    el.NO_NEW_HIGH_BARS,
    el.GIVEBACK_TRIGGERED,
    el.EXIT_SIGNAL,
    el.EXIT_PRICE,
    el.FEES_APPLIED,
    round(el.EARLY_EXIT_PNL, 2)                           as EARLY_EXIT_PNL,
    round(el.HOLD_TO_END_RETURN * 100, 4)                 as HOLD_TO_END_RETURN_PCT,
    round(el.HOLD_TO_END_PNL, 2)                          as HOLD_TO_END_PNL,
    round(el.PNL_DELTA, 2)                                as PNL_DELTA,
    el.MODE,
    el.EXECUTION_STATUS,
    el.REASON_CODES,
    el.CREATED_AT
from MIP.APP.EARLY_EXIT_LOG el
join MIP.APP.PORTFOLIO p on p.PORTFOLIO_ID = el.PORTFOLIO_ID;


-- ═══════════════════════════════════════════════════════════════════
-- 5. V_EARLY_EXIT_POSITION_TRACKER — current state of each tracked position
-- ═══════════════════════════════════════════════════════════════════
create or replace view MIP.MART.V_EARLY_EXIT_POSITION_TRACKER as
select
    ps.PORTFOLIO_ID,
    p.NAME as PORTFOLIO_NAME,
    ps.SYMBOL,
    op.MARKET_TYPE,
    ps.ENTRY_TS,
    op.ENTRY_PRICE,
    op.QUANTITY,
    op.COST_BASIS,
    op.HOLD_UNTIL_INDEX,
    round(ps.FIRST_HIT_RETURN * 100, 4)                  as FIRST_HIT_RETURN_PCT,
    ps.FIRST_HIT_TS,
    datediff('minute', ps.ENTRY_TS, ps.FIRST_HIT_TS)     as HIT_AFTER_MINS,
    round(ps.MFE_RETURN * 100, 4)                         as MFE_RETURN_PCT,
    ps.MFE_TS,
    round(ps.MAE_RETURN * 100, 4)                         as MAE_RETURN_PCT,
    ps.MAE_TS,
    ps.LAST_EVALUATED_TS,
    ps.EARLY_EXIT_FIRED,
    ps.EARLY_EXIT_TS,
    op.IS_OPEN,
    ps.UPDATED_AT
from MIP.APP.EARLY_EXIT_POSITION_STATE ps
join MIP.APP.PORTFOLIO p on p.PORTFOLIO_ID = ps.PORTFOLIO_ID
left join MIP.MART.V_PORTFOLIO_OPEN_POSITIONS_CANONICAL op
  on op.PORTFOLIO_ID = ps.PORTFOLIO_ID
 and op.SYMBOL = ps.SYMBOL
 and op.ENTRY_TS = ps.ENTRY_TS;
