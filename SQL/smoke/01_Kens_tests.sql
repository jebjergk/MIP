use role MIP_ADMIN_ROLE;
use database MIP;

-- =============================================================================
-- DIAGNOSTIC: Where did the $44 go? (Portfolio 1 lost money with no trades)
-- =============================================================================


ALTER GIT REPOSITORY MIP.APP.MIP FETCH;
select * from mip.app.mip_audit_log order by event_ts desc;

select * from mip.app.mip_audit_log where event_ts::date = '2026-02-01' order by event_ts desc;

call MIP.APP.SP_RUN_DAILY_PIPELINE();


select max(PROPOSED_AT) as MAX_PROPOSED_AT, count(*) as TOTAL
from MIP.AGENT_OUT.ORDER_PROPOSALS;

select EVENT_TS, EVENT_NAME, STATUS, DETAILS
from MIP.APP.MIP_AUDIT_LOG
where EVENT_TS >= '2026-01-10'
  and (EVENT_NAME ilike '%PROPOSE%' or EVENT_NAME ilike '%ORDER_PROPOSALS%' or EVENT_NAME ilike '%AGENT%')
order by EVENT_TS desc
limit 200;

select STATUS, count(*) as CNT
from MIP.AGENT_OUT.ORDER_PROPOSALS
where PROPOSED_AT is null
group by 1
order by 2 desc;

select
  PORTFOLIO_ID,
  count(*) as PROPOSALS,
  min(PROPOSED_AT) as MIN_TS,
  max(PROPOSED_AT) as MAX_TS
from MIP.AGENT_OUT.ORDER_PROPOSALS
group by 1
order by MAX_TS desc nulls last;

select market_type, count(*) as ELIGIBLE
from MIP.APP.V_SIGNALS_ELIGIBLE_TODAY
group by 1
order by 2 desc;

select market_type, count(*) as RECS, max(TS) as MAX_TS
from MIP.APP.RECOMMENDATION_LOG
group by 1;

-- =============================================================================
-- PROPOSER DIAGNOSTICS (2026-02-06 fix)
-- These queries verify the proposer candidate chain is working correctly
-- =============================================================================

-- 1. Check if signals exist at latest bar TS
-- Raw signals should be > 0 if recommendations exist for today's bar date
select 
    'V_SIGNALS_LATEST_TS' as VIEW_NAME,
    count(*) as SIGNAL_COUNT,
    max(SIGNAL_TS) as MAX_SIGNAL_TS
from MIP.MART.V_SIGNALS_LATEST_TS;

-- 2. Check if trusted signals exist
-- Trusted signals should be > 0 if there are trusted patterns and signals at latest TS
select 
    'V_TRUSTED_SIGNALS_LATEST_TS' as VIEW_NAME,
    count(*) as TRUSTED_SIGNAL_COUNT,
    max(SIGNAL_TS) as MAX_SIGNAL_TS
from MIP.MART.V_TRUSTED_SIGNALS_LATEST_TS;

-- 3. Check trusted pattern count
-- Should be > 0 if any patterns have passed training thresholds
select 
    'V_TRUSTED_PATTERN_HORIZONS' as VIEW_NAME,
    count(*) as TRUSTED_PATTERN_COUNT
from MIP.MART.V_TRUSTED_PATTERN_HORIZONS;

-- 4. Check bar vs rec timestamp alignment
-- latest_bar_ts and latest_rec_ts should match for proposals to be generated
select
    (select max(TS) from MIP.MART.MARKET_BARS where INTERVAL_MINUTES = 1440) as LATEST_BAR_TS,
    (select max(TS) from MIP.APP.RECOMMENDATION_LOG where INTERVAL_MINUTES = 1440) as LATEST_REC_TS,
    iff(
        (select max(TS) from MIP.MART.MARKET_BARS where INTERVAL_MINUTES = 1440) = 
        (select max(TS) from MIP.APP.RECOMMENDATION_LOG where INTERVAL_MINUTES = 1440),
        'ALIGNED',
        'MISALIGNED - recs may be stale'
    ) as ALIGNMENT_STATUS;

-- 5. Recommendation freshness by market type
-- All market types (STOCK, ETF, FX) should have recent MAX_TS
select 
    MARKET_TYPE,
    count(*) as REC_COUNT,
    max(TS) as MAX_TS,
    datediff('day', max(TS), (select max(TS) from MIP.MART.MARKET_BARS where INTERVAL_MINUTES = 1440)) as DAYS_BEHIND_BAR
from MIP.APP.RECOMMENDATION_LOG
where INTERVAL_MINUTES = 1440
group by MARKET_TYPE
order by MARKET_TYPE;

-- 6. Latest proposer audit events
-- Check what the proposer reported about candidates
select 
    EVENT_TS,
    STATUS,
    DETAILS:candidate_count_raw::int as RAW_CANDIDATES,
    DETAILS:candidate_count_trusted::int as TRUSTED_CANDIDATES,
    DETAILS:trusted_rejected_count::int as REJECTED,
    DETAILS:no_candidates_reason::string as REASON,
    DETAILS:remaining_capacity::int as CAPACITY,
    DETAILS:entries_blocked::boolean as ENTRIES_BLOCKED
from MIP.APP.MIP_AUDIT_LOG
where EVENT_NAME = 'SP_AGENT_PROPOSE_TRADES'
order by EVENT_TS desc
limit 10;

-- 7. Verify view chain works end-to-end
-- This CTE replicates the candidate selection logic
with signal_check as (
    select 
        'Step 1: Raw Signals' as STEP,
        count(*) as CNT,
        max(SIGNAL_TS) as MAX_TS
    from MIP.MART.V_SIGNALS_LATEST_TS
),
trusted_check as (
    select
        'Step 2: Trusted Signals' as STEP,
        count(*) as CNT,
        max(SIGNAL_TS) as MAX_TS
    from MIP.MART.V_TRUSTED_SIGNALS_LATEST_TS
),
pattern_check as (
    select
        'Step 3: Trusted Patterns' as STEP,
        count(*) as CNT,
        null as MAX_TS
    from MIP.MART.V_TRUSTED_PATTERN_HORIZONS
)
select * from signal_check
union all select * from trusted_check
union all select * from pattern_check
order by STEP;

-- =============================================================================
-- RECOMMENDATION GENERATION DIAGNOSTICS
-- Why are recommendations stale for STOCK/ETF?
-- =============================================================================

-- 8. Latest RECOMMENDATIONS audit events by market type
-- Shows skip_reason for each market type
select 
    DETAILS:market_type::string as MARKET_TYPE,
    EVENT_TS,
    DETAILS:inserted_count::int as INSERTED,
    DETAILS:skip_reason::string as SKIP_REASON,
    DETAILS:latest_market_bars_ts::timestamp as BARS_TS,
    DETAILS:latest_returns_ts::timestamp as RETURNS_TS,
    DETAILS:existing_recs_at_latest_ts::int as ALREADY_EXISTS,
    DETAILS:pattern_count::int as PATTERN_COUNT
from MIP.APP.MIP_AUDIT_LOG
where EVENT_NAME = 'RECOMMENDATIONS'
  and DETAILS:scope::string = 'MARKET_TYPE'
order by EVENT_TS desc
limit 15;

-- 9. Check MARKET_RETURNS freshness
-- STOCK/ETF recs use MARKET_RETURNS as data source
select 
    MARKET_TYPE,
    INTERVAL_MINUTES,
    count(*) as ROW_COUNT,
    max(TS) as MAX_TS,
    datediff('day', max(TS), current_date()) as DAYS_STALE
from MIP.MART.MARKET_RETURNS
where INTERVAL_MINUTES = 1440
group by MARKET_TYPE, INTERVAL_MINUTES
order by MARKET_TYPE;

-- 10. Check MARKET_BARS freshness (compared to MARKET_RETURNS)
-- FX recs use MARKET_BARS directly
select 
    MARKET_TYPE,
    INTERVAL_MINUTES,
    count(*) as ROW_COUNT,
    max(TS) as MAX_TS_BARS,
    (select max(TS) from MIP.MART.MARKET_RETURNS r 
     where r.MARKET_TYPE = b.MARKET_TYPE 
       and r.INTERVAL_MINUTES = b.INTERVAL_MINUTES) as MAX_TS_RETURNS,
    datediff('day', 
        (select max(TS) from MIP.MART.MARKET_RETURNS r 
         where r.MARKET_TYPE = b.MARKET_TYPE 
           and r.INTERVAL_MINUTES = b.INTERVAL_MINUTES),
        max(TS)
    ) as RETURNS_BEHIND_BARS_DAYS
from MIP.MART.MARKET_BARS b
where INTERVAL_MINUTES = 1440
group by MARKET_TYPE, INTERVAL_MINUTES
order by MARKET_TYPE;

-- 11. Check active patterns by market type
-- If pattern_count = 0 for a market type, no recs will be generated
select 
    upper(coalesce(PARAMS_JSON:market_type::string, 'STOCK')) as MARKET_TYPE,
    coalesce(PARAMS_JSON:interval_minutes::number, 1440) as INTERVAL_MINUTES,
    count(*) as ACTIVE_PATTERN_COUNT
from MIP.APP.PATTERN_DEFINITION
where coalesce(IS_ACTIVE, 'N') = 'Y'
  and coalesce(ENABLED, true)
group by 1, 2
order by 1;

-- 12. Check if V_MARKET_RETURNS is a view that needs refresh or has issues
-- Get the most recent returns for each market type
select 
    MARKET_TYPE,
    max(TS) as LATEST_RETURN_TS,
    count(*) as RETURNS_LAST_7_DAYS
from MIP.MART.MARKET_RETURNS
where TS >= dateadd(day, -7, current_date())
  and INTERVAL_MINUTES = 1440
group by MARKET_TYPE
order by MARKET_TYPE;

-- =============================================================================
-- PATTERN THRESHOLD DIAGNOSTICS
-- Why is everything FILTERED_BY_THRESHOLD?
-- =============================================================================

-- 13. Check active patterns and their thresholds
select 
    PATTERN_ID,
    NAME,
    PARAMS_JSON:market_type::string as MARKET_TYPE,
    PARAMS_JSON:interval_minutes::number as INTERVAL_MINUTES,
    PARAMS_JSON:min_return::float as MIN_RETURN,
    PARAMS_JSON:min_zscore::float as MIN_ZSCORE,
    PARAMS_JSON:slow_window::number as SLOW_WINDOW,
    PARAMS_JSON:fast_window::number as FAST_WINDOW,
    PARAMS_JSON:lookback_days::number as LOOKBACK_DAYS,
    IS_ACTIVE,
    ENABLED,
    LAST_TRADE_COUNT
from MIP.APP.PATTERN_DEFINITION
where coalesce(IS_ACTIVE, 'N') = 'Y'
  and coalesce(ENABLED, true)
order by MARKET_TYPE, NAME;

-- 14. Check today's returns to see what's passing thresholds
-- How many symbols had positive returns today?
select 
    MARKET_TYPE,
    count(*) as TOTAL_SYMBOLS,
    count_if(RETURN_SIMPLE > 0) as POSITIVE_RETURNS,
    count_if(RETURN_SIMPLE >= 0.002) as ABOVE_0_2_PCT,
    count_if(RETURN_SIMPLE >= 0.005) as ABOVE_0_5_PCT,
    count_if(RETURN_SIMPLE >= 0.01) as ABOVE_1_PCT,
    avg(RETURN_SIMPLE) as AVG_RETURN,
    max(RETURN_SIMPLE) as MAX_RETURN
from MIP.MART.MARKET_RETURNS
where TS = (select max(TS) from MIP.MART.MARKET_BARS where INTERVAL_MINUTES = 1440)
  and INTERVAL_MINUTES = 1440
group by MARKET_TYPE
order by MARKET_TYPE;

-- 15. Check recent price trends - do any symbols have consecutive positive days?
-- This checks the POSITIVE_LAG_COUNT requirement
with recent_returns as (
    select 
        SYMBOL,
        MARKET_TYPE,
        TS,
        RETURN_SIMPLE,
        lag(RETURN_SIMPLE, 1) over (partition by SYMBOL, MARKET_TYPE order by TS) as LAG1,
        lag(RETURN_SIMPLE, 2) over (partition by SYMBOL, MARKET_TYPE order by TS) as LAG2,
        lag(RETURN_SIMPLE, 3) over (partition by SYMBOL, MARKET_TYPE order by TS) as LAG3
    from MIP.MART.MARKET_RETURNS
    where INTERVAL_MINUTES = 1440
      and TS >= dateadd(day, -7, current_date())
),
at_latest as (
    select *
    from recent_returns
    where TS = (select max(TS) from MIP.MART.MARKET_BARS where INTERVAL_MINUTES = 1440)
)
select 
    MARKET_TYPE,
    count(*) as TOTAL,
    count_if(LAG1 > 0) as HAS_1_POSITIVE_LAG,
    count_if(LAG1 > 0 and LAG2 > 0) as HAS_2_POSITIVE_LAGS,
    count_if(LAG1 > 0 and LAG2 > 0 and LAG3 > 0) as HAS_3_POSITIVE_LAGS,
    count_if(RETURN_SIMPLE > 0 and LAG1 > 0 and LAG2 > 0) as MOMENTUM_CANDIDATES
from at_latest
group by MARKET_TYPE
order by MARKET_TYPE;

-- 16. Find actual momentum candidates at latest TS
-- These would be the candidates before threshold filtering
select 
    SYMBOL,
    MARKET_TYPE,
    TS,
    RETURN_SIMPLE,
    CLOSE,
    VOLUME
from MIP.MART.MARKET_RETURNS
where TS = (select max(TS) from MIP.MART.MARKET_BARS where INTERVAL_MINUTES = 1440)
  and INTERVAL_MINUTES = 1440
  and RETURN_SIMPLE >= 0.002  -- 0.2% threshold
  and VOLUME >= 1000
order by RETURN_SIMPLE desc
limit 20;

-- 17. Check APP_CONFIG thresholds
select CONFIG_KEY, CONFIG_VALUE
from MIP.APP.APP_CONFIG
where CONFIG_KEY in ('MIN_VOLUME', 'VOL_ADJ_THRESHOLD', 'PATTERN_MIN_TRADES')
order by CONFIG_KEY;

-- Option 1: Lower the minimum trades threshold
update MIP.APP.APP_CONFIG 
set CONFIG_VALUE = '10' 
where CONFIG_KEY = 'PATTERN_MIN_TRADES';

-- Option 2: Reset trade counts so patterns are treated as "new"
update MIP.APP.PATTERN_DEFINITION
set LAST_TRADE_COUNT = 0
where PATTERN_ID in (2, 3);  -- STOCK_MOMENTUM_FAST and STOCK_MOMENTUM_SLOW

-- Then re-run the pipeline
call MIP.APP.SP_RUN_DAILY_PIPELINE();

-- Check the pipeline completed without errors
SELECT EVENT_TS, EVENT_NAME, STATUS, ROWS_AFFECTED, DETAILS
FROM MIP.APP.MIP_AUDIT_LOG
WHERE EVENT_TS > DATEADD('hour', -1, CURRENT_TIMESTAMP())
ORDER BY EVENT_TS DESC
LIMIT 20;

-- Check rejection reasons
SELECT 
    SYMBOL, 
    PROPOSED_AT,
    STATUS,
    VALIDATION_ERRORS,
    SIDE,
    TARGET_WEIGHT
FROM MIP.AGENT_OUT.ORDER_PROPOSALS
WHERE PROPOSED_AT::date = CURRENT_DATE()
ORDER BY SYMBOL, PROPOSED_AT DESC;

-- Check if risk gate is blocking
SELECT * FROM MIP.MART.V_PORTFOLIO_RISK_GATE;

-- Check position counts vs limits
SELECT 
    p.PORTFOLIO_ID,
    prof.MAX_POSITIONS,
    (SELECT COUNT(*) FROM MIP.MART.V_PORTFOLIO_OPEN_POSITIONS_CANONICAL 
     WHERE PORTFOLIO_ID = p.PORTFOLIO_ID) as open_positions
FROM MIP.APP.PORTFOLIO p
LEFT JOIN MIP.APP.PORTFOLIO_PROFILE prof ON prof.PROFILE_ID = p.PROFILE_ID;

-- Clean up duplicate rejected proposals from today
DELETE FROM MIP.AGENT_OUT.ORDER_PROPOSALS
WHERE PROPOSED_AT::date = CURRENT_DATE()
  AND STATUS = 'REJECTED';

-- Verify cleanup
SELECT COUNT(*) FROM MIP.AGENT_OUT.ORDER_PROPOSALS WHERE PROPOSED_AT::date = CURRENT_DATE();

-- Check trades vs positions
SELECT 
    t.PORTFOLIO_ID,
    t.SYMBOL,
    t.SIDE,
    t.TRADE_TS,
    t.QUANTITY,
    t.NOTIONAL
FROM MIP.APP.PORTFOLIO_TRADES t
WHERE t.PORTFOLIO_ID = 1
ORDER BY t.TRADE_TS DESC
LIMIT 10;

-- Check positions (including closed ones)
SELECT 
    p.PORTFOLIO_ID,
    p.SYMBOL,
    p.ENTRY_TS,
    p.QUANTITY,
    p.ENTRY_INDEX,
    p.HOLD_UNTIL_INDEX,
    b.CURRENT_BAR_INDEX,
    (p.HOLD_UNTIL_INDEX >= b.CURRENT_BAR_INDEX) as IS_STILL_OPEN
FROM MIP.APP.PORTFOLIO_POSITIONS p
CROSS JOIN (SELECT max(BAR_INDEX) as CURRENT_BAR_INDEX FROM MIP.MART.V_BAR_INDEX WHERE INTERVAL_MINUTES = 1440) b
WHERE p.PORTFOLIO_ID = 1
ORDER BY p.ENTRY_TS DESC
LIMIT 10;