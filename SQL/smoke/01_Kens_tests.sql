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
