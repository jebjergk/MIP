use role MIP_ADMIN_ROLE;
use database MIP;

-- =============================================================================
-- DIAGNOSTIC: Where did the $44 go? (Portfolio 1 lost money with no trades)
-- =============================================================================


ALTER GIT REPOSITORY MIP.APP.MIP FETCH;
select * from mip.app.mip_audit_log order by event_ts desc;

select * from mip.app.mip_audit_log where event_ts::date = '2026-02-01' order by event_ts desc;

call MIP.APP.SP_RUN_DAILY_PIPELINE();


select
  max(EVENT_TS) as LATEST_PIPELINE_EVENT_TS
from MIP.APP.MIP_AUDIT_LOG
where EVENT_TYPE = 'PIPELINE'
  and STATUS in ('SUCCESS', 'SUCCESS_WITH_SKIPS');

select
  PORTFOLIO_ID,
  max(TS) as LATEST_PORTFOLIO_DAILY_TS
from MIP.APP.PORTFOLIO_DAILY
where PORTFOLIO_ID in (1,2)
group by 1
order by 1;


-- =============================================================================
-- SMOKE TESTS: Portfolio page fixes verification
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Item 1: Drawdown chart episode scoping
-- Verify that episode-local peak is used, not lifetime peak
-- When episode has no trades and equity is flat, drawdown should be ~0
-- -----------------------------------------------------------------------------

-- Show episode bounds and equity at start
select 
    e.PORTFOLIO_ID,
    e.EPISODE_ID,
    e.START_TS,
    e.END_TS,
    e.STATUS,
    e.START_EQUITY,
    -- First day equity in episode
    (select min(d.TOTAL_EQUITY) 
     from MIP.APP.PORTFOLIO_DAILY d 
     where d.PORTFOLIO_ID = e.PORTFOLIO_ID 
       and d.TS >= e.START_TS 
       and (e.END_TS is null or d.TS <= e.END_TS)
    ) as FIRST_DAY_EQUITY,
    -- Peak equity within episode (should be episode-local)
    (select max(d.TOTAL_EQUITY) 
     from MIP.APP.PORTFOLIO_DAILY d 
     where d.PORTFOLIO_ID = e.PORTFOLIO_ID 
       and d.TS >= e.START_TS 
       and (e.END_TS is null or d.TS <= e.END_TS)
    ) as EPISODE_PEAK_EQUITY,
    -- Current equity (latest in episode)
    (select max_by(d.TOTAL_EQUITY, d.TS)
     from MIP.APP.PORTFOLIO_DAILY d 
     where d.PORTFOLIO_ID = e.PORTFOLIO_ID 
       and d.TS >= e.START_TS 
       and (e.END_TS is null or d.TS <= e.END_TS)
    ) as EPISODE_CURRENT_EQUITY
from MIP.APP.PORTFOLIO_EPISODE e
where e.STATUS = 'ACTIVE'
order by e.PORTFOLIO_ID;

-- Verify episode drawdown: should be (peak - current) / peak
-- If no activity and equity flat, this should be 0 or very small
select 
    e.PORTFOLIO_ID,
    e.EPISODE_ID,
    peak.PEAK_EQUITY as EPISODE_PEAK,
    current_eq.CURRENT_EQUITY,
    case 
        when peak.PEAK_EQUITY > 0 
        then round((peak.PEAK_EQUITY - current_eq.CURRENT_EQUITY) / peak.PEAK_EQUITY * 100, 2)
        else 0 
    end as EPISODE_DRAWDOWN_PCT,
    trades.TRADE_COUNT as TRADES_IN_EPISODE
from MIP.APP.PORTFOLIO_EPISODE e
cross join lateral (
    select max(d.TOTAL_EQUITY) as PEAK_EQUITY
    from MIP.APP.PORTFOLIO_DAILY d 
    where d.PORTFOLIO_ID = e.PORTFOLIO_ID 
      and d.TS >= e.START_TS 
      and (e.END_TS is null or d.TS <= e.END_TS)
) peak
cross join lateral (
    select max_by(d.TOTAL_EQUITY, d.TS) as CURRENT_EQUITY
    from MIP.APP.PORTFOLIO_DAILY d 
    where d.PORTFOLIO_ID = e.PORTFOLIO_ID 
      and d.TS >= e.START_TS 
      and (e.END_TS is null or d.TS <= e.END_TS)
) current_eq
cross join lateral (
    select count(*) as TRADE_COUNT
    from MIP.APP.PORTFOLIO_TRADES t
    where t.PORTFOLIO_ID = e.PORTFOLIO_ID
      and t.TRADE_TS >= e.START_TS
      and (e.END_TS is null or t.TRADE_TS <= e.END_TS)
) trades
where e.STATUS = 'ACTIVE'
order by e.PORTFOLIO_ID;

-- -----------------------------------------------------------------------------
-- Item 2: Risk regime / Drawdown consistency
-- The risk gate and episode drawdown should agree
-- If episode drawdown breaches threshold, risk should not say SAFE
-- -----------------------------------------------------------------------------

select 
    g.PORTFOLIO_ID,
    g.RISK_STATUS as RISK_GATE_STATUS,
    g.ENTRIES_BLOCKED,
    g.BLOCK_REASON,
    g.MAX_DRAWDOWN as RISK_GATE_MAX_DRAWDOWN,
    p.DRAWDOWN_STOP_PCT,
    -- Compare with episode-local drawdown (from Item 1 query logic)
    ep_dd.EPISODE_DRAWDOWN_PCT,
    case 
        when ep_dd.EPISODE_DRAWDOWN_PCT >= (p.DRAWDOWN_STOP_PCT * 100) then 'BREACHED'
        else 'WITHIN_LIMITS'
    end as EPISODE_DD_VS_THRESHOLD,
    -- Consistency check: should not be SAFE if episode drawdown breaches
    case 
        when g.RISK_STATUS = 'OK' 
         and ep_dd.EPISODE_DRAWDOWN_PCT >= (p.DRAWDOWN_STOP_PCT * 100)
        then 'MISMATCH - GATE SAYS OK BUT EPISODE DD BREACHED'
        else 'CONSISTENT'
    end as CONSISTENCY_CHECK
from MIP.MART.V_PORTFOLIO_RISK_GATE g
join (
    select p.PORTFOLIO_ID, coalesce(prof.DRAWDOWN_STOP_PCT, 0.10) as DRAWDOWN_STOP_PCT
    from MIP.APP.PORTFOLIO p
    left join MIP.APP.PORTFOLIO_PROFILE prof on prof.PROFILE_ID = p.PROFILE_ID
) p on p.PORTFOLIO_ID = g.PORTFOLIO_ID
left join (
    select 
        e.PORTFOLIO_ID,
        round(
            case 
                when max(d.TOTAL_EQUITY) > 0 
                then (max(d.TOTAL_EQUITY) - max_by(d.TOTAL_EQUITY, d.TS)) / max(d.TOTAL_EQUITY) * 100
                else 0 
            end, 2
        ) as EPISODE_DRAWDOWN_PCT
    from MIP.APP.PORTFOLIO_EPISODE e
    join MIP.APP.PORTFOLIO_DAILY d 
      on d.PORTFOLIO_ID = e.PORTFOLIO_ID 
     and d.TS >= e.START_TS 
     and (e.END_TS is null or d.TS <= e.END_TS)
    where e.STATUS = 'ACTIVE'
    group by e.PORTFOLIO_ID
) ep_dd on ep_dd.PORTFOLIO_ID = g.PORTFOLIO_ID
order by g.PORTFOLIO_ID;

-- -----------------------------------------------------------------------------
-- Item 3: Stale banner logic verification
-- Compare portfolio simulated-through date vs pipeline run timestamp
-- Should NOT say "newer run available" just because pipeline timestamp is newer
-- -----------------------------------------------------------------------------

select
    'Pipeline last ran at' as METRIC,
    max(EVENT_TS) as VALUE
from MIP.APP.MIP_AUDIT_LOG
where EVENT_TYPE = 'PIPELINE'
  and EVENT_NAME = 'SP_RUN_DAILY_PIPELINE'
  and STATUS in ('SUCCESS', 'SUCCESS_WITH_SKIPS')

union all

select
    'Latest available bar date' as METRIC,
    max(TS)::timestamp as VALUE
from MIP.MART.V_BAR_INDEX
where INTERVAL_MINUTES = 1440

union all

select
    'Portfolio 1 simulated through' as METRIC,
    max(TS) as VALUE
from MIP.APP.PORTFOLIO_DAILY
where PORTFOLIO_ID = 1

union all

select
    'Portfolio 2 simulated through' as METRIC,
    max(TS) as VALUE
from MIP.APP.PORTFOLIO_DAILY
where PORTFOLIO_ID = 2;

-- Staleness check: only STALE if portfolio TS < latest bar date
select
    p.PORTFOLIO_ID,
    pd.PORTFOLIO_SIMULATED_THROUGH,
    bars.LATEST_BAR_DATE,
    case 
        when pd.PORTFOLIO_SIMULATED_THROUGH::date < bars.LATEST_BAR_DATE::date
        then 'TRULY STALE - new market date not yet simulated'
        else 'CURRENT - portfolio up to date'
    end as STALENESS_STATUS
from MIP.APP.PORTFOLIO p
left join (
    select PORTFOLIO_ID, max(TS) as PORTFOLIO_SIMULATED_THROUGH
    from MIP.APP.PORTFOLIO_DAILY
    group by PORTFOLIO_ID
) pd on pd.PORTFOLIO_ID = p.PORTFOLIO_ID
cross join (
    select max(TS) as LATEST_BAR_DATE
    from MIP.MART.V_BAR_INDEX
    where INTERVAL_MINUTES = 1440
) bars
order by p.PORTFOLIO_ID;
