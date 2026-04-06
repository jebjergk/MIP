-- 15_autonomous_trust_momentum_smoke.sql
-- Validates autonomous trust slice: MOMENTUM STOCK/1440 in gate; MR/bearish not in autonomous horizons;
-- V_TRUSTED_SIGNALS_LATEST_TS can include pattern 2/3 at latest TS when signals exist.

use role MIP_ADMIN_ROLE;
use database MIP;

-- 1) Gate table seeded
select 'AUTONOMOUS_TRUST_FAMILY_GATE' as chk, count(*) as n from MIP.APP.AUTONOMOUS_TRUST_FAMILY_GATE;

-- 2) Autonomous horizons include MOMENTUM STOCK 1440 (patterns 2 and/or 3 expected)
select 'V_AUTONOMOUS_horizons_MOMENTUM_STOCK_1440' as chk, PATTERN_ID, HORIZON_BARS, HIT_RATE_SUCCESS
from MIP.MART.V_AUTONOMOUS_PROPOSAL_TRUSTED_PATTERN_HORIZONS
where MARKET_TYPE = 'STOCK' and INTERVAL_MINUTES = 1440
order by PATTERN_ID, HORIZON_BARS;

-- 3) No MEAN_REVERSION / BEARISH_MOMENTUM in autonomous slice for STOCK 1440
select 'V_AUTONOMOUS_no_MR_bearish_STOCK_1440' as chk, count(*) as bad_rows
from MIP.MART.V_AUTONOMOUS_PROPOSAL_TRUSTED_PATTERN_HORIZONS tph
join MIP.APP.PATTERN_DEFINITION pd on pd.PATTERN_ID = tph.PATTERN_ID
where tph.MARKET_TYPE = 'STOCK' and tph.INTERVAL_MINUTES = 1440
  and pd.PATTERN_TYPE in ('MEAN_REVERSION', 'BEARISH_MOMENTUM');

-- 4) Latest trusted signals: pattern types present (expect MOMENTUM when market has signals)
select 'V_TRUSTED_SIGNALS_LATEST_TS_pattern_types' as chk, pd.PATTERN_TYPE, count(*) as n
from MIP.MART.V_TRUSTED_SIGNALS_LATEST_TS t
join MIP.APP.PATTERN_DEFINITION pd on pd.PATTERN_ID = t.PATTERN_ID
group by pd.PATTERN_TYPE
order by 1;

-- 5) Default proposal policy is v2
select 'PROPOSAL_POLICY_DEFAULT' as chk, POLICY_VERSION, IS_DEFAULT
from MIP.APP.PROPOSAL_POLICY_MANIFEST
where IS_DEFAULT;

-- 6) MR not autonomous-eligible under v2
select 'POLICY_MR_BULLISH_v2' as chk, IS_ELIGIBLE, NOTES
from MIP.APP.PROPOSAL_POLICY_RULE
where POLICY_VERSION = '2026_04_07_V2' and PATTERN_FAMILY = 'MEAN_REVERSION' and MR_DIRECTION = 'BULLISH';
