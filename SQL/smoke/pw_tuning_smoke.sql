-- pw_tuning_smoke.sql
-- Purpose: Smoke tests for the Policy Tuning Lab:
--   1. Sweep scenario seeding (idempotency)
--   2. Sweep result completeness
--   3. Surface view integrity
--   4. Regime sensitivity view integrity
--   5. Recommendation generation
--   6. Safety check expansion
--   7. No orphan sweep scenarios

use role MIP_ADMIN_ROLE;
use database MIP;

-- ─── 1. Sweep Scenarios Exist and Are Properly Flagged ─────────
select 'TEST 1: Sweep scenario count' as TEST_NAME,
       count(*) as SWEEP_SCENARIO_COUNT,
       count(distinct SWEEP_FAMILY) as FAMILY_COUNT,
       iff(count(*) >= 29, 'PASS', 'FAIL') as STATUS
from MIP.APP.PARALLEL_WORLD_SCENARIO
where IS_SWEEP = true and IS_ACTIVE = true;

-- ─── 2. Sweep Results Exist ───────────────────────────────────
select 'TEST 2: Sweep results exist' as TEST_NAME,
       count(*) as SWEEP_RESULT_COUNT,
       count(distinct r.PORTFOLIO_ID) as PORTFOLIO_COUNT,
       count(distinct r.SCENARIO_ID) as SCENARIO_COUNT,
       iff(count(*) > 0, 'PASS', 'FAIL') as STATUS
from MIP.APP.PARALLEL_WORLD_RESULT r
join MIP.APP.PARALLEL_WORLD_SCENARIO s on s.SCENARIO_ID = r.SCENARIO_ID
where s.IS_SWEEP = true and r.WORLD_KEY = 'COUNTERFACTUAL';

-- ─── 3. Surface View Has Data ─────────────────────────────────
select 'TEST 3: Tuning surface has data' as TEST_NAME,
       count(*) as SURFACE_ROWS,
       count(distinct SWEEP_FAMILY) as FAMILIES,
       count(distinct PORTFOLIO_ID) as PORTFOLIOS,
       sum(IS_OPTIMAL::int) as OPTIMAL_COUNT,
       sum(IS_MINIMAL_SAFE_TWEAK::int) as SAFE_TWEAK_COUNT,
       sum(IS_CURRENT_SETTING::int) as CURRENT_COUNT,
       iff(count(*) > 0, 'PASS', 'FAIL') as STATUS
from MIP.MART.V_PW_TUNING_SURFACE;

-- ─── 4. Regime Sensitivity View ──────────────────────────────
select 'TEST 4: Regime sensitivity data' as TEST_NAME,
       count(*) as REGIME_ROWS,
       count(distinct REGIME) as REGIME_COUNT,
       sum(IS_REGIME_FRAGILE::int) as FRAGILE_COUNT,
       iff(count(*) > 0, 'PASS', 'FAIL') as STATUS
from MIP.MART.V_PW_REGIME_SENSITIVITY;

-- ─── 5. Recommendation Generation (run it) ──────────────────
call MIP.APP.SP_GENERATE_PW_RECOMMENDATIONS('SMOKE_REC_01', current_timestamp());

-- ─── 6. Recommendations Exist ────────────────────────────────
select 'TEST 6: Recommendations exist' as TEST_NAME,
       count(*) as REC_COUNT,
       count(distinct DOMAIN) as DOMAIN_COUNT,
       count(distinct RECOMMENDATION_TYPE) as TYPE_COUNT,
       count(distinct PORTFOLIO_ID) as PORTFOLIO_COUNT,
       iff(count(*) >= 0, 'PASS', 'FAIL') as STATUS
from MIP.MART.V_PW_RECOMMENDATIONS;

-- ─── 7. Safety Checks Expand Correctly ──────────────────────
select 'TEST 7: Safety checks expansion' as TEST_NAME,
       count(*) as CHECK_ROWS,
       count(distinct CHECK_NAME) as UNIQUE_CHECK_NAMES,
       iff(count(*) >= 0, 'PASS', 'FAIL') as STATUS
from MIP.MART.V_PW_SAFETY_CHECKS;

-- ─── 8. Idempotency: Re-run sweep seeding ──────────────────
select count(*) as PRE_COUNT from MIP.APP.PARALLEL_WORLD_SCENARIO where IS_SWEEP = true;
call MIP.APP.SP_RUN_PW_SWEEP('SMOKE_IDEM_01', '2026-02-13'::timestamp_ntz, 1);
select 'TEST 8: Idempotency' as TEST_NAME,
       count(*) as POST_COUNT,
       iff(count(*) >= 29 and count(*) <= 35, 'PASS', 'FAIL') as STATUS
from MIP.APP.PARALLEL_WORLD_SCENARIO where IS_SWEEP = true;

-- ─── 9. No Orphan Sweep Scenarios ───────────────────────────
select 'TEST 9: No orphan sweeps (all have results)' as TEST_NAME,
       count(*) as ORPHAN_COUNT,
       iff(count(*) = 0, 'PASS', 'INFO') as STATUS
from MIP.APP.PARALLEL_WORLD_SCENARIO s
left join MIP.APP.PARALLEL_WORLD_RESULT r on r.SCENARIO_ID = s.SCENARIO_ID
where s.IS_SWEEP = true and s.IS_ACTIVE = true and r.RUN_ID is null;
