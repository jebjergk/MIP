-- parallel_worlds_smoke.sql
-- Smoke tests for the Parallel Worlds counterfactual simulation system.
-- Tests: determinism (same inputs => same results), idempotency (re-run doesn't duplicate),
--        alignment (actual baseline matches PORTFOLIO_DAILY), and config-gate behaviour.

use role MIP_ADMIN_ROLE;
use database MIP;

-- =============================================================================
-- 0) SETUP: Verify prerequisites exist
-- =============================================================================

-- Scenarios should be seeded
select 'SCENARIO_COUNT' as check_label, count(*) as cnt
from MIP.APP.PARALLEL_WORLD_SCENARIO
where IS_ACTIVE = true;
-- Expect cnt >= 8

-- Active portfolios should exist
select 'ACTIVE_PORTFOLIOS' as check_label, count(*) as cnt
from MIP.APP.PORTFOLIO
where STATUS = 'ACTIVE';
-- Expect cnt >= 1

-- Config flag should exist
select 'CONFIG_FLAG' as check_label, CONFIG_KEY, CONFIG_VALUE
from MIP.APP.APP_CONFIG
where CONFIG_KEY = 'PARALLEL_WORLDS_ENABLED';
-- Expect 1 row with CONFIG_VALUE = 'true'


-- =============================================================================
-- 1) DETERMINISM: Run twice with same inputs, results must match exactly
-- =============================================================================

-- First run
call MIP.APP.SP_RUN_PARALLEL_WORLDS(
    'SMOKE_DETERMINISM_R1',
    (select max(TS) from MIP.APP.PORTFOLIO_DAILY),
    null,
    'DEFAULT_ACTIVE'
);

-- Second run with same parameters
call MIP.APP.SP_RUN_PARALLEL_WORLDS(
    'SMOKE_DETERMINISM_R2',
    (select max(TS) from MIP.APP.PORTFOLIO_DAILY),
    null,
    'DEFAULT_ACTIVE'
);

-- Compare numeric results between the two runs
-- Any rows returned means non-determinism (BAD)
select 'DETERMINISM_CHECK' as check_label,
       r1.SCENARIO_ID,
       r1.PORTFOLIO_ID,
       r1.PNL_SIMULATED        as R1_PNL,
       r2.PNL_SIMULATED        as R2_PNL,
       r1.RETURN_PCT_SIMULATED  as R1_RET,
       r2.RETURN_PCT_SIMULATED  as R2_RET,
       r1.TRADES_SIMULATED      as R1_TRADES,
       r2.TRADES_SIMULATED      as R2_TRADES
from MIP.APP.PARALLEL_WORLD_RESULT r1
join MIP.APP.PARALLEL_WORLD_RESULT r2
  on  r1.SCENARIO_ID   = r2.SCENARIO_ID
  and r1.PORTFOLIO_ID   = r2.PORTFOLIO_ID
  and r1.AS_OF_TS        = r2.AS_OF_TS
where r1.RUN_ID = 'SMOKE_DETERMINISM_R1'
  and r2.RUN_ID = 'SMOKE_DETERMINISM_R2'
  and (   r1.PNL_SIMULATED        != r2.PNL_SIMULATED
       or r1.RETURN_PCT_SIMULATED  != r2.RETURN_PCT_SIMULATED
       or r1.TRADES_SIMULATED      != r2.TRADES_SIMULATED
       or r1.OPEN_POSITIONS_END    != r2.OPEN_POSITIONS_END
      );
-- Expect 0 rows (deterministic)


-- =============================================================================
-- 2) IDEMPOTENCY: Re-running with same RUN_ID should not duplicate rows
-- =============================================================================

-- Count before re-run
set pre_count = (select count(*) from MIP.APP.PARALLEL_WORLD_RESULT
                 where RUN_ID = 'SMOKE_DETERMINISM_R1');

-- Re-run with same RUN_ID
call MIP.APP.SP_RUN_PARALLEL_WORLDS(
    'SMOKE_DETERMINISM_R1',
    (select max(TS) from MIP.APP.PORTFOLIO_DAILY),
    null,
    'DEFAULT_ACTIVE'
);

-- Count after re-run
set post_count = (select count(*) from MIP.APP.PARALLEL_WORLD_RESULT
                  where RUN_ID = 'SMOKE_DETERMINISM_R1');

select 'IDEMPOTENCY_CHECK' as check_label,
       $pre_count  as rows_before,
       $post_count as rows_after,
       iff($pre_count = $post_count, 'PASS', 'FAIL') as result;
-- Expect PASS (row count unchanged)


-- =============================================================================
-- 3) ALIGNMENT: Actual baseline (SCENARIO_ID=0) matches PORTFOLIO_DAILY
-- =============================================================================

-- The SCENARIO_TYPE='BASELINE' result for each portfolio should match
-- what we see in PORTFOLIO_DAILY for equity and PNL.
select 'ALIGNMENT_CHECK' as check_label,
       pw.PORTFOLIO_ID,
       pw.AS_OF_TS,
       pw.PNL_SIMULATED        as PW_PNL,
       pd.DAILY_PNL            as PD_PNL,
       pw.END_EQUITY_SIMULATED  as PW_EQUITY,
       pd.TOTAL_EQUITY         as PD_EQUITY,
       abs(pw.PNL_SIMULATED - pd.DAILY_PNL)         as PNL_DIFF,
       abs(pw.END_EQUITY_SIMULATED - pd.TOTAL_EQUITY) as EQUITY_DIFF
from MIP.APP.PARALLEL_WORLD_RESULT pw
join MIP.APP.PORTFOLIO_DAILY pd
  on pw.PORTFOLIO_ID = pd.PORTFOLIO_ID
  and pw.AS_OF_TS    = pd.TS
where pw.RUN_ID = 'SMOKE_DETERMINISM_R1'
  and pw.SCENARIO_ID = 0
  and (abs(pw.PNL_SIMULATED - pd.DAILY_PNL) > 0.01
       or abs(pw.END_EQUITY_SIMULATED - pd.TOTAL_EQUITY) > 0.01);
-- Expect 0 rows (alignment within penny)


-- =============================================================================
-- 4) DATA INTEGRITY: Every counterfactual has matching actual baseline
-- =============================================================================

select 'MISSING_BASELINE' as check_label,
       r.RUN_ID,
       r.PORTFOLIO_ID,
       r.AS_OF_TS,
       r.SCENARIO_ID
from MIP.APP.PARALLEL_WORLD_RESULT r
left join MIP.APP.PARALLEL_WORLD_RESULT b
  on  r.RUN_ID       = b.RUN_ID
  and r.PORTFOLIO_ID  = b.PORTFOLIO_ID
  and r.AS_OF_TS      = b.AS_OF_TS
  and b.SCENARIO_ID   = 0
where r.RUN_ID in ('SMOKE_DETERMINISM_R1', 'SMOKE_DETERMINISM_R2')
  and r.SCENARIO_ID  != 0
  and b.RUN_ID is null;
-- Expect 0 rows (every counterfactual has a baseline)


-- =============================================================================
-- 5) RESULT_JSON: Decision trace should be populated for non-baseline
-- =============================================================================

select 'DECISION_TRACE_CHECK' as check_label,
       SCENARIO_ID,
       PORTFOLIO_ID,
       RESULT_JSON:decision_trace is not null as has_trace,
       array_size(RESULT_JSON:decision_trace:gates) as gate_count
from MIP.APP.PARALLEL_WORLD_RESULT
where RUN_ID = 'SMOKE_DETERMINISM_R1'
  and SCENARIO_ID != 0
order by SCENARIO_ID, PORTFOLIO_ID;
-- Expect all has_trace = TRUE


-- =============================================================================
-- 6) VIEW SMOKE: Verify all mart views return data
-- =============================================================================

select 'V_PW_ACTUAL' as check_label, count(*) as cnt
from MIP.MART.V_PARALLEL_WORLD_ACTUAL;
-- Expect cnt >= 1

select 'V_PW_DIFF' as check_label, count(*) as cnt
from MIP.MART.V_PARALLEL_WORLD_DIFF
where RUN_ID = 'SMOKE_DETERMINISM_R1';
-- Expect cnt >= 1

select 'V_PW_REGRET' as check_label, count(*) as cnt
from MIP.MART.V_PARALLEL_WORLD_REGRET;
-- Expect cnt >= 0 (may be 0 if only 1 day of data)

select 'V_PW_SNAPSHOT' as check_label, count(*) as cnt
from MIP.MART.V_PARALLEL_WORLD_SNAPSHOT;
-- Expect cnt >= 1


-- =============================================================================
-- 7) NARRATIVE: Generate and verify narrative
-- =============================================================================

call MIP.APP.SP_GENERATE_PW_NARRATIVE(
    'SMOKE_DETERMINISM_R1',
    (select max(TS) from MIP.APP.PORTFOLIO_DAILY),
    null
);

-- Narrative should exist with matching source hash
select 'NARRATIVE_CHECK' as check_label,
       n.RUN_ID,
       n.PORTFOLIO_ID,
       n.SOURCE_FACTS_HASH,
       s.SOURCE_FACTS_HASH as SNAPSHOT_HASH,
       iff(n.SOURCE_FACTS_HASH = s.SOURCE_FACTS_HASH, 'PASS', 'FAIL') as hash_match,
       length(n.NARRATIVE_TEXT) as narrative_len
from MIP.AGENT_OUT.PARALLEL_WORLD_NARRATIVE n
left join MIP.AGENT_OUT.PARALLEL_WORLD_SNAPSHOT s
  on n.RUN_ID = s.RUN_ID and n.PORTFOLIO_ID = s.PORTFOLIO_ID
where n.RUN_ID = 'SMOKE_DETERMINISM_R1';
-- Expect hash_match = 'PASS' and narrative_len > 0


-- =============================================================================
-- 8) PIPELINE INTEGRATION: Config gate works
-- =============================================================================

-- Disable Parallel Worlds
update MIP.APP.APP_CONFIG
set CONFIG_VALUE = 'false', UPDATED_AT = current_timestamp()
where CONFIG_KEY = 'PARALLEL_WORLDS_ENABLED';

-- The pipeline should skip PW when disabled
-- (not running full pipeline here — just verifying the config reads correctly)
select 'CONFIG_GATE' as check_label,
       try_to_boolean(CONFIG_VALUE) as is_enabled
from MIP.APP.APP_CONFIG
where CONFIG_KEY = 'PARALLEL_WORLDS_ENABLED';
-- Expect is_enabled = FALSE

-- Re-enable for normal operations
update MIP.APP.APP_CONFIG
set CONFIG_VALUE = 'true', UPDATED_AT = current_timestamp()
where CONFIG_KEY = 'PARALLEL_WORLDS_ENABLED';


-- =============================================================================
-- 9) RUN LOG: Verify run log entries were created
-- =============================================================================

select 'RUN_LOG_CHECK' as check_label,
       RUN_ID, STATUS, SCENARIO_COUNT, RESULT_COUNT, STARTED_AT, COMPLETED_AT
from MIP.APP.PARALLEL_WORLD_RUN_LOG
where RUN_ID in ('SMOKE_DETERMINISM_R1', 'SMOKE_DETERMINISM_R2')
order by STARTED_AT desc;
-- Expect 2+ rows with STATUS = 'SUCCESS' or 'COMPLETED_WITH_ERRORS'


-- =============================================================================
-- CLEANUP: Remove smoke test data
-- =============================================================================

delete from MIP.AGENT_OUT.PARALLEL_WORLD_NARRATIVE
where RUN_ID in ('SMOKE_DETERMINISM_R1', 'SMOKE_DETERMINISM_R2');

delete from MIP.AGENT_OUT.PARALLEL_WORLD_SNAPSHOT
where RUN_ID in ('SMOKE_DETERMINISM_R1', 'SMOKE_DETERMINISM_R2');

delete from MIP.APP.PARALLEL_WORLD_RESULT
where RUN_ID in ('SMOKE_DETERMINISM_R1', 'SMOKE_DETERMINISM_R2');

delete from MIP.APP.PARALLEL_WORLD_RUN_LOG
where RUN_ID in ('SMOKE_DETERMINISM_R1', 'SMOKE_DETERMINISM_R2');

select 'CLEANUP_DONE' as check_label, 'All smoke test data removed' as status;
