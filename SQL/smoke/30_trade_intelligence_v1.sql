-- 30_trade_intelligence_v1.sql
-- Smoke + validation for MIP.MART.V_TRADE_INTELLIGENCE (TIR Phase 1 / 1.5).
-- Run as MIP_ADMIN_ROLE or MIP_UI_API_ROLE.
--
-- WHEN FIRST CLOSEOUTS LAND:
--   1) Set CHANGE_ME_PORTFOLIO_ID below (session variable) OR replace :pid in section 6–8 manually.
--   2) Run entire file. Expect: §2 zero duplicate rows; §3 orphan count stable at 0; §9 tir vs eligible match.
--   3) Null-rates (§6–8): if tir_row_count_for_portfolio = 0, AVG(...) columns are NULL — expected. When count > 0, interpret rates.
--      High PW null-rate often means no V_PARALLEL_WORLD_DIFF row for that calendar date (pipeline / holiday / new portfolio).
--   4) API sanity: GET /parallel-worlds/trade-intelligence?portfolio_id=<same>&limit=500 — JSON "count" should equal §5 for that portfolio.

use role MIP_ADMIN_ROLE;
use database MIP;

-- Portfolio filter for sections 5–9 (change when validating a specific book)
set CHANGE_ME_PORTFOLIO_ID = 1;

-- 1) View resolves
select count(*) as tir_row_count from MIP.MART.V_TRADE_INTELLIGENCE;

-- 2) Uniqueness: one row per CLOSEOUT_ID (MUST return 0 rows)
select closeout_id, count(*) as n
from MIP.MART.V_TRADE_INTELLIGENCE
group by 1
having count(*) > 1;

-- 3) Orphan closeouts excluded from TIR (expect 0 in steady state)
select count(*) as orphan_closeout_count
from MIP.LIVE.TRADE_CLOSEOUT tc
left join MIP.LIVE.LIVE_ACTIONS la on la.ACTION_ID = tc.ENTRY_ACTION_ID
left join MIP.LIVE.ENTRY_INTEL_SNAPSHOT e on e.SNAPSHOT_ID = tc.SNAPSHOT_ID
where coalesce(e.PORTFOLIO_ID, la.PORTFOLIO_ID) is null;

-- 4) Eligible closeouts (same join as TIR inclusion) for selected portfolio
select count(*) as eligible_closeout_count_for_portfolio
from MIP.LIVE.TRADE_CLOSEOUT tc
left join MIP.LIVE.LIVE_ACTIONS la on la.ACTION_ID = tc.ENTRY_ACTION_ID
left join MIP.LIVE.ENTRY_INTEL_SNAPSHOT e on e.SNAPSHOT_ID = tc.SNAPSHOT_ID
where coalesce(e.PORTFOLIO_ID, la.PORTFOLIO_ID) = $CHANGE_ME_PORTFOLIO_ID;

-- 5) TIR rows for selected portfolio
select count(*) as tir_row_count_for_portfolio
from MIP.MART.V_TRADE_INTELLIGENCE
where portfolio_id = $CHANGE_ME_PORTFOLIO_ID;

-- 6) Null-rate: EIS (0 = all have EIS flag true is not this metric — rate missing expectation when has_eis false)
select
    avg(iff(not has_eis, 1.0, 0.0)) as rate_has_eis_false,
    avg(iff(has_eis and expected_return is null, 1.0, 0.0)) as rate_eis_but_null_expected_return,
    avg(iff(has_eis and expectation_summary is null, 1.0, 0.0)) as rate_eis_but_null_summary
from MIP.MART.V_TRADE_INTELLIGENCE
where portfolio_id = $CHANGE_ME_PORTFOLIO_ID;

-- 7) Null-rate: committee + PW + recon
select
    avg(iff(committee_action_raw is null and committee_action_normalized is null, 1.0, 0.0)) as rate_no_committee_fields,
    avg(iff(best_pw_scenario_name is null, 1.0, 0.0)) as rate_missing_pw,
    avg(iff(reconciliation_class is null, 1.0, 0.0)) as rate_missing_recon
from MIP.MART.V_TRADE_INTELLIGENCE
where portfolio_id = $CHANGE_ME_PORTFOLIO_ID;

-- 8) Spot-check sample rows
select *
from MIP.MART.V_TRADE_INTELLIGENCE
where portfolio_id = $CHANGE_ME_PORTFOLIO_ID
order by exit_ts desc nulls last
limit 10;

-- 9) Contract sanity: eligible closeouts for portfolio should equal TIR rows (same grain)
select
    e.eligible_cnt,
    t.tir_cnt,
    e.eligible_cnt - t.tir_cnt as delta
from (
    select count(*) as eligible_cnt
    from MIP.LIVE.TRADE_CLOSEOUT tc
    left join MIP.LIVE.LIVE_ACTIONS la on la.ACTION_ID = tc.ENTRY_ACTION_ID
    left join MIP.LIVE.ENTRY_INTEL_SNAPSHOT e on e.SNAPSHOT_ID = tc.SNAPSHOT_ID
    where coalesce(e.PORTFOLIO_ID, la.PORTFOLIO_ID) = $CHANGE_ME_PORTFOLIO_ID
) e
cross join (
    select count(*) as tir_cnt from MIP.MART.V_TRADE_INTELLIGENCE where portfolio_id = $CHANGE_ME_PORTFOLIO_ID
) t;
