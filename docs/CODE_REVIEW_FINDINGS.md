# MIP Code Review Findings - Autonomy Safety & Governance

**Review Date:** 2026-01-25  
**Reviewer:** AI Assistant  
**Scope:** Run scoping, entry gate enforcement, idempotency, and autonomy safety

## Executive Summary

This review identified **1 critical gap**, **2 high-priority issues**, and **3 medium-priority improvements** related to autonomy safety and governance. The most critical finding is that `SP_VALIDATE_AND_EXECUTE_PROPOSALS` does not check the entry gate before executing trades, which could allow BUY trades to execute when `entries_blocked=true`.

## Critical Issues (Must Fix)

### CRIT-001: Missing Entry Gate Check in Execution Procedure

**File:** `MIP/SQL/app/189_sp_validate_and_execute_proposals.sql`  
**Severity:** CRITICAL  
**Impact:** Autonomy Safety Violation

**Issue:**
`SP_VALIDATE_AND_EXECUTE_PROPOSALS` does not check `V_PORTFOLIO_RISK_GATE.ENTRIES_BLOCKED` before executing trades. This means BUY trades could execute even when the portfolio is in drawdown stop mode.

**Current Behavior:**
- Procedure validates proposals against signal eligibility, position limits, etc.
- Does NOT check if entry gate is active
- Executes all approved proposals regardless of portfolio risk state

**Expected Behavior:**
- Check `V_PORTFOLIO_RISK_GATE` at procedure start
- When `entries_blocked=true`, reject all BUY-side proposals immediately
- Allow SELL-side proposals to proceed (exits-only mode)
- Log rejection reason as `ENTRY_GATE_BLOCKED`

**Recommendation:**
Add entry gate check similar to `SP_AGENT_PROPOSE_TRADES` (lines 96-143). See Phase 3 implementation plan.

**Code Location:**
- Missing check: `189_sp_validate_and_execute_proposals.sql` (should be added after line 50)
- Reference implementation: `188_sp_agent_propose_trades.sql` lines 96-143

---

## High Priority Issues

### HIGH-001: Run ID Type Inconsistency

**Files:**
- `MIP/SQL/app/145_sp_run_daily_pipeline.sql` (uses `string`)
- `MIP/SQL/app/188_sp_agent_propose_trades.sql` (uses `number`)
- `MIP/SQL/app/189_sp_validate_and_execute_proposals.sql` (uses `number`)
- `MIP/SQL/app/148_sp_pipeline_write_morning_briefs.sql` (uses `string` for pipeline, `number` for signal)

**Severity:** HIGH  
**Impact:** Run scoping failures, potential data integrity issues

**Issue:**
Run IDs are inconsistently typed across procedures:
- Pipeline run ID: `string` (UUID) in `SP_RUN_DAILY_PIPELINE`
- Signal run ID: `number` (extracted from string) in proposal/validation procedures
- Signal run ID matching uses fragile logic: `RUN_ID = :v_run_id_string or try_to_number(replace(RUN_ID, 'T', '')) = :P_RUN_ID`

**Specific Problems:**
1. **Line 182 in `145_sp_run_daily_pipeline.sql`:** Queries `ORDER_PROPOSALS` with `RUN_ID = :v_signal_run_id` where `v_signal_run_id` is a number, but `ORDER_PROPOSALS.RUN_ID` is stored as number (may not match string pipeline run ID)
2. **Lines 151-152, 219-220, 264-265, 314-315 in `188_sp_agent_propose_trades.sql`:** Complex matching logic suggests type mismatch issues
3. **MERGE statement line 473:** Uses `target.RUN_ID = source.RUN_ID` which may fail if types don't match

**Recommendation:**
- Standardize on `string` for all run IDs (pipeline and signal)
- Create helper view `V_PIPELINE_RUN_ID` to normalize run ID format
- Update all procedures to use consistent run ID matching
- Remove `try_to_number(replace(...))` workarounds

**Code Locations:**
- `145_sp_run_daily_pipeline.sql:18,29,150-158,167-170,182`
- `188_sp_agent_propose_trades.sql:9,28,151-152,219-220,314-315,429,473`
- `189_sp_validate_and_execute_proposals.sql:9,75,157,190,235`
- `148_sp_pipeline_write_morning_briefs.sql:8,9,26,50-58`

---

### HIGH-002: Proposal Count Query Uses Wrong Run ID

**File:** `MIP/SQL/app/145_sp_run_daily_pipeline.sql`  
**Severity:** HIGH  
**Impact:** Incorrect proposal counts in pipeline summary

**Issue:**
Line 182 queries `ORDER_PROPOSALS` using `v_signal_run_id` (a number), but proposals are created with `P_RUN_ID` which may be the signal run ID converted to number. However, the pipeline run ID (`v_run_id` string) is not used, causing potential mismatch.

**Current Code (line 172-182):**
```sql
select
    count(*) as proposed_count,
    count_if(STATUS in ('APPROVED', 'EXECUTED')) as approved_count,
    count_if(STATUS = 'REJECTED') as rejected_count,
    count_if(STATUS = 'EXECUTED') as executed_count
  into :v_proposed_count,
       :v_approved_count,
       :v_rejected_count,
       :v_executed_count
  from MIP.AGENT_OUT.ORDER_PROPOSALS
 where RUN_ID = :v_signal_run_id;
```

**Problem:**
- `v_signal_run_id` is extracted from `V_SIGNALS_ELIGIBLE_TODAY.RUN_ID` (string) and converted to number
- But `ORDER_PROPOSALS.RUN_ID` is set to `P_RUN_ID` (number) in `SP_AGENT_PROPOSE_TRADES`
- This should work, but the type conversion is fragile
- Should also filter by pipeline run ID or use a join to ensure correct scoping

**Recommendation:**
- Query should use both pipeline run ID and signal run ID for proper scoping
- Or create a view that links proposals to pipeline runs
- Ensure type consistency

**Code Location:**
- `145_sp_run_daily_pipeline.sql:172-182`

---

## Medium Priority Issues

### MED-001: MERGE Key Validation

**Files:**
- `MIP/SQL/app/188_sp_agent_propose_trades.sql` (line 294)
- `MIP/SQL/app/189_sp_validate_and_execute_proposals.sql` (line 152)

**Severity:** MEDIUM  
**Impact:** Potential duplicate proposals/trades if run ID matching fails

**Issue:**
MERGE statements use composite keys that depend on run ID matching:
- `SP_AGENT_PROPOSE_TRADES`: `(RUN_ID, PORTFOLIO_ID, RECOMMENDATION_ID)` - should prevent duplicates ✅
- `SP_VALIDATE_AND_EXECUTE_PROPOSALS`: `(PORTFOLIO_ID, PROPOSAL_ID)` - should prevent duplicate trades ✅

**Concern:**
If run ID type mismatch causes MERGE to not match existing rows, it could create duplicates. The keys look correct, but the run ID type inconsistency (HIGH-001) could cause MERGE failures.

**Recommendation:**
- Fix run ID type consistency first (HIGH-001)
- Add explicit validation that MERGE matched correctly
- Consider adding unique constraints on these key combinations

**Code Locations:**
- `188_sp_agent_propose_trades.sql:294,472-475`
- `189_sp_validate_and_execute_proposals.sql:152,194-195`

---

### MED-002: Entry Gate View Consistency

**File:** `MIP/SQL/views/mart/v_portfolio_risk_gate.sql`  
**Severity:** MEDIUM  
**Impact:** Potential inconsistency between simulation and view

**Issue:**
`V_PORTFOLIO_RISK_GATE` determines `ENTRIES_BLOCKED` based on:
- Latest portfolio run KPIs
- Drawdown stop timestamp
- Open positions

However, `SP_RUN_PORTFOLIO_SIMULATION` sets `v_entries_blocked` flag during simulation (lines 343-359), but this may not always align with the view's logic.

**Recommendation:**
- Ensure view logic exactly matches simulation logic
- Add validation that view and simulation agree on entry gate state
- Consider making the view the single source of truth

**Code Locations:**
- `v_portfolio_risk_gate.sql:70-77`
- `180_sp_run_portfolio_simulation.sql:343-359`

---

### MED-003: Morning Brief Consistency Validation Missing

**File:** `MIP/SQL/app/186_sp_write_morning_brief.sql`  
**Severity:** MEDIUM  
**Impact:** Briefs may not reflect actual executions

**Issue:**
`SP_WRITE_MORNING_BRIEF` writes briefs but doesn't validate that:
- Proposal counts match actual proposals
- Execution counts match actual trades
- Risk status matches `V_PORTFOLIO_RISK_GATE`

**Recommendation:**
- Add validation before writing brief
- Compare brief counts to actual table counts
- Log warnings if inconsistencies found
- Consider making brief generation fail if counts don't match

**Code Location:**
- `186_sp_write_morning_brief.sql` (entire procedure)

---

## Positive Findings

### ✅ Entry Gate Check in Proposal Procedure
`SP_AGENT_PROPOSE_TRADES` correctly checks `V_PORTFOLIO_RISK_GATE` and returns early if `entries_blocked=true` (lines 96-143). This is the correct pattern that should be replicated in `SP_VALIDATE_AND_EXECUTE_PROPOSALS`.

### ✅ Idempotent MERGE Logic
Both proposal and execution procedures use appropriate MERGE keys that should prevent duplicates:
- Proposals: `(RUN_ID, PORTFOLIO_ID, RECOMMENDATION_ID)`
- Trades: `(PORTFOLIO_ID, PROPOSAL_ID)`

### ✅ Audit Logging
All major procedures log to `MIP_AUDIT_LOG` with appropriate details, enabling traceability.

---

## Recommendations Summary

### Immediate Actions (Phase 3)
1. **CRIT-001:** Add entry gate check to `SP_VALIDATE_AND_EXECUTE_PROPOSALS`
2. **HIGH-001:** Standardize run ID types across all procedures
3. **HIGH-002:** Fix proposal count query to use correct run ID scoping

### Short-term Improvements
4. **MED-001:** Add MERGE validation and unique constraints
5. **MED-002:** Ensure entry gate view matches simulation logic
6. **MED-003:** Add morning brief consistency validation

### Long-term Enhancements
- Create `SP_ENFORCE_RUN_SCOPING` helper procedure
- Create canonical open positions view
- Create `SP_MONITOR_AUTONOMY_SAFETY` for ongoing monitoring

---

## Testing Recommendations

After implementing fixes, run validation checks:
1. `run_scoping_validation.sql` - Verify run ID consistency
2. `entry_gate_consistency.sql` - Verify entry gate enforcement
3. `morning_brief_consistency.sql` - Verify brief accuracy
4. `integrity_checks.sql` - Comprehensive integrity validation

Use `SP_RUN_INTEGRITY_CHECKS` to automate validation.

---

## Related Files

- Validation scripts: `MIP/SQL/checks/`
- Procedures reviewed:
  - `145_sp_run_daily_pipeline.sql`
  - `148_sp_pipeline_write_morning_briefs.sql`
  - `188_sp_agent_propose_trades.sql`
  - `189_sp_validate_and_execute_proposals.sql`
  - `180_sp_run_portfolio_simulation.sql`
- Views reviewed:
  - `v_portfolio_risk_gate.sql`
  - `v_signals_eligible_today.sql`
