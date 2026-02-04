# MIP Code Review Findings - Autonomy Safety & Governance

**Review Date:** 2026-01-27  
**Reviewer:** AI Assistant  
**Scope:** Run scoping, entry gate enforcement, idempotency, and autonomy safety

## Executive Summary

This review identified **0 critical issues**, **1 high-priority issue**, and **3 medium-priority improvements** related to autonomy safety and governance. The prior critical gap (entry gate enforcement in execution) and the proposal count scoping issue are now resolved. The most significant remaining item is run ID type consistency across pipeline orchestration and agent proposal workflows.

## Resolved Since Last Review

### ✅ CRIT-001: Entry Gate Check in Execution Procedure (Resolved)

**File:** `MIP/SQL/app/189_sp_validate_and_execute_proposals.sql`  
**Resolution:** The execution procedure now queries `MIP.MART.V_PORTFOLIO_RISK_STATE` and blocks BUY-side proposals when `ENTRIES_BLOCKED=true`, writing `ENTRY_GATE_BLOCKED` into `VALIDATION_ERRORS` and recording an audit log entry.

**Resolution Location:**
- `189_sp_validate_and_execute_proposals.sql` (entry gate check + BUY rejection block)

---

### ✅ HIGH-002: Proposal Count Query Uses Wrong Run ID (Resolved)

**File:** `MIP/SQL/app/145_sp_run_daily_pipeline.sql`  
**Resolution:** Proposal counts now use robust scoping by matching `RUN_ID` **or** `SIGNAL_RUN_ID`, reducing mismatches between numeric signal run IDs and string pipeline run IDs.

**Resolution Location:**
- `145_sp_run_daily_pipeline.sql` (proposal count query under the `v_signal_run_id` block)

---

## High Priority Issues

### HIGH-001: Run ID Type Inconsistency (Still Open)

**Files:**
- `MIP/SQL/app/145_sp_run_daily_pipeline.sql` (pipeline run IDs are `string`)
- `MIP/SQL/app/188_sp_agent_propose_trades.sql` (proposal run IDs are `number`)
- `MIP/SQL/app/189_sp_validate_and_execute_proposals.sql` (execution run IDs are `number`)
- `MIP/SQL/app/187_agent_out_order_proposals.sql` (stores both `RUN_ID` and `SIGNAL_RUN_ID`)

**Severity:** HIGH  
**Impact:** Run scoping complexity, fragile matching logic, and higher risk of inconsistent joins/merges.

**Current State:**
- The pipeline uses a UUID string (`v_run_id`) while signal/proposal paths use numeric run IDs.
- The proposal table now includes `SIGNAL_RUN_ID` to help reconcile string vs. numeric values, and pipeline counting logic uses both identifiers.
- The underlying type mismatch remains, requiring duplicated filtering logic throughout the workflow.

**Recommendation:**
- Standardize on a single run ID representation (preferably string) across pipeline, signal eligibility, proposals, and executions.
- Remove `try_to_number(replace(...))` workarounds once run ID normalization is complete.
- Update views/procedures to emit a canonical run ID mapping for consistent downstream joins.

---

## Medium Priority Issues

### MED-001: MERGE Key Validation

**Files:**
- `MIP/SQL/app/188_sp_agent_propose_trades.sql` (line 294)
- `MIP/SQL/app/189_sp_validate_and_execute_proposals.sql` (line 152)

**Severity:** MEDIUM  
**Impact:** Potential duplicate proposals/trades if run ID matching fails

**Issue:**
MERGE statements use composite keys that depend on run ID matching. The keys look correct, but run ID type inconsistency (HIGH-001) can still cause MERGE misses and duplication risk.

**Recommendation:**
- Fix run ID type consistency first (HIGH-001)
- Add explicit validation that MERGE matched correctly
- Consider unique constraints on key combinations

---

### MED-002: Entry Gate View Consistency

**File:** `MIP/SQL/views/mart/v_portfolio_risk_state.sql`  
**Severity:** MEDIUM  
**Impact:** Potential inconsistency between simulation and risk state view

**Issue:**
`V_PORTFOLIO_RISK_STATE` determines `ENTRIES_BLOCKED` based on recent KPIs/events. `SP_RUN_PORTFOLIO_SIMULATION` also computes a local entry gate flag during simulation. These should remain aligned to avoid conflicting logic.

**Recommendation:**
- Ensure view logic matches simulation logic
- Consider making the view the single source of truth

---

### MED-003: Morning Brief Consistency Validation Missing

**File:** `MIP/SQL/app/186_sp_write_morning_brief.sql`  
**Severity:** MEDIUM  
**Impact:** Briefs may not reflect actual executions

**Issue:**
`SP_WRITE_MORNING_BRIEF` writes briefs but does not validate that proposal counts, execution counts, and risk status match actual tables/views.

**Recommendation:**
- Add validation before writing brief
- Compare brief counts to actual table counts
- Log warnings if inconsistencies are found

---

## Positive Findings

### ✅ Entry Gate Enforcement in Proposal + Execution Procedures
Both `SP_AGENT_PROPOSE_TRADES` and `SP_VALIDATE_AND_EXECUTE_PROPOSALS` consult `V_PORTFOLIO_RISK_STATE` and prevent BUY-side proposals when entries are blocked, enforcing the exits-only mode.

### ✅ Improved Proposal Count Scoping
`SP_RUN_DAILY_PIPELINE` now counts proposals by matching `RUN_ID` and `SIGNAL_RUN_ID`, reducing miscounts caused by run ID type differences.

### ✅ Audit Logging
All major procedures log to `MIP_AUDIT_LOG` with appropriate details, enabling traceability.

---

## Recommendations Summary

### Immediate Actions
1. **HIGH-001:** Standardize run ID types across pipeline, signals, proposals, and executions.

### Short-term Improvements
2. **MED-001:** Add MERGE validation and/or unique constraints after run ID normalization.
3. **MED-002:** Align `V_PORTFOLIO_RISK_STATE` with simulation entry gate logic.
4. **MED-003:** Add morning brief consistency validation.

### Long-term Enhancements
- Create `SP_ENFORCE_RUN_SCOPING` helper procedure
- Create canonical open positions view
- Expand `SP_MONITOR_AUTONOMY_SAFETY` checks for ongoing monitoring

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
  - `v_portfolio_risk_state.sql`
  - `v_signals_eligible_today.sql`
