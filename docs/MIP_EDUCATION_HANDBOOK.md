# MIP Education Handbook

Practical handbook for understanding MIP in plain English while keeping the full MIP learning style: analogies, examples, and decision flow illustrations.

---

## Table of Contents

1. What MIP is
2. How MIP learns from evidence
3. Illustrated pipeline map
4. What's newly implemented (and how it changes operation)
5. Daily operating playbook
6. Core concepts and interpretation
7. Page-by-page learning guide (current UI)
8. Worked examples
9. Ask MIP usage guidance
10. Quick troubleshooting matrix

---

## 1) What MIP Is

MIP (Market Intelligence Platform) is an evidence-first decision system.

It does not ask, "Can we predict exactly what price does next?"  
It asks, "When this setup happened before, what usually happened next, and how reliable is that evidence?"

### Analogy: medical triage, not fortune telling

- **Signals** are symptoms noticed by scanners.
- **Training metrics** are the medical history and confidence scores.
- **Committee decisions** are the treatment plan.
- **Risk gates** are safety checks before action.

MIP is primarily operated as a controlled paper-trading and decision-support environment, including live-linked workflow controls with explicit lifecycle governance.

---

## 2) How MIP Learns From Evidence

MIP learning loop:

`Data -> Signals -> Outcome Evaluation -> Training Metrics -> Trust -> Proposals -> Decisions -> Execution Flow -> Review`

### Key learning principles

- Signals are observations, not automatic actions.
- Outcomes are measured at fixed forward horizons.
- Trust is earned by evidence quality and consistency.
- Policy/risk gates remain authoritative even when signals are strong.
- Realized outcomes are fed back via Learning Ledger for tuning.

---

## 3) Illustrated Pipeline Map

Think of this as MIP's "airport flow" from check-in to takeoff:

1. **Data arrives** (market bars ingested)
2. **Candidates found** (signals generated)
3. **Past evidence updated** (outcomes + maturity + coverage)
4. **Eligibility determined** (trust/quality gates)
5. **Actions drafted** (proposals)
6. **Committee review** (verdict + reason codes)
7. **Operational validation** (revalidation, approvals, freshness)
8. **Execution path** (paper/live-linked workflow)
9. **Learning trace** (ledger and retrospective causality)

### Where to verify each stage in UI

| Pipeline stage | Primary page |
|---|---|
| Run freshness and step health | Runs (`/runs`) |
| Signal and event context | Market Timeline (`/market-timeline`) |
| Evidence quality/trust | Training Status (`/training`) |
| Proposal to decision verdict | AI Agent Decisions (`/decision-console`) |
| Lifecycle status transitions | Live Portfolio Activity (`/live-portfolio-activity`) |
| Policy context from news | News Intelligence (`/news-intelligence`) |
| Decision-to-outcome trace | Learning Ledger (`/learning-ledger`) |

---

## 4) What's Newly Implemented (and Why It Matters)

This section keeps the handbook current to application functionality.

### A) News Intelligence is now policy-aware context

- News can influence ranking/confidence.
- News can block new entries under configured conditions.
- News **cannot** bypass trust/risk/policy gates.

Operational impact: "HOT" is a context signal, not an auto-trade signal.

### B) Live-linked flow has explicit lifecycle stages

A proposal can be:

`APPROVED -> delayed/blocked -> revalidated -> executed (or not)`

Operational impact: "approved" no longer means "already executed."

### C) Reason codes + revalidation are first-class diagnostics

AI Agent Decisions and Live Portfolio Activity now provide structured reasons for each status outcome.

Operational impact: investigate clusters of reason codes, not isolated rows.

### D) Learning Ledger is now an active operating tool

You can tie decision logic and evidence to realized outcomes and recurring motifs.

Operational impact: weekly policy tuning can be evidence-backed instead of anecdotal.

### E) Runs status interpretation is richer

`SUCCESS_WITH_SKIPS` may be expected behavior in no-data windows.

Operational impact: step-level context determines whether you have an incident.

---

## 5) Daily Operating Playbook

### 5-minute morning loop

1. **Home (`/home`)**: confirm recency/freshness.
2. **Cockpit (`/cockpit`)**: identify what changed and where attention is needed.
3. **Performance Dashboard (`/performance-dashboard`)**: check portfolio-level impact.
4. **Training Status (`/training`)**: validate evidence quality behind active opportunities.
5. **AI Agent Decisions (`/decision-console`)**: inspect verdicts, reasons, and revalidation.
6. **Live Portfolio Activity (`/live-portfolio-activity`)**: confirm expected lifecycle transitions.
7. **Runs (`/runs`)**: verify step health when anything looks inconsistent.

### Weekly learning loop

- Use **Learning Ledger** to identify recurring motifs.
- Use **Parallel Worlds** to review counterfactual policy behavior.
- Use **News Intelligence** to validate whether context adjustments are helping or over-blocking.

---

## 6) Core Concepts and Interpretation

| Term | Practical meaning |
|---|---|
| Signal | "I detected a setup." |
| Outcome | "What happened after that setup?" |
| Maturity | "How much evidence do we have?" |
| Coverage | "How complete is measurable evidence?" |
| Trust | "Is this reliable enough to influence decisions?" |
| Proposal | Suggested action pending committee and policy checks. |
| Decision | Committee verdict plus structured reason codes. |
| Revalidation | Final checks before action intent can progress. |
| Risk Gate | Safety state controlling new-entry allowance. |
| Learning Ledger | Decision-to-outcome causality over time. |

### Interpretation guardrails

- Strong metrics do not override policy gates.
- `APPROVED` is not equivalent to `EXECUTED`.
- Always correlate decision rows with run context and activity lifecycle.

---

## 7) Page-by-Page Learning Guide (Current UI)

### Dashboard

- **Home (`/home`)**: freshness, quick health check, launchpad.
- **Cockpit (`/cockpit`)**: daily narrative and action routing.
- **Performance (`/performance-dashboard`)**: return/drawdown trend interpretation.

### Portfolio

- **Live Portfolio Link (`/live-portfolio-config`)**: control layer for live-linked safety and readiness.
- **Live Portfolio Activity (`/live-portfolio-activity`)**: lifecycle feed with delay/block reasons.
- **Live Symbol Tracker (`/symbol-tracker`)**: symbol-first operational monitoring.

### Research

- **Market Timeline (`/market-timeline`)**: event chain by symbol.
- **News Intelligence (`/news-intelligence`)**: context intelligence with guarded policy influence.
- **Training Status (`/training`)**: evidence depth and trust posture.
- **Parallel Worlds (`/parallel-worlds`)**: research-only counterfactual analysis.

### Decision Executions

- **AI Agent Decisions (`/decision-console`)**: verdicts, reason codes, revalidation outcomes.
- **Learning Ledger (`/learning-ledger`)**: decision-to-outcome accountability and motif tracking.

### Operations + Reference

- **Runs (`/runs`)**: run-status interpretation and step diagnostics.
- **Debug (`/debug`)**: targeted troubleshooting tools.
- **User Guide (`/guide`)**: route-aware in-app guide source.

---

## 8) Worked Examples

### Example A: "No trades today"

1. In **Runs**, verify latest run status and skipped-step context.
2. In **Training Status**, inspect maturity/coverage/trust for active symbols.
3. In **AI Agent Decisions**, inspect verdict and reason-code patterns.
4. In **Live Portfolio Activity**, confirm whether downstream checks blocked progression.

### Example B: "Approved but not executed"

1. Read decision row in **AI Agent Decisions**.
2. Find matching lifecycle row in **Live Portfolio Activity**.
3. Correlate to same window in **Runs**.
4. Confirm readiness fields in **Live Portfolio Link**.

### Example C: "News looked important, but action was blocked"

1. Review **News Intelligence** impact rows for actual evidence fields.
2. Confirm reason codes in **AI Agent Decisions**.
3. Verify gate/freshness state in **Live Portfolio Activity**.

---

## 9) Ask MIP Usage Guidance

Ask MIP should treat the User Guide and these handbook definitions as primary behavior references.

### Best question structure

- include page route
- include symbol and portfolio
- include time window
- include status/reason-code text when troubleshooting

### Good prompt examples

- "In `/decision-console`, portfolio 3, why was this action approved but not executed between 09:00-10:00?"
- "In `/news-intelligence`, explain why ticker X is HOT but no action progressed."
- "In `/runs`, explain whether `SUCCESS_WITH_SKIPS` at 16:05 is expected."

---

## 10) Quick Troubleshooting Matrix

| Symptom | First checks |
|---|---|
| Data seems stale | Home + Runs |
| Evidence looks weak | Training Status + Market Timeline |
| Decisions rejected unexpectedly | AI Agent Decisions + Learning Ledger |
| Approved but not executed | AI Agent Decisions + Live Portfolio Activity + Runs |
| Live-linked activity unclear | Live Portfolio Activity + Live Portfolio Link |
| News impact confusion | News Intelligence + AI Agent Decisions |

---

*Last updated: March 20, 2026*
