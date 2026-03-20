# MIP Education Handbook

Practical handbook for understanding how MIP works and how to operate it using the current, non-deprecated UI.

---

## Table of Contents

1. What MIP is
2. How MIP learns from evidence
3. Daily operating flow
4. What's newer in MIP
5. Core concepts
6. Active UI pages and what each is for
7. Ask MIP usage guidance
8. Quick troubleshooting

---

## 1) What MIP Is

MIP (Market Intelligence Platform) is an evidence-first decision system.

It ingests market data, detects pattern-based signals, evaluates outcomes over future horizons, and uses measured evidence to inform proposals and decisions.

MIP is primarily operated as a controlled paper-trading and decision-support environment.

---

## 2) How MIP Learns From Evidence

MIP learning loop:

`Data -> Signals -> Outcome Evaluation -> Training Metrics -> Trust -> Proposals -> Decisions -> Review`

Key principles:

- Signals are observations, not automatic actions.
- Outcomes are measured at fixed future horizons.
- Trust is earned by evidence quality and consistency.
- Policy and risk gates can block actions even when signals exist.

---

## 3) Daily Operating Flow

Recommended daily sequence:

1. **Home**: confirm freshness and run status.
2. **Cockpit**: review what changed and what needs attention.
3. **Performance Dashboard**: compare return and drawdown trends.
4. **Training Status**: validate evidence behind active opportunities.
5. **AI Agent Decisions**: inspect accepts/rejects and rationale.
6. **Live Portfolio Activity**: validate expected lifecycle transitions (if live-linked workflow is active).
7. **Runs (Audit)**: verify pipeline details when something looks off.

---

## 4) What's Newer in MIP

Recent implemented behavior that operators should account for:

- **News Intelligence now acts as policy-aware context.**  
  It can influence ranking/confidence and block new entries in specific conditions, but never bypasses core risk/trust gates.
- **Live-linked operations have explicit lifecycle stages.**  
  A proposal can be approved and still be blocked later by validation, freshness, approvals, or execution controls.
- **Reason-code interpretation is central to troubleshooting.**  
  AI Agent Decisions and Live Portfolio Activity are meant to be read together when diagnosing "why not executed."
- **Learning Ledger is now a practical retrospective tool.**  
  It links decision evidence to realized outcomes and helps identify recurring policy tuning opportunities.
- **Runs statuses require nuanced interpretation.**  
  `SUCCESS_WITH_SKIPS` can be expected in no-data windows and should be validated step-by-step, not treated as a failure.

---

## 5) Core Concepts

| Term | Plain-English meaning |
|---|---|
| Signal | "I detected a setup." |
| Outcome | "What happened after that setup?" |
| Maturity | "How much evidence do we have?" |
| Coverage | "How complete is evaluated evidence?" |
| Trust | "Is this reliable enough to use?" |
| Proposal | "Suggested action pending controls." |
| Decision | "Committee verdict plus structured reasons for a proposal." |
| Risk Gate | "Are entries currently allowed?" |
| Revalidation | "Final checks right before execution intent." |
| Learning Ledger | "Why decisions changed and what happened next." |

---

## 6) Active UI Pages (Current Sidebar)

### Dashboard
- **Cockpit (`/cockpit`)**: daily command center.
- **Home (`/home`)**: status and freshness checks.
- **Performance (`/performance-dashboard`)**: cross-portfolio performance view.

### Portfolio
- **Live Portfolio Link (`/live-portfolio-config`)**: live-linked paper workflow controls.
- **Live Portfolio Activity (`/live-portfolio-activity`)**: operational activity, lifecycle status transitions, and block reasons.
- **Live Symbol Tracker (`/symbol-tracker`)**: symbol-first monitoring.

### Research
- **Market Timeline (`/market-timeline`)**: symbol event history.
- **News Intelligence (`/news-intelligence`)**: structured news context.
- **Training Status (`/training`)**: evidence/maturity/trust context.
- **Parallel Worlds (`/parallel-worlds`)**: counterfactual policy analysis.

### Decision Executions
- **AI Agent Decisions (`/decision-console`)**: decision logs, rationale, reason codes, and revalidation outcomes.
- **Learning Ledger (`/learning-ledger`)**: decision-to-outcome causality and recurring motif review.

### Operations
- **Runs (Audit) (`/runs`)**: run history, status interpretation, and step diagnostics.
- **Debug (`/debug`)**: troubleshooting surfaces.

### Reference
- **User Guide (`/guide`)**: in-app documentation.

---

## 7) Ask MIP Usage Guidance

Ask MIP should use the User Guide as primary truth for app behavior.

If a term is not explicitly defined in the guide, Ask MIP may still provide a plain-language explanation and label it clearly as best-effort context.

Best question format:

- Include page, symbol, portfolio, and time window when possible.
- Ask one concrete question at a time.
- For live values, ask where to verify in UI.
- Include status/reason-code text when asking why an action was blocked.

---

## 8) Quick Troubleshooting

| Symptom | First place to check |
|---|---|
| Data seems stale | Home + Runs |
| Evidence looks weak | Training Status + Market Timeline |
| Decisions rejected unexpectedly | AI Agent Decisions + Learning Ledger |
| Live-linked activity unclear | Live Portfolio Activity + Live Portfolio Link |
| News impact confusion | News Intelligence |
| Approved but not executed | AI Agent Decisions + Live Portfolio Activity + Runs |

---

*Last updated: March 20, 2026*
