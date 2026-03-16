# MIP Education Handbook

Practical handbook for understanding how MIP works and how to operate it using the current, non-deprecated UI.

---

## Table of Contents

1. What MIP is
2. How MIP learns from evidence
3. Daily operating flow
4. Core concepts
5. Active UI pages and what each is for
6. Ask MIP usage guidance
7. Quick troubleshooting

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
6. **Runs (Audit)**: verify pipeline details when something looks off.

---

## 4) Core Concepts

| Term | Plain-English meaning |
|---|---|
| Signal | "I detected a setup." |
| Outcome | "What happened after that setup?" |
| Maturity | "How much evidence do we have?" |
| Coverage | "How complete is evaluated evidence?" |
| Trust | "Is this reliable enough to use?" |
| Proposal | "Suggested action pending controls." |
| Risk Gate | "Are entries currently allowed?" |
| Learning Ledger | "Why decisions changed and what happened next." |

---

## 5) Active UI Pages (Current Sidebar)

### Dashboard
- **Cockpit (`/cockpit`)**: daily command center.
- **Home (`/home`)**: status and freshness checks.
- **Performance (`/performance-dashboard`)**: cross-portfolio performance view.

### Portfolio
- **Live Portfolio Link (`/live-portfolio-config`)**: live-linked paper workflow controls.
- **Live Portfolio Activity (`/live-portfolio-activity`)**: operational activity and status transitions.
- **Live Symbol Tracker (`/symbol-tracker`)**: symbol-first monitoring.

### Research
- **Market Timeline (`/market-timeline`)**: symbol event history.
- **News Intelligence (`/news-intelligence`)**: structured news context.
- **Training Status (`/training`)**: evidence/maturity/trust context.
- **Parallel Worlds (`/parallel-worlds`)**: counterfactual policy analysis.

### Decision Executions
- **AI Agent Decisions (`/decision-console`)**: decision logs and rationale.
- **Learning Ledger (`/learning-ledger`)**: decision-to-outcome causality.

### Operations
- **Runs (Audit) (`/runs`)**: run history and diagnostics.
- **Debug (`/debug`)**: troubleshooting surfaces.

### Reference
- **User Guide (`/guide`)**: in-app documentation.

---

## 6) Ask MIP Usage Guidance

Ask MIP should use the User Guide as primary truth for app behavior.

If a term is not explicitly defined in the guide, Ask MIP may still provide a plain-language explanation and label it clearly as best-effort context.

Best question format:

- Include page, symbol, portfolio, and time window when possible.
- Ask one concrete question at a time.
- For live values, ask where to verify in UI.

---

## 7) Quick Troubleshooting

| Symptom | First place to check |
|---|---|
| Data seems stale | Home + Runs |
| Evidence looks weak | Training Status + Market Timeline |
| Decisions rejected unexpectedly | AI Agent Decisions + Learning Ledger |
| Live-linked activity unclear | Live Portfolio Activity + Live Portfolio Link |
| News impact confusion | News Intelligence |

---

*Last updated: March 16, 2026*
