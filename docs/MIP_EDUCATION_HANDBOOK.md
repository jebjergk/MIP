# MIP Education Handbook

A beginner-friendly guide to how MIP works, written for people with no prior knowledge of the platform's internals. Uses plain language, analogies, and visual diagrams throughout.

---

## Contents

1. What Is MIP?
2. How MIP Learns (The Learning Loop)
3. The Daily Pipeline, Step by Step
4. Core Concepts Explained
5. The Trust Journey
6. Your Screens: Page-by-Page Tour
7. Live Execution: From Idea to Action
8. What's New: Recent Platform Additions
9. Worked Scenarios
10. Quick Reference & Troubleshooting

---

## Chapter 1 — What Is MIP?

MIP stands for **Market Intelligence Platform**. It is a system that watches financial markets every day, detects repeating patterns, measures whether those patterns led to good outcomes in the past, and uses that evidence to suggest possible trades.

> **Analogy — The Weather Station:**
> Imagine a weather station that does not just tell you today's weather. Instead, it keeps a log of every weather pattern it has ever seen and records what happened next. After thousands of observations it can say: "When we saw pattern X, it rained within 3 days 72% of the time." MIP does exactly this, but with market prices instead of rain.

### What MIP is NOT

- It is **not** a crystal ball. It does not predict exact prices.
- It is **not** a "black box." Every decision can be traced back to evidence.
- It is **not** fully automatic. Safety checks and human approval gates exist at multiple stages.

> **Key principle:** MIP trusts evidence over intuition. A pattern must prove itself statistically before it can influence any decision.

---

## Chapter 2 — How MIP Learns (The Learning Loop)

Everything in MIP follows one continuous cycle:

`Observe → Measure → Judge → Decide → Act → Review → Observe…`

> **Analogy — The Football Scout:**
> A scout watches matches (observes), logs player behavior (measures), decides who is reliable (judges), recommends plays (decides), the team executes (acts), and after the game the scout reviews what worked (review). Then the cycle starts again with the next match.

### The Loop in MIP Terms

`Market Data → Signals → Outcomes → Training → Trust → Proposals → Decisions → Execution`

> **Key insight:** Signals are observations, not orders. A signal only says "I noticed something." Many safety layers sit between a signal and an actual trade.

---

## Chapter 3 — The Daily Pipeline, Step by Step

Every day, an automated pipeline runs through 9 stages:

| # | What happens | Verify in |
|---|---|---|
| 1 | Ingest market data | Runs |
| 2 | Detect signals (pattern scanners fire) | Market Timeline |
| 3 | Evaluate past signals (did they work?) | Training Status |
| 4 | Update learning metrics (maturity, hit rate) | Training Status |
| 5 | Recompute trust eligibility | Training Status |
| 6 | Generate proposals (trusted signals only) | AI Agent Decisions |
| 7 | Committee review (verdict + reasons) | AI Agent Decisions |
| 8 | Risk gates + execution | Live Portfolio Activity |
| 9 | Publish digest and narratives | Cockpit |

> **Analogy — The Airport:**
> Think of each stage like an airport departure sequence. Data arrives at the terminal (1-2). Security checks credentials (3-5). The gate agent reviews the boarding list (6-7). Final safety checks happen at the jet bridge (8). Once cleared, the flight departs and the status board updates (9).

> **Weekends & Holidays:** If no new market data arrives, the pipeline still runs but skips signal generation. Training evaluation and the AI digest still happen, so you will always see a fresh narrative in Cockpit.

---

## Chapter 4 — Core Concepts Explained

### Signals

> **Analogy:** A signal is like a smoke detector going off. It says "something is happening" — not "the house is burning down." It is an observation that needs investigation, not an automatic action.

MIP has multiple **patterns** (named strategies with specific rules). Each pattern scans market data and fires a signal when its conditions are met.

### Outcomes & Horizons

After a signal fires, MIP waits and measures what actually happened. It checks at multiple time windows called **horizons**:

`H1 (1 day) → H3 (3 days) → H5 (5 days) → H10 (10 days) → H20 (20 days)`

> **Analogy:** If you plant a seed (signal), you check it after 1 day, 3 days, 5 days, etc. to see if it sprouted. MIP does this with every signal it detects.

### Maturity & Coverage

**Maturity** (0-100) measures how complete and reliable the evidence is. **Coverage** measures what fraction of signals have been fully evaluated.

> **Think of it this way:** Maturity is your confidence in a restaurant review. 3 reviews = low confidence. 300 reviews = high confidence. Coverage is whether all the reviews have been read, or some are still pending.

### Hit Rate & Average Return

- **Hit Rate** = what percentage of outcomes were favorable.
- **Avg Return** = the average gain/loss across all evaluated outcomes.

> **Analogy:** A basketball player's shooting percentage (hit rate) and average points per shot (avg return). You want both to be good.

### Proposals & Decisions

A **proposal** is a suggested trade that passed initial quality filters. A **decision** is the committee's verdict on that proposal — with structured reason codes explaining why it was approved, rejected, or flagged.

### Risk Gates

- **SAFE**: entries allowed (green light)
- **CAUTION**: warning zone (yellow light)
- **STOPPED**: exits only, no new entries (red light)

> **Analogy:** Traffic lights. Green = go ahead. Yellow = proceed carefully. Red = no new entries allowed, only exits.

---

## Chapter 5 — The Trust Journey

Before a pattern can influence any real decision, it must earn trust through evidence:

### Maturity Stages

| Stage | Score | What it means |
|---|---|---|
| INSUFFICIENT | 0 – 24 | Too little data to draw conclusions |
| WARMING UP | 25 – 49 | Building sample, early signals |
| LEARNING | 50 – 74 | Meaningful evidence, still improving |
| CONFIDENT | 75 – 100 | Strong historical support |

### Trust Gates (all three must pass)

| Gate | Threshold |
|---|---|
| Sample Size | ≥ 40 signals |
| Hit Rate | ≥ 55% |
| Avg Return | ≥ 0.05% |

> **Analogy — Pilot Certification:**
> A pilot cannot fly passengers until they have enough flight hours (sample size), a high enough pass rate on checks (hit rate), and consistent safe landings (avg return). Only after passing all three gates do they earn their certification (TRUSTED).

> **Important:** High maturity does not guarantee future success. It means the historical evidence is statistically solid. Markets change, and MIP continuously re-evaluates.

---

## Chapter 6 — Your Screens: Page-by-Page Tour

### Dashboard
- **Home** (`/home`): Launchpad — freshness check, quick links.
- **Cockpit** (`/cockpit`): Daily command center — AI digests, attention routing.
- **Performance** (`/performance-dashboard`): Return/drawdown trend comparison.

### Portfolio
- **Live Portfolio Link** (`/live-portfolio-config`): Broker connection and guardrails.
- **Live Portfolio Activity** (`/live-portfolio-activity`): Lifecycle transitions and block reasons.
- **Live Symbol Tracker** (`/symbol-tracker`): Symbol-level monitoring and thesis status.

### Research
- **Market Timeline** (`/market-timeline`): Signal-to-trade event chain by symbol.
- **News Intelligence** (`/news-intelligence`): Evidence-backed news context with policy influence.
- **Training Status** (`/training`): Evidence depth, maturity, coverage, trust.
- **Parallel Worlds** (`/parallel-worlds`): Read-only what-if counterfactual analysis.

### Decision Executions
- **AI Agent Decisions** (`/decision-console`): Verdicts, reason codes, revalidation outcomes.
- **Learning Ledger** (`/learning-ledger`): Decision-to-outcome causality and motif review.

### Operations
- **Runs** (`/runs`): Pipeline health, step diagnostics, status interpretation.
- **Debug** (`/debug`): Technical health checks and connectivity diagnostics.

---

## Chapter 7 — Live Execution: From Idea to Action

When MIP moves from research to execution, a proposal goes through multiple checkpoints:

`Signal → Evidence → Proposal → Committee → Revalidation → Compliance → Execution → Learning Trace`

> **Analogy — Hospital Medication Order:**
> A doctor identifies a condition (signal). Lab results confirm diagnosis (evidence). The doctor writes a prescription (proposal). The pharmacy reviews for interactions (committee). A nurse verifies patient ID and allergies at bedside (revalidation). Then the medication is administered (execution). The outcome is recorded in the patient chart (learning trace).

### Connection Chain

`Source Portfolio → MIP Live Portfolio → IBKR Account`

`MIP Live Portfolio → Activation Guard → Execution Readiness`

> **Critical understanding:** Saving a configuration does NOT mean execution is ready. Multiple independent gates must all pass before any action can proceed.

---

## Chapter 8 — What's New: Recent Platform Additions

### News Intelligence
News now acts as policy-aware context. It can boost or reduce confidence scores and block entries under specific conditions — but it can **never** bypass core trust/risk gates.

> **Analogy:** A weather advisory can delay a flight (block entry), but it cannot overrule a fundamental aircraft safety check.

### Learning Ledger
Decisions are now traced to outcomes over time. You can identify recurring patterns: which evidence features led to good outcomes, and which ones consistently failed.

> **Analogy:** A coach's game-film review that connects pre-game strategy decisions to in-game results.

### Structured Reason Codes
Every decision now carries machine-readable reason codes and a revalidation outcome. Clusters of the same reason code usually point to a systemic policy threshold, not random failure.

### Lifecycle Status Interpretation
`APPROVED` no longer means `EXECUTED`. The live-linked workflow has explicit stages: validation, compliance, intent, and execution — each of which can independently block progression.

---

## Chapter 9 — Worked Scenarios

### Scenario A: "No trades happened today"

1. Go to **Runs** — did the pipeline complete successfully?
2. Go to **Training Status** — do active patterns have enough maturity and trust?
3. Go to **AI Agent Decisions** — were proposals generated but rejected? What reason codes appear?
4. Go to **Live Portfolio Activity** — was progression blocked at a downstream gate?

### Scenario B: "Action was approved but never executed"

1. In **AI Agent Decisions**, find the row and check its latest status and reason codes.
2. In **Live Portfolio Activity**, find the matching lifecycle entry. Is it stuck at validation or compliance?
3. In **Runs**, confirm the run completed without errors in the execution window.
4. In **Live Portfolio Link**, check if the Activation Guard shows red on any prerequisite.

### Scenario C: "News looks important but nothing happened"

1. In **News Intelligence**, check the Decision Impact panel. Are there rows with actual evidence fields?
2. In **AI Agent Decisions**, check reason codes. Was the proposal blocked by a trust or risk gate?
3. Remember: news context can influence ranking but **cannot bypass** core safety gates.

### Scenario D: "A metric looks wrong"

1. Confirm the episode scope in Portfolio (metrics reset when episodes change).
2. Compare run timestamps with the date shown in the metric.
3. Check whether a recent profile change or cash event shifted the baseline.

---

## Chapter 10 — Quick Reference & Troubleshooting

### 5-Minute Morning Routine

`Home → Cockpit → Performance → Training → Decisions → Runs (if needed)`

### Troubleshooting Matrix

| Symptom | What is likely happening | First pages to check |
|---|---|---|
| Data seems stale | Pipeline has not run or is delayed | Home + Runs |
| No proposals generated | Weak evidence or low trust | Training Status + Market Timeline |
| Proposals exist but no trades | Gate, limits, or committee rejection | AI Agent Decisions + Live Portfolio Activity |
| Approved but not executed | Downstream validation/revalidation block | AI Agent Decisions + Live Portfolio Activity + Runs |
| News seems relevant but no effect | News context did not overcome trust/risk gates | News Intelligence + AI Agent Decisions |
| Page looks outdated | Latest run not reflected yet | Runs + Home |

### Ask MIP — Getting Good Answers

MIP has a built-in AI assistant called **Ask MIP**. For the best answers:

- Tell it which page you are looking at.
- Include the symbol, portfolio, and time window.
- When troubleshooting, paste the status or reason code text.
- Ask one question at a time.

---

> **Final thought:** MIP is a learning system, not a certainty machine. Trust the evidence, verify with the right page, and follow the pipeline sequence when something looks off.

---

*Last updated: March 20, 2026*
