# MIP User Guide (Training Edition)

**Market Intelligence Platform (MIP)** in plain English, with practical examples and a logical learning sequence.

---

## How To Use This Guide

- If you are new: read sections 1 to 5 in order.
- If you operate MIP daily: jump to section 6 (daily routine).
- If something looks wrong in the UI: jump to section 9 (troubleshooting).

---

## 1) What MIP Actually Does

MIP is an evidence-first trading system.

It does **not** try to magically predict exact prices.
It asks: **"When this market pattern happened before, what usually happened next?"**

### Simple analogy

Think of MIP like a football scout:
- watches matches (market data),
- logs repeated player behavior (signals),
- checks what happened after each behavior (outcomes),
- trusts patterns only after enough evidence,
- then recommends plays (proposals/trades).

---

## 2) The Core Event Sequence (Bottom-Up)

This is the backbone of the whole app:

`Market bars -> Signals -> Outcome evaluation -> Training metrics -> Trust -> Proposals -> Execution -> Narrative`

### 2.1 Step-by-step

1. **Market bars are ingested** (OHLC data arrives).
2. **Patterns detect signals** (potential opportunities).
3. **Past signals are evaluated** at time horizons (H1/H3/H5/H10/H20 for daily).
4. **Training metrics update** (maturity, coverage, hit rate, average return).
5. **Trust/eligibility updates** (which patterns are reliable enough).
6. **Proposals are generated** (candidate trades).
7. **Risk and gate checks run** before execution.
8. **Execution and narrative outputs** are published to UI.

### 2.2 Where to verify each step in UI

| Event step | Best page to verify |
|---|---|
| Pipeline run health | Runs (Audit Viewer) |
| Signals generated | Market Timeline |
| Learning metrics | Training Status |
| Proposal decisions | AI Agent Decisions |
| Live workflow state | Live Portfolio Activity |
| Portfolio-level impact | Performance Dashboard |
| Day summary | Cockpit |

---

## 3) Key Concepts (No Jargon Version)

| Term | Meaning |
|---|---|
| **Signal** | "I saw a setup." |
| **Outcome** | "Did that setup work after 1/3/5/... periods?" |
| **Maturity** | "How much evidence do we have?" |
| **Coverage** | "How much of that evidence is already measurable?" |
| **Trust** | "Is this reliable enough to influence trading?" |
| **Proposal** | "Suggested trade, pending checks/approval." |
| **Trade** | "Executed action (paper/live flow dependent)." |
| **Risk Gate** | Safety state: SAFE / CAUTION / STOPPED. |
| **Episode** | A portfolio lifecycle segment ("trading season"). |

---

## 4) How To Read MIP Metrics

### 4.1 Horizon columns

- **H1** = result after 1 period
- **H5** = result after 5 periods
- positive average means historical edge in expected direction

Example:
- `Avg H5 = +0.008` means roughly **+0.8% average** after 5 periods.

### 4.2 Maturity score

Treat maturity as confidence-in-evidence, not a guarantee.

- Low score: too little data
- Mid score: learning phase
- High score: stronger historical support

### 4.3 Gate states

- **SAFE**: entries allowed
- **CAUTION**: warning zone
- **STOPPED**: no new entries, exits only

---

## 5) Page-by-Page Guide

### 5.1 Home (`/home`)

Use as your "pre-flight check":
- last pipeline run
- new evaluations
- latest digest recency
- quick links to core pages

### 5.2 Cockpit (`/cockpit`)

Your daily command center:
- what changed
- what matters
- what to watch next
- portfolio and system narratives

### 5.3 Training Status (`/training`)

Evidence table for symbol + pattern learning:
- maturity
- sample size
- coverage
- horizon averages

### 5.4 Performance Dashboard (`/performance-dashboard`)

Cross-portfolio comparison view:
- return trend comparison
- drawdown profile by portfolio
- stability vs performance tradeoff

### 5.5 Live Symbol Tracker (`/symbol-tracker`)

Symbol-first operational monitoring:
- symbol activity state
- context shifts worth follow-up
- handoff into decision review

### 5.6 Market Timeline (`/market-timeline`)

Follow one symbol end-to-end:
- signal count
- proposal path
- trade outcomes
- chart overlays and narrative context

### 5.7 Runs / Audit Viewer (`/runs`)

Operational truth source:
- what ran
- when
- status/duration/errors
- step-level diagnostics

### 5.8 AI Agent Decisions (`/decision-console`)

Committee-style decision log:
- simulation and live decision streams
- verdicts, reason codes, statuses
- detailed rationale per action

### 5.9 Live Portfolio Link (`/live-portfolio-config`)

Control surface for live-linked paper workflow:
- link state and configuration
- readiness and gating context
- portfolio-level link controls

### 5.10 Live Portfolio Activity (`/live-portfolio-activity`)

Operational activity feed for live-linked flow:
- recent lifecycle events
- validation/execution transitions
- current status by step

### 5.11 Learning Ledger (`/learning-ledger`)

Decision causality and learning trace:
- why a decision changed
- what evidence contributed
- whether change helped/hurt

### 5.12 News Intelligence (`/news-intelligence`)

News context + proposal impact:
- market context KPIs
- symbol cards
- evidence-backed impact rows

### 5.13 User Guide (`/guide`)

In-app reference manual sourced from versioned guide sections.

---

## 6) Daily Operating Routine (Recommended)

1. **Home**: confirm pipeline freshness.
2. **Cockpit**: read what changed / what matters.
3. **Performance Dashboard**: review return and drawdown changes.
4. **Training Status**: validate evidence behind active patterns.
5. **AI Agent Decisions**: inspect accepts/rejects and rationale.
6. **Runs**: if anything looks off, verify run-level details.

---

## 7) Ask MIP Assistant

Ask MIP is a route-aware help panel.

Best questions:
- "Explain this page in plain English."
- "What does this metric mean and what is a healthy range?"
- "Why might proposals exist but no trades execute?"
- "What should I check next if this status is STOPPED?"

### Pro tip

Include scope in your question:
- portfolio id
- symbol
- date/time window
- page you are looking at

That gives better, more precise answers.

---

## 8) Practical Examples

### Example A: "No trades today"

Check in order:
1. Runs page: did pipeline run successfully?
2. Training page: are mature/trustworthy patterns available?
3. Portfolio page: is gate STOPPED or capacity full?
4. AI Agent Decisions: were proposals rejected, and why?

### Example B: "Metric looks strange"

1. Confirm episode scope in Portfolio.
2. Compare with Runs timestamp and as-of dates.
3. Verify whether recent profile/cash events changed baseline.

---

## 9) Troubleshooting Shortlist

| Symptom | Likely cause | First check |
|---|---|---|
| Digest feels old | New run not reflected yet | Cockpit + Runs |
| No proposals | Weak trust/evidence or no fresh signals | Training + Timeline |
| Proposals but no execution | Gate/limits or decision rejection | Portfolio + AI Agent Decisions |
| A page looks stale | Data not refreshed yet | Runs + Home freshness cards |

---

## 10) Final Reminder

MIP is a learning system, not a certainty machine.

Use it like this:
- trust evidence over intuition,
- verify with the right page for each step,
- follow the event sequence when debugging.

---

*Last updated: March 16, 2026*
