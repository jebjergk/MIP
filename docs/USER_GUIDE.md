# MIP User Guide

**Market Intelligence Platform** - A complete guide to understanding and using the system.

---

## Table of Contents

1. [Introduction](#1-introduction)
2. [Understanding the Screens](#2-understanding-the-screens)
   - [Home](#21-home)
   - [Portfolio](#23-portfolio)
   - [Cockpit](#24-cockpit)
   - [Portfolio Management](#210-portfolio-management)
   - [Signals](#25-signals)
   - [Market Timeline](#26-market-timeline)
   - [Suggestions](#27-suggestions)
   - [Training Status](#28-training-status)
   - [Audit Viewer](#29-audit-viewer-run-explorer)
3. [How the Backend Works](#3-how-the-backend-works)
   - [Pipeline Overview](#31-pipeline-overview)
   - [Step-by-Step Breakdown](#32-step-by-step-breakdown)
   - [Data Flow](#33-data-flow)
   - [Intraday Subsystem](#34-intraday-subsystem)
4. [Key Concepts](#4-key-concepts)
   - [Episodes](#41-episodes)
   - [Crystallization](#42-crystallization)
   - [Risk Profiles](#43-risk-profiles)
   - [Run IDs](#44-run-ids)
   - [Risk Gates](#45-risk-gates)
   - [Trust Levels](#46-trust-levels)
   - [Proposals vs Trades](#47-proposals-vs-trades)
   - [Pipeline Lock](#48-pipeline-lock)
5. [Troubleshooting](#5-troubleshooting)
6. [Glossary](#6-glossary)

---

## 1. Introduction

MIP (Market Intelligence Platform) is a paper trading system that:

1. **Collects market data** from financial APIs
2. **Detects patterns** in price movements
3. **Generates trading signals** based on those patterns
4. **Evaluates signal quality** over time
5. **Simulates portfolios** with risk management
6. **Proposes and executes trades** (paper trading)
7. **Generates daily briefs** summarizing opportunities

The system runs automatically every day, but you can also trigger it manually. This guide explains what you see in the UI and how everything works behind the scenes.

---

## 2. Understanding the Screens

### 2.1 Home

**What it shows:** A quick overview of the system status and shortcuts to common actions.

| Element | What it means |
|---------|---------------|
| **Last Pipeline Run** | When the system last processed data. Shows time ago (e.g., "2 hours ago") |
| **New Evaluations** | How many signals were evaluated since the last run |
| **Latest Digest** | When the last digest was generated |
| **Quick Action Cards** | Shortcuts to Portfolios, Cockpit, Training Status, and Suggestions |

**When to use:** Start here to see if everything is running normally. If the "Last Pipeline Run" is old, something may be wrong.

---

### 2.3 Portfolio

**What it shows:** Detailed view of a trading portfolio's performance and current state.

#### List View (all portfolios)

| Column | What it means |
|--------|---------------|
| **Gate** | Risk status (SAFE/CAUTION/STOPPED) |
| **Health** | Overall portfolio health indicator |
| **Equity** | Current total value |
| **Paid Out** | Money withdrawn/distributed |
| **Active Episode** | Current trading period |
| **Status** | ACTIVE, BUST, or STOPPED |

#### Detail View (single portfolio)

**Header Metrics:**

| Metric | What it means |
|--------|---------------|
| **Starting Cash** | How much money the portfolio started with |
| **Final Equity** | Current total value (cash + positions) |
| **Total Return** | Percentage gain or loss |
| **Max Drawdown** | Largest peak-to-trough decline (worst case scenario) |
| **Win Days** | Days with positive returns |
| **Loss Days** | Days with negative returns |
| **Status** | Current state (ACTIVE, BUST, etc.) |

**Charts:**

| Chart | What it shows |
|-------|---------------|
| **Equity** | Portfolio value over time. Blue dot = current value. Red dashed line = bust threshold |
| **Drawdown** | Current decline from peak. More negative = worse. Dotted line = warning threshold |
| **Trades per Day** | Bar chart of trading activity |
| **Risk Regime** | Color strip showing SAFE (green), CAUTION (yellow), STOPPED (red) over time |

**Portfolio Snapshot Cards:**

| Card | What it shows |
|------|---------------|
| **Cash & Exposure** | Cash on hand, invested amount, total equity |
| **Open Positions** | Current holdings (stocks/assets owned) |
| **Trades** | Recent trade history with lookback filter |
| **Risk Gate** | Current risk controls and thresholds |

---

### 2.4 Cockpit

**What it shows:** The unified daily command center. Shows AI-generated narratives, portfolio status, training insights, signal candidates, and upcoming symbols — all in one view.

**Layout:**

| Row | Left Card | Right Card |
|-----|-----------|------------|
| **Row 1** | System Overview (Global Digest) | Portfolio Intelligence (Per-Portfolio Digest) |
| **Row 2** | Global Training Digest (full width) | |
| **Row 3** | Today's Signal Candidates | Upcoming Symbols |

**Portfolio Picker:** Top-right dropdown to select which portfolio's intelligence to show.

**System Overview (Left Card):**

| Element | What it shows |
|---------|---------------|
| **Headline** | AI-generated one-sentence summary of the day |
| **Cortex AI / Deterministic badge** | Whether the narrative was AI-generated or template-based |
| **Fresh / Stale badge** | Digest age — Fresh if under 2 hours, Stale if older |
| **What Changed** | Bullet points of differences from yesterday |
| **What Matters** | Important observations and implications |
| **Waiting For** | Upcoming triggers or thresholds to watch |
| **Detector Pills** | Color-coded interest detectors (green/orange/red severity) |

**Portfolio Intelligence (Right Card):**

Same structure as System Overview but scoped to the selected portfolio. Includes:

| Badge | What it shows |
|-------|---------------|
| **Episode badge (purple)** | Current episode number, e.g. "Episode 3 (of 3)". Hover for episode start date. All performance numbers in the narrative are scoped to this episode. |

**When to use:** Check this page first every morning. It tells you what changed overnight and what to focus on.

---

### 2.10 Portfolio Management

**What it shows:** Create and configure portfolios, manage risk profiles, deposit/withdraw cash, view lifecycle history, and generate AI portfolio stories.

> **Pipeline Lock:** When the daily pipeline is running, all editing is disabled. A yellow warning banner appears and buttons are greyed out. Editing re-enables automatically once the pipeline finishes (polls every 15 seconds).

**Four tabs:**

#### Portfolios Tab

| Action | What it does |
|--------|--------------|
| **+ Create Portfolio** | Create a new portfolio with name, currency, starting cash, and risk profile |
| **Edit** | Update name, currency, notes (starting cash is fixed after creation) |
| **Cash** | Deposit or withdraw money. P&L tracking stays intact — cost basis adjusts automatically |
| **Profile** | Attach a different risk profile. Warning: this ends the current episode and starts a new one |

#### Profiles Tab

Create/edit reusable risk profiles with:
- Position limits (max positions, max position %)
- Risk thresholds (bust equity %, drawdown stop %)
- Crystallization settings (profit target, mode, cooldown, max episode days)

#### Lifecycle Timeline Tab

Visual history of every portfolio event — 4 charts (equity, P&L, cash flows, cash vs equity) plus a vertical event timeline showing CREATE, DEPOSIT, WITHDRAW, CRYSTALLIZE, PROFILE_CHANGE, EPISODE_START, EPISODE_END, and BUST events.

#### Portfolio Story Tab

AI-generated narrative "biography" of a portfolio. Auto-generates on first visit, can be manually regenerated. Shows headline, narrative paragraphs, key moments, and outlook.

---

### 2.5 Signals

**What it shows:** A searchable list of all trading signals generated by the system.

| Column | What it means |
|--------|---------------|
| **Symbol** | The stock/asset (e.g., AAPL, GOOGL) |
| **Market** | Type of asset (STOCK, ETF, FX) |
| **Pattern** | Which pattern detected this signal |
| **Score** | Signal strength (higher = stronger) |
| **Trust** | Trust level (TRUSTED, WATCH, UNTRUSTED) |
| **Action** | BUY or SELL |
| **Eligible** | Can this signal be traded today? |
| **Signal Time** | When the signal was generated |

**Filters:**
- Filter by symbol, market type, pattern, trust level, run ID, or date
- Use "From Cockpit" to jump here with filters pre-set from the Cockpit

---

### 2.6 Market Timeline

**What it shows:** End-to-end view of what happened with each symbol — from signals to proposals to trades.

**Main grid:**
- Each card shows a symbol with signal (S), proposal (P), and trade (T) counts
- **Trust badges**: Trust level for this symbol's patterns
- **ACTION badge**: Shows if there's a proposal for today
- **Portfolio filter**: Dynamically loaded from the system — when a portfolio is selected, only symbols where that portfolio has proposals or trades are shown

**Expanded detail view:**

| Element | What it shows |
|---------|---------------|
| **Chart mode toggle** | Switch between **Line** (clean close-price line with high/low range) and **Candlestick** (traditional OHLC candles, green up / red down) |
| **Event overlays** | Blue dots = signals, orange dots = proposals, green dots = trades — shown in both chart modes |
| **Decision Narrative** | AI-generated explanation of what happened |
| **Trust Summary** | Trust levels by pattern for this symbol |
| **Signal Chains** | Tree view showing the full lifecycle: Signal → Proposal(s) per portfolio → BUY trade → SELL trade, with status badges (Open, Closed, Pending, Rejected), portfolio links, prices, and realized PnL |

The chart always extends to today's date so current-day activity is visible. Signal chains look back as far as the chart window (default 60 bars ≈ 3 months). Signals that never led to a proposal are counted in the header but not shown individually.

---

### 2.7 Suggestions

**What it shows:** Ranked list of trading opportunities based on historical performance.

**Two categories:**

| Category | Meaning |
|----------|---------|
| **Strong Candidates** | Patterns with 10+ recommendations (more reliable) |
| **Early Signals** | Patterns with 3-9 recommendations (still learning) |

**For each suggestion:**

| Element | What it means |
|---------|---------------|
| **Rank** | Overall ranking based on suggestion score |
| **Symbol/Pattern** | Which stock and pattern |
| **Suggestion Score** | Combined score (higher = better opportunity) |
| **Maturity Stage** | INSUFFICIENT → WARMING_UP → LEARNING → CONFIDENT |
| **Maturity Score** | 0-100% confidence in this pattern |
| **What History Suggests** | Summary of past performance |
| **Horizon Strip** | Performance at different time horizons (1, 3, 5, 10, 20 bars) |

**Evidence drawer:**
Click any suggestion to see detailed charts and statistics about its historical performance.

---

### 2.8 Training Status

**What it shows:** How well the system has learned each pattern for each symbol. Has two modes — **Daily (1440m)** and **Intraday (15m)** — toggled at the top of the page.

#### Daily Mode

| Column | What it means |
|--------|---------------|
| **Market Type** | STOCK, ETF, or FX |
| **Symbol** | The asset |
| **Pattern** | Pattern ID |
| **Interval** | Time interval (1440 = daily) |
| **Maturity** | Learning stage and score |
| **Sample Size** | How many times this pattern was observed |
| **Coverage** | What percentage of observations have been evaluated |
| **Horizons** | Which forward periods have been measured |
| **Avg H1–H20** | Average returns at each daily horizon (1, 3, 5, 10, 20 days) |

**Maturity stages:**

| Stage | Meaning |
|-------|---------|
| **INSUFFICIENT** | Not enough data to draw conclusions |
| **WARMING_UP** | Starting to gather data |
| **LEARNING** | Building confidence |
| **CONFIDENT** | Enough data for reliable predictions |

#### Intraday Mode (Cockpit Layout)

When toggled to Intraday (15m), the page becomes a **decision-first cockpit** with three layers:

**Layer 1 — Executive Summary** (visible without scrolling):
- **Status Banner**: System stage (INSUFFICIENT / EMERGING / LEARNING / CONFIDENT), total signals, outcomes evaluated, symbol count, tradable pattern count
- **Pattern Readiness Tiles**: One card per pattern family (ORB, MEAN_REVERSION, PULLBACK_CONTINUATION) with events, trust, confidence, best edge, and trend arrow
- **Pipeline Health Strip**: Compact single-line summary of the latest intraday pipeline run

**Layer 2 — Pattern Insights** (visible by default):
- **Trust Scoreboard**: One row per pattern with best horizon. Click to expand per-horizon breakdown
- **Signal Activity Chart**: Collapsible price chart with signal overlays

**Layer 3 — Advanced Diagnostics** (collapsed by default):
- Pattern Stability, Excursion Analysis
- Toggle with "Show advanced diagnostics" button (persisted in browser)

**Intraday horizon columns** are different from daily: **Avg H1** (+1 bar / 15m), **Avg H4** (+4 bars / ~1hr), **Avg H8** (+8 bars / ~2hr), **Avg EOD** (end-of-day close).

---

### 2.9 Audit Viewer (Run Explorer)

**What it shows:** Detailed history of all pipeline runs, including errors.

**Daily / Intraday Toggle:** Switch between "Daily Pipeline" and "Intraday Pipeline" views at the top of the page. Each pipeline type has its own run history, metrics, and detail panels.

**Run list:**
- Shows all pipeline runs with status, duration, and timestamp
- Filter by status (SUCCESS, FAIL, etc.) or date range

**Run detail:**

| Section | What it shows |
|---------|---------------|
| **Summary Cards** | Status, duration, as-of date, portfolio count, error count |
| **Run Narrative** | Plain English: what happened, why, impact, next steps |
| **Error Panel** | If failed: error message, SQL query ID, debug SQL to investigate |
| **Step Timeline** | Visual flow of each step (Ingestion → Returns → Recommendations → etc.) |
| **Step Details** | Click any step to see specifics |

---

## 3. How the Backend Works

### 3.1 Pipeline Overview

The system runs a daily pipeline that processes data through these steps:

```
1. INGESTION        → Fetch market data from API
2. RETURNS          → Calculate price returns
3. RECOMMENDATIONS  → Generate trading signals
4. EVALUATION       → Measure signal quality
5. SIMULATION       → Run portfolio paper trading
6. CRYSTALLIZATION  → Check profit targets, lock in gains, cycle episodes
7. PROPOSALS        → Create trade suggestions
8. EXECUTION        → Execute approved trades (paper)
9. DAILY DIGEST     → Generate AI narrative summaries (viewed in Cockpit UI)
```

**When does it run?**
- Automatically at 07:00 Europe/Berlin time
- Can be triggered manually anytime

**How long does it take?**
- Usually 2-5 minutes depending on market data availability

---

### 3.2 Step-by-Step Breakdown

#### Step 1: Ingestion

**What happens:** Fetches OHLC (Open, High, Low, Close) price data from AlphaVantage API.

**Tables affected:**
- `MARKET_BARS` - Stores all price data

**Key behavior:**
- Checks if new data exists since last run
- If rate-limited by API, may skip downstream steps
- Tracks "new bars" to decide if full processing is needed

#### Step 2: Returns Refresh

**What happens:** Calculates daily returns from price data.

**Tables affected:**
- `MARKET_RETURNS` (view) - Shows percentage changes

**Key behavior:**
- Calculates simple returns: (today - yesterday) / yesterday
- Calculates log returns: ln(today / yesterday)

#### Step 3: Generate Recommendations

**What happens:** Runs pattern detection algorithms to find trading signals.

**Tables affected:**
- `RECOMMENDATION_LOG` - Stores all detected signals

**Key behavior:**
- Scans each market type (STOCK, ETF, FX)
- Applies momentum and reversal patterns
- Generates a "score" for each signal

#### Step 4: Evaluate Recommendations

**What happens:** Measures how well past signals performed.

**Tables affected:**
- `RECOMMENDATION_OUTCOMES` - Stores evaluation results

**Key behavior:**
- For each past signal, checks: "What happened after?"
- **Daily signals**: measures returns at 1, 3, 5, 10, and 20 days forward
- **Intraday signals**: measures returns at +1, +4, +8 bars and end-of-day close
- Horizons are configured in the `HORIZON_DEFINITION` table
- Tracks "hit rate" (did it go the right direction?)

#### Step 5: Portfolio Simulation

**What happens:** Runs paper trading simulation for each active portfolio.

**Tables affected:**
- `PORTFOLIO_DAILY` - Daily equity snapshots
- `PORTFOLIO_TRADES` - Simulated trades
- `PORTFOLIO_POSITIONS` - Current holdings

**Key behavior:**
- Uses trusted signals to decide when to buy/sell
- Applies risk rules (position limits, drawdown stops)
- Updates portfolio metrics (equity, drawdown, etc.)

#### Step 6: Trade Proposals

**What happens:** Creates trade suggestions from eligible signals.

**Tables affected:**
- `ORDER_PROPOSALS` - Suggested trades

**Key behavior:**
- Filters for signals that are:
  - From trusted patterns
  - Within portfolio limits
  - Not duplicating existing positions
- Creates PROPOSED status entries

#### Step 7: Execution

**What happens:** Validates and executes approved proposals.

**Tables affected:**
- `ORDER_PROPOSALS` - Status updated to APPROVED/REJECTED/EXECUTED
- `PORTFOLIO_TRADES` - New trades inserted
- `PORTFOLIO_POSITIONS` - New positions created

**Key behavior:**
- Checks risk gate (entries blocked?)
- Validates position limits
- Executes as paper trades

#### Step 8: Daily Digest

**What happens:** Generates AI-powered narrative summaries of the day.

**Tables affected:**
- `DAILY_DIGEST_SNAPSHOT` / `DAILY_DIGEST_NARRATIVE` - Stores digest snapshots and AI narratives (viewed in Cockpit)
- `TRAINING_DIGEST_SNAPSHOT` / `TRAINING_DIGEST_NARRATIVE` - Stores training digests

**Key behavior:**
- Compiles opportunities, risks, and portfolio state into deterministic snapshots
- Calls Snowflake Cortex to generate AI narratives explaining what matters
- Persists with RUN_ID for lineage and SOURCE_FACTS_HASH for auditability

---

### 3.3 Data Flow

```
AlphaVantage API
       ↓
┌─────────────────┐
│  MARKET_BARS    │  ← Raw price data
└────────┬────────┘
         ↓
┌─────────────────┐
│ MARKET_RETURNS  │  ← Calculated returns
└────────┬────────┘
         ↓
┌─────────────────┐
│RECOMMENDATION_  │  ← Generated signals
│     LOG         │
└────────┬────────┘
         ↓
┌─────────────────┐
│RECOMMENDATION_  │  ← Evaluated performance
│   OUTCOMES      │
└────────┬────────┘
         ↓
┌─────────────────┐
│V_TRUSTED_SIGNALS│  ← Trust classification
└────────┬────────┘
         ↓
┌─────────────────────────────────────┐
│  PORTFOLIO_DAILY / TRADES / POSITIONS │  ← Simulation results
└────────┬────────────────────────────┘
         ↓
┌─────────────────┐
│ORDER_PROPOSALS  │  ← Trade suggestions
└────────┬────────┘
         ↓
┌─────────────────┐
│ MORNING_BRIEF   │  ← Final output (viewed in Cockpit UI)
└─────────────────┘
```

---

### 3.4 Intraday Subsystem

MIP has two independent pipelines that share the same database but run separately:

| Aspect | Daily Pipeline | Intraday Pipeline |
|--------|---------------|-------------------|
| **Bar interval** | 1440 minutes (daily) | 15 minutes |
| **Horizons** | 1, 3, 5, 10, 20 days | +1 bar (15m), +4 bars (~1hr), +8 bars (~2hr), EOD close |
| **Patterns** | Momentum, Reversal | ORB, Pullback Continuation, Mean-Reversion Overshoot |
| **Symbol universe** | Full (23+ symbols) | Focused (14–16 high-liquidity symbols) |
| **Schedule** | Daily 07:00 Berlin | Configurable (initially hourly during market hours) |
| **Stage** | Active trading | Learning only (no live trading yet) |

**Intraday Learning Loop:**

```
1. INGEST         → Fetch 15-minute bars from Alpha Vantage (delayed)
2. DETECT         → Run intraday pattern detectors (ORB, Pullback, Mean-Reversion)
3. LOG            → Record signals as hypotheses
4. EVALUATE       → Measure outcomes at +1, +4, +8 bars and EOD
5. SCORE          → Build trust/confidence per pattern
6. REPEAT         → Each run adds more evidence
```

**Three Intraday Patterns:**

- **Opening Range Breakout (ORB)** — Detects breakout from the early-session trading range
- **Pullback Continuation** — Impulse move → consolidation → breakout in original direction
- **Mean-Reversion Overshoot** — Extreme deviation from short-term average, expecting reversion

**Data Isolation:** The intraday pipeline does not modify daily signals, training, or execution. It has its own Snowflake task, pattern definitions, and trust scoring. Disabling it has zero impact on daily.

**Horizon Metadata:** Both daily and intraday horizons are stored in the `HORIZON_DEFINITION` table with type (BAR, DAY, SESSION), length, resolution, and display labels.

---

## 4. Key Concepts

### 4.1 Episodes

**What is an episode?**

An episode is a lifecycle period for a portfolio. Think of it as a "trading season" with a clear start and end.

**Episode states:**

| State | Meaning |
|-------|---------|
| **ACTIVE** | Currently trading |
| **ENDED** | Finished normally |
| **CRYSTALLIZED** | Profit target hit, gains locked in |
| **STOPPED** | Risk limits breached |
| **BUST** | Lost too much, trading halted |

**Why episodes matter:**
- All portfolio data (trades, positions, equity, KPIs) is scoped to the current episode
- AI narratives in the Cockpit and digest know which episode is active and report performance accordingly
- The Cockpit shows an "Episode N (of M)" badge on the Portfolio Intelligence card
- When you change a profile, deposit/withdraw cash that triggers crystallization, or hit a profit target, the current episode ends and a new one starts
- Historical episodes are preserved — view them on the Portfolio page or Lifecycle Timeline

**What triggers a new episode:**
- **Crystallization**: Profit target reached → gains locked in → new episode
- **Profile change**: Attaching a different risk profile ends the current episode
- **Manual reset**: Forced restart via system operator
- **Max episode days**: If configured in the profile, the episode auto-ends after N days

**Episode boundaries:**
- **Start**: When the episode begins (creation, crystallization, profile change)
- **End**: When a trigger occurs (profit target, drawdown stop, profile change, bust)

---

### 4.2 Crystallization

**What is crystallization?**

Crystallization is the process of locking in profits when a portfolio hits its profit target. It's configured per risk profile and runs automatically as part of the daily pipeline.

**How it works:**

1. The pipeline checks if the portfolio's return exceeds the profit target (e.g., +10%)
2. If yes, it "crystallizes" — the current episode ends, gains are recorded
3. A new episode starts with either the original capital (Withdraw Profits) or the higher equity (Rebase)

**Two modes:**

| Mode | What happens to profits | Example |
|------|------------------------|---------|
| **Withdraw Profits** | Gains are withdrawn from the portfolio. New episode starts with original capital. | Started with $100K, earned $10K → $10K paid out, new episode starts at $100K |
| **Rebase (compound)** | Gains stay in the portfolio. New episode starts with higher cost basis. | Started with $100K, earned $10K → new episode starts at $110K, no payout |

**Configuration options:**
- **Profit Target %**: The return threshold (e.g., 10%)
- **Cooldown Days**: Minimum days between crystallizations (e.g., 30)
- **Max Episode Days**: Force a new episode after N days even without hitting the target
- **Take Profit On**: Check target at End of Day or Intraday

---

### 4.3 Risk Profiles

**What is a risk profile?**

A reusable template that defines how a portfolio should behave. You create profiles once and attach them to any number of portfolios.

**Profile settings:**

| Setting | What it controls | Example |
|---------|-----------------|---------|
| **Max Positions** | Maximum concurrent holdings | 10 |
| **Max Position %** | Size limit per position (% of cash) | 8% |
| **Bust Equity %** | Equity floor before bust (% of starting cash) | 50% |
| **Bust Action** | What happens at bust | Allow Exits Only |
| **Drawdown Stop %** | Max decline from peak before entries blocked | 15% |
| **Crystallization** | Profit-taking settings (target, mode, cooldown) | 10% target, Withdraw Profits |

**Changing profiles:** You can attach a different profile at any time through Portfolio Management. Changing the profile ends the current episode and starts a new one — this is intentional, as different risk parameters mean a different "trading context."

---

### 4.4 Run IDs

**What is a Run ID?**

A unique identifier that tracks every pipeline execution and simulation run.

**Types of Run IDs:**

| Type | Format | Example | Used for |
|------|--------|---------|----------|
| **Pipeline Run ID** | UUID | `05a39214-cc22-44ea-85e3-38e8b5116c06` | Tracking the overall pipeline |
| **Simulation Run ID** | UUID | `1cb7388a-48f9-4674-8143-4271b209863d` | Portfolio simulation |
| **Signal Run ID** | Timestamp | `20260204T170000` | Legacy signals (being phased out) |

**Why Run IDs matter:**
- Lets you trace any data back to when it was created
- Enables "idempotency" - re-running won't create duplicates
- Used for debugging issues

---

### 4.5 Risk Gates

**What is a risk gate?**

A safety mechanism that can block new trades when risk thresholds are breached.

**Gate states:**

| State | Color | Meaning |
|-------|-------|---------|
| **SAFE** | Green | Normal operation, entries allowed |
| **CAUTION** | Yellow | Warning level, monitor closely |
| **STOPPED** | Red | Entries blocked, only exits allowed |

**What triggers STOPPED?**
- **Drawdown stop**: Portfolio dropped too much from peak (e.g., 10%)
- **Bust threshold**: Portfolio value fell below bust level (e.g., 60% of starting cash)

**Profile thresholds:**
Each portfolio profile defines its own risk limits:

| Threshold | Meaning |
|-----------|---------|
| **Drawdown Stop %** | Max allowed decline from peak (e.g., 10%) |
| **Bust Equity %** | Minimum value before bust (e.g., 60%) |
| **Max Positions** | Maximum number of holdings |
| **Max Position %** | Maximum size of any single position |

---

### 4.6 Trust Levels

**What is trust?**

A classification of how reliable a pattern is, based on historical performance.

**Trust levels:**

| Level | Meaning | Threshold |
|-------|---------|-----------|
| **TRUSTED** | Reliable, can trade | 30+ successes, 80%+ coverage, positive returns |
| **WATCH** | Promising but needs more data | Some successes, monitoring |
| **UNTRUSTED** | Not reliable, avoid | Negative returns or insufficient data |

**How trust is calculated:**
- System evaluates past signals: "Did the price move as predicted?"
- Counts successes vs failures at each time horizon
- Patterns that consistently work become TRUSTED

**Why trust matters:**
- Only TRUSTED patterns are used for actual trade proposals
- WATCH patterns are monitored but not traded
- UNTRUSTED patterns are ignored

---

### 4.7 Proposals vs Trades

**Understanding the difference:**

| Concept | What it is | Where to see it |
|---------|------------|-----------------|
| **Signal** | A pattern detection (raw opportunity) | Signals page |
| **Proposal** | A suggested trade (passed filters) | Cockpit, ORDER_PROPOSALS table |
| **Trade** | An executed transaction (paper) | Portfolio page, PORTFOLIO_TRADES table |

**The journey:**

```
SIGNAL → [Trust check] → [Eligibility check] → PROPOSAL → [Risk check] → TRADE
```

**Status progression:**

| Status | Meaning |
|--------|---------|
| **PROPOSED** | Suggestion created, awaiting validation |
| **APPROVED** | Passed all checks, ready to execute |
| **REJECTED** | Failed validation (risk gate, limits, etc.) |
| **EXECUTED** | Successfully traded (paper) |

---

### 4.8 Pipeline Lock

**What is the pipeline lock?**

A safety mechanism that prevents portfolio editing while the daily pipeline is actively running. This avoids data conflicts — for example, depositing cash at the exact moment the simulation is calculating equity could lead to inconsistent results.

**How it works:**
- The Portfolio Management page polls the system status every 15 seconds
- If the pipeline is detected as running (a START event exists with no matching completion), all Create/Edit/Cash/Profile buttons are disabled
- A yellow warning banner appears: "Pipeline is currently running — editing is disabled until the run completes"
- Once the pipeline finishes (SUCCESS, FAIL, or SUCCESS_WITH_SKIPS), buttons automatically re-enable
- Read-only tabs (Lifecycle Timeline, Portfolio Story) remain accessible during a lock

**When you might see it:**
- During the daily automated pipeline run (typically at 07:00 Europe/Berlin)
- During a manually triggered pipeline run
- The lock typically lasts 2–5 minutes

---

## 5. Troubleshooting

### "STALE" digest showing

**Symptom:** Cockpit shows "STALE" badge for the digest

**Cause:** A newer pipeline run completed but the digest is from an older run

**Solution:**
1. Check Audit Viewer for recent pipeline runs
2. If a run completed successfully, the digest should refresh
3. If stuck, check if the brief write step failed

---

### No trades happening

**Symptom:** Portfolio shows no trades despite active signals

**Possible causes:**

| Cause | How to check |
|-------|--------------|
| Risk gate STOPPED | Check Portfolio page - Risk Gate card |
| No trusted patterns | Check Training Status for maturity |
| No new market data | Check if pipeline shows "NO_NEW_BARS" |
| Position limits full | Check Open Positions vs Max Positions |

---

### Wrong metrics (drawdown, win/loss days)

**Symptom:** Metrics don't match expected values

**Possible cause:** Historical data from before episode reset

**Solution:**
1. Run the episode cleanup migration
2. Re-run the pipeline to refresh metrics

---

### Pipeline shows "SUCCESS_WITH_SKIPS"

**Symptom:** Pipeline completed but with skips

**Meaning:** Some steps were skipped, usually because:
- No new market data available
- Rate limit hit on API
- Downstream steps not needed

**This is normal** when there's no new data to process.

---

## 6. Glossary

| Term | Definition |
|------|------------|
| **Bar** | A single OHLC price data point (Open, High, Low, Close) |
| **Bust** | When portfolio value drops below the bust threshold |
| **Cost Basis** | Average entry price for a position, adjusted for deposits/withdrawals |
| **Cortex AI** | Snowflake's built-in LLM service used to generate narrative digests and portfolio stories |
| **Crystallization** | Locking in gains when a profit target is hit. Ends the current episode. Two modes: Withdraw Profits or Rebase (compound) |
| **Deposit / Withdraw** | Cash events that add/remove money without affecting P&L. Cost basis adjusts automatically |
| **Drawdown** | Decline from peak equity (expressed as %) |
| **Episode** | A portfolio lifecycle period with defined start/end. All KPIs are scoped to the active episode |
| **Equity** | Total portfolio value (cash + positions) |
| **Hit Rate** | Percentage of signals that went in the predicted direction |
| **Horizon** | Forward time period for evaluation. Daily: 1, 3, 5, 10, 20 days. Intraday: +1, +4, +8 bars and EOD close |
| **Horizon Definition** | Metadata table storing horizon type (BAR, DAY, SESSION), length, and display labels for both daily and intraday |
| **Intraday Pipeline** | Independent pipeline ingesting 15-min bars, detecting intraday patterns, and evaluating bar-based outcomes |
| **Opening Range Breakout (ORB)** | Intraday pattern detecting price breakout from early-session trading range |
| **Pullback Continuation** | Intraday pattern: impulse move → consolidation → breakout continuation |
| **Mean-Reversion Overshoot** | Intraday pattern detecting extreme deviation from a short-term average |
| **Idempotent** | Can be run multiple times without creating duplicates |
| **Lifecycle Event** | Immutable record of a portfolio state change (CREATE, DEPOSIT, WITHDRAW, CRYSTALLIZE, etc.) |
| **Maturity** | How well the system has learned a pattern |
| **OHLC** | Open, High, Low, Close - standard price bar format |
| **Pattern** | A price behavior that signals a trading opportunity |
| **Pipeline** | The automated sequence of data processing steps |
| **Pipeline Lock** | Safety mechanism that disables portfolio editing while the pipeline is running |
| **Portfolio Story** | AI-generated narrative biography of a portfolio — creation through current state |
| **Position** | A holding in an asset (shares owned) |
| **Proposal** | A suggested trade that passed initial filters |
| **Risk Gate** | Safety mechanism controlling trade entries |
| **Risk Profile** | Reusable template defining portfolio risk rules (position limits, drawdown stops, crystallization) |
| **Run ID** | Unique identifier for a pipeline or simulation execution |
| **Signal** | A detected trading opportunity from pattern matching |
| **Simulation** | Paper trading that doesn't use real money |
| **Trust** | Reliability classification of a pattern |
| **Early Exit** | Execution optimization that closes daily positions before horizon when intraday data confirms payoff achieved and giveback risk is high |
| **Giveback Risk** | Risk that a position reverses after reaching its target return, erasing gains |
| **Shadow Mode** | Early-exit mode that evaluates and logs decisions without actually closing positions |
| **Decision Console** | Live UI page showing open positions, decision events, and gate traces in real time |
| **Decision Diff** | Comparison of exit-now vs hold-to-horizon outcomes for an open position |
| **Gate Trace** | Timeline of all decision gates evaluated for a position, with pass/fail results and metrics |
| **Max Favorable Excursion (MFE)** | Highest unrealized return a position achieves before exit |
| **Server-Sent Events (SSE)** | Web protocol used by the Decision Console for real-time updates without polling |

---

*Last updated: February 21, 2026*
