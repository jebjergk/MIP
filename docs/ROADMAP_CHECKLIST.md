# MIP Roadmap Checklist

This document tracks delivery vs backlog across three phases. Checkboxes: ✅ done, ⏳ in progress / next, ⬜ backlog.

---

## Phase 1 — Foundations: Ingest → Bars/Returns → Signals → Evaluation (Daily-bar training)

**Goal:** reliable daily pipeline + trustworthy outcome data + no hardcoding surprises.

### Pipeline + Data plumbing

- ✅ Daily pipeline runs end-to-end (`SP_RUN_DAILY_PIPELINE`) and writes structured audit JSON
- ✅ Ingest supports STOCK + ETF + FX universe (e.g. 24 symbols across types)
- ✅ Returns refresh produces returns for latest bars
- ✅ Graceful degradation:
  - ✅ handles `RATE_LIMIT` with `SUCCESS_WITH_SKIPS`
  - ✅ detects `NO_NEW_BARS` and skips downstream steps cleanly (`SKIPPED_NO_NEW_BARS`)
- ✅ Recommendations step runs without errors (even if it inserts 0)
- ✅ Evaluation step runs and writes horizon counts (e.g. horizons 1/3/5/10/20)
- ✅ Audit lineage is clean: one step row per run (duplicate-check query confirms)

### Signal generation behavior (quality)

- ✅ Threshold gating exists (e.g. `FILTERED_BY_THRESHOLD`)
- ✅ Dedup logic exists (e.g. `ALREADY_GENERATED_FOR_TS`)
- ⏳ Make "why 0 inserted" more transparent:
  - add counts for "candidates", "passed threshold", "dedup removed", "inserted"
- ⬜ Add "signal coverage KPIs" (per market_type/symbol):
  - candidates/day, inserted/day, hit-rate by horizon, avg return, drawdown

### Evaluation dataset readiness

- ✅ You are generating outcomes at scale (hundreds+ per run)
- ⏳ Confirm evaluation semantics are exactly what you expect:
  - entry price definition, return calc, horizon alignment, handling gaps/holidays
- ⬜ Add "evaluation integrity checks":
  - missing bars, missing returns, duplicate recs, misaligned timestamps

---

## Phase 2 — Paper Portfolio Simulation with cash + constraints (Execution realism)

**Goal:** turn signals into a consistent simulated trading process.

### Portfolio simulation engine

- ✅ Portfolio simulation runs (`PORTFOLIO_SIMULATION`)
- ✅ It creates trades (e.g. 4–5 trades per run)
- ✅ It computes equity, total return, win/loss days, max drawdown
- ✅ Risk stop works:
  - ✅ `DRAWDOWN_STOP` triggered
  - ✅ switches regime to `ALLOW_EXITS_ONLY`
- ✅ Morning brief is produced per portfolio (brief rows inserted, brief ids increasing)
- ✅ Proposer/executor pipeline steps exist and run

### The key "next unlock"

Right now the system correctly blocks entries most of the time because drawdown stop is hit quickly:

- ✅ That's good risk behavior
- ⏳ But it prevents learning/training iteration because you don't generate enough new trades

**Phase 2 backlog:**

- ⏳ Add "profile tuning knobs" + logging so you can experiment systematically:
  - initial cash, max_positions, max_position_pct, drawdown_stop_pct, bust_equity_pct
- ⏳ Add a controlled "training mode" profile:
  - less strict drawdown stop, smaller position sizing, fewer symbols, or fewer candidates/day
- ⬜ Add transaction costs + slippage (even simple bps model)
- ⬜ Add exposure rules (per asset class / correlation / sector for ETFs)

---

## Phase 3 — Risk layer + Hedging + AI Brief/Suggestions (Decision support)

**Goal:** risk-aware proposals + "why" + recommendations you can defend.

### Risk layer

- ⏳ Risk summaries per portfolio:
  - exposure by market_type, concentration, recent volatility, drawdown trajectory
- ⬜ Hedging logic:
  - e.g. if equity drawdown rising, reduce gross; hedge with index ETF; FX hedge rules
- ⬜ Stress scenarios:
  - "what if vol doubles", "gap down", "correlation spikes"

### Agent / Briefing layer ("AI suggestions" stage)

- ✅ You already have a "Morning Brief" write-out mechanism working
- ⏳ Upgrade brief content from status → insight:
  - what changed since last run, why entries blocked, what would unblock, top candidates with rationale
- ⬜ Multi-agent roles (later, but structured):
  - Signal Scout (opportunities)
  - Risk Officer (blocks/limits)
  - Portfolio Manager (sizing/rotation)
  - Market Context (news/sentiment, later)

---

## Current state from 2026-01-27 logs

- **Rate-limit handling works:** `RATE_LIMIT` → `SUCCESS_WITH_SKIPS`.
- **No-new-bars gating works:** `NO_NEW_BARS` → `SKIPPED_NO_NEW_BARS`; downstream steps skipped cleanly.
- **Drawdown stop causes exits-only:** `DRAWDOWN_STOP` triggers; regime switches to `ALLOW_EXITS_ONLY`; entries blocked, exits allowed.
