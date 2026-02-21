# Daily Trading Edge Audit Report

**Date:** 2026-02-21
**Scope:** Full historical signal window (Sep 10, 2025 -- Feb 20, 2026)
**Data:** 354 daily signals, 101 portfolio trades, 4 portfolios, 4 pattern families, 1,605 parallel-world results

---

## Executive Summary

Portfolio equity is flat because three structural factors compound to suppress any latent signal edge:

1. **Massive deployment gap** -- signals were generated for 83 trading days but portfolio trades only occurred during the final 10 days (Feb 3-20, 2026). The first 73 days of signals went untraded entirely.
2. **Ultra-short holding horizon** -- 87% of all positions are held for exactly 1 bar (1 day), yet the best risk-adjusted returns for stock momentum patterns occur at 3-5 bars, where avg return is 3-4x higher.
3. **Extreme cash drag** -- on average only 10.2% of portfolio capital is deployed at any time; 43% of trading days have zero open positions.

The underlying signals show a weak but directionally positive edge at short horizons (1-5 bars) for stock momentum patterns, but this edge is too small to survive once it is filtered through ultra-short holds, low deployment, and conservative sizing. No pattern/horizon combination produces returns that are statistically distinguishable from zero at conventional significance levels (except ETF 20-bar with only 7 observations).

**Primary limiting factor:** Holding horizon mismatch combined with deployment gap.
**Current trading is statistically indistinguishable from random behavior** at conventional confidence levels.

---

## 1. Signal Quality Analysis

### 1.1 Pattern/Horizon Outcome Summary

| Pattern | Horizon | N | Win Rate | Avg Return | Sharpe-Like | Net After 10bp Fees |
|---------|---------|---|----------|------------|-------------|---------------------|
| STOCK_MOMENTUM_FAST | 1 | 88 | 59.1% | 0.147% | 0.096 | +0.047% |
| STOCK_MOMENTUM_FAST | 3 | 83 | 59.0% | 0.466% | 0.159 | +0.366% |
| STOCK_MOMENTUM_FAST | 5 | 82 | 56.1% | 0.561% | 0.135 | +0.461% |
| STOCK_MOMENTUM_FAST | 10 | 78 | 55.1% | 0.054% | 0.009 | -0.046% |
| STOCK_MOMENTUM_FAST | 20 | 75 | 48.0% | 0.036% | 0.004 | -0.065% |
| STOCK_MOMENTUM_SLOW | 1 | 117 | 57.3% | 0.083% | 0.056 | -0.017% |
| STOCK_MOMENTUM_SLOW | 3 | 110 | 56.4% | 0.239% | 0.086 | +0.139% |
| STOCK_MOMENTUM_SLOW | 5 | 108 | 51.9% | 0.308% | 0.079 | +0.208% |
| STOCK_MOMENTUM_SLOW | 10 | 102 | 53.9% | -0.021% | -0.004 | -0.121% |
| STOCK_MOMENTUM_SLOW | 20 | 99 | 45.5% | -0.173% | -0.022 | -0.273% |
| FX_MOMENTUM_DAILY | 1 | 91 | 53.8% | 0.076% | 0.166 | -0.024% |
| FX_MOMENTUM_DAILY | 3 | 91 | 53.8% | 0.114% | 0.149 | +0.014% |
| FX_MOMENTUM_DAILY | 5 | 91 | 52.7% | 0.012% | 0.013 | -0.088% |
| ETF_MOMENTUM_SMOKE | 1 | 19 | 52.6% | -0.016% | -0.026 | -0.116% |
| ETF_MOMENTUM_SMOKE | 20 | 7 | 100% | 3.779% | 1.375 | +3.679% |

**Key findings:**

- The best risk-adjusted returns come from **STOCK_MOMENTUM_FAST at 3-bar horizon** (Sharpe 0.159, avg 0.47%) and **5-bar horizon** (Sharpe 0.135, avg 0.56%).
- At 1-bar, STOCK_MOMENTUM_FAST barely survives fees (+4.7 bp net); STOCK_MOMENTUM_SLOW turns negative after fees.
- FX patterns show higher Sharpe-like ratios at 1-bar (0.166) due to lower volatility, but the raw return is tiny (7.6 bp) and goes negative after fees.
- Stock patterns degrade sharply beyond 5 bars; 10 and 20-bar horizons are near zero or negative.
- ETF 20-bar result (100% win rate, 3.8% avg return) is based on only 7 observations and is unreliable.

### 1.2 Statistical Significance (Binomial + t-tests)

| Pattern | Horizon | N | Win Rate | Z-score | t-stat | Significance |
|---------|---------|---|----------|---------|--------|--------------|
| STOCK_MOMENTUM_FAST | 1 | 88 | 59.1% | 1.706 | 0.899 | MARGINAL (win rate) / NS (returns) |
| STOCK_MOMENTUM_FAST | 3 | 83 | 59.0% | 1.646 | 1.450 | MARGINAL / NS |
| STOCK_MOMENTUM_FAST | 5 | 82 | 56.1% | 1.104 | 1.221 | NS |
| FX_MOMENTUM_DAILY | 1 | 91 | 53.8% | 0.734 | 1.582 | NS |
| FX_MOMENTUM_DAILY | 3 | 91 | 53.8% | 0.734 | 1.423 | NS |

No pattern/horizon combination achieves statistical significance at the 5% level on returns. STOCK_MOMENTUM_FAST 1-bar and 3-bar win rates are marginally significant (z > 1.645). The system has a directional edge but insufficient sample size and/or return magnitude to confirm it statistically.

### 1.3 Signal Stability Over Time

Signal quality is not stable. Splitting each pattern into three equal time windows:

| Pattern | Horizon | Window 1 Avg | Window 2 Avg | Window 3 Avg | Trend |
|---------|---------|-------------|-------------|-------------|-------|
| STOCK_MOMENTUM_FAST | 1 | +0.605% | -0.098% | -0.081% | Decaying |
| STOCK_MOMENTUM_FAST | 3 | +1.427% | +0.025% | -0.073% | Decaying |
| FX_MOMENTUM_DAILY | 1 | +0.087% | +0.088% | +0.054% | Flat/slight decay |
| STOCK_MOMENTUM_SLOW | 1 | +0.301% | -0.048% | -0.003% | Decaying |

Early signals (Oct-Nov 2025) showed substantially stronger returns. Recent signals (Jan-Feb 2026) are near zero. This may reflect regime change, data snooping in early parameter selection, or normal mean-reversion of sample edge.

### 1.4 Score-Return Correlation

Score (entry return signal) is a weak predictor of forward returns:

| Pattern | Horizon | Correlation |
|---------|---------|-------------|
| STOCK_MOMENTUM_FAST | 1 | 0.189 |
| STOCK_MOMENTUM_FAST | 5 | 0.196 |
| STOCK_MOMENTUM_SLOW | 1 | 0.101 |
| FX_MOMENTUM_DAILY | 1 | -0.024 |
| FX_MOMENTUM_DAILY | 3 | -0.052 |

Stock patterns show weak positive correlation (0.10-0.20) -- score has modest predictive value. FX score has no predictive value (near-zero or negative correlation).

---

## 2. Trust Gate Effectiveness

### 2.1 Trust Filtering Outcome Comparison

| Trust Label | Horizon | N | Avg Return | Win Rate |
|-------------|---------|---|------------|----------|
| TRUSTED | 1 | 296 | +0.100% | 56.8% |
| TRUSTED | 3 | 284 | +0.265% | 56.3% |
| TRUSTED | 5 | 173 | +0.272% | 54.3% |
| TRUSTED | 10 | 269 | +0.002% | 54.7% |
| WATCH | 5 | 108 | +0.308% | 51.9% |
| WATCH | 20 | 174 | -0.083% | 46.6% |
| UNTRUSTED | 1-10 | 49 | negative | mixed |
| UNTRUSTED | 20 | 7 | +3.779% | 100% |

**Assessment:** The trust gate correctly separates higher-quality from lower-quality pattern/horizons on average. TRUSTED signals at 1-5 bars outperform UNTRUSTED signals. However:

- WATCH patterns at 5-bar (STOCK_MOMENTUM_SLOW) have higher avg returns (0.31%) than some TRUSTED combinations. This is a false exclusion.
- The gate has a 44% false-positive rate (trusted signals with negative outcome) and a ~50-59% false-negative rate (untrusted/watch signals with positive outcome). This is expected for noisy signals and does not indicate a broken gate.

### 2.2 Gate Calibration

All 7 trusted pattern/horizons barely clear the training gate thresholds (MIN_HIT_RATE 55%, MIN_AVG_RETURN 0.05%). The gate is not too permissive -- if anything, it is appropriately conservative. STOCK_MOMENTUM_FAST at 10-bar (0.054% avg return, 55.1% hit rate) passes the gate but has near-zero Sharpe.

**Conclusion:** Trust gate is functioning correctly. It enhances signal quality by filtering out ETF_MOMENTUM_SMOKE (negative returns at most horizons) and long-horizon stock patterns. One improvement opportunity: STOCK_MOMENTUM_SLOW at 5-bar (WATCH status) could be promoted -- it outperforms several TRUSTED combinations.

---

## 3. Portfolio Selection Policy

### 3.1 The Deployment Gap

This is the single most consequential finding. Of 83 signal days:

- **73 days (88%)** had signals but zero portfolio trades
- **10 days (12%)** had actual trading activity (Feb 3-20, 2026)

The portfolio simulation only covers a narrow recent window. All signals from Sep 2025 through Jan 2026 were evaluated for outcomes but never traded. This means the portfolio's return is based on approximately 18 sell events over 10 trading days, not the full 83-day signal history.

### 3.2 Rank Effectiveness

Counterintuitively, higher-scored signals (TOP_5 by score on each day) underperform lower-scored signals at 1-bar and 3-bar horizons:

| Horizon | TOP_5 Avg Return | REST Avg Return | TOP_5 Win Rate | REST Win Rate |
|---------|-----------------|----------------|----------------|---------------|
| 1 | 0.058% | 0.258% | 53.8% | 69.1% |
| 3 | 0.221% | 0.386% | 54.4% | 70.5% |
| 5 | 0.306% | -0.051% | 53.0% | 55.8% |
| 10 | 0.094% | -0.637% | 53.6% | 55.8% |

At short horizons, the score-based ranking actually selects worse signals. This is consistent with the weak/negative score-return correlation. At longer horizons (5+), top-scored signals do outperform, but with insufficient margin.

### 3.3 Slot Utilization

- Average utilization: variable (0% to 100%)
- 43% of trading days had zero positions
- Full capacity (5/5 slots) reached only 2 of 14 days

---

## 4. Position Sizing & Allocation Effects

### 4.1 Cash Drag

| Metric | Value |
|--------|-------|
| Average cash % | 89.8% |
| Average invested % | 10.2% |
| Days fully in cash | 6 of 14 (43%) |
| Max exposure reached | 25.0% |

Nearly 90% of capital sits idle. Even on days with maximum position count (5), only 25% of the portfolio is deployed (5 positions x 5% each). This structurally limits portfolio-level returns to one-quarter of the signal-level edge at best.

### 4.2 Winner/Loser Asymmetry

| | Count | Avg Notional | Avg |PnL| | Total PnL |
|---|---|---|---|---|
| Winners | 12 | $5,249 | $66 | +$792 |
| Losers | 6 | $4,287 | $68 | -$410 |

Winners are appropriately larger (higher notional) and more numerous (2:1 ratio). The net +$382 is the total lifetime PnL for Portfolio 1. Position sizing is neutral -- winners and losers have comparable absolute PnL per trade (~$66-68), so sizing does not systematically dilute gains.

### 4.3 Score-PnL Correlation at Portfolio Level

Portfolio 1 score-to-PnL correlation: **0.039** (effectively zero). Entry score does not predict trade profitability at the portfolio level.

---

## 5. Holding Horizon Alignment

### 5.1 The 1-Bar Problem

| Portfolio | 1-bar positions | 5-bar positions | Total |
|-----------|----------------|----------------|-------|
| 1 | 20 (87%) | 3 (13%) | 23 |
| 2 | 20 (100%) | 0 | 20 |
| 301 | 16 (100%) | 0 | 16 |

Near-universal 1-day holding. This is the second most impactful structural finding.

### 5.2 Optimal Horizon by Pattern

| Pattern | Optimal Horizon | Optimal Sharpe | Actual (1-bar) Sharpe | Return Left on Table |
|---------|----------------|----------------|----------------------|---------------------|
| STOCK_MOMENTUM_FAST | 3 bars | 0.159 | 0.096 | +0.32% per signal |
| STOCK_MOMENTUM_SLOW | 3 bars | 0.086 | 0.056 | +0.16% per signal |
| FX_MOMENTUM_DAILY | 1 bar | 0.166 | 0.166 | None |

For stock patterns, the optimal horizon is 3 bars, not 1. Holding 3 bars instead of 1 would roughly triple the average per-signal return for STOCK_MOMENTUM_FAST (0.47% vs 0.15%).

### 5.3 Missed Continuation

Across all 315 signals with both 1-bar and 3-bar outcomes:
- **52%** of signals had higher returns at 3 bars than at 1 bar
- Average incremental gain from holding to 3 bars: **+0.13%** per signal
- Average incremental gain from holding to 5 bars: **+0.16%** per signal

This confirms that the system systematically exits too early for stock momentum signals.

---

## 6. Parallel Worlds Counterfactual Review

### 6.1 Scenario Comparison

| Scenario | Avg Delta vs Actual | Days Better | Assessment |
|----------|-------------------|-------------|------------|
| SIZING 1.5x | +$15.42/day | 32% | Doubles PnL but low win rate |
| TIMING delay 2 bars | +$2.22/day | 3% | Marginal improvement |
| All threshold sweeps | $0 | 0% | No impact (signals unchanged) |
| DO_NOTHING | -$4.98/day | 28% | Confirms slight edge vs cash |

**Key observations:**
- Threshold sweeps have zero effect -- adjusting entry thresholds doesn't change which signals fire. This suggests the signal generation thresholds are already at a natural boundary.
- Sizing at 1.5x is the only lever that moves the needle, but it simply amplifies existing PnL linearly.
- The DO_NOTHING baseline confirms the portfolio does slightly better than holding cash (+$5/day on average), but this edge is very small.

### 6.2 Recommendation Viability

All 8 parallel-world recommendations carry WEAK confidence (13 days observation, 8-39% win rate). None are actionable yet. The recommendation system correctly identifies that insufficient data exists to make confident parameter changes.

---

## 7. Classification of Limiting Factors

### Primary Factors (Structural)

| Rank | Factor | Impact | Evidence |
|------|--------|--------|----------|
| 1 | **Deployment gap** | Critical | 88% of signal days had zero trades; portfolio only actively traded for 10 of 83 days |
| 2 | **Horizon mismatch** | High | 87-100% of positions use 1-bar hold; optimal is 3-bar for stock patterns, tripling per-signal return |
| 3 | **Cash drag** | High | 89.8% average cash; max 25% exposure; structural 4x dilution of any signal edge |

### Secondary Factors (Moderate)

| Rank | Factor | Impact | Evidence |
|------|--------|--------|----------|
| 4 | **Signal decay** | Moderate | Early signals showed 0.6% avg return; recent signals near 0%; edge may be eroding |
| 5 | **Weak score predictiveness** | Moderate | Score-return correlation 0.04-0.20; score-based ranking at 1-bar actually selects worse signals |

### Non-Factors (Functioning Correctly)

| Factor | Assessment |
|--------|-----------|
| Trust gate | Correctly filters bad patterns; appropriate calibration |
| Position sizing per trade | Winners and losers are symmetrically sized; no systematic dilution |
| Pattern diversity | 3 pattern families covering stocks and FX; reasonable breadth |

---

## 8. Ranked Improvement Opportunities

### Tier 1: High Impact, Supported by Evidence

**1. Extend simulation history to cover the full signal window**
- Currently, 73 of 83 signal days go untraded
- Expected impact: 7-8x more trading activity, dramatically more data for evaluation
- Evidence: Signal outcomes exist for the full period; only portfolio simulation is missing
- Confidence: HIGH -- this is a mechanical fix, not a strategy change

**2. Shift stock momentum holding horizon from 1-bar to 3-bar**
- STOCK_MOMENTUM_FAST Sharpe improves 66% (0.096 to 0.159)
- Avg per-signal return triples (0.15% to 0.47%)
- 52% of 1-bar exits show higher returns at 3 bars
- Evidence: 83+ signals per pattern across 5 months
- Confidence: MODERATE -- edge is directional but not statistically significant yet
- Note: FX_MOMENTUM_DAILY should remain at 1-bar (its optimal horizon)

**3. Increase position sizing or max positions to reduce cash drag**
- Current: 5% of cash per position, max 5 positions = max 25% exposure
- Parallel world confirms 1.5x sizing doubles PnL
- Even doubling to 10% per position would still leave 50%+ in cash
- Evidence: Cash drag averages 89.8%; winner/loser symmetry means larger size amplifies edge
- Confidence: MODERATE -- contingent on signal edge being real

### Tier 2: Moderate Impact, Requires Further Data

**4. Review score-based ranking for signal selection**
- Top-scored signals underperform lower-scored signals at 1 and 3-bar horizons
- Consider alternative ranking: recent hit rate, pattern Sharpe, or equal-weight random selection
- Confidence: LOW-MODERATE -- counterintuitive result may reflect small sample

**5. Monitor signal decay and consider pattern retraining cadence**
- Early window (Oct-Nov 2025) showed strong edge; recent window (Jan-Feb 2026) near zero
- May indicate regime change or parameter overfitting to initial training period
- Confidence: LOW -- only 3 time windows, need more data

**6. Evaluate promoting STOCK_MOMENTUM_SLOW 5-bar from WATCH to TRUSTED**
- This combination shows 0.31% avg return and 51.9% win rate
- Currently excluded from trading due to WATCH status
- Confidence: LOW-MODERATE -- 108 signals, but below 55% hit rate gate

---

## 9. Evidence That Current Trading Approximates Random Behavior

| Test | Result |
|------|--------|
| Portfolio 1 daily return t-test | t = 0.994, NOT SIGNIFICANT |
| Portfolio 2 daily return t-test | t = 0.270, NOT SIGNIFICANT |
| Portfolio 301 daily return t-test | t = 1.045, NOT SIGNIFICANT |
| Best pattern/horizon signal t-test (STOCK_MOMENTUM_FAST 3-bar) | t = 1.450, NOT SIGNIFICANT |
| Best win rate z-test (STOCK_MOMENTUM_FAST 1-bar) | z = 1.706, MARGINAL |

**Conclusion:** At conventional significance levels (p < 0.05), current portfolio returns and all individual signal returns are indistinguishable from zero. The win rate for STOCK_MOMENTUM_FAST at 1-bar is marginally significant (p ~ 0.09). More data is needed -- extending the simulation to the full signal history would roughly double sample sizes and may push the stock momentum 3-bar result across the significance threshold.

---

## 10. Structural Root Cause Diagram

```
Signal Edge (~0.15-0.47% per signal for stock momentum at 1-3 bar)
    |
    v
[Deployment Gap: 88% of signal days untraded]  --> ~8x dilution
    |
    v
[1-Bar Holding: exits before payoff matures]   --> ~3x dilution (vs 3-bar optimal)
    |
    v
[Cash Drag: only 10% of capital deployed]      --> ~4x dilution
    |
    v
Net Portfolio Impact: 0.03% daily return (statistically indistinguishable from zero)
```

Combined structural dilution: if the raw 3-bar stock signal edge is ~0.47%, the cascade of deployment gap, short holds, and low capital utilization reduces effective portfolio-level return by a factor of roughly 50-100x, yielding the observed ~0.03%/day.

---

## Appendix: Diagnostic Queries

All analytical queries are preserved in `MIP/SQL/smoke/edge_audit_diagnostic.sql` for reproducibility.
