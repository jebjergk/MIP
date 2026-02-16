# 5. Training Stages

Every symbol/pattern is assigned a **maturity score** (0–100) based on three factors: sample size, outcome coverage, and horizon completeness. The score determines the training stage:

| Stage | Score Range | Description |
|-------|-------------|-------------|
| **INSUFFICIENT** | 0–24 | Not enough data yet. Maybe only 5 signals have been generated. The system needs at least 30–40 before it can judge quality. No trading possible. |
| **WARMING UP** | 25–49 | Some data exists — maybe 10-20 signals with partial outcome evaluations. The system is collecting evidence but it's early days. |
| **LEARNING** | 50–74 | Enough data to start judging quality — maybe 25+ signals with outcomes across most horizons. Metrics are becoming statistically meaningful. |
| **CONFIDENT** | 75–100 | Strong evidence — 40+ signals, outcomes across all 5 horizons. If it also passes trust rules, it becomes trade-eligible. |

## What Makes Up the Maturity Score?

The maturity score (0-100) is calculated from three components:

- **30% — Sample Size:** How many signals have been generated. Needs at least 40 for full marks (30 pts). Example: 25 signals → 25/40 × 30 = **18.75 points**

- **40% — Coverage:** What fraction of signals have been evaluated across horizons. 100% means every signal has outcomes for all time windows. Example: 80% coverage → 0.80 × 40 = **32.0 points**

- **30% — Horizons:** How many evaluation windows (1, 3, 5, 10, 20 bars) have data. All 5 = full horizon coverage. Example: 4 of 5 horizons → 4/5 × 30 = **24.0 points**

### Full Calculation Example

AUD/USD with FX_MOMENTUM_DAILY: 25 signals, 80% coverage, 4 of 5 horizons.

Score = 18.75 + 32.0 + 24.0 = **74.75 → LEARNING stage**

To reach CONFIDENT (75+), it needs either more signals (pushing past 30 of 40) or the 5th horizon to start populating (which happens after 20 bars pass from the earliest signals).
