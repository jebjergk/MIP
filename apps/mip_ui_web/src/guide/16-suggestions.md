# 16. Suggestions

Ranked list of trading candidates based on historical performance. These are NOT predictions — they're ranked by which symbol/pattern pairs have the best track record.

## Two Tiers of Candidates

| Tier | Requirement | Meaning |
|------|-------------|---------|
| **Strong Candidates** | Sample size ≥ 10, Horizons ≥ 3 | Enough data for reasonable confidence. These are the primary candidates. |
| **Early Signals** | Sample size ≥ 3, Horizons ≥ 3 | Very early data — treat with caution. Shown with a "Low confidence" badge. |

## What Each Suggestion Card Shows

- **Rank (#1, #2, ...)** — Position in the ranking, ordered by Suggestion Score (highest first).

- **Symbol / Market / Pattern triple** — The specific combination being ranked, e.g., "AAPL / STOCK / pattern 2".

- **Suggestion Score** — A transparent, deterministic score combining three factors:
  ```
  Score = 0.6 × maturity_score + 0.2 × (mean_return × 1000) + 0.2 × (pct_positive × 100)
  ```
  Example: Maturity 80, mean_return 0.008 (0.8%), pct_positive 0.75 (75%): Score = 0.6 × 80 + 0.2 × (0.008 × 1000) + 0.2 × (0.75 × 100) = 48 + 1.6 + 15 = **64.60**

- **Sample Size** — Number of signals evaluated. Higher = more evidence. Example: 45.

- **Maturity Stage + Bar** — Stage badge (CONFIDENT, LEARNING, etc.) with a progress bar showing the score (0–100).

- **What History Suggests** — Two plain-language lines summarizing the historical evidence.
  - Line 1: "Based on 45 recommendations and 40 evaluated outcomes, positive at 75.0% over the strongest horizon (5 bars)."
  - Line 2: "Mean return at that horizon: 0.81%."

- **Horizon Strip** — Five mini-bars (sparkline) representing the five horizons (1, 3, 5, 10, 20 bars). Below each bar is the percentage — either pct_positive or mean_return. Example: 1d: 62.5% | 3d: 68.0% | 5d: 75.0% | 10d: 55.0% | 20d: 42.0%. This tells you the pattern works best at the 5-day horizon (75% positive).

- **Effective Score (early signals only)** — For early signals, the score is penalized for small sample size:
  ```
  Effective Score = Score × min(1, recs_total / 10)
  ```
  Example: Score 50 with only 5 samples: Effective = 50 × (5/10) = **25.0**

## Evidence Drawer (click a card)

Click any card to open a detailed evidence panel with charts and data:

- **Horizon Strip Charts (bar charts)** — Three bar charts showing performance across all 5 horizons:
  - **Average Return:** Mean return at each horizon. Bars above 0% are profitable.
  - **% Positive:** What fraction of outcomes were positive. Above 50% means more winners than losers.
  - **Hit Rate:** What fraction exceeded the minimum threshold. Similar to % positive but against the threshold.

- **Distribution Histogram** — A histogram of all realized returns for the selected horizon. Shows the shape of the return distribution. The red dashed line is the mean, and the blue dashed line is the median. Use the horizon selector buttons (1, 3, 5, 10, 20 days) to switch between horizons. A healthy pattern has most returns on the positive side, with the mean clearly above zero.

- **Confidence Panel** — Shows maturity score (progress bar), coverage ratio (%), and reason strings explaining the maturity assessment.

- **Data Table** — Raw numbers for each horizon: N (sample size), Mean realized return, % positive, % hit, Min return, Max return. All returns are in percentage format.
