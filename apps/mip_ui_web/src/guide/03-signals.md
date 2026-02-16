# 3. How Signals Are Generated

A **signal** is not a prediction — it's a detection. Each "pattern" is a set of rules that looks for specific price behavior. When the rules match, a signal is logged.

## Example: FX Pattern (Moving-Average Crossover)

The **FX_MOMENTUM_DAILY** pattern for AUD/USD checks: "Did today's return exceed 0.1% AND is the price above both the 10-bar and 20-bar moving averages?"

- If **yes** → a signal is logged with the observed return as its score.
- If **no** → nothing happens. No signal, no record.

Today AUD/USD closed at 0.6520, up from yesterday's 0.6502.
Daily return = (0.6520 − 0.6502) / 0.6502 = **+0.277%**.
The 0.277% exceeds the pattern's 0.1% minimum → signal fires with score 0.00277.

## Example: STOCK Pattern (Breakout + Momentum)

The **STOCK_MOMENTUM_FAST** pattern for AAPL checks three conditions simultaneously:

1. **Minimum return:** Did today's return exceed 0.2%?
2. **Momentum confirmation (slow_window=1):** Was yesterday also a green (positive) day?
3. **Breakout (fast_window=5):** Is today's close at a new 5-day high?
4. **Z-score ≥ 1.0:** Is today's move at least 1 standard deviation above the recent average (using 5-day volatility)?

All four must be true simultaneously. This means the stock must be on consecutive green days, breaking out to new short-term highs, with an unusually large move.

AAPL closed at $195.00 today, up from $193.50 yesterday (return = +0.78%).
Yesterday was also green (up from $192.00 → +0.78%). ✓ momentum check passed.
The highest close in the last 5 days was $194.20. Today's $195.00 exceeds it. ✓ breakout passed.
5-day return std dev = 0.5%. Z-score = 0.78% / 0.5% = 1.56. ✓ z-score ≥ 1.0.
→ Signal fires with score 0.0078.

## Key Parameters in Each Pattern

- **min_return** — Minimum observed return to fire a signal (e.g., 0.001 = 0.1%). Filters out tiny, insignificant moves. If a stock moved 0.01%, that's noise — not a signal. Example: If min_return = 0.002 (0.2%), then AAPL going up 0.15% today would NOT fire a signal. AAPL going up 0.35% WOULD fire one.

- **min_zscore** — Minimum z-score — how unusual the move is compared to the symbol's recent volatility. A 0.5% move might be huge for a stable stock but normal for a volatile one. Z-score adjusts for this. Z-score = today's return ÷ standard deviation of returns over the fast_window period. Example: If AAPL's return std dev over the last 5 bars is 0.4% and today's return is 0.78%, then z-score = 0.78 / 0.4 = **1.95**. With min_zscore = 1.0, this signal fires. A move of only 0.3% (z-score = 0.75) would not.

- **fast_window and slow_window** — **Warning: These names are misleading.** They originate from a moving-average crossover concept, but in the STOCK/ETF patterns they are repurposed for different filters:

  | Parameter | FX Patterns | STOCK / ETF Patterns |
  |-----------|-------------|----------------------|
  | **slow_window** | Slow moving average — the longer lookback (e.g., 20 bars). Price must be above this average. | Momentum confirmation — the shorter lookback (e.g., STOCK=1, ETF=3). The system checks the last N bars before today and requires all N to have positive returns (green days). |
  | **fast_window** | Fast moving average — the shorter lookback (e.g., 10 bars). Price must be above this average. | Breakout + volatility window — the longer lookback (e.g., STOCK=5, ETF=20). Two uses: 1) Breakout: Today's close must exceed the highest close in the prior N bars. 2) Z-score: The standard deviation of returns over the last N bars is used to calculate how unusual today's move is. |

  **Why the naming is counterintuitive:** For STOCK/ETF, "slow" is actually the shorter window and "fast" is the longer one. This is an artifact of the codebase reusing the same parameter names for a different algorithm.

- **lookback_days** — How many days of history to use for computing z-scores and statistics (e.g., 90 days). Determines "normal" for this symbol.

> **Important:** Signals that fire are not automatically traded. They enter the training pipeline first. Only signals from TRUSTED patterns can become trade proposals.
