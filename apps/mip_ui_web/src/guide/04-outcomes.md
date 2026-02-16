# 4. Outcome Evaluation (How Training Works)

Training is **not** teaching an AI model. It's building a track record. Every day, the system looks back at old signals and checks what actually happened in the market afterward.

## Timeline Example

- **Day 1 — Signal Fires:** Pattern detects momentum in AAPL. AAPL returned +1.2% today, above the pattern's 0.2% threshold. A signal is logged with score = 0.012. No trade happens yet — just a record. AAPL price at signal: $190.00.

- **Day 2 — 1-bar evaluation:** System checks: what happened 1 bar later? AAPL closed at $190.95 the next day — up +0.5% from $190.00. realized_return = +0.005, hit_flag = true (exceeded minimum).

- **Day 4 — 3-bar evaluation:** System checks: what happened 3 bars later? AAPL closed at $192.30 — up +1.2% from $190.00 over 3 days. realized_return = +0.012, hit_flag = true.

- **Day 6 — 5-bar evaluation:** System checks: what happened 5 bars later? AAPL closed at $193.99 — up +2.1% from $190.00 over 5 trading days. realized_return = +0.021, hit_flag = true.

- **Day 11 — 10-bar evaluation:** System checks: what happened 10 bars later? AAPL closed at $191.71 — up +0.9% from $190.00 over 10 days. realized_return = +0.009, hit_flag = true.

- **Day 21 — 20-bar evaluation:** Over 20 days, AAPL dropped to $189.43 — down -0.3% from entry at $190.00. realized_return = -0.003, hit_flag = false (below threshold).

- **Ongoing — Metrics accumulate:** Each evaluation feeds into training metrics. After many signals and evaluations, the system has a track record: "Out of 40 signals, 31 were hits (77% hit rate) with an average return of +0.81%."

## The Five Horizons

Every signal is evaluated at **5 different time windows** (called "horizons"):

| Horizon | Meaning | What it tells you |
|---------|---------|-------------------|
| **1 bar** | Next trading day | Very short-term reaction — did the momentum continue tomorrow? |
| **3 bars** | 3 trading days later | Short-term follow-through — did the move extend over a few days? |
| **5 bars** | 1 trading week later | The "standard" holding horizon — the main metric used for scoring. |
| **10 bars** | 2 trading weeks later | Medium-term — does the pattern have staying power? |
| **20 bars** | 1 trading month later | Longer-term — was this a meaningful trend or just a blip? |

> **This is backtesting in production.** The system generates signals every day, then evaluates them at multiple time horizons. Over weeks and months, this builds a statistically meaningful track record for each symbol/pattern combination.
