# Key Terms Glossary

| Term | Definition |
|------|------------|
| **Signal** | A detection by a pattern that interesting price action occurred. Not a trade — just a record. |
| **Pattern** | A named strategy with specific parameters (min_return, min_zscore, etc.) that scans market data for signals. |
| **Horizon** | A time window (1, 3, 5, 10, or 20 bars) at which the system evaluates what happened after a signal. |
| **Hit Rate** | Percentage of evaluated outcomes that were favorable. Threshold: ≥ 55%. |
| **Avg Return** | Average realized return across all outcomes. Threshold: ≥ 0.05% (0.0005). |
| **Maturity Score** | A 0–100 score measuring data quality: 30% sample size + 40% coverage + 30% horizons. |
| **Trust Label** | TRUSTED / WATCH / UNTRUSTED. Determined by passing 3 gates: sample ≥ 40, hit rate ≥ 55%, avg return ≥ 0.05%. |
| **Proposal** | A suggested trade order generated when a TRUSTED signal passes risk/capacity checks. |
| **Risk Gate** | Safety mechanism that blocks new entries when portfolio drawdown exceeds a threshold. |
| **Episode** | A "generation" of the portfolio. Starts at creation or after crystallization/profile change. All KPIs and performance numbers are scoped to the active episode. |
| **Crystallization** | The process of locking in gains when a profit target is hit. Ends the current episode and starts a new one. Two modes: Withdraw Profits (pay out gains) or Rebase (compound gains into new cost basis). |
| **Lifecycle Event** | An immutable record of a portfolio state change — CREATE, DEPOSIT, WITHDRAW, CRYSTALLIZE, PROFILE_CHANGE, EPISODE_START, EPISODE_END, or BUST. Stored permanently for audit and timeline views. |
| **Risk Profile** | A reusable template defining portfolio behavior: position limits, drawdown stops, bust threshold, and crystallization rules. Attached to portfolios and can be changed at any time (which starts a new episode). |
| **Pipeline Lock** | A safety mechanism that disables all portfolio editing while the daily pipeline is running. Prevents data conflicts. Buttons re-enable automatically once the pipeline completes. |
| **Deposit / Withdraw** | Cash events that add or remove money from a portfolio without affecting P&L tracking. The system adjusts the cost basis so deposits aren't counted as profit and withdrawals aren't counted as losses. |
| **Drawdown** | The percentage decline from a portfolio's peak equity. -5% drawdown = 5% below the high water mark. |
| **Cortex AI** | Snowflake's built-in LLM service used to generate narrative digests and portfolio stories from snapshot data. |
| **Pipeline** | The daily automated process: fetch data → detect signals → evaluate outcomes → update trust → trade → check crystallization → generate digest. |
| **Z-Score** | How many standard deviations a value is from the mean. Z-score of 2 means the move is unusually large (2σ above average). |
| **Coverage Ratio** | What fraction of signals have been fully evaluated across all horizons. 100% = complete evaluation. |
| **Notional** | The total monetary value of a trade: Price × Quantity. A buy of 100 shares at $150 = $15,000 notional. |
| **Cost Basis** | The average price at which a position was entered, adjusted for deposits and withdrawals. Used to calculate unrealized profit/loss. |
| **Portfolio Story** | An AI-generated narrative biography of a portfolio — covering its creation, cash events, episodes, crystallizations, and current outlook. Found in Portfolio Management → Portfolio Story tab. |
| **Parallel Worlds** | A read-only "what-if" analysis system. Replays each day's market data through alternative rule sets (scenarios) and compares their outcomes to your actual results. Never affects your real portfolio. |
| **Scenario** | An alternative set of trading rules used in Parallel Worlds. Examples: "Looser Signal Filter," "Bigger Positions (125%)," "Wait 1 Day Before Entering," "Stay in Cash." Each scenario produces a counterfactual PnL. |
| **Counterfactual** | The hypothetical outcome that would have occurred under different rules. "Counterfactual PnL of $142" means the scenario would have made $142 that day, compared to your actual result. |
| **Regret** | The dollar amount by which a scenario outperformed your actual result, summed over time. Only positive differences count — days you beat the scenario don't reduce regret. |
| **Confidence Class** | A reliability tier assigned to each scenario: Strong (reliable outperformance), Emerging (pattern forming), Weak (inconsistent), or Noise (no meaningful signal). Based on win-rate, cumulative impact, and rolling consistency. |
| **Decision Trace** | A human-readable record of what happened at each decision gate (Trust, Risk, Threshold, Sizing, Timing) during a scenario's simulation. Shows which gates passed, blocked, or modified trades, and explains why in plain English. |
| **Policy Health** | An at-a-glance assessment of whether your current trading rules are optimal. Combines confidence signals, regret attribution, and stability into a single health rating: Healthy, Watch, Monitor, Review Suggested, or Needs Attention. |
| **Stability Score** | A 0–100 score measuring how "settled" your trading rules are. 100 = every alternative is noise (very stable). Lower scores mean more scenarios are showing signal. |
| **Parameter Sweep** | A systematic test that replays trading history with dozens of slightly different settings (e.g., z-score thresholds from -0.50 to +0.50 in small steps). Used in the Signal Tuning and Portfolio Tuning tabs to build tuning surfaces. |
| **Tuning Surface** | A chart showing cumulative PnL impact across a range of parameter values. The "surface" lets you see which setting would have produced the best results — like a topographic map of profitability. |
| **Optimal Point** | The single parameter setting that would have produced the highest cumulative PnL improvement across all observed days. Shown as a green marker on tuning surface charts. |
| **Minimal Safe Tweak** | The smallest parameter change from your current setting that still improves performance. A conservative recommendation — the least disruptive improvement. |
| **Regime Sensitivity** | An analysis of how a parameter setting performs across different market volatility environments: Quiet (low-vol), Normal, and Volatile (high-vol). Settings that only work in one regime are flagged as "fragile." |
| **Regime Fragile** | A scenario or parameter setting that only outperforms in one market volatility regime (e.g., only in calm markets). Flagged as a safety concern because it may fail when conditions change. |
| **Safety Check** | An automated verification that a recommendation passes minimum reliability thresholds: enough observation days, stable trade counts, and multi-regime robustness. All three must pass for a recommendation to be marked "Ready for Review." |
| **Recommendation** | A tuning suggestion generated from sweep results. Comes in two flavors: Conservative (smallest helpful change) and Aggressive (optimal setting). Always requires human approval — never auto-applied. |
