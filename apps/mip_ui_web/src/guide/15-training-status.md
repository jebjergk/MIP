# 15. Training Status

Monitor the learning process for every symbol/pattern combination. See which symbols are gathering evidence, which are close to earning trust, and which are already trade-eligible.

## Global Training Digest (top of page)

An AI-generated narrative covering training progress system-wide. Same format as the Cockpit (headline, what changed, what matters, waiting for) but focused entirely on training.

## Filters

Use the **Market Type** dropdown (FX, STOCK, etc.) and the **Symbol Search** input to narrow the table to specific assets.

## Training Table — Every Column Explained

| Column | What it means | Where the number comes from | Example |
|--------|---------------|----------------------------|---------|
| **Market Type** | Asset class (FX, STOCK, CRYPTO, etc.) | From the pattern definition | FX |
| **Symbol** | Specific asset being tracked | From the recommendation log | AUD/USD |
| **Pattern** | Which signal pattern is being evaluated | Pattern ID from pattern definitions | 2 |
| **Interval** | Bar interval in minutes (1440 = daily) | From pattern definition | 1440 |
| **As Of** | Date this training data was last updated | Timestamp of last pipeline run | 2026-02-07 |
| **Maturity** | Stage badge + score (0–100) with progress bar. See section 5 for stage definitions. | Calculated from sample size (30%) + coverage (40%) + horizons (30%) | CONFIDENT (82) |
| **Sample Size** | Total number of signals (recommendations) generated | Count of rows in RECOMMENDATION_LOG for this symbol/pattern | 45 |
| **Coverage** | What % of signals have been fully evaluated across horizons | Evaluated outcomes ÷ (signals × expected horizons) × 100 | 92% |
| **Horizons** | How many of the 5 evaluation windows have data | Count of distinct horizon_bars with outcomes | 5 |
| **Avg H1** | Average outcome return at the 1-bar horizon | Mean of realized_return for all outcomes at horizon_bars = 1 | +0.0032 |
| **Avg H3** | Average outcome return at the 3-bar horizon | Same calculation for horizon_bars = 3 | +0.0058 |
| **Avg H5** | Average outcome return at the 5-bar horizon | Same calculation for horizon_bars = 5 | +0.0081 |
| **Avg H10** | Average outcome return at the 10-bar horizon | Same calculation for horizon_bars = 10 | +0.0045 |
| **Avg H20** | Average outcome return at the 20-bar horizon | Same calculation for horizon_bars = 20 | -0.0012 |

### Reading Avg H Columns

The "Avg H5" column shows +0.0081 for AUD/USD. This means that, on average, 5 trading days after the pattern fired, the price had moved +0.81% in the right direction. Positive values are good — they indicate the pattern has historically been profitable at that horizon.

The "Avg H20" column shows -0.0012 — meaning at the 20-day horizon, the average return is slightly negative (-0.12%). This tells you the momentum doesn't persist that long for this particular symbol/pattern. The 5-bar horizon is the sweet spot.

## Expanded Row (click any row)

Clicking a row expands it to show two additional components:

- **Per-Symbol Training Digest** — An AI-generated narrative specific to this symbol/pattern. Describes what changed in training, whether it's close to trust, and what outcomes the system is waiting for. Same format as the global digest but focused on one symbol.

- **Training Timeline Chart** — A chart showing the training metrics over time for this symbol. Helps you see if the hit rate and average return are trending up (good) or down (concerning).
