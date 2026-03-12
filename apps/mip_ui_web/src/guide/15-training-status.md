# 15. Training Status

This page answers one question: **"How much evidence do we have for each symbol + pattern?"**

If Cockpit is the morning newspaper, Training Status is the research notebook.

## What This Page Shows Today

Training Status currently shows the **daily (1440m)** training table.

Use this page to:
- see maturity and trust-readiness by symbol/pattern
- inspect sample size and coverage quality
- compare average outcomes across horizons (H1, H3, H5, H10, H20)

## Filters

- **Market Type**: Narrow to STOCK / ETF / FX
- **Symbol Search**: Jump directly to one symbol

## Column Guide (Plain English)

| Column | What it means |
|--------|----------------|
| **Market Type** | Asset class (for example FX or STOCK) |
| **Symbol** | The instrument being evaluated (for example AUD/USD) |
| **Pattern** | Which detector generated the historical signals |
| **Interval** | Timeframe in minutes (1440 means daily bars) |
| **As Of** | Last date this row was updated |
| **Maturity** | Learning stage + score (0-100) |
| **Sample Size** | Number of observed signals for this symbol/pattern |
| **Coverage** | How much of those signals already have evaluable outcomes |
| **Horizons** | How many forward windows are tracked |
| **Avg H1/H3/H5/H10/H20** | Average return after each horizon window |

## How To Read Horizon Columns

- **Avg H1** = average move after 1 day
- **Avg H5** = average move after 5 days
- Positive values mean historical edge in the expected direction

Example: if **Avg H5 = +0.008**, that means roughly **+0.8% average** after 5 days for that pattern/symbol.

## How To Use This In Practice

1. Start with rows that have **higher sample size + high coverage**.
2. Then check whether average returns are consistently positive across multiple horizons.
3. Use Cockpit and Decision pages to see whether those trained patterns are now producing proposals.

## Common Misread (Important)

High maturity does **not** mean "guaranteed next trade wins."
It means: "historically, with enough evidence, this behavior has looked reliable."
