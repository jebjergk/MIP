# 15. Training Status

Monitor the learning process for every symbol/pattern combination. See which symbols are gathering evidence, which are close to earning trust, and which are already trade-eligible.

## Daily / Intraday Toggle

The Training page has a **Daily (1440m)** / **Intraday (15m)** toggle at the top. This switches the entire table between daily and intraday training data. The columns, horizons, and expanded row charts all adapt to the selected interval.

## Global Training Digest (top of page — daily mode)

An AI-generated narrative covering training progress system-wide. Same format as the Cockpit (headline, what changed, what matters, waiting for) but focused entirely on training. This digest is shown in daily mode only.

## Intraday Mode — Cockpit Layout

When viewing intraday training, the page switches to a **decision-first cockpit** with three layers. See section 23 (The Intraday Subsystem) for full details. The key sections are:

- **Executive Summary** — status banner, pattern readiness tiles, compressed pipeline health
- **Pattern Trust Scoreboard** — compact view with one row per pattern, expandable for per-horizon detail
- **Signal Activity Chart** — collapsible price chart with signal overlays
- **Advanced Diagnostics** — pattern stability and excursion analysis (hidden by default)

## Filters (daily mode)

Use the **Market Type** dropdown (FX, STOCK, etc.) and the **Symbol Search** input to narrow the table to specific assets.

## Training Table — Column Reference

### Daily Mode (1440m)

| Column | What it means | Example |
|--------|---------------|---------|
| **Market Type** | Asset class (FX, STOCK, etc.) | FX |
| **Symbol** | Specific asset being tracked | AUD/USD |
| **Pattern** | Which signal pattern is being evaluated | 2 |
| **Interval** | Bar interval in minutes (1440 = daily) | 1440 |
| **As Of** | Date this training data was last updated | 2026-02-07 |
| **Maturity** | Stage badge + score (0–100). See section 5 for stage definitions. | CONFIDENT (82) |
| **Sample Size** | Total signals generated for this symbol/pattern | 45 |
| **Coverage** | % of signals fully evaluated across all horizons | 92% |
| **Horizons** | How many evaluation windows have data | 5 |
| **Avg H1** | Average return at the 1-day horizon | +0.0032 |
| **Avg H3** | Average return at the 3-day horizon | +0.0058 |
| **Avg H5** | Average return at the 5-day horizon | +0.0081 |
| **Avg H10** | Average return at the 10-day horizon | +0.0045 |
| **Avg H20** | Average return at the 20-day horizon | -0.0012 |

### Intraday Mode (15m) — Dynamic Horizons

Intraday horizon columns are different from daily. They come from the **HORIZON_DEFINITION** table and adapt automatically:

| Column | What it means | Example |
|--------|---------------|---------|
| **Avg H1** | Average return at +1 bar (15 minutes) | +0.0008 |
| **Avg H4** | Average return at +4 bars (~1 hour) | +0.0015 |
| **Avg H8** | Average return at +8 bars (~2 hours) | +0.0022 |
| **Avg EOD** | Average return at end-of-day close | +0.0031 |

### Reading Avg H Columns

For daily: "Avg H5" = +0.0081 means 5 trading days after the pattern fired, price moved +0.81% in the right direction on average. Positive = historically profitable.

For intraday: "Avg H4" = +0.0015 means ~1 hour after the intraday pattern fired, price moved +0.15% in the right direction on average.

## Expanded Row (click any row — daily mode)

Clicking a row expands it to show two additional components:

- **Per-Symbol Training Digest** — An AI-generated narrative specific to this symbol/pattern. Describes what changed in training, whether it's close to trust, and what outcomes the system is waiting for.

- **Training Timeline Chart** — A chart showing training metrics over time for this symbol. Helps you see if the hit rate and average return are trending up (good) or down (concerning). In daily mode, defaults to the 5-day horizon. In intraday mode, defaults to the +4 bar (~1hr) horizon.
