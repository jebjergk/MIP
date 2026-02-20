# 23. The Intraday Subsystem

MIP has two independent pipelines: the **Daily Pipeline** (the original system) and the **Intraday Pipeline** (a newer, additive subsystem). They share the same database and UI but run on separate schedules, separate cadences, and separate evaluation logic.

## Why Intraday?

Daily signals evaluate whether a pattern leads to profitable moves over days or weeks. But many patterns resolve within hours. The intraday subsystem captures these shorter-lived opportunities by:

- Ingesting **15-minute bars** for a focused symbol universe
- Detecting **intraday-specific patterns** (Opening Range Breakout, Pullback Continuation, Mean-Reversion Overshoot)
- Evaluating outcomes on **bar-based horizons** (+1 bar, +4 bars, +8 bars, end-of-day)
- Building trust independently of the daily system

## Key Differences from Daily

| Aspect | Daily | Intraday |
|--------|-------|----------|
| Bar interval | 1440 minutes (1 day) | 15 minutes |
| Horizons | 1, 3, 5, 10, 20 days | +1 bar (15m), +4 bars (~1hr), +8 bars (~2hr), EOD close |
| Patterns | Momentum, Reversal | ORB, Pullback Continuation, Mean-Reversion |
| Symbol universe | Full (23+ symbols) | Focused (14–16 high-liquidity symbols) |
| Trading | Active (proposals + execution) | Learning only (no live trading yet) |
| Pipeline schedule | Daily at 07:00 Berlin | Configurable (initially hourly during market hours) |

## Intraday Learning Loop

The intraday pipeline follows the same philosophy as daily — **learn before you trade**:

1. **Ingest** 15-minute bars from Alpha Vantage (delayed data)
2. **Detect patterns** using deterministic intraday detectors
3. **Log signals** as hypotheses (not trades)
4. **Evaluate outcomes** at intraday-relevant horizons
5. **Score reliability** — patterns must pass trust gates before they become tradable
6. **Repeat** — each pipeline run adds more evidence

## The Three Intraday Patterns

### Opening Range Breakout (ORB)
Detects when price breaks above or below the early-session trading range. The "opening range" is defined by the first few bars of the session. A breakout with sufficient distance from the range boundary generates a signal.

### Pullback Continuation
Identifies an impulse move (strong directional bar), followed by a consolidation (contracted range), and then a breakout from that consolidation in the original direction. Captures mid-trend continuation opportunities.

### Mean-Reversion Overshoot
Detects extreme deviation from a short-term anchor (rolling average). When price overshoots by a configurable threshold, a reversion signal fires. Provides diversification against trend-following patterns.

## Intraday Horizons

Unlike daily horizons (measured in days), intraday horizons are measured in **bars** or **session events**:

| Horizon | Label | Meaning |
|---------|-------|---------|
| +1 bar | H1 | Immediate signal validation — did the next 15-min bar confirm? |
| +4 bars | H4 | ~1 hour continuation — did the move extend? |
| +8 bars | H8 | ~2 hour persistence — does the pattern have staying power? |
| EOD close | EOD | End-of-day session impact — did it matter by market close? |

The **HORIZON_DEFINITION** table stores metadata for both daily and intraday horizons, allowing the system to interpret them consistently while scaling by timeframe.

## Intraday Training Status (Cockpit View)

The Intraday Training page is a decision-first cockpit with three layers:

### Layer 1: Executive Summary (top of page, no scroll needed)
- **Status Banner** — Shows the system stage (INSUFFICIENT / EMERGING / LEARNING / CONFIDENT), total signal count, evaluated outcomes, symbol count, and how many patterns are tradable
- **Pattern Readiness Tiles** — One card per pattern family showing event count, trust level, confidence, best fee-adjusted edge, and a trend arrow
- **Pipeline Health Strip** — Compact single-line summary: enabled/disabled, last run, bars ingested, signals generated, runs/7d, compute cost

### Layer 2: Pattern Insights (visible by default)
- **Trust Scoreboard** — Compact table with one row per pattern showing its best horizon. Click to expand and see all horizons. Sorted by Sharpe-like metric (best first)
- **Signal Activity Chart** — Collapsible price chart with signal overlays per symbol

### Layer 3: Deep Diagnostics (collapsed by default)
- **Pattern Stability** — Compares all-time vs recent performance to detect drift
- **Excursion Analysis** — Max favorable/adverse excursion data for stop-loss/take-profit design
- Toggle "Show advanced diagnostics" to reveal these sections (persisted in browser)

## Intraday in the Audit Viewer

The Runs (Audit) page has a **Daily Pipeline / Intraday Pipeline** toggle at the top. When set to "Intraday Pipeline", it shows intraday pipeline runs with their own metrics: bars ingested, signals generated, outcomes evaluated, and compute time.

## Intraday in the Live Header

The top-of-screen live header shows both **Daily** and **Intraday** last pipeline run status, so you can see at a glance whether both pipelines are healthy.

## Data Isolation

The intraday subsystem is fully isolated from daily:

- It does **not** modify daily signals, training, or execution
- It does **not** write into daily outcome tables
- It **may** read daily signals as directional context (read-only)
- It has its own Snowflake task, its own pattern definitions, and its own trust scoring
- Disabling intraday has zero impact on the daily pipeline

## Current Stage

The intraday subsystem is in **learning mode** — it detects patterns, evaluates outcomes, and builds trust. It does not yet propose or execute trades. Once patterns earn sufficient trust, a future phase will enable intraday paper trading.
