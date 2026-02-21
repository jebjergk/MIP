# 17. Decision Explorer

Understand **why trades happened — or didn't**. The Decision Explorer connects signals to portfolio actions and presents clear, readable explanations instead of raw data.

## Executive Summary

At the top of the page, a summary banner shows:

- **Total Signals** in scope for the selected date/run
- **Traded** — signals that became actual trades
- **Rejected** — signals that failed trust gating
- **Eligible · Not Selected** — signals that passed all gates but were not chosen

## Filters

Filter by symbol, market type, pattern, trust level, outcome category, pipeline run ID, or date. Filters can be set via URL parameters (often linked from Cockpit) or adjusted directly on the page.

## Decision Table — Every Column Explained

| Column | What it means | Example |
|--------|---------------|---------|
| **Symbol** | Which asset generated this signal | EUR/USD |
| **Pattern** | Which pattern definition detected this signal, plus market tag | 2 · FX |
| **Outcome** | What happened to this signal — color-coded pill | ✔ Traded, ✖ Rejected · Trust, ○ Eligible · Not Selected |
| **Trust** | Current trust label for this symbol/pattern combination | TRUSTED (green), WATCH (amber), UNTRUSTED (red) |
| **Score** | Signal strength — higher = stronger detection | 0.85 |
| **Signal Time** | When the signal was generated | 2026-02-07 14:30 |
| **Why** | One-line human-readable explanation of the outcome | "Trusted pattern — selected and executed" |

## Outcome Categories

Each signal is classified into exactly one outcome:

- **TRADED** — Passed trust gate, eligible, and selected for trading
- **REJECTED · TRUST** — Trust level is WATCH or UNTRUSTED; not eligible
- **ELIGIBLE · NOT SELECTED** — Passed all gates but was not chosen (e.g., ranked below alternatives or portfolio at capacity)

## Decision Trace (Expand a Row)

Click any row to expand a step-by-step decision trace:

1. **Signal Detected** — ✓ (always true if row exists) with score
2. **Trust Gate** — ✓ or ✗ with threshold comparison (score vs min, hit rate)
3. **Eligibility** — ✓ or ✗ derived from trust + recommended action
4. **Selection** — ✓ or ✗ (only shown for eligible signals)
5. **Final Decision** — Trade executed or not

The expanded view also shows **Supporting Metrics** (hit rate, avg return, coverage, horizon) and **Trade Details** (price, quantity, notional) when applicable.

## Advanced View

For power users: click **"Show raw policy JSON"** at the bottom of any expanded row to see the full gating reason object, including all policy thresholds and scoring details.

## Fallback Logic

If no signals match your filters for the current run, the system automatically tries a broader search. Results will include signals from a wider time window.
