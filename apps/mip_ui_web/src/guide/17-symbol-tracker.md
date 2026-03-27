# 17. Living Chart

Routes: `/symbol-tracker` (primary) · `/living-chart` (alias)

Living Chart is a **chart-first companion** to the [Live Intelligence Cockpit](/live-intelligence). It shows **one open position at a time** with price, entry / stop / target, expected path (and optional band), and a small set of **visual state cues** (for example near stop, near target, thesis pressure). Use the cockpit for multi-symbol reasoning and recommendations; use Living Chart to **observe** the tape for the symbol in focus.

From the cockpit, open **Chart** on a tile to jump here with `?symbol=` in the URL.

## Data refresh

- **Reload context** loads Snowflake-backed tracker tiles once (bootstrap).
- **Refresh live** and the automatic interval merge **IB live bars** only — no ongoing warehouse polling for chart updates.

## Controls

- **Follow latest**: keeps the time window anchored on the latest bar until you pan or zoom; then use **Snap to latest** to re-anchor.
- **More context (VWAP, BB, S/R)**: optional technical overlays (off by default to avoid clutter).

## Labels you may see

### Thesis

Same meaning as elsewhere: validity of the trade idea vs live price and expectation context (`THESIS_INTACT`, weakening states, `INVALIDATED`, etc.).

### Distance to TP / Distance to SL

Side-aware relative distance from last price to take-profit or stop-loss (also surfaced as compact zones on the chart when close).

### Expected path / band

Trained forward median and band stitched forward from the last bar for comparison to actual price — not a guarantee of future price.
