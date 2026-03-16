# 17. Live Symbol Tracker

Route: `/symbol-tracker`

Live Symbol Tracker gives a symbol-first view of live-linked monitoring context.

## What you can verify here

- Which symbols are currently active and relevant.
- Per-symbol state changes that may affect attention.
- Monitoring signals that require follow-up in decision pages.

## Typical workflow

1. Identify symbols with elevated activity or warnings.
2. Drill into affected symbols to inspect context.
3. Cross-check decisions in AI Agent Decisions.
4. Validate supporting evidence in News Intelligence and Runs.

## When to use this page

- During intraday monitoring.
- When a symbol suddenly becomes high priority.
- Before reviewing a symbol-specific decision thread.

## Common labels on this page

### Thesis

`Thesis` is the current validity status of the trade idea based on live price vs risk and expectation context.

- `THESIS_INTACT`: live price is still inside expected behavior range.
- `WEAKENING`: setup quality is degrading (for example, price near stop or near expectation-band edge).
- `INVALIDATED`: setup has materially broken (for example, stop crossed or strong divergence from trained path).

### Open R

`Open R` is current open reward-to-risk multiple from entry, measured against stop distance.

Formula:

- `risk = entry - stop` (for LONG) or `stop - entry` (for SHORT)
- `reward = current - entry` (for LONG) or `entry - current` (for SHORT)
- `Open R = reward / risk`

Interpretation:

- `+1.0R`: current gain equals initial risk.
- `0.0R`: at entry (flat vs entry).
- negative `R`: currently losing relative to entry.

### Expected move reached

How much of the trained expected move has already been realized.

- Around `100%`: current move is near trained expectation.
- Above `100%`: current move exceeded trained expectation.
- Below `100%`: move has not yet reached trained expectation.

### Distance to TP / Distance to SL

Relative distance from current price to take-profit or stop-loss.

- `Distance to TP`: remaining upside/downside distance to target (side-aware).
- `Distance to SL`: remaining safety buffer to stop (side-aware).

### Position status badges

- `PROTECTED_FULL`: both TP and SL exist.
- `PROTECTED_PARTIAL`: only TP or only SL exists.
- `UNPROTECTED`: no TP/SL protection.
- `IN_PROFIT` / `UNDERWATER`: unrealized P&L sign.
