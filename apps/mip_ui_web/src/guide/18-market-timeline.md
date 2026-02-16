# 18. Market Timeline

End-to-end symbol observability. See every symbol as a tile showing signal, proposal, and trade counts. Click a tile to see the price chart with event overlays.

## Overview Grid

Each tile represents one symbol (e.g., AAPL, EUR/USD) and shows:

- **S: (number)** — Signals count — how many signals were detected for this symbol in the selected window (e.g., last 30 bars). Example: S:12 means 12 detections.
- **P: (number)** — Proposals count — how many of those became trade proposals. Example: P:3.
- **T: (number)** — Trades count — how many were actually executed. Example: T:1.
- **ACTION badge** — Highlighted in yellow if there are proposals TODAY that may require attention.
- **Trust badge** — TRUSTED / WATCH / UNTRUSTED label for this symbol.

## Tile Colors

| Color | Meaning |
|-------|---------|
| Green border | **Executed** — trades happened for this symbol |
| Orange border | **Proposed** — proposals exist but not yet executed |
| Blue border | **Signals only** — signals detected but no proposals |
| Grey | **Inactive** — no signals in this window |

## Filters

- **Portfolio:** Filter to signals/proposals/trades for a specific portfolio or "All."
- **Market:** Filter by market type (FX, STOCK, etc.).
- **Window:** How many bars of history to show (30, 60, 90, or 180 bars).

## Expanded Detail (click a tile)

Clicking a symbol tile expands it to show a detailed view:

- **Price Chart (OHLC)** — A line chart showing the close price (blue solid line) with high/low range (grey dashed lines). Event markers are overlaid on the chart:
  - Blue circles = Signal fired (pattern detected something)
  - Orange circles = Proposal generated (trade suggested)
  - Green circles = Trade executed (position opened)

- **Counts Bar** — Shows the signal → proposal → trade funnel: "12 signals → 3 proposals → 1 trade"

- **Decision Narrative** — Bullet points explaining what happened and why. Example: "Pattern 2 detected momentum, but risk gate was in CAUTION mode, blocking new entries."

- **Trust Status by Pattern** — Shows trust label and coverage for each pattern tracking this symbol.

- **Recent Events Table** — Last 20 events (signals, proposals, trades) with dates, types, and details like price, quantity, and portfolio links.
