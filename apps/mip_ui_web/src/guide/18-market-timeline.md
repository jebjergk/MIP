# 18. Market Timeline

End-to-end symbol observability. See every symbol as a tile showing signal, proposal, and trade counts. Click a tile to see the price chart with event overlays and a signal chain tree.

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

- **Portfolio:** Filter to a specific portfolio. When selected, only symbols where that portfolio has proposals or trades are shown — not all symbols, just the ones relevant to that portfolio. Portfolios are loaded dynamically from the system.
- **Market:** Filter by market type (FX, STOCK, etc.).
- **Window:** How many bars of history to show (30, 60, 90, or 180 bars). This also determines how far back signal chains look.

## Expanded Detail (click a tile)

Clicking a symbol tile expands it to show a detailed view:

### Chart Mode Toggle

A **Line / Candlestick** toggle lets you switch between two chart styles:

- **Line mode** (default) — Close price as a blue line with high/low range shown as grey dashed lines. Clean and calm.
- **Candlestick mode** — Traditional OHLC candles. Green bodies for up days, red bodies for down days, with wicks showing the full high-low range.

Both modes show the same event markers overlaid on the chart:
- **Blue circles** = Signal fired (pattern detected something)
- **Orange circles** = Proposal generated (trade suggested)
- **Green circles** = Trade executed (position opened)

The chart always extends to today's date, even if market data hasn't arrived yet, so you can see today's activity (signals, proposals, trades) on the chart.

### Counts Bar

Shows the signal → proposal → trade funnel: "12 signals → 3 proposals → 1 trade"

### Decision Narrative

Bullet points explaining what happened and why. Example: "Pattern 2 detected momentum, but risk gate was in CAUTION mode, blocking new entries."

### Trust Status by Pattern

Shows trust label and coverage for each pattern tracking this symbol.

### Signal Chains (Tree View)

The signal chain tree shows the full lifecycle of each signal as an indented tree structure. Each signal branches into its proposals across portfolios, then into trades:

```
Signal  2026-02-18  Pattern 2  Score 0.0068
  ├── BUY Proposal  2026-02-19  Portfolio 1  Weight 0.05
  │     └── BUY  2026-02-19  20.46 × $245.04 ($5014)  Portfolio 1  Open
  └── BUY Proposal  2026-02-19  Portfolio 2  Weight 0.05
        └── BUY  2026-02-19  0.33 × $244.60 ($80)  Portfolio 2
              └── SELL  2026-02-17  0.33 × $243.28  +$0.44  Closed
```

- **Signal** (level 0): The root — pattern ID, score, and date when the signal fired.
- **Proposal** (level 1): Each portfolio that received a proposal, with the portfolio link (clickable), side (BUY), weight, and status. A single signal can branch into multiple proposals for different portfolios.
- **BUY Trade** (level 2): Executed buy with quantity, price, notional value, and portfolio link.
- **SELL Trade** (level 3): Executed sell with quantity, price, realized PnL (green for profit, red for loss), and a "Closed" badge.

Status badges on the tree:
- **Open** (blue, pulsing) — Position is currently held.
- **Closed** (green) — Round-trip complete (bought and sold).
- **Pending** (orange) — Proposal exists but not yet executed.
- **Rejected** (red) — Proposal was rejected.

Signals that never led to a proposal ("signal-only") are not shown individually but are counted in the header as "· N signal-only."
