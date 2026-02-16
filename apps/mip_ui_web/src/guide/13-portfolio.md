# 13. Portfolio

Deep dive into a single portfolio — its money, positions, trades, risk status, and historical performance. If the Cockpit is your morning summary, the Portfolio page is where you go for the full picture.

## Portfolio List View (Control Tower)

When you navigate to /portfolios without selecting one, you see the **Control Tower** — a table showing all portfolios at a glance.

| Column | What it means | Example |
|--------|---------------|---------|
| **Name** | Portfolio name (clickable link) | Main FX Portfolio |
| **ID** | Unique identifier | 1 |
| **Gate** | Risk regime: SAFE (green) = entries allowed, CAUTION (yellow) = approaching threshold, STOPPED (red) = entries blocked | SAFE |
| **Health** | Data freshness: OK (green) = recent run, STALE (yellow) = older than 24h, BROKEN (red) = very old or failed | OK |
| **Equity** | Latest total equity (cash + position value) | $102,450 |
| **Paid Out** | Cumulative profits withdrawn across all episodes | $5,200 |
| **Active Episode** | Current episode ID and start date | #3 since 2026-01-15 |
| **Status** | ACTIVE or CLOSED | ACTIVE |

## Portfolio Detail View

Click a portfolio to see the full detail page. Here's every section:

### Freshness Header

**CURRENT / PENDING UPDATE** — CURRENT = portfolio is simulated up to the latest market date. PENDING UPDATE = new market data exists but the pipeline hasn't run yet. The header also shows: "Simulated through 2026-02-07 · Pipeline ran at 14:30".

### Active Period Dashboard (Mini Charts)

Four small charts showing the current episode's performance at a glance:

- **Equity Chart** — Shows total portfolio value over time during this episode. The line goes up when positions gain value or profitable trades close, and down when positions lose value. Example: Starting at $100,000, the line might rise to $102,450 over 3 weeks.

- **Drawdown Chart** — Shows the percentage drop from the episode's peak equity. A drawdown of -3% means the portfolio has fallen 3% from its highest point. If drawdown exceeds the threshold (e.g., -5%), the risk gate switches to CAUTION or STOPPED.

- **Trades per Day** — Bar chart showing how many trades were executed each day. Most days will show 0-3 trades. A spike might indicate many positions closing at once.

- **Regime per Day** — Shows the risk regime (NORMAL / CAUTION / DEFENSIVE) for each day. Helps you see when the portfolio was in a restricted state.

### KPI Cards (Header)

Eight key numbers about the portfolio's lifetime performance:

| KPI | What it means | How it's calculated | Example |
|-----|---------------|---------------------|---------|
| **Starting Cash** | Initial capital when portfolio was created | Set when creating the portfolio | $100,000 |
| **Current Cash** | Cash available right now (not invested) | Starting cash + profits − invested amount | $87,234.50 |
| **Final Equity** | Total value: cash + all position values | Cash + sum(quantity × current_price) for each position | $102,450 |
| **Total Return** | Overall profit/loss as a percentage | (Final Equity − Starting Cash) / Starting Cash × 100 | +2.45% |
| **Max Drawdown** | Worst peak-to-trough decline ever | Largest % drop from any high to subsequent low | -4.20% |
| **Win Days** | Days where equity increased | Count of days where end-of-day equity > previous day | 42 |
| **Loss Days** | Days where equity decreased | Count of days where end-of-day equity < previous day | 18 |
| **Status** | ACTIVE or CLOSED | Set by the system | ACTIVE |

### Cash & Exposure Card

- **Cash** — Money not currently invested. Available for new positions. Example: $87,234.50
- **Exposure** — Total value of all open positions (quantity × current price). Example: $15,215.50
- **Total Equity** — Cash + Exposure = your total portfolio value. Example: $87,234.50 + $15,215.50 = $102,450.00

### Open Positions Table

Current holdings the portfolio has right now:

| Column | Meaning | Example |
|--------|---------|---------|
| **Symbol** | Which asset (stock, FX pair) | AAPL |
| **Side** | BUY (long) or SELL (short) | BUY |
| **Quantity** | How many shares/units held | 50 |
| **Cost Basis** | Average price paid when entering | $190.00 |
| **Hold Until (bar)** | Bar index when this position is scheduled to close | 1245 |
| **Hold Until (date)** | Calendar date when the position should close | 2026-02-15 |

> **Positions are sorted by "Hold Until" (soonest first).** This lets you see at a glance which positions are about to close.

### Recent Trades Table

Execution history — every buy and sell the portfolio has made:

| Column | Meaning | Example |
|--------|---------|---------|
| **Symbol** | Which asset was traded | AUD/USD |
| **Side** | BUY (opening long) or SELL (closing) | BUY |
| **Quantity** | How many units traded | 10,000 |
| **Price** | Execution price | 0.6520 |
| **Notional** | Total value of the trade (Price × Quantity) | $6,520.00 |

Use the **Lookback** dropdown (1 day, 7 days, 30 days, All) to control how far back the trade history goes. The "total" count shows how many trades exist in the selected window.

### Risk Gate Panel

The risk gate protects the portfolio from taking too much risk. It has three states:

| State | What it means | What happens |
|-------|---------------|--------------|
| **NORMAL** | Portfolio is within safe limits | New entries AND exits allowed |
| **CAUTION** | Drawdown is approaching the threshold | Entries may still be allowed but the system is watching closely |
| **DEFENSIVE** | Drawdown has breached the safety threshold | New entries BLOCKED. Only exits (closing positions) allowed. |

- **Reason Text** — Explains why the gate is in its current state. Example: "Episode drawdown at 4.2% (threshold: 5.0%). Approaching risk limit."
- **What to do now** — Actionable guidance. Example: "Wait for existing positions to close and drawdown to recover before new entries are opened."
- **Risk Strategy Rules** — The specific rules being enforced, e.g., "Episode drawdown stop: -5%", "Max concurrent positions: 10".

### Proposer Diagnostics

Why proposals may be zero or low — technical details about the proposal engine:

- **Raw Signals (latest bar)** — Total signals detected today across all patterns. Example: 15. This is before any filtering.
- **Trusted Signals** — Of those raw signals, how many came from TRUSTED patterns. Example: 3 (only these can become proposals).
- **Trusted Patterns** — How many distinct patterns currently have TRUSTED status. Example: 2.
- **Rec TS = Bar TS?** — Whether the recommendation timestamp matches today's bar date. "Yes" = everything is current. "No" = data may be stale.
- **Proposals Inserted** — How many proposals the engine actually created. Example: 1 (after capacity and duplicate checks).
- **Reason for Zero** — If zero proposals: the specific reason, e.g., "NO_TRUSTED_CANDIDATES" or "ENTRIES_BLOCKED".

### Cumulative Performance Section

Your investment journey across all episodes:

- **Total Paid Out** — Profits withdrawn at episode ends. When an episode closes profitably, the gains are "paid out." Example: $5,200 means the portfolio has withdrawn $5,200 in profits over its lifetime.
- **Total Realized P&L** — Cumulative profit/loss across all episodes. Green = overall profit, Red = overall loss. Example: $3,850 means the portfolio has earned $3,850 net across all closed trades.
- **Episodes** — How many "generations" the portfolio has been through. Each episode starts with fresh capital and a clean slate. Example: 3 episodes means the portfolio has been reset/restarted twice.
- **Cumulative Growth Chart (line chart)** — Two lines over time: Green line (Paid Out) = cumulative profits withdrawn. Blue line (Realized P&L) = cumulative profit/loss. The X-axis is time (dates), Y-axis is amount.
- **P&L by Episode (bar chart)** — One bar per episode showing its profit or loss. Green bars = profitable episodes. Red bars = losing episodes. Blue bar = the currently active episode. Click a bar to scroll down to that episode's detail card.

### Episodes Section

Expandable cards for each episode (generation) of the portfolio. Each card shows equity curves, drawdown charts, trade counts, and risk regime for that specific episode period. The active episode is highlighted.
