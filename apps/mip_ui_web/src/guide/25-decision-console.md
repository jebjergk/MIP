# 25. Decision Console

The Decision Console is MIP's real-time command center for monitoring open positions and the decisions being made about them. Think of it as a flight control tower — you can see every aircraft (position), its status, and every decision the system is making moment by moment.

## Three Modes

### Open Positions

The default view. Shows every open daily position with:

- **Symbol and portfolio** — which stock, in which portfolio
- **Stage badge** — the position's current state:
  - 🟢 **On Track** — position is open, target not yet reached
  - 🟡 **Candidate** — target return achieved, watching for reversal
  - 🟡 **Watching** — significant giveback detected, monitoring closely
  - 🔴 **Exit Triggered** — both stages passed, exit signal fired
  - 🔴 **Exited** — position was closed via early exit
- **Metrics** — current return vs target, distance to target, time in trade, max favorable excursion (MFE)
- **Pin** — star a symbol to keep it at the top of the list

Positions are sorted by severity — exit-triggered positions float to the top, on-track positions sit at the bottom.

### Live Decisions

A rolling event feed that updates via **Server-Sent Events (SSE)** — no manual refresh needed. Each event appears as a "story card":

- **Green cards** — routine monitoring, position on track
- **Yellow cards** — payoff reached, position is a candidate for early exit
- **Red cards** — exit triggered or executed

Each card shows a concise summary ("XOM hit target +0.60% in 11 min → EARLY EXIT CANDIDATE"), key metrics, and a "View trace" button to inspect the full decision chain.

Auto-scroll keeps the newest events at the top. Toggle it off if you want to read through the history.

### History

Same card format as Live Decisions, but for past events. Use the date picker to replay what happened on any previous trading day. Useful for reviewing decisions after market close.

## Position Inspector

Click any position or event to open the inspector panel on the right. It shows:

### Decision Diff

The "smart assistant" view — compares what happens if you exit now versus holding to horizon:

- **Exit Now** — current return and P&L if the position were closed right now
- **Hold (Expected)** — the target return and expected P&L based on the pattern's historical average
- **Delta** — the difference, highlighted green (exit is better) or red (holding is better)
- **Bars remaining** — how many daily bars are left until the planned exit

### Gate Trace Timeline

A vertical timeline showing every evaluation the system has performed on this position:

- Each node shows the **timestamp**, **stage**, and **decision type**
- **Gate pills** show pass/fail for each check (Payoff reached? Giveback triggered? No new highs?)
- Click **Advanced** on any node to see the raw JSON with all metrics and reason codes

This is full traceability — you can reconstruct exactly why any decision was made, what data was used, and when.

## KPI Strip

The top of the page shows three headline numbers:

- **Open** — total open daily positions being monitored
- **Candidates** — positions that have reached their payoff target
- **Exited** — positions closed via early exit

These update with each data refresh.

## Filtering

Use the dropdown filters to focus on:

- **Portfolio** — show only one portfolio's positions
- **Symbol** — show only one symbol across all portfolios

Filters apply to all three modes.

## Connection Status

The green dot next to the page title indicates whether the live stream is connected. If it goes grey, the system will attempt to reconnect automatically. The page remains fully functional in all modes even when the stream is disconnected — it falls back to periodic polling.

## Cost Control

The Decision Console polls Snowflake every **30 minutes** (not continuously) to keep warehouse costs minimal. Since the intraday pipeline itself runs hourly, this gives you fresh data within one polling cycle of any new evaluations.
