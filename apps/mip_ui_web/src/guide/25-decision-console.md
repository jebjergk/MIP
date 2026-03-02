# 25. Decision Console

The Decision Console is MIP's position-monitoring control center. It combines open-position state, live event flow, and full decision traceability.

## Three Modes

### Open Positions

Default mode, optimized for portfolio monitoring.

- **Grouped by symbol** — one symbol header, then portfolio rows underneath
- **Pinned symbols** — star a symbol to keep it near the top
- **Stage badge per portfolio row**:
  - 🟢 **On Track**
  - 🔴 **Exit Triggered**
  - 🔴 **Exited**
- **Portfolio-row metrics** — current vs target, distance, time in trade, MFE, news badge/age
- **Inline inspector** — clicking a row expands trace content directly below that row

### Live Decisions

Live event feed via **Server-Sent Events (SSE)**.

- Event cards show severity, summary, key metrics, and trace entry point
- Auto-scroll can be toggled on/off
- Designed for "what changed right now?" monitoring

### History

Historical replay of decision events with the same event card model.

- Filter by date, portfolio, symbol
- Useful for after-hours audit/review

## Position Inspector (Inline or Side Panel)

In Open Positions mode, inspector opens inline under the selected row.
In Live/History mode, inspector opens in the right pane.

Inspector order:

1. **News Context** — badge, count, freshness/staleness, snapshot timestamps, headlines
2. **Decision Diff** — exit-now vs hold expected comparison
3. **State Summary** — first hit, MFE, last evaluated, exit-fired
4. **Gate Trace Timeline** — full event-by-event timeline with Advanced JSON

Gate pills focus on:
- **Threshold** (trigger target and multiplier)
- **MFE**

## KPI Strip

Top strip currently shows:

- **Open** — total open daily positions monitored
- **Exited** — positions closed via early-exit logic

## Filters

Filters apply across all three modes:

- **Portfolio**
- **Symbol**
- **Date** (History mode)

## Refresh & Connectivity

- Open Positions refreshes every **15 minutes**
- Live stream uses SSE with reconnect fallback
- If stream disconnects, page remains usable and refresh logic continues
