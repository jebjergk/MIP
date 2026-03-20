# 29. Live Portfolio Activity

Route: `/live-portfolio-activity`

Live Portfolio Activity is the operational timeline for live-linked paper workflow.

It connects import-time proposal sourcing, validation checks, approvals, and execution lifecycle outcomes.

## What you can verify here

- Current live-paper portfolio state and recent activity
- Actions moving through validation, compliance, and execution lifecycle
- Transitions that may need approval, investigation, or re-run follow-up
- Timestamps and reason fields for delayed or blocked actions

## Top-level KPI cards and charts

The page header shows key portfolio health indicators:

- **NAV (Net Asset Value)**: Total portfolio value including cash and all open positions. The NAV sparkline chart shows how portfolio value has trended over recent snapshots.
- **Unrealized P&L**: The aggregate paper profit or loss across all currently open positions. Positive means the portfolio is up on paper; negative means down. The Unrealized P&L chart shows the trend of unrealized gains/losses over time.
- **Drift**: Whether the live portfolio has drifted from its expected state. Values include `ALIGNED`, `MINOR_DRIFT`, and `SIGNIFICANT_DRIFT`.
- **Winners / Losers**: Count of open positions currently showing positive vs negative unrealized P&L.
- **Snapshot Trends**: Mini sparkline charts tracking NAV, unrealized P&L, and position count over the most recent snapshots synced from the broker (IBKR).

## Open Positions section

Shows all currently held positions with:
- Symbol, side (LONG/SHORT), quantity, average cost, market value
- **Unrealized P&L** per position: the difference between current market value and cost basis for that position
- **Protection status**: whether take-profit (TP) and stop-loss (SL) bracket orders are armed at the broker

## Execution History section

Shows recently executed trades with:
- Symbol, side, quantity, fill price, timestamp
- **Realized P&L**: actual profit/loss locked in when a position was closed. An `~` prefix means the value is estimated.

## Activity Feed / Decision Pipeline

Shows trade actions flowing through the lifecycle:
- **PROPOSED → APPROVED → EXECUTED** (or REJECTED/CANCELLED)
- Committee verdict, PM approval, compliance approval, intent submission
- Reason codes explaining blocks or delays
- Revalidation outcome before execution

## Typical workflow

1. Confirm portfolio state freshness.
2. Review NAV and Unrealized P&L KPIs for overall health.
3. Check open positions for any outsized winners or losers.
4. Review newest activity entries and status outcomes.
5. Trace unexpected blocks or delays to reason fields.
6. Cross-check with AI Agent Decisions for committee rationale.
7. Cross-check with Runs for pipeline/run-level context.

## When to use this page

- During live-paper operating windows.
- After approvals, to verify expected state transitions.
- When investigating execution/validation drift.
- Morning check: confirm NAV, review overnight position changes, check for blocked actions.

## Common status interpretation guidance

- A row can pass committee review but still fail later operational checks.
- Delays are often freshness/validation-related rather than strategy-related.
- Repeated transition stalls usually point to a systemic gate (policy, approvals, or data freshness), not random failure.

## Fast troubleshooting path

1. Find latest blocked/delayed row in activity feed.
2. Capture status + reason fields and timestamp.
3. Open matching decision in AI Agent Decisions.
4. Open matching run in Runs by time window/run ID.
5. Confirm whether block was expected policy behavior or an incident.
