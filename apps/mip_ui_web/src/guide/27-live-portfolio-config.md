# 27. Live Portfolio Link

This page binds live execution controls to broker truth.
Research proposal source is selected at import time in Live Trades.

## What This Page Is For

- Create or select a **Live Config** (system-assigned ID)
- Optionally set a **default source portfolio fallback** (legacy behavior)
- Link an **IBKR Account** (broker truth source)
- Configure pre-trade controls (size, freshness, risk limits)
- Verify readiness using the **Activation Guard**

## Setup Flow

1. Enter IBKR Account ID (for example `DU...`).
2. Click **Create New Live Config** to enter draft/create mode.
3. Review fields, then click **Save Config** to persist.
4. Confirm schematic and guard status.
5. In Live Trades, choose source research portfolio at import time.

Important behavior:

- Clicking **Create New Live Config** does **not** insert into database.
- The row is only written when **Save Config** is pressed.
- You can cancel draft mode using **Cancel Create**.

## Connection Schematic (Color Meaning)

- **Green**: connected / passing
- **Red**: blocking issue
- **Gray**: not configured yet

Main chain:

- `Source Portfolio (import-time) -> MIP Live Portfolio -> IBKR Account`
- `MIP Live Portfolio -> Activation Guard -> Execution Readiness`

## What Save Actually Does

Save writes the selected config row to:

- `MIP.LIVE.LIVE_PORTFOLIO_CONFIG`

This is a governance/control record. It does not place orders by itself.

## Field Meaning (Practical)

- **Default Source Portfolio**: legacy fallback only (optional)
- **IBKR Account ID**: broker account used as truth mirror
- **Adapter Mode**: PAPER vs LIVE policy mode
- **Max Position % / Max Positions / Cash Buffer %**: sizing guardrails
- **Quote Freshness Sec / Snapshot Freshness Sec**: stale-data blockers
- **Validity Window Sec / Cooldown Bars**: action lifecycle controls
- **Drawdown Stop % / Bust %**: portfolio safety brakes

## Common Questions

### Why no manual Live Portfolio ID entry?

IDs are now system-created to reduce operator error and ensure stable linkage.

### Why are sizing/risk controls separate from research portfolio?

Research source selection controls what gets imported. Live guardrails govern broker-side execution safety and are configured independently.

### Why is execution still blocked if config is saved?

Saved config is only one gate. Execution still requires:

- realism validation,
- PM + compliance approvals,
- on-click revalidation,
- healthy broker drift/snapshot state.

