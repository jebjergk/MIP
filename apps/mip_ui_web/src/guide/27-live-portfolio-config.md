# 27. Live Portfolio Link

This page binds live execution controls to broker truth and research source in one place.

## What This Page Is For

- Create or select a **Live Config** (system-assigned ID)
- Link a **SIM Portfolio** (proposal source)
- Link an **IBKR Account** (broker truth source)
- Configure pre-trade controls (size, freshness, risk limits)
- Verify readiness using the **Activation Guard**

## Setup Flow

1. Pick a SIM Portfolio from the dropdown.
2. Enter IBKR Account ID (for example `DU...`).
3. Click **Create New Live Config** to enter draft/create mode.
4. Review fields, then click **Save Config** to persist.
5. Confirm schematic and guard status.

Important behavior:

- Clicking **Create New Live Config** does **not** insert into database.
- The row is only written when **Save Config** is pressed.
- You can cancel draft mode using **Cancel Create**.

## Connection Schematic (Color Meaning)

- **Green**: connected / passing
- **Red**: blocking issue
- **Gray**: not configured yet

Main chain:

- `SIM Portfolio -> MIP Live Portfolio -> IBKR Account`
- `MIP Live Portfolio -> Activation Guard -> Execution Readiness`

## What Save Actually Does

Save writes the selected config row to:

- `MIP.LIVE.LIVE_PORTFOLIO_CONFIG`

This is a governance/control record. It does not place orders by itself.

## Field Meaning (Practical)

- **SIM Portfolio**: research proposal source for import
- **IBKR Account ID**: broker account used as truth mirror
- **Adapter Mode**: PAPER vs LIVE policy mode
- **Max Position % / Max Positions / Cash Buffer %**: sizing guardrails
- **Quote Freshness Sec / Snapshot Freshness Sec**: stale-data blockers
- **Validity Window Sec / Cooldown Bars**: action lifecycle controls
- **Drawdown Stop % / Bust %**: portfolio safety brakes

## Common Questions

### Why no manual Live Portfolio ID entry?

IDs are now system-created to reduce operator error and ensure stable linkage.

### Why are sizing/risk controls separate from SIM portfolio?

SIM linkage supplies proposal source only. Live guardrails govern broker-side execution safety and can be configured independently.

### Why is execution still blocked if config is saved?

Saved config is only one gate. Execution still requires:

- realism validation,
- PM + compliance approvals,
- on-click revalidation,
- healthy broker drift/snapshot state.

