# 27. Live Portfolio Link

This page binds live execution controls to broker truth and defines portfolio-level execution guardrails.

Research proposal source is selected at import time in Live Portfolio Activity.

## What This Page Is For

- View a **cloud-style list** of all live configs with wiring status
- Create or edit a **Live Config** (system-assigned ID)
- Link an **IBKR Account** (broker truth source)
- Configure pre-trade controls (size, freshness, risk limits)
- Verify readiness using the **Activation Guard**

## Setup Flow

1. From the list page, click **Create New Live Config** or **Edit** on an existing row.
2. Enter IBKR Account ID (for example `DU...`) and execution mode.
3. Set risk/freshness controls and click **Save Config**.
4. Confirm schematic and guard status.
5. In Live Portfolio Activity, choose source research portfolio at import time.
6. Re-check readiness before expected execution windows.

Important behavior:

- Config IDs are assigned by the backend on create.
- No SIM fallback is stored in this page.
- Research proposal source is selected explicitly in Live Portfolio Activity import.
- Save success does not imply execution readiness at run-time.

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

## Readiness checklist before execution windows

- Live config saved successfully
- IBKR account linkage valid
- Freshness controls passing
- Drawdown/bust protections not tripped
- Required approvals and revalidation available in activity/decision flow

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

