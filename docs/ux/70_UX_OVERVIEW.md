# UX Overview

High-level UX goals, personas, and mapping from screens to canonical MIP objects. Copy/paste friendly; no wireframes.

## Goals

- **Visibility**: Operators and developers can inspect pipeline runs, portfolio state, morning briefs, and training status without writing SQL.
- **Read-only**: The UI and API do not write to Snowflake; no trading approvals or agent orchestration in v1.
- **Canonical source**: All displayed data comes from the documented canonical objects only.

## Personas

- **Operator**: Checks run success, inspects run timeline and interpreted summary, validates briefs, restarts a portfolio episode when needed.
- **Developer**: Debugs pipeline steps via audit log and DETAILS JSON; verifies portfolio snapshots (positions, trades, daily, KPIs, risk).
- **Viewer**: Browses portfolios, latest morning brief per portfolio, and training status (first draft).

## Screens and canonical objects

| Screen | Purpose | Canonical objects consumed |
|--------|---------|----------------------------|
| **Home** | Landing and nav to Runs, Portfolios, Audit, Brief, Training. | None (navigation only). |
| **Audit Viewer** | List recent pipeline runs; drill into run timeline + interpreted summary. | `MIP.APP.MIP_AUDIT_LOG` (EVENT_TYPE=PIPELINE / PIPELINE_STEP, RUN_ID, PARENT_RUN_ID, DETAILS). |
| **Portfolio** | List portfolios; detail header and snapshot (positions, trades, daily, KPIs, risk). | `MIP.APP.PORTFOLIO`, `MIP.APP.PORTFOLIO_TRADES`, `MIP.APP.PORTFOLIO_POSITIONS`, `MIP.APP.PORTFOLIO_DAILY`, `MIP.MART.V_PORTFOLIO_RUN_KPIS`, `MIP.MART.V_PORTFOLIO_RISK_GATE`, `MIP.MART.V_PORTFOLIO_RISK_STATE`, `MIP.MART.V_PORTFOLIO_RUN_EVENTS`. |
| **Morning Brief** | Latest brief per portfolio. | `MIP.AGENT_OUT.MORNING_BRIEF`. |
| **Training Status** | First-draft view of training state (e.g. row counts, last run). | `MIP.MART.V_TRAINING_LEADERBOARD` and/or training-related audit/tables; minimal. |

## Out of scope (v1)

- Trading approvals or any write to Snowflake from the UI/API.
- AI agent orchestration in the UI.
