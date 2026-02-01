# UX Overview

High-level UX goals, personas, and mapping from screens to canonical MIP objects. Copy/paste friendly; no wireframes.

## Quickstart

Get the local UX running in a few steps:

1. **Generate a keypair** and store the private key (e.g. `~/.snowflake/rsa_key.p8`, `chmod 600`). See [73_UX_RUNBOOK.md](73_UX_RUNBOOK.md) “Deploying the Local UX API User”.

2. **Run the deployment scripts** (in order, as SECURITYADMIN / MIP_ADMIN_ROLE):
   - `MIP/SQL/deploy/ux_api_user/01_create_role_and_user.sql`
   - `MIP/SQL/deploy/ux_api_user/02_grants_readonly.sql`
   - `MIP/SQL/deploy/ux_api_user/03_set_rsa_public_key.sql` (paste public key body, replace placeholders)

3. **Fill `.env`** from `.env.example` with `SNOWFLAKE_AUTH_METHOD=keypair`, `SNOWFLAKE_PRIVATE_KEY_PATH`, and other vars.

4. **Start backend and frontend:**
   ```bash
   uvicorn app.main:app --reload --app-dir MIP/apps/mip_ui_api
   ```
   ```bash
   cd MIP/apps/mip_ui_web && npm run dev
   ```

5. **Validate:** `GET /api/status` returns `snowflake_ok: true`.

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
