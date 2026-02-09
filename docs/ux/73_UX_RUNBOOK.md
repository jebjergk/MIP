# UX Runbook

Operational runbook for the read-only UX: deployment, inspecting a run, validating digests, restarting a portfolio episode, and where to look for failures. Aligns with [MIP/docs/60_RUNBOOK_TROUBLESHOOTING.md](MIP/docs/60_RUNBOOK_TROUBLESHOOTING.md).

## Deployment

The UX app (FastAPI + React) runs **externally** and connects to Snowflake via `snowflake.connector`. It uses the same database and schema as the MIP Snowflake machinery (tables, views, stored procs).

### Environment variables

From repo root, copy `.env.example` to `.env` and set:

| Variable | Purpose |
|----------|---------|
| `SNOWFLAKE_ACCOUNT` | Snowflake account identifier |
| `SNOWFLAKE_USER` | Read-only Snowflake user |
| `SNOWFLAKE_PASSWORD` | User password (required when `SNOWFLAKE_AUTH_METHOD=password`) |
| `SNOWFLAKE_AUTH_METHOD` | `password` (default) or `keypair`. MFA users must use `keypair`. |
| `SNOWFLAKE_ROLE` | Role (read-only recommended for UX) |
| `SNOWFLAKE_WAREHOUSE` | Warehouse for query execution |
| `SNOWFLAKE_DATABASE` | Database (must match MIP deployment) |
| `SNOWFLAKE_SCHEMA` | Schema (must match MIP deployment; typically `APP` or the schema where MIP objects live) |

The API ([MIP/apps/mip_ui_api/app/config.py](MIP/apps/mip_ui_api/app/config.py)) reads these; the schema determines which `MIP.APP`, `MIP.MART`, `MIP.AGENT_OUT` objects are queried. Use the same database and schema as your MIP SQL deployment so the API sees the canonical tables and views.

### Key-pair authentication (MFA environments)

When your Snowflake account requires MFA or you prefer key-pair auth, see [Deploying the Local UX API User (Key-Pair Auth)](#deploying-the-local-ux-api-user-key-pair-auth) below.

If auth fails (e.g. MFA user with password), the UI shows: *Snowflake auth failed — MFA users must use keypair or OAuth.*

## Deploying the Local UX API User (Key-Pair Auth)

Use this section when deploying the UX API to a **new Snowflake account** or when the account requires MFA. The deployment kit lives in `MIP/SQL/deploy/ux_api_user/`.

### Why MFA makes password auth unsuitable for a local backend

When Snowflake enforces MFA (e.g. Duo Push, TOTP), password-only auth requires interactive approval on each connection. A headless API server cannot respond to push notifications or enter TOTP codes. Key-pair authentication (or OAuth) avoids that by using a pre-registered public key; the server signs requests with the private key and Snowflake verifies without human interaction.

### How to generate the keypair

Use OpenSSL to create a 2048-bit RSA key in PEM/PKCS#8 format:

```bash
# Unencrypted (simpler; protect file with OS permissions)
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt

# Encrypted (recommended; set SNOWFLAKE_PRIVATE_KEY_PASSPHRASE in .env)
openssl genrsa 2048 | openssl pkcs8 -topk8 -v2 des3 -inform PEM -out rsa_key.p8
```

Then generate the public key:

```bash
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

### Where to store the private key locally

Store the private key in a secure location, e.g. `~/.snowflake/rsa_key.p8` or a project-local path. Restrict permissions so only the API process can read it:

```bash
chmod 600 rsa_key.p8
```

Never commit the private key to version control. Add `*.p8` and the key path to `.gitignore`.

### How to set `rsa_public_key` in Snowflake

Use script `MIP/SQL/deploy/ux_api_user/03_set_rsa_public_key.sql`:

1. Open `rsa_key.pub` and copy the base64 content **between** `-----BEGIN PUBLIC KEY-----` and `-----END PUBLIC KEY-----`. Remove the header/footer lines and concatenate the base64 lines into a single string (no newlines).
2. Edit `03_set_rsa_public_key.sql`, replace `REPLACE_ME_WITH_PUBLIC_KEY_BODY` with that string, and replace `:ux_user` if you use a different user name.
3. Run the script as SECURITYADMIN (or equivalent).

### Environment variables in `.env`

Copy `.env.example` to `.env` and set:

| Variable | Value | Purpose |
|----------|-------|---------|
| `SNOWFLAKE_AUTH_METHOD` | `keypair` | Use key-pair auth (required for MFA accounts) |
| `SNOWFLAKE_USER` | `:ux_user` (e.g. `MIP_UI_API`) | UX API service user |
| `SNOWFLAKE_ROLE` | `:ux_role` (e.g. `MIP_UI_API_ROLE`) | Role granted to the user |
| `SNOWFLAKE_PRIVATE_KEY_PATH` | `/path/to/rsa_key.p8` | Absolute path to the private key file |
| `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE` | *(optional)* | Passphrase if the key is encrypted |
| `SNOWFLAKE_ACCOUNT` | e.g. `xy12345.us-east-1` | Snowflake account identifier |
| `SNOWFLAKE_WAREHOUSE` | e.g. `MIP_WH_XS` | Warehouse for queries |
| `SNOWFLAKE_DATABASE` | `MIP` | Database (must match MIP deployment) |
| `SNOWFLAKE_SCHEMA` | `APP` | Schema (typically `APP`) |

### Verification checklist

After deployment:

1. Run the API: `uvicorn app.main:app --reload --app-dir MIP/apps/mip_ui_api`
2. Call `GET /api/status` (or `http://localhost:8000/status`).
3. Expect `snowflake_ok: true` and `auth_method: "keypair"`.

If `snowflake_ok` is `false`, check the `snowflake_message` field for the error; the UI header will also show the message.

### Local development

1. **API**: From repo root, `uvicorn app.main:app --reload --app-dir MIP/apps/mip_ui_api` (or from `MIP/apps/mip_ui_api`: `uvicorn app.main:app --reload`). Default port 8000.
2. **Web**: From `MIP/apps/mip_ui_web`, `npm run dev`. Vite serves on port 5173 and proxies `/api` to `http://127.0.0.1:8000`.

### Production deployment

- **API**: Deploy the FastAPI app (e.g., Gunicorn + Uvicorn, Docker, or a PaaS). Ensure `.env` or equivalent secrets are set in the runtime environment. The API is stateless; it connects to Snowflake per request.
- **Web**: Build with `npm run build` (from `MIP/apps/mip_ui_web`); serve the `dist/` output via a static host or CDN. Configure the API base URL (e.g., `VITE_API_BASE` or update the proxy/target) so the frontend calls the deployed API.

### Streamlit vs external UX

- **Streamlit** (`MIP/streamlit_app.py`): Runs **inside** Snowflake (Streamlit in Snowflake). Uses `get_active_session()`; no `.env` needed. Targets Snowflake-native deployment.
- **React + FastAPI** (`MIP/apps/`): Runs **externally**. Uses `snowflake.connector` and `.env`. Targets external hosting (VPS, cloud, on-prem). Both can coexist; they read the same MIP objects.

## Inspecting a run

Use a single run identifier (e.g. from `MIP_AUDIT_LOG` after a pipeline run). Replace `:run_id` with your run ID (UUID string).

1. **Run summary (steps + counts + errors)**  
   Query `MIP.APP.MIP_AUDIT_LOG` for `(EVENT_TYPE = 'PIPELINE' and RUN_ID = :run_id)` or `(EVENT_TYPE = 'PIPELINE_STEP' and PARENT_RUN_ID = :run_id)`; order by `EVENT_TS`. See [72_UX_QUERIES.md](72_UX_QUERIES.md) “Run timeline by RUN_ID”.

2. **Interpreted summary**  
   The API’s audit interpreter turns pipeline step rows (and their `DETAILS` JSON) into summary cards and narrative bullets. Use the **Audit Viewer** screen or `GET /runs/{run_id}` to see timeline + interpreted summary.

3. **Where to look for failures**  
   - **Audit log**: Rows with `STATUS = 'FAIL'` or non-null `ERROR_MESSAGE`.  
   - **Step DETAILS**: Each pipeline step row has `DETAILS` (variant/JSON). Keys written by the pipeline include `step_name`, `scope`, `scope_key`, `started_at`, `completed_at`, `status`, and step-specific fields (e.g. `portfolio_count`, `ingest_status`). Use these to see why a step failed or skipped.

4. **Portfolio/KPI context for the run**  
   - Portfolio run events: `MIP.MART.V_PORTFOLIO_RUN_EVENTS` where `RUN_ID = :run_id`.  
   - KPI summary: `MIP.MART.V_PORTFOLIO_RUN_KPIS` where `RUN_ID = :run_id`.  
   See [MIP/docs/60_RUNBOOK_TROUBLESHOOTING.md](MIP/docs/60_RUNBOOK_TROUBLESHOOTING.md) for the exact queries.

## Cockpit drill-down

The Cockpit page shows opportunities (signal-based recommendations). Each opportunity card has a "View in Signals" link that navigates to the **Signals Explorer** page (`/signals`) with pre-filled filters.

**Deep-link parameters:**
- `portfolioId` — Portfolio context
- `asOf` — Digest's as-of timestamp (market date)
- `pipelineRunId` — Digest's pipeline run ID
- `symbol` — Symbol from the opportunity
- `market_type` — Market type (STOCK, FX)
- `pattern_id` — Pattern ID
- `from=cockpit` — Indicates navigation from Cockpit

The Signals Explorer automatically applies fallback logic if the exact filters return no rows (e.g., drops `run_id` filter, expands time window). See [72_UX_QUERIES.md](72_UX_QUERIES.md) "Signals Explorer (GET /signals)" for the full API contract.

## Validating digests

1. **Latest digest per portfolio**  
   Use [72_UX_QUERIES.md](72_UX_QUERIES.md) “Latest cockpit digest by portfolio_id” (or `GET /cockpit/latest?portfolio_id=...`).

2. **Attribution smoke (new digests)**  
   For a given `:run_id`, check that no digest rows incorrectly carry attribution from a previous run:  
   `select count(*) as bad_rows from MIP.AGENT_OUT.MORNING_BRIEF where RUN_ID = :run_id and BRIEF:"attribution":"latest_run_id" is not null;`  
   Expect 0. See [MIP/docs/60_RUNBOOK_TROUBLESHOOTING.md](MIP/docs/60_RUNBOOK_TROUBLESHOOTING.md) “Attribution smoke”.

3. **Consistency checks**  
   For full consistency checks (digest vs ORDER_PROPOSALS, risk gate, signals), use the checks documented in the repo (e.g. morning_brief_consistency).

## Restarting a portfolio episode

To start a **new portfolio** (new portfolio id):

1. Run the script **`MIP/SQL/scripts/restart_portfolio_episode.sql`**.  
   It inserts a new row into `MIP.APP.PORTFOLIO` with the exact columns from [160_app_portfolio_tables.sql](MIP/SQL/app/160_app_portfolio_tables.sql). Required: PROFILE_ID, NAME, BASE_CURRENCY, STARTING_CASH.

2. **Note**: This creates a new portfolio; positions, trades, and daily snapshots are keyed by PORTFOLIO_ID, so the new portfolio starts with no child rows. The script does not delete existing portfolios. Optional “cleanup old test portfolio” sections, if added, must be clearly marked optional.

## Where to look for failures

| Source | What to check |
|--------|----------------|
| **MIP_AUDIT_LOG** | `STATUS = 'FAIL'`, `ERROR_MESSAGE` not null; filter by `EVENT_TYPE` ('PIPELINE', 'PIPELINE_STEP') and `RUN_ID` / `PARENT_RUN_ID`. |
| **Step DETAILS** | JSON in `DETAILS`: `step_name`, `started_at`, `completed_at`, `status`, and step-specific keys (e.g. ingest_status, portfolio_count). Use these to see skip/fail reasons. |
| **60_RUNBOOK_TROUBLESHOOTING.md** | Common failure modes (missing bars, insufficient outcomes, missing entry bars, no recommendations, audit log failures, Snowflake FOR loop issues) and their checks. |

## RUN_ID column inventory

To verify RUN_ID-related columns are string types (no numeric coercion), run the script **`MIP/SQL/scripts/run_id_inventory.sql`** or the query in [MIP/docs/60_RUNBOOK_TROUBLESHOOTING.md](MIP/docs/60_RUNBOOK_TROUBLESHOOTING.md) “RUN_ID column inventory”.
