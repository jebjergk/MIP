# MIP Context (Current UX Phase)

**Last updated:** 2026-02-01

Current context for the Market Intelligence Platform: machinery-complete pipeline plus local UX platform (FastAPI + React). Use this doc when switching context. Only canonical Snowflake objects from [74_CANONICAL_OBJECTS.md](74_CANONICAL_OBJECTS.md) are referenced below.

---

## 1. Milestone: Machinery Complete, UX Phase

**Phase 1 (foundation) and Phase 2 (paper portfolio) core machinery are done.**

- Pipeline runs end-to-end: **ingest → returns → recommendations → evaluation → portfolio sim → AI digest**.
- Run ID canonicalization and AI digest idempotency implemented.
- **Current focus:** Local UX platform — FastAPI + React to surface AI digests, agent outputs, portfolio KPIs, and training status.

---

## 2. Project Overview

### What MIP is

- **MIP (Market Intelligence Platform)** is a **Snowflake-native** daily data pipeline. All logic runs inside Snowflake.
- **Orchestrator:** `SP_RUN_DAILY_PIPELINE`. **Task:** `TASK_RUN_DAILY_PIPELINE`.
- **Markets:** STOCK, ETF, FX. AlphaVantage ingestion.

### What MIP is not

- Not a live trading system; no broker integration. Trades are simulated via `SP_VALIDATE_AND_EXECUTE_PROPOSALS`.
- Not real-time; daily cadence (default `INTERVAL_MINUTES=1440`).

---

## 3. Local UX Platform (FastAPI + React)

### Stack

- **API:** FastAPI (`MIP/apps/mip_ui_api/`), read-only, connects to Snowflake via env config.
- **Web:** React + Vite (`MIP/apps/mip_ui_web/`), consumes API; React Router for navigation.

### FastAPI routes

| Route | Purpose | Canonical objects |
|-------|---------|-------------------|
| `GET /runs` | Recent pipeline runs | `MIP.APP.MIP_AUDIT_LOG` |
| `GET /runs/{run_id}` | Run timeline + interpreted summary | `MIP.APP.MIP_AUDIT_LOG` |
| `GET /portfolios` | Portfolio list | `MIP.APP.PORTFOLIO` |
| `GET /portfolios/{id}` | Portfolio header | `MIP.APP.PORTFOLIO` |
| `GET /portfolios/{id}/snapshot` | Positions, trades, daily, KPIs, risk | `MIP.APP.PORTFOLIO_POSITIONS`, `MIP.APP.PORTFOLIO_TRADES`, `MIP.APP.PORTFOLIO_DAILY`, `MIP.MART.V_PORTFOLIO_RUN_KPIS`, `MIP.MART.V_PORTFOLIO_RISK_GATE`, `MIP.MART.V_PORTFOLIO_RISK_STATE` |
| `GET /cockpit/latest` | Latest AI digest per portfolio | `MIP.AGENT_OUT.MORNING_BRIEF` (backend table; UI page is "Cockpit") |
| `GET /training/status` | Row count + top leaderboard | `MIP.MART.V_TRAINING_LEADERBOARD` |

### React pages

| Page | Purpose |
|------|---------|
| **Home** | Landing and nav to Runs, Portfolios, Audit, Cockpit, Training. |
| **Portfolio** | List portfolios; detail header and snapshot (positions, trades, daily, KPIs, risk). |
| **Audit Viewer** | List recent pipeline runs; drill into run timeline + interpreted summary. |
| **Cockpit** | Latest AI digest per portfolio. |
| **Training Status** | First-draft view of training state (row counts, top leaderboard). |

### Explain Mode

- Toggle in nav: when ON, shows tooltips and helper callouts (glossary entries).
- Context: `ExplainModeContext`, provider in `main.jsx`, toggle in `ExplainModeToggle.jsx`.
- Used by: `EmptyState`, `InfoTooltip`, pages (AuditViewer, Portfolio, Cockpit) for status badges and glossary tooltips. Note: `MorningBrief` React component redirects to Cockpit.

### Glossary

- **Source of truth:** `MIP/docs/ux/UX_METRIC_GLOSSARY.yml`.
- **Generated JSON:** `MIP/docs/ux/UX_METRIC_GLOSSARY.json`, `MIP/apps/mip_ui_web/src/data/UX_METRIC_GLOSSARY.json`.
- **Usage:** `getGlossaryEntry` / `getGlossaryEntryByDotKey` in `glossary.js`; `InfoTooltip` and status badges.
- **Regenerate:** `npm run generate:glossary` (from `MIP/apps/mip_ui_web`) or `python scripts/generate_glossary_json.py` (from repo root).

---

## 4. UX documentation (canonical)

| Doc | Purpose |
|-----|---------|
| [70_UX_OVERVIEW.md](70_UX_OVERVIEW.md) | Goals, personas, screens, canonical objects mapping. |
| [71_UX_DATA_CONTRACT.md](71_UX_DATA_CONTRACT.md) | Data contract: object names, schemas, grain. |
| [72_UX_QUERIES.md](72_UX_QUERIES.md) | Canonical read-only queries (no app code). |
| [73_UX_RUNBOOK.md](73_UX_RUNBOOK.md) | Inspecting runs, validating AI digests, restarting portfolio episodes. |

---

## 5. Streamlit (legacy, reference only)

The previous UX was a **Streamlit app** running in Snowflake native app (Snowpark `get_active_session()`). It is **legacy** and kept for reference only.

- **Location:** `MIP/streamlit_app.py`, `MIP/ui/layout.py`.
- **Pages (legacy):** Overview | Opportunities | Portfolio | Training & Trust | Admin / Ops.
- **Stack:** Snowpark, runs inside Snowflake.

The current UX is **FastAPI + React** (see §3). For canonical objects and runbooks, use the docs above.

---

## 6. Machinery summary (no UX object names changed)

- **Pipeline:** End-to-end daily automation; rate limit and no-new-bars handling; idempotent steps.
- **Run ID:** Canonical `RUN_ID_VARCHAR` on `ORDER_PROPOSALS`; dual scoping removed.
- **AI digest (backend `MORNING_BRIEF` table):** Deterministic and idempotent on `(portfolio_id, as_of_ts, run_id, agent_name)`. UI page is "Cockpit".
- **Risk / entry gate:** Drawdown stop → `ALLOW_EXITS_ONLY`; execution blocks BUY when `ENTRIES_BLOCKED=true`.
- **Agent flow:** Propose → validate → execute → write AI digest; `MORNING_BRIEF` (backend table; UI page is "Cockpit"), `ORDER_PROPOSALS` populated.

---

## 7. Operating rules

- **Run ID:** `RUN_ID` / `PIPELINE_RUN_ID` are **varchar(64)** UUID strings. Do not treat as numeric.
- **Idempotency:** Re-runs for same `to_ts` must not duplicate outputs.
- **Canonical objects:** Only objects in [74_CANONICAL_OBJECTS.md](74_CANONICAL_OBJECTS.md) may be referenced by the UX. No inventing table, view, or procedure names.

---

## 8. Repo landmarks

- **SQL:** `MIP/SQL/bootstrap/`, `app/`, `mart/`, `views/mart/`, `views/app/`, `checks/`, `smoke/`
- **UX API:** `MIP/apps/mip_ui_api/` (FastAPI)
- **UX Web:** `MIP/apps/mip_ui_web/` (React + Vite)
- **Docs:** `MIP/docs/ux/70_UX_OVERVIEW.md`, `MIP/docs/ux/71_UX_DATA_CONTRACT.md`, `MIP/docs/ux/72_UX_QUERIES.md`, `MIP/docs/ux/73_UX_RUNBOOK.md`, `MIP/docs/ux/74_CANONICAL_OBJECTS.md`
- **Streamlit (legacy):** `MIP/streamlit_app.py`, `MIP/ui/layout.py`
