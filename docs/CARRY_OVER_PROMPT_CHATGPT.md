# MIP Carry-Over Prompt for ChatGPT

**Last updated:** 2026-01-29

**How to use:** Paste this entire document into a new ChatGPT conversation when switching context. It gives ChatGPT everything it needs to understand the MIP project and where you left off. Optionally start with: *"You are the Lead Architect and Subject Matter Expert (SME) for the Market Intelligence Platform (MIP). Use the following context."*

---

## 1. Project Overview

### What MIP is
- **MIP (Market Intelligence Platform)** is a **Snowflake-native** daily data pipeline: **ingest → returns → recommendations → evaluation → portfolio simulation → morning brief**. All logic runs inside Snowflake.
- **Orchestrator:** `MIP.APP.SP_RUN_DAILY_PIPELINE`. **Scheduled task:** `MIP.APP.TASK_RUN_DAILY_PIPELINE` (runs once per day).
- **Outputs:** Structured analytics tables and views (e.g. `MIP.MART.MARKET_RETURNS`, `MIP.APP.RECOMMENDATION_LOG`, `MIP.APP.RECOMMENDATION_OUTCOMES`, portfolio KPIs in `MIP.MART`, morning briefs in `MIP.AGENT_OUT.MORNING_BRIEF`).

### What MIP is not
- **Not a live trading system or broker:** no external order routing or broker integration. "Trades" are simulated in Snowflake via `SP_VALIDATE_AND_EXECUTE_PROPOSALS`, which writes paper trades into portfolio tables.
- **Not real-time:** scheduled run is daily; default universe uses **daily bars** (`INTERVAL_MINUTES=1440`) for STOCK, ETF, and FX.

### Markets and ingestion
- **Market types:** STOCK, ETF, FX. Universe is seeded in config; pipeline discovers types from universe.
- **Ingestion:** AlphaVantage via `SP_INGEST_ALPHAVANTAGE_BARS` → `MIP.MART.MARKET_BARS`.

---

## 2. Architecture (schemas and key objects)

### Schemas
- **`MIP.RAW_EXT`:** Raw external/staging data.
- **`MIP.MART`:** Analytics — bars, returns, KPIs, training views.
- **`MIP.APP`:** Application tables, procedures, orchestration, audit log, recommendations, outcomes, portfolios.
- **`MIP.AGENT_OUT`:** Persisted agent outputs (morning brief, order proposals).

### Core pipeline procedures (daily sequence)
1. **Ingest:** `SP_PIPELINE_INGEST` → `SP_INGEST_ALPHAVANTAGE_BARS` → `MIP.MART.MARKET_BARS`
2. **Returns:** `SP_PIPELINE_REFRESH_RETURNS` → `MIP.MART.MARKET_RETURNS`
3. **Recommendations:** `SP_PIPELINE_GENERATE_RECOMMENDATIONS` → `MIP.APP.RECOMMENDATION_LOG`
4. **Evaluation:** `SP_PIPELINE_EVALUATE_RECOMMENDATIONS` → `MIP.APP.RECOMMENDATION_OUTCOMES`
5. **Portfolios:** `SP_PIPELINE_RUN_PORTFOLIOS` → `SP_PIPELINE_RUN_PORTFOLIO` (per portfolio) → `SP_RUN_PORTFOLIO_SIMULATION`
6. **Morning briefs:** `SP_PIPELINE_WRITE_MORNING_BRIEFS` → `SP_AGENT_PROPOSE_TRADES` → `SP_VALIDATE_AND_EXECUTE_PROPOSALS` → `SP_WRITE_MORNING_BRIEF` → `MIP.AGENT_OUT.MORNING_BRIEF`

### Agent flow
- **Propose:** `SP_AGENT_PROPOSE_TRADES` writes to `MIP.AGENT_OUT.ORDER_PROPOSALS`.
- **Execute:** `SP_VALIDATE_AND_EXECUTE_PROPOSALS` validates, executes paper trades, updates portfolio tables.
- **Brief:** `SP_WRITE_MORNING_BRIEF` persists JSON to `MIP.AGENT_OUT.MORNING_BRIEF`.

### Key views
- **`V_PORTFOLIO_RUN_KPIS`**, **`V_PORTFOLIO_RUN_EVENTS`:** Portfolio performance and events.
- **`V_MORNING_BRIEF_JSON`:** Composes brief content (signals, risk, deltas).
- **`V_PORTFOLIO_RISK_STATE`:** Entry gate (`ENTRIES_BLOCKED`), drawdown stop, etc. Used by propose/execute.
- **`V_SIGNALS_ELIGIBLE_TODAY`:** Signals eligible for proposals.
- **`V_TRAINING_KPIS`**, **`V_TRAINING_LEADERBOARD`:** Training metrics by pattern/market/interval/horizon.
- **`V_SIGNAL_OUTCOMES_BASE`:** One row per (recommendation_id, horizon_bars); joins LOG + OUTCOMES.
- **`V_TRUSTED_SIGNALS_LATEST_TS`:** Trusted signal classification.

---

## 3. Operating Rules

- **Idempotency:** Re-runs for the same `to_ts` must **not** duplicate outputs. Skip cleanly when there is no new data.
- **Audit:** Every pipeline step writes to `MIP.APP.MIP_AUDIT_LOG` via `SP_LOG_EVENT` with `status`, `reason`, and counts where relevant.
- **No hardcoding:** Symbols and market types come from universe/config tables, not literals.
- **Snowflake-native:** Prefer stored procedures, tasks, views; avoid external runtimes unless required.
- **Timestamp policy:** System timestamps (e.g. `CREATED_AT`, `EVENT_TS`) use Berlin-local `TIMESTAMP_NTZ`, default `CURRENT_TIMESTAMP()`. Market timestamps (`TS`, `BAR_TS`, etc.) stay as-is. See `MIP/docs/TIMESTAMP_POLICY.md`.

---

## 4. Recent Work and Current State (last ~5 days)

*Inferred from ROADMAP_CHECKLIST, CODE_REVIEW_FINDINGS (2026-01-27), migrations, and smoke/checks.*

### Stabilization
- **Rate-limit:** Ingestion may `SKIP_RATE_LIMIT` → pipeline `SUCCESS_WITH_SKIPS`.
- **No new bars:** When `NO_NEW_BARS`, pipeline `SKIPPED_NO_NEW_BARS`; downstream steps skip without duplicating.
- **Drawdown stop:** `DRAWDOWN_STOP` → regime `ALLOW_EXITS_ONLY`; entries blocked, exits allowed.

### Code review (2026-01-27) — resolved
- **Entry gate in execution:** `SP_VALIDATE_AND_EXECUTE_PROPOSALS` now checks `V_PORTFOLIO_RISK_STATE` and blocks BUY when `ENTRIES_BLOCKED=true`.
- **Proposal count scoping:** Pipeline counts proposals by `RUN_ID` **or** `SIGNAL_RUN_ID` to handle type mismatches.

### Code review — still open
- **HIGH-001:** Run ID type inconsistency — pipeline uses UUID **string**; signals/proposals use **numeric** run IDs. Scoping uses both; underlying mismatch remains.
- **MED-001:** MERGE key validation — run ID mismatch can cause MERGE misses/duplication.
- **MED-002:** `V_PORTFOLIO_RISK_STATE` vs simulation entry-gate logic — keep aligned.
- **MED-003:** Morning brief consistency checks — validate brief counts vs actual tables before write.

### Migrations
- **Berlin timestamp defaults:** System timestamp columns default to `CURRENT_TIMESTAMP()`. See `MIP/migrations/alter_defaults_berlin_report.md`.
- **ORDER_PROPOSALS signal linkage:** Added `RECOMMENDATION_ID`, `SIGNAL_TS`, `SIGNAL_RUN_ID`, etc. See `MIP/migrations/alter_order_proposals_signal_linkage.sql`.

### Smoke and validation
- **Smoke:** `MIP/SQL/smoke/01_Kens_tests.sql`, `smoke_daily_pipeline.sql`, `smoke_tests.sql`, and others. See `MIP/SQL/smoke/README.md`.
- **Integrity checks:** `MIP/SQL/checks/` — `run_scoping_validation.sql`, `entry_gate_consistency.sql`, `morning_brief_consistency.sql`, `integrity_checks.sql`, `SP_RUN_INTEGRITY_CHECKS`.
- **Order proposals dedup:** Keep latest per `(PORTFOLIO_ID, SYMBOL, MARKET_TYPE, INTERVAL_MINUTES, SIDE)`; cleanup script in `checks/cleanup_order_proposals_duplicates.sql`.

---

## 5. Roadmap Snapshot (where we are)

### Phase 1 — Foundation
- **Done:** Ingest, returns, recommendations, evaluation, portfolio sim, morning brief; rate-limit and no-new-bars handling; drawdown stop.
- **In progress:** "Why 0 inserted" transparency (candidate / threshold / dedup / inserted counts); evaluation semantics confirmation; ETF first-class everywhere.

### Phase 2 — Paper portfolio execution
- **Done:** Sim runs, trades, drawdown stop, morning brief, propose/validate/execute flow.
- **Unlock:** Training-mode profile (relaxed drawdown, smaller sizing) to generate more trades for learning. Transaction costs, exposure rules in backlog.

### Phase 3 — Risk, hedging, AI brief
- **Backlog / in progress:** Risk layer, hedging, brief upgrades (status → insight), multi-agent roles.

---

## 6. Repo Landmarks

### SQL
- **`MIP/SQL/bootstrap/`:** Roles, warehouses, schemas.
- **`MIP/SQL/app/`:** Pipeline and agent procedures, app tables, audit log.
- **`MIP/SQL/mart/`:** Market bars, returns, training views.
- **`MIP/SQL/views/mart/`**, **`MIP/SQL/views/app/`:** Curated views.
- **`MIP/SQL/checks/`:** Integrity and validation scripts.
- **`MIP/SQL/smoke/`:** Smoke tests.

### Docs (recommended read order)
- **Onboarding:** `05_DEVELOPER_ONBOARDING.md`
- **Architecture / workflows / procs:** `10_ARCHITECTURE.md`, `30_WORKFLOWS.md`, `35_STORED_PROCEDURES.md`
- **Data / KPIs:** `40_TABLE_CATALOG.md`, `50_KPIS_AND_TRAINING.md`
- **Operations:** `60_RUNBOOK_TROUBLESHOOTING.md`, `70_GLOSSARY.md`
- **Planning / review:** `ROADMAP_CHECKLIST.md`, `CODE_REVIEW_FINDINGS.md`, `01_GPT_PROMPT_SUMMARY.md`

### Tooling
- **Streamlit:** `MIP/streamlit_app.py` (Snowpark); `MIP/ui/` for layout.
- **Snowflake connection:** `cursorfiles/snowflake_connection.py`; credentials in `.env`.

---

## 7. Implementation Conventions (when touching code)

- **Observability:** Every step has clear `status` and `reason` when skipped. Audit JSON includes counts: `candidate_count`, `filtered_by_threshold_count`, `dedup_skipped_count`, `inserted_count`. Top-level pipeline audit: `has_new_bars`, `pipeline_status_reason`.
- **Runbook:** Add or update operator queries (e.g. in `60_RUNBOOK_TROUBLESHOOTING.md`) so we can quickly answer: "Did the pipeline run? Ingest? Recs? Eval? Sim? Why skipped?"
- **Idempotency:** No duplicate inserts for same `(pattern_id, symbol, interval_minutes, ts)` unless explicitly intended.
- **Snowflake SQL:** Use scalar subqueries or RESULTSET + FOR loop; avoid `SELECT ... INTO` in stored procedures. See `MIP/docs/SNOWFLAKE_SQL_LIMITATIONS.md`.

---

## 8. Optional Extras

- **Known gaps:** `RUNBOOK.md` and `ACCEPTANCE_TESTS.md` are not yet in repo (referenced in `01_GPT_PROMPT_SUMMARY.md`). Consider adding them when changing behavior.
- **Agent catalog:** Read-only analysts (e.g. Performance Analyst) in `MIP/docs/AGENTS.md`; they only read MART/AGENT_OUT views.

---

## 9. Clarifications (do not assume done)

- **HIGH-001 addressed (T4):** `ORDER_PROPOSALS` now has `RUN_ID_VARCHAR`; procedures write and filter by it. Dual scoping removed; pipeline and checks use `RUN_ID_VARCHAR = :run_id` only. Migration: `migrations/order_proposals_run_id_varchar_phase_a.sql`. Some integrity/run-scoping checks may still reference `RUN_ID`/`SIGNAL_RUN_ID` and could be updated to `RUN_ID_VARCHAR` for consistency.
- **Morning brief idempotency (T1–T3):** `SP_WRITE_MORNING_BRIEF` takes `(P_PORTFOLIO_ID, P_AS_OF_TS, P_RUN_ID, P_AGENT_NAME)`; MERGE key uses only those params. `V_MORNING_BRIEF_JSON` is content-only (no `AS_OF_TS`). Pipeline passes `as_of_ts` / `run_id`; brief step is skipped when `has_new_bars=false`. Smoke: `morning_brief_idempotency_smoke.sql`.

---

*Changelog: Initial version. Clarifications added 2026-01-29 (HIGH-001, morning brief idempotency).*
