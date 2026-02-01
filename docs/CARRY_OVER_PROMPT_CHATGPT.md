# MIP Carry-Over Prompt for ChatGPT

**Last updated:** 2026-01-29

**How to use:** Paste this entire document into a new ChatGPT conversation when switching context. Start with: *"You are the Lead Architect and Subject Matter Expert (SME) for the Market Intelligence Platform (MIP). Use the following context. Do not invent table, view, or procedure names—use the canonical list exactly."*

---

## 1. Milestone: Machinery Complete, UX Phase

**Phase 1 (foundation) and Phase 2 (paper portfolio) core machinery are done.**

- Pipeline runs end-to-end: **ingest → returns → recommendations → evaluation → portfolio sim → morning brief**.
- Run ID canonicalization and morning brief idempotency implemented.
- **Current focus:** UX development — improve the Streamlit app to surface morning briefs, agent outputs, portfolio KPIs, and training/trust views.

---

## 2. Project Overview

### What MIP is
- **MIP (Market Intelligence Platform)** is a **Snowflake-native** daily data pipeline. All logic runs inside Snowflake.
- **Orchestrator:** `MIP.APP.SP_RUN_DAILY_PIPELINE`. **Task:** `MIP.APP.TASK_RUN_DAILY_PIPELINE`.
- **Markets:** STOCK, ETF, FX. AlphaVantage ingestion into `MIP.MART.MARKET_BARS`.

### What MIP is not
- Not a live trading system; no broker integration. Trades are simulated via `SP_VALIDATE_AND_EXECUTE_PROPOSALS`.
- Not real-time; daily cadence (default `INTERVAL_MINUTES=1440`).

---

## 3. Table Catalog (canonical names — use exactly)

**MIP.APP:**  
`INGEST_UNIVERSE`, `PATTERN_DEFINITION`, `RECOMMENDATION_LOG`, `RECOMMENDATION_OUTCOMES`, `PORTFOLIO`, `PORTFOLIO_POSITIONS`, `PORTFOLIO_TRADES`, `PORTFOLIO_DAILY`, `MIP_AUDIT_LOG`, `TRAINING_GATE_PARAMS`, `PORTFOLIO_PROFILE`, `APP_CONFIG`

**MIP.AGENT_OUT:**  
`MORNING_BRIEF`, `ORDER_PROPOSALS`, `AGENT_RUN_LOG`

- **ORDER_PROPOSALS:** `RUN_ID_VARCHAR` is canonical; `RUN_ID` (numeric) and `SIGNAL_RUN_ID` are legacy.
- **MORNING_BRIEF:** Merge key `(PORTFOLIO_ID, AS_OF_TS, RUN_ID, AGENT_NAME)`; key columns `BRIEF` (JSON), `PIPELINE_RUN_ID`, `RUN_ID`.

**MIP.MART:**  
`MARKET_BARS` (table), `MARKET_RETURNS` (view)

---

## 4. View Catalog (canonical names — use exactly)

**MIP.MART:**  
`V_PORTFOLIO_RUN_KPIS`, `V_PORTFOLIO_RUN_EVENTS`, `V_PORTFOLIO_ATTRIBUTION`, `V_PORTFOLIO_ATTRIBUTION_BY_PATTERN`, `V_PORTFOLIO_RISK_STATE`, `V_PORTFOLIO_RISK_GATE`, `V_MORNING_BRIEF_JSON`, `V_MORNING_BRIEF_WITH_DELTA`, `V_SIGNAL_OUTCOME_KPIS`, `V_SCORE_CALIBRATION`, `V_SIGNALS_WITH_EXPECTED_RETURN`, `V_TRUSTED_SIGNAL_POLICY`, `V_TRUSTED_SIGNALS`, `V_TRUSTED_SIGNALS_LATEST_TS`, `V_AGENT_DAILY_SIGNAL_BRIEF`, `V_AGENT_DAILY_ATTRIBUTION_BRIEF`, `V_AGENT_DAILY_RISK_BRIEF`, `V_PORTFOLIO_OPEN_POSITIONS_CANONICAL`, `V_SIGNAL_OUTCOMES_BASE`, `V_TRAINING_KPIS`, `V_TRAINING_LEADERBOARD`

**MIP.APP:**  
`V_SIGNALS_ELIGIBLE_TODAY`, `V_PIPELINE_RUN_SCOPING`, `V_OPPORTUNITY_FEED`, `V_PATTERN_KPIS`, `V_PATTERN_SCORECARD`, `V_TRUSTED_SIGNAL_CLASSIFICATION`

**MIP.AGENT_OUT:**  
`V_MORNING_BRIEF_SUMMARY` — flattens `MORNING_BRIEF` for ops/Streamlit (risk status, proposal/signal counts).

---

## 5. Stored Procedure Catalog (canonical names — use exactly)

**Pipeline:**  
`SP_RUN_DAILY_PIPELINE`, `SP_PIPELINE_INGEST`, `SP_PIPELINE_REFRESH_RETURNS`, `SP_PIPELINE_GENERATE_RECOMMENDATIONS`, `SP_PIPELINE_EVALUATE_RECOMMENDATIONS`, `SP_PIPELINE_RUN_PORTFOLIOS`, `SP_PIPELINE_RUN_PORTFOLIO`, `SP_PIPELINE_WRITE_MORNING_BRIEFS`, `SP_PIPELINE_WRITE_MORNING_BRIEF`

**Agent / brief:**  
`SP_AGENT_PROPOSE_TRADES`, `SP_VALIDATE_AND_EXECUTE_PROPOSALS`, `SP_WRITE_MORNING_BRIEF(P_PORTFOLIO_ID, P_AS_OF_TS, P_RUN_ID, P_AGENT_NAME)`

**Core:**  
`SP_INGEST_ALPHAVANTAGE_BARS`, `SP_GENERATE_MOMENTUM_RECS`, `SP_EVALUATE_RECOMMENDATIONS`, `SP_RUN_PORTFOLIO_SIMULATION`, `SP_LOG_EVENT`, `SP_AUDIT_LOG_STEP`, `SP_ENFORCE_RUN_SCOPING`, `SP_SEED_MIP_DEMO`

**Replay:**  
`SP_REPLAY_TIME_TRAVEL` — time-travel replay for historical runs (see `MIP/SQL/app/149_sp_replay_time_travel.sql`, `MIP/SQL/scripts/replay_time_travel.sql`).

---

## 6. What We Achieved (summary)

- **Pipeline:** End-to-end daily automation; rate limit and no-new-bars handling; idempotent steps.
- **Run ID:** Canonical `RUN_ID_VARCHAR` on `ORDER_PROPOSALS`; dual scoping removed.
- **Morning brief:** Deterministic and idempotent on `(portfolio_id, as_of_ts, run_id, agent_name)`; pipeline passes `as_of_ts`/`run_id`; `V_MORNING_BRIEF_JSON` is content-only.
- **Risk / entry gate:** Drawdown stop → `ALLOW_EXITS_ONLY`; execution blocks BUY when `ENTRIES_BLOCKED=true`.
- **Agent flow:** Propose → validate → execute → write brief; `MORNING_BRIEF`, `ORDER_PROPOSALS` populated.

---

## 7. UI Planning and Current Streamlit Structure

### Intended UI mapping (from KPI_LAYER.md)
- **Portfolio dashboard:** `V_PORTFOLIO_RUN_KPIS`, `V_PORTFOLIO_RUN_EVENTS`
- **Attribution view:** `V_PORTFOLIO_ATTRIBUTION`, `V_PORTFOLIO_ATTRIBUTION_BY_PATTERN`
- **Signal quality and calibration:** `V_SIGNAL_OUTCOME_KPIS`, `V_SCORE_CALIBRATION`, `V_SIGNALS_WITH_EXPECTED_RETURN`

### Phase 3 goal (from roadmap)
Store agent outputs in `AGENT_OUT` and **render in Streamlit UI**.

### Current Streamlit app (streamlit_app.py, ui/layout.py)
- **Stack:** Snowpark `get_active_session()`, runs in Snowflake native app.
- **Pages:** Overview | Opportunities | Portfolio | Training & Trust | Admin / Ops
- **Overview:** Pipeline health, data freshness, portfolio snapshot, opportunities snapshot
- **Opportunities:** Screener using `V_OPPORTUNITY_FEED`, filters
- **Portfolio:** Portfolio config, trades, equity
- **Training & Trust:** Pattern KPIs, backtesting
- **Admin / Ops:** Tabs — Pipeline, Ingestion, Patterns, Audit Log, Advanced (task controls, health checks)
- **Layout helpers:** `apply_layout`, `section_header`, `render_badge` from `ui.layout`

### Views for UI
- **V_MORNING_BRIEF_SUMMARY:** Flattens `MORNING_BRIEF` for ops/Streamlit (risk status, proposal/signal counts).
- **MORNING_BRIEF.BRIEF:** JSON variant with full brief content (signals, risk, attribution, proposals).

---

## 8. Operating Rules

- **Run ID:** `RUN_ID` / `PIPELINE_RUN_ID` are **varchar(64)** UUID strings. Do not treat as numeric.
- **Idempotency:** Re-runs for same `to_ts` must not duplicate outputs.
- **Timestamp policy:** System timestamps use Berlin `CURRENT_TIMESTAMP()`; market timestamps stay as-is.
- **Do not:** Invent table, view, or procedure names; assume views exist without checking the catalog.

---

## 9. Repo Landmarks

- **SQL:** `MIP/SQL/bootstrap/`, `app/`, `mart/`, `views/mart/`, `views/app/`, `checks/`, `smoke/`
- **UI:** `MIP/streamlit_app.py`, `MIP/ui/layout.py`
- **Docs:** `05_DEVELOPER_ONBOARDING.md`, `10_ARCHITECTURE.md`, `40_TABLE_CATALOG.md`, `35_STORED_PROCEDURES.md`, `KPI_LAYER.md`, `ROADMAP_CHECKLIST.md`

---

*Changelog: Replaced with UX-phase carry-over 2026-01-29. Machinery complete; focus on Streamlit UX. Canonical table/view/procedure lists to prevent hallucinated names.*
