# ChatGPT Carry-Over Summary

**Copy the entire content below (from "## Context for ChatGPT" through the final paragraph) into a new ChatGPT message.** (GPT has no file access; the summary includes exact object names so GPT does not guess.)

---

## Context for ChatGPT (carry-over summary)

**Project:** MIP (mip_0.7) — a Snowflake-based investment/portfolio pipeline. SQL objects live in database `MIP` with schemas such as `MIP.APP`, `MIP.MART`, `MIP.AGENT_OUT`. There is a daily pipeline procedure `SP_RUN_DAILY_PIPELINE` and various stored procedures, views, and tables for morning briefs, order proposals, and risk/attribution.

**Important convention:** The canonical run identifier is a **pipeline RUN_ID** (UUID string). Any `RUN_ID` or `PIPELINE_RUN_ID` columns are varchar(64) for this UUID. Do not treat run IDs as numeric.

---

### Canonical object names (copy exactly)

**Tables (MIP.APP):**  
`MIP.APP.PORTFOLIO`, `MIP.APP.PORTFOLIO_TRADES`, `MIP.APP.PORTFOLIO_POSITIONS`, `MIP.APP.PORTFOLIO_DAILY`, `MIP.APP.MIP_AUDIT_LOG`, `MIP.APP.RECOMMENDATION_LOG`, `MIP.APP.RECOMMENDATION_OUTCOMES`, `MIP.APP.INGEST_UNIVERSE`, `MIP.APP.PATTERN_DEFINITION`, `MIP.APP.TRAINING_GATE_PARAMS`

**Tables (MIP.AGENT_OUT):**  
`MIP.AGENT_OUT.MORNING_BRIEF`, `MIP.AGENT_OUT.ORDER_PROPOSALS`, `MIP.AGENT_OUT.AGENT_RUN_LOG`

**Tables (MIP.MART):**  
`MIP.MART.MARKET_BARS`, `MIP.MART.MARKET_RETURNS` (view in code but often referred to as core mart)

**Views (morning brief / agent flow):**  
`MIP.AGENT_OUT.V_MORNING_BRIEF_SUMMARY`  
`MIP.MART.V_MORNING_BRIEF_JSON`  
`MIP.MART.V_PORTFOLIO_RISK_GATE`, `MIP.MART.V_PORTFOLIO_RISK_STATE`  
`MIP.APP.V_SIGNALS_ELIGIBLE_TODAY`  
`MIP.MART.V_TRUSTED_SIGNAL_POLICY`, `MIP.MART.V_TRUSTED_SIGNALS`, `MIP.MART.V_TRUSTED_SIGNALS_LATEST_TS`  
`MIP.MART.V_PORTFOLIO_RUN_KPIS`, `MIP.MART.V_PORTFOLIO_RUN_EVENTS`

**Stored procedures (pipeline and morning brief):**  
`MIP.APP.SP_RUN_DAILY_PIPELINE`  
`MIP.APP.SP_WRITE_MORNING_BRIEF`  
`MIP.APP.SP_PIPELINE_WRITE_MORNING_BRIEF`, `MIP.APP.SP_PIPELINE_WRITE_MORNING_BRIEFS`  
`MIP.APP.SP_AGENT_PROPOSE_TRADES`, `MIP.APP.SP_VALIDATE_AND_EXECUTE_PROPOSALS`  
`MIP.APP.SP_PIPELINE_INGEST`, `MIP.APP.SP_PIPELINE_REFRESH_RETURNS`, `MIP.APP.SP_PIPELINE_GENERATE_RECOMMENDATIONS`, `MIP.APP.SP_PIPELINE_EVALUATE_RECOMMENDATIONS`, `MIP.APP.SP_PIPELINE_RUN_PORTFOLIO`, `MIP.APP.SP_PIPELINE_RUN_PORTFOLIOS`  
`MIP.APP.SP_RUN_PORTFOLIO_SIMULATION`  
`MIP.APP.SP_LOG_EVENT`, `MIP.APP.SP_ENFORCE_RUN_SCOPING`

**Check script (name only, no path):**  
`morning_brief_consistency.sql` — validates brief counts vs `ORDER_PROPOSALS`.

---

### What we worked on lately

1. **Morning brief persistence and attribution**
   - **Table:** `MIP.AGENT_OUT.MORNING_BRIEF` — stores morning brief outputs (portfolio briefs and agent briefs). Key columns: `PORTFOLIO_ID`, `AS_OF_TS`, `RUN_ID`, `AGENT_NAME`, `PIPELINE_RUN_ID`, `BRIEF` (variant/JSON).
   - **Stored procedure:** `SP_WRITE_MORNING_BRIEF(P_PORTFOLIO_ID, P_AS_OF_TS, P_RUN_ID, P_AGENT_NAME)` — reads brief content from `MIP.MART.V_MORNING_BRIEF_JSON`, then **overwrites attribution** in the brief with only `pipeline_run_id` and `as_of_ts` (no `latest_run_id`). Deterministic and idempotent on `(PORTFOLIO_ID, AS_OF_TS, RUN_ID, AGENT_NAME)`.
   - **Pipeline wrapper:** `SP_PIPELINE_WRITE_MORNING_BRIEF` (in `SP_RUN_DAILY_PIPELINE`) calls `SP_WRITE_MORNING_BRIEF` with the pipeline's run_id.
   - **View:** `MIP.AGENT_OUT.V_MORNING_BRIEF_SUMMARY` — flattens `MORNING_BRIEF` for ops/Streamlit (e.g. `PIPELINE_RUN_ID` from `BRIEF:pipeline_run_id`, risk status, proposal/signal counts).

2. **Run ID and merge key alignment (schema fix)**
   - `PIPELINE_RUN_ID` and `RUN_ID` were made **varchar(64)** so pipeline UUIDs can be stored and compared (fixes "Numeric value '...' is not recognized" when comparing to UUID strings).
   - **Merge/uniqueness key** for `MORNING_BRIEF` is `(PORTFOLIO_ID, AS_OF_TS, RUN_ID, AGENT_NAME)` — unique constraint `UQ_MORNING_BRIEF_AS_OF_RUN_AGENT`. Old key `(PORTFOLIO_ID, RUN_ID)` was dropped so multiple rows per run (e.g. different `AS_OF_TS`) are allowed.

3. **Order proposals**
   - **Table:** `MIP.AGENT_OUT.ORDER_PROPOSALS` — stores agent trade proposals (RUN_ID, PORTFOLIO_ID, SYMBOL, SIDE, TARGET_WEIGHT, STATUS, etc.). Canonical run key is pipeline `RUN_ID`; `SIGNAL_RUN_ID` is optional/legacy.

4. **Morning brief consistency checks**
   - **Check:** `morning_brief_consistency.sql` — validates that brief JSON proposal/execution counts match `ORDER_PROPOSALS` (e.g. proposed_count, approved_count, executed_count). Used to ensure brief content and ORDER_PROPOSALS stay in sync.

5. **Smoke tests and manual tests**
   - **Attribution smoke:** New briefs must not have `attribution:latest_run_id` (expect 0 rows where that field is present).
   - **Idempotency smoke:** Calling `SP_WRITE_MORNING_BRIEF` twice with same params should be idempotent.
   - **Kens tests (`01_Kens_tests.sql`):** Ad-hoc flow: set `test_run_id`, `as_of_ts`, `portfolio_id`; call `SP_WRITE_MORNING_BRIEF`; inspect `MORNING_BRIEF` (e.g. `brief:"attribution"`, `pipeline_run_id`); optionally archive "legacy" briefs (where `agent_name != 'MORNING_BRIEF'` or `brief:"pipeline_run_id" != run_id`) into `MORNING_BRIEF_LEGACY_ARCHIVE` and delete those from `MORNING_BRIEF`; check `V_MORNING_BRIEF_SUMMARY` for nulls/counts.

6. **Brief content source**
   - **View:** `MIP.MART.V_MORNING_BRIEF_JSON` — content-only (PORTFOLIO_ID, BRIEF). Builds the brief JSON (trusted signals, watch lists, risk, proposals summary, etc.). Attribution in the **written** brief is overwritten by `SP_WRITE_MORNING_BRIEF` with `pipeline_run_id` and `as_of_ts` only.

---

**For you (ChatGPT):** You do not have access to the codebase or file paths. Use this summary as the source of truth for schema names, procedure behavior, attribution rules, run_id handling, and recent cleanup/consistency work. Use the **canonical object names** listed above exactly; do not invent or guess table, view, or procedure names. When the user asks about "morning brief," "attribution," "run_id," "ORDER_PROPOSALS," or "consistency," refer to the above.
