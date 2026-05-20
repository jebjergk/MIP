# MIP Introduction Deck — Repository Findings

Internal reference written before/with the introduction presentation. Slide copy for **implemented paths** must align with tables here. **Slides 15–16** use approved *target-architecture* language ( literature RAG layer; Cortex Code + Cursor workflow); those items remain **Not built / envisioned** below.

---

## Key files reviewed (representative)

| Area | Paths |
|------|-------|
| Daily pipeline orchestration | `MIP/SQL/app/145_sp_run_daily_pipeline.sql`, `142_sp_pipeline_ingest.sql`, `143_sp_pipeline_refresh_returns.sql`, `144_sp_pipeline_generate_recommendations.sql`, `146_sp_pipeline_evaluate_recommendations.sql`, `147_sp_pipeline_run_portfolios.sql`, `148_sp_pipeline_write_morning_briefs.sql` |
| Snowflake scheduling | `MIP/SQL/app/150_task_run_daily_training.sql` → `TASK_RUN_DAILY_PIPELINE` |
| Ingest routing | `MIP/SQL/app/401_sp_ingest_market_bars.sql`, `030_sp_ingest_alphavantage_bars.sql`; `cursorfiles/ingest_ibkr_bars.py` |
| Workflows narrative | `MIP/docs/30_WORKFLOWS.md`, `MIP/docs/10_ARCHITECTURE.md` |
| Structural daily DAG | `MIP/SQL/app/521_structural_pipeline_and_views.sql` (`SP_RUN_STRUCTURAL_DAILY_PIPELINE`), `500_structural_strategy_foundation_tables.sql`, `511_sp_compute_structural_trust.sql`, level/setup detect procs referenced in `521_*` |
| Structural timeline mart | `MIP/SQL/views/app/v_structural_timeline_views.sql` |
| Structural training mart | `MIP/SQL/views/app/v_structural_training_views.sql` |
| Proposal board dossier | `MIP/SQL/views/mart/v_proposal_board_symbol_dossier.sql`, `v_proposal_board_candidate_evidence.sql` |
| Parallel worlds | `MIP/apps/mip_ui_api/app/routers/parallel_worlds.py`, `MIP/SQL/views/mart/v_parallel_world_regret.sql` |
| Ask MIP | `MIP/apps/mip_ui_web/src/components/AskMipPanel.jsx`, `MIP/apps/mip_ui_api/app/routers/ask.py`, `MIP/apps/mip_ui_api/app/services/ask/orchestrator.py`, `MIP/docs/ask_mip/ask_mip_v2_design.md` |
| Phase 4 board | `MIP/scripts/proposal_board_phase4/run_board.py`, `orchestrator.py` |
| Committee API | `MIP/apps/mip_ui_api/app/routers/committee.py`, `MIP/apps/mip_ui_api/app/committee/` |
| LPA UI | `MIP/apps/mip_ui_web/src/pages/LivePortfolioActivity.jsx`, `LpaCommittee2Exhibits.jsx` |
| Live / IBKR | `MIP/apps/mip_ui_api/app/routers/live.py`, `MIP/apps/mip_ui_web/src/pages/LivePortfolioConfig.jsx`; `MIP/SQL/migrations/20260501_ibkr_account_mode.sql` |
| Manual daily job trigger | `MIP/apps/mip_ui_api/app/routers/management.py` (`POST /ib/daily-job/run`, optional `run_proposal_board`) |
| Position health | `MIP/SQL/app/551_sp_run_daily_position_verdict.sql`, `550_daily_position_verdict_tables.sql`; `PositionHealth.jsx` |
| Routing map | `MIP/apps/mip_ui_web/src/App.jsx`, `guide/index.js` |
| Existing deck tooling | `cursorfiles/generate_mip_presentation.py` (palette + layout patterns), `MIP/docs/build_mip_reference_style_deck.py` (expects `MIP_Presentation_v2.pptx` at repo root — not tracked here) |

**Schedule correction:** Use `150_task_run_daily_training.sql`: `USING CRON 0 17 * * MON-FRI America/New_York` for `TASK_RUN_DAILY_PIPELINE`. Some markdown (`30_WORKFLOWS.md`) still mentions Europe/Berlin — treat SQL as authoritative.

---

## Topic matrix (1–16)

| # | Topic | Implemented | Partially implemented | Planned / not in repo |
|---|--------|-------------|------------------------|----------------------|
| 1 | Data ingestion | **`SP_PIPELINE_INGEST` → `SP_INGEST_MARKET_BARS`**; bars in **`MIP.MART.MARKET_BARS`**. Provider routing via **`APP_CONFIG.MARKET_DATA_PROVIDER_DEFAULT`**; **`cursorfiles/ingest_ibkr_bars.py`** IB path; **`SP_INGEST_ALPHAVANTAGE_BARS`** legacy. | Synth intraday helper flags on **`POST /ib/daily-job/run`** (`synth_intraday_daily`). | — |
| 2 | Daily pipeline | **`SP_RUN_DAILY_PIPELINE`** (returns, momentum recs, outcomes, portfolio sim, briefs, audit); **`TASK_RUN_DAILY_PIPELINE`**. | Fail-soft structural branch inside `145_*`. Parallel worlds sweep steps conditional in same proc. | — |
| 3 | Training / evidence | **Momentum:** `RECOMMENDATION_LOG`, `RECOMMENDATION_OUTCOMES`, horizons 1/3/5/10/20. **Structural:** `STRUCTURAL_SETUP_OUTCOMES`, **`SP_EVALUATE_STRUCTURAL_OUTCOMES`**, **`SP_COMPUTE_STRUCTURAL_TRUST`**, **`STRUCTURAL_SETUP_TRUST`** (meaningful/path stats, trust labels PROVISIONAL/TRUSTED/etc. per codebase). **`V_STRUCTURAL_TRAINING_*`**, training UI `/structural-training`, `/training`. | — | — |
| 4 | Structural pattern detection | **`SP_RUN_STRUCTURAL_DAILY_PIPELINE`** steps: **`SP_DETECT_STRUCTURAL_LEVELS`**, **`SP_COMPUTE_STRUCTURAL_STATE`**, **`SP_COMPUTE_REGIME_TAGS`**, **`SP_DETECT_STRUCTURAL_SETUPS`**, **`SP_UPDATE_SETUP_LIFECYCLE`**. Setup families and narratives in **`V_STRUCTURAL_TIMELINE_SETUPS`** (e.g. breakout/retest, wick, pullback, failed breakout naming in view SQL). **`v_symbol_market_structure_map`** for broader structure tagging. | — | — |
| 5 | Setup families | **`STRUCTURAL_SETUP_EVENTS.SETUP_FAMILY`**, policy **`STRUCTURAL_RISK_POLICY`** (see timeline view joins). | ETF exclusion patterns (`MARKET_TYPE != 'ETF'`) in several timeline views. | — |
| 6 | Structural timeline | **`MIP.MART.V_STRUCTURAL_TIMELINE_*`** rail + setups; **`/structural-timeline`**; **`GET /structural-timeline/detail`**. | — | — |
| 7 | Proposal generation | **Deterministic:** **`SP_PROPOSE_STRUCTURAL_TRADES`** inside structural pipeline. **`SP_EXPIRE_STALE_DAILY_PROPOSALS`** at pipeline start. | Agentic publishes rows with **`BOARD_RUN_ID`**; timeline views distinguish agentic vs legacy (`BOARD_RUN_ID IS NOT NULL`). | — |
| 8 | Agentic proposal board / committee prep | **`run_board.py`** Phase 4 Cortex orchestration → **`STRUCTURAL_TRADE_PROPOSALS`**; dossier/evidence **`V_PROPOSAL_BOARD_*`**. Wired from **`POST /ib/daily-job/run`** when `run_proposal_board=true`. **`IBKR_ACCOUNT_MODE`** gate for shorts (migration + `orchestrator.py`). | — | Full automation vs operator triggers varies by deployment. |
| 9 | Committee / agentic revalidation | **`/structural-committee`**, **`committee.py`** (hearings open/refresh/commit, shadow board routes). **`LpaCommittee2Exhibits`** embedded in LPA. | Depth of committee2 rollout feature-flag dependent (`_committee2_enabled` pattern in router). | — |
|10 | Live portfolio activity | **`/live-portfolio-activity`**, **`LivePortfolioActivity.jsx`**: actions, drift/reason codes, revalidation staleness (**`EXECUTION_CLICK_REVALIDATION_*`**, **`PRICE_GUARD_FAIL`**, etc.). | — | — |
|11 | Trade configuration | **`/live-portfolio-config`**; **`MIP.LIVE.LIVE_PORTFOLIO_CONFIG`** ( **`ADAPTER_MODE`**, **`IBKR_ACCOUNT_ID`**, **`IBKR_ACCOUNT_MODE`**, brackets/risk knobs as exposed in SQL + UI). **`live.py`** bracket realism, execute paths. | Terminology **`ADAPTER_MODE`** vs **`IBKR_ACCOUNT_MODE`** still confusing in UX (documented backlog in `phase4_cleanup_backlog.md`). | — |
|12 | IBKR integration | **`ADAPTER_MODE`='LIVE'** drives IBKR submit when not overridden by **`LIVE_EXECUTION_MODE`** (`live.py`). Ingest via **`ingest_ibkr_bars.py`** + pipeline. Explicit paper/real guard: **`IBKR_ACCOUNT_MODE`**. | — | — |
|13 | Agentic revalidation / position review | **`SP_RUN_DAILY_POSITION_VERDICT`** (thesis integrity, fragility, etc.), **`POSITION_HEALTH_ENABLED`** toggle in daily pipeline; **`/position-health`**. | “Agent” wording: daily verdict SQL is deterministic; Cortex agents elsewhere for board/Ask. | — |
|14 | Ask MIP | **`POST /ask/v3`**; Snowflake **`Cortex COMPLETE`** via `ask.py` / orchestrator; guide + glossary grounding per `ask_mip_v2_design.md`. | Web fallback policies as configured. | **Literature corpus RAG** (slide 15 design) |
|15 | Parallel Worlds | **`/parallel-worlds`**; **`V_PARALLEL_WORLD_REGRET`**, actuals join pattern in **`parallel_worlds.py`**; scenarios table **`PARALLEL_WORLD_SCENARIO`**. | Some endpoints `.catch(() => null)` in UI — optional panels. | — |
|16 | Literature RAG (concept cards) | — | **`Ask_mip`** doc-first + glossary (**not** unstructured book ingestion). | **Target:** external literature → structured **concept cards**, observability tags vs MIP data, debate-only input for proposal/revalidation/position-health narratives; **no** decision authority swap. **Not implemented** as of this audit. |
|17 | Cortex Code + Cursor (engineering) | — | Developers use Cursor on this repo; Snowflake DDL in **`MIP/SQL/**`**. | **Target:** Snowflake Cortex **Code** to accelerate proc/view authoring, smoke scaffolding, semantic comments — **human-reviewed** PR workflow. **No** Cortex Code integration in-repo. |

---

## UI routes worth capturing for slides

From `App.jsx`:

- `/cockpit` — operational hub  
- `/structural-training` — structural evidence  
- `/training` — training status (momentum-style digest)  
- `/structural-timeline` — market memory rail  
- `/structural-committee`, `/structural-committee/:hearingId`  
- `/live-portfolio-activity`, `/live-portfolio-config`  
- `/parallel-worlds`  
- `/position-health`  
- Ask MIP **FAB / slide-over** (global)  
- `/runs`, `/runs/:runId` — audit trail  

---

## Exact named objects cited in slides (short list)

**Procedures (sample):**  
`SP_RUN_DAILY_PIPELINE`, `SP_PIPELINE_INGEST`, `SP_INGEST_MARKET_BARS`, `SP_INGEST_ALPHAVANTAGE_BARS`, `SP_PIPELINE_REFRESH_RETURNS`, `SP_GENERATE_MOMENTUM_RECS`, `SP_PIPELINE_EVALUATE_RECOMMENDATIONS`, `SP_EVALUATE_RECOMMENDATIONS`, `SP_RUN_PORTFOLIO_SIMULATION`, `SP_WRITE_MORNING_BRIEF`, `SP_AGENT_PROPOSE_TRADES`, `SP_VALIDATE_AND_EXECUTE_PROPOSALS`, `SP_RUN_STRUCTURAL_DAILY_PIPELINE`, `SP_DETECT_STRUCTURAL_LEVELS`, `SP_COMPUTE_STRUCTURAL_STATE`, `SP_COMPUTE_REGIME_TAGS`, `SP_DETECT_STRUCTURAL_SETUPS`, `SP_UPDATE_SETUP_LIFECYCLE`, `SP_EVALUATE_STRUCTURAL_OUTCOMES`, `SP_COMPUTE_STRUCTURAL_TRUST`, `SP_PROPOSE_STRUCTURAL_TRADES`, `SP_EXPIRE_STALE_DAILY_PROPOSALS`, `SP_RUN_DAILY_POSITION_VERDICT`, `SP_LOG_EVENT`.

**Tables (sample):**  
`MARKET_BARS`, `MARKET_RETURNS`, `RECOMMENDATION_LOG`, `RECOMMENDATION_OUTCOMES`, `PORTFOLIO_DAILY`, `PORTFOLIO_TRADES`, `MIP_AUDIT_LOG`, `STRUCTURAL_SETUP_EVENTS`, `STRUCTURAL_SETUP_OUTCOMES`, `STRUCTURAL_SETUP_TRUST`, `STRUCTURAL_LEVEL_CACHE`, `STRUCTURAL_TRADE_PROPOSALS`, `LIVE_PORTFOLIO_CONFIG`, `MORNING_BRIEF` (AGENT_OUT).

**Views (sample):**  
`V_STRUCTURAL_TIMELINE_SETUPS`, `V_STRUCTURAL_TIMELINE_EVENTS`, `V_STRUCTURAL_TIMELINE_LEVELS`, `V_STRUCTURAL_TRAINING_SYMBOL`, `V_PROPOSAL_BOARD_SYMBOL_DOSSIER`, `V_PARALLEL_WORLD_REGRET`, `V_MORNING_BRIEF_JSON`.

**REST (sample):**  
`/ask/v3`, `/structural-timeline/detail`, `/parallel-worlds/*`, `/committee/*`, `/live/*`, `/ib/daily-job/run`, management routes on `management.py`.

**Scripts:**  
`MIP/scripts/proposal_board_phase4/run_board.py`, `cursorfiles/ingest_ibkr_bars.py`, `cursorfiles/query_snowflake.py`.

**Tasks:**  
`TASK_RUN_DAILY_PIPELINE` (active schedule per `150_*`); **`TASK_RUN_INTRADAY_PIPELINE`** exists but **`suspend`** — on hold (`325_task_run_intraday_pipeline.sql`).

---

## Acceptance cross-check

- Slides **1–14, 17–20** operational claims trace to this file or linked SQL/UI.  
- Slides **15–16**: on-deck footer marks **architecture target — not yet in production**; this file marks both **Planned**.  
- No invented **`INFORMATION_SCHEMA` / mystery tables** for RAG/Cortex Code.
