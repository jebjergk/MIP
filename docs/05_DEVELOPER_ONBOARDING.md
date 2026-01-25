# MIP Developer Onboarding (Current State)

## Purpose
This guide summarizes the **latest delivered capabilities** and where they live in the repo so a second developer can orient quickly.

## Latest delivered capabilities (what exists today)
- **Daily pipeline orchestration**: `MIP.APP.SP_RUN_DAILY_PIPELINE` is the canonical entry point, and `MIP.APP.TASK_RUN_DAILY_PIPELINE` schedules it once per day. The procedure runs ingestion → returns → recommendations → evaluation → portfolio simulation → morning brief persistence.【F:SQL/app/145_sp_run_daily_pipeline.sql†L1-L108】【F:SQL/app/150_task_run_daily_training.sql†L1-L14】
- **Market ingestion (AlphaVantage)**: `MIP.APP.SP_INGEST_ALPHAVANTAGE_BARS` ingests daily bars into `MIP.MART.MARKET_BARS` using external access integration support.【F:SQL/app/030_sp_ingest_alphavantage_bars.sql†L1-L220】
- **Core analytics tables/views**: `MIP.MART.MARKET_RETURNS` is built from `MIP.MART.MARKET_BARS`; recommendations and evaluation results land in `MIP.APP.RECOMMENDATION_LOG` and `MIP.APP.RECOMMENDATION_OUTCOMES`.【F:SQL/mart/010_mart_market_bars.sql†L1-L107】【F:SQL/app/050_app_core_tables.sql†L194-L239】
- **Portfolio simulation + KPIs**: portfolio runs are produced by `MIP.APP.SP_RUN_PORTFOLIO_SIMULATION`, with KPI rollups in `MIP.MART.V_PORTFOLIO_RUN_KPIS`.【F:SQL/app/180_sp_run_portfolio_simulation.sql†L1-L120】【F:SQL/views/mart/v_portfolio_run_kpis.sql†L1-L122】
- **Morning brief output**: `MIP.MART.V_MORNING_BRIEF_JSON` composes the brief content (including deltas), and `MIP.APP.SP_WRITE_MORNING_BRIEF` persists it to `MIP.AGENT_OUT.MORNING_BRIEF`.【F:SQL/views/mart/v_morning_brief_json.sql†L1-L139】【F:SQL/views/mart/v_morning_brief_with_delta.sql†L1-L190】【F:SQL/app/186_sp_write_morning_brief.sql†L1-L48】【F:SQL/app/185_agent_out_morning_brief.sql†L1-L17】
- **Audit logging**: pipeline steps use `MIP.APP.SP_LOG_EVENT` to write structured entries into `MIP.APP.MIP_AUDIT_LOG`.【F:SQL/app/055_app_audit_log.sql†L1-L54】

## Repo map (where to look)
- **SQL/**: all Snowflake DDL, procedures, tasks, and views.
  - `SQL/bootstrap`: roles/warehouses/schemas bootstrap definitions.【F:SQL/bootstrap/001_bootstrap_mip_infra.sql†L1-L92】
  - `SQL/app`: stored procedures, tasks, and application tables (pipeline, logging, recommendation tables).【F:SQL/app/145_sp_run_daily_pipeline.sql†L1-L108】【F:SQL/app/055_app_audit_log.sql†L1-L54】
  - `SQL/mart`: analytic tables (market bars/returns, outcomes).【F:SQL/mart/010_mart_market_bars.sql†L1-L107】【F:SQL/mart/030_mart_rec_outcome_views.sql†L1-L71】
  - `SQL/views`: curated views (KPIs, morning brief outputs).【F:SQL/views/mart/v_portfolio_run_kpis.sql†L1-L122】【F:SQL/views/mart/v_morning_brief_json.sql†L1-L139】
- **docs/**: documentation pack for architecture, lineage, workflows, and troubleshooting.
- **ui/**: Streamlit front-end assets for the UI experience.
- **streamlit_app.py**: Streamlit entry point for the app.

## Recommended read order for a new developer
1. **Architecture overview**: `docs/10_ARCHITECTURE.md` (components + schemas).
2. **Workflow sequencing**: `docs/30_WORKFLOWS.md` (pipeline steps).
3. **Stored procedures**: `docs/35_STORED_PROCEDURES.md` (entry points + how they chain).
4. **Data models & KPIs**: `docs/40_TABLE_CATALOG.md` and `docs/50_KPIS_AND_TRAINING.md`.
5. **Troubleshooting**: `docs/60_RUNBOOK_TROUBLESHOOTING.md`.

## Common first tasks
- **Trace a daily run** by starting with `SP_RUN_DAILY_PIPELINE` and following the invoked procedures to understand the full DAG.【F:SQL/app/145_sp_run_daily_pipeline.sql†L1-L108】
- **Inspect portfolio performance** through `MIP.MART.V_PORTFOLIO_RUN_KPIS` and attribution views to understand run outcomes.【F:SQL/views/mart/v_portfolio_run_kpis.sql†L1-L122】【F:SQL/views/mart/v_portfolio_attribution.sql†L1-L120】
- **Verify morning brief payloads** via `V_MORNING_BRIEF_JSON` and the persisted `MIP.AGENT_OUT.MORNING_BRIEF` table.【F:SQL/views/mart/v_morning_brief_json.sql†L1-L139】【F:SQL/app/185_agent_out_morning_brief.sql†L1-L17】
