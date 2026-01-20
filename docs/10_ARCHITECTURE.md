# MIP Architecture (Snowflake-native)

## Component overview
- **Ingestion (Snowflake stored procedure + external access)**: `MIP.APP.SP_INGEST_ALPHAVANTAGE_BARS` pulls bars from AlphaVantage using an external access integration and writes them into `MIP.MART.MARKET_BARS`.【F:SQL/app/030_sp_ingest_alphavantage_bars.sql†L1-L220】
- **Core analytics (views and tables in MART/APP)**: `MIP.MART.MARKET_RETURNS` is built from `MIP.MART.MARKET_BARS`, recommendations are stored in `MIP.APP.RECOMMENDATION_LOG`, and evaluation results land in `MIP.APP.RECOMMENDATION_OUTCOMES`.【F:SQL/mart/010_mart_market_bars.sql†L1-L107】【F:SQL/app/050_app_core_tables.sql†L194-L239】
- **Orchestration**: `MIP.APP.TASK_RUN_DAILY_PIPELINE` schedules a daily run of `MIP.APP.SP_RUN_DAILY_PIPELINE`.【F:SQL/app/150_task_run_daily_training.sql†L1-L14】
- **Monitoring & audit**: `MIP.APP.SP_LOG_EVENT` writes to `MIP.APP.MIP_AUDIT_LOG` for pipeline and step-level logging.【F:SQL/app/055_app_audit_log.sql†L1-L54】

## Schemas and responsibilities
- **`MIP.RAW_EXT`**: raw external data (staging/ingestion footprint).【F:SQL/bootstrap/001_bootstrap_mip_infra.sql†L29-L43】
- **`MIP.MART`**: analytic models and views (e.g., `MARKET_BARS`, `MARKET_RETURNS`, KPI views).【F:SQL/bootstrap/001_bootstrap_mip_infra.sql†L29-L43】【F:SQL/mart/010_mart_market_bars.sql†L1-L107】【F:SQL/mart/030_mart_rec_outcome_views.sql†L1-L71】
- **`MIP.APP`**: application tables, stored procedures, and configuration for the pipeline (recommendation logs, outcomes, audit log, stored procedures).【F:SQL/bootstrap/001_bootstrap_mip_infra.sql†L29-L43】【F:SQL/app/050_app_core_tables.sql†L194-L239】【F:SQL/app/055_app_audit_log.sql†L1-L54】
- **`MIP.AGENT_OUT`**: reserved for future narrative/agent outputs (not used in the daily pipeline today).【F:SQL/bootstrap/001_bootstrap_mip_infra.sql†L29-L43】

## Roles and warehouse assumptions
- **Roles**: `MIP_ADMIN_ROLE` (full control), `MIP_APP_ROLE` (read MART/AGENT_OUT + execute APP procedures).【F:SQL/bootstrap/001_bootstrap_mip_infra.sql†L7-L92】
- **Warehouse**: `MIP_WH_XS` is defined for ingestion, analytics, and Streamlit runtime usage and is referenced by the daily task.【F:SQL/bootstrap/001_bootstrap_mip_infra.sql†L16-L23】【F:SQL/app/150_task_run_daily_training.sql†L8-L13】

## Known unknowns / TODO
- **Missing from repo:** None identified for the objects explicitly requested in this documentation pack.
