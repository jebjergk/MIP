# MIP Executive Summary

## What MIP is
- **MIP (Market Intelligence Platform)** is a Snowflake-native data pipeline that ingests daily market bars, calculates returns, generates recommendations, evaluates outcomes, runs portfolio simulations, and writes morning briefs inside Snowflake. The canonical orchestration lives in `MIP.APP.SP_RUN_DAILY_PIPELINE`, which chains ingestion, returns refresh, recommendations, evaluation, portfolio simulation, and brief persistence.【F:SQL/app/145_sp_run_daily_pipeline.sql†L1-L108】【F:SQL/app/147_sp_pipeline_run_portfolios.sql†L1-L120】【F:SQL/app/148_sp_pipeline_write_morning_briefs.sql†L1-L86】
- **Outputs are structured analytics tables and views** (e.g., `MIP.MART.MARKET_RETURNS`, `MIP.APP.RECOMMENDATION_LOG`, `MIP.APP.RECOMMENDATION_OUTCOMES`, portfolio KPIs in `MIP.MART`, and persisted morning briefs in `MIP.AGENT_OUT.MORNING_BRIEF`).【F:SQL/mart/010_mart_market_bars.sql†L1-L107】【F:SQL/app/050_app_core_tables.sql†L194-L239】【F:SQL/views/mart/v_portfolio_run_kpis.sql†L1-L122】【F:SQL/app/185_agent_out_morning_brief.sql†L1-L17】

## What MIP is not
- **Not a trading system or broker**: there is no order execution or trade placement in the repository; the pipeline focuses on data ingestion, analytics, and evaluation (plus paper portfolio simulation).【F:SQL/app/145_sp_run_daily_pipeline.sql†L1-L108】【F:SQL/app/180_sp_run_portfolio_simulation.sql†L1-L120】
- **Not real-time**: the scheduled task runs once per day, and the default seed universe uses **daily bars (`INTERVAL_MINUTES=1440`)** for STOCK, ETF, and FX markets.【F:SQL/app/050_app_core_tables.sql†L10-L83】

## Current scope
- **Market types (ETF supported):** STOCK, ETF, and FX are explicitly seeded in the ingestion universe, and the daily pipeline generates recommendations per market type discovered in that universe.【F:SQL/app/050_app_core_tables.sql†L10-L83】【F:SQL/app/145_sp_run_daily_pipeline.sql†L31-L80】
- **Time resolution:** daily OHLC (open/high/low/close) bars are represented by `INTERVAL_MINUTES=1440` in the seed universe.【F:SQL/app/050_app_core_tables.sql†L10-L83】
- **Daily pipeline = source of truth:** `TASK_RUN_DAILY_PIPELINE` triggers the stored procedure `SP_RUN_DAILY_PIPELINE`, which runs the canonical sequence: **ingest → returns → recommendations → evaluation → portfolio simulation → morning brief**.【F:SQL/app/150_task_run_daily_training.sql†L1-L14】【F:SQL/app/145_sp_run_daily_pipeline.sql†L31-L98】

## Known unknowns / TODO
- **Missing from repo:** None identified for the objects explicitly requested in this documentation pack.
