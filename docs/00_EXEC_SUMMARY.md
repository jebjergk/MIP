# MIP Executive Summary

## What MIP is
- **MIP (Market Intelligence Platform)** is a Snowflake-native data pipeline that ingests daily market bars, calculates returns, generates recommendations, and evaluates their outcomes inside Snowflake. The core pipeline is orchestrated in `MIP.APP.SP_RUN_DAILY_PIPELINE`, which calls ingestion, returns refresh, recommendation generation, and outcome evaluation steps. This is the canonical daily workflow in the repo.【F:SQL/app/145_sp_run_daily_pipeline.sql†L1-L519】
- **Outputs are structured analytics tables and views** (e.g., `MIP.MART.MARKET_RETURNS`, `MIP.APP.RECOMMENDATION_LOG`, `MIP.APP.RECOMMENDATION_OUTCOMES`, and KPI views in `MIP.MART`).【F:SQL/mart/010_mart_market_bars.sql†L1-L107】【F:SQL/app/050_app_core_tables.sql†L194-L239】【F:SQL/mart/030_mart_rec_outcome_views.sql†L1-L71】

## What MIP is not
- **Not a trading system or broker**: there is no order execution or trade placement in the repository; the pipeline focuses on data ingestion, analytics, and evaluation.【F:SQL/app/145_sp_run_daily_pipeline.sql†L1-L519】
- **Not real-time**: the scheduled task runs once per day, and the default seed universe uses **daily bars (`INTERVAL_MINUTES=1440`)** for STOCK, ETF, and FX markets.【F:SQL/app/050_app_core_tables.sql†L10-L83】

## Current scope
- **Market types:** STOCK, ETF, and FX are explicitly seeded in the ingestion universe.【F:SQL/app/050_app_core_tables.sql†L10-L83】
- **Time resolution:** daily OHLC (open/high/low/close) bars are represented by `INTERVAL_MINUTES=1440` in the seed universe.【F:SQL/app/050_app_core_tables.sql†L10-L83】
- **Daily pipeline = source of truth:** `TASK_RUN_DAILY_PIPELINE` triggers the stored procedure `SP_RUN_DAILY_PIPELINE`, which runs the canonical sequence: **ingest → returns → recommendations → evaluation**. (Portfolio simulation exists separately but is not part of the daily task in this repo.)【F:SQL/app/150_task_run_daily_training.sql†L1-L14】【F:SQL/app/145_sp_run_daily_pipeline.sql†L1-L519】

## Known unknowns / TODO
- **Missing from repo:** None identified for the objects explicitly requested in this documentation pack.
