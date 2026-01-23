# MIP Workflows

## 1) Daily pipeline run (scheduled)
The daily task `MIP.APP.TASK_RUN_DAILY_PIPELINE` runs at 07:00 Europe/Berlin and calls `MIP.APP.SP_RUN_DAILY_PIPELINE`.【F:SQL/app/150_task_run_daily_training.sql†L1-L14】

**Steps inside the pipeline**:
1. **Ingest bars**: `SP_PIPELINE_INGEST` wraps `SP_INGEST_ALPHAVANTAGE_BARS` to upsert the latest bars into `MIP.MART.MARKET_BARS`.【F:SQL/app/145_sp_run_daily_pipeline.sql†L31-L38】【F:SQL/app/142_sp_pipeline_ingest.sql†L1-L63】【F:SQL/app/030_sp_ingest_alphavantage_bars.sql†L407-L450】
2. **Refresh returns**: `SP_PIPELINE_REFRESH_RETURNS` recreates `MIP.MART.MARKET_RETURNS` from `MARKET_BARS` with simple/log returns per bar.【F:SQL/app/145_sp_run_daily_pipeline.sql†L39-L40】【F:SQL/app/143_sp_pipeline_refresh_returns.sql†L1-L104】
3. **Generate recommendations (ETF included)**: `SP_PIPELINE_GENERATE_RECOMMENDATIONS` calls `SP_GENERATE_MOMENTUM_RECS` for each market type in the ingest universe (STOCK/ETF/FX), inserting into `MIP.APP.RECOMMENDATION_LOG`.【F:SQL/app/145_sp_run_daily_pipeline.sql†L55-L80】【F:SQL/app/144_sp_pipeline_generate_recommendations.sql†L1-L120】【F:SQL/app/050_app_core_tables.sql†L10-L83】
4. **Evaluate outcomes**: `SP_PIPELINE_EVALUATE_RECOMMENDATIONS` upserts forward returns into `MIP.APP.RECOMMENDATION_OUTCOMES` for multiple horizons (1, 3, 5, 10, 20 bars).【F:SQL/app/145_sp_run_daily_pipeline.sql†L86-L88】【F:SQL/app/146_sp_pipeline_evaluate_recommendations.sql†L1-L74】【F:SQL/app/105_sp_evaluate_recommendations.sql†L33-L115】
5. **Run portfolio simulations**: `SP_PIPELINE_RUN_PORTFOLIOS` loops active portfolios and calls `SP_RUN_PORTFOLIO_SIMULATION`, writing portfolio daily/trade/position tables and auditing results.【F:SQL/app/145_sp_run_daily_pipeline.sql†L89-L90】【F:SQL/app/147_sp_pipeline_run_portfolios.sql†L1-L120】【F:SQL/app/180_sp_run_portfolio_simulation.sql†L1-L180】
6. **Persist morning briefs**: `SP_PIPELINE_WRITE_MORNING_BRIEFS` writes `V_MORNING_BRIEF_JSON` outputs into `MIP.AGENT_OUT.MORNING_BRIEF` via `SP_WRITE_MORNING_BRIEF`.【F:SQL/app/145_sp_run_daily_pipeline.sql†L91-L92】【F:SQL/app/148_sp_pipeline_write_morning_briefs.sql†L1-L86】【F:SQL/app/186_sp_write_morning_brief.sql†L1-L48】

### Simple pseudo-data example
- **Input bar (daily)**: `AAPL`, `TS=2024-06-01`, `CLOSE=190` in `MARKET_BARS`.
- **Return**: if the prior close was 185, then `RETURN_SIMPLE=(190-185)/185` in `MARKET_RETURNS`.
- **Recommendation**: a momentum pattern may insert a recommendation for `AAPL` at `TS=2024-06-01` into `RECOMMENDATION_LOG`.
- **Outcome**: for a 5-bar horizon, the evaluation finds the close 5 bars later and calculates `REALIZED_RETURN` stored in `RECOMMENDATION_OUTCOMES`.

## 2) Recommendation generation workflow
- The momentum generator reads `MIP.MART.MARKET_RETURNS` filtered by market type and interval (including ETF), then inserts new recommendations into `MIP.APP.RECOMMENDATION_LOG`. It also removes recommendations tied to inactive patterns.【F:SQL/app/070_sp_generate_momentum_recs.sql†L1-L235】
- The parameters are driven by pattern definitions (e.g., fast/slow window, lookback days, minimum return, z-score thresholds).【F:SQL/app/070_sp_generate_momentum_recs.sql†L64-L186】

### Simple pseudo-data example
- If a pattern is active for `STOCK` and daily interval, it scans the last `lookback_days` of returns and inserts a recommendation when the pattern criteria are met.

## 3) Evaluation workflow (multi-horizon, bar-based)
- The evaluation procedure defines **bar-based horizons** (1, 3, 5, 10, 20 bars) and uses **strict lookahead rules**: only bars *after* the recommendation timestamp are eligible for the exit price.【F:SQL/app/105_sp_evaluate_recommendations.sql†L33-L79】
- It writes `REALIZED_RETURN`, `HIT_FLAG`, and `EVAL_STATUS` into `MIP.APP.RECOMMENDATION_OUTCOMES` and upserts by `(RECOMMENDATION_ID, HORIZON_BARS)` so re-runs are safe.【F:SQL/app/105_sp_evaluate_recommendations.sql†L33-L154】

### Simple pseudo-data example
- A recommendation at `2024-06-01` with entry close `100` and a 3-bar horizon exits at `2024-06-04` close `103`.
- The realized return is `(103/100)-1 = 0.03` (3%). If the minimum return threshold is `0.0`, then `HIT_FLAG` is true.【F:SQL/app/105_sp_evaluate_recommendations.sql†L92-L124】

## 4) Portfolio simulation workflow
- `SP_RUN_PORTFOLIO_SIMULATION` reads portfolio configuration, constructs eligible signals from `MIP.MART.V_PORTFOLIO_SIGNALS`, and writes positions, trades, and daily equity series into `MIP.APP.PORTFOLIO_POSITIONS`, `MIP.APP.PORTFOLIO_TRADES`, and `MIP.APP.PORTFOLIO_DAILY`.【F:SQL/app/180_sp_run_portfolio_simulation.sql†L1-L180】【F:SQL/app/160_app_portfolio_tables.sql†L71-L170】【F:SQL/mart/030_mart_rec_outcome_views.sql†L111-L129】
- Run-level KPIs and events are calculated in `MIP.MART.V_PORTFOLIO_RUN_KPIS` and `MIP.MART.V_PORTFOLIO_RUN_EVENTS` for downstream reporting and brief composition.【F:SQL/views/mart/v_portfolio_run_kpis.sql†L1-L122】【F:SQL/views/mart/v_portfolio_run_events.sql†L1-L62】

## 5) Morning brief workflow
- `MIP.MART.V_MORNING_BRIEF_JSON` composes trusted signals, risk, attribution, and delta sections from agent input views and the delta view (`V_MORNING_BRIEF_WITH_DELTA`).【F:SQL/views/mart/v_morning_brief_json.sql†L1-L139】【F:SQL/views/mart/v_morning_brief_with_delta.sql†L1-L190】
- `SP_WRITE_MORNING_BRIEF` merges the JSON output into `MIP.AGENT_OUT.MORNING_BRIEF` for persistent agent access; the pipeline wrapper runs this for all active portfolios.【F:SQL/app/186_sp_write_morning_brief.sql†L1-L48】【F:SQL/app/148_sp_pipeline_write_morning_briefs.sql†L1-L86】【F:SQL/app/185_agent_out_morning_brief.sql†L1-L17】

## Known unknowns / TODO
- **Missing from repo:** None identified for the objects explicitly requested in this documentation pack.
