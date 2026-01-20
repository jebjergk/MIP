# MIP Workflows

## 1) Daily pipeline run (scheduled)
The daily task `MIP.APP.TASK_RUN_DAILY_PIPELINE` runs at 07:00 Europe/Berlin and calls `MIP.APP.SP_RUN_DAILY_PIPELINE`.【F:SQL/app/150_task_run_daily_training.sql†L1-L14】

**Steps inside the pipeline**:
1. **Ingest bars**: `SP_INGEST_ALPHAVANTAGE_BARS` upserts the latest bars into `MIP.MART.MARKET_BARS`.【F:SQL/app/145_sp_run_daily_pipeline.sql†L31-L118】【F:SQL/app/030_sp_ingest_alphavantage_bars.sql†L407-L450】
2. **Refresh returns**: `MIP.MART.MARKET_RETURNS` is created/replaced from `MARKET_BARS` with simple/log returns per bar.【F:SQL/app/145_sp_run_daily_pipeline.sql†L119-L218】
3. **Generate recommendations**: `SP_GENERATE_MOMENTUM_RECS` inserts rows into `MIP.APP.RECOMMENDATION_LOG` for each active pattern and market type.【F:SQL/app/145_sp_run_daily_pipeline.sql†L255-L371】【F:SQL/app/070_sp_generate_momentum_recs.sql†L1-L235】
4. **Evaluate outcomes**: `SP_EVALUATE_RECOMMENDATIONS` upserts forward returns into `MIP.APP.RECOMMENDATION_OUTCOMES` for multiple horizons (1, 3, 5, 10, 20 bars).【F:SQL/app/145_sp_run_daily_pipeline.sql†L401-L444】【F:SQL/app/105_sp_evaluate_recommendations.sql†L33-L115】

### Simple pseudo-data example
- **Input bar (daily)**: `AAPL`, `TS=2024-06-01`, `CLOSE=190` in `MARKET_BARS`.
- **Return**: if the prior close was 185, then `RETURN_SIMPLE=(190-185)/185` in `MARKET_RETURNS`.
- **Recommendation**: a momentum pattern may insert a recommendation for `AAPL` at `TS=2024-06-01` into `RECOMMENDATION_LOG`.
- **Outcome**: for a 5-bar horizon, the evaluation finds the close 5 bars later and calculates `REALIZED_RETURN` stored in `RECOMMENDATION_OUTCOMES`.

## 2) Recommendation generation workflow
- The momentum generator reads `MIP.MART.MARKET_RETURNS` filtered by market type and interval, then inserts new recommendations into `MIP.APP.RECOMMENDATION_LOG`. It also removes recommendations tied to inactive patterns.【F:SQL/app/070_sp_generate_momentum_recs.sql†L1-L235】
- The parameters are driven by pattern definitions (e.g., fast/slow window, lookback days, minimum return, z-score thresholds).【F:SQL/app/070_sp_generate_momentum_recs.sql†L64-L186】

### Simple pseudo-data example
- If a pattern is active for `STOCK` and daily interval, it scans the last `lookback_days` of returns and inserts a recommendation when the pattern criteria are met.

## 3) Evaluation workflow (multi-horizon, bar-based)
- The evaluation procedure defines **bar-based horizons** (1, 3, 5, 10, 20 bars) and uses **strict lookahead rules**: only bars *after* the recommendation timestamp are eligible for the exit price.【F:SQL/app/105_sp_evaluate_recommendations.sql†L33-L79】
- It writes `REALIZED_RETURN`, `HIT_FLAG`, and `EVAL_STATUS` into `MIP.APP.RECOMMENDATION_OUTCOMES` and upserts by `(RECOMMENDATION_ID, HORIZON_BARS)` so re-runs are safe.【F:SQL/app/105_sp_evaluate_recommendations.sql†L33-L154】

### Simple pseudo-data example
- A recommendation at `2024-06-01` with entry close `100` and a 3-bar horizon exits at `2024-06-04` close `103`.
- The realized return is `(103/100)-1 = 0.03` (3%). If the minimum return threshold is `0.0`, then `HIT_FLAG` is true.【F:SQL/app/105_sp_evaluate_recommendations.sql†L92-L124】

## Known unknowns / TODO
- **Missing from repo:** None identified for the objects explicitly requested in this documentation pack.
