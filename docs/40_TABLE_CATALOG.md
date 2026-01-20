# MIP Table & View Catalog

| Object | Purpose | Grain (one row = …) | Key columns | How populated |
| --- | --- | --- | --- | --- |
| `MIP.MART.MARKET_BARS` | Cleaned base table of market OHLCV bars. | One bar for a symbol/market type/interval/timestamp. | `MARKET_TYPE`, `SYMBOL`, `INTERVAL_MINUTES`, `TS` | Upserted by `SP_INGEST_ALPHAVANTAGE_BARS` (called by daily pipeline).【F:SQL/mart/010_mart_market_bars.sql†L12-L24】【F:SQL/app/030_sp_ingest_alphavantage_bars.sql†L407-L450】【F:SQL/app/145_sp_run_daily_pipeline.sql†L31-L118】 |
| `MIP.MART.MARKET_RETURNS` | Returns per bar (simple and log), derived from `MARKET_BARS`. | One bar with return metrics for each symbol/interval. | `RETURN_SIMPLE`, `RETURN_LOG`, `PREV_CLOSE` | `CREATE OR REPLACE VIEW` in daily pipeline (and also in mart build).【F:SQL/mart/010_mart_market_bars.sql†L48-L107】【F:SQL/app/145_sp_run_daily_pipeline.sql†L119-L218】 |
| `MIP.APP.RECOMMENDATION_LOG` | Log of recommendations emitted by patterns. | One recommendation event. | `RECOMMENDATION_ID`, `PATTERN_ID`, `SYMBOL`, `TS`, `SCORE` | Inserted by `SP_GENERATE_MOMENTUM_RECS` (called in pipeline).【F:SQL/app/050_app_core_tables.sql†L194-L212】【F:SQL/app/070_sp_generate_momentum_recs.sql†L1-L235】【F:SQL/app/145_sp_run_daily_pipeline.sql†L255-L371】 |
| `MIP.APP.RECOMMENDATION_OUTCOMES` | Evaluation results for recommendations across horizons. | One recommendation-horizon result. | `RECOMMENDATION_ID`, `HORIZON_BARS`, `REALIZED_RETURN`, `HIT_FLAG`, `EVAL_STATUS` | Upserted by `SP_EVALUATE_RECOMMENDATIONS` (called in pipeline).【F:SQL/app/050_app_core_tables.sql†L215-L239】【F:SQL/app/105_sp_evaluate_recommendations.sql†L33-L154】【F:SQL/app/145_sp_run_daily_pipeline.sql†L401-L444】 |
| `MIP.MART.REC_OUTCOME_COVERAGE` | Coverage/maturity status of outcomes by pattern and horizon. | One row per pattern/market/interval/horizon. | `N_TOTAL`, `N_SUCCESS`, `COVERAGE_RATE`, `LATEST_MATURED_ENTRY_TS` | View over outcomes + log.【F:SQL/mart/030_mart_rec_outcome_views.sql†L1-L27】 |
| `MIP.MART.REC_OUTCOME_PERF` | Performance stats for matured outcomes. | One row per pattern/market/interval/horizon. | `AVG_RETURN`, `HIT_RATE`, `SCORE_RETURN_CORR` | View over outcomes + log, filtered to `EVAL_STATUS='SUCCESS'`.【F:SQL/mart/030_mart_rec_outcome_views.sql†L29-L55】 |
| `MIP.MART.REC_PATTERN_TRUST_RANKING` | Combined coverage/performance trust score. | One row per pattern/market/interval/horizon. | `TRUST_SCORE` | View derived from coverage + performance views.【F:SQL/mart/030_mart_rec_outcome_views.sql†L57-L82】 |
| `MIP.MART.REC_TRAINING_KPIS` | Training KPI view (hit rate, expectancy, loss streaks, time windows). | One row per pattern/market/interval/horizon. | `HIT_RATE`, `EXPECTANCY`, `MAX_LOSS_STREAK`, 30/90-day metrics | View over outcomes + log.【F:SQL/mart/020_mart_rec_training_kpis.sql†L1-L109】 |
| `MIP.APP.V_PATTERN_KPIS` | Pattern-level KPI aggregation (sample count, hit rate, return stats). | One row per pattern/market/interval/horizon. | `SAMPLE_COUNT`, `HIT_RATE`, `AVG_FORWARD_RETURN` | View over outcomes + log.【F:SQL/app/090_recommendation_outcome_kpis.sql†L1-L22】 |
| `MIP.APP.MIP_AUDIT_LOG` | Append-only audit log for pipeline and procedures. | One event log entry. | `EVENT_TS`, `RUN_ID`, `EVENT_TYPE`, `STATUS`, `DETAILS` | Inserted by `SP_LOG_EVENT` and pipeline steps.【F:SQL/app/055_app_audit_log.sql†L7-L39】 |

## Explicit semantics for key tables
- **`RECOMMENDATION_LOG`**: each row is a recommendation emitted by a pattern at a specific bar timestamp; `SCORE` holds the pattern score/strength used for later KPI correlation.【F:SQL/app/050_app_core_tables.sql†L194-L212】
- **`RECOMMENDATION_OUTCOMES`**: each row is an evaluation result for a recommendation at a given horizon. `REALIZED_RETURN` and `HIT_FLAG` represent actual outcome performance, while `EVAL_STATUS` signals whether data was sufficient to evaluate the horizon.【F:SQL/app/050_app_core_tables.sql†L215-L239】【F:SQL/app/105_sp_evaluate_recommendations.sql†L92-L124】

## Known unknowns / TODO
- **Missing from repo:** None identified for the objects explicitly requested in this documentation pack.
