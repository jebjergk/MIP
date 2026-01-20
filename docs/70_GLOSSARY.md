# Glossary (Plain Language)

## Core terms
- **Bar (OHLCV)**: A single time interval with open, high, low, close, and volume. In MIP, bars are stored in `MIP.MART.MARKET_BARS` with fields like `OPEN`, `HIGH`, `LOW`, `CLOSE`, and `VOLUME`.【F:SQL/mart/010_mart_market_bars.sql†L12-L24】
- **Daily bar**: A bar with `INTERVAL_MINUTES=1440` (24 hours). The seed ingestion universe uses this interval for STOCK, ETF, and FX symbols, representing daily data.【F:SQL/app/050_app_core_tables.sql†L10-L83】
- **Recommendation**: A row in `MIP.APP.RECOMMENDATION_LOG` emitted by a pattern at a specific timestamp and symbol. It includes a `SCORE` and optional `DETAILS` for later analysis.【F:SQL/app/050_app_core_tables.sql†L194-L212】
- **Outcome**: The evaluated result of a recommendation at a particular horizon, stored in `MIP.APP.RECOMMENDATION_OUTCOMES` with realized return and status flags.【F:SQL/app/050_app_core_tables.sql†L215-L239】
- **Horizon (bars)**: The number of future bars used to evaluate an outcome (1, 3, 5, 10, 20). These are defined in `SP_EVALUATE_RECOMMENDATIONS`.【F:SQL/app/105_sp_evaluate_recommendations.sql†L33-L63】
- **Realized return**: The percentage change between the entry price and the exit price at the horizon, calculated in `SP_EVALUATE_RECOMMENDATIONS`.【F:SQL/app/105_sp_evaluate_recommendations.sql†L92-L103】
- **Hit flag**: A boolean that indicates whether realized return meets a minimum threshold (`MIN_RETURN_THRESHOLD`) in `SP_EVALUATE_RECOMMENDATIONS`.【F:SQL/app/105_sp_evaluate_recommendations.sql†L104-L115】
- **Maturity / coverage**: Whether outcomes have enough future bars to be evaluated. This is tracked via `EVAL_STATUS` and aggregated into `REC_OUTCOME_COVERAGE` as `COVERAGE_RATE` and counts of successful evaluations.【F:SQL/app/105_sp_evaluate_recommendations.sql†L116-L124】【F:SQL/mart/030_mart_rec_outcome_views.sql†L8-L27】
- **Strict lookahead rule**: Evaluation only uses bars strictly after the recommendation timestamp to avoid peeking into the future; this is enforced in `SP_EVALUATE_RECOMMENDATIONS` with `b.TS > ENTRY_TS`.【F:SQL/app/105_sp_evaluate_recommendations.sql†L60-L79】

## Quality requirements (current safeguards)
- **No duplicate bars**: ingestion runs a duplicate key check on `(MARKET_TYPE, SYMBOL, INTERVAL_MINUTES, TS)` and fails if duplicates are detected, protecting downstream analytics and evaluation accuracy.【F:SQL/app/030_sp_ingest_alphavantage_bars.sql†L420-L484】
- **Auditability**: pipeline steps and procedures log start/success/failure events to `MIP.APP.MIP_AUDIT_LOG` via `SP_LOG_EVENT` for traceability.【F:SQL/app/055_app_audit_log.sql†L7-L54】【F:SQL/app/145_sp_run_daily_pipeline.sql†L31-L477】

## Known unknowns / TODO
- **Missing from repo:** None identified for the objects explicitly requested in this documentation pack.
