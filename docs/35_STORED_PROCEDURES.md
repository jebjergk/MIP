# MIP Stored Procedure Catalog

## Pipeline & orchestration
| Procedure | Inputs | Returns | Outputs / Side Effects |
| --- | --- | --- | --- |
| `MIP.APP.SP_RUN_DAILY_PIPELINE` | None | `variant` summary | Orchestrates ingest → returns → recs → evaluation → portfolio sims → proposals/validation → morning briefs.【F:SQL/app/145_sp_run_daily_pipeline.sql†L1-L168】 |
| `MIP.APP.SP_PIPELINE_INGEST` | None | `variant` step summary | Wraps `SP_INGEST_ALPHAVANTAGE_BARS`, logs audit rows, updates `MART.MARKET_BARS`.【F:SQL/app/142_sp_pipeline_ingest.sql†L1-L80】 |
| `MIP.APP.SP_PIPELINE_REFRESH_RETURNS` | None | `variant` step summary | Recreates `MART.MARKET_RETURNS` from `MART.MARKET_BARS` and logs audit rows.【F:SQL/app/143_sp_pipeline_refresh_returns.sql†L1-L104】 |
| `MIP.APP.SP_PIPELINE_GENERATE_RECOMMENDATIONS` | `P_MARKET_TYPE`, `P_INTERVAL_MINUTES` | `variant` step summary | Calls `SP_GENERATE_MOMENTUM_RECS` and logs recommendation counts per market type (ETF included).【F:SQL/app/144_sp_pipeline_generate_recommendations.sql†L1-L120】 |
| `MIP.APP.SP_PIPELINE_EVALUATE_RECOMMENDATIONS` | `P_FROM_TS`, `P_TO_TS` | `variant` step summary | Calls `SP_EVALUATE_RECOMMENDATIONS` and logs outcome row counts.【F:SQL/app/146_sp_pipeline_evaluate_recommendations.sql†L1-L74】 |
| `MIP.APP.SP_PIPELINE_RUN_PORTFOLIOS` | `P_FROM_TS`, `P_TO_TS`, `P_RUN_ID` | `variant` step summary | Loops active portfolios and calls `SP_RUN_PORTFOLIO_SIMULATION` to populate portfolio tables and audit rows.【F:SQL/app/147_sp_pipeline_run_portfolios.sql†L1-L120】 |
| `MIP.APP.SP_PIPELINE_WRITE_MORNING_BRIEFS` | `P_RUN_ID`, `P_SIGNAL_RUN_ID` | `variant` step summary | Calls `SP_AGENT_PROPOSE_TRADES`/`SP_VALIDATE_AND_EXECUTE_PROPOSALS` then `SP_WRITE_MORNING_BRIEF` per active portfolio and audits persistence counts.【F:SQL/app/148_sp_pipeline_write_morning_briefs.sql†L1-L101】 |

## Ingestion & recommendation generation
| Procedure | Inputs | Returns | Outputs / Side Effects |
| --- | --- | --- | --- |
| `MIP.APP.SP_INGEST_ALPHAVANTAGE_BARS` | None | `variant` | Ingests AlphaVantage bars into `MART.MARKET_BARS` (MERGE).【F:SQL/app/030_sp_ingest_alphavantage_bars.sql†L1-L220】 |
| `MIP.APP.SP_GENERATE_MOMENTUM_RECS` | `P_MIN_RETURN`, `P_MARKET_TYPE`, `P_INTERVAL_MINUTES`, `P_LOOKBACK_DAYS`, `P_MIN_ZSCORE` | `variant` | Inserts recommendations into `APP.RECOMMENDATION_LOG` based on momentum filters.【F:SQL/app/070_sp_generate_momentum_recs.sql†L1-L85】 |
| `MIP.APP.SP_EVALUATE_RECOMMENDATIONS` | `P_FROM_TS`, `P_TO_TS` | `variant` | Upserts evaluation outcomes into `APP.RECOMMENDATION_OUTCOMES` for bar horizons.【F:SQL/app/105_sp_evaluate_recommendations.sql†L7-L58】 |
| `MIP.APP.SP_EVALUATE_MOMENTUM_OUTCOMES` | `P_HORIZON_MINUTES`, `P_HIT_THRESHOLD`, `P_MISS_THRESHOLD`, `P_MARKET_TYPE`, `P_INTERVAL_MINUTES` | `varchar` | Inserts rows into `APP.OUTCOME_EVALUATION` for the specified horizon minutes.【F:SQL/app/100_sp_evaluate_momentum_outcomes.sql†L7-L33】 |

## Backtesting & training
| Procedure | Inputs | Returns | Outputs / Side Effects |
| --- | --- | --- | --- |
| `MIP.APP.SP_RUN_BACKTEST` | `P_HORIZON_MINUTES`, `P_HIT_THRESHOLD`, `P_MISS_THRESHOLD`, `P_FROM_TS`, `P_TO_TS`, `P_MARKET_TYPE`, `P_INTERVAL_MINUTES` | `variant` | Writes `BACKTEST_RUN` + `BACKTEST_RESULT` and updates `PATTERN_DEFINITION` metrics.【F:SQL/app/110_sp_run_backtest.sql†L7-L40】【F:SQL/app/110_sp_run_backtest.sql†L126-L185】 |
| `MIP.APP.SP_TRAIN_PATTERNS_FROM_BACKTEST` | `P_BACKTEST_RUN_ID`, `P_MARKET_TYPE`, `P_INTERVAL_MINUTES` | `varchar` | Updates pattern activation/metrics from backtest results.【F:SQL/app/120_sp_train_patterns.sql†L7-L33】【F:SQL/app/120_sp_train_patterns.sql†L55-L109】 |
| `MIP.APP.SP_RUN_MIP_LEARNING_CYCLE` | `P_MARKET_TYPE`, `P_INTERVAL_MINUTES`, `P_HORIZON_MINUTES`, `P_MIN_RETURN`, `P_HIT_THRESHOLD`, `P_MISS_THRESHOLD`, `P_FROM_TS`, `P_TO_TS`, `P_DO_INGEST`, `P_DO_SIGNALS`, `P_DO_EVALUATE`, `P_DO_BACKTEST`, `P_DO_TRAIN` | `variant` | Orchestrates ingest/signals/evaluate/backtest/train flow with pattern summaries.【F:SQL/app/130_sp_run_mip_learning_cycle.sql†L7-L37】【F:SQL/app/130_sp_run_mip_learning_cycle.sql†L60-L170】 |
| `MIP.APP.SP_RUN_DAILY_TRAINING` | None | `variant` | Daily training loop that generates signals and evaluates outcomes, reporting KPI row counts.【F:SQL/app/140_sp_run_daily_training.sql†L7-L55】 |

## Portfolio simulation & readiness
| Procedure | Inputs | Returns | Outputs / Side Effects |
| --- | --- | --- | --- |
| `MIP.APP.SP_SIMULATE_PORTFOLIO` | `P_PORTFOLIO_ID`, `P_FROM_DATE`, `P_TO_DATE`, `P_HOLD_DAYS`, `P_MAX_POSITIONS`, `P_MAX_POSITION_PCT`, `P_MIN_ABS_SCORE`, `P_MARKET_TYPE`, `P_LIQUIDATE_ON_BUST`, `P_DRAWDOWN_RECOVERY_PCT` | `variant` | Legacy portfolio simulator writing portfolio daily/trade data and summary metrics.【F:SQL/app/170_sp_simulate_portfolio.sql†L7-L33】 |
| `MIP.APP.SP_VALIDATE_SIM_READINESS` | `P_AS_OF_DATE` | `variant` | Writes `SIM_READINESS_AUDIT` with readiness status and reasons.【F:SQL/app/175_sp_validate_sim_readiness.sql†L7-L33】【F:SQL/app/175_sp_validate_sim_readiness.sql†L43-L146】 |
| `MIP.APP.SP_RUN_PORTFOLIO_SIMULATION` | `P_PORTFOLIO_ID`, `P_FROM_TS`, `P_TO_TS` | `variant` | Deterministic portfolio simulation that populates positions/trades/daily tables.【F:SQL/app/180_sp_run_portfolio_simulation.sql†L7-L33】【F:SQL/app/180_sp_run_portfolio_simulation.sql†L103-L180】 |

## Agent outputs & utilities
| Procedure | Inputs | Returns | Outputs / Side Effects |
| --- | --- | --- | --- |
| `MIP.APP.SP_AGENT_PROPOSE_TRADES` | `P_RUN_ID`, `P_PORTFOLIO_ID` | `variant` | Inserts `PROPOSED` rows into `AGENT_OUT.ORDER_PROPOSALS` with equal-weight targets for eligible signals.【F:SQL/app/188_sp_agent_propose_trades.sql†L1-L94】 |
| `MIP.APP.SP_VALIDATE_AND_EXECUTE_PROPOSALS` | `P_RUN_ID`, `P_PORTFOLIO_ID` | `variant` | Validates proposals against eligibility + portfolio constraints, rejects invalid rows, and executes approved trades into `APP.PORTFOLIO_TRADES`.【F:SQL/app/189_sp_validate_and_execute_proposals.sql†L1-L177】 |
| `MIP.APP.SP_WRITE_MORNING_BRIEF` | `P_PORTFOLIO_ID`, `P_PIPELINE_RUN_ID` | `variant` | Merges `V_MORNING_BRIEF_JSON` into `AGENT_OUT.MORNING_BRIEF`.【F:SQL/app/186_sp_write_morning_brief.sql†L7-L48】 |
| `MIP.APP.SP_SEED_MIP_DEMO` | None | `string` | Seeds demo pattern + market bars for non-destructive demos.【F:SQL/app/060_sp_seed_mip_demo.sql†L4-L72】 |
| `MIP.APP.SP_LOG_EVENT` | `P_EVENT_TYPE`, `P_EVENT_NAME`, `P_STATUS`, `P_ROWS_AFFECTED`, `P_DETAILS`, `P_ERROR_MESSAGE`, `P_RUN_ID`, `P_PARENT_RUN_ID` | `varchar` | Inserts audit rows into `MIP.APP.MIP_AUDIT_LOG`.【F:SQL/app/055_app_audit_log.sql†L19-L54】 |
