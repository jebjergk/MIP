# Smoke Tests

All smoke test queries live in this folder. Use them individually or as a suite.

Key entries:
- `smoke_daily_pipeline.sql` validates that the latest `SP_RUN_DAILY_PIPELINE` run
  wrote `PIPELINE_STEP` audit entries for every major step.
- `smoke_tests.sql` runs the core system health checks (tables, views, audits).
- `morning_brief_idempotency_smoke.sql` proves `SP_WRITE_MORNING_BRIEF` is idempotent on
  `(portfolio_id, as_of_ts, run_id, agent_name)`: two identical calls yield exactly one row.
- `transaction_costs_smoke.sql` validates fee/slippage/spread cost modeling on a
  synthetic single-trade run.

If no run exists yet, execute the pipeline first and re-run the pipeline checks.
