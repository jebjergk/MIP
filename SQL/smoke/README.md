# Daily Pipeline Smoke Checks

`smoke_daily_pipeline.sql` validates that the latest `SP_RUN_DAILY_PIPELINE` run wrote
`PIPELINE_STEP` audit entries for every major step.

If no run exists yet, execute the pipeline first and re-run the smoke query.
