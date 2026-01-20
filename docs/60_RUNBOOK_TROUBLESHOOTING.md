# Runbook & Troubleshooting

## Common failure modes & checks

### 1) Missing bars → insufficient outcomes
**Symptom**: `RECOMMENDATION_OUTCOMES` has `EVAL_STATUS='INSUFFICIENT_FUTURE_DATA'`.
**Why it happens**: The evaluation step looks for a bar strictly after the recommendation timestamp and for each horizon. If future bars do not exist yet, the status is marked as insufficient future data.【F:SQL/app/105_sp_evaluate_recommendations.sql†L56-L115】
**Check**:
```sql
-- Find recommendations with insufficient future data
select eval_status, count(*)
from MIP.APP.RECOMMENDATION_OUTCOMES
group by eval_status;
```

### 2) Missing entry bars → outcomes cannot be evaluated
**Symptom**: `EVAL_STATUS='FAILED_NO_ENTRY_BAR'`.
**Why it happens**: The evaluation procedure requires an exact match to the entry bar (same `SYMBOL`, `MARKET_TYPE`, `INTERVAL_MINUTES`, and `TS`). If the bar is missing, evaluation fails for that recommendation.【F:SQL/app/105_sp_evaluate_recommendations.sql†L44-L115】
**Check**:
```sql
-- Recommendations missing their entry bar
select r.recommendation_id, r.symbol, r.market_type, r.interval_minutes, r.ts
from MIP.APP.RECOMMENDATION_LOG r
left join MIP.MART.MARKET_BARS b
  on b.symbol = r.symbol
 and b.market_type = r.market_type
 and b.interval_minutes = r.interval_minutes
 and b.ts = r.ts
where b.ts is null;
```

### 3) No recommendations generated
**Symptom**: `RECOMMENDATION_LOG` count is flat after a pipeline run.
**Why it happens**: If patterns are inactive or there are no returns for the specified market/interval, the generator skips inserts and logs a status message.【F:SQL/app/070_sp_generate_momentum_recs.sql†L49-L134】
**Check**:
```sql
select count(*) as recs
from MIP.APP.RECOMMENDATION_LOG;
```

### 4) Audit log shows failures in pipeline steps
**Symptom**: `MIP_AUDIT_LOG` has `STATUS='FAIL'` for a pipeline step.
**Why it happens**: Each step logs to the audit table with success/fail status, including error messages when failures occur.【F:SQL/app/145_sp_run_daily_pipeline.sql†L31-L477】【F:SQL/app/055_app_audit_log.sql†L7-L39】
**Check**:
```sql
select event_ts, event_name, status, error_message
from MIP.APP.MIP_AUDIT_LOG
where event_type = 'PIPELINE_STEP'
order by event_ts desc
limit 50;
```

## Smoke test queries
```sql
-- 1) Latest ingestion timestamp
select max(ingested_at) as latest_ingest
from MIP.MART.MARKET_BARS;

-- 2) Returns view populated
select count(*) as return_rows
from MIP.MART.MARKET_RETURNS;

-- 3) Recommendations present
select count(*) as rec_rows
from MIP.APP.RECOMMENDATION_LOG;

-- 4) Outcomes present
select count(*) as outcome_rows
from MIP.APP.RECOMMENDATION_OUTCOMES;

-- 5) Trusted signal buckets
select count(*) as trusted_buckets
from MIP.MART.V_TRUSTED_SIGNALS
where is_trusted;

-- 6) Portfolio signals populated
select count(*) as portfolio_rows
from MIP.MART.V_PORTFOLIO_SIGNALS;

-- 7) Score calibration buckets present
select count(*) as calibration_rows
from MIP.MART.SCORE_CALIBRATION;

-- 8) Signals mapped to expected returns
select count(*) as expected_return_rows
from MIP.MART.V_SIGNALS_WITH_EXPECTED_RETURN;
```

## Backfill procedure (safe re-run)
Because outcomes are **upserted** with a `MERGE` keyed on `(RECOMMENDATION_ID, HORIZON_BARS)`, it is safe to re-run evaluation for a time window without creating duplicates. Use a bounded window to limit work.【F:SQL/app/105_sp_evaluate_recommendations.sql†L33-L154】

```sql
-- Backfill a specific window (example: last 30 days)
call MIP.APP.SP_EVALUATE_RECOMMENDATIONS(
    dateadd(day, -30, current_timestamp()),
    current_timestamp(),
    0.0
);
```

## Known unknowns / TODO
- **Missing from repo:** None identified for the objects explicitly requested in this documentation pack.
