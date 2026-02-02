# Training Status validation (SQL truth vs API truth)

This doc describes how to verify that Training Status maturity bars are correct and stable by comparing **Snowflake SQL output** (source of truth) to **API debug output** (GET /training/status/debug).

## Snowflake constraints

- **Filter strictly to INTERVAL_MINUTES = 1440** (daily bars only).
- **Canonical objects only:**
  - `MIP.APP.RECOMMENDATION_LOG`
  - `MIP.APP.RECOMMENDATION_OUTCOMES`
  - Optional: `MIP.APP.PATTERN_DEFINITION` (labels)
  - Optional: `MIP.APP.TRAINING_GATE_PARAMS` (MIN_SIGNALS threshold)

## Canonical SQL query (reproduces aggregation in Snowflake)

Run this query directly in Snowflake to get the same aggregation the API uses. No placeholders; filter by symbol in the `recs` and `outcomes_agg` CTEs if you want to compare 2–3 symbols.

```sql
-- Training Status v1: INTERVAL_MINUTES = 1440 only.
-- Canonical: MIP.APP.RECOMMENDATION_LOG, MIP.APP.RECOMMENDATION_OUTCOMES.
with recs as (
  select
    r.MARKET_TYPE,
    r.SYMBOL,
    r.PATTERN_ID,
    r.INTERVAL_MINUTES,
    count(*) as recs_total,
    max(r.TS) as as_of_ts
  from MIP.APP.RECOMMENDATION_LOG r
  where r.INTERVAL_MINUTES = 1440
  group by r.MARKET_TYPE, r.SYMBOL, r.PATTERN_ID, r.INTERVAL_MINUTES
),
outcomes_agg as (
  select
    r.MARKET_TYPE,
    r.SYMBOL,
    r.PATTERN_ID,
    r.INTERVAL_MINUTES,
    count(*) as outcomes_total,
    count(distinct o.HORIZON_BARS) as horizons_covered,
    avg(case when o.HORIZON_BARS = 1 and o.EVAL_STATUS = 'SUCCESS' then o.REALIZED_RETURN end) as avg_outcome_h1,
    avg(case when o.HORIZON_BARS = 3 and o.EVAL_STATUS = 'SUCCESS' then o.REALIZED_RETURN end) as avg_outcome_h3,
    avg(case when o.HORIZON_BARS = 5 and o.EVAL_STATUS = 'SUCCESS' then o.REALIZED_RETURN end) as avg_outcome_h5,
    avg(case when o.HORIZON_BARS = 10 and o.EVAL_STATUS = 'SUCCESS' then o.REALIZED_RETURN end) as avg_outcome_h10,
    avg(case when o.HORIZON_BARS = 20 and o.EVAL_STATUS = 'SUCCESS' then o.REALIZED_RETURN end) as avg_outcome_h20
  from MIP.APP.RECOMMENDATION_LOG r
  join MIP.APP.RECOMMENDATION_OUTCOMES o on o.RECOMMENDATION_ID = r.RECOMMENDATION_ID
  where r.INTERVAL_MINUTES = 1440
  group by r.MARKET_TYPE, r.SYMBOL, r.PATTERN_ID, r.INTERVAL_MINUTES
)
select
  recs.MARKET_TYPE as market_type,
  recs.SYMBOL as symbol,
  recs.PATTERN_ID as pattern_id,
  recs.INTERVAL_MINUTES as interval_minutes,
  recs.as_of_ts as as_of_ts,
  recs.recs_total as recs_total,
  coalesce(o.outcomes_total, 0) as outcomes_total,
  coalesce(o.horizons_covered, 0) as horizons_covered,
  case when recs.recs_total > 0 and (recs.recs_total * 5) > 0
    then least(1.0, coalesce(o.outcomes_total, 0)::float / (recs.recs_total * 5))
    else 0.0 end as coverage_ratio,
  o.avg_outcome_h1 as avg_outcome_h1,
  o.avg_outcome_h3 as avg_outcome_h3,
  o.avg_outcome_h5 as avg_outcome_h5,
  o.avg_outcome_h10 as avg_outcome_h10,
  o.avg_outcome_h20 as avg_outcome_h20
from recs
left join outcomes_agg o
  on o.MARKET_TYPE = recs.MARKET_TYPE and o.SYMBOL = recs.SYMBOL
  and o.PATTERN_ID = recs.PATTERN_ID and o.INTERVAL_MINUTES = recs.INTERVAL_MINUTES
order by recs.MARKET_TYPE, recs.SYMBOL, recs.PATTERN_ID;
```

To limit to 2–3 symbols (e.g. for a quick check), add a filter in both CTEs:

```sql
-- In recs: add   and r.SYMBOL in ('AAPL', 'SPY', 'QQQ')
-- In outcomes_agg: add   and r.SYMBOL in ('AAPL', 'SPY', 'QQQ')
```

## Compare SQL output to API debug output

1. **Enable the debug endpoint** (dev-only): set `ENABLE_TRAINING_DEBUG=1` in your environment and restart the API.
2. **Run the canonical SQL** in Snowflake (optionally filtered to 2–3 symbols). Export or note: `market_type`, `symbol`, `pattern_id`, `interval_minutes`, `as_of_ts`, `recs_total`, `outcomes_total`, `horizons_covered`, `coverage_ratio`, `avg_outcome_h1` … `avg_outcome_h20`.
3. **Call the API debug endpoint**:  
   `GET /api/training/status/debug`  
   (or `GET http://localhost:8000/training/status/debug` if running locally.)
4. **Compare** for each row (keyed by market_type, symbol, pattern_id, interval_minutes):
   - **API `raw`** should match the SQL row: same `recs_total`, `outcomes_total`, `horizons_covered`, `coverage_ratio`, and avg_outcome_h* values (within type/rounding).
   - **API `scoring`** shows the inputs used for scoring (`recs_total`, `outcomes_total`, `horizons_covered`, `min_signals`, `coverage_ratio`) and the computed `maturity_score`, `maturity_stage`, `reasons`, and component scores (`score_sample`, `score_coverage`, `score_horizons`).  
   Note: The API recomputes `coverage_ratio` as `outcomes_total / (recs_total * 5)` capped at 1.0; it should match the SQL `coverage_ratio` (which uses the same formula).

## Checklist for 2–3 symbols

- [ ] Run the canonical SQL in Snowflake filtered to 2–3 symbols (e.g. AAPL, SPY, QQQ).
- [ ] Call `GET /training/status/debug` and find the same (market_type, symbol, pattern_id) rows.
- [ ] For each: `raw.recs_total`, `raw.outcomes_total`, `raw.horizons_covered`, `raw.coverage_ratio` match the SQL row.
- [ ] For each: `scoring.scoring_inputs.coverage_ratio` is ≤ 1.0 and matches the logic `min(1.0, outcomes_total / (recs_total * 5))` when recs_total > 0.
- [ ] `scoring.maturity_score` is 0–100; `scoring.maturity_stage` is one of INSUFFICIENT, WARMING_UP, LEARNING, CONFIDENT.

## Related

- **72_UX_QUERIES.md** — same Training Status query and optional market filter.
- **apps/mip_ui_api/app/training_status.py** — deterministic scoring (sample 0–30, coverage 0–40, horizons 0–30; stage thresholds &lt;25, 25–49, 50–74, ≥75).
- **apps/mip_ui_api/tests/test_training_status_scoring.py** — unit tests for recs_total=0, partial horizons, coverage edge cases.
