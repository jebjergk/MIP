# UX canonical read-only queries

Canonical read-only queries for the UX. Queries only; no app code. Replace `:run_id`, `:portfolio_id`, `:limit` with actual values or bind parameters.

## Recent pipeline runs

From `MIP_AUDIT_LOG`; pipeline root events only.

```sql
select
    EVENT_TS,
    RUN_ID,
    STATUS,
    ROWS_AFFECTED,
    DETAILS
from MIP.APP.MIP_AUDIT_LOG
where EVENT_TYPE = 'PIPELINE'
  and EVENT_NAME = 'SP_RUN_DAILY_PIPELINE'
order by EVENT_TS desc
limit :limit;
```

## Run timeline by RUN_ID

Audit rows for a given run (root + steps).

```sql
select
    EVENT_TS,
    EVENT_TYPE,
    EVENT_NAME,
    STATUS,
    ROWS_AFFECTED,
    ERROR_MESSAGE,
    DETAILS
from MIP.APP.MIP_AUDIT_LOG
where RUN_ID = :run_id
   or PARENT_RUN_ID = :run_id
order by EVENT_TS;
```

## Portfolio list

```sql
select
    PORTFOLIO_ID,
    NAME,
    STATUS,
    LAST_SIMULATED_AT,
    PROFILE_ID,
    STARTING_CASH,
    FINAL_EQUITY,
    TOTAL_RETURN
from MIP.APP.PORTFOLIO
order by PORTFOLIO_ID;
```

## Portfolio header (single portfolio)

All columns from `MIP.APP.PORTFOLIO` for one portfolio.

```sql
select
    PORTFOLIO_ID,
    PROFILE_ID,
    NAME,
    BASE_CURRENCY,
    STARTING_CASH,
    LAST_SIMULATION_RUN_ID,
    LAST_SIMULATED_AT,
    FINAL_EQUITY,
    TOTAL_RETURN,
    MAX_DRAWDOWN,
    WIN_DAYS,
    LOSS_DAYS,
    STATUS,
    BUST_AT,
    NOTES,
    CREATED_AT,
    UPDATED_AT
from MIP.APP.PORTFOLIO
where PORTFOLIO_ID = :portfolio_id;
```

## Portfolio snapshot – positions

Optional filter by `run_id` for a specific run.

```sql
select *
from MIP.APP.PORTFOLIO_POSITIONS
where PORTFOLIO_ID = :portfolio_id
  and (:run_id is null or RUN_ID = :run_id)
order by ENTRY_TS desc;
```

## Portfolio snapshot – trades

```sql
select *
from MIP.APP.PORTFOLIO_TRADES
where PORTFOLIO_ID = :portfolio_id
  and (:run_id is null or RUN_ID = :run_id)
order by TRADE_TS desc;
```

## Portfolio snapshot – daily

```sql
select *
from MIP.APP.PORTFOLIO_DAILY
where PORTFOLIO_ID = :portfolio_id
  and (:run_id is null or RUN_ID = :run_id)
order by TS desc;
```

## Portfolio snapshot – run KPIs

```sql
select *
from MIP.MART.V_PORTFOLIO_RUN_KPIS
where PORTFOLIO_ID = :portfolio_id
  and (:run_id is null or RUN_ID = :run_id)
order by TO_TS desc;
```

## Portfolio snapshot – risk (gate)

```sql
select *
from MIP.MART.V_PORTFOLIO_RISK_GATE
where PORTFOLIO_ID = :portfolio_id;
```

## Portfolio snapshot – risk (state)

```sql
select *
from MIP.MART.V_PORTFOLIO_RISK_STATE
where PORTFOLIO_ID = :portfolio_id;
```

## Latest morning brief by portfolio_id

```sql
select *
from MIP.AGENT_OUT.MORNING_BRIEF
where PORTFOLIO_ID = :portfolio_id
  and coalesce(AGENT_NAME, '') = 'MORNING_BRIEF'
order by AS_OF_TS desc
limit 1;
```

## Training Status v1 (daily only, INTERVAL_MINUTES = 1440)

Per (market_type, symbol, pattern_id, interval_minutes) from MIP.APP.RECOMMENDATION_LOG and MIP.APP.RECOMMENDATION_OUTCOMES only. Optional join to MIP.APP.PATTERN_DEFINITION (labels) and MIP.APP.TRAINING_GATE_PARAMS (MIN_SIGNALS threshold). No placeholders required for base query; use :market_type only if filtering by market.

```sql
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

Optional filter by market (use placeholder only when needed):

```sql
-- Add to the recs CTE and outcomes_agg CTE: and r.MARKET_TYPE = :market_type
```

Backend adds maturity_score (0–100), maturity_stage (INSUFFICIENT / WARMING_UP / LEARNING / CONFIDENT), and reasons[] from deterministic scoring (sample 0–30, coverage 0–40, horizons 0–30; stage thresholds &lt;25, 25–49, 50–74, ≥75).

## Performance summary (GET /performance/summary)

Query params (all optional): :market_type, :symbol, :pattern_id. Filter strictly to daily bars: rl.INTERVAL_MINUTES = 1440. Join MIP.APP.RECOMMENDATION_LOG rl and MIP.APP.RECOMMENDATION_OUTCOMES ro on ro.RECOMMENDATION_ID = rl.RECOMMENDATION_ID. Only include evaluated rows: ro.EVAL_STATUS = 'COMPLETED' (non-COMPLETED excluded). Use REALIZED_RETURN as the numeric outcome. Null-safe HIT_FLAG: coalesce(ro.HIT_FLAG, false).

Canonical SQL – by-horizon (one row per market_type, symbol, pattern_id, interval_minutes, horizon_bars):

```sql
select
  rl.MARKET_TYPE as market_type,
  rl.SYMBOL as symbol,
  rl.PATTERN_ID as pattern_id,
  rl.INTERVAL_MINUTES as interval_minutes,
  count(distinct rl.RECOMMENDATION_ID) as recs_total,
  count(*) as outcomes_total,
  count(distinct ro.HORIZON_BARS) as horizons_covered,
  max(rl.TS) as last_recommendation_ts,
  ro.HORIZON_BARS as horizon_bars,
  count(*) as n,
  avg(ro.REALIZED_RETURN) as mean_realized_return,
  avg(case when ro.REALIZED_RETURN > 0 then 1 else 0 end) as pct_positive,
  avg(case when coalesce(ro.HIT_FLAG, false) then 1 else 0 end) as pct_hit,
  min(ro.REALIZED_RETURN) as min_realized_return,
  max(ro.REALIZED_RETURN) as max_realized_return
from MIP.APP.RECOMMENDATION_LOG rl
join MIP.APP.RECOMMENDATION_OUTCOMES ro
  on ro.RECOMMENDATION_ID = rl.RECOMMENDATION_ID
where rl.INTERVAL_MINUTES = 1440
  and (:market_type is null or rl.MARKET_TYPE = :market_type)
  and (:symbol is null or rl.SYMBOL = :symbol)
  and (:pattern_id is null or rl.PATTERN_ID = :pattern_id)
  and ro.EVAL_STATUS = 'COMPLETED'
group by
  rl.MARKET_TYPE, rl.SYMBOL, rl.PATTERN_ID, rl.INTERVAL_MINUTES,
  ro.HORIZON_BARS
order by
  rl.MARKET_TYPE, rl.SYMBOL, rl.PATTERN_ID, ro.HORIZON_BARS;
```

Triple-level (recs_total, last_recommendation_ts per triple) – for aggregation when building items:

```sql
select
  rl.MARKET_TYPE as market_type,
  rl.SYMBOL as symbol,
  rl.PATTERN_ID as pattern_id,
  rl.INTERVAL_MINUTES as interval_minutes,
  count(*) as recs_total,
  max(rl.TS) as last_recommendation_ts
from MIP.APP.RECOMMENDATION_LOG rl
where rl.INTERVAL_MINUTES = 1440
  and (:market_type is null or rl.MARKET_TYPE = :market_type)
  and (:symbol is null or rl.SYMBOL = :symbol)
  and (:pattern_id is null or rl.PATTERN_ID = :pattern_id)
group by rl.MARKET_TYPE, rl.SYMBOL, rl.PATTERN_ID, rl.INTERVAL_MINUTES
order by rl.MARKET_TYPE, rl.SYMBOL, rl.PATTERN_ID;
```
