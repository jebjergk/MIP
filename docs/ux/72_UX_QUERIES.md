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

Read-only. Query params (all optional): :market_type, :symbol, :pattern_id.

**Rules:**

- Daily bars only: `rl.INTERVAL_MINUTES = 1440`
- Join: `MIP.APP.RECOMMENDATION_LOG rl` and `MIP.APP.RECOMMENDATION_OUTCOMES ro` on `ro.RECOMMENDATION_ID = rl.RECOMMENDATION_ID`
- Include only completed evaluations: `ro.EVAL_STATUS = 'COMPLETED'`
- Outcome metric: `ro.REALIZED_RETURN`
- Null-safe HIT_FLAG: `coalesce(ro.HIT_FLAG, false)`

**Return shape:** Grouped by item key `(market_type, symbol, pattern_id, interval_minutes)`, then horizons.

- Item fields: `recs_total` = count(distinct rl.RECOMMENDATION_ID), `outcomes_total` = count(*), `horizons_covered` = count(distinct ro.HORIZON_BARS), `last_recommendation_ts` = max(rl.TS)
- `by_horizon[]`: `horizon_bars`, `n`, `mean_realized_return` = avg(ro.REALIZED_RETURN), `pct_positive` = avg(case when ro.REALIZED_RETURN > 0 then 1 else 0 end), `pct_hit` = avg(case when coalesce(ro.HIT_FLAG, false) then 1 else 0 end), `min_realized_return`, `max_realized_return`

**Canonical SQL – triple-level** (recs_total, last_recommendation_ts per item):

```sql
select
  rl.MARKET_TYPE as market_type,
  rl.SYMBOL as symbol,
  rl.PATTERN_ID as pattern_id,
  rl.INTERVAL_MINUTES as interval_minutes,
  count(distinct rl.RECOMMENDATION_ID) as recs_total,
  max(rl.TS) as last_recommendation_ts
from MIP.APP.RECOMMENDATION_LOG rl
where rl.INTERVAL_MINUTES = 1440
  and (:market_type is null or rl.MARKET_TYPE = :market_type)
  and (:symbol is null or rl.SYMBOL = :symbol)
  and (:pattern_id is null or rl.PATTERN_ID = :pattern_id)
group by rl.MARKET_TYPE, rl.SYMBOL, rl.PATTERN_ID, rl.INTERVAL_MINUTES
order by rl.MARKET_TYPE, rl.SYMBOL, rl.PATTERN_ID;
```

**Canonical SQL – by-horizon** (one row per market_type, symbol, pattern_id, horizon_bars; join to OUTCOMES, EVAL_STATUS = 'COMPLETED'):

```sql
select
  rl.MARKET_TYPE as market_type,
  rl.SYMBOL as symbol,
  rl.PATTERN_ID as pattern_id,
  rl.INTERVAL_MINUTES as interval_minutes,
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
group by rl.MARKET_TYPE, rl.SYMBOL, rl.PATTERN_ID, rl.INTERVAL_MINUTES, ro.HORIZON_BARS
order by rl.MARKET_TYPE, rl.SYMBOL, rl.PATTERN_ID, ro.HORIZON_BARS;
```

Backend builds items from the two queries above: triple-level gives recs_total and last_recommendation_ts; by-horizon gives outcomes_total (sum of n), horizons_covered (distinct horizon_bars), and by_horizon[] with horizons 1/3/5/10/20 where present.

## Signals Explorer (GET /signals)

Canonical signal/recommendation rows for drill-down from Morning Brief opportunities. Used by the `/signals` page.

**Route:** `GET /signals`

**Query params** (all optional):
| Param | Type | Description |
|-------|------|-------------|
| `symbol` | string | Filter by symbol (e.g., AAPL) |
| `market_type` | string | Filter by market type (STOCK, FX) |
| `pattern_id` | string | Filter by pattern ID |
| `horizon_bars` | int | Filter by horizon bars |
| `run_id` | string | Filter by pipeline run ID |
| `as_of_ts` | string | Filter by as-of timestamp (ISO format) |
| `trust_label` | string | Filter by trust label (TRUSTED, WATCH, UNTRUSTED) |
| `limit` | int | Max rows to return (default 100, max 500) |
| `include_fallback` | bool | Include fallback results if primary query returns 0 (default true) |

**Response shape:**
```json
{
  "signals": [
    {
      "recommendation_id": "...",
      "run_id": "...",
      "signal_ts": "2026-01-15T16:00:00",
      "symbol": "AAPL",
      "market_type": "STOCK",
      "interval_minutes": 1440,
      "pattern_id": "AAPL_2",
      "score": 0.85,
      "details": {...},
      "trust_label": "TRUSTED",
      "recommended_action": "BUY",
      "is_eligible": true,
      "gating_reason": null
    }
  ],
  "count": 1,
  "query_type": "primary",
  "filters_applied": { "symbol": "AAPL" },
  "fallback_used": false,
  "fallback_reason": null
}
```

**Fallback logic** (when `include_fallback=true` and primary returns 0):
1. Drop `run_id` filter, keep other filters → `query_type: "fallback_no_run_id"`
2. Drop `as_of_ts` filter, use 7-day window → `query_type: "fallback_7day_window"`
3. Keep symbol only, 30-day window → `query_type: "fallback_symbol_only"`
4. If still 0 → `query_type: "no_results"` with helpful message

**Canonical SQL:**
```sql
select
    s.RECOMMENDATION_ID,
    s.RUN_ID,
    s.TS as signal_ts,
    s.SYMBOL,
    s.MARKET_TYPE,
    s.INTERVAL_MINUTES,
    s.PATTERN_ID,
    s.SCORE,
    s.DETAILS,
    s.TRUST_LABEL,
    s.RECOMMENDED_ACTION,
    s.IS_ELIGIBLE,
    s.GATING_REASON
from MIP.APP.V_SIGNALS_ELIGIBLE_TODAY s
where s.INTERVAL_MINUTES = 1440
  and (:symbol is null or s.SYMBOL = :symbol)
  and (:market_type is null or s.MARKET_TYPE = :market_type)
  and (:pattern_id is null or s.PATTERN_ID = :pattern_id)
  and (:run_id is null or s.RUN_ID = :run_id)
  and (:trust_label is null or s.TRUST_LABEL = :trust_label)
order by s.TS desc, s.SCORE desc
limit :limit;
```

**Example queries:**

```bash
# All signals for a symbol
GET /signals?symbol=AAPL

# Filter by Morning Brief context (from opportunity link)
GET /signals?symbol=AAPL&pattern_id=AAPL_2&run_id=abc123&from=brief

# Trusted signals only
GET /signals?trust_label=TRUSTED&limit=50
```

## Latest Pipeline Run (GET /signals/latest-run)

Returns the latest successful pipeline run info, used to determine if a Morning Brief is stale.

**Route:** `GET /signals/latest-run`

**Response shape:**
```json
{
  "found": true,
  "latest_run_id": "abc-123-def",
  "latest_run_ts": "2026-01-15T16:30:00"
}
```

**Canonical SQL:**
```sql
select
    LAST_SIMULATION_RUN_ID as run_id,
    LAST_SIMULATED_AT as run_ts
from MIP.APP.PORTFOLIO
where STATUS = 'ACTIVE'
  and LAST_SIMULATION_RUN_ID is not null
order by LAST_SIMULATED_AT desc
limit 1;
```

## Daily Intelligence Digest (GET /digest/latest, GET /digest)

AI-generated narrative layer synthesising deterministic MIP facts into a daily story. Supports two scopes:
- **PORTFOLIO**: per-portfolio digest grounded in portfolio-specific snapshot.
- **GLOBAL**: system-wide digest aggregating across all active portfolios.

The digest is grounded in a deterministic snapshot (Layer 1) and an AI narrative (Layer 2) produced by Snowflake Cortex.

### GET /digest/latest?portfolio_id=:pid&scope=:scope

Latest digest. Pass `scope=GLOBAL` for system-wide digest (no `portfolio_id` needed). Pass `portfolio_id` for a portfolio-scoped digest.

**Query parameters:**
| Parameter | Type | Description |
|---|---|---|
| `portfolio_id` | int (optional) | Portfolio ID for portfolio-scoped digest |
| `scope` | string (optional) | `PORTFOLIO` or `GLOBAL`. If `scope=GLOBAL`, returns system-wide digest |

**Response shape (portfolio scope):**
```json
{
  "found": true,
  "scope": "PORTFOLIO",
  "portfolio_id": 1,
  "as_of_ts": "2026-02-08T...",
  "run_id": "abc-123-...",
  "snapshot_created_at": "2026-02-08T...",
  "narrative_created_at": "2026-02-08T...",
  "source_facts_hash": "sha256...",
  "narrative": {
    "headline": "Portfolio 1 held steady at $2,012 equity...",
    "what_changed": ["Gate remained SAFE...", "..."],
    "what_matters": ["Capacity at 3/5 slots..."],
    "waiting_for": ["EUR/USD approaching trust threshold..."],
    "where_to_look": [{"label": "Signals Explorer", "route": "/signals"}, ...]
  },
  "narrative_text": "raw text from Cortex",
  "model_info": "mistral-large2",
  "is_ai_narrative": true,
  "snapshot": {
    "gate": {"risk_status": "OK", "entries_blocked": false, ...},
    "capacity": {"max_positions": 5, "open_positions": 3, ...},
    "signals": {"total_signals": 12, "total_eligible": 4, ...},
    "proposals": {"proposed_count": 2, ...},
    "trades": {"trade_count": 1, ...},
    "training": {"trusted_count": 8, ...},
    "kpis": {"total_return": 0.015, ...},
    "exposure": {"total_equity": 2012.50, ...},
    "pipeline": {"latest_run_id": "...", ...},
    "detectors": [{"detector": "CAPACITY_STATE", "fired": true, ...}, ...]
  },
  "links": {
    "signals": "/signals",
    "training": "/training",
    "portfolio": "/portfolios/1",
    "brief": "/brief",
    "market_timeline": "/market-timeline",
    "suggestions": "/suggestions",
    "runs": "/runs"
  }
}
```

**Response shape (global scope):**
```json
{
  "found": true,
  "scope": "GLOBAL",
  "portfolio_id": null,
  "as_of_ts": "2026-02-08T...",
  "run_id": "abc-123-...",
  "narrative": {
    "headline": "MIP system: 2 active portfolios, 15 signals generated today...",
    "what_changed": ["Trust distribution shifted: 8→9 trusted patterns...", "..."],
    "what_matters": ["System at 60% capacity saturation..."],
    "waiting_for": ["3 patterns approaching trust threshold..."],
    "where_to_look": [{"label": "Signals Explorer", "route": "/signals"}, ...]
  },
  "snapshot": {
    "system": {"active_portfolios": 2, "portfolio_summary": [...]},
    "gates": {"ok_count": 2, "warn_count": 0, "blocked_count": 0, ...},
    "capacity": {"total_max_positions": 10, "total_open": 6, "total_remaining": 4, ...},
    "signals": {"total_signals": 15, "total_eligible": 8, ...},
    "proposals": {"total_proposed": 4, "total_executed": 2, ...},
    "trades": {"total_trades": 2, ...},
    "training": {"trusted_count": 9, "watch_count": 5, ...},
    "pipeline": {"latest_run_id": "...", ...},
    "detectors": [{"detector": "SIGNAL_COUNT_CHANGE", "fired": true, ...}, ...]
  },
  "links": {
    "signals": "/signals",
    "training": "/training",
    "digest": "/digest",
    "brief": "/brief",
    "market_timeline": "/market-timeline",
    "suggestions": "/suggestions",
    "runs": "/runs"
  }
}
```

### GET /digest?portfolio_id=:pid&as_of_ts=:ts&scope=:scope

Historical digest lookup with date and scope filter. Same response shape as `/digest/latest`.

### Canonical SQL (latest portfolio digest)

```sql
select
    s.SCOPE,
    s.PORTFOLIO_ID,
    s.AS_OF_TS,
    s.RUN_ID,
    s.SNAPSHOT_JSON,
    s.SOURCE_FACTS_HASH,
    s.CREATED_AT          as SNAPSHOT_CREATED_AT,
    n.NARRATIVE_TEXT,
    n.NARRATIVE_JSON,
    n.MODEL_INFO,
    n.AGENT_NAME,
    n.CREATED_AT          as NARRATIVE_CREATED_AT
from MIP.AGENT_OUT.DAILY_DIGEST_SNAPSHOT s
left join MIP.AGENT_OUT.DAILY_DIGEST_NARRATIVE n
    on  n.SCOPE         = s.SCOPE
    and n.PORTFOLIO_ID  = s.PORTFOLIO_ID
    and n.AS_OF_TS      = s.AS_OF_TS
    and n.RUN_ID        = s.RUN_ID
where s.SCOPE = 'PORTFOLIO'
  and s.PORTFOLIO_ID = :portfolio_id
order by s.CREATED_AT desc
limit 1;
```

### Canonical SQL (latest global digest)

```sql
select
    s.SCOPE,
    s.PORTFOLIO_ID,
    s.AS_OF_TS,
    s.RUN_ID,
    s.SNAPSHOT_JSON,
    s.SOURCE_FACTS_HASH,
    s.CREATED_AT          as SNAPSHOT_CREATED_AT,
    n.NARRATIVE_TEXT,
    n.NARRATIVE_JSON,
    n.MODEL_INFO,
    n.AGENT_NAME,
    n.CREATED_AT          as NARRATIVE_CREATED_AT
from MIP.AGENT_OUT.DAILY_DIGEST_SNAPSHOT s
left join MIP.AGENT_OUT.DAILY_DIGEST_NARRATIVE n
    on  n.SCOPE         = s.SCOPE
    and n.PORTFOLIO_ID is null
    and s.PORTFOLIO_ID is null
    and n.AS_OF_TS      = s.AS_OF_TS
    and n.RUN_ID        = s.RUN_ID
where s.SCOPE = 'GLOBAL'
  and s.PORTFOLIO_ID is null
order by s.CREATED_AT desc
limit 1;
```

### Interest Detectors — Portfolio Scope

The portfolio snapshot includes an array of interest detectors — deterministic checks that identify notable changes. Each has `{detector, fired, severity, detail}`:

| Detector | Severity | Description |
|---|---|---|
| `GATE_CHANGED` | HIGH | Risk gate status changed (OK/WARN) |
| `HEALTH_CHANGED` | HIGH | Entries blocked/unblocked |
| `TRUST_CHANGED` | MEDIUM | Patterns changed trust label (WATCH/TRUSTED) |
| `NEAR_MISS` | MEDIUM | Symbols in WATCH close to eligibility |
| `PROPOSAL_FUNNEL` | LOW | Funnel counts: signals → eligible → proposed → executed |
| `NOTHING_HAPPENED` | LOW | No signals, no proposals, no trades (with reason) |
| `CAPACITY_STATE` | HIGH/MEDIUM | Remaining position slots (HIGH if 0 remaining) |
| `CONFLICT_BLOCKED` | MEDIUM | Strong signals blocked by portfolio rules |
| `KPI_MOVEMENT` | MEDIUM | Significant return or drawdown change (>0.5%) |
| `TRAINING_PROGRESS` | LOW | Trusted pattern count changed vs prior snapshot |

### Interest Detectors — Global Scope

| Detector | Severity | Description |
|---|---|---|
| `GATE_CHANGED_ANY` | HIGH | Any portfolio's gate status changed vs prior global snapshot |
| `TRUST_DELTA` | MEDIUM | System-wide trusted pattern count changed |
| `NO_NEW_BARS` | LOW | No new market bars detected or pipeline ran with skips |
| `PROPOSAL_FUNNEL_GLOBAL` | LOW | Aggregate funnel: signals → eligible → proposed → executed (with biggest dropoff) |
| `NOTHING_HAPPENED` | LOW | No signals, no proposals, no trades system-wide |
| `CAPACITY_GLOBAL` | HIGH/MEDIUM | Total remaining slots across all portfolios (HIGH if 0) |
| `SIGNAL_COUNT_CHANGE` | MEDIUM | Total signal count changed vs prior global snapshot |

The AI narrative prioritises fired detectors by severity when composing bullets.
