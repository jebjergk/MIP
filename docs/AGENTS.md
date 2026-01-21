# MIP Agent Catalog (Read-Only)

This document defines the initial read-only agents available for MIP analysis. These agents **only read** schema-qualified views and **never** modify data, configuration, or execute workflows.

## Performance Analyst Agent

**Purpose (short)**
Analyze run-to-run performance shifts and highlight degraders/improvers.

**Inputs (schema-qualified views)**
- `MIP.MART.V_PORTFOLIO_RUN_KPIS`

**Output fields**
- Portfolio/run identifiers (portfolio_id, run_id)
- Comparison window (from_ts, to_ts)
- Return metrics (total_return, avg_daily_return, daily_volatility)
- Shift deltas (delta_total_return, delta_avg_daily_return)
- Ranking label (degrader/improver)

**Guardrails**
- Read-only: no inserts/updates/deletes.
- No configuration or parameter changes.
- No execution of procedures, tasks, or external calls.

**Example SQL snippets**

Performance shifts (top degraders/improvers):
```sql
with run_returns as (
    select
        portfolio_id,
        run_id,
        to_ts,
        total_return,
        avg_daily_return,
        daily_volatility,
        lag(total_return) over (
            partition by portfolio_id
            order by to_ts
        ) as prior_total_return,
        lag(avg_daily_return) over (
            partition by portfolio_id
            order by to_ts
        ) as prior_avg_daily_return
    from MIP.MART.V_PORTFOLIO_RUN_KPIS
), deltas as (
    select
        portfolio_id,
        run_id,
        to_ts,
        total_return,
        avg_daily_return,
        daily_volatility,
        total_return - prior_total_return as delta_total_return,
        avg_daily_return - prior_avg_daily_return as delta_avg_daily_return
    from run_returns
    where prior_total_return is not null
)
select *
from deltas
order by delta_total_return asc
limit 10;
```

Risk events (stop trigger + high drawdown):
```sql
select
    k.portfolio_id,
    k.run_id,
    k.max_drawdown,
    e.drawdown_stop_ts,
    e.stop_reason
from MIP.MART.V_PORTFOLIO_RUN_KPIS k
join MIP.MART.V_PORTFOLIO_RUN_EVENTS e
  on e.portfolio_id = k.portfolio_id
 and e.run_id = k.run_id
where e.drawdown_stop_ts is not null
  and k.max_drawdown >= 0.20
order by k.max_drawdown desc;
```

Attribution (top contributors/detractors):
```sql
select
    portfolio_id,
    run_id,
    symbol,
    total_realized_pnl,
    contribution_pct
from MIP.MART.V_PORTFOLIO_ATTRIBUTION
where portfolio_id = 1
order by total_realized_pnl desc
limit 10;
```

## Risk Observer Agent

**Purpose (short)**
Monitor stop events and drawdown spikes for risk escalation.

**Inputs (schema-qualified views)**
- `MIP.MART.V_PORTFOLIO_RUN_EVENTS`
- `MIP.MART.V_PORTFOLIO_RUN_KPIS`

**Output fields**
- Portfolio/run identifiers (portfolio_id, run_id)
- Stop event timestamps (drawdown_stop_ts, first_flat_no_positions_ts)
- Risk metrics (max_drawdown, daily_volatility)
- Stop reason label

**Guardrails**
- Read-only: no inserts/updates/deletes.
- No configuration or parameter changes.
- No execution of procedures, tasks, or external calls.

**Example SQL snippets**

Performance shifts (top degraders/improvers):
```sql
with ranked as (
    select
        portfolio_id,
        run_id,
        to_ts,
        total_return,
        lag(total_return) over (
            partition by portfolio_id
            order by to_ts
        ) as prior_total_return
    from MIP.MART.V_PORTFOLIO_RUN_KPIS
)
select
    portfolio_id,
    run_id,
    to_ts,
    total_return,
    total_return - prior_total_return as delta_total_return
from ranked
where prior_total_return is not null
order by delta_total_return asc
limit 10;
```

Risk events (stop trigger + high drawdown):
```sql
select
    e.portfolio_id,
    e.run_id,
    e.drawdown_stop_ts,
    e.first_flat_no_positions_ts,
    e.stop_reason,
    k.max_drawdown
from MIP.MART.V_PORTFOLIO_RUN_EVENTS e
join MIP.MART.V_PORTFOLIO_RUN_KPIS k
  on k.portfolio_id = e.portfolio_id
 and k.run_id = e.run_id
where e.stop_reason = 'DRAWDOWN_STOP'
  and k.max_drawdown >= 0.15
order by k.max_drawdown desc;
```

Attribution (top contributors/detractors):
```sql
select
    portfolio_id,
    run_id,
    symbol,
    total_realized_pnl,
    contribution_pct
from MIP.MART.V_PORTFOLIO_ATTRIBUTION
where total_realized_pnl < 0
order by total_realized_pnl asc
limit 10;
```

## Attribution Narrator Agent

**Purpose (short)**
Explain what drove PnL by symbol and pattern contributions.

**Inputs (schema-qualified views)**
- `MIP.MART.V_PORTFOLIO_ATTRIBUTION`
- `MIP.MART.V_PORTFOLIO_ATTRIBUTION_BY_PATTERN`

**Output fields**
- Portfolio/run identifiers (portfolio_id, run_id)
- Contributor identifiers (symbol, pattern_id, market_type, horizon_bars)
- Contribution metrics (total_realized_pnl, contribution_pct, win_rate)
- Ranking label (top contributor/detractor)

**Guardrails**
- Read-only: no inserts/updates/deletes.
- No configuration or parameter changes.
- No execution of procedures, tasks, or external calls.

**Example SQL snippets**

Performance shifts (top degraders/improvers):
```sql
with run_totals as (
    select
        portfolio_id,
        run_id,
        sum(total_realized_pnl) as run_realized_pnl
    from MIP.MART.V_PORTFOLIO_ATTRIBUTION
    group by portfolio_id, run_id
), ranked as (
    select
        portfolio_id,
        run_id,
        run_realized_pnl,
        lag(run_realized_pnl) over (
            partition by portfolio_id
            order by run_id
        ) as prior_run_realized_pnl
    from run_totals
)
select
    portfolio_id,
    run_id,
    run_realized_pnl,
    run_realized_pnl - prior_run_realized_pnl as delta_run_realized_pnl
from ranked
where prior_run_realized_pnl is not null
order by delta_run_realized_pnl asc
limit 10;
```

Risk events (stop trigger + high drawdown):
```sql
select
    k.portfolio_id,
    k.run_id,
    k.max_drawdown,
    e.drawdown_stop_ts,
    e.stop_reason
from MIP.MART.V_PORTFOLIO_RUN_KPIS k
join MIP.MART.V_PORTFOLIO_RUN_EVENTS e
  on e.portfolio_id = k.portfolio_id
 and e.run_id = k.run_id
where e.drawdown_stop_ts is not null
  and k.max_drawdown >= 0.20
order by k.max_drawdown desc;
```

Attribution (top contributors/detractors):
```sql
select
    portfolio_id,
    run_id,
    pattern_id,
    market_type,
    horizon_bars,
    total_realized_pnl,
    contribution_pct
from MIP.MART.V_PORTFOLIO_ATTRIBUTION_BY_PATTERN
where portfolio_id = 1
order by total_realized_pnl desc
limit 10;
```
