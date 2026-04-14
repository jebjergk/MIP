# Live trade diagnostic memo (post-reset: 2026-04-07+)

Read-only investigation pack: [`MIP/SQL/scripts/live_trade_diagnostic/`](../SQL/scripts/live_trade_diagnostic/). Join architecture: [`live_trade_diagnostic_architecture.md`](live_trade_diagnostic_architecture.md).

## How to run

From repo root (agent venv):

```text
cursorfiles\.venv\Scripts\python.exe cursorfiles/query_snowflake.py -f MIP/SQL/scripts/live_trade_diagnostic/00_scope_and_trade_set.sql
```

Use `-s N` to run a single statement from multi-statement files. Reset date and fallback interval are literals in SQL (`2026-04-07`, `60` minutes).

## UI vs diagnostic data source (lineage)

The **Live Portfolio Activity** “Trades” table ([`LivePortfolioActivity.jsx`](../apps/mip_ui_web/src/pages/LivePortfolioActivity.jsx)) loads **`overview.executions`** from **`GET /live/activity/overview`** ([`get_live_activity_overview`](../apps/mip_ui_api/app/routers/live.py)). That list is built **primarily** from **`MIP.LIVE.BROKER_SNAPSHOTS`** rows where **`SNAPSHOT_TYPE = 'EXECUTION'`**, parsing the IB **`PAYLOAD`** variant (plus **`POSITION`** snapshots for open/close context and estimated close P&amp;L). It **merges** in fills from **`LIVE_ORDERS`** only when status/qty indicate a fill; **dedupe prefers IB snapshot rows**.

**Population A** in scripts `00`–`08` instead requires **`LIVE_ORDERS.FILLED_AT`** (entry leg, non-protective) **or** **`TRADE_CLOSEOUT`**. If IB executions are ingested into **`BROKER_SNAPSHOTS`** but **`LIVE_ORDERS`** never transitions to filled timestamps (e.g. stuck `PENDINGSUBMIT`), **Population A is empty** while the UI still shows trades.

**Align with the UI (broker truth for fills):** run [`09_ib_snapshot_executions_post_reset.sql`](../SQL/scripts/live_trade_diagnostic/09_ib_snapshot_executions_post_reset.sql) (**Population A′**) — cohort of **`EXECUTION`** snapshots since reset, with **`LIVE_ORDERS` / `LIVE_ACTIONS`** joined on **`BROKER_ORDER_ID`**. Use A′ when reconciling to what operators see on the MIP screen; keep A for lifecycle analysis when `LIVE_ORDERS` is complete.

## Methodology (evidence vs inference)

| Topic | Evidence (system records) | Inference (interpretation) |
|-------|---------------------------|----------------------------|
| Cohort membership | **A:** `LIVE_ORDERS` entry-leg `FILLED_AT`, `TRADE_CLOSEOUT.ENTRY_TS` fallback, `ENTRY_TS_SOURCE`. **A′ (UI-aligned):** `BROKER_SNAPSHOTS` `EXECUTION` + `SNAPSHOT_TS` / payload time | A′ matches IB-ingested fills when `LIVE_ORDERS` is stale |
| Realized P&amp;L | `TRADE_CLOSEOUT` | None |
| MFE/MAE | `MARKET_BARS` over `[entry, exit]` | Path labels when daily fallback or sparse bars |
| Bracket geometry | `PARAM_SNAPSHOT`, protective `LIVE_ORDERS` | Whether stop was “too tight” causally |
| Committee value-add | `COMMITTEE_VERDICT` vs executed subset | Whether blocked trades would have lost |
| Regime | `V_MARKET_REGIME` on entry date | Whether regime “caused” loss |
| Trust | `V_TRUSTED_SIGNALS_LATEST_TS` join | **Approximate** — view is current, not time-traveled |

## Populations

1. **Executed entries post-reset (A):** live `ADAPTER_MODE`, `ENTRY` intent, first fill timestamp ≥ reset (with documented fallbacks).  
2. **IB execution snapshots post-reset (A′):** same live book; rows from `BROKER_SNAPSHOTS` `EXECUTION` with `SNAPSHOT_TS` ≥ reset (see script `09`). **Grain:** one row per ingested execution snapshot (not deduped across replays — check `exec_id` / keys).  
3. **Committee considered post-reset (B):** `COMMITTEE_RUN.STARTED_AT` ≥ reset, same live filter, `ENTRY` intent. **Not merged** with A or A′ in grain.

## What happened since 2026-04-07 (fill after running SQL)

**Evidence:** Run `00_scope_and_trade_set.sql`, `01_trade_scorecard.sql`, `09_ib_snapshot_executions_post_reset.sql`, `08_chronological_trades.sql`, then paste aggregates here.

- Executed row count (A): _TBD_  
- IB execution snapshot rows (A′): _TBD_  
- Committee runs (B): _TBD_  
- Win/loss / gross P&amp;L / profit factor: _TBD_  
- Breakdowns by symbol / committee / regime / day: _TBD_

## Top failure modes (rank after results)

1. _TBD — label from `07_casefile_trade_detail.sql` `FAILURE_MODE_LABEL` frequency_  
2. _TBD_  
3. _TBD_

**Note:** `FAILURE_MODE_CONFIDENCE` is conservative; `INSUFFICIENT_EVIDENCE` is used when regime vs path signals conflict.

## Committee additivity

**Evidence:** Compare Population A outcomes to Population B verdict distribution (`03_committee_vs_outcome.sql`).  
**Inference:** _TBD after query run._  
**Unknown without assumptions:** P&amp;L of blocked proposals (no entry fill).

## What is still unknown

- True session-bound **NEXT_SESSION** vs calendar proxy.  
- Intraday bar completeness per symbol (IBKR / mart ingestion).  
- Historical trust state at entry (`V_TRUSTED_SIGNALS_LATEST_TS` is point-in-time).  
- Full counterfactual for `BLOCK` decisions.

## Next investigations (no production changes)

- Drill into rows where `ENTRY_TS_SOURCE <> 'ORDER_FILL'`.  
- Compare `PATH_APPROXIMATION_LEVEL = 'DAILY'` share vs intraday.  
- Cross-read `MIP.MART.V_TRADE_INTELLIGENCE` narrative fields for closed trades.  
- Optional: enrich with `COMMITTEE_ROLE_OUTPUT` for role-level stances.

## Guardrails

- No threshold or strategy changes based on this memo alone.  
- Small sample: avoid “strategy is dead” conclusions.  
- Do not treat `INSUFFICIENT_EVIDENCE` as absolution — it flags diagnostic conflict.
