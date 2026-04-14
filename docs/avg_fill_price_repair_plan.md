# AVG_FILL_PRICE repair plan (broker execution → `LIVE_ORDERS`)

## Finding

**Snowflake check (portfolio 1, post–entry reconcile):** all **`FILLED`** entry legs had **`AVG_FILL_PRICE IS NULL`** (22 / 22) while **`BROKER_SNAPSHOTS`** execution payloads contained economics under **`PAYLOAD:avg_price`** and **`PAYLOAD:price`** (not only `avgPrice` camelCase).

**Root cause:** [`_execution_fill_fields`](../apps/mip_ui_api/app/services/broker_execution_reconcile.py) only read **`avgPrice`** / **`price`**, so reconcile **`proposed_avg_fill_price`** stayed null and **`update_live_order_status`** did not set **`AVG_FILL_PRICE`**.

**Impact:** [`entry_intel_hooks`](../apps/mip_ui_api/app/entry_intel_hooks.py) aggregates require **`AVG_FILL_PRICE IS NOT NULL`** on fills — protective / exit **closeout** can fail with **`missing_exit_execution_data`** (or missing entry averages) even when **`STATUS = 'FILLED'`**.

---

## Smallest repair (three layers)

### 1) Code — future reconciles (required)

| File | Change |
|------|--------|
| [`broker_execution_reconcile.py`](../apps/mip_ui_api/app/services/broker_execution_reconcile.py) | Read **`avg_price`**, then **`avgPrice`**, then **`price`** for execution price; keep qty keys as today. |
| [`live.py`](../apps/mip_ui_api/app/routers/live.py) `update_live_order_status` | If already **`FILLED`** and **`avg_fill_price`** is still null in DB but the request supplies **`avg_fill_price`**, **`UPDATE`** only **`AVG_FILL_PRICE`** (no duplicate **`ORDER_FILLED`** / closeout replay). |

Redeploy **`mip_ui_api`** after merge.

### 2) Data — already-reconciled rows (one-shot)

Run preview `SELECT` then **`UPDATE … FROM`** using latest **`EXECUTION`** row per broker key — see [`MIP/SQL/scripts/repair_avg_fill_from_executions.sql`](../SQL/scripts/repair_avg_fill_from_executions.sql).

**Applied in Cursor agent session (2026-04-14):** **22** rows on **`PORTFOLIO_ID = 1`**; post-check **`COUNT(*) WHERE FILLED AND AVG_FILL_PRICE IS NULL` = 0**.

**Other portfolios:** edit **`PORTFOLIO_ID`** / account subquery and lookback in the script.

### 3) Ops — first exit / protective E2E

Before the first closeout proof, re-run the null-avg **`SELECT`** from [first_exit_closeout_tir_pw_verification_plan.md](first_exit_closeout_tir_pw_verification_plan.md) §2.2.

---

## Optional hardening (not required for MVP)

- Extend `_execution_fill_fields` for string numbers only in VARIANT (already handled by **`float()`** when JSON typed).
- If IB sends **`lastFillPrice`** only on some feeds, add that key after **`price`**.

---

## What was not changed

- No change to **`TRADE_CLOSEOUT`** DDL or view definitions.
- No automatic re-run of closeout after SQL backfill (entry fills do not insert closeout). **Protective** fill still triggers closeout when entry legs now have **avg** prices.
