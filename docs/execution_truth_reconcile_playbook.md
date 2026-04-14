# Execution-truth reconcile playbook (Phase 1)

**Goal:** Broker-real **`BROKER_SNAPSHOTS` `EXECUTION`** rows propagate into **`LIVE_ORDERS`** (FILLED / PARTIAL_FILL + qty/avg), which drives **`LIVE_ACTIONS.EXECUTED`**, then existing hooks write **`TRADE_CLOSEOUT`** → **`MIP.MART.V_TRADE_INTELLIGENCE` (TIR)** → PW fields on TIR when exit-day **`V_PARALLEL_WORLD_DIFF`** rows exist.

**IB reference:** [TWS API / IBKR Campus](https://www.interactivebrokers.com/campus/ibkr-api-page/twsapi-doc/) (general API model; this playbook is **MIP persistence**, not new IB subscriptions).

**Code paths:**

| Step | Location |
|------|----------|
| Match logic | [`broker_execution_reconcile.py`](../apps/mip_ui_api/app/services/broker_execution_reconcile.py) |
| HTTP | [`live.py`](../apps/mip_ui_api/app/routers/live.py) — `POST /live/trades/reconcile-executions/dry-run` and `/apply` |
| Order update + closeout | [`update_live_order_status`](../apps/mip_ui_api/app/routers/live.py) → [`entry_intel_hooks.py`](../apps/mip_ui_api/app/entry_intel_hooks.py) |

**Scope guardrail:** **No strategy, committee, bracket policy, or training changes** — only reconciliation of broker truth into `LIVE_ORDERS` and downstream effects of **`FILLED`**.

---

## 1. Preconditions

1. **`LIVE_PORTFOLIO_CONFIG`** has **`IBKR_ACCOUNT_ID`** for the portfolio under test.
2. **`BROKER_SNAPSHOTS`** contains recent **`SNAPSHOT_TYPE = 'EXECUTION'`** rows for that account (sync/ingest running).
3. **`LIVE_ORDERS`** rows exist with **`BROKER_ORDER_ID`** (or payload keys **`perm_id` / `orderId`**) alignable to executions.

---

## 2. Step A — Dry-run (read-only)

**Endpoint:** `POST /live/trades/reconcile-executions/dry-run`

**Body (JSON):**

```json
{
  "portfolio_id": 1,
  "lookback_days": 14,
  "actor": "reconcile_operator"
}
```

**Interpretation:**

| `counts` bucket | Meaning |
|-----------------|--------|
| `matched` | Safe candidate for apply — each item has `order_id`, `proposed_status`, `proposed_qty_filled`, `proposed_avg_fill_price` |
| `ambiguous` | **Do not auto-apply** — multiple orders or qty conflict |
| `unmatched` | No `LIVE_ORDERS` row for execution keys, or terminal order status |
| `already_synced` | Execution maps to order already **`FILLED`** |

**High-confidence apply set (recommended filter):**

- `category == "matched"` **and**
- `reason_detail` in **`"ok"`**, **`"partial_fill_from_execution"`** (partial only if you intend to apply partial state).

**Exclude:** any row with `reason_detail` implying ambiguity, wrong terminal state, or missing qty.

**Required output to archive:** full JSON response (especially `matched`, `ambiguous`, `unmatched`, `counts`).

---

## 3. Step B — Apply (writes)

**Endpoint:** `POST /live/trades/reconcile-executions/apply`

**Body (JSON):**

```json
{
  "portfolio_id": 1,
  "lookback_days": 14,
  "actor": "your_name",
  "confirm_apply": true,
  "items": [
    { "exec_key": "<from dry-run matched[].exec_key>", "order_id": "<from dry-run matched[].order_id>" }
  ]
}
```

**Behavior:**

1. Re-validates each item against current snapshots + orders (`verify_apply_item`).
2. Inserts **`BROKER_EVENT_LEDGER`** row **`EVENT_TYPE = 'EXECUTION_RECONCILE_APPLY'`** (phase `before_update_live_order_status`).
3. Calls **`update_live_order_status`** → may insert **`ORDER_FILLED`** ledger event, update **`LIVE_ACTIONS`**, and on **`FILLED`** invoke **`maybe_write_trade_closeout_*`**.

**Response:** `reconcile_run_id`, per-item `ok` / errors, and nested `update_live_order_status` when successful.

**Required output to archive:** full JSON response.

---

## 4. Post-apply verification checklist

Run after apply (same **`portfolio_id`**). Replace literals or use [`31_execution_reconcile_post_apply.sql`](../SQL/smoke/31_execution_reconcile_post_apply.sql).

### 4.1 Reconciled apply events

```sql
select
  EVENT_TS,
  EVENT_TYPE,
  ACTION_ID,
  PAYLOAD:reconcile_run_id::string as reconcile_run_id,
  PAYLOAD:exec_key::string as exec_key,
  PAYLOAD:order_id::string as order_id
from MIP.LIVE.BROKER_EVENT_LEDGER
where PORTFOLIO_ID = <PORTFOLIO_ID>
  and EVENT_TYPE = 'EXECUTION_RECONCILE_APPLY'
  and EVENT_TS >= dateadd(hour, -48, current_timestamp())
order by EVENT_TS desc;
```

**Pass:** One row per applied item; `reconcile_run_id` matches apply response.

### 4.2 Orders moved to broker-aligned fill state

```sql
select ORDER_ID, ACTION_ID, STATUS, QTY_FILLED, AVG_FILL_PRICE, FILLED_AT, LAST_UPDATED_AT
from MIP.LIVE.LIVE_ORDERS
where PORTFOLIO_ID = <PORTFOLIO_ID>
  and ORDER_ID in (<ORDER_IDS_FROM_APPLY>)
order by LAST_UPDATED_AT desc;
```

**Pass:** `STATUS` is **`FILLED`** or **`PARTIAL_FILL`** per dry-run proposal; qty/avg consistent with execution payload.

### 4.3 Entry / exit actions executed

```sql
select ACTION_ID, ACTION_INTENT, STATUS, REASON_CODES, UPDATED_AT
from MIP.LIVE.LIVE_ACTIONS
where ACTION_ID in (
  select ACTION_ID from MIP.LIVE.LIVE_ORDERS
  where PORTFOLIO_ID = <PORTFOLIO_ID>
    and ORDER_ID in (<ORDER_IDS_FROM_APPLY>)
);
```

**Pass:** **`FILLED`** orders for entry legs → **`EXECUTED`** where applicable; protective-leg fills trigger closeout path per hooks.

### 4.4 New `TRADE_CLOSEOUT` rows

TIR is a **view** over **`TRADE_CLOSEOUT`** (grain: one row per **`CLOSEOUT_ID`** / **`ENTRY_ACTION_ID`**).

```sql
select CLOSEOUT_ID, ENTRY_ACTION_ID, EXIT_ACTION_ID, SYMBOL, EXIT_TYPE, ENTRY_TS, EXIT_TS, CREATED_TS
from MIP.LIVE.TRADE_CLOSEOUT
where ENTRY_ACTION_ID in (
  select distinct ACTION_ID
  from MIP.LIVE.LIVE_ORDERS
  where PORTFOLIO_ID = <PORTFOLIO_ID>
    and ORDER_ID in (<ORDER_IDS_FROM_APPLY>)
)
   or EXIT_ACTION_ID in (
  select distinct ACTION_ID
  from MIP.LIVE.LIVE_ORDERS
  where PORTFOLIO_ID = <PORTFOLIO_ID>
    and ORDER_ID in (<ORDER_IDS_FROM_APPLY>)
)
order by CREATED_TS desc;
```

**Pass:** New **`CREATED_TS`** after reconcile for closes that were blocked only because orders were not **`FILLED`** in MIP.

**Note:** Reconciling **`ENTRY`** legs to **`FILLED`** updates **`LIVE_ACTIONS`** to **`EXECUTED`** but does **not** insert **`TRADE_CLOSEOUT`** — closeouts are written on **exit** fills or **protective (bracket) leg** fills only. Expect **`0`** new closeouts from a batch that is **all entry** orders.

**Note:** Closeout requires **`FILLED`** (not partial) for the v1 hook path; partial fills may need a later fill or manual follow-up.

### 4.5 New TIR rows (same grain as closeout)

```sql
select CLOSEOUT_ID, ENTRY_ACTION_ID, SYMBOL, ENTRY_TS, EXIT_TS,
       HAS_EIS, REALIZED_RETURN, ALIGNMENT_CLASS, BEST_PW_SCENARIO_NAME
from MIP.MART.V_TRADE_INTELLIGENCE
where PORTFOLIO_ID = <PORTFOLIO_ID>
  and ENTRY_ACTION_ID in (
    select ENTRY_ACTION_ID from MIP.LIVE.TRADE_CLOSEOUT
    where CREATED_TS >= dateadd(hour, -48, current_timestamp())
  )
order by EXIT_TS desc nulls last;
```

**Pass:** Every new **`TRADE_CLOSEOUT`** with valid portfolio join appears here (see view definition — orphan closeouts without portfolio may be excluded).

### 4.6 PW-visible closed trades (regret-capable grain)

PW columns on TIR (**`BEST_PW_*`**, **`PW_REGRET_*`**) join **`V_PARALLEL_WORLD_DIFF`** on **portfolio** and **calendar date** of **`EXIT_TS`** (fallback **`ENTRY_TS`**). If no diff rows exist for that day, PW fields are **NULL** even when TIR exists.

```sql
select
  t.CLOSEOUT_ID,
  t.ENTRY_ACTION_ID,
  t.EXIT_TS::date as pw_join_date,
  t.BEST_PW_SCENARIO_NAME,
  t.PW_REGRET_AMOUNT,
  exists (
    select 1
    from MIP.MART.V_PARALLEL_WORLD_DIFF d
    where d.PORTFOLIO_ID = t.PORTFOLIO_ID
      and d.AS_OF_TS::date = coalesce(t.EXIT_TS::date, t.ENTRY_TS::date)
  ) as pw_diff_exists_exit_or_entry_day
from MIP.MART.V_TRADE_INTELLIGENCE t
where t.PORTFOLIO_ID = <PORTFOLIO_ID>
  and t.CLOSEOUT_ID in (
    select CLOSEOUT_ID from MIP.LIVE.TRADE_CLOSEOUT
    where CREATED_TS >= dateadd(hour, -48, current_timestamp())
  );
```

**Pass:** **`pw_diff_exists_exit_or_entry_day = true`** for trades where you expect regret UI; if false, fix is **PW pipeline / scenario activity for that day**, not reconcile.

### 4.7 API spot-check (optional)

**Endpoint:** `GET /parallel-worlds/trade-intelligence?portfolio_id=<PORTFOLIO_ID>&limit=100`

**Pass:** New closeouts appear in **`trades`** with snake_case keys matching `V_TRADE_INTELLIGENCE` columns (contract `parallel_worlds_trade_intelligence_v1`).

---

## 5. Dry-run / apply result template (paste after runs)

| Field | Value |
|-------|--------|
| Portfolio ID | |
| Dry-run timestamp | |
| `counts` (exec / matched / ambiguous / unmatched / synced) | |
| Applied `reconcile_run_id` | |
| Items applied (exec_key → order_id) | |
| Post-apply: ledger rows | |
| Post-apply: new CLOSEOUT_IDs | |
| Post-apply: TIR rows confirmed | |
| PW diff present for exit days? | |

---

## 6. Related docs

- [post_diagnostic_repair_plan.md](post_diagnostic_repair_plan.md) — Tracks A/B scope
- [live_trade_diagnostic_memo.md](live_trade_diagnostic_memo.md) — cohort SQL / UI lineage
- [live_market_data_path_fix_spec.md](live_market_data_path_fix_spec.md) — §12 delayed-data period; §13 intraday hold
- [first_exit_closeout_tir_pw_verification_plan.md](first_exit_closeout_tir_pw_verification_plan.md) — first **exit / protective** fill → **closeout → TIR → PW** E2E checklist
- [avg_fill_price_repair_plan.md](avg_fill_price_repair_plan.md) — **`AVG_FILL_PRICE`** null after reconcile; execution payload field names
