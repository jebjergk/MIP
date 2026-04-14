# Verification plan: first broker-real exit / protective fill → closeout → TIR → PW

**Purpose:** Prove **end-to-end propagation** for the **first** closing event after entry reconciliation:

`BROKER_SNAPSHOTS (EXECUTION)` + **`LIVE_ORDERS.FILLED`** → **`update_live_order_status`** → **`TRADE_CLOSEOUT`** → **`MIP.MART.V_TRADE_INTELLIGENCE`** (TIR) → **PW_* fields** (when parallel-world diff exists for the exit calendar day).

**Companion SQL:** [`MIP/SQL/smoke/32_first_exit_closeout_tir_pw_verification.sql`](../SQL/smoke/32_first_exit_closeout_tir_pw_verification.sql) — set literals at top, run statements in order.

**Related:** [execution_truth_reconcile_playbook.md](execution_truth_reconcile_playbook.md) (entry reconcile); [live_trade_diagnostic_memo.md](live_trade_diagnostic_memo.md) (lineage).

---

## 1. Two closing paths (pick one for the “first proof”)

| Path | When it fires | `TRADE_CLOSEOUT.EXIT_ACTION_ID` | Typical `EXIT_TYPE` |
|------|----------------|----------------------------------|---------------------|
| **A — Protective leg** | Bracket **TP** or **SL** child order reaches **`FILLED`** | `NULL` (same `ENTRY_ACTION_ID` on parent row) | `TP` / `SL` from `IDEMPOTENCY_KEY` suffix |
| **B — Explicit exit action** | A **`LIVE_ACTIONS.ACTION_INTENT = 'EXIT'`** order reaches **`FILLED`** | Set to that exit action’s id | From `LIVE_ACTIONS.EXIT_TYPE` / mapping |

**Code:** [`entry_intel_hooks.py`](../apps/mip_ui_api/app/entry_intel_hooks.py) — `maybe_write_trade_closeout_on_protective_leg_filled` (path A), `maybe_write_trade_closeout_on_exit_filled` (path B). **Router:** [`live.py`](../apps/mip_ui_api/app/routers/live.py) `update_live_order_status` calls the appropriate hook when `target_status == 'FILLED'`.

**Protective detection (path A):** `LIVE_ORDERS.IDEMPOTENCY_KEY` must end with **`:TP`** or **`:SL`** (case-insensitive). Same `ACTION_ID` as the **entry** parent.

---

## 2. Preconditions (must pass before you expect a closeout)

### 2.1 Entry book

- **`LIVE_ACTIONS`:** `ACTION_INTENT = 'ENTRY'`, **`STATUS = 'EXECUTED'`** (after entry fill / reconcile).
- **`ENTRY_INTEL_ACTION_LINK`** row for that **`ENTRY_ACTION_ID`** (EIS / proposal link) — needed for rich TIR fields (`HAS_EIS`, expectation columns when snapshot present).

### 2.2 Economics columns (critical)

Closeout insertion uses **volume-weighted** entry/exit averages from orders with **`QTY_FILLED > 0`** and **`AVG_FILL_PRICE IS NOT NULL`**.

- **Path A (protective):** [`_aggregate_entry_side_fills_for_action`](../apps/mip_ui_api/app/entry_intel_hooks.py) **excludes** `:TP`/`:SL` legs but **requires** entry leg(s) with **`AVG_FILL_PRICE`**. Protective leg fill **must** also have **`AVG_FILL_PRICE`** or the hook returns **`missing_exit_execution_data`** and **does not** insert closeout.
- **Path B (exit action):** Entry and exit actions both need usable fill rows with prices for sensible `REALIZED_*`; alignment still runs with missing flags if prices absent.

**If entry reconcile set `FILLED` but left `AVG_FILL_PRICE` null:** run the null-avg **`SELECT`** on `LIVE_ORDERS`, then follow [avg_fill_price_repair_plan.md](avg_fill_price_repair_plan.md) (SQL **`repair_avg_fill_from_executions.sql`** + **`mip_ui_api`** reconcile / status patch fixes) **before** the first exit/protective closeout proof.

### 2.3 Broker + MIP order row for the closing event

- IB **`EXECUTION`** snapshot ingested for the **closing** perm/order (same account as portfolio).
- **`LIVE_ORDERS`** row for that exit/protective order: correct **`BROKER_ORDER_ID`**, not yet **`FILLED`** in MIP if you are about to reconcile/apply.

### 2.4 Parallel Worlds (PW) — optional layer of the proof

TIR **`BEST_PW_*` / `PW_REGRET_*`** are **non-null** only if **`MIP.MART.V_PARALLEL_WORLD_DIFF`** has rows for:

- `PORTFOLIO_ID` = trade’s portfolio, and  
- `AS_OF_TS::date` = **`coalesce(TRADE_CLOSEOUT.EXIT_TS, ENTRY_TS)::date`** (see [`v_trade_intelligence.sql`](../SQL/views/mart/v_trade_intelligence.sql)).

**End-to-end “full stack” proof:** schedule or run PW pipeline so **at least one active scenario** exists for that **calendar day**, or accept **TIR with PW nulls** as partial pass and document **PW diff gap** separately.

---

## 3. Execution sequence (operator)

1. **Record baseline** — run SQL §1 in `32_…sql` for chosen **`ENTRY_ACTION_ID`** / **`PORTFOLIO_ID`**: confirm no pre-existing **`TRADE_CLOSEOUT`** for that entry (or document idempotency if re-run).
2. **Ensure closing order is broker-real** — IB shows fill; **`BROKER_SNAPSHOTS`** has matching **`EXECUTION`** (optional pre-check query §2).
3. **Dry-run reconcile** (if MIP order stuck): `POST /live/trades/reconcile-executions/dry-run` — confirm **`matched`** for the **exit/protective** `ORDER_ID` / `exec_key`.
4. **Apply** only high-confidence items for that order: `POST /live/trades/reconcile-executions/apply` **or** if fills arrive via normal API path, call **`POST /live/trades/orders/{order_id}/status`** with **`FILLED`** and **`avg_fill_price`** / **`qty_filled`** (must hit the same `update_live_order_status` code path).
5. **Immediately** run post-hooks verification (SQL §3–§7) and API spot-check (§4 below).

---

## 4. Exact verification steps (acceptance criteria)

### Step V1 — Order and action state

**Query** (see `32_…sql` §3): closing **`ORDER_ID`** → **`STATUS = 'FILLED'`**, **`FILLED_AT`** set, **`QTY_FILLED`** consistent with IB.

**Pass:** Row updated; **`LIVE_ACTIONS`** for exit path updated if applicable (`EXECUTED` / terminal per intent).

### Step V2 — `BROKER_EVENT_LEDGER`

**Query** §4: recent **`ORDER_FILLED`** (or **`ORDER_PARTIAL_FILL`** if policy allows) for that **`ORDER_ID`** / **`ACTION_ID`**.

**Pass:** Append-only event exists; payload references actor/notes if used.

### Step V3 — `TRADE_CLOSEOUT` (grain: one per `ENTRY_ACTION_ID`)

**Query** §5: **`ENTRY_ACTION_ID`** = parent entry; new **`CLOSEOUT_ID`**, **`CREATED_TS`** after fill; **`EXIT_TYPE`** in (`TP`,`SL`,…); **`EXIT_ACTION_ID`** null for protective path A, non-null for path B.

**Pass:** Exactly **one** new row (or idempotent skip if already present — then document **`duplicate_closeout`** reason from logs).

### Step V4 — TIR (`V_TRADE_INTELLIGENCE`)

**Query** §6: same **`CLOSEOUT_ID`** / **`ENTRY_ACTION_ID`**, **`PORTFOLIO_ID`** not null.

**Pass:** Row visible. **`HAS_EIS`** / **`EXPECTED_RETURN`** optional but should match snapshot presence.

**Fail:** No row → check view filter `coalesce(e.PORTFOLIO_ID, la.PORTFOLIO_ID) is not null` (orphan closeout).

### Step V5 — PW propagation

**Query** §7a: for that trade’s **`EXIT_TS::date`** (or entry date fallback), count **`V_PARALLEL_WORLD_DIFF`** rows for **`PORTFOLIO_ID`**.

**Query** §7b: TIR row — **`BEST_PW_SCENARIO_NAME`** or **`PW_REGRET_AMOUNT`** non-null when §7a > 0.

**Pass (strict):** §7a > 0 **and** at least one PW field populated.  
**Pass (partial):** TIR OK, §7a = 0 — document **PW pipeline / day not built**; rerun PW for that day and re-check §7b.

### Step V6 — API contract (UX / integration)

**Request:** `GET /parallel-worlds/trade-intelligence?portfolio_id=<PORTFOLIO_ID>&limit=200`

**Pass:** Response **`trades`** contains an object with **`closeout_id`** / **`entry_action_id`** matching Step V3–V4; keys are **snake_case** per `parallel_worlds_trade_intelligence_v1`.

---

## 5. Evidence bundle to archive

| Artifact | Content |
|----------|---------|
| `dry_run.json` | Reconcile dry-run for the closing order(s) |
| `apply.json` | Apply response or `update_live_order_status` JSON |
| `V1–V7` query outputs | Paste or CSV from `32_…sql` |
| `tir_api.json` | Snippet of matching trade from `/trade-intelligence` |
| Log lines | `trade_closeout_attempt` from API logs (`outcome=written`) |

---

## 6. Failure decision tree (short)

| Symptom | Likely cause |
|---------|----------------|
| No **`TRADE_CLOSEOUT`** | Order not **`FILLED`**; not detected as protective (`IDEMPOTENCY_KEY`); **`missing_exit_execution_data`** (null **`AVG_FILL_PRICE`**); entry not **`EXECUTED`**; duplicate already exists |
| Closeout exists, no TIR | Portfolio null on EIS + entry action (view exclusion) |
| TIR exists, PW all null | No **`V_PARALLEL_WORLD_DIFF`** for exit calendar day or no **active** scenarios |
| Reconcile apply fails | Same Snowflake **`BROKER_EVENT_LEDGER`** / `parse_json` pattern — ensure **`insert_reconcile_audit`** uses **`INSERT … SELECT`** (fixed in repo) |

---

## 7. Definition of “done” for the first proof

Minimum:

- [ ] One **broker-real** closing fill reflected as **`LIVE_ORDERS.FILLED`** with prices as required by hooks.  
- [ ] **`TRADE_CLOSEOUT`** row created for the correct **`ENTRY_ACTION_ID`**.  
- [ ] **`V_TRADE_INTELLIGENCE`** row exists for that **`CLOSEOUT_ID`**.  
- [ ] **`GET /parallel-worlds/trade-intelligence`** returns that trade.

Full stack (optional but ideal):

- [ ] **`BEST_PW_SCENARIO_NAME`** or **`PW_REGRET_*`** populated after confirming **`V_PARALLEL_WORLD_DIFF`** for the exit day.
