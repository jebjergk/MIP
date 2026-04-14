# Post-diagnostic repair plan (implementation scope: Tracks A + B only)

**Evidence base:** [live_trade_diagnostic_phase4_memo.md](live_trade_diagnostic_phase4_memo.md), [19_closeout_feedback_path_trace.md](19_closeout_feedback_path_trace.md), [17_ib_api_bar_path_and_market_data_mode.md](17_ib_api_bar_path_and_market_data_mode.md), [18_strategy_shape_misalignment.sql](../SQL/scripts/live_trade_diagnostic/18_strategy_shape_misalignment.sql).

This document is the **binding implementation guardrail** for the first repair build. Strategy redesign stays **out of scope** until after A/B ship, diagnostics rerun, and a **formal post-fix strategy review**.

---

## 1. Implementation scope (this build)

| Track | In scope for implementation | Out of scope |
|-------|----------------------------|--------------|
| **A** | Broker execution → **`LIVE_ORDERS` FILLED** (correct qty/price) → existing **`TRADE_CLOSEOUT`** hooks → **TIR / PW** trade grain | Any change to how brackets are *chosen* or training thresholds |
| **B** | Freshness **instrumentation** + explicit **stale / delayed** gating on live decision paths | Changing universe, patterns, or risk *policy* values |

**Track C (strategy-shape / patient-hold / tiering):** **Memo and planning only** in this build. **Do not** implement pattern selection, bracket logic, training gates, expected-return floors, horizon math, or universe tiering as part of A/B.

---

## 2. Explicit non-goals (do not ship in A/B)

- Strategy, signal pattern, or recommendation **redesign**
- Bracket construction **policy** changes (widths, targets, stops) beyond what is required to **reconcile broker truth** (reconciliation may *set* fill fields only, not re-derive brackets)
- Training **thresholds**, `TRAINING_GATE_PARAMS`, or digest **universes**
- Parallel Worlds **logic** changes (downstream should **consume** existing surfaces once `TRADE_CLOSEOUT` exists)
- TWS/Gateway **subscription** or entitlement purchases (instrumentation may *read* mode; do not require new IB products as part of MVP)

---

## 3. Formal post-fix strategy review (gate after A/B)

After **Track A and Track B** are working and **diagnostics are rerun** (same or extended SQL pack), hold a **documented strategy review** (meeting + short memo) to decide whether to open Track C. Review questions **explicitly include**:

1. **Patterns** — Are strategies trained or filtered on the **wrong basis** relative to live path reality?
2. **Expected return / horizon** — Is logic **too small** and **too linear** vs actual path volatility and gaps?
3. **Brackets** — Are stops/targets **misaligned** with realized path volatility (per symbol class)?
4. **Universe** — Should the universe be **tiered** (e.g. large-cap hold tier) or otherwise **redesigned**?

**No commitment** to implement answers in the A/B release; answers feed **Track C** backlog only.

---

## 4. Track A — Execution truth → closeout → TIR / PW

### 4.1 First broken stage (unchanged)

Persist broker-real **fill state** on the correct **`LIVE_ORDERS`** row so `update_live_order_status` can reach **`FILLED`**, which triggers [entry_intel_hooks.py](../apps/mip_ui_api/app/entry_intel_hooks.py) from [live.py](../apps/mip_ui_api/app/routers/live.py) (~11195+).

Reuse **`_recent_unmapped_execution_summary`** match-key thinking; add the **missing** step: **apply** or **propose** reconciliation.

### 4.2 Required: dry-run before write

**Mandatory workflow:**

1. **Dry-run reconcile** (default, or separate endpoint/mode flag):  
   - Return **matched** executions (would update which `ORDER_ID`, with what fill fields).  
   - Return **unmatched** executions (no safe `LIVE_ORDERS` row).  
   - Return **ambiguous** executions (multiple candidates or conflicting qty); **never** auto-apply.

2. **Write / apply mode** (explicit opt-in, e.g. `confirm_token` or `apply=true` after reviewing dry-run):  
   - Only after operator or automated policy has consumed dry-run output.  
   - Each applied row goes through the **same** code path as manual status update (`update_live_order_status` or extracted inner function) so closeout + ledger stay consistent.

### 4.3 Audit trail (every applied fill)

For **every** reconciliation that mutates `LIVE_ORDERS` / triggers closeout:

- Append **`BROKER_EVENT_LEDGER`** (or equivalent append-only table) with: execution key, broker ids, `ORDER_ID`, `ACTION_ID`, actor, dry-run snapshot id or hash, before/after status/qty/price, timestamp, reconcile run id.  
- Log structured line (existing `trade_closeout_attempt` pattern) for observability.

### 4.4 File / function impact (Track A)

| Area | Location |
|------|----------|
| Reconcile dry-run + apply | [live.py](../apps/mip_ui_api/app/routers/live.py) — new route(s) or service; shared matching with `_recent_unmapped_execution_summary` |
| Status + hooks | `update_live_order_status`, `maybe_write_trade_closeout_*` (prefer **no** duplicate closeout insert logic) |
| Audit | `BROKER_EVENT_LEDGER` inserts on apply |
| Tests | Dry-run returns counts; apply → FILLED → closeout idempotency |

---

## 5. Track B — Freshness instrumentation + stale / delayed gating

### 5.1 In scope

- **Instrumentation:** e.g. market data type readback where IB allows, `fetched_at_utc` vs `bar_end_ts`, persist or return in API/reason_codes for committee and revalidation paths ([fetch_ibkr_live_bars.py](../../cursorfiles/fetch_ibkr_live_bars.py), [live.py](../apps/mip_ui_api/app/routers/live.py) `revalidate_live_action`).
- **Explicit gating:** extend beyond single `now - bar_ts` check where diagnostics showed **bar-end vs decision** lag; add configurable **max bar-end lag** (or equivalent) and **delayed-data** flags — **block or degrade** per policy (entries stricter than exits; align with existing exit bypass behavior but **visible** in reason_codes).

### 5.2 Out of scope in Track B

- Changing **numeric** risk thresholds for trading (e.g. default stop %) — **unless** purely renaming/config wiring for **freshness** keys only (new columns like `MAX_BAR_END_LAG_SEC` are OK; changing `MIN_AVG_RETURN` is **not**).

---

## 6. Phased rollout (this build only)

| Phase | Deliverable |
|-------|-------------|
| **A0** | API contract: dry-run response schema (matched / unmatched / ambiguous), apply contract, audit event shape |
| **A1** | Dry-run reconcile endpoint + tests (no writes) |
| **A2** | Apply reconcile + ledger audit + integration test → closeout written |
| **B1** | Instrumentation on IB bar fetch + revalidation payload / reason_codes |
| **B2** | Stale + delayed-aware gates + config keys (freshness-only) |
| **Post** | Rerun diagnostic SQL; schedule **§3 strategy review**; Track C stays memo-only until then |

---

## 7. Recommendation order (unchanged)

1. **Track A** first — unblocks **TRADE_CLOSEOUT** → TIR/PW trade grain.  
2. **Track B** in parallel or immediately after A MVP — does not fix missing closeouts.  
3. **Track C** — only after **§3** review.

---

## 8. Validation checklist

- [x] Dry-run: `POST /live/trades/reconcile-executions/dry-run` (read-only)  
- [x] Apply: `POST /live/trades/reconcile-executions/apply` with `confirm_apply: true` + `BROKER_EVENT_LEDGER` `EXECUTION_RECONCILE_APPLY` per item  
- [ ] Re-run [14_closeout_propagation_pw_gap.sql](../SQL/scripts/live_trade_diagnostic/14_closeout_propagation_pw_gap.sql) after real applies  
- [ ] Re-run [18_strategy_shape_misalignment.sql](../SQL/scripts/live_trade_diagnostic/18_strategy_shape_misalignment.sql) stmt 3 as needed  
- [ ] **§3 strategy review** after green diagnostics  

**Implemented (Tracks A + B, no strategy redesign):** `broker_execution_reconcile.py`, routes on `live.py`, migration `20260413_live_max_bar_end_lag_sec.sql` (deployed), `fetch_ibkr_live_bars.py` instrumentation fields, unit tests `test_broker_execution_reconcile.py`.
