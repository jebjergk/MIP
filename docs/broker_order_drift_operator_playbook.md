# Broker order drift — operator playbook

When the broker truth diverges from `MIP.LIVE.LIVE_ORDERS` / `LIVE_ACTIONS` (rejects, after-hours submissions, OCA bracket cancels, or executions keyed under a different perm id than MIP’s child rows), use this sequence so the book and closeout logic stay aligned.

## Principles

1. **Prefer automated reconcile first** — match `BROKER_SNAPSHOTS` execution rows to `LIVE_ORDERS` by broker id keys (`reconcile-executions/dry-run` → `apply`).
2. **If reconcile cannot match** — executions often carry `OPEN_ORDER_ID` / perm ids that do not match the bracket child ids stored on older rows; then apply **verified** manual status updates from TWS / IB statements.
3. **Always record context** — use `notes` on manual updates (for example `EXCHANGE_CLOSED`, `OCA_SIBLING_CANCELED`, `MANUAL_SL_FILL_FROM_TWS`).
4. **Then rebuild action state** — `POST /live/trades/rebuild-state` re-derives `LIVE_ACTIONS` from persisted orders when counts disagree.

---

## A. Sell rejected — exchange closed (example: CAT)

**Symptom:** MIP shows a working exit; broker rejected the order (market closed).

**Recovery:**

1. Identify the `ORDER_ID` for the rejected exit (`GET /live/trades/orders?portfolio_id=…` or Snowflake on `MIP.LIVE.LIVE_ORDERS`).
2. Mark the order terminal at broker truth:

```http
POST /live/trades/orders/{order_id}/status
Content-Type: application/json

{
  "actor": "operator_name",
  "status": "REJECTED",
  "notes": "EXCHANGE_CLOSED: broker rejected after-hours"
}
```

3. If the parent `LIVE_ACTIONS` row still looks wrong (mixed terminal + open orders on the same action), run rebuild:

```http
POST /live/trades/rebuild-state
{ "portfolio_id": 1, "dry_run": true, "actor": "operator_name" }
```

Review `changes`, then:

```http
POST /live/trades/rebuild-state
{ "portfolio_id": 1, "dry_run": false, "actor": "operator_name" }
```

**Effect:** `LIVE_ACTIONS` moves to `EXECUTION_REJECTED` with `ORDER_REJECTED` for that leg; ledger rows capture `notes`.

---

## B. Protective leg filled; TP/SL canceled at broker (example: NUE, SL hit)

**Symptom:** Entry filled; bracket TP/SL still `PENDINGSUBMIT` / `SUBMITTED` in MIP; broker shows flat / SL filled and siblings canceled (OCA).

**Recovery order (important for closeout):**

1. **Refresh broker snapshots** so new `EXECUTION` / `OPEN_ORDER` rows exist in `MIP.LIVE.BROKER_SNAPSHOTS` (your usual IB ingest path).
2. **Dry-run execution reconcile:**

```http
POST /live/trades/reconcile-executions/dry-run
{ "portfolio_id": 1, "lookback_days": 14, "actor": "operator_name" }
```

3. If the SL row appears under **`matched`** with a proposed `FILLED`, use **`reconcile-executions/apply`** with the returned `exec_key` + `order_id` items only.
4. If the SL fill appears only under **`unmatched`** (execution perm id does not match any `LIVE_ORDERS.BROKER_ORDER_ID`), **after confirming fill in TWS:**
   - `POST /live/trades/orders/{sl_order_id}/status` with `"status": "FILLED"`, correct `qty_filled`, `avg_fill_price`, optional `total_commission`, and `notes` explaining the perm mismatch.
   - That triggers protective-leg closeout hooks when the row is classified as a protective leg.
5. **Mark canceled siblings** (TP and any dead bracket leg) with:

```http
POST /live/trades/orders/{tp_or_other_order_id}/status
{
  "actor": "operator_name",
  "status": "CANCELED",
  "notes": "OCA_SIBLING_CANCELED_AFTER_SL_FILL"
}
```

6. **`rebuild-state`** (dry-run → apply) for the portfolio if any `LIVE_ACTIONS` counts still disagree with orders.

---

## C. Position-level drift (flat at IB, open in MIP)

Use lifecycle / intelligence reconciliation outputs (`LIFECYCLE_RECONCILIATION_STATE`, UI `RECON_*` fields) as the **diagnostic** layer. Fixing the underlying **orders** (sections A and B) is what corrects execution state and closeouts; lifecycle refresh runs on your normal intelligence pipeline.

---

## Quick reference — routes

| Step | Method | Path |
|------|--------|------|
| List orders | GET | `/live/trades/orders?portfolio_id=` |
| Manual terminal / fill | POST | `/live/trades/orders/{order_id}/status` |
| Reconcile preview | POST | `/live/trades/reconcile-executions/dry-run` |
| Reconcile apply | POST | `/live/trades/reconcile-executions/apply` |
| Rebuild actions from orders | POST | `/live/trades/rebuild-state` |

`UpdateLiveOrderStatusRequest.status` must be one of: `PARTIAL_FILL`, `FILLED`, `CANCELED`, `REJECTED`.

---

## Follow-up engineering (not required for one-off recovery)

- Optional: classify executions against open **symbol + side + time window** when broker keys diverge (higher risk; needs strict caps).
- Optional: job that marks bracket siblings `CANCELED` when one OCA leg is `FILLED` and broker open-order snapshot no longer lists siblings.

For immediate operations, sections A–B are sufficient.
