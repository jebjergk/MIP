"""
Broker EXECUTION snapshot → LIVE_ORDERS reconciliation (dry-run + apply).

Matching is conservative: normalized BROKER_ORDER_ID from execution payload ↔ LIVE_ORDERS.BROKER_ORDER_ID.
"""

from __future__ import annotations

import json
import uuid
from typing import Any

from app.db import fetch_all


def _norm_id(v: Any) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    if s.endswith(".0") and s[:-2].isdigit():
        s = s[:-2]
    return s


def _coerce_payload_dict(payload: Any) -> dict[str, Any]:
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, str):
        try:
            parsed = json.loads(payload)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _broker_keys_from_execution_row(row: dict[str, Any]) -> list[str]:
    payload = _coerce_payload_dict(row.get("PAYLOAD"))
    keys: list[str] = []
    for k in (
        row.get("OPEN_ORDER_ID"),
        payload.get("perm_id"),
        payload.get("orderId"),
        payload.get("order_id"),
    ):
        nk = _norm_id(k)
        if nk and nk not in keys:
            keys.append(nk)
    extra: list[str] = []
    for k in keys:
        try:
            iv = str(int(float(k)))
            if iv not in keys and iv not in extra:
                extra.append(iv)
        except Exception:
            pass
    return keys + [x for x in extra if x not in keys]


def _preferred_broker_order_id(row: dict[str, Any]) -> str:
    """
    Return the most stable broker id for an execution row: prefer perm_id (OPEN_ORDER_ID
    is coalesced to perm_id in fetch_deduped_executions), then payload.perm_id, then
    fall back to TWS local order_id. Returns '' if nothing usable.
    """
    payload = _coerce_payload_dict(row.get("PAYLOAD"))
    for k in (
        row.get("OPEN_ORDER_ID"),
        payload.get("perm_id"),
        payload.get("orderId"),
        payload.get("order_id"),
    ):
        nk = _norm_id(k)
        if nk:
            try:
                iv = int(float(nk))
                # Treat 0 as "not yet assigned" — IB returns 0 for ApiPending.
                if iv == 0:
                    continue
                return str(iv)
            except Exception:
                return nk
    return ""


def _execution_fill_fields(payload: Any) -> tuple[float | None, float | None]:
    payload = _coerce_payload_dict(payload)
    qty = None
    for qk in ("shares", "cumQty", "qty"):
        if payload.get(qk) is not None:
            try:
                qty = float(payload.get(qk))
                break
            except Exception:
                pass
    # IBKR / snapshot serialization varies: camelCase (avgPrice), snake (avg_price), or last price only.
    px = None
    for pk in ("avg_price", "avgPrice", "price"):
        if payload.get(pk) is not None:
            try:
                px = float(payload.get(pk))
                break
            except Exception:
                pass
    return qty, px


def _execution_fill_time(exec_row: dict[str, Any]) -> Any:
    """Return the broker-truth fill time for an execution snapshot row.

    Prefers PAYLOAD:time (the IB-reported execution time) and falls back to
    SNAPSHOT_TS. Returned as the original value (str or datetime) — the
    receiving side coerces via Pydantic / Snowflake binding.
    """
    payload = _coerce_payload_dict(exec_row.get("PAYLOAD"))
    t = payload.get("time")
    if t is None or (isinstance(t, str) and not t.strip()):
        t = exec_row.get("SNAPSHOT_TS")
    return t


def fetch_deduped_executions(cur, account_id: str, lookback_days: int) -> list[dict[str, Any]]:
    cur.execute(
        """
        select
          SNAPSHOT_TS,
          SNAPSHOT_ROW_ID,
          OPEN_ORDER_ID,
          upper(coalesce(SYMBOL, '')) as SYMBOL,
          coalesce(
            OPEN_ORDER_ID::string,
            PAYLOAD:perm_id::string,
            PAYLOAD:orderId::string,
            PAYLOAD:order_id::string
          ) as BROKER_ORDER_ID,
          coalesce(
            PAYLOAD:exec_id::string,
            concat_ws(
              ':',
              coalesce(OPEN_ORDER_ID::string, PAYLOAD:perm_id::string, PAYLOAD:orderId::string, PAYLOAD:order_id::string, ''),
              upper(coalesce(SYMBOL, '')),
              coalesce(PAYLOAD:time::string, SNAPSHOT_TS::string),
              coalesce(PAYLOAD:shares::string, ''),
              coalesce(PAYLOAD:price::string, '')
            )
          ) as EXEC_KEY,
          PAYLOAD
        from MIP.LIVE.BROKER_SNAPSHOTS
        where SNAPSHOT_TYPE = 'EXECUTION'
          and IBKR_ACCOUNT_ID = %s
          and SNAPSHOT_TS >= dateadd(day, -%s, current_timestamp())
        qualify row_number() over (
          partition by coalesce(
            PAYLOAD:exec_id::string,
            concat_ws(
              ':',
              coalesce(OPEN_ORDER_ID::string, PAYLOAD:perm_id::string, PAYLOAD:orderId::string, PAYLOAD:order_id::string, ''),
              upper(coalesce(SYMBOL, '')),
              coalesce(PAYLOAD:time::string, SNAPSHOT_TS::string),
              coalesce(PAYLOAD:shares::string, ''),
              coalesce(PAYLOAD:price::string, '')
            )
          )
          order by SNAPSHOT_TS desc
        ) = 1
        order by SNAPSHOT_TS desc
        """,
        (account_id, int(lookback_days)),
    )
    rows = fetch_all(cur)
    return [dict(x) for x in (rows or []) if isinstance(x, dict)]


def fetch_live_orders_for_portfolio(cur, portfolio_id: int, lookback_days: int) -> list[dict[str, Any]]:
    cur.execute(
        """
        select
          ORDER_ID, ACTION_ID, PORTFOLIO_ID, IBKR_ACCOUNT_ID, BROKER_ORDER_ID,
          STATUS, SYMBOL, SIDE, QTY_ORDERED, QTY_FILLED, AVG_FILL_PRICE
        from MIP.LIVE.LIVE_ORDERS
        where PORTFOLIO_ID = %s
          and coalesce(LAST_UPDATED_AT, CREATED_AT) >= dateadd(day, -%s, current_timestamp())
        """,
        (portfolio_id, int(lookback_days)),
    )
    rows = fetch_all(cur)
    return [dict(x) for x in (rows or []) if isinstance(x, dict)]


def _index_orders_by_broker_id(orders: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    idx: dict[str, list[dict[str, Any]]] = {}
    for o in orders:
        bid = _norm_id(o.get("BROKER_ORDER_ID"))
        if not bid:
            continue
        idx.setdefault(bid, []).append(o)
        try:
            alt = str(int(float(bid)))
            if alt != bid:
                idx.setdefault(alt, []).append(o)
        except Exception:
            pass
    return idx


def classify_execution_against_orders(
    exec_row: dict[str, Any],
    order_index: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    keys = _broker_keys_from_execution_row(exec_row)
    payload = _coerce_payload_dict(exec_row.get("PAYLOAD"))
    exec_qty, exec_price = _execution_fill_fields(payload)
    exec_time = _execution_fill_time(exec_row)
    sym = str(exec_row.get("SYMBOL") or "").upper().strip()

    candidates: list[dict[str, Any]] = []
    seen_oid: set[str] = set()
    for k in keys:
        for o in order_index.get(k, []) or []:
            oid = str(o.get("ORDER_ID") or "")
            if oid and oid not in seen_oid:
                seen_oid.add(oid)
                candidates.append(o)

    base = {
        "exec_key": str(exec_row.get("EXEC_KEY") or ""),
        "snapshot_ts": exec_row.get("SNAPSHOT_TS"),
        "symbol": sym,
        "broker_order_keys_tried": keys,
        "preferred_broker_order_id": _preferred_broker_order_id(exec_row),
        "execution_qty": exec_qty,
        "execution_price": exec_price,
        "execution_time": exec_time,
    }

    if not keys:
        return {**base, "category": "unmatched", "reason_detail": "no_broker_id_in_execution"}

    if len(candidates) == 0:
        return {**base, "category": "unmatched", "reason_detail": "no_live_order_with_broker_id"}

    if len(candidates) > 1:
        return {
            **base,
            "category": "ambiguous",
            "reason_detail": "multiple_live_orders_match_broker_keys",
            "candidate_order_ids": [str(c.get("ORDER_ID")) for c in candidates],
        }

    o = candidates[0]
    st = str(o.get("STATUS") or "").upper()
    oid = str(o.get("ORDER_ID") or "")
    aid = str(o.get("ACTION_ID") or "")
    q_ord = float(o.get("QTY_ORDERED") or 0.0)

    if st == "FILLED":
        return {
            **base,
            "category": "already_synced",
            "order_id": oid,
            "action_id": aid,
            "reason_detail": "live_order_already_filled",
        }

    if st in ("CANCELED", "REJECTED"):
        return {
            **base,
            "category": "unmatched",
            "order_id": oid,
            "action_id": aid,
            "reason_detail": f"live_order_terminal_status_{st}",
        }

    if exec_qty is not None and exec_qty <= 0:
        return {
            **base,
            "category": "unmatched",
            "order_id": oid,
            "action_id": aid,
            "reason_detail": "non_positive_execution_qty",
        }

    if q_ord > 0 and exec_qty is not None and exec_qty > q_ord + 1e-6:
        return {
            **base,
            "category": "ambiguous",
            "order_id": oid,
            "action_id": aid,
            "reason_detail": "execution_qty_exceeds_qty_ordered",
        }

    is_partial = (
        q_ord > 0
        and exec_qty is not None
        and exec_qty + 1e-6 < q_ord
        and st not in ("FILLED", "CANCELED", "REJECTED")
    )

    if is_partial:
        return {
            **base,
            "category": "matched",
            "order_id": oid,
            "action_id": aid,
            "current_status": st,
            "proposed_status": "PARTIAL_FILL",
            "proposed_qty_filled": float(exec_qty),
            "proposed_avg_fill_price": exec_price,
            "proposed_filled_at": exec_time,
            "reason_detail": "partial_fill_from_execution",
        }

    return {
        **base,
        "category": "matched",
        "order_id": oid,
        "action_id": aid,
        "current_status": st,
        "proposed_status": "FILLED",
        "proposed_qty_filled": float(q_ord) if q_ord > 0 else (float(exec_qty) if exec_qty is not None else 0.0),
        "proposed_avg_fill_price": exec_price,
        "proposed_filled_at": exec_time,
        "reason_detail": "ok",
    }


def run_reconcile_dry_run(
    cur,
    *,
    portfolio_id: int,
    account_id: str,
    lookback_days: int = 14,
) -> dict[str, Any]:
    executions = fetch_deduped_executions(cur, account_id, lookback_days)
    orders = fetch_live_orders_for_portfolio(cur, portfolio_id, lookback_days)
    idx = _index_orders_by_broker_id(orders)

    matched: list[dict[str, Any]] = []
    ambiguous: list[dict[str, Any]] = []
    unmatched: list[dict[str, Any]] = []
    already_synced: list[dict[str, Any]] = []

    for ex in executions:
        c = classify_execution_against_orders(ex, idx)
        cat = str(c.get("category") or "")
        if cat == "matched":
            matched.append(c)
        elif cat == "ambiguous":
            ambiguous.append(c)
        elif cat == "already_synced":
            already_synced.append(c)
        else:
            unmatched.append(c)

    return {
        "portfolio_id": portfolio_id,
        "account_id": account_id,
        "lookback_days": lookback_days,
        "counts": {
            "executions_deduped": len(executions),
            "matched": len(matched),
            "ambiguous": len(ambiguous),
            "unmatched": len(unmatched),
            "already_synced": len(already_synced),
        },
        "matched": matched,
        "ambiguous": ambiguous,
        "unmatched": unmatched,
        "already_synced": already_synced,
    }


def verify_apply_item(
    cur,
    *,
    portfolio_id: int,
    account_id: str,
    lookback_days: int,
    exec_key: str,
    order_id: str,
) -> dict[str, Any]:
    executions = fetch_deduped_executions(cur, account_id, lookback_days)
    ex_row = None
    for ex in executions:
        if str(ex.get("EXEC_KEY") or "") == str(exec_key):
            ex_row = ex
            break
    if ex_row is None:
        return {"ok": False, "reason": "exec_key_not_found_in_lookback"}

    orders = fetch_live_orders_for_portfolio(cur, portfolio_id, lookback_days)
    idx = _index_orders_by_broker_id(orders)
    c = classify_execution_against_orders(ex_row, idx)
    if str(c.get("category") or "") != "matched":
        return {"ok": False, "reason": "not_matched", "classification": c}
    if str(c.get("order_id") or "") != str(order_id):
        return {"ok": False, "reason": "order_id_mismatch", "classification": c}
    return {"ok": True, "classification": c}


def insert_reconcile_audit(
    cur,
    *,
    portfolio_id: int,
    action_id: str | None,
    event_type: str,
    payload: dict[str, Any],
) -> str:
    eid = str(uuid.uuid4())
    cur.execute(
        """
        insert into MIP.LIVE.BROKER_EVENT_LEDGER (
          EVENT_ID, EVENT_TS, EVENT_TYPE, PORTFOLIO_ID, ACTION_ID, PAYLOAD
        )
        select %s, current_timestamp(), %s, %s, %s, parse_json(%s)
        """,
        (eid, event_type, portfolio_id, action_id, json.dumps(payload, default=str)),
    )
    return eid
