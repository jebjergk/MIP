"""
Layer 1 — Broker Mirror

Pure normalization of BROKER_SNAPSHOTS into per-symbol state objects.
No business logic. No MIP comparison. Just what IB says exists right now.

Input:  Raw BROKER_SNAPSHOTS rows (types: POSITION, OPEN_ORDER, EXECUTION)
Output: dict[symbol, BrokerMirror] with positions, open orders, recent executions
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any

from app.db import fetch_all

_log = logging.getLogger(__name__)


def _safe_float(v, default: float | None = None) -> float | None:
    if v is None:
        return default
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _safe_str(v, default: str = "") -> str:
    return str(v).strip() if v is not None else default


@dataclass
class BrokerOrder:
    broker_order_id: str
    perm_id: str | None
    order_type: str            # LMT, STP, TRAIL, MKT, etc.
    side: str                  # BUY or SELL
    qty: float
    filled_qty: float
    remaining_qty: float
    limit_price: float | None
    stop_price: float | None   # auxPrice for STP orders
    trail_amount: float | None
    trail_percent: float | None
    status: str                # PreSubmitted, Submitted, Filled, Cancelled, etc.
    parent_id: str | None      # IB parentId if child order
    oca_group: str | None
    symbol: str
    snapshot_ts: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "broker_order_id": self.broker_order_id,
            "perm_id": self.perm_id,
            "order_type": self.order_type,
            "side": self.side,
            "qty": self.qty,
            "filled_qty": self.filled_qty,
            "remaining_qty": self.remaining_qty,
            "limit_price": self.limit_price,
            "stop_price": self.stop_price,
            "trail_amount": self.trail_amount,
            "trail_percent": self.trail_percent,
            "status": self.status,
            "parent_id": self.parent_id,
            "oca_group": self.oca_group,
            "symbol": self.symbol,
            "snapshot_ts": self.snapshot_ts,
        }

    @property
    def is_protective_stop(self) -> bool:
        return self.order_type in ("STP", "TRAIL") and self.status not in ("Cancelled", "Inactive", "ApiCancelled")

    @property
    def is_take_profit(self) -> bool:
        return self.order_type == "LMT" and self.status not in ("Cancelled", "Inactive", "ApiCancelled")

    @property
    def is_trailing(self) -> bool:
        return self.order_type == "TRAIL"


@dataclass
class BrokerExecution:
    exec_id: str
    side: str
    qty: float
    price: float
    time: str | None
    broker_order_id: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "exec_id": self.exec_id,
            "side": self.side,
            "qty": self.qty,
            "price": self.price,
            "time": self.time,
            "broker_order_id": self.broker_order_id,
        }


@dataclass
class BrokerMirror:
    symbol: str
    has_position: bool
    position_qty: float        # signed: positive=long, negative=short
    avg_cost: float | None
    market_value: float | None
    unrealized_pnl: float | None
    open_orders: list[BrokerOrder] = field(default_factory=list)
    recent_executions: list[BrokerExecution] = field(default_factory=list)
    snapshot_ts: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "has_position": self.has_position,
            "position_qty": self.position_qty,
            "avg_cost": self.avg_cost,
            "market_value": self.market_value,
            "unrealized_pnl": self.unrealized_pnl,
            "open_orders": [o.to_dict() for o in self.open_orders],
            "recent_executions": [e.to_dict() for e in self.recent_executions],
            "snapshot_ts": self.snapshot_ts,
        }

    @property
    def is_long(self) -> bool:
        return self.position_qty > 0

    @property
    def is_short(self) -> bool:
        return self.position_qty < 0

    @property
    def protective_stops(self) -> list[BrokerOrder]:
        return [o for o in self.open_orders if o.is_protective_stop]

    @property
    def take_profits(self) -> list[BrokerOrder]:
        return [o for o in self.open_orders if o.is_take_profit]

    @property
    def trailing_stops(self) -> list[BrokerOrder]:
        return [o for o in self.open_orders if o.is_trailing]


def _parse_payload(raw: Any) -> dict:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return {}
    return {}


def _build_order(row: dict, payload: dict) -> BrokerOrder:
    order_type = _safe_str(payload.get("orderType")).upper()
    side = _safe_str(payload.get("action")).upper()
    total_qty = _safe_float(payload.get("remaining"), 0.0) + _safe_float(payload.get("filled"), 0.0)
    aux = _safe_float(payload.get("auxPrice"))

    stop_price = aux if order_type in ("STP", "TRAIL") else None
    trail_amount = aux if order_type == "TRAIL" and aux and aux > 0 else None
    trail_pct = _safe_float(payload.get("trailingPercent"))

    return BrokerOrder(
        broker_order_id=_safe_str(row.get("OPEN_ORDER_ID") or payload.get("permId")),
        perm_id=_safe_str(payload.get("permId")) or None,
        order_type=order_type,
        side=side,
        qty=total_qty or 0.0,
        filled_qty=_safe_float(payload.get("filled"), 0.0),
        remaining_qty=_safe_float(payload.get("remaining"), 0.0),
        limit_price=_safe_float(payload.get("lmtPrice")) if order_type in ("LMT", "LMT+MKT_PRT") else None,
        stop_price=stop_price,
        trail_amount=trail_amount,
        trail_percent=trail_pct,
        status=_safe_str(payload.get("status")),
        parent_id=_safe_str(payload.get("parentId")) or None,
        oca_group=_safe_str(payload.get("ocaGroup")) or None,
        symbol=_safe_str(row.get("SYMBOL")).upper(),
        snapshot_ts=str(row.get("SNAPSHOT_TS") or ""),
    )


def _build_execution(row: dict, payload: dict) -> BrokerExecution:
    return BrokerExecution(
        exec_id=_safe_str(payload.get("exec_id") or payload.get("execId") or row.get("SNAPSHOT_ROW_ID")),
        side=_safe_str(payload.get("side") or payload.get("action")).upper(),
        qty=_safe_float(payload.get("shares") or payload.get("cumQty") or payload.get("qty"), 0.0),
        price=_safe_float(payload.get("avg_price") or payload.get("avgPrice") or payload.get("price"), 0.0),
        time=_safe_str(payload.get("time") or row.get("SNAPSHOT_TS")),
        broker_order_id=_safe_str(row.get("OPEN_ORDER_ID") or payload.get("perm_id") or payload.get("orderId")) or None,
    )


def build_broker_mirror(
    cur,
    account_id: str,
    *,
    lookback_days: int = 7,
) -> dict[str, BrokerMirror]:
    """
    Build per-symbol broker mirror from latest BROKER_SNAPSHOTS.

    Returns dict keyed by uppercase symbol.
    """
    mirrors: dict[str, BrokerMirror] = {}

    # 1. Positions — latest snapshot per symbol
    cur.execute(
        """
        SELECT SYMBOL, SNAPSHOT_TS, PAYLOAD
        FROM MIP.LIVE.BROKER_SNAPSHOTS
        WHERE SNAPSHOT_TYPE = 'POSITION'
          AND IBKR_ACCOUNT_ID = %s
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY UPPER(SYMBOL)
            ORDER BY SNAPSHOT_TS DESC
        ) = 1
        """,
        (account_id,),
    )
    for row in fetch_all(cur):
        sym = _safe_str(row.get("SYMBOL")).upper()
        if not sym:
            continue
        p = _parse_payload(row.get("PAYLOAD"))
        qty = _safe_float(p.get("position"), 0.0)
        mirrors[sym] = BrokerMirror(
            symbol=sym,
            has_position=abs(qty) > 0,
            position_qty=qty,
            avg_cost=_safe_float(p.get("avgCost")),
            market_value=_safe_float(p.get("marketValue")),
            unrealized_pnl=_safe_float(p.get("unrealizedPNL")),
            snapshot_ts=str(row.get("SNAPSHOT_TS") or ""),
        )

    # 2. Open orders — latest per permId/orderId
    cur.execute(
        """
        SELECT SYMBOL, OPEN_ORDER_ID, SNAPSHOT_TS, PAYLOAD
        FROM MIP.LIVE.BROKER_SNAPSHOTS
        WHERE SNAPSHOT_TYPE = 'OPEN_ORDER'
          AND IBKR_ACCOUNT_ID = %s
          AND SNAPSHOT_TS >= DATEADD(DAY, -%s, CURRENT_TIMESTAMP())
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY COALESCE(OPEN_ORDER_ID, PAYLOAD:permId::STRING)
            ORDER BY SNAPSHOT_TS DESC
        ) = 1
        """,
        (account_id, lookback_days),
    )
    for row in fetch_all(cur):
        sym = _safe_str(row.get("SYMBOL")).upper()
        if not sym:
            continue
        p = _parse_payload(row.get("PAYLOAD"))
        status = _safe_str(p.get("status"))
        if status.upper() in ("CANCELLED", "APICANCELLED", "INACTIVE"):
            continue
        order = _build_order(row, p)
        if sym not in mirrors:
            mirrors[sym] = BrokerMirror(
                symbol=sym, has_position=False, position_qty=0.0,
                avg_cost=None, market_value=None, unrealized_pnl=None,
            )
        mirrors[sym].open_orders.append(order)

    # 3. Recent executions
    cur.execute(
        """
        SELECT SYMBOL, OPEN_ORDER_ID, SNAPSHOT_TS, PAYLOAD
        FROM MIP.LIVE.BROKER_SNAPSHOTS
        WHERE SNAPSHOT_TYPE = 'EXECUTION'
          AND IBKR_ACCOUNT_ID = %s
          AND SNAPSHOT_TS >= DATEADD(DAY, -%s, CURRENT_TIMESTAMP())
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY COALESCE(
                PAYLOAD:exec_id::STRING,
                CONCAT_WS(':', COALESCE(OPEN_ORDER_ID::STRING, ''), UPPER(COALESCE(SYMBOL, '')),
                           COALESCE(PAYLOAD:time::STRING, SNAPSHOT_TS::STRING))
            )
            ORDER BY SNAPSHOT_TS DESC
        ) = 1
        ORDER BY SNAPSHOT_TS DESC
        """,
        (account_id, lookback_days),
    )
    for row in fetch_all(cur):
        sym = _safe_str(row.get("SYMBOL")).upper()
        if not sym:
            continue
        p = _parse_payload(row.get("PAYLOAD"))
        ex = _build_execution(row, p)
        if sym not in mirrors:
            mirrors[sym] = BrokerMirror(
                symbol=sym, has_position=False, position_qty=0.0,
                avg_cost=None, market_value=None, unrealized_pnl=None,
            )
        mirrors[sym].recent_executions.append(ex)

    return mirrors
