"""
Trade-plan loader for the cockpit's open-position rows.

Pulls three pieces of operator-facing context per currently-open
live position:

  1. Broker truth (canonical P&L source)
       MIP.MART.V_LIVE_OPEN_POSITIONS
         - QUANTITY (signed; negative = short)
         - AVG_COST
         - MARKET_VALUE
         - UNREALIZED_PNL              (dollars)
         - ENTRY_PRICE                 (structural entry, may differ
                                        slightly from AVG_COST after
                                        partial fills/dividends)
         - PROPOSAL_ID                 (joins to LIVE_ACTIONS)

  2. Protective levels (LIVE_ORDERS)
       Take-profit  : ORDER_ROLE='PROTECTIVE_TP' (LMT, opposite side)
       Stop loss    : ORDER_ROLE='PROTECTIVE_STOP'   (STP, hard level)
                      ORDER_ROLE='TRAILING_STOP'      (TRAIL, dynamic)
       For trailing stops we expose:
         sl_price       — best known stop level
         sl_is_dynamic  — True (so the UI shows it as approximate)
         sl_label       — "Trailing stop (last known)" when sourced
                          from LAST_KNOWN_STOP_LEVEL, otherwise
                          "Trailing stop"
       Active statuses considered: SUBMITTED, PENDINGSUBMIT,
       PRESUBMITTED. (FILLED / CANCELLED / REJECTED / EXPIRED ignored.)

  3. Thesis text (LIVE_ACTIONS)
       SETUP_NARRATIVE       → distilled into thesis_line
       PROPOSAL_RATIONALE    → distilled into expectation_line
       INVALIDATION_LEVEL    → exposed verbatim
       SUPPORTING_LEVEL      → exposed verbatim
       ENTRY_ZONE_LOW/HIGH   → exposed verbatim
       DIRECTION             → 'LONG' | 'SHORT' | None

All three queries are bounded by the active portfolio, so they scale
with portfolio size, not universe size.

This module is fail-soft: any Snowflake error returns an empty
TradePlanIndex so the cockpit still renders.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from app.db import fetch_all, get_connection, serialize_row

logger = logging.getLogger(__name__)


# Active LIVE_ORDERS statuses that count as "still working at IB".
_ACTIVE_ORDER_STATUSES = ("SUBMITTED", "PENDINGSUBMIT", "PRESUBMITTED")


# --- Public types -----------------------------------------------------------


@dataclass(frozen=True)
class TradePlan:
    """Full trade-plan view for a single open position. All fields are
    optional because broker, orders, and thesis are independent sources;
    a position can have any subset of them."""
    symbol: str
    portfolio_id: int
    direction: Optional[str]            # 'LONG' | 'SHORT' | None
    quantity: Optional[float]           # signed broker qty
    avg_cost: Optional[float]
    entry_price: Optional[float]        # structural entry from view
    market_value: Optional[float]
    current_price: Optional[float]      # MARKET_VALUE / |qty| if both set
    unrealized_pnl: Optional[float]     # dollars (broker)
    unrealized_pnl_pct: Optional[float] # DECIMAL fraction (e.g. 0.0123)

    tp_price: Optional[float]
    tp_label: Optional[str]             # "Take profit"

    sl_price: Optional[float]
    sl_label: Optional[str]             # "Stop" | "Trailing stop (last known)" | "Trailing stop"
    sl_is_dynamic: bool                 # True for trailing stops

    invalidation_level: Optional[float] # from LIVE_ACTIONS
    supporting_level: Optional[float]   # from LIVE_ACTIONS
    entry_zone_low: Optional[float]
    entry_zone_high: Optional[float]

    thesis_line: Optional[str]          # one-line distilled setup
    expectation_line: Optional[str]     # one-line distilled rationale


@dataclass
class TradePlanIndex:
    """Symbol-keyed lookup so callers don't have to filter lists."""
    by_symbol: Dict[str, TradePlan] = field(default_factory=dict)

    def get(self, symbol: str) -> Optional[TradePlan]:
        return self.by_symbol.get((symbol or "").upper())


# --- SQL --------------------------------------------------------------------


_BROKER_FACTS_SQL = """
    SELECT
        v.SYMBOL,
        v.QUANTITY,
        v.AVG_COST,
        v.MARKET_VALUE,
        v.UNREALIZED_PNL,
        v.ENTRY_PRICE,
        v.PROPOSAL_ID
    FROM MIP.MART.V_LIVE_OPEN_POSITIONS v
    WHERE v.PORTFOLIO_ID = %(portfolio_id)s
"""

# Pull every active order for the portfolio's symbols. We keep multiple
# rows per symbol (e.g. TP + SL + manual MKT) and pick the canonical
# protective levels in Python so the rules are easy to read.
_ACTIVE_ORDERS_SQL = """
    SELECT
        SYMBOL,
        SIDE,
        ORDER_TYPE,
        STATUS,
        LIMIT_PRICE,
        STOP_PRICE,
        LAST_KNOWN_STOP_LEVEL,
        TRAIL_AMOUNT,
        TRAIL_PERCENT,
        TRAIL_ACTIVATION_PRICE,
        ORDER_ROLE,
        OCA_GROUP
    FROM MIP.LIVE.LIVE_ORDERS
    WHERE PORTFOLIO_ID = %(portfolio_id)s
      AND STATUS IN ('SUBMITTED', 'PENDINGSUBMIT', 'PRESUBMITTED')
"""

# Latest LIVE_ACTIONS row per PROPOSAL_ID. PROPOSAL_ID can be null on
# some V_LIVE_OPEN_POSITIONS rows (manual entry, legacy), so the join
# in Python is keyed by both PROPOSAL_ID and SYMBOL fallback.
_THESIS_SQL = """
    SELECT
        a.PROPOSAL_ID,
        a.SYMBOL,
        a.DIRECTION,
        a.SETUP_NARRATIVE,
        a.PROPOSAL_RATIONALE,
        a.INVALIDATION_LEVEL,
        a.SUPPORTING_LEVEL,
        a.ENTRY_ZONE_LOW,
        a.ENTRY_ZONE_HIGH
    FROM MIP.LIVE.LIVE_ACTIONS a
    WHERE a.PORTFOLIO_ID = %(portfolio_id)s
      AND a.PROPOSAL_ID IN (
        SELECT v.PROPOSAL_ID
        FROM MIP.MART.V_LIVE_OPEN_POSITIONS v
        WHERE v.PORTFOLIO_ID = %(portfolio_id)s
          AND v.PROPOSAL_ID IS NOT NULL
      )
"""


def _query(sql: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        try:
            cur.execute(sql, params)
            return [serialize_row(r) for r in fetch_all(cur)]
        finally:
            cur.close()
    finally:
        conn.close()


# --- Helpers ----------------------------------------------------------------


def _f(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _direction_from_quantity(qty: Optional[float]) -> Optional[str]:
    if qty is None:
        return None
    if qty > 0:
        return "LONG"
    if qty < 0:
        return "SHORT"
    return None


def _classify_protective_orders(
    direction: Optional[str],
    orders: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    From all active orders for one symbol, pick the canonical TP and SL.
    Preference order:
      TP : ORDER_ROLE='PROTECTIVE_TP'   → LIMIT_PRICE
           else first active LMT on the opposite side of `direction`
      SL : ORDER_ROLE='TRAILING_STOP'   → trailing levels (dynamic)
           ORDER_ROLE='PROTECTIVE_STOP' → COALESCE(STOP_PRICE, LIMIT_PRICE)
           else first active STP/TRAIL on the opposite side of `direction`
    """
    out: Dict[str, Any] = {
        "tp_price": None,
        "tp_label": None,
        "sl_price": None,
        "sl_label": None,
        "sl_is_dynamic": False,
    }
    if not orders:
        return out

    exit_side = "SELL" if direction == "LONG" else ("BUY" if direction == "SHORT" else None)

    role_tp = next(
        (o for o in orders if (o.get("ORDER_ROLE") or "").upper() == "PROTECTIVE_TP"),
        None,
    )
    role_trail = next(
        (o for o in orders if (o.get("ORDER_ROLE") or "").upper() == "TRAILING_STOP"),
        None,
    )
    role_stop = next(
        (o for o in orders if (o.get("ORDER_ROLE") or "").upper() == "PROTECTIVE_STOP"),
        None,
    )

    fallback_tp = next(
        (
            o for o in orders
            if (o.get("ORDER_TYPE") or "").upper() == "LMT"
            and (exit_side is None or (o.get("SIDE") or "").upper() == exit_side)
            and (o.get("ORDER_ROLE") or "").upper() != "ENTRY"
        ),
        None,
    )
    fallback_stop = next(
        (
            o for o in orders
            if (o.get("ORDER_TYPE") or "").upper() in ("STP", "TRAIL", "STP LMT")
            and (exit_side is None or (o.get("SIDE") or "").upper() == exit_side)
        ),
        None,
    )

    chosen_tp = role_tp or fallback_tp
    if chosen_tp is not None:
        out["tp_price"] = _f(chosen_tp.get("LIMIT_PRICE")) or _f(chosen_tp.get("STOP_PRICE"))
        out["tp_label"] = "Take profit"

    trail_candidate: Optional[Dict[str, Any]] = None
    if role_trail is not None:
        last_known = _f(role_trail.get("LAST_KNOWN_STOP_LEVEL"))
        activation = _f(role_trail.get("TRAIL_ACTIVATION_PRICE"))
        stop_px = _f(role_trail.get("STOP_PRICE"))
        trail_price = (
            last_known if last_known is not None
            else (activation if activation is not None else stop_px)
        )
        if last_known is not None:
            trail_label = "Trailing stop (last known)"
        elif activation is not None:
            trail_label = "Trailing stop (activates)"
        else:
            trail_label = "Trailing stop"
        trail_candidate = {
            "price": trail_price,
            "label": trail_label,
            "is_dynamic": True,
        }

    static_candidate: Optional[Dict[str, Any]] = None
    if role_stop is not None:
        static_candidate = {
            "price": _f(role_stop.get("STOP_PRICE")) or _f(role_stop.get("LIMIT_PRICE")),
            "label": "Stop",
            "is_dynamic": False,
        }

    # Prefer trailing only if it has a concrete level. Otherwise the static
    # protective stop is the actual operational stop right now (e.g. trail
    # not yet activated and not snapshotted).
    chosen_sl: Optional[Dict[str, Any]] = None
    if trail_candidate is not None and trail_candidate["price"] is not None:
        chosen_sl = trail_candidate
    elif static_candidate is not None and static_candidate["price"] is not None:
        chosen_sl = static_candidate
    elif trail_candidate is not None:
        chosen_sl = trail_candidate
    elif static_candidate is not None:
        chosen_sl = static_candidate

    if chosen_sl is not None:
        out["sl_price"] = chosen_sl["price"]
        out["sl_label"] = chosen_sl["label"]
        out["sl_is_dynamic"] = bool(chosen_sl["is_dynamic"])
    elif fallback_stop is not None:
        is_trail = (fallback_stop.get("ORDER_TYPE") or "").upper() == "TRAIL"
        if is_trail:
            last_known = _f(fallback_stop.get("LAST_KNOWN_STOP_LEVEL"))
            activation = _f(fallback_stop.get("TRAIL_ACTIVATION_PRICE"))
            stop_px = _f(fallback_stop.get("STOP_PRICE"))
            out["sl_price"] = last_known if last_known is not None else (activation if activation is not None else stop_px)
            out["sl_is_dynamic"] = True
            out["sl_label"] = (
                "Trailing stop (last known)" if last_known is not None else "Trailing stop"
            )
        else:
            out["sl_price"] = _f(fallback_stop.get("STOP_PRICE")) or _f(fallback_stop.get("LIMIT_PRICE"))
            out["sl_label"] = "Stop"
            out["sl_is_dynamic"] = False

    return out


def _distill_thesis(narrative: Optional[str], rationale: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """
    Compress LIVE_ACTIONS narrative + rationale into two display lines
    (≤ 140 chars each) for the trade panel.

    The source strings already start with operator-tone copy; we trim
    aggressively rather than parse them, so this remains deterministic
    even when narrative formatting drifts.
    """
    cap = 140

    def _clean(s: Optional[str]) -> Optional[str]:
        if not s:
            return None
        t = " ".join(str(s).split()).strip()
        if not t:
            return None
        if len(t) > cap:
            t = t[: cap - 1].rstrip(",;:.") + "…"
        return t

    return _clean(narrative), _clean(rationale)


# --- Public entry point ----------------------------------------------------


def load_trade_plans(portfolio_id: int) -> TradePlanIndex:
    """
    Build the full {symbol → TradePlan} index for the portfolio's open
    positions. Each Snowflake call is wrapped in try/except so partial
    data still surfaces (broker P&L can render even if LIVE_ORDERS or
    LIVE_ACTIONS is unavailable).
    """
    pid = int(portfolio_id)

    try:
        broker_rows = _query(_BROKER_FACTS_SQL, {"portfolio_id": pid})
    except Exception as exc:
        logger.warning("trade_plan: broker facts query failed: %s", exc)
        return TradePlanIndex()

    if not broker_rows:
        return TradePlanIndex()

    try:
        order_rows = _query(_ACTIVE_ORDERS_SQL, {"portfolio_id": pid})
    except Exception as exc:
        logger.warning("trade_plan: live-orders query failed: %s", exc)
        order_rows = []

    try:
        thesis_rows = _query(_THESIS_SQL, {"portfolio_id": pid})
    except Exception as exc:
        logger.warning("trade_plan: live-actions query failed: %s", exc)
        thesis_rows = []

    orders_by_symbol: Dict[str, List[Dict[str, Any]]] = {}
    for o in order_rows:
        sym = str(o.get("SYMBOL") or "").upper()
        if not sym:
            continue
        orders_by_symbol.setdefault(sym, []).append(o)

    thesis_by_proposal: Dict[int, Dict[str, Any]] = {}
    thesis_by_symbol: Dict[str, Dict[str, Any]] = {}
    for t in thesis_rows:
        pid_key = t.get("PROPOSAL_ID")
        if pid_key is not None:
            try:
                thesis_by_proposal[int(pid_key)] = t
            except (TypeError, ValueError):
                pass
        sym = str(t.get("SYMBOL") or "").upper()
        if sym:
            thesis_by_symbol.setdefault(sym, t)

    index = TradePlanIndex()
    for r in broker_rows:
        symbol = str(r.get("SYMBOL") or "").upper()
        if not symbol:
            continue

        qty = _f(r.get("QUANTITY"))
        avg_cost = _f(r.get("AVG_COST"))
        market_value = _f(r.get("MARKET_VALUE"))
        upnl = _f(r.get("UNREALIZED_PNL"))
        entry_price = _f(r.get("ENTRY_PRICE"))
        proposal_id = r.get("PROPOSAL_ID")
        direction = _direction_from_quantity(qty)

        current_price: Optional[float] = None
        if market_value is not None and qty is not None and qty != 0:
            current_price = market_value / abs(qty)

        unrealized_pct: Optional[float] = None
        if (
            avg_cost is not None
            and avg_cost > 0
            and current_price is not None
            and direction in ("LONG", "SHORT")
        ):
            move = (current_price - avg_cost) / avg_cost
            unrealized_pct = move if direction == "LONG" else -move
        elif (
            upnl is not None
            and avg_cost is not None
            and avg_cost > 0
            and qty is not None
            and qty != 0
        ):
            cost_basis = abs(qty) * avg_cost
            if cost_basis > 0:
                unrealized_pct = upnl / cost_basis

        protective = _classify_protective_orders(direction, orders_by_symbol.get(symbol, []))

        thesis_row: Optional[Dict[str, Any]] = None
        if proposal_id is not None:
            try:
                thesis_row = thesis_by_proposal.get(int(proposal_id))
            except (TypeError, ValueError):
                thesis_row = None
        if thesis_row is None:
            thesis_row = thesis_by_symbol.get(symbol)

        thesis_line: Optional[str] = None
        expectation_line: Optional[str] = None
        invalidation_level: Optional[float] = None
        supporting_level: Optional[float] = None
        entry_zone_low: Optional[float] = None
        entry_zone_high: Optional[float] = None
        if thesis_row is not None:
            thesis_line, expectation_line = _distill_thesis(
                thesis_row.get("SETUP_NARRATIVE"),
                thesis_row.get("PROPOSAL_RATIONALE"),
            )
            invalidation_level = _f(thesis_row.get("INVALIDATION_LEVEL"))
            supporting_level = _f(thesis_row.get("SUPPORTING_LEVEL"))
            entry_zone_low = _f(thesis_row.get("ENTRY_ZONE_LOW"))
            entry_zone_high = _f(thesis_row.get("ENTRY_ZONE_HIGH"))
            if not direction:
                d = (thesis_row.get("DIRECTION") or "").upper()
                if d in ("LONG", "SHORT"):
                    direction = d

        index.by_symbol[symbol] = TradePlan(
            symbol=symbol,
            portfolio_id=pid,
            direction=direction,
            quantity=qty,
            avg_cost=avg_cost,
            entry_price=entry_price,
            market_value=market_value,
            current_price=current_price,
            unrealized_pnl=upnl,
            unrealized_pnl_pct=unrealized_pct,
            tp_price=protective["tp_price"],
            tp_label=protective["tp_label"],
            sl_price=protective["sl_price"],
            sl_label=protective["sl_label"],
            sl_is_dynamic=bool(protective["sl_is_dynamic"]),
            invalidation_level=invalidation_level,
            supporting_level=supporting_level,
            entry_zone_low=entry_zone_low,
            entry_zone_high=entry_zone_high,
            thesis_line=thesis_line,
            expectation_line=expectation_line,
        )

    return index
