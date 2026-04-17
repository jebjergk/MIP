"""
Layer 2 — Semantic Reconciliation V2

Compares broker mirror (Layer 1) to MIP expected state (LIVE_ACTIONS + LIVE_ORDERS).
Produces per-symbol reconciliation results with position, entry order, protection,
and orphan checks.

Replaces lifecycle_reconciliation_v1 with:
  - Protection check (FIXED_STOP, TRAILING_STOP, TAKE_PROFIT alignment)
  - Orphan detection (MIP-only orders, broker-only orders)
  - Structural-aware classification
"""

from __future__ import annotations

import json
import logging
import uuid
from typing import Any

from app.db import fetch_all
from app.services.live_intelligence.broker_mirror import BrokerMirror, BrokerOrder

_log = logging.getLogger(__name__)

RULE_VERSION = "RECON_V2"

# ── Reconciliation classes ────────────────────────────────────────────
ALIGNED = "ALIGNED"
POSITION_DRIFT = "POSITION_DRIFT"
POSITION_IN_IB_NOT_IN_MIP = "POSITION_IN_IB_NOT_IN_MIP"
FLAT_IN_IB_OPEN_IN_MIP = "FLAT_IN_IB_OPEN_IN_MIP"
STATUS_MISMATCH = "STATUS_MISMATCH"
PROTECTION_MISSING = "PROTECTION_MISSING"
PROTECTION_MISMATCH = "PROTECTION_MISMATCH"
PROTECTION_CANCELLED = "PROTECTION_CANCELLED"
SUPERSEDED_BY_BROKER = "SUPERSEDED_BY_BROKER"
ORPHANED_MIP_ORDER = "ORPHANED_MIP_ORDER"
ORPHANED_BROKER_ORDER = "ORPHANED_BROKER_ORDER"
PRE_ENTRY = "PRE_ENTRY"

OPERATOR_HEADLINE: dict[str, str] = {
    ALIGNED: "Broker and MIP agree — position, entry, and protection aligned.",
    POSITION_DRIFT: "Position size differs between MIP and IB.",
    POSITION_IN_IB_NOT_IN_MIP: "IB has position but MIP has no matching entry action.",
    FLAT_IN_IB_OPEN_IN_MIP: "MIP shows open trade but IB position is flat.",
    STATUS_MISMATCH: "Order status disagrees between MIP and IB.",
    PROTECTION_MISSING: "Entry filled but no protective stop found in IB.",
    PROTECTION_MISMATCH: "Protection type or price differs between MIP and IB.",
    PROTECTION_CANCELLED: "MIP expects protection but IB cancelled it.",
    SUPERSEDED_BY_BROKER: "Broker modified order parameters MIP did not expect.",
    ORPHANED_MIP_ORDER: "MIP has order record but no matching IB order.",
    ORPHANED_BROKER_ORDER: "IB has order but no matching MIP record.",
    PRE_ENTRY: "Not yet submitted — no broker state expected.",
}


def _safe_float(v, default=None) -> float | None:
    if v is None:
        return default
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _norm_status(s: Any) -> str:
    return str(s or "").strip().upper()


def _norm_broker_id(v: Any) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    if s.endswith(".0") and s[:-2].isdigit():
        s = s[:-2]
    return s


# ── MIP state fetchers ───────────────────────────────────────────────

def _fetch_mip_actions_by_symbol(cur, portfolio_id: int) -> dict[str, dict]:
    """Latest entry action per symbol with structural fields."""
    cur.execute(
        """
        SELECT
            UPPER(SYMBOL) AS SYM,
            ACTION_ID, STATUS, PROPOSED_QTY, SIDE, DIRECTION,
            SETUP_FAMILY, INVALIDATION_LEVEL, TRAIL_STYLE
        FROM MIP.LIVE.LIVE_ACTIONS
        WHERE PORTFOLIO_ID = %s
          AND UPPER(COALESCE(ACTION_INTENT, '')) = 'ENTRY'
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY UPPER(SYMBOL)
            ORDER BY UPDATED_AT DESC NULLS LAST, ACTION_ID DESC
        ) = 1
        """,
        (portfolio_id,),
    )
    out: dict[str, dict] = {}
    for r in fetch_all(cur):
        sym = str(r.get("SYM") or "").upper()
        if sym:
            out[sym] = dict(r)
    return out


def _fetch_mip_orders_by_action(cur, portfolio_id: int) -> dict[str, list[dict]]:
    """All live orders grouped by ACTION_ID."""
    cur.execute(
        """
        SELECT
            ORDER_ID, ACTION_ID, SYMBOL, SIDE, ORDER_TYPE, STATUS,
            QTY_ORDERED, QTY_FILLED, BROKER_ORDER_ID,
            PARENT_ORDER_ID, ORDER_ROLE, PROTECTION_TYPE, OCA_GROUP,
            STOP_PRICE, LIMIT_PRICE
        FROM MIP.LIVE.LIVE_ORDERS
        WHERE PORTFOLIO_ID = %s
          AND UPPER(COALESCE(STATUS, '')) NOT IN ('CANCELED', 'REJECTED', 'EXPIRED')
        """,
        (portfolio_id,),
    )
    out: dict[str, list[dict]] = {}
    for r in fetch_all(cur):
        aid = str(r.get("ACTION_ID") or "")
        if aid:
            out.setdefault(aid, []).append(dict(r))
    return out


def _fetch_ghost_opens(cur, portfolio_id: int, open_symbols: set[str]) -> list[dict]:
    """MIP entries that are EXECUTED but IB has no position."""
    cur.execute(
        """
        SELECT UPPER(SYMBOL) AS SYM, ACTION_ID
        FROM MIP.LIVE.LIVE_ACTIONS
        WHERE PORTFOLIO_ID = %s
          AND UPPER(COALESCE(ACTION_INTENT, '')) = 'ENTRY'
          AND UPPER(COALESCE(STATUS, '')) = 'EXECUTED'
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY UPPER(SYMBOL)
            ORDER BY UPDATED_AT DESC NULLS LAST
        ) = 1
        """,
        (portfolio_id,),
    )
    ghosts = []
    for r in fetch_all(cur):
        sym = str(r.get("SYM") or "").upper()
        if sym and sym not in open_symbols:
            ghosts.append({"symbol": sym, "action_id": str(r.get("ACTION_ID") or "")})
    return ghosts


# ── Position check ────────────────────────────────────────────────────

def _check_position(
    mirror: BrokerMirror,
    mip_action: dict | None,
) -> dict:
    """Compare position quantity."""
    broker_qty = mirror.position_qty
    if mip_action is None:
        return {
            "expected_qty": None,
            "broker_qty": broker_qty,
            "drift": False,
            "detail": "no_mip_action",
        }

    mip_status = _norm_status(mip_action.get("STATUS"))
    expected_qty = _safe_float(mip_action.get("PROPOSED_QTY"))

    if mip_status not in ("EXECUTED", "PARTIAL_FILL"):
        return {
            "expected_qty": expected_qty,
            "broker_qty": broker_qty,
            "drift": False,
            "detail": f"mip_status_{mip_status}_no_position_expected",
        }

    if expected_qty is None:
        return {
            "expected_qty": None,
            "broker_qty": broker_qty,
            "drift": False,
            "detail": "no_expected_qty",
        }

    drift = abs(abs(broker_qty) - abs(expected_qty)) > max(0.05 * abs(expected_qty), 1.0)
    return {
        "expected_qty": expected_qty,
        "broker_qty": broker_qty,
        "drift": drift,
        "detail": "qty_drift" if drift else "aligned",
    }


# ── Entry order check ─────────────────────────────────────────────────

def _check_entry_order(
    mirror: BrokerMirror,
    mip_orders: list[dict],
) -> dict:
    """Check if the entry order status aligns between MIP and IB."""
    entry_orders = [o for o in mip_orders if _norm_status(o.get("ORDER_ROLE")) in ("ENTRY", "") and _norm_status(o.get("SIDE")) == "BUY"]
    if not entry_orders:
        entry_orders = [o for o in mip_orders if _norm_status(o.get("ORDER_TYPE")) in ("LMT", "MKT") and _norm_status(o.get("SIDE")) == "BUY"]

    if not entry_orders:
        return {
            "mip_order_id": None,
            "broker_order_id": None,
            "status_aligned": True,
            "mip_status": None,
            "broker_status": None,
            "detail": "no_entry_order_in_mip",
        }

    entry = entry_orders[0]
    mip_oid = str(entry.get("ORDER_ID") or "")
    mip_bid = _norm_broker_id(entry.get("BROKER_ORDER_ID"))
    mip_status = _norm_status(entry.get("STATUS"))

    broker_match = None
    for bo in mirror.open_orders:
        if _norm_broker_id(bo.broker_order_id) == mip_bid or (bo.perm_id and _norm_broker_id(bo.perm_id) == mip_bid):
            broker_match = bo
            break

    if mip_status == "FILLED":
        return {
            "mip_order_id": mip_oid,
            "broker_order_id": mip_bid,
            "status_aligned": True,
            "mip_status": mip_status,
            "broker_status": "FILLED_NOT_IN_OPEN_ORDERS",
            "detail": "entry_filled",
        }

    if broker_match is None and mip_status in ("SUBMITTED", "PRESUBMITTED", "PENDINGSUBMIT"):
        return {
            "mip_order_id": mip_oid,
            "broker_order_id": mip_bid,
            "status_aligned": False,
            "mip_status": mip_status,
            "broker_status": "NOT_FOUND",
            "detail": "mip_order_not_in_broker",
        }

    broker_st = broker_match.status.upper() if broker_match else "UNKNOWN"
    aligned = (mip_status == broker_st) or (
        mip_status in ("SUBMITTED", "PRESUBMITTED", "PENDINGSUBMIT")
        and broker_st in ("SUBMITTED", "PRESUBMITTED")
    )

    return {
        "mip_order_id": mip_oid,
        "broker_order_id": mip_bid,
        "status_aligned": aligned,
        "mip_status": mip_status,
        "broker_status": broker_st,
        "detail": "aligned" if aligned else "status_mismatch",
    }


# ── Protection check ──────────────────────────────────────────────────

def _check_protection(
    mirror: BrokerMirror,
    mip_action: dict | None,
    mip_orders: list[dict],
) -> dict:
    """Compare expected protection to what IB actually has."""
    if mip_action is None or _norm_status(mip_action.get("STATUS")) not in ("EXECUTED", "PARTIAL_FILL"):
        return {
            "expected_type": None,
            "expected_price": None,
            "broker_type": None,
            "broker_price": None,
            "aligned": True,
            "mismatch_detail": None,
            "detail": "no_active_position",
        }

    # Find expected protection from MIP orders
    protection_orders = [
        o for o in mip_orders
        if _norm_status(o.get("ORDER_ROLE")) in ("PROTECTIVE_STOP", "TRAILING_STOP")
        or _norm_status(o.get("ORDER_TYPE")) == "STP"
    ]

    expected_type = None
    expected_price = None
    expected_bid = None
    if protection_orders:
        po = protection_orders[0]
        ot = _norm_status(po.get("ORDER_TYPE"))
        pt = _norm_status(po.get("PROTECTION_TYPE"))
        if pt in ("TRAILING_STOP",) or ot == "TRAIL":
            expected_type = "TRAILING_STOP"
        elif pt in ("FIXED_STOP", "STRUCTURAL_INVALIDATION") or ot == "STP":
            expected_type = "FIXED_STOP"
        else:
            expected_type = pt or "FIXED_STOP"
        expected_price = _safe_float(po.get("STOP_PRICE"))
        expected_bid = _norm_broker_id(po.get("BROKER_ORDER_ID"))
    elif mip_action.get("INVALIDATION_LEVEL") is not None:
        expected_type = "STRUCTURAL_INVALIDATION"
        expected_price = _safe_float(mip_action.get("INVALIDATION_LEVEL"))

    # Find actual protection in broker
    broker_stops = mirror.protective_stops
    broker_type = None
    broker_price = None
    broker_trailing = False

    if broker_stops:
        bs = broker_stops[0]
        if bs.is_trailing:
            broker_type = "TRAILING_STOP"
            broker_trailing = True
        else:
            broker_type = "FIXED_STOP"
        broker_price = bs.stop_price
    broker_status_cancelled = False
    if expected_bid:
        for bo in mirror.open_orders:
            bid = _norm_broker_id(bo.broker_order_id)
            if bid == expected_bid and bo.status.upper() in ("CANCELLED", "APICANCELLED", "INACTIVE"):
                broker_status_cancelled = True

    # Classify
    if expected_type is None and broker_type is None:
        return {
            "expected_type": None, "expected_price": None,
            "broker_type": None, "broker_price": None,
            "aligned": True, "mismatch_detail": None,
            "detail": "no_protection_expected_or_present",
        }

    if expected_type is not None and broker_type is None:
        if broker_status_cancelled:
            return {
                "expected_type": expected_type, "expected_price": expected_price,
                "broker_type": None, "broker_price": None,
                "aligned": False, "mismatch_detail": "PROTECTION_CANCELLED_EXTERNALLY",
                "detail": "protection_cancelled",
            }
        return {
            "expected_type": expected_type, "expected_price": expected_price,
            "broker_type": None, "broker_price": None,
            "aligned": False, "mismatch_detail": "CHILD_MISSING_AFTER_PARENT_FILL",
            "detail": "protection_missing",
        }

    if expected_type is None and broker_type is not None:
        return {
            "expected_type": None, "expected_price": None,
            "broker_type": broker_type, "broker_price": broker_price,
            "aligned": True, "mismatch_detail": None,
            "detail": "broker_has_extra_protection",
        }

    # Both exist — check alignment
    type_match = (expected_type == broker_type) or (
        expected_type == "STRUCTURAL_INVALIDATION" and broker_type == "FIXED_STOP"
    )
    price_match = True
    if expected_price is not None and broker_price is not None:
        price_match = abs(expected_price - broker_price) < 0.01 * max(abs(expected_price), 1e-6)

    if type_match and price_match:
        return {
            "expected_type": expected_type, "expected_price": expected_price,
            "broker_type": broker_type, "broker_price": broker_price,
            "aligned": True, "mismatch_detail": None,
            "detail": "aligned",
        }

    mismatches: list[str] = []
    if not type_match:
        mismatches.append(f"PROTECTION_TYPE_CHANGED({expected_type}->{broker_type})")
    if not price_match:
        mismatches.append(f"PROTECTION_PRICE_CHANGED({expected_price}->{broker_price})")

    return {
        "expected_type": expected_type, "expected_price": expected_price,
        "broker_type": broker_type, "broker_price": broker_price,
        "aligned": False,
        "mismatch_detail": "; ".join(mismatches),
        "detail": "protection_mismatch",
    }


# ── Orphan check ──────────────────────────────────────────────────────

def _check_orphans(
    mirror: BrokerMirror,
    mip_orders: list[dict],
) -> dict:
    """Detect MIP-only and broker-only orders."""
    mip_broker_ids: set[str] = set()
    for o in mip_orders:
        bid = _norm_broker_id(o.get("BROKER_ORDER_ID"))
        if bid:
            mip_broker_ids.add(bid)

    broker_ids: set[str] = set()
    for bo in mirror.open_orders:
        bid = _norm_broker_id(bo.broker_order_id)
        if bid:
            broker_ids.add(bid)
        if bo.perm_id:
            broker_ids.add(_norm_broker_id(bo.perm_id))

    mip_only = []
    for o in mip_orders:
        bid = _norm_broker_id(o.get("BROKER_ORDER_ID"))
        status = _norm_status(o.get("STATUS"))
        if bid and bid not in broker_ids and status in ("SUBMITTED", "PRESUBMITTED", "PENDINGSUBMIT"):
            mip_only.append(str(o.get("ORDER_ID") or ""))

    broker_only = []
    for bo in mirror.open_orders:
        bid = _norm_broker_id(bo.broker_order_id)
        perm = _norm_broker_id(bo.perm_id) if bo.perm_id else None
        if bid not in mip_broker_ids and (perm is None or perm not in mip_broker_ids):
            broker_only.append(bid)

    return {
        "mip_only_orders": mip_only,
        "broker_only_orders": broker_only,
    }


# ── Top-level classification ─────────────────────────────────────────

def _classify_symbol(
    mirror: BrokerMirror,
    mip_action: dict | None,
    mip_orders: list[dict],
) -> dict:
    """Run all checks and determine reconciliation class."""
    pos_check = _check_position(mirror, mip_action)
    entry_check = _check_entry_order(mirror, mip_orders)
    prot_check = _check_protection(mirror, mip_action, mip_orders)
    orphan_check = _check_orphans(mirror, mip_orders)

    reason_codes: list[str] = []
    requires_attention = False
    recon_class = ALIGNED

    mip_status = _norm_status(mip_action.get("STATUS")) if mip_action else ""

    # Pre-entry: not submitted yet
    if mip_action and mip_status in ("PROPOSED", "RESEARCH_IMPORTED", "PENDING_OPEN_VALIDATION",
                                      "OPEN_BLOCKED", "OPEN_ELIGIBLE", "OPEN_CAUTION",
                                      "READY_FOR_APPROVAL_FLOW", "PENDING_OPEN_STABILITY_REVIEW"):
        return {
            "symbol": mirror.symbol,
            "reconciliation_class": PRE_ENTRY,
            "reason_codes": [],
            "operator_headline": OPERATOR_HEADLINE[PRE_ENTRY],
            "requires_attention": False,
            "position_check": pos_check,
            "entry_order_check": entry_check,
            "protection_check": prot_check,
            "orphan_check": orphan_check,
        }

    # No MIP action for this symbol
    if mip_action is None and mirror.has_position:
        recon_class = POSITION_IN_IB_NOT_IN_MIP
        reason_codes.append("POSITION_IN_IB_NO_MIP_ACTION")
        requires_attention = True
    elif mip_action is None:
        recon_class = ALIGNED
    # IB flat but MIP open
    elif not mirror.has_position and mip_status == "EXECUTED":
        recon_class = FLAT_IN_IB_OPEN_IN_MIP
        reason_codes.append("POSITION_CLOSED_EXTERNALLY")
        requires_attention = True
    # Position drift
    elif pos_check["drift"]:
        recon_class = POSITION_DRIFT
        reason_codes.append("QUANTITY_DRIFT")
        requires_attention = True
    # Protection issues (only when position is filled)
    elif mip_status in ("EXECUTED", "PARTIAL_FILL"):
        if prot_check.get("detail") == "protection_missing":
            recon_class = PROTECTION_MISSING
            reason_codes.append(prot_check.get("mismatch_detail") or "CHILD_MISSING_AFTER_PARENT_FILL")
            requires_attention = True
        elif prot_check.get("detail") == "protection_cancelled":
            recon_class = PROTECTION_CANCELLED
            reason_codes.append("PROTECTION_CANCELLED_EXTERNALLY")
            requires_attention = True
        elif prot_check.get("detail") == "protection_mismatch":
            recon_class = PROTECTION_MISMATCH
            for md in (prot_check.get("mismatch_detail") or "").split("; "):
                if md:
                    reason_codes.append(md.split("(")[0])
            requires_attention = True
    # Entry order status
    elif not entry_check.get("status_aligned"):
        if entry_check.get("detail") == "mip_order_not_in_broker":
            recon_class = ORPHANED_MIP_ORDER
            reason_codes.append("ORPHAN_MIP_ORDER_DETECTED")
            requires_attention = True
        else:
            recon_class = STATUS_MISMATCH
            reason_codes.append("ORDER_STATUS_MISMATCH")
            requires_attention = True

    # Orphan checks (additive — don't override primary class)
    if orphan_check["mip_only_orders"]:
        reason_codes.append("ORPHAN_MIP_ORDER_DETECTED")
        requires_attention = True
        if recon_class == ALIGNED:
            recon_class = ORPHANED_MIP_ORDER
    if orphan_check["broker_only_orders"]:
        reason_codes.append("ORPHAN_BROKER_ORDER_DETECTED")
        requires_attention = True
        if recon_class == ALIGNED:
            recon_class = ORPHANED_BROKER_ORDER

    return {
        "symbol": mirror.symbol,
        "reconciliation_class": recon_class,
        "reason_codes": reason_codes,
        "operator_headline": OPERATOR_HEADLINE.get(recon_class, recon_class),
        "requires_attention": requires_attention,
        "position_check": pos_check,
        "entry_order_check": entry_check,
        "protection_check": prot_check,
        "orphan_check": orphan_check,
    }


# ── Persistence ───────────────────────────────────────────────────────

def _persist_findings(
    cur,
    portfolio_id: int,
    ibkr_account_id: str,
    findings: list[dict],
) -> None:
    for f in findings:
        sym = str(f.get("symbol") or "").upper()
        if not sym:
            continue
        recon_class = f.get("reconciliation_class", ALIGNED)
        details_json = json.dumps(f, default=str)

        try:
            cur.execute(
                """
                SELECT RECONCILIATION_CLASS
                FROM MIP.LIVE.LIFECYCLE_RECONCILIATION_STATE
                WHERE PORTFOLIO_ID = %s AND SYMBOL = %s
                """,
                (portfolio_id, sym),
            )
            prev_rows = fetch_all(cur)
            prev_class = _norm_status((prev_rows[0] or {}).get("RECONCILIATION_CLASS")) if prev_rows else None
        except Exception:
            prev_class = None

        try:
            cur.execute(
                """
                MERGE INTO MIP.LIVE.LIFECYCLE_RECONCILIATION_STATE t
                USING (
                    SELECT
                        %s::NUMBER AS PORTFOLIO_ID,
                        %s AS SYMBOL,
                        %s AS IBKR_ACCOUNT_ID,
                        'STK' AS SECURITY_TYPE,
                        %s AS RULE_VERSION,
                        %s AS RECONCILIATION_CLASS,
                        %s::FLOAT AS BROKER_POSITION_QTY,
                        CURRENT_TIMESTAMP() AS BROKER_SNAPSHOT_TS,
                        %s AS MIP_ENTRY_ACTION_ID,
                        %s AS MIP_ENTRY_STATUS,
                        FALSE AS HAS_ENTRY_INTEL_LINK,
                        PARSE_JSON(%s) AS DETAILS
                ) s
                ON t.PORTFOLIO_ID = s.PORTFOLIO_ID AND t.SYMBOL = s.SYMBOL
                WHEN MATCHED THEN UPDATE SET
                    IBKR_ACCOUNT_ID = s.IBKR_ACCOUNT_ID,
                    RULE_VERSION = s.RULE_VERSION,
                    RECONCILIATION_CLASS = s.RECONCILIATION_CLASS,
                    BROKER_POSITION_QTY = s.BROKER_POSITION_QTY,
                    BROKER_SNAPSHOT_TS = s.BROKER_SNAPSHOT_TS,
                    MIP_ENTRY_ACTION_ID = s.MIP_ENTRY_ACTION_ID,
                    MIP_ENTRY_STATUS = s.MIP_ENTRY_STATUS,
                    DETAILS = s.DETAILS,
                    UPDATED_TS = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN INSERT (
                    PORTFOLIO_ID, SYMBOL, IBKR_ACCOUNT_ID, SECURITY_TYPE, RULE_VERSION,
                    RECONCILIATION_CLASS, BROKER_POSITION_QTY, BROKER_SNAPSHOT_TS,
                    MIP_ENTRY_ACTION_ID, MIP_ENTRY_STATUS, HAS_ENTRY_INTEL_LINK, DETAILS
                ) VALUES (
                    s.PORTFOLIO_ID, s.SYMBOL, s.IBKR_ACCOUNT_ID, s.SECURITY_TYPE, s.RULE_VERSION,
                    s.RECONCILIATION_CLASS, s.BROKER_POSITION_QTY, s.BROKER_SNAPSHOT_TS,
                    s.MIP_ENTRY_ACTION_ID, s.MIP_ENTRY_STATUS, s.HAS_ENTRY_INTEL_LINK, s.DETAILS
                )
                """,
                (
                    portfolio_id, sym, ibkr_account_id,
                    RULE_VERSION, recon_class,
                    f.get("position_check", {}).get("broker_qty", 0.0),
                    f.get("mip_action_id"),
                    f.get("mip_status"),
                    details_json,
                ),
            )
        except Exception as exc:
            _log.warning("Failed to persist recon finding for %s: %s", sym, exc)

        if prev_class and prev_class != recon_class:
            try:
                eid = str(uuid.uuid4())
                cur.execute(
                    """
                    INSERT INTO MIP.LIVE.LIFECYCLE_RECONCILIATION_EVENT (
                        EVENT_ID, PORTFOLIO_ID, SYMBOL, RULE_VERSION, NEW_CLASS, PREVIOUS_CLASS, DETAILS
                    )
                    SELECT %s, %s, %s, %s, %s, %s, PARSE_JSON(%s)
                    """,
                    (eid, portfolio_id, sym, RULE_VERSION, recon_class, prev_class, details_json),
                )
            except Exception as exc:
                _log.warning("Failed to persist recon transition for %s: %s", sym, exc)


# ── Public API ────────────────────────────────────────────────────────

def run_semantic_reconciliation(
    cur,
    portfolio_id: int,
    ibkr_account_id: str,
    broker_mirrors: dict[str, BrokerMirror],
) -> tuple[dict[str, dict], dict]:
    """
    Run Layer 2 semantic reconciliation.

    Returns (reconciliation_by_symbol, meta).
    """
    mip_actions = _fetch_mip_actions_by_symbol(cur, portfolio_id)
    mip_orders_by_action = _fetch_mip_orders_by_action(cur, portfolio_id)

    reconciliation_by_symbol: dict[str, dict] = {}
    findings: list[dict] = []

    all_symbols = set(broker_mirrors.keys()) | set(mip_actions.keys())

    for sym in sorted(all_symbols):
        mirror = broker_mirrors.get(sym)
        mip_action = mip_actions.get(sym)
        action_id = str(mip_action.get("ACTION_ID") or "") if mip_action else ""
        mip_orders = mip_orders_by_action.get(action_id, []) if action_id else []

        if mirror is None:
            # MIP has action, broker has nothing
            mip_status = _norm_status(mip_action.get("STATUS")) if mip_action else ""
            if mip_status == "EXECUTED":
                result = {
                    "symbol": sym,
                    "reconciliation_class": FLAT_IN_IB_OPEN_IN_MIP,
                    "reason_codes": ["POSITION_CLOSED_EXTERNALLY"],
                    "operator_headline": OPERATOR_HEADLINE[FLAT_IN_IB_OPEN_IN_MIP],
                    "requires_attention": True,
                    "position_check": {"expected_qty": _safe_float(mip_action.get("PROPOSED_QTY")), "broker_qty": 0, "drift": True},
                    "entry_order_check": {},
                    "protection_check": {},
                    "orphan_check": {"mip_only_orders": [], "broker_only_orders": []},
                }
            elif mip_status in ("PROPOSED", "RESEARCH_IMPORTED", "PENDING_OPEN_VALIDATION",
                                 "OPEN_BLOCKED", "OPEN_ELIGIBLE", "OPEN_CAUTION",
                                 "READY_FOR_APPROVAL_FLOW", "PENDING_OPEN_STABILITY_REVIEW"):
                result = {
                    "symbol": sym,
                    "reconciliation_class": PRE_ENTRY,
                    "reason_codes": [],
                    "operator_headline": OPERATOR_HEADLINE[PRE_ENTRY],
                    "requires_attention": False,
                    "position_check": {},
                    "entry_order_check": {},
                    "protection_check": {},
                    "orphan_check": {"mip_only_orders": [], "broker_only_orders": []},
                }
            else:
                continue
        else:
            result = _classify_symbol(mirror, mip_action, mip_orders)

        result["mip_action_id"] = action_id
        result["mip_status"] = _norm_status(mip_action.get("STATUS")) if mip_action else None
        result["is_structural"] = bool(mip_action.get("SETUP_FAMILY")) if mip_action else False
        reconciliation_by_symbol[sym] = result
        findings.append(result)

    # Ghost detection is implicit: MIP EXECUTED + no mirror = FLAT_IN_IB_OPEN_IN_MIP above

    summary = {
        "total": len(reconciliation_by_symbol),
        "aligned": sum(1 for r in reconciliation_by_symbol.values() if r["reconciliation_class"] == ALIGNED),
        "attention_required": sum(1 for r in reconciliation_by_symbol.values() if r.get("requires_attention")),
        "protection_issues": sum(1 for r in reconciliation_by_symbol.values()
                                  if r["reconciliation_class"] in (PROTECTION_MISSING, PROTECTION_MISMATCH, PROTECTION_CANCELLED)),
        "orphans": sum(1 for r in reconciliation_by_symbol.values()
                        if r["reconciliation_class"] in (ORPHANED_MIP_ORDER, ORPHANED_BROKER_ORDER)),
    }

    meta = {
        "rule_version": RULE_VERSION,
        "summary": summary,
        "description": (
            "RECON_V2: Two-layer reconciliation. Layer 1 (broker mirror) normalizes IB snapshots. "
            "Layer 2 (this) compares positions, entry orders, protection (stop/trail/TP), and orphan detection."
        ),
    }

    try:
        _persist_findings(cur, portfolio_id, ibkr_account_id, findings)
    except Exception as exc:
        _log.warning("Recon V2 persistence skipped: %s", exc, exc_info=True)

    return reconciliation_by_symbol, meta
