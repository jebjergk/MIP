"""
Broker vs MIP lifecycle reconciliation (RECON_V1).

IB portfolio snapshots are authoritative for *positions*. This module classifies
per-symbol drift between broker truth and MIP LIVE_ACTIONS / ENTRY_INTEL_ACTION_LINK,
persists latest state + transition events, and supplies operator-facing copy for LIC.

No synthetic links: classifications are explicit; we do not invent ENTRY_INTEL_ACTION_LINK rows.
"""

from __future__ import annotations

import json
import logging
import uuid
from typing import Any

from app.db import fetch_all
from app.routers.symbol_tracker import _in_placeholders, _to_float

_log = logging.getLogger(__name__)

RULE_VERSION = "RECON_V1"

# Primary reconciliation classes (v1)
LINKED = "LINKED"
RECONCILED = "RECONCILED"
POSITION_IN_IB_NOT_IN_MIP = "POSITION_IN_IB_NOT_IN_MIP"
BROKER_ORIGINATED_UNLINKED = "BROKER_ORIGINATED_UNLINKED"
STATUS_MISMATCH = "STATUS_MISMATCH"
POSITION_DRIFT = "POSITION_DRIFT"
FLAT_IN_IB_NOT_IN_MIP = "FLAT_IN_IB_NOT_IN_MIP"

OPERATOR_HEADLINE: dict[str, str] = {
    LINKED: "Broker and MIP lifecycle agree (linked entry).",
    RECONCILED: "No open reconciliation issue for this symbol.",
    POSITION_IN_IB_NOT_IN_MIP: "Broker position with no MIP entry action",
    BROKER_ORIGINATED_UNLINKED: "Broker position without entry-intelligence link",
    STATUS_MISMATCH: "Broker position vs MIP execution status disagree",
    POSITION_DRIFT: "Linked entry but size differs from broker position",
    FLAT_IN_IB_NOT_IN_MIP: "MIP shows open lifecycle; broker snapshot is flat",
}

OPERATOR_DETAIL: dict[str, str] = {
    LINKED: "Interactive Brokers shows an open position that matches an EXECUTED entry action linked to entry intelligence. Pre-trade analysis applies.",
    RECONCILED: "Used for symbols without an open broker position when no ghost MIP open entry was detected.",
    POSITION_IN_IB_NOT_IN_MIP: "IB reports a position, but there is no ENTRY row in LIVE_ACTIONS for this symbol. Treat as broker-originated; do not infer entry analysis.",
    BROKER_ORIGINATED_UNLINKED: "MIP has an ENTRY action, but it is not linked to ENTRY_INTEL_ACTION_LINK (or is not the basis for lifecycle intel). Entry analysis is not available until properly linked—never fabricate a link.",
    STATUS_MISMATCH: "Position exists at IB while the latest MIP entry row is not EXECUTED, is EXECUTED without fills recorded, or otherwise disagrees with observable broker state. Review LIVE_ACTIONS and LIVE_ORDERS before trusting lifecycle automation.",
    POSITION_DRIFT: "Lifecycle is linked, but PROPOSED_QTY (or signed size) on the MIP entry action differs materially from the IB position quantity.",
    FLAT_IN_IB_NOT_IN_MIP: "MIP still has an EXECUTED, linked entry without TRADE_CLOSEOUT while IB shows no position—likely broker-side exit or snapshot timing. Do not assume the position is open at IB.",
}


def _norm_status(s: Any) -> str:
    return str(s or "").strip().upper()


def _signed_broker_qty(tile: dict[str, Any]) -> float:
    q = abs(_to_float(tile.get("quantity")) or 0.0)
    side = _norm_status(tile.get("side"))
    return q if side != "SHORT" else -q


def _qty_drift(
    broker_abs_qty: float,
    proposed: float | None,
    *,
    rel_tol: float = 0.05,
    abs_floor: float = 1.0,
) -> bool:
    if proposed is None:
        return False
    try:
        p = abs(float(proposed))
    except (TypeError, ValueError):
        return False
    if broker_abs_qty <= 0:
        return False
    diff = abs(broker_abs_qty - p)
    if diff <= abs_floor:
        return False
    return diff > max(rel_tol * max(broker_abs_qty, p), 1e-9)


def _fetch_latest_entry_by_symbol(cur, portfolio_id: int, symbols_upper: list[str]) -> dict[str, dict[str, Any]]:
    sym_params = list(dict.fromkeys(s.upper() for s in symbols_upper if s))
    if not sym_params:
        return {}
    ph = _in_placeholders(sym_params)
    cur.execute(
        f"""
        with entry_actions as (
          select
            upper(la.SYMBOL) as SYM,
            la.ACTION_ID,
            la.STATUS,
            la.PROPOSED_QTY,
            row_number() over (
              partition by upper(la.SYMBOL)
              order by la.UPDATED_AT desc nulls last, la.ACTION_ID desc
            ) as RN
          from MIP.LIVE.LIVE_ACTIONS la
          where la.PORTFOLIO_ID = %s
            and upper(la.SYMBOL) in ({ph})
            and upper(coalesce(la.ACTION_INTENT, '')) = 'ENTRY'
        )
        select
          e.SYM,
          e.ACTION_ID,
          e.STATUS,
          e.PROPOSED_QTY,
          exists (
            select 1 from MIP.LIVE.ENTRY_INTEL_ACTION_LINK l
            where l.ENTRY_ACTION_ID = e.ACTION_ID
          ) as HAS_LINK
        from entry_actions e
        where e.RN = 1
        """,
        [portfolio_id, *sym_params],
    )
    out: dict[str, dict[str, Any]] = {}
    for r in fetch_all(cur):
        sym = str(r.get("SYM") or r.get("sym") or "").upper()
        if sym:
            out[sym] = r
    return out


def _fetch_order_fill_flags(cur, action_ids: list[str]) -> dict[str, bool]:
    ids = [str(a) for a in action_ids if a]
    if not ids:
        return {}
    ph = _in_placeholders(ids)
    cur.execute(
        f"""
        select
          lo.ACTION_ID,
          max(
            case
              when upper(coalesce(lo.STATUS, '')) in ('FILLED', 'PARTIAL_FILL', 'PARTIALLY_FILLED')
              then 1 else 0
            end
          ) as HAS_FILL
        from MIP.LIVE.LIVE_ORDERS lo
        where lo.ACTION_ID in ({ph})
        group by lo.ACTION_ID
        """,
        ids,
    )
    return {
        str(r.get("ACTION_ID") or r.get("action_id") or ""): bool(r.get("HAS_FILL") or r.get("has_fill"))
        for r in fetch_all(cur)
        if r.get("ACTION_ID") or r.get("action_id")
    }


def _fetch_ghost_mip_opens(cur, portfolio_id: int, open_symbols_upper: set[str]) -> list[dict[str, Any]]:
    """EXECUTED + link + no closeout, symbol not in current IB open positions."""
    cur.execute(
        """
        with picked as (
          select
            upper(la.SYMBOL) as SYM,
            la.ACTION_ID as ENTRY_ACTION_ID,
            row_number() over (
              partition by upper(la.SYMBOL)
              order by la.UPDATED_AT desc nulls last, la.ACTION_ID desc
            ) as RN
          from MIP.LIVE.LIVE_ACTIONS la
          inner join MIP.LIVE.ENTRY_INTEL_ACTION_LINK l
            on l.ENTRY_ACTION_ID = la.ACTION_ID
          where la.PORTFOLIO_ID = %s
            and upper(coalesce(la.ACTION_INTENT, '')) = 'ENTRY'
            and upper(coalesce(la.STATUS, '')) = 'EXECUTED'
        )
        select p.SYM, p.ENTRY_ACTION_ID
        from picked p
        left join MIP.LIVE.TRADE_CLOSEOUT tc
          on tc.ENTRY_ACTION_ID = p.ENTRY_ACTION_ID
        where p.RN = 1
          and tc.CLOSEOUT_ID is null
        """,
        [portfolio_id],
    )
    ghosts: list[dict[str, Any]] = []
    for r in fetch_all(cur):
        sym = str(r.get("SYM") or r.get("sym") or "").upper()
        if not sym or sym in open_symbols_upper:
            continue
        ghosts.append(
            {
                "symbol": sym,
                "entry_action_id": str(r.get("ENTRY_ACTION_ID") or ""),
            }
        )
    return ghosts


def _classify_open_tile(
    sym: str,
    tile: dict[str, Any],
    lifecycle: dict[str, Any] | None,
    mip_row: dict[str, Any] | None,
    has_fill_on_action: bool | None,
) -> tuple[str, dict[str, Any]]:
    details: dict[str, Any] = {
        "rule_version": RULE_VERSION,
        "symbol": sym,
        "broker_side": tile.get("side"),
        "broker_quantity_abs": abs(_to_float(tile.get("quantity")) or 0.0),
    }
    lifecycle = lifecycle or {}
    linked_intel = bool(lifecycle.get("has_entry_intel_link"))

    if linked_intel:
        base = LINKED
        aid = str(lifecycle.get("entry_action_id") or "")
        details["mip_entry_action_id"] = aid
        mq = _to_float(mip_row.get("PROPOSED_QTY") if mip_row else None)
        if _qty_drift(details["broker_quantity_abs"], mq):
            details["proposed_qty"] = mq
            return POSITION_DRIFT, details
        return base, details

    if not mip_row:
        return POSITION_IN_IB_NOT_IN_MIP, details

    aid = str(mip_row.get("ACTION_ID") or mip_row.get("action_id") or "")
    st = _norm_status(mip_row.get("STATUS"))
    has_link = bool(mip_row.get("HAS_LINK") or mip_row.get("has_link"))
    details["mip_entry_action_id"] = aid
    details["mip_entry_status"] = st
    details["has_entry_intel_link"] = has_link

    if not has_link:
        mq = _to_float(mip_row.get("PROPOSED_QTY"))
        if _qty_drift(details["broker_quantity_abs"], mq):
            details["proposed_qty"] = mq
        return BROKER_ORIGINATED_UNLINKED, details

    if st != "EXECUTED":
        return STATUS_MISMATCH, details

    if not has_fill_on_action:
        return STATUS_MISMATCH, details

    # Link exists + EXECUTED + fills but lifecycle query missed (e.g. missing EIS) — still not 'linked' for UI.
    return STATUS_MISMATCH, details


def _persist_findings(
    cur,
    portfolio_id: int,
    ibkr_account_id: str,
    findings: list[dict[str, Any]],
) -> None:
    cur.execute(
        """
        select SYMBOL, RECONCILIATION_CLASS
        from MIP.LIVE.LIFECYCLE_RECONCILIATION_STATE
        where PORTFOLIO_ID = %s
        """,
        [portfolio_id],
    )
    prev = {
        str(r.get("SYMBOL") or r.get("symbol") or "").upper(): _norm_status(
            r.get("RECONCILIATION_CLASS") or r.get("reconciliation_class")
        )
        for r in fetch_all(cur)
    }

    for f in findings:
        sym = str(f.get("symbol") or "").upper()
        if not sym:
            continue
        new_c = _norm_status(f.get("reconciliation_class"))
        old_c = prev.get(sym) or None
        details_json = json.dumps(f.get("details") or {})
        snap_ts = f.get("broker_snapshot_ts")

        cur.execute(
            """
            merge into MIP.LIVE.LIFECYCLE_RECONCILIATION_STATE t
            using (
              select
                %s::number as PORTFOLIO_ID,
                %s as SYMBOL,
                %s as IBKR_ACCOUNT_ID,
                %s as SECURITY_TYPE,
                %s as RULE_VERSION,
                %s as RECONCILIATION_CLASS,
                %s::float as BROKER_POSITION_QTY,
                %s::timestamp_ntz as BROKER_SNAPSHOT_TS,
                %s as MIP_ENTRY_ACTION_ID,
                %s as MIP_ENTRY_STATUS,
                %s::boolean as HAS_ENTRY_INTEL_LINK,
                parse_json(%s) as DETAILS
            ) s
            on t.PORTFOLIO_ID = s.PORTFOLIO_ID and t.SYMBOL = s.SYMBOL
            when matched then update set
              IBKR_ACCOUNT_ID = s.IBKR_ACCOUNT_ID,
              SECURITY_TYPE = s.SECURITY_TYPE,
              RULE_VERSION = s.RULE_VERSION,
              RECONCILIATION_CLASS = s.RECONCILIATION_CLASS,
              BROKER_POSITION_QTY = s.BROKER_POSITION_QTY,
              BROKER_SNAPSHOT_TS = s.BROKER_SNAPSHOT_TS,
              MIP_ENTRY_ACTION_ID = s.MIP_ENTRY_ACTION_ID,
              MIP_ENTRY_STATUS = s.MIP_ENTRY_STATUS,
              HAS_ENTRY_INTEL_LINK = s.HAS_ENTRY_INTEL_LINK,
              DETAILS = s.DETAILS,
              UPDATED_TS = current_timestamp()
            when not matched then insert (
              PORTFOLIO_ID, SYMBOL, IBKR_ACCOUNT_ID, SECURITY_TYPE, RULE_VERSION,
              RECONCILIATION_CLASS, BROKER_POSITION_QTY, BROKER_SNAPSHOT_TS,
              MIP_ENTRY_ACTION_ID, MIP_ENTRY_STATUS, HAS_ENTRY_INTEL_LINK, DETAILS
            ) values (
              s.PORTFOLIO_ID, s.SYMBOL, s.IBKR_ACCOUNT_ID, s.SECURITY_TYPE, s.RULE_VERSION,
              s.RECONCILIATION_CLASS, s.BROKER_POSITION_QTY, s.BROKER_SNAPSHOT_TS,
              s.MIP_ENTRY_ACTION_ID, s.MIP_ENTRY_STATUS, s.HAS_ENTRY_INTEL_LINK, s.DETAILS
            )
            """,
            [
                portfolio_id,
                sym,
                ibkr_account_id,
                f.get("security_type"),
                RULE_VERSION,
                new_c,
                f.get("broker_position_qty"),
                snap_ts,
                f.get("mip_entry_action_id"),
                f.get("mip_entry_status"),
                f.get("has_entry_intel_link"),
                details_json,
            ],
        )

        if old_c and old_c != new_c:
            eid = str(uuid.uuid4())
            cur.execute(
                """
                insert into MIP.LIVE.LIFECYCLE_RECONCILIATION_EVENT (
                  EVENT_ID, PORTFOLIO_ID, SYMBOL, RULE_VERSION, NEW_CLASS, PREVIOUS_CLASS, DETAILS
                )
                select %s, %s, %s, %s, %s, %s, parse_json(%s)
                """,
                [
                    eid,
                    portfolio_id,
                    sym,
                    RULE_VERSION,
                    new_c,
                    old_c,
                    details_json,
                ],
            )


def run_lifecycle_reconciliation(
    cur,
    portfolio_id: int,
    ibkr_account_id: str,
    tiles: list[dict[str, Any]],
    entry_lifecycle_by_symbol: dict[str, Any],
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    """
    Returns (reconciliation_by_symbol, meta).

    reconciliation_by_symbol[sym] includes operator_headline, operator_detail, reconciliation_class, ib_is_truth_note.
    """
    open_syms = {str(t.get("symbol") or "").upper() for t in tiles if t.get("symbol")}
    symbols = sorted(open_syms)
    mip_by_sym = _fetch_latest_entry_by_symbol(cur, portfolio_id, symbols) if symbols else {}
    action_ids = [str(r.get("ACTION_ID") or r.get("action_id") or "") for r in mip_by_sym.values()]
    fill_by_action = _fetch_order_fill_flags(cur, action_ids)

    reconciliation_by_symbol: dict[str, dict[str, Any]] = {}
    findings: list[dict[str, Any]] = []

    for tile in tiles:
        sym = str(tile.get("symbol") or "").upper()
        if not sym:
            continue
        raw_lc = entry_lifecycle_by_symbol.get(sym)
        lifecycle = raw_lc if isinstance(raw_lc, dict) else {}
        mip_row = mip_by_sym.get(sym)
        aid = str(mip_row.get("ACTION_ID") or mip_row.get("action_id") or "") if mip_row else ""
        has_fill = bool(fill_by_action.get(aid)) if aid else False

        cls, details = _classify_open_tile(sym, tile, lifecycle, mip_row, has_fill)
        signed_qty = _signed_broker_qty(tile)
        mip_st = details.get("mip_entry_status") or (
            _norm_status(mip_row.get("STATUS")) if mip_row else None
        )
        hil = bool(lifecycle.get("has_entry_intel_link")) if cls == LINKED else bool(
            details.get("has_entry_intel_link")
            if "has_entry_intel_link" in details
            else (mip_row and bool(mip_row.get("HAS_LINK") or mip_row.get("has_link")))
        )
        rec = {
            "symbol": sym,
            "reconciliation_class": cls,
            "rule_version": RULE_VERSION,
            "operator_headline": OPERATOR_HEADLINE.get(cls, cls),
            "operator_detail": OPERATOR_DETAIL.get(cls, ""),
            "ib_is_truth_note": "Interactive Brokers snapshot is authoritative for open quantity; MIP linkage is informational.",
            "requires_operator_attention": cls != LINKED,
            "details": details,
        }
        reconciliation_by_symbol[sym] = rec
        findings.append(
            {
                "symbol": sym,
                "reconciliation_class": cls,
                "security_type": tile.get("security_type"),
                "broker_position_qty": signed_qty,
                "broker_snapshot_ts": None,
                "mip_entry_action_id": details.get("mip_entry_action_id"),
                "mip_entry_status": mip_st,
                "has_entry_intel_link": hil,
                "details": details,
            }
        )

    ghosts = _fetch_ghost_mip_opens(cur, portfolio_id, open_syms)
    ghost_payload: list[dict[str, Any]] = []
    for g in ghosts:
        sym = g["symbol"]
        rec = {
            "symbol": sym,
            "reconciliation_class": FLAT_IN_IB_NOT_IN_MIP,
            "rule_version": RULE_VERSION,
            "operator_headline": OPERATOR_HEADLINE[FLAT_IN_IB_NOT_IN_MIP],
            "operator_detail": OPERATOR_DETAIL[FLAT_IN_IB_NOT_IN_MIP],
            "ib_is_truth_note": "No open IB position in latest NAV-linked snapshot; MIP lifecycle still shows an open linked entry.",
            "requires_operator_attention": True,
            "details": {
                "rule_version": RULE_VERSION,
                "symbol": sym,
                "entry_action_id": g.get("entry_action_id"),
                "ghost_open_mip": True,
            },
        }
        ghost_payload.append(rec)
        findings.append(
            {
                "symbol": sym,
                "reconciliation_class": FLAT_IN_IB_NOT_IN_MIP,
                "security_type": None,
                "broker_position_qty": 0.0,
                "broker_snapshot_ts": None,
                "mip_entry_action_id": g.get("entry_action_id"),
                "mip_entry_status": "EXECUTED",
                "has_entry_intel_link": True,
                "details": rec["details"],
            }
        )

    meta = {
        "rule_version": RULE_VERSION,
        "ghost_symbols": ghost_payload,
        "matching_rules_summary": (
            "RECON_V1: Open IB tiles are keyed by symbol_tracker positions (latest NAV snapshot). "
            "Latest ENTRY LIVE_ACTION per symbol (by UPDATED_AT) vs ENTRY_INTEL_ACTION_LINK and lifecycle bootstrap row. "
            "STATUS_MISMATCH if linked+EXECUTED but no LIVE_ORDERS fill rows, or EXECUTED without lifecycle row. "
            "Ghost opens: EXECUTED+link+no TRADE_CLOSEOUT with symbol absent from open tiles."
        ),
    }

    try:
        _persist_findings(cur, portfolio_id, ibkr_account_id, findings)
    except Exception as exc:
        _log.warning(
            "lifecycle reconciliation persist skipped (grants or DDL): %s",
            exc,
            exc_info=True,
        )

    return reconciliation_by_symbol, meta
