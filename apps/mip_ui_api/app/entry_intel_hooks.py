"""Entry Intelligence Snapshot (EIS) lifecycle hooks — Snowflake only."""
from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime
from typing import Any

from app.closeout_alignment_v1 import (
    classify_realized_outcome_class,
    compute_alignment_rule_v1,
    compute_position_return_pct,
    recommended_action_from_alpha_spec,
)

_log = logging.getLogger(__name__)


def fetch_latest_snapshot_id_for_proposal(cur, proposal_id: int) -> str | None:
    cur.execute(
        """
        select SNAPSHOT_ID
        from MIP.LIVE.ENTRY_INTEL_SNAPSHOT
        where PROPOSAL_ID = %s
        order by EIS_VERSION desc
        limit 1
        """,
        (proposal_id,),
    )
    row = cur.fetchone()
    if not row or row[0] is None:
        return None
    return str(row[0])


def ensure_entry_intel_for_proposal(cur, proposal_id: int) -> None:
    try:
        cur.execute("call MIP.APP.SP_ENSURE_ENTRY_INTEL_FOR_PROPOSAL(%s)", (proposal_id,))
    except Exception as exc:
        _log.warning(
            "EIS SP_ENSURE_ENTRY_INTEL_FOR_PROPOSAL failed proposal_id=%s: %s",
            proposal_id,
            exc,
            exc_info=True,
        )


def insert_entry_intel_action_link(cur, proposal_id: int, entry_action_id: str) -> None:
    sid = fetch_latest_snapshot_id_for_proposal(cur, proposal_id)
    if not sid:
        ensure_entry_intel_for_proposal(cur, proposal_id)
        sid = fetch_latest_snapshot_id_for_proposal(cur, proposal_id)
    if not sid:
        return
    link_id = str(uuid.uuid4())
    cur.execute(
        """
        insert into MIP.LIVE.ENTRY_INTEL_ACTION_LINK (LINK_ID, SNAPSHOT_ID, PROPOSAL_ID, ENTRY_ACTION_ID)
        select %s, %s, %s, %s
        where not exists (
            select 1 from MIP.LIVE.ENTRY_INTEL_ACTION_LINK where ENTRY_ACTION_ID = %s
        )
        """,
        (link_id, sid, proposal_id, entry_action_id, entry_action_id),
    )


def map_exit_type_code(raw: str | None) -> str:
    """Maps LIVE_ACTIONS.EXIT_TYPE / order hints to TRADE_CLOSEOUT.EXIT_TYPE."""
    s = (raw or "").upper()
    if "STOP" in s or s == "SL":
        return "SL"
    if "TAKE" in s or "TP" in s or "PROFIT" in s:
        return "TP"
    if "REVAL" in s:
        return "REVALIDATION"
    if s == "MANUAL":
        return "MANUAL"
    if s == "EARLY":
        return "EARLY"
    return "OTHER"


def _parse_variant(val: Any) -> dict:
    if val is None:
        return {}
    if isinstance(val, dict):
        return val
    if isinstance(val, str):
        try:
            o = json.loads(val)
            return o if isinstance(o, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _aggregate_fills_for_action(cur, action_id: str) -> tuple[float | None, float | None, datetime | None, datetime | None]:
    """
    Broker-weighted aggregates for FILLED/PARTIAL_FILL rows with qty and price.
    Returns (avg_fill_price, total_qty_filled, min_filled_at, max_filled_at).
    """
    cur.execute(
        """
        select ORDER_ID, QTY_FILLED, AVG_FILL_PRICE, FILLED_AT, STATUS
        from MIP.LIVE.LIVE_ORDERS
        where ACTION_ID = %s
          and coalesce(QTY_FILLED, 0) > 0
          and AVG_FILL_PRICE is not null
        order by ORDER_ID
        """,
        (action_id,),
    )
    rows = cur.fetchall() or []
    if not rows:
        return None, None, None, None
    num = 0.0
    den = 0.0
    ts_list: list[datetime] = []
    for _oid, qty, px, filled_at, _st in rows:
        q = float(qty or 0)
        p = float(px or 0)
        if q <= 0:
            continue
        num += p * q
        den += q
        if filled_at is not None:
            ts_list.append(filled_at)
    if den <= 0:
        return None, None, None, None
    avg = num / den
    tmin = min(ts_list) if ts_list else None
    tmax = max(ts_list) if ts_list else None
    return avg, den, tmin, tmax


def _alpha_override_from_committee(cur, committee_run_id: str | None) -> str | None:
    if not committee_run_id:
        return None
    cur.execute(
        """
        select VERDICT_JSON
        from MIP.LIVE.COMMITTEE_VERDICT
        where RUN_ID = %s
        limit 1
        """,
        (str(committee_run_id),),
    )
    row = cur.fetchone()
    if not row or row[0] is None:
        return None
    vj = _parse_variant(row[0])
    ac = vj.get("alpha_override_class")
    return str(ac) if ac is not None else None


def _build_frozen_entry_expectation(
    *,
    alpha: dict,
    eis_source_version: str | None,
    eis_version: int | None,
    alpha_override_class_at_entry: str | None,
) -> dict:
    return {
        "expected_value_net_at_entry": alpha.get("expected_value_net"),
        "confidence_band_at_entry": alpha.get("confidence_band"),
        "downside_risk_band_at_entry": alpha.get("downside_risk_band"),
        "recommended_action_at_entry": alpha.get("recommended_action"),
        "recommended_size_band_at_entry": alpha.get("recommended_size_band"),
        "eis_source_version": eis_source_version,
        "eis_version": eis_version,
        "alpha_override_class_at_entry": alpha_override_class_at_entry,
        "alpha_schema_version_at_entry": alpha.get("alpha_schema_version"),
    }


def build_closeout_api_intel(closeout_row: dict | None) -> dict | None:
    """Parsed VARIANTs + compact summary for closeout GET (single page-open fetch)."""
    if not closeout_row:
        return None
    return {
        "frozen_entry_expectation": _parse_variant(closeout_row.get("FROZEN_ENTRY_EXPECTATION")),
        "alignment": _parse_variant(closeout_row.get("ALIGNMENT_JSON")),
        "summary": build_closeout_summary_for_api(closeout_row),
    }


def build_closeout_summary_for_api(closeout_row: dict | None) -> dict | None:
    """Compact projection for LIC / summary routes (no extra Snowflake calls)."""
    if not closeout_row:
        return None
    frozen = _parse_variant(closeout_row.get("FROZEN_ENTRY_EXPECTATION"))
    align = _parse_variant(closeout_row.get("ALIGNMENT_JSON"))
    return {
        "closeout_id": closeout_row.get("CLOSEOUT_ID"),
        "entry_action_id": closeout_row.get("ENTRY_ACTION_ID"),
        "exit_action_id": closeout_row.get("EXIT_ACTION_ID"),
        "snapshot_id": closeout_row.get("SNAPSHOT_ID"),
        "proposal_id": closeout_row.get("PROPOSAL_ID"),
        "symbol": closeout_row.get("SYMBOL"),
        "exit_type": closeout_row.get("EXIT_TYPE"),
        "entry_ts": str(closeout_row.get("ENTRY_TS")) if closeout_row.get("ENTRY_TS") else None,
        "exit_ts": str(closeout_row.get("EXIT_TS")) if closeout_row.get("EXIT_TS") else None,
        "holding_period_sec": closeout_row.get("HOLDING_PERIOD_SEC"),
        "realized_return_pct": closeout_row.get("REALIZED_RETURN_PCT"),
        "realized_size": closeout_row.get("REALIZED_SIZE"),
        "realized_pnl": closeout_row.get("REALIZED_PNL"),
        "entry_baseline_summary": {
            "recommended_action": frozen.get("recommended_action_at_entry"),
            "expected_value_net": frozen.get("expected_value_net_at_entry"),
            "confidence_band": frozen.get("confidence_band_at_entry"),
            "downside_risk_band": frozen.get("downside_risk_band_at_entry"),
            "size_band": frozen.get("recommended_size_band_at_entry"),
            "eis_source_version": frozen.get("eis_source_version"),
            "eis_version": frozen.get("eis_version"),
            "alpha_override_class_at_entry": frozen.get("alpha_override_class_at_entry"),
        },
        "realized_outcome_summary": {
            "realized_outcome_class": align.get("realized_outcome_class"),
            "realized_return_pct": closeout_row.get("REALIZED_RETURN_PCT"),
            "epsilon_flat_pct": align.get("epsilon_flat_pct"),
        },
        "alignment_summary": {
            "alignment_class": align.get("alignment_class"),
            "comparison_rule_version": align.get("comparison_rule_version"),
            "alignment_reason_codes": align.get("alignment_reason_codes"),
            "summary": align.get("summary"),
        },
    }


def maybe_write_trade_closeout_on_exit_filled(cur, exit_action_id: str) -> None:
    """
    Write TRADE_CLOSEOUT when this EXIT live action's order is fully FILLED.
    Broker truth: LIVE_ORDERS fills for entry and exit actions; EIS frozen from ENTRY_INTEL_SNAPSHOT.
    One row per ENTRY_ACTION_ID (unique). First qualifying exit wins (see ADR partial-fill v1).
    """
    cur.execute(
        """
        select PORTFOLIO_ID, SYMBOL, ACTION_INTENT, EXIT_TYPE, UPDATED_AT
        from MIP.LIVE.LIVE_ACTIONS
        where ACTION_ID = %s
        """,
        (exit_action_id,),
    )
    row = cur.fetchone()
    if not row:
        return
    portfolio_id, symbol, action_intent, exit_type, _exit_updated = row
    if str(action_intent or "").upper() != "EXIT":
        return
    cur.execute(
        """
        select la.ACTION_ID, l.SNAPSHOT_ID, l.PROPOSAL_ID
        from MIP.LIVE.LIVE_ACTIONS la
        left join MIP.LIVE.ENTRY_INTEL_ACTION_LINK l on l.ENTRY_ACTION_ID = la.ACTION_ID
        where la.PORTFOLIO_ID = %s
          and upper(la.SYMBOL) = upper(%s)
          and upper(coalesce(la.ACTION_INTENT, '')) = 'ENTRY'
          and upper(coalesce(la.STATUS, '')) = 'EXECUTED'
          and la.UPDATED_AT <= (select UPDATED_AT from MIP.LIVE.LIVE_ACTIONS where ACTION_ID = %s)
        order by la.UPDATED_AT desc
        limit 1
        """,
        (portfolio_id, symbol, exit_action_id),
    )
    pair = cur.fetchone()
    if not pair:
        return
    entry_action_id, snapshot_id, link_proposal_id = pair[0], pair[1], pair[2]
    cur.execute(
        "select 1 from MIP.LIVE.TRADE_CLOSEOUT where ENTRY_ACTION_ID = %s limit 1",
        (str(entry_action_id),),
    )
    if cur.fetchone():
        return

    cur.execute(
        """
        select PROPOSAL_ID, SIDE, COMMITTEE_RUN_ID
        from MIP.LIVE.LIVE_ACTIONS
        where ACTION_ID = %s
        """,
        (str(entry_action_id),),
    )
    la_entry = cur.fetchone()
    proposal_id = int(link_proposal_id) if link_proposal_id is not None else None
    entry_side = None
    committee_run_id = None
    if la_entry:
        if la_entry[0] is not None:
            proposal_id = int(la_entry[0])
        entry_side = la_entry[1]
        committee_run_id = la_entry[2]

    alpha_override_class = _alpha_override_from_committee(cur, committee_run_id)

    alpha: dict = {}
    eis_source_version = None
    eis_version = None
    if snapshot_id:
        cur.execute(
            """
            select ALPHA_SPEC, SOURCE_VERSION, EIS_VERSION
            from MIP.LIVE.ENTRY_INTEL_SNAPSHOT
            where SNAPSHOT_ID = %s
            limit 1
            """,
            (str(snapshot_id),),
        )
        eis_row = cur.fetchone()
        if eis_row:
            alpha = _parse_variant(eis_row[0])
            eis_source_version = str(eis_row[1]) if eis_row[1] is not None else None
            eis_version = int(eis_row[2]) if eis_row[2] is not None else None

    rec_action = recommended_action_from_alpha_spec(alpha)
    frozen = _build_frozen_entry_expectation(
        alpha=alpha,
        eis_source_version=eis_source_version,
        eis_version=eis_version,
        alpha_override_class_at_entry=alpha_override_class,
    )

    entry_avg, entry_qty, entry_tmin, _entry_tmax = _aggregate_fills_for_action(cur, str(entry_action_id))
    exit_avg, exit_qty, _exit_tmin, exit_tmax = _aggregate_fills_for_action(cur, str(exit_action_id))

    missing_entry = entry_avg is None or entry_qty is None
    missing_exit = exit_avg is None or exit_qty is None
    pos_ret = compute_position_return_pct(entry_avg, exit_avg, entry_side)
    roc, _ = classify_realized_outcome_class(pos_ret)

    exit_code = map_exit_type_code(exit_type)
    alignment = compute_alignment_rule_v1(
        recommended_action=rec_action,
        alpha_override_class=alpha_override_class,
        realized_outcome_class=roc,
        exit_type=exit_code,
        missing_entry_fill=missing_entry,
        missing_exit_fill=missing_exit,
    )

    realized_size = None
    realized_pnl = None
    if entry_qty is not None and exit_qty is not None:
        realized_size = min(float(entry_qty), float(exit_qty))
        if entry_avg is not None and exit_avg is not None and realized_size > 0:
            es = (entry_side or "").upper()
            if es == "SELL":
                realized_pnl = float((entry_avg - exit_avg) * realized_size)
            else:
                realized_pnl = float((exit_avg - entry_avg) * realized_size)

    holding_sec = None
    entry_ts = entry_tmin
    exit_ts = exit_tmax
    if entry_ts is not None and exit_ts is not None:
        holding_sec = int((exit_ts - entry_ts).total_seconds())

    closeout_id = str(uuid.uuid4())
    cur.execute(
        """
        insert into MIP.LIVE.TRADE_CLOSEOUT (
          CLOSEOUT_ID, ENTRY_ACTION_ID, SNAPSHOT_ID, PROPOSAL_ID, SYMBOL, EXIT_ACTION_ID,
          EXIT_TYPE, REALIZED_RETURN_PCT, REALIZED_SIZE, REALIZED_PNL, HOLDING_PERIOD_SEC,
          ENTRY_TS, EXIT_TS, FROZEN_ENTRY_EXPECTATION, ALIGNMENT_JSON
        )
        values (
          %s, %s, %s, %s, %s, %s,
          %s, %s, %s, %s, %s,
          %s, coalesce(%s, current_timestamp()), parse_json(%s), parse_json(%s)
        )
        """,
        (
            closeout_id,
            str(entry_action_id),
            str(snapshot_id) if snapshot_id else None,
            proposal_id,
            str(symbol) if symbol else None,
            str(exit_action_id),
            exit_code,
            float(pos_ret) if pos_ret is not None else None,
            realized_size,
            realized_pnl,
            holding_sec,
            entry_ts,
            exit_ts,
            json.dumps(frozen),
            json.dumps(alignment),
        ),
    )
