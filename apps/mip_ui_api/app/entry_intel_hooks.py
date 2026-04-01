"""Phase 1 Entry Intelligence Snapshot (EIS) lifecycle hooks — Snowflake only."""
from __future__ import annotations

import json
import uuid


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
    cur.execute("call MIP.APP.SP_ENSURE_ENTRY_INTEL_FOR_PROPOSAL(%s)", (proposal_id,))


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
    s = (raw or "").upper()
    if "STOP" in s or s == "SL":
        return "SL"
    if "TAKE" in s or "TP" in s or "PROFIT" in s:
        return "TP"
    if "REVAL" in s:
        return "EARLY"
    if s == "MANUAL":
        return "MANUAL"
    return "OTHER"


def maybe_write_trade_closeout_on_exit_filled(cur, exit_action_id: str) -> None:
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
        select la.ACTION_ID, l.SNAPSHOT_ID
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
    entry_action_id, snapshot_id = pair[0], pair[1]
    cur.execute(
        "select 1 from MIP.LIVE.TRADE_CLOSEOUT where ENTRY_ACTION_ID = %s limit 1",
        (str(entry_action_id),),
    )
    if cur.fetchone():
        return
    closeout_id = str(uuid.uuid4())
    exit_code = map_exit_type_code(exit_type)
    alignment = {
        "alignment_class": "NEUTRAL",
        "expected_band": "UNKNOWN",
        "recommended_action": "UNKNOWN",
        "comparison_rule_version": "ALIGN_V1",
    }
    cur.execute(
        """
        insert into MIP.LIVE.TRADE_CLOSEOUT (
          CLOSEOUT_ID, ENTRY_ACTION_ID, SNAPSHOT_ID, EXIT_TYPE,
          REALIZED_RETURN_PCT, HOLDING_PERIOD_SEC, EXIT_TS, ALIGNMENT_JSON
        )
        values (%s, %s, %s, %s, %s, %s, current_timestamp(), parse_json(%s))
        """,
        (
            closeout_id,
            str(entry_action_id),
            str(snapshot_id) if snapshot_id else None,
            exit_code,
            None,
            None,
            json.dumps(alignment),
        ),
    )
