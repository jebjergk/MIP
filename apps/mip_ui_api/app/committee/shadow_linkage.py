"""
Phase 1 dual-hearing — durable executed-vs-shadow linkage.

Called from the REAL execute path (`execute_live_action`) the moment IBKR
returns a pending-order ack. Writes one SHADOW_TRADE_LINKAGE row binding:

  - the real action / committee run / broker order ids
  - the deterministic real-board trade configuration
  - the latest completed shadow session for the same EVIDENCE_PACK_HASH
  - the symbolic shadow trade configuration (or NULL with a note)

Shadow code never writes to this table. Failures here are logged but never
block the real order — linkage is observability, not authority.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


def _jdump(obj: Any) -> str:
    return json.dumps(obj, default=str)


def _normalize_broker_order_ids(broker_payload: Any) -> list[str]:
    """Flatten a broker-submit payload into a sorted list of order ids."""
    if not broker_payload:
        return []
    ids: set[str] = set()

    def walk(node: Any) -> None:
        if node is None:
            return
        if isinstance(node, dict):
            for k, v in node.items():
                k_low = str(k).lower()
                if k_low in ("order_id", "perm_id", "ib_order_id", "broker_order_id") and v not in (None, ""):
                    ids.add(str(v))
                walk(v)
        elif isinstance(node, (list, tuple)):
            for v in node:
                walk(v)

    walk(broker_payload)
    return sorted(ids)


def write_shadow_trade_linkage(
    cur,
    action_row: Dict[str, Any],
    broker_order_payload: Optional[Dict[str, Any]] = None,
    real_trade_config: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    """
    Insert one SHADOW_TRADE_LINKAGE row for this execute event.

    Returns the LINK_ID of the inserted row (as string), or None on no-op.
    Idempotent: if a row already exists for this REAL_ACTION_ID, no-op.
    Never raises — all errors are logged and swallowed.
    """
    try:
        action_id = str(action_row.get("ACTION_ID") or "").strip()
        if not action_id:
            logger.warning("shadow_linkage: missing ACTION_ID; skipping")
            return None

        # Idempotent guard: at most one linkage per REAL_ACTION_ID.
        cur.execute(
            "SELECT LINK_ID FROM MIP.APP.SHADOW_TRADE_LINKAGE WHERE REAL_ACTION_ID = %s LIMIT 1",
            (action_id,),
        )
        existing = cur.fetchall()
        if existing:
            logger.info("shadow_linkage: already linked for action_id=%s", action_id)
            return None

        # Resolve hearing identity from COMMITTEE_FINAL_DECISION (bound to action).
        cur.execute(
            """
            SELECT fd.HEARING_ID, fd.PROPOSAL_ID,
                   fd.STANCE, fd.CONFIDENCE,
                   fd.POSTURE_JSON, fd.CHAIR_OUTPUT_JSON,
                   h.SNAPSHOT_ID, h.EVIDENCE_PACK_HASH
              FROM MIP.APP.COMMITTEE_FINAL_DECISION fd
              JOIN MIP.APP.COMMITTEE_HEARING h ON h.HEARING_ID = fd.HEARING_ID
             WHERE fd.ACTION_ID = %s
             LIMIT 1
            """,
            (action_id,),
        )
        rows = cur.fetchall()
        if not rows:
            logger.info(
                "shadow_linkage: no COMMITTEE_FINAL_DECISION found for action_id=%s; skipping",
                action_id,
            )
            return None

        # Snowflake connector returns tuples here (DictCursor not in use); fetch
        # column metadata once.
        cols = [d[0].upper() for d in cur.description]
        row = dict(zip(cols, rows[0]))

        hearing_id = row.get("HEARING_ID")
        proposal_id = row.get("PROPOSAL_ID")
        snapshot_id = row.get("SNAPSHOT_ID")
        evidence_pack_hash = row.get("EVIDENCE_PACK_HASH")

        # Real-board trade config: prefer caller-supplied (richer) over POSTURE_JSON.
        real_cfg = real_trade_config
        if real_cfg is None:
            posture = row.get("POSTURE_JSON")
            if isinstance(posture, str):
                try:
                    posture = json.loads(posture)
                except Exception:
                    posture = {"raw": posture}
            chair = row.get("CHAIR_OUTPUT_JSON")
            if isinstance(chair, str):
                try:
                    chair = json.loads(chair)
                except Exception:
                    chair = {}
            real_cfg = {
                "stance": row.get("STANCE"),
                "confidence": row.get("CONFIDENCE"),
                "posture": posture or {},
                "execution_shaping": (chair or {}).get("execution_shaping") or {},
            }

        # Most recent COMPLETE / DEGRADED shadow session for this snapshot identity.
        shadow_session_id = None
        shadow_stance = None
        shadow_confidence = None
        shadow_trade_cfg: Any = None
        shadow_completed_at = None
        shadow_status = None
        shadow_notes = None

        if evidence_pack_hash:
            cur.execute(
                """
                SELECT s.SESSION_ID, s.STATUS, s.SHADOW_STANCE, s.SHADOW_CONFIDENCE,
                       s.COMPLETED_AT, c.SHADOW_TRADE_JSON
                  FROM MIP.APP.SHADOW_BOARD_SESSION s
                  LEFT JOIN MIP.APP.SHADOW_CHAIR_RULING c ON c.SESSION_ID = s.SESSION_ID
                 WHERE s.HEARING_ID = %s
                   AND s.EVIDENCE_PACK_HASH = %s
                   AND s.STATUS IN ('COMPLETE', 'DEGRADED')
                 ORDER BY s.CREATED_AT DESC
                 LIMIT 1
                """,
                (hearing_id, evidence_pack_hash),
            )
            srows = cur.fetchall()
            if srows:
                scols = [d[0].upper() for d in cur.description]
                srow = dict(zip(scols, srows[0]))
                shadow_session_id = srow.get("SESSION_ID")
                shadow_status = srow.get("STATUS")
                shadow_stance = srow.get("SHADOW_STANCE")
                shadow_confidence = srow.get("SHADOW_CONFIDENCE")
                shadow_completed_at = srow.get("COMPLETED_AT")
                shadow_trade_cfg = srow.get("SHADOW_TRADE_JSON")
                if isinstance(shadow_trade_cfg, str):
                    try:
                        shadow_trade_cfg = json.loads(shadow_trade_cfg)
                    except Exception:
                        pass
            else:
                # Either still RUNNING or never started — capture the gap explicitly.
                cur.execute(
                    """
                    SELECT SESSION_ID, STATUS
                      FROM MIP.APP.SHADOW_BOARD_SESSION
                     WHERE HEARING_ID = %s
                       AND EVIDENCE_PACK_HASH = %s
                     ORDER BY CREATED_AT DESC
                     LIMIT 1
                    """,
                    (hearing_id, evidence_pack_hash),
                )
                rrows = cur.fetchall()
                if rrows:
                    rcols = [d[0].upper() for d in cur.description]
                    rrow = dict(zip(rcols, rrows[0]))
                    shadow_session_id = rrow.get("SESSION_ID")
                    shadow_status = rrow.get("STATUS")
                    shadow_notes = "Shadow board did not reach COMPLETE before order submit."
                else:
                    shadow_notes = "No shadow session existed for this hearing snapshot."
        else:
            shadow_notes = "EVIDENCE_PACK_HASH missing on COMMITTEE_HEARING; cannot link."

        broker_ids = _normalize_broker_order_ids(broker_order_payload or {})
        committee_run_id = action_row.get("COMMITTEE_RUN_ID")
        if committee_run_id is not None:
            committee_run_id = str(committee_run_id)

        cur.execute(
            """
            INSERT INTO MIP.APP.SHADOW_TRADE_LINKAGE (
                HEARING_ID, SNAPSHOT_ID, EVIDENCE_PACK_HASH, PROPOSAL_ID,
                REAL_ACTION_ID, REAL_COMMITTEE_RUN_ID,
                BROKER_ORDER_IDS, REAL_TRADE_CONFIG_JSON,
                SHADOW_SESSION_ID, SHADOW_STANCE, SHADOW_CONFIDENCE,
                SHADOW_TRADE_CONFIG_JSON, SHADOW_STATUS, SHADOW_NOTES,
                REAL_EXECUTION_TS, SHADOW_COMPLETED_AT, CREATED_AT
            )
            SELECT
                %s, %s, %s, %s,
                %s, %s,
                PARSE_JSON(%s), PARSE_JSON(%s),
                %s, %s, %s,
                PARSE_JSON(%s), %s, %s,
                CURRENT_TIMESTAMP(), %s, CURRENT_TIMESTAMP()
            """,
            (
                hearing_id, snapshot_id, evidence_pack_hash, proposal_id,
                action_id, committee_run_id,
                _jdump(broker_ids), _jdump(real_cfg or {}),
                shadow_session_id, shadow_stance, shadow_confidence,
                _jdump(shadow_trade_cfg or {}), shadow_status, (shadow_notes or "")[:500],
                shadow_completed_at,
            ),
        )
        logger.info(
            "shadow_linkage: wrote linkage action_id=%s shadow_session=%s status=%s",
            action_id, shadow_session_id, shadow_status,
        )
        return action_id  # caller can correlate by action_id; LINK_ID is auto.
    except Exception as exc:
        # Never block the real execute path on linkage failure.
        logger.error("shadow_linkage: write FAILED action_id=%s: %s", action_row.get("ACTION_ID"), exc, exc_info=True)
        return None
