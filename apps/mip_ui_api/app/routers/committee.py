"""
Committee 2.0 API — structural hearing room (proposal-scoped).
Separate from MIP.LIVE.COMMITTEE_*.
"""
from __future__ import annotations

import json
import uuid
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Body
from pydantic import BaseModel, Field

from app.db import get_connection, fetch_all, serialize_row
from app.committee.engine import (
    LiveContext,
    compute_hearing_bundle,
    bundle_to_db_json,
)

router = APIRouter(prefix="/committee", tags=["committee"])


def _underlying_sf_conn(conn):
    """PooledConnection wraps the raw Snowflake connection in `_conn`."""
    return getattr(conn, "_conn", conn)


class HearingOpenRequest(BaseModel):
    proposal_id: int = Field(..., ge=1)
    force_rebuild: bool = False


class HearingCommitRequest(BaseModel):
    action_id: Optional[str] = None
    trade_id: Optional[str] = None
    note: Optional[str] = None


def _norm_action_id(action_id: Any) -> Optional[str]:
    if action_id is None:
        return None
    s = str(action_id).strip()
    return s if s else None


def _json_dumps(obj: Any) -> str:
    return json.dumps(obj, default=str)


def _variant(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, (dict, list)):
        return v
    if isinstance(v, str):
        try:
            return json.loads(v)
        except json.JSONDecodeError:
            return v
    return v


def _committee2_enabled(cur) -> bool:
    cur.execute(
        "SELECT CONFIG_VALUE FROM MIP.APP.APP_CONFIG WHERE CONFIG_KEY = %s",
        ("COMMITTEE2_ENABLED",),
    )
    rows = fetch_all(cur)
    if not rows:
        return False
    val = (rows[0].get("CONFIG_VALUE") or "").strip().lower()
    return val in ("1", "true", "yes")


def _require_enabled(conn):
    cur = conn.cursor()
    if not _committee2_enabled(cur):
        raise HTTPException(status_code=503, detail="Committee 2.0 is disabled (COMMITTEE2_ENABLED).")


def _fetch_proposal(cur, proposal_id: int) -> Optional[Dict[str, Any]]:
    cur.execute(
        """
        SELECT PROPOSAL_ID, SYMBOL, DIRECTION, SETUP_FAMILY, STATUS, CREATED_AT,
               COMMITTEE_PAYLOAD, REGIME_COMPAT, STRUCTURE_CONFIDENCE
        FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS
        WHERE PROPOSAL_ID = %s
        """,
        (proposal_id,),
    )
    rows = fetch_all(cur)
    return rows[0] if rows else None


def _fetch_snapshot(cur, proposal_id: int) -> Optional[Dict[str, Any]]:
    cur.execute(
        """
        SELECT *
        FROM MIP.APP.STRUCTURAL_PROPOSAL_SNAPSHOT
        WHERE PROPOSAL_ID = %s
        """,
        (proposal_id,),
    )
    rows = fetch_all(cur)
    return rows[0] if rows else None


def _fetch_hearing_by_proposal(cur, proposal_id: int) -> Optional[Dict[str, Any]]:
    cur.execute(
        "SELECT * FROM MIP.APP.COMMITTEE_HEARING WHERE PROPOSAL_ID = %s",
        (proposal_id,),
    )
    rows = fetch_all(cur)
    return rows[0] if rows else None


def _fetch_hearing_by_id(cur, hearing_id: str) -> Optional[Dict[str, Any]]:
    cur.execute(
        "SELECT * FROM MIP.APP.COMMITTEE_HEARING WHERE HEARING_ID = %s",
        (hearing_id,),
    )
    rows = fetch_all(cur)
    return rows[0] if rows else None


def _live_context(cur, symbol: str, market_type: str = "STOCK") -> LiveContext:
    cur.execute(
        """
        SELECT BAR_DATE, OPEN, CLOSE, HIGH, LOW, STRUCTURAL_STATE, TREND_REGIME, VOL_REGIME
        FROM MIP.MART.V_STRUCTURAL_TIMELINE_PRICE
        WHERE SYMBOL = %s AND MARKET_TYPE = %s
        ORDER BY BAR_DATE DESC
        LIMIT 5
        """,
        (symbol.upper(), market_type.upper()),
    )
    bars = fetch_all(cur)
    if not bars:
        raise HTTPException(
            status_code=400,
            detail={"code": "NO_MARKET_BARS", "message": f"No daily bars for {symbol} ({market_type})."},
        )
    latest = bars[0]
    prior = bars[1] if len(bars) > 1 else None
    price = float(latest["CLOSE"])
    open_p = float(latest["OPEN"]) if latest.get("OPEN") is not None else None
    prior_close = float(prior["CLOSE"]) if prior and prior.get("CLOSE") is not None else None
    dates = [str(b["BAR_DATE"]) for b in bars if b.get("BAR_DATE") is not None]
    return LiveContext(
        latest_price=price,
        open_price=open_p,
        prior_close=prior_close,
        structural_state_now=latest.get("STRUCTURAL_STATE"),
        trend_regime_now=latest.get("TREND_REGIME"),
        vol_regime_now=latest.get("VOL_REGIME"),
        bar_dates=dates,
    )


def _roles_rows(cur, hearing_id: str) -> List[Dict[str, Any]]:
    cur.execute(
        """
        SELECT ROLE_NAME, OUTPUT_JSON, EVIDENCE_REFS
        FROM MIP.APP.COMMITTEE_ROLE_OUTPUT
        WHERE HEARING_ID = %s
        ORDER BY ROLE_NAME
        """,
        (hearing_id,),
    )
    return fetch_all(cur)


def _artifacts_rows(cur, hearing_id: str) -> List[Dict[str, Any]]:
    cur.execute(
        """
        SELECT ARTIFACT_KIND, PAYLOAD_JSON, SCHEMA_VERSION, EVIDENCE_REFS
        FROM MIP.APP.COMMITTEE_EVIDENCE_ARTIFACT
        WHERE HEARING_ID = %s
        ORDER BY ARTIFACT_KIND
        """,
        (hearing_id,),
    )
    return fetch_all(cur)


def _build_snapshot_engine_dict(row: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize DB snapshot row for engine (include PROPOSAL_ID / SNAPSHOT_ID keys)."""
    out = dict(row)
    for k in list(out.keys()):
        if k.endswith("_JSON") or k in ("PROPOSAL_SUMMARY_JSON",):
            out[k] = _variant(out.get(k))
    return out


def _assemble_payload(
    hearing: Dict[str, Any],
    snapshot: Dict[str, Any],
    proposal: Dict[str, Any],
    cur=None,
) -> Dict[str, Any]:
    hid = hearing.get("HEARING_ID")
    snap_eng = _build_snapshot_engine_dict(snapshot)
    roles = []
    arts = []
    if cur is not None:
        role_cur = cur
        for r in _roles_rows(role_cur, hid):
            roles.append(
                {
                    "role_name": r.get("ROLE_NAME"),
                    "output": _variant(r.get("OUTPUT_JSON")),
                    "evidence_refs": _variant(r.get("EVIDENCE_REFS")),
                }
            )
        for a in _artifacts_rows(role_cur, hid):
            arts.append(
                {
                    "artifact_kind": a.get("ARTIFACT_KIND"),
                    "schema_version": a.get("SCHEMA_VERSION"),
                    "payload": _variant(a.get("PAYLOAD_JSON")),
                    "evidence_refs": _variant(a.get("EVIDENCE_REFS")),
                }
            )
    else:
        conn = get_connection()
        try:
            c2 = conn.cursor()
            for r in _roles_rows(c2, hid):
                roles.append(
                    {
                        "role_name": r.get("ROLE_NAME"),
                        "output": _variant(r.get("OUTPUT_JSON")),
                        "evidence_refs": _variant(r.get("EVIDENCE_REFS")),
                    }
                )
            for a in _artifacts_rows(c2, hid):
                arts.append(
                    {
                        "artifact_kind": a.get("ARTIFACT_KIND"),
                        "schema_version": a.get("SCHEMA_VERSION"),
                        "payload": _variant(a.get("PAYLOAD_JSON")),
                        "evidence_refs": _variant(a.get("EVIDENCE_REFS")),
                    }
                )
        finally:
            conn.close()

    evidence = _variant(hearing.get("EVIDENCE_JSON")) or {}
    deltas = _variant(hearing.get("DELTAS_JSON")) or {}
    chair = _variant(hearing.get("CHAIR_OUTPUT_JSON")) or {}
    operational = _variant(hearing.get("OPERATIONAL_JSON")) or {}

    return {
        "ok": True,
        "hearing_id": hid,
        "proposal_id": hearing.get("PROPOSAL_ID"),
        "snapshot_id": hearing.get("SNAPSHOT_ID"),
        "hearing_ts": serialize_row(hearing).get("HEARING_TS"),
        "updated_at": serialize_row(hearing).get("UPDATED_AT"),
        "stance": hearing.get("STANCE"),
        "confidence": hearing.get("CONFIDENCE"),
        "evidence_pack_version": hearing.get("EVIDENCE_PACK_VERSION"),
        "proposal": {
            "proposal_id": proposal.get("PROPOSAL_ID"),
            "symbol": proposal.get("SYMBOL"),
            "direction": proposal.get("DIRECTION"),
            "setup_family": proposal.get("SETUP_FAMILY"),
            "status": proposal.get("STATUS"),
        },
        "snapshot_panel": serialize_row(snapshot),
        "hearing_evidence": evidence,
        "deltas": deltas.get("categories") if isinstance(deltas, dict) else deltas,
        "chair": chair,
        "operational": operational,
        "roles": roles,
        "artifacts": arts,
    }


def _persist_hearing_atomic(
    conn,
    hearing_id: str,
    proposal_id: int,
    snapshot_id: int,
    bundle: Dict[str, Any],
    evidence_pack_version: str,
) -> None:
    evidence_j, deltas_j, chair_j, op_j = bundle_to_db_json(bundle)
    stance = bundle["stance"]
    conf = bundle["confidence"]
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE MIP.APP.COMMITTEE_HEARING
           SET HEARING_TS = CURRENT_TIMESTAMP(),
               SNAPSHOT_ID = %s,
               EVIDENCE_JSON = PARSE_JSON(%s),
               DELTAS_JSON = PARSE_JSON(%s),
               CHAIR_OUTPUT_JSON = PARSE_JSON(%s),
               OPERATIONAL_JSON = PARSE_JSON(%s),
               STANCE = %s,
               CONFIDENCE = %s,
               EVIDENCE_PACK_VERSION = %s,
               UPDATED_AT = CURRENT_TIMESTAMP()
         WHERE HEARING_ID = %s
        """,
        (
            snapshot_id,
            _json_dumps(evidence_j),
            _json_dumps(deltas_j),
            _json_dumps(chair_j),
            _json_dumps(op_j),
            stance,
            conf,
            evidence_pack_version,
            hearing_id,
        ),
    )
    if cur.rowcount == 0:
        # INSERT ... SELECT: Snowflake rejects PARSE_JSON(%s) inside VALUES when binds are inlined.
        cur.execute(
            """
            INSERT INTO MIP.APP.COMMITTEE_HEARING (
                HEARING_ID, PROPOSAL_ID, SNAPSHOT_ID, HEARING_TS,
                EVIDENCE_JSON, DELTAS_JSON, CHAIR_OUTPUT_JSON, OPERATIONAL_JSON,
                STANCE, CONFIDENCE, STATUS, EVIDENCE_PACK_VERSION, UPDATED_AT
            )
            SELECT
                %s, %s, %s, CURRENT_TIMESTAMP(),
                PARSE_JSON(%s), PARSE_JSON(%s), PARSE_JSON(%s), PARSE_JSON(%s),
                %s, %s, 'OPEN', %s, CURRENT_TIMESTAMP()
            """,
            (
                hearing_id,
                proposal_id,
                snapshot_id,
                _json_dumps(evidence_j),
                _json_dumps(deltas_j),
                _json_dumps(chair_j),
                _json_dumps(op_j),
                stance,
                conf,
                evidence_pack_version,
            ),
        )

    cur.execute("DELETE FROM MIP.APP.COMMITTEE_ROLE_OUTPUT WHERE HEARING_ID = %s", (hearing_id,))
    for role in bundle["roles"]:
        cur.execute(
            """
            INSERT INTO MIP.APP.COMMITTEE_ROLE_OUTPUT (HEARING_ID, ROLE_NAME, OUTPUT_JSON, EVIDENCE_REFS, UPDATED_AT)
            SELECT %s, %s, PARSE_JSON(%s), PARSE_JSON(%s), CURRENT_TIMESTAMP()
            """,
            (
                hearing_id,
                role["role_name"],
                _json_dumps(
                    {
                        "stance_badge": role.get("stance_badge"),
                        "one_liner": role.get("one_liner"),
                        "bullets": role.get("bullets"),
                        "influence": role.get("influence"),
                        "output": role.get("output"),
                    }
                ),
                _json_dumps(role.get("evidence_refs") or []),
            ),
        )

    cur.execute("DELETE FROM MIP.APP.COMMITTEE_EVIDENCE_ARTIFACT WHERE HEARING_ID = %s", (hearing_id,))
    for art in bundle["artifacts"]:
        cur.execute(
            """
            INSERT INTO MIP.APP.COMMITTEE_EVIDENCE_ARTIFACT (
                HEARING_ID, ARTIFACT_KIND, PAYLOAD_JSON, SCHEMA_VERSION, EVIDENCE_REFS, UPDATED_AT
            )
            SELECT %s, %s, PARSE_JSON(%s), %s, PARSE_JSON(%s), CURRENT_TIMESTAMP()
            """,
            (
                hearing_id,
                art["artifact_kind"],
                _json_dumps(art.get("payload") or {}),
                art.get("schema_version") or "1",
                _json_dumps(art.get("evidence_refs") or []),
            ),
        )


def _run_refresh(conn, hearing_id: str, proposal_id: int, snapshot: Dict[str, Any], proposal: Dict[str, Any]) -> Dict[str, Any]:
    symbol = proposal.get("SYMBOL") or snapshot.get("SYMBOL")
    cur = conn.cursor()
    live = _live_context(cur, symbol)
    snap_eng = _build_snapshot_engine_dict(snapshot)
    bundle = compute_hearing_bundle(snap_eng, live)
    ver = "1.0.0"
    _persist_hearing_atomic(conn, hearing_id, proposal_id, int(snapshot["SNAPSHOT_ID"]), bundle, ver)
    cur.execute("SELECT * FROM MIP.APP.COMMITTEE_HEARING WHERE HEARING_ID = %s", (hearing_id,))
    hrows = fetch_all(cur)
    hearing = hrows[0]
    return _assemble_payload(hearing, snapshot, proposal, cur=cur)


def committee_final_decision_commit_for_action(cur, hearing_id: str, req: HearingCommitRequest) -> Dict[str, Any]:
    """
    Insert or reconcile COMMITTEE_FINAL_DECISION for a hearing with ACTION_ID-aware rules:
    same action_id -> idempotent; null ACTION_ID + request action_id -> UPDATE; conflicting ACTION_ID -> 409.
    """
    cur.execute(
        "SELECT * FROM MIP.APP.COMMITTEE_FINAL_DECISION WHERE HEARING_ID = %s",
        (hearing_id,),
    )
    existing_fd = fetch_all(cur)
    req_aid = _norm_action_id(req.action_id)

    if existing_fd:
        row_raw = existing_fd[0]
        ex_aid = _norm_action_id(row_raw.get("ACTION_ID"))
        if ex_aid is not None and req_aid is None:
            raise HTTPException(
                status_code=400,
                detail={
                    "message": "This hearing is already bound to an action; pass action_id to confirm.",
                    "reason_codes": ["COMMITTEE2_ACTION_ID_REQUIRED"],
                },
            )
        if ex_aid is not None and req_aid is not None and ex_aid != req_aid:
            raise HTTPException(
                status_code=409,
                detail={
                    "message": "Final decision for this hearing is bound to a different LIVE_ACTIONS row.",
                    "reason_codes": ["COMMITTEE2_HEARING_BOUND_TO_OTHER_ACTION"],
                    "bound_action_id": ex_aid,
                },
            )
        if ex_aid is None and req_aid is not None:
            cur.execute(
                """
                UPDATE MIP.APP.COMMITTEE_FINAL_DECISION
                   SET ACTION_ID = %s,
                       TRADE_ID = COALESCE(%s, TRADE_ID),
                       COMMIT_NOTE = COALESCE(%s, COMMIT_NOTE)
                 WHERE HEARING_ID = %s
                """,
                (req.action_id, req.trade_id, req.note, hearing_id),
            )
            cur.execute(
                "SELECT * FROM MIP.APP.COMMITTEE_FINAL_DECISION WHERE HEARING_ID = %s",
                (hearing_id,),
            )
            row = serialize_row(fetch_all(cur)[0])
            row["already_committed"] = True
            row["action_id_bound_updated"] = True
            row["ok"] = True
            return row
        row = serialize_row(row_raw)
        row["already_committed"] = True
        row["ok"] = True
        return row

    hearing = _fetch_hearing_by_id(cur, hearing_id)
    if not hearing:
        raise HTTPException(status_code=404, detail="Hearing not found.")
    proposal_id = int(hearing["PROPOSAL_ID"])
    snapshot = _fetch_snapshot(cur, proposal_id)
    if not snapshot:
        raise HTTPException(status_code=400, detail="Snapshot missing.")

    evidence = _variant(hearing.get("EVIDENCE_JSON")) or {}
    deltas = _variant(hearing.get("DELTAS_JSON")) or {}
    chair = _variant(hearing.get("CHAIR_OUTPUT_JSON")) or {}
    operational = _variant(hearing.get("OPERATIONAL_JSON")) or {}
    stance = hearing.get("STANCE")
    conf = hearing.get("CONFIDENCE")

    roles_out: Dict[str, Any] = {}
    for r in _roles_rows(cur, hearing_id):
        roles_out[r["ROLE_NAME"]] = _variant(r.get("OUTPUT_JSON"))
    arts: Dict[str, Any] = {}
    for a in _artifacts_rows(cur, hearing_id):
        arts[a["ARTIFACT_KIND"]] = {
            "payload": _variant(a.get("PAYLOAD_JSON")),
            "schema_version": a.get("SCHEMA_VERSION"),
            "evidence_refs": _variant(a.get("EVIDENCE_REFS")),
        }

    posture = (operational or {}).get("posture") or {}
    evidence_refs = (chair or {}).get("evidence_refs") or []

    try:
        cur.execute(
            """
            INSERT INTO MIP.APP.COMMITTEE_FINAL_DECISION (
                HEARING_ID, PROPOSAL_ID, SNAPSHOT_ID, STANCE, CONFIDENCE,
                CHAIR_OUTPUT_JSON, ROLE_OUTPUTS_JSON, DELTA_SUMMARY_JSON, POSTURE_JSON,
                EVIDENCE_REFS_JSON, ARTIFACTS_JSON, ACTION_ID, TRADE_ID, COMMIT_NOTE, DECISION_TS
            )
            SELECT
                %s, %s, %s, %s, %s,
                PARSE_JSON(%s), PARSE_JSON(%s), PARSE_JSON(%s), PARSE_JSON(%s),
                PARSE_JSON(%s), PARSE_JSON(%s), %s, %s, %s, CURRENT_TIMESTAMP()
            """,
            (
                hearing_id,
                proposal_id,
                int(hearing["SNAPSHOT_ID"]),
                stance,
                conf,
                _json_dumps(chair),
                _json_dumps(roles_out),
                _json_dumps(deltas),
                _json_dumps(posture),
                _json_dumps(evidence_refs),
                _json_dumps(arts),
                req.action_id,
                req.trade_id,
                req.note,
            ),
        )
    except Exception as exc:
        err = str(exc).lower()
        if "unique" in err or "already exists" in err:
            cur.execute(
                "SELECT * FROM MIP.APP.COMMITTEE_FINAL_DECISION WHERE HEARING_ID = %s",
                (hearing_id,),
            )
            rows = fetch_all(cur)
            if rows:
                return committee_final_decision_commit_for_action(cur, hearing_id, req)
        raise
    cur.execute(
        "SELECT * FROM MIP.APP.COMMITTEE_FINAL_DECISION WHERE HEARING_ID = %s",
        (hearing_id,),
    )
    row = serialize_row(fetch_all(cur)[0])
    row["already_committed"] = False
    row["ok"] = True
    return row


@router.post("/hearing/open")
def committee_hearing_open(req: HearingOpenRequest = Body(...)):
    conn = get_connection()
    try:
        _require_enabled(conn)
        cur = conn.cursor()
        proposal = _fetch_proposal(cur, req.proposal_id)
        if not proposal:
            raise HTTPException(status_code=404, detail="Proposal not found.")
        snapshot = _fetch_snapshot(cur, req.proposal_id)
        if not snapshot:
            raise HTTPException(
                status_code=400,
                detail={"code": "NO_SNAPSHOT", "message": "Immutable proposal snapshot missing; re-run structural propose or backfill snapshots."},
            )

        existing = _fetch_hearing_by_proposal(cur, req.proposal_id)
        if existing and not req.force_rebuild:
            hearing = existing
            return _assemble_payload(hearing, snapshot, proposal)

        hearing_id = existing["HEARING_ID"] if existing else str(uuid.uuid4())

        raw = conn._conn  # underlying snowflake connection for transaction
        raw.autocommit(False)
        try:
            payload = _run_refresh(conn, hearing_id, req.proposal_id, snapshot, proposal)
            raw.commit()
        except Exception:
            raw.rollback()
            raise
        finally:
            raw.autocommit(True)

        payload["rebuilt"] = bool(req.force_rebuild or not existing)
        return payload
    finally:
        conn.close()


@router.get("/hearing/{hearing_id}")
def committee_hearing_get(hearing_id: str):
    conn = get_connection()
    try:
        _require_enabled(conn)
        cur = conn.cursor()
        hearing = _fetch_hearing_by_id(cur, hearing_id)
        if not hearing:
            raise HTTPException(status_code=404, detail="Hearing not found.")
        proposal = _fetch_proposal(cur, int(hearing["PROPOSAL_ID"]))
        snapshot = _fetch_snapshot(cur, int(hearing["PROPOSAL_ID"]))
        if not proposal or not snapshot:
            raise HTTPException(status_code=404, detail="Proposal or snapshot missing for hearing.")
        return _assemble_payload(hearing, snapshot, proposal)
    finally:
        conn.close()


@router.post("/hearing/{hearing_id}/refresh")
def committee_hearing_refresh(hearing_id: str):
    conn = get_connection()
    try:
        _require_enabled(conn)
        cur = conn.cursor()
        hearing = _fetch_hearing_by_id(cur, hearing_id)
        if not hearing:
            raise HTTPException(status_code=404, detail="Hearing not found.")
        proposal_id = int(hearing["PROPOSAL_ID"])
        proposal = _fetch_proposal(cur, proposal_id)
        snapshot = _fetch_snapshot(cur, proposal_id)
        if not proposal or not snapshot:
            raise HTTPException(status_code=404, detail="Proposal or snapshot missing.")

        raw = _underlying_sf_conn(conn)
        raw.autocommit(False)
        try:
            payload = _run_refresh(conn, hearing_id, proposal_id, snapshot, proposal)
            raw.commit()
        except Exception:
            raw.rollback()
            raise
        finally:
            raw.autocommit(True)
        payload["refreshed"] = True
        return payload
    finally:
        conn.close()


@router.post("/hearing/{hearing_id}/commit")
def committee_hearing_commit(hearing_id: str, req: HearingCommitRequest = Body(default_factory=HearingCommitRequest)):
    conn = get_connection()
    try:
        _require_enabled(conn)
        cur = conn.cursor()
        return committee_final_decision_commit_for_action(cur, hearing_id, req)
    finally:
        conn.close()


@router.get("/proposal/{proposal_id}/final-decision")
def committee_proposal_final_decision(proposal_id: int):
    conn = get_connection()
    try:
        _require_enabled(conn)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT * FROM MIP.APP.COMMITTEE_FINAL_DECISION
            WHERE PROPOSAL_ID = %s
            ORDER BY DECISION_TS DESC
            LIMIT 1
            """,
            (proposal_id,),
        )
        rows = fetch_all(cur)
        if not rows:
            return {"ok": True, "final_decision": None}
        return {"ok": True, "final_decision": serialize_row(rows[0])}
    finally:
        conn.close()
