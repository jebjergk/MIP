"""
Shadow Board Phase 1 — runtime orchestrator.

Entry point: orchestrate_shadow_board(hearing_id, conn_factory)

Stages:
  0  Build ShadowEvidencePack + stage into SHADOW_EVIDENCE_PACK_CACHE
  1  Run 6 specialist agents in parallel (CREATE AGENT objects)
  2  Detect conflicts (Python-side)
  3  Challenge turn (objectless AGENT_RUN, one round, no recursion)
  4  Revision turn  (objectless AGENT_RUN, one round, no recursion)
  5  Chair agent    (CREATE AGENT object)
  6  Persist all results; expire cache entry

Guarantees:
  - COMMITTEE_FINAL_DECISION is never touched
  - Real board stance / confidence / chair are never read or written
  - All shadow tables get a DEGRADED=TRUE row on any stage failure
  - Instrumentation: logs stage entry/exit + elapsed_ms for every agent call
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

from app.config import get_snowflake_config
from app.db import fetch_all, get_connection

from .shadow_types import (
    ChallengeTurn,
    ConflictEntry,
    DegradedPosition,
    RevisionTurn,
    ShadowBoardResult,
    ShadowChairRuling,
    ShadowEvidencePack,
    SpecialistPosition,
    build_shadow_evidence_pack,
    detect_conflicts,
    parse_chair_ruling,
    parse_specialist_position,
    pick_primary_conflict,
)
from .shadow_cortex_client import (
    extract_agent_text,
    run_agent_object,
    run_agent_objectless,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Agent object names (must match CREATE AGENT DDL in 542_shadow_board_agents.sql)
# ---------------------------------------------------------------------------
_SPECIALIST_AGENTS = {
    "STRUCTURAL_THESIS": "SHADOW_STRUCTURAL_THESIS_AGENT",
    "ENTRY_GEOMETRY":    "SHADOW_ENTRY_GEOMETRY_AGENT",
    "REGIME":            "SHADOW_REGIME_AGENT",
    "PATH_TRADEABILITY": "SHADOW_PATH_TRADEABILITY_AGENT",
    "PROTECTION_EXIT":   "SHADOW_PROTECTION_EXIT_AGENT",
    "SYMBOL_BEHAVIOR":   "SHADOW_SYMBOL_BEHAVIOR_AGENT",
}
_CHAIR_AGENT = "SHADOW_CHAIR_AGENT"

_OBJECTLESS_MODEL = "claude-haiku-4-5"
_AGENT_MODEL = "claude-haiku-4-5"  # Baked into all SHADOW_*_AGENT specs; mirrored here so SHADOW_BOARD_SESSION.AGENT_MODEL is recorded explicitly instead of relying on the (stale) column default.
_CACHE_TTL_HOURS = 24


# ---------------------------------------------------------------------------
# Helpers: Snowflake JSON persistence (sync, run via asyncio.to_thread)
# ---------------------------------------------------------------------------

def _jdump(obj: Any) -> str:
    return json.dumps(obj, default=str)


def _get_snowflake_creds() -> Tuple[str, str, str]:
    cfg = get_snowflake_config()
    return (
        cfg.get("account") or "",
        cfg.get("user") or "",
        cfg.get("private_key_path") or "",
    )


# ---------------------------------------------------------------------------
# Phase 1 dual-hearing: snapshot identity (hash) + idempotent kickoff
#
# The hash is a deterministic fingerprint of the shared frozen evidence both
# boards reason from. Inputs:
#   - snapshot_id (frozen at proposal time)
#   - the proposal-snapshot row's stable fields
#   - the live proposal row's stable fields (symbol, direction, setup family)
# Volatile timestamps are excluded so the hash is stable across orchestrate
# replays for the same underlying snapshot.
# ---------------------------------------------------------------------------

# Volatile / per-call fields that must not contribute to the snapshot identity.
_HASH_EXCLUDE_KEYS = {
    "CREATED_AT", "UPDATED_AT", "HEARING_TS", "PROPOSAL_TS",
    "DECISION_TS", "EXPIRES_AT", "COMPLETED_AT",
}


def _canonical_for_hash(value: Any) -> Any:
    """Recursively normalize a value into something deterministic & JSON-safe."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, dict):
        out: Dict[str, Any] = {}
        for k in sorted(value.keys()):
            ku = str(k).upper()
            if ku in _HASH_EXCLUDE_KEYS:
                continue
            v = value[k]
            try:
                out[ku] = _canonical_for_hash(v)
            except Exception:
                out[ku] = repr(v)
        return out
    if isinstance(value, (list, tuple)):
        return [_canonical_for_hash(v) for v in value]
    # Fall back to a stable string form (datetimes, Decimals, etc.).
    try:
        return json.loads(json.dumps(value, default=str))
    except Exception:
        return str(value)


def compute_evidence_pack_hash(
    snapshot_id: int,
    snapshot_row: Dict[str, Any],
    proposal_row: Dict[str, Any],
    hearing_row: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Deterministic SHA-256 over the frozen-snapshot identity inputs PLUS the
    current hearing's live-price fingerprint.

    Rationale: the snapshot is immutable but the committee hearing is re-run
    as intraday price ticks land. Without mixing the live-price fingerprint
    into the hash, every re-run on the same snapshot produced the same hash
    and `_find_existing_shadow_session_sync` returned the first shadow
    session forever — the shadow board would stay stuck on the original
    (possibly stale) price while the real board had already moved on.

    Live-price fingerprint inputs (all optional, all coerced to strings):
      - EVIDENCE_JSON.latest_price
      - EVIDENCE_JSON.latest_price_ts_utc
      - EVIDENCE_JSON.latest_price_source

    When `hearing_row` is None the hash degrades to the legacy snapshot-only
    fingerprint to preserve backwards compatibility with callers that do
    not have a hearing yet.
    """
    ev: Dict[str, Any] = {}
    if isinstance(hearing_row, dict):
        raw_ev = hearing_row.get("EVIDENCE_JSON") or hearing_row.get("evidence_json")
        if isinstance(raw_ev, (bytes, bytearray)):
            try:
                raw_ev = raw_ev.decode("utf-8")
            except Exception:
                raw_ev = None
        if isinstance(raw_ev, str):
            try:
                ev = json.loads(raw_ev) or {}
            except Exception:
                ev = {}
        elif isinstance(raw_ev, dict):
            ev = raw_ev

    live_price_fp = {
        "latest_price": _canonical_for_hash(ev.get("latest_price")),
        "latest_price_ts_utc": _canonical_for_hash(ev.get("latest_price_ts_utc")),
        "latest_price_source": _canonical_for_hash(ev.get("latest_price_source")),
    }

    payload = {
        "snapshot_id": int(snapshot_id),
        "snapshot": _canonical_for_hash(snapshot_row or {}),
        "proposal": {
            k: _canonical_for_hash((proposal_row or {}).get(k))
            for k in ("PROPOSAL_ID", "SYMBOL", "DIRECTION", "SETUP_FAMILY")
        },
        "live_price_fp": live_price_fp,
    }
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _find_existing_shadow_session_sync(
    hearing_id: str,
    evidence_pack_hash: str,
) -> Optional[Dict[str, Any]]:
    """
    Return the most recent non-FAILED shadow session matching the hash.
    Used for idempotent kickoff: if a RUNNING / COMPLETE / DEGRADED session
    already exists for this snapshot identity, do not start another.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT SESSION_ID, STATUS, STAGE_REACHED, SHADOW_STANCE, SHADOW_CONFIDENCE,
                   DEGRADED, DEGRADED_REASON, RUN_MS, CREATED_AT, COMPLETED_AT
              FROM MIP.APP.SHADOW_BOARD_SESSION
             WHERE HEARING_ID = %s
               AND EVIDENCE_PACK_HASH = %s
               AND STATUS IN ('RUNNING', 'COMPLETE', 'DEGRADED')
             ORDER BY CREATED_AT DESC
             LIMIT 1
            """,
            (hearing_id, evidence_pack_hash),
        )
        rows = fetch_all(cur)
        return rows[0] if rows else None
    finally:
        conn.close()


def _insert_running_placeholder_sync(
    session_id: str,
    hearing_id: str,
    proposal_id: int,
    snapshot_id: Optional[int],
    evidence_pack_hash: str,
) -> bool:
    """
    Race-safe placeholder insert: writes a RUNNING SHADOW_BOARD_SESSION row
    only if no non-FAILED session already exists for (hearing, hash).

    Returns True if our row was inserted (we own this session); False if
    another caller raced ahead and we should reuse.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO MIP.APP.SHADOW_BOARD_SESSION
                (SESSION_ID, HEARING_ID, PROPOSAL_ID,
                 SNAPSHOT_ID, EVIDENCE_PACK_HASH,
                 STAGE_REACHED, STATUS, DEGRADED,
                 AGENT_MODEL, PACK_VERSION, CREATED_AT)
            SELECT %s, %s, %s, %s, %s, 0, 'RUNNING', FALSE,
                   %s, '2.0.0', CURRENT_TIMESTAMP()
            WHERE NOT EXISTS (
                SELECT 1 FROM MIP.APP.SHADOW_BOARD_SESSION
                 WHERE HEARING_ID = %s
                   AND EVIDENCE_PACK_HASH = %s
                   AND STATUS IN ('RUNNING', 'COMPLETE', 'DEGRADED')
            )
            """,
            (
                session_id, hearing_id, proposal_id,
                snapshot_id, evidence_pack_hash,
                _AGENT_MODEL,
                hearing_id, evidence_pack_hash,
            ),
        )
        return int(getattr(cur, "rowcount", 0) or 0) > 0
    finally:
        conn.close()


async def kickoff_shadow_board_for_snapshot(
    hearing_id: str,
    proposal_id: int,
    snapshot_id: int,
    evidence_pack_hash: str,
    timeout_sec: float = 120.0,
    force: bool = False,
    action_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Phase 1 dual-hearing kickoff.

    - If a non-FAILED session already exists for (hearing_id, evidence_pack_hash)
      and force is False, return that session (reused=True).
    - Otherwise: insert a RUNNING placeholder row SYNCHRONOUSLY (so the LPA
      poll has something to render within the same request), then schedule
      the full orchestration as a background asyncio task.

    Returns: {session_id, status, reused, evidence_pack_hash}
    """
    if not force:
        existing = await asyncio.to_thread(
            _find_existing_shadow_session_sync, hearing_id, evidence_pack_hash
        )
        if existing:
            logger.info(
                "shadow_kickoff: reusing existing session %s status=%s for hash=%s...",
                existing.get("SESSION_ID"), existing.get("STATUS"), evidence_pack_hash[:12],
            )
            return {
                "session_id": existing.get("SESSION_ID"),
                "status": existing.get("STATUS"),
                "reused": True,
                "evidence_pack_hash": evidence_pack_hash,
            }

    session_id = str(uuid.uuid4())

    # Guardrail: placeholder MUST land before the task is scheduled.
    inserted = await asyncio.to_thread(
        _insert_running_placeholder_sync,
        session_id, hearing_id, proposal_id, snapshot_id, evidence_pack_hash,
    )
    if not inserted:
        # Lost a race; reuse whichever placeholder won.
        existing = await asyncio.to_thread(
            _find_existing_shadow_session_sync, hearing_id, evidence_pack_hash
        )
        if existing:
            return {
                "session_id": existing.get("SESSION_ID"),
                "status": existing.get("STATUS"),
                "reused": True,
                "evidence_pack_hash": evidence_pack_hash,
            }
        # Pathological: insert refused but no row visible. Fall through to
        # schedule under our session_id and let orchestrate_shadow_board
        # surface any failure during its own persistence.
        logger.warning(
            "shadow_kickoff: placeholder insert refused but no existing row found "
            "for hearing=%s hash=%s; proceeding under session %s",
            hearing_id, evidence_pack_hash[:12], session_id,
        )

    # Schedule the full orchestration; the running task carries the snapshot
    # identity so persistence can write SNAPSHOT_ID/EVIDENCE_PACK_HASH.
    asyncio.create_task(
        _run_shadow_in_background(
            session_id=session_id,
            hearing_id=hearing_id,
            snapshot_id=snapshot_id,
            evidence_pack_hash=evidence_pack_hash,
            timeout_sec=timeout_sec,
            action_id=action_id,
        )
    )
    logger.info(
        "shadow_kickoff: scheduled session %s for hearing=%s hash=%s...",
        session_id, hearing_id, evidence_pack_hash[:12],
    )
    return {
        "session_id": session_id,
        "status": "RUNNING",
        "reused": False,
        "evidence_pack_hash": evidence_pack_hash,
    }


async def _run_shadow_in_background(
    session_id: str,
    hearing_id: str,
    snapshot_id: Optional[int],
    evidence_pack_hash: Optional[str],
    timeout_sec: float,
    action_id: Optional[str] = None,
) -> None:
    """Background wrapper. Never raises into the event loop."""
    try:
        await orchestrate_shadow_board(
            hearing_id=hearing_id,
            timeout_sec=timeout_sec,
            session_id=session_id,
            snapshot_id=snapshot_id,
            evidence_pack_hash=evidence_pack_hash,
            action_id=action_id,
        )
    except Exception as exc:  # defensive — orchestrate_shadow_board already swallows
        logger.error(
            "shadow_kickoff: background run failed session=%s hearing=%s: %s",
            session_id, hearing_id, exc, exc_info=True,
        )


# ---------------------------------------------------------------------------
# Stage 0: Build + stage evidence pack
# ---------------------------------------------------------------------------

def _fetch_hearing_data(hearing_id: str) -> Tuple[
    Dict[str, Any], Dict[str, Any], Dict[str, Any],
    List[Dict[str, Any]], List[Dict[str, Any]],
    Dict[str, Any], Dict[str, Any],
]:
    """Fetch all required rows for building ShadowEvidencePack.

    Returns:
        hearing, snapshot, proposal, roles, artifacts,
        phase4_thesis (may be empty dict if no Phase 4 lineage),
        phase4_dossier (may be empty dict if no Phase 4 lineage)

    Phase 4 fields are NULL-safe: proposals without BOARD_DOSSIER_ID or
    BOARD_RUN_ID yield empty dicts. build_shadow_evidence_pack sets
    phase4_available=False for those slices and the board runs normally.
    Join keys mirror board/explanation.py _PHASE4_CHAIR_SQL exactly:
    WHERE tv.RUN_ID = %s AND tv.DOSSIER_ID = %s.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM MIP.APP.COMMITTEE_HEARING WHERE HEARING_ID = %s", (hearing_id,))
        rows = fetch_all(cur)
        if not rows:
            raise ValueError(f"Hearing {hearing_id} not found")
        hearing = rows[0]

        proposal_id = int(hearing["PROPOSAL_ID"])
        cur.execute("SELECT * FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS WHERE PROPOSAL_ID = %s", (proposal_id,))
        proposals = fetch_all(cur)
        if not proposals:
            raise ValueError(f"Proposal {proposal_id} not found")
        proposal = proposals[0]

        cur.execute("SELECT * FROM MIP.APP.STRUCTURAL_PROPOSAL_SNAPSHOT WHERE PROPOSAL_ID = %s", (proposal_id,))
        snapshots = fetch_all(cur)
        if not snapshots:
            raise ValueError(f"Snapshot for proposal {proposal_id} not found")
        snapshot = snapshots[0]

        cur.execute(
            "SELECT ROLE_NAME, OUTPUT_JSON, EVIDENCE_REFS FROM MIP.APP.COMMITTEE_ROLE_OUTPUT WHERE HEARING_ID = %s",
            (hearing_id,),
        )
        roles = fetch_all(cur)

        cur.execute(
            "SELECT ARTIFACT_KIND, PAYLOAD_JSON FROM MIP.APP.COMMITTEE_EVIDENCE_ARTIFACT WHERE HEARING_ID = %s",
            (hearing_id,),
        )
        artifacts = fetch_all(cur)

        # Phase 4 evidence — NULL-safe. Only fetched when BOARD_RUN_ID and
        # BOARD_DOSSIER_ID are present on the proposal (Phase 4-native rows).
        # Pre-Phase-4 proposals return empty dicts; no exception is raised.
        board_run_id = proposal.get("BOARD_RUN_ID")
        board_dossier_id = proposal.get("BOARD_DOSSIER_ID")

        phase4_thesis: Dict[str, Any] = {}
        phase4_dossier: Dict[str, Any] = {}

        if board_run_id and board_dossier_id:
            try:
                cur.execute(
                    """
                    SELECT
                        FINAL_ACTION, FINAL_DIRECTION, PRIMARY_REASON_CODE,
                        SECONDARY_REASON_CODE, FINAL_THESIS,
                        WHY_NOT_OPPOSITE, WHY_NOT_NO_TRADE, RISK_TREATMENT,
                        CHAIR_OUTPUT_JSON
                    FROM MIP.APP.PROPOSAL_BOARD_THESIS_VERDICT
                    WHERE RUN_ID = %s AND DOSSIER_ID = %s
                    LIMIT 1
                    """,
                    (board_run_id, int(board_dossier_id)),
                )
                tv_rows = fetch_all(cur)
                if tv_rows:
                    phase4_thesis = tv_rows[0]
            except Exception as exc:
                logger.warning(
                    "shadow_fetch: phase4 thesis query failed for proposal=%s "
                    "(run=%s dossier=%s): %s — continuing with phase4_available=false",
                    proposal_id, board_run_id, board_dossier_id, exc,
                )

            try:
                cur.execute(
                    """
                    SELECT DOSSIER_PAYLOAD_JSON
                    FROM MIP.APP.PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT
                    WHERE RUN_ID = %s AND DOSSIER_ID = %s
                    LIMIT 1
                    """,
                    (board_run_id, int(board_dossier_id)),
                )
                ds_rows = fetch_all(cur)
                if ds_rows:
                    phase4_dossier = ds_rows[0]
            except Exception as exc:
                logger.warning(
                    "shadow_fetch: phase4 dossier query failed for proposal=%s "
                    "(run=%s dossier=%s): %s — continuing with phase4_available=false",
                    proposal_id, board_run_id, board_dossier_id, exc,
                )

        return hearing, snapshot, proposal, roles, artifacts, phase4_thesis, phase4_dossier
    finally:
        conn.close()


def _stage_evidence_pack(pack: ShadowEvidencePack, session_id: str) -> None:
    """Insert ShadowEvidencePack into SHADOW_EVIDENCE_PACK_CACHE."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO MIP.APP.SHADOW_EVIDENCE_PACK_CACHE
                (HEARING_ID, PACK_JSON, SESSION_ID, CREATED_AT, EXPIRES_AT)
            SELECT
                %s,
                PARSE_JSON(%s),
                %s,
                CURRENT_TIMESTAMP(),
                DATEADD('hour', %s, CURRENT_TIMESTAMP())
            """,
            (pack.hearing_id, _jdump(pack.to_cache_dict()), session_id, _CACHE_TTL_HOURS),
        )
    finally:
        conn.close()


def _expire_evidence_pack(hearing_id: str) -> None:
    """Immediately expire the cache entry (set EXPIRES_AT = now)."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE MIP.APP.SHADOW_EVIDENCE_PACK_CACHE
               SET EXPIRES_AT = CURRENT_TIMESTAMP()
             WHERE HEARING_ID = %s
            """,
            (hearing_id,),
        )
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Stage 1: Specialist agents
# ---------------------------------------------------------------------------

async def _run_specialist(
    role: str,
    agent_name: str,
    hearing_id: str,
    account: str,
    user: str,
    pk_path: str,
    timeout: float,
) -> Tuple[str, SpecialistPosition | DegradedPosition, int]:
    """Run one specialist agent; return (role, position, elapsed_ms)."""
    t0 = time.monotonic()
    try:
        logger.info("shadow_stage1: starting %s (%s)", role, agent_name)
        messages = [
            {
                "role": "user",
                "content": (
                    f"Hearing ID: {hearing_id}\n"
                    f"You are the {role} specialist. Call get_evidence_slice to retrieve your evidence "
                    f"slices, then form your position and return the JSON object as instructed."
                ),
            }
        ]
        resp = await run_agent_object(
            account=account,
            user=user,
            private_key_path=pk_path,
            agent_name=agent_name,
            messages=messages,
            timeout=timeout,
        )
        raw_text = extract_agent_text(resp)
        position = parse_specialist_position(raw_text, role)
        elapsed = int((time.monotonic() - t0) * 1000)
        logger.info("shadow_stage1: %s done in %dms stance=%s", role, elapsed, getattr(position, "stance", "?"))
        return role, position, elapsed
    except Exception as exc:
        elapsed = int((time.monotonic() - t0) * 1000)
        logger.warning("shadow_stage1: %s FAILED in %dms: %s: %s", role, elapsed, type(exc).__name__, exc)
        return role, DegradedPosition(
            role=role,
            degraded=True,
            degraded_reason=(f"{type(exc).__name__}: {exc}" if str(exc) else type(exc).__name__)[:400],
        ), elapsed


# ---------------------------------------------------------------------------
# Stage 3: Challenge turn (objectless)
# ---------------------------------------------------------------------------

def _challenge_system_prompt(challenger_role: str, target_role: str) -> str:
    return (
        f"You are the {challenger_role} specialist on the Shadow Investment Committee. "
        f"You disagree with the {target_role} specialist's position. "
        f"You must articulate a focused, evidence-based challenge to their stance. "
        f"Reference your own evidence and explain why their position is inconsistent with the evidence. "
        f"Keep your challenge to 3-5 sentences. Be specific, not rhetorical. "
        f"Return ONLY a JSON object: {{\"challenge_text\": \"<your challenge>\"}}"
    )


def _challenge_user_message(
    challenger_role: str,
    challenger_pos: SpecialistPosition | DegradedPosition,
    target_role: str,
    target_pos: SpecialistPosition | DegradedPosition,
) -> str:
    return (
        f"Your stance: {getattr(challenger_pos, 'stance', 'DEFER')} "
        f"(confidence {getattr(challenger_pos, 'confidence', 0.0):.2f}). "
        f"Your rationale: {getattr(challenger_pos, 'rationale', 'n/a')}\n\n"
        f"Target specialist {target_role} stance: {getattr(target_pos, 'stance', 'DEFER')} "
        f"(confidence {getattr(target_pos, 'confidence', 0.0):.2f}). "
        f"Their rationale: {getattr(target_pos, 'rationale', 'n/a')}\n\n"
        f"Challenge their position with evidence from your domain."
    )


async def _run_challenge(
    conflict: ConflictEntry,
    positions: Dict[str, SpecialistPosition | DegradedPosition],
    hearing_id: str,
    account: str,
    user: str,
    pk_path: str,
    timeout: float,
) -> Tuple[ChallengeTurn, int]:
    t0 = time.monotonic()
    challenger_role = conflict.challenger_role
    target_role = conflict.target_role
    try:
        logger.info("shadow_stage3: challenge %s -> %s", challenger_role, target_role)
        sys_prompt = _challenge_system_prompt(challenger_role, target_role)
        user_msg = _challenge_user_message(
            challenger_role,
            positions.get(challenger_role, DegradedPosition(role=challenger_role)),
            target_role,
            positions.get(target_role, DegradedPosition(role=target_role)),
        )
        resp = await run_agent_objectless(
            account=account,
            user=user,
            private_key_path=pk_path,
            model=_OBJECTLESS_MODEL,
            system_prompt=sys_prompt,
            user_message=user_msg,
            timeout=timeout,
        )
        raw_text = extract_agent_text(resp)
        # Parse challenge_text from JSON or use raw text
        try:
            data = json.loads(raw_text)
            challenge_text = str(data.get("challenge_text") or raw_text)
        except Exception:
            challenge_text = raw_text[:2000]

        elapsed = int((time.monotonic() - t0) * 1000)
        logger.info("shadow_stage3: challenge done in %dms", elapsed)
        return ChallengeTurn(
            challenger_role=challenger_role,
            target_role=target_role,
            challenge_text=challenge_text,
        ), elapsed
    except Exception as exc:
        elapsed = int((time.monotonic() - t0) * 1000)
        logger.warning("shadow_stage3: challenge FAILED in %dms: %s", elapsed, exc)
        return ChallengeTurn(
            challenger_role=challenger_role or "UNKNOWN",
            target_role=target_role or "UNKNOWN",
            challenge_text="",
            parse_ok=False,
            degraded=True,
            degraded_reason=str(exc)[:400],
        ), elapsed


# ---------------------------------------------------------------------------
# Stage 4: Revision turn (objectless)
# ---------------------------------------------------------------------------

def _revision_system_prompt(target_role: str) -> str:
    return (
        f"You are the {target_role} specialist on the Shadow Investment Committee. "
        f"A colleague has challenged your position. "
        f"Review the challenge carefully. You may maintain your position if you can justify it, "
        f"or revise your stance if the challenge reveals something you overlooked. "
        f"You MUST pick exactly one of these stances: APPROVE, APPROVE_REDUCED, WAIT_RECLAIM, DEFER, DENY. "
        f"Return ONLY a JSON object: "
        f"{{\"revised_stance\": \"<stance>\", \"revision_note\": \"<2-3 sentences justifying your decision>\"}}"
    )


def _revision_user_message(
    original_pos: SpecialistPosition | DegradedPosition,
    challenge: ChallengeTurn,
) -> str:
    return (
        f"Your original stance: {getattr(original_pos, 'stance', 'DEFER')} "
        f"(confidence {getattr(original_pos, 'confidence', 0.0):.2f}). "
        f"Your rationale: {getattr(original_pos, 'rationale', 'n/a')}\n\n"
        f"Challenge from {challenge.challenger_role}:\n{challenge.challenge_text}\n\n"
        f"Maintain or revise your stance. Justify your decision."
    )


async def _run_revision(
    target_role: str,
    original_pos: SpecialistPosition | DegradedPosition,
    challenge: ChallengeTurn,
    account: str,
    user: str,
    pk_path: str,
    timeout: float,
) -> Tuple[RevisionTurn, int]:
    t0 = time.monotonic()
    original_stance = getattr(original_pos, "stance", "DEFER")
    try:
        logger.info("shadow_stage4: revision for %s", target_role)
        sys_prompt = _revision_system_prompt(target_role)
        user_msg = _revision_user_message(original_pos, challenge)
        resp = await run_agent_objectless(
            account=account,
            user=user,
            private_key_path=pk_path,
            model=_OBJECTLESS_MODEL,
            system_prompt=sys_prompt,
            user_message=user_msg,
            timeout=timeout,
        )
        raw_text = extract_agent_text(resp)
        try:
            data = json.loads(raw_text)
        except Exception:
            # strip markdown fences
            clean = raw_text.strip()
            if clean.startswith("```"):
                lines = [l for l in clean.splitlines() if not l.startswith("```")]
                clean = "\n".join(lines).strip()
            data = json.loads(clean)

        revised = str(data.get("revised_stance") or original_stance).upper()
        from .shadow_types import ALLOWED_STANCES
        if revised not in ALLOWED_STANCES:
            revised = original_stance

        elapsed = int((time.monotonic() - t0) * 1000)
        logger.info("shadow_stage4: revision done in %dms %s -> %s", elapsed, original_stance, revised)
        return RevisionTurn(
            role_name=target_role,
            revised_stance=revised,
            original_stance=original_stance,
            revision_note=str(data.get("revision_note") or "")[:2000],
        ), elapsed
    except Exception as exc:
        elapsed = int((time.monotonic() - t0) * 1000)
        logger.warning("shadow_stage4: revision FAILED in %dms: %s", elapsed, exc)
        return RevisionTurn(
            role_name=target_role,
            revised_stance=original_stance,
            original_stance=original_stance,
            parse_ok=False,
            degraded=True,
            degraded_reason=str(exc)[:400],
        ), elapsed


# ---------------------------------------------------------------------------
# Stage 5: Chair agent
# ---------------------------------------------------------------------------

def _chair_context_message(
    hearing_id: str,
    positions: Dict[str, Any],
    revisions: List[RevisionTurn],
    conflicts: List[ConflictEntry],
) -> str:
    lines = [f"Hearing ID: {hearing_id}", "", "SPECIALIST POSITIONS (final after any revisions):"]
    for role, pos in positions.items():
        stance = getattr(pos, "stance", "DEFER")
        conf = getattr(pos, "confidence", 0.0)
        # Apply any revision
        rev = next((r for r in revisions if r.role_name == role), None)
        if rev and not rev.degraded:
            stance = rev.revised_stance
            lines.append(f"  {role}: {stance} (conf {conf:.2f}) [REVISED from {rev.original_stance}]")
        else:
            lines.append(f"  {role}: {stance} (conf {conf:.2f})")

    if conflicts:
        lines.append("")
        lines.append(f"CONFLICTS DETECTED ({len(conflicts)}):")
        for c in conflicts:
            lines.append(f"  {c.role_a} [{c.stance_a}] vs {c.role_b} [{c.stance_b}] — {c.severity}")

    lines.extend([
        "",
        "Now call get_evidence_slice with role_name=SHADOW_CHAIR to retrieve evidence slices as needed.",
        "Then issue your shadow ruling as the JSON object specified in your instructions.",
    ])
    return "\n".join(lines)


async def _run_chair(
    hearing_id: str,
    positions: Dict[str, Any],
    revisions: List[RevisionTurn],
    conflicts: List[ConflictEntry],
    account: str,
    user: str,
    pk_path: str,
    timeout: float,
) -> Tuple[ShadowChairRuling, int]:
    t0 = time.monotonic()
    try:
        logger.info("shadow_stage5: chair agent starting")
        context_msg = _chair_context_message(hearing_id, positions, revisions, conflicts)
        messages = [{"role": "user", "content": context_msg}]
        resp = await run_agent_object(
            account=account,
            user=user,
            private_key_path=pk_path,
            agent_name=_CHAIR_AGENT,
            messages=messages,
            timeout=timeout,
        )
        raw_text = extract_agent_text(resp)
        ruling = parse_chair_ruling(raw_text)
        elapsed = int((time.monotonic() - t0) * 1000)
        logger.info("shadow_stage5: chair done in %dms stance=%s", elapsed, ruling.shadow_stance)
        return ruling, elapsed
    except Exception as exc:
        elapsed = int((time.monotonic() - t0) * 1000)
        logger.warning("shadow_stage5: chair FAILED in %dms: %s: %s", elapsed, type(exc).__name__, exc)
        return ShadowChairRuling(
            shadow_stance="DEFER",
            shadow_confidence=0.0,
            parse_ok=False,
            degraded=True,
            degraded_reason=(f"{type(exc).__name__}: {exc}" if str(exc) else type(exc).__name__)[:400],
        ), elapsed


# ---------------------------------------------------------------------------
# Incremental persistence checkpoints
# ---------------------------------------------------------------------------
# Without these, the orchestrator persists nothing until the entire 7-stage
# pipeline completes (Stage 6 in `finally`). The frontend then sees a long
# silence and a single "everything appears at once" snap. With these inline
# checkpoints the LPA poll sees the deliberation unfold one bubble at a
# time: stage rail advances, specialist bubbles cascade in, conflict marker
# appears, challenge / revision land as threaded replies, chair finale
# closes the session.
#
# Each helper is best-effort and idempotent enough that a partial run leaves
# a coherent (if incomplete) row set behind. Wrap every call site in
# try/except — a write hiccup must NOT break orchestration.

def _checkpoint_session_progress_sync(
    session_id: str,
    stage_reached: int,
    status: str = "RUNNING",
) -> None:
    """Bump SHADOW_BOARD_SESSION.{STAGE_REACHED,STATUS} so polling sees motion."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE MIP.APP.SHADOW_BOARD_SESSION
               SET STAGE_REACHED = %s,
                   STATUS = %s
             WHERE SESSION_ID = %s
            """,
            (int(stage_reached), status, session_id),
        )
    finally:
        conn.close()


def _insert_specialist_position_sync(
    session_id: str,
    hearing_id: str,
    pos: SpecialistPosition | DegradedPosition,
    elapsed_ms: int,
) -> None:
    """Persist one specialist row immediately. Skip if a row for this
    (session_id, role) already exists (idempotent for retried writes)."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        role = getattr(pos, "role", "UNKNOWN")
        cur.execute(
            """
            INSERT INTO MIP.APP.SHADOW_SPECIALIST_POSITION
                (SESSION_ID, HEARING_ID, ROLE_NAME, STANCE, CONFIDENCE,
                 RATIONALE, EVIDENCE_USED, RAW_RESPONSE,
                 PARSE_OK, DEGRADED, DEGRADED_REASON, AGENT_ELAPSED_MS)
            SELECT
                %s, %s, %s, %s, %s,
                %s, PARSE_JSON(%s), PARSE_JSON(%s),
                %s, %s, %s, %s
            WHERE NOT EXISTS (
                SELECT 1 FROM MIP.APP.SHADOW_SPECIALIST_POSITION
                 WHERE SESSION_ID = %s AND ROLE_NAME = %s
            )
            """,
            (
                session_id, hearing_id, role,
                getattr(pos, "stance", "DEFER"),
                getattr(pos, "confidence", 0.0),
                getattr(pos, "rationale", "")[:2000],
                _jdump(getattr(pos, "evidence_used", [])),
                "{}",
                not getattr(pos, "degraded", False),
                bool(getattr(pos, "degraded", False)),
                (getattr(pos, "degraded_reason", "") or "")[:500],
                int(elapsed_ms or 0),
                session_id, role,
            ),
        )
    finally:
        conn.close()


def _replace_specialist_row_sync(
    session_id: str,
    hearing_id: str,
    pos: SpecialistPosition | DegradedPosition,
    elapsed_ms: int,
) -> None:
    """Replace any existing row for (session_id, role) with the latest result.
    Used after the seeded "thinking" placeholder is superseded by the real
    specialist response. DELETE+INSERT rather than UPDATE because the table
    holds VARIANT columns (EVIDENCE_USED, RAW_RESPONSE) that PARSE_JSON in
    INSERT but are awkward to set in UPDATE."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        role = getattr(pos, "role", "UNKNOWN")
        cur.execute(
            "DELETE FROM MIP.APP.SHADOW_SPECIALIST_POSITION WHERE SESSION_ID = %s AND ROLE_NAME = %s",
            (session_id, role),
        )
        cur.execute(
            """
            INSERT INTO MIP.APP.SHADOW_SPECIALIST_POSITION
                (SESSION_ID, HEARING_ID, ROLE_NAME, STANCE, CONFIDENCE,
                 RATIONALE, EVIDENCE_USED, RAW_RESPONSE,
                 PARSE_OK, DEGRADED, DEGRADED_REASON, AGENT_ELAPSED_MS)
            SELECT
                %s, %s, %s, %s, %s,
                %s, PARSE_JSON(%s), PARSE_JSON(%s),
                %s, %s, %s, %s
            """,
            (
                session_id, hearing_id, role,
                getattr(pos, "stance", "DEFER"),
                getattr(pos, "confidence", 0.0),
                getattr(pos, "rationale", "")[:2000],
                _jdump(getattr(pos, "evidence_used", [])),
                "{}",
                not getattr(pos, "degraded", False),
                bool(getattr(pos, "degraded", False)),
                (getattr(pos, "degraded_reason", "") or "")[:500],
                int(elapsed_ms or 0),
            ),
        )
    finally:
        conn.close()


def _insert_conflict_row_sync(
    session_id: str,
    hearing_id: str,
    c: ConflictEntry,
) -> None:
    """Persist one conflict row immediately."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO MIP.APP.SHADOW_CONFLICT_MAP
                (SESSION_ID, HEARING_ID, ROLE_A, ROLE_B,
                 STANCE_A, STANCE_B, SEVERITY,
                 CHALLENGER_ROLE, TARGET_ROLE)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                session_id, hearing_id, c.role_a, c.role_b,
                c.stance_a, c.stance_b, c.severity,
                c.challenger_role, c.target_role,
            ),
        )
    finally:
        conn.close()


def _insert_challenge_row_sync(
    session_id: str,
    hearing_id: str,
    ch: ChallengeTurn,
    elapsed_ms: int,
) -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO MIP.APP.SHADOW_CHALLENGE_TURN
                (SESSION_ID, HEARING_ID, CHALLENGER_ROLE, TARGET_ROLE,
                 CHALLENGE_TEXT, RAW_RESPONSE,
                 PARSE_OK, DEGRADED, DEGRADED_REASON, AGENT_ELAPSED_MS)
            SELECT
                %s, %s, %s, %s,
                %s, PARSE_JSON(%s),
                %s, %s, %s, %s
            """,
            (
                session_id, hearing_id, ch.challenger_role, ch.target_role,
                (ch.challenge_text or "")[:4000],
                "{}",
                ch.parse_ok, ch.degraded,
                (ch.degraded_reason or "")[:500],
                int(elapsed_ms or 0),
            ),
        )
    finally:
        conn.close()


def _insert_revision_row_sync(
    session_id: str,
    hearing_id: str,
    rev: RevisionTurn,
    elapsed_ms: int,
) -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO MIP.APP.SHADOW_REVISION_TURN
                (SESSION_ID, HEARING_ID, ROLE_NAME,
                 REVISED_STANCE, ORIGINAL_STANCE, STANCE_CHANGED,
                 REVISION_NOTE, RAW_RESPONSE,
                 PARSE_OK, DEGRADED, DEGRADED_REASON, AGENT_ELAPSED_MS)
            SELECT
                %s, %s, %s,
                %s, %s, %s,
                %s, PARSE_JSON(%s),
                %s, %s, %s, %s
            """,
            (
                session_id, hearing_id, rev.role_name,
                rev.revised_stance, rev.original_stance, rev.stance_changed,
                (rev.revision_note or "")[:2000],
                "{}",
                rev.parse_ok, rev.degraded,
                (rev.degraded_reason or "")[:500],
                int(elapsed_ms or 0),
            ),
        )
    finally:
        conn.close()


def _insert_chair_row_sync(
    session_id: str,
    hearing_id: str,
    ch: ShadowChairRuling,
    elapsed_ms: int,
) -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        trade_json = ch.shadow_trade.model_dump() if ch.shadow_trade else {}
        cur.execute(
            """
            INSERT INTO MIP.APP.SHADOW_CHAIR_RULING
                (SESSION_ID, HEARING_ID,
                 SHADOW_STANCE, SHADOW_CONFIDENCE,
                 PLURALITY_BASIS, CONFLICT_RESOLUTION,
                 SHADOW_TRADE_JSON, TOP_SUPPORTS, TOP_TENSIONS,
                 RAW_RESPONSE,
                 PARSE_OK, DEGRADED, DEGRADED_REASON, AGENT_ELAPSED_MS)
            SELECT
                %s, %s,
                %s, %s,
                %s, %s,
                PARSE_JSON(%s), PARSE_JSON(%s), PARSE_JSON(%s),
                PARSE_JSON(%s),
                %s, %s, %s, %s
            """,
            (
                session_id, hearing_id,
                ch.shadow_stance, ch.shadow_confidence,
                (ch.plurality_basis or "")[:500],
                (ch.conflict_resolution or "")[:2000],
                _jdump(trade_json),
                _jdump(ch.top_supports),
                _jdump(ch.top_tensions),
                "{}",
                ch.parse_ok, ch.degraded,
                (ch.degraded_reason or "")[:500],
                int(elapsed_ms or 0),
            ),
        )
    finally:
        conn.close()


def _finalize_session_sync(
    session_id: str,
    stage_reached: int,
    status: str,
    shadow_stance: str,
    shadow_confidence: float,
    degraded: bool,
    degraded_reason: str,
    run_ms: int,
    proposal_id: int,
    snapshot_id: Optional[int] = None,
    evidence_pack_hash: Optional[str] = None,
) -> None:
    """Final UPDATE on the session row — terminal state only. Child rows are
    already in place via the per-stage checkpoints above."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE MIP.APP.SHADOW_BOARD_SESSION
               SET PROPOSAL_ID        = COALESCE(%s, PROPOSAL_ID),
                   SNAPSHOT_ID        = COALESCE(%s, SNAPSHOT_ID),
                   EVIDENCE_PACK_HASH = COALESCE(%s, EVIDENCE_PACK_HASH),
                   SHADOW_STANCE      = %s,
                   SHADOW_CONFIDENCE  = %s,
                   STAGE_REACHED      = %s,
                   STATUS             = %s,
                   DEGRADED           = %s,
                   DEGRADED_REASON    = %s,
                   RUN_MS             = %s,
                   PACK_VERSION       = '2.0.0',
                   COMPLETED_AT       = CURRENT_TIMESTAMP()
             WHERE SESSION_ID = %s
            """,
            (
                proposal_id,
                snapshot_id, evidence_pack_hash,
                shadow_stance, shadow_confidence,
                int(stage_reached), status,
                bool(degraded), (degraded_reason or "")[:500],
                int(run_ms or 0),
                session_id,
            ),
        )
    finally:
        conn.close()


def _run_agentic_materializer_after_auto_commit(
    action_id: Optional[str],
    commit_result: Dict[str, Any],
) -> None:
    """Phase 5C — fire the agentic materializer after an auto-commit so
    LIVE_ACTIONS transitions in lock-step with the OPERATOR_COMMITTED row.

    Mirrors `app.routers.agentic_authority_router._maybe_run_agentic_materializer`
    but lives here so the shadow-board completion hook can call it without
    requiring an HTTP round-trip. Deferred imports avoid a circular dependency
    with `app.routers.live`.

    Fail-closed: any exception is logged but never propagated. The auto-commit
    authority row is preserved even if materialization fails, so the operator
    can re-trigger materialization via the explicit endpoint if needed.
    """
    if not action_id:
        return
    try:
        from app.committee.agentic_authority import (
            is_agentic_primary_materialization_enabled,
        )
        from app.routers.agentic_authority_router import (
            _build_authority_row_from_commit,
            agentic_materializer_status_eligible,
        )
        from app.routers.committee import _underlying_sf_conn
        from app.routers.live import (
            _fetch_live_action,
            _materialize_structural_entry_agentic_apply,
            is_structural_live_action,
        )
    except Exception as imp_exc:  # noqa: BLE001
        logger.warning(
            "auto_commit materializer: import failed (skipping) action=%s: %s",
            action_id, imp_exc,
        )
        return

    conn = None
    try:
        conn = get_connection()
        if not is_agentic_primary_materialization_enabled(conn):
            logger.info(
                "auto_commit materializer: skipped (flag off) action=%s", action_id,
            )
            return

        cur = conn.cursor()
        try:
            action_row = _fetch_live_action(cur, action_id)
            if not action_row:
                logger.info(
                    "auto_commit materializer: action not found action=%s",
                    action_id,
                )
                return
            if not is_structural_live_action(action_row):
                logger.info(
                    "auto_commit materializer: non-structural action=%s",
                    action_id,
                )
                return
            status = str(action_row.get("STATUS") or "").upper()
            eligible, eligibility_reason = agentic_materializer_status_eligible(dict(action_row))
            if not eligible:
                logger.info(
                    "auto_commit materializer: status not eligible action=%s status=%s reason=%s",
                    action_id, status, eligibility_reason,
                )
                return

            recovery_late_stage = (
                eligibility_reason == "recovery_incomplete_contract"
                and status in ("REVALIDATED_PASS", "REVALIDATED_FAIL")
            )
            authority_row = _build_authority_row_from_commit(action_id, commit_result)
            raw = _underlying_sf_conn(conn)
            raw.autocommit(False)
            try:
                out = _materialize_structural_entry_agentic_apply(
                    cur,
                    action_id,
                    dict(action_row),
                    authority_row,
                    apply_detail_source="AGENTIC_AUTO_COMMIT",
                    recovery_late_stage=recovery_late_stage,
                )
                raw.commit()
                logger.info(
                    "auto_commit materializer: applied action=%s out_status=%s",
                    action_id, (out or {}).get("status"),
                )
            except Exception:
                raw.rollback()
                raise
            finally:
                raw.autocommit(True)
        finally:
            try:
                cur.close()
            except Exception:  # noqa: BLE001
                pass
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "auto_commit materializer: failed (swallowed) action=%s: %s",
            action_id, exc,
        )
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass


def _auto_audit_authority_after_finalize(
    session_id: str,
    hearing_id: str,
    evidence_pack_hash: Optional[str],
    action_id: Optional[str],
) -> None:
    """Stage 4b + Phase 5C — best-effort authority commit after shadow finalize.

    Stage 4b: writes one AUTHORITY_MODE=AUTO_AUDIT row gated by
    AGENTIC_AUTO_AUDIT_ENABLED.

    Phase 5C: when the AUTO_AUDIT row carries a clean APPROVE / APPROVE_REDUCED
    verdict on a fresh non-degraded session with a supported pack version, the
    hook immediately promotes it to AUTHORITY_MODE=OPERATOR_COMMITTED with
    actor=`system_auto_commit_v1`. This removes the manual "Apply Agentic
    Review" click for the happy path. Auto-promote is gated by
    AGENTIC_AUTO_COMMIT_ENABLED and fail-closed on every non-approve status,
    staleness, degraded session, unsupported pack version, or DB error.

    Always fail-closed: any exception is logged but never propagated. Never
    touches LIVE_ACTIONS directly, Submit gating, LPA, or COMMITTEE_FINAL_DECISION.
    Materialization continues to run from the operator-commit endpoint path or
    from the agentic materializer.
    """
    try:
        from app.committee.agentic_authority import (
            commit_auto_audit_authority_for_session,
            auto_promote_authority_to_operator_committed,
        )
    except Exception as imp_exc:  # noqa: BLE001
        logger.warning(
            "auto_audit: agentic_authority import failed (skipping) session=%s: %s",
            session_id, imp_exc,
        )
        return

    conn = None
    try:
        conn = get_connection()
        audit_result = commit_auto_audit_authority_for_session(
            conn,
            session_id=session_id,
            hearing_id=hearing_id,
            evidence_pack_hash=evidence_pack_hash,
            action_id=action_id,
            enforce_config_flag=True,
        )
        if audit_result.get("committed"):
            logger.info(
                "auto_audit: wrote authority row session=%s action=%s authority_id=%s status=%s",
                session_id,
                audit_result.get("action_id"),
                audit_result.get("authority_id"),
                audit_result.get("authority_status"),
            )
        else:
            logger.info(
                "auto_audit: skipped session=%s hearing=%s reason=%s",
                session_id, hearing_id, audit_result.get("skip_reason"),
            )
            return

        # Phase 5C: try to auto-promote AUTO_AUDIT -> OPERATOR_COMMITTED for
        # the happy path. Any non-approve / stale / degraded / pack-unsupported
        # case is rejected by the eligibility filter and leaves the AUTO_AUDIT
        # row in place so the operator can still override via the explicit
        # endpoint.
        promote_result = auto_promote_authority_to_operator_committed(
            conn,
            audit_result,
            enforce_config_flag=True,
        )
        if promote_result.get("auto_promoted"):
            logger.info(
                "auto_commit: promoted AUTO_AUDIT -> OPERATOR_COMMITTED action=%s "
                "authority_id=%s status=%s",
                promote_result.get("action_id"),
                promote_result.get("authority_id"),
                promote_result.get("authority_status"),
            )
            # Phase 5C: also fire the agentic materializer so LIVE_ACTIONS
            # transitions in lock-step with the auto-committed authority row.
            # Without this the action would carry a fresh OPERATOR_COMMITTED
            # row but LIVE_ACTIONS.STATUS would not advance, so Submit would
            # still be gated by `submission_allowed` even though the operator
            # never has to click anything. Mirrors the materializer call in
            # `agentic_authority_router.commit_agentic_authority`.
            _run_agentic_materializer_after_auto_commit(
                promote_result.get("action_id"),
                promote_result,
            )
        else:
            logger.info(
                "auto_commit: skipped action=%s reason=%s",
                audit_result.get("action_id"),
                promote_result.get("skip_reason"),
            )
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "auto_audit: hook failed (swallowed) session=%s hearing=%s: %s",
            session_id, hearing_id, exc,
        )
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass


async def _safe_checkpoint(coro_call_label: str, fn, *args) -> None:
    """Run a sync persistence helper on a thread, swallowing exceptions.
    Persistence hiccups must not break orchestration — at worst the user
    loses one row's worth of "live" state and the final dump catches up."""
    try:
        await asyncio.to_thread(fn, *args)
    except Exception as exc:
        logger.warning("shadow_checkpoint: %s failed: %s", coro_call_label, exc)


# ---------------------------------------------------------------------------
# Stage 6: Persistence (legacy bulk fallback — kept for safety)
# ---------------------------------------------------------------------------

def _persist_shadow_session(
    result: ShadowBoardResult,
    positions_elapsed: Dict[str, int],
    challenge_elapsed: int,
    revision_elapsed: Dict[str, int],
    chair_elapsed: int,
    snapshot_id: Optional[int] = None,
    evidence_pack_hash: Optional[str] = None,
) -> None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        sid = result.session_id
        hid = result.hearing_id

        # SHADOW_BOARD_SESSION — UPDATE first (Phase 1 dual-hearing kickoff
        # may have inserted a RUNNING placeholder); fall through to INSERT
        # if no placeholder existed (legacy / direct-call path).
        cur.execute(
            """
            UPDATE MIP.APP.SHADOW_BOARD_SESSION
               SET PROPOSAL_ID        = %s,
                   SNAPSHOT_ID        = COALESCE(%s, SNAPSHOT_ID),
                   EVIDENCE_PACK_HASH = COALESCE(%s, EVIDENCE_PACK_HASH),
                   SHADOW_STANCE      = %s,
                   SHADOW_CONFIDENCE  = %s,
                   STAGE_REACHED      = %s,
                   STATUS             = %s,
                   DEGRADED           = %s,
                   DEGRADED_REASON    = %s,
                   RUN_MS             = %s,
                   COMPLETED_AT       = CURRENT_TIMESTAMP()
             WHERE SESSION_ID = %s
            """,
            (
                result.proposal_id,
                snapshot_id, evidence_pack_hash,
                result.shadow_stance, result.shadow_confidence,
                result.stage_reached, result.status,
                result.degraded, (result.degraded_reason or "")[:500],
                result.run_ms,
                sid,
            ),
        )
        if int(getattr(cur, "rowcount", 0) or 0) == 0:
            cur.execute(
                """
                INSERT INTO MIP.APP.SHADOW_BOARD_SESSION
                    (SESSION_ID, HEARING_ID, PROPOSAL_ID,
                     SNAPSHOT_ID, EVIDENCE_PACK_HASH,
                     SHADOW_STANCE, SHADOW_CONFIDENCE,
                     STAGE_REACHED, STATUS, DEGRADED, DEGRADED_REASON,
                     AGENT_MODEL, RUN_MS, CREATED_AT, COMPLETED_AT)
                SELECT
                    %s, %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
                """,
                (
                    sid, hid, result.proposal_id,
                    snapshot_id, evidence_pack_hash,
                    result.shadow_stance, result.shadow_confidence,
                    result.stage_reached, result.status,
                    result.degraded, (result.degraded_reason or "")[:500],
                    _AGENT_MODEL, result.run_ms,
                ),
            )

        # SHADOW_SPECIALIST_POSITION
        for pos in result.positions:
            role = getattr(pos, "role", "UNKNOWN")
            cur.execute(
                """
                INSERT INTO MIP.APP.SHADOW_SPECIALIST_POSITION
                    (SESSION_ID, HEARING_ID, ROLE_NAME, STANCE, CONFIDENCE,
                     RATIONALE, EVIDENCE_USED, RAW_RESPONSE,
                     PARSE_OK, DEGRADED, DEGRADED_REASON, AGENT_ELAPSED_MS)
                SELECT
                    %s, %s, %s, %s, %s,
                    %s, PARSE_JSON(%s), PARSE_JSON(%s),
                    %s, %s, %s, %s
                """,
                (
                    sid, hid, role,
                    getattr(pos, "stance", "DEFER"),
                    getattr(pos, "confidence", 0.0),
                    getattr(pos, "rationale", "")[:2000],
                    _jdump(getattr(pos, "evidence_used", [])),
                    "{}",
                    not getattr(pos, "degraded", False),
                    bool(getattr(pos, "degraded", False)),
                    (getattr(pos, "degraded_reason", "") or "")[:500],
                    positions_elapsed.get(role, 0),
                ),
            )

        # SHADOW_CONFLICT_MAP
        for c in result.conflicts:
            cur.execute(
                """
                INSERT INTO MIP.APP.SHADOW_CONFLICT_MAP
                    (SESSION_ID, HEARING_ID, ROLE_A, ROLE_B,
                     STANCE_A, STANCE_B, SEVERITY,
                     CHALLENGER_ROLE, TARGET_ROLE)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    sid, hid, c.role_a, c.role_b,
                    c.stance_a, c.stance_b, c.severity,
                    c.challenger_role, c.target_role,
                ),
            )

        # SHADOW_CHALLENGE_TURN
        if result.challenge:
            ch = result.challenge
            cur.execute(
                """
                INSERT INTO MIP.APP.SHADOW_CHALLENGE_TURN
                    (SESSION_ID, HEARING_ID, CHALLENGER_ROLE, TARGET_ROLE,
                     CHALLENGE_TEXT, RAW_RESPONSE,
                     PARSE_OK, DEGRADED, DEGRADED_REASON, AGENT_ELAPSED_MS)
                SELECT
                    %s, %s, %s, %s,
                    %s, PARSE_JSON(%s),
                    %s, %s, %s, %s
                """,
                (
                    sid, hid, ch.challenger_role, ch.target_role,
                    ch.challenge_text[:4000],
                    "{}",
                    ch.parse_ok, ch.degraded,
                    (ch.degraded_reason or "")[:500],
                    challenge_elapsed,
                ),
            )

        # SHADOW_REVISION_TURN
        for rev in result.revisions:
            cur.execute(
                """
                INSERT INTO MIP.APP.SHADOW_REVISION_TURN
                    (SESSION_ID, HEARING_ID, ROLE_NAME,
                     REVISED_STANCE, ORIGINAL_STANCE, STANCE_CHANGED,
                     REVISION_NOTE, RAW_RESPONSE,
                     PARSE_OK, DEGRADED, DEGRADED_REASON, AGENT_ELAPSED_MS)
                SELECT
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, PARSE_JSON(%s),
                    %s, %s, %s, %s
                """,
                (
                    sid, hid, rev.role_name,
                    rev.revised_stance, rev.original_stance, rev.stance_changed,
                    rev.revision_note[:2000],
                    "{}",
                    rev.parse_ok, rev.degraded,
                    (rev.degraded_reason or "")[:500],
                    revision_elapsed.get(rev.role_name, 0),
                ),
            )

        # SHADOW_CHAIR_RULING
        if result.chair:
            ch = result.chair
            trade_json = ch.shadow_trade.model_dump() if ch.shadow_trade else {}
            cur.execute(
                """
                INSERT INTO MIP.APP.SHADOW_CHAIR_RULING
                    (SESSION_ID, HEARING_ID,
                     SHADOW_STANCE, SHADOW_CONFIDENCE,
                     PLURALITY_BASIS, CONFLICT_RESOLUTION,
                     SHADOW_TRADE_JSON, TOP_SUPPORTS, TOP_TENSIONS,
                     RAW_RESPONSE,
                     PARSE_OK, DEGRADED, DEGRADED_REASON, AGENT_ELAPSED_MS)
                SELECT
                    %s, %s,
                    %s, %s,
                    %s, %s,
                    PARSE_JSON(%s), PARSE_JSON(%s), PARSE_JSON(%s),
                    PARSE_JSON(%s),
                    %s, %s, %s, %s
                """,
                (
                    sid, hid,
                    ch.shadow_stance, ch.shadow_confidence,
                    ch.plurality_basis[:500],
                    ch.conflict_resolution[:2000],
                    _jdump(trade_json),
                    _jdump(ch.top_supports),
                    _jdump(ch.top_tensions),
                    "{}",
                    ch.parse_ok, ch.degraded,
                    (ch.degraded_reason or "")[:500],
                    chair_elapsed,
                ),
            )

        logger.info("shadow_stage6: persisted session %s", sid)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

async def orchestrate_shadow_board(
    hearing_id: str,
    timeout_sec: float = 120.0,
    session_id: Optional[str] = None,
    snapshot_id: Optional[int] = None,
    evidence_pack_hash: Optional[str] = None,
    action_id: Optional[str] = None,
) -> ShadowBoardResult:
    """
    Run the full shadow board session for a given hearing_id.
    Returns ShadowBoardResult regardless of success or failure.
    Never touches COMMITTEE_FINAL_DECISION.

    Phase 1 dual-hearing: when called from kickoff_shadow_board_for_snapshot,
    `session_id` is the placeholder row's id and `snapshot_id`/
    `evidence_pack_hash` are written through to persistence so the row binds
    to the same snapshot identity as COMMITTEE_HEARING.
    """
    if not session_id:
        session_id = str(uuid.uuid4())
    run_start = time.monotonic()
    account, user, pk_path = _get_snowflake_creds()

    result = ShadowBoardResult(
        session_id=session_id,
        hearing_id=hearing_id,
        proposal_id=0,
        status="RUNNING",
        stage_reached=0,
    )

    positions_elapsed: Dict[str, int] = {}
    challenge_elapsed = 0
    revision_elapsed: Dict[str, int] = {}
    chair_elapsed = 0

    try:
        # ------------------------------------------------------------------
        # Stage 0: Build + stage evidence pack
        # ------------------------------------------------------------------
        logger.info("shadow_stage0: building evidence pack for hearing %s", hearing_id)
        hearing, snapshot, proposal, roles, artifacts, phase4_thesis, phase4_dossier = (
            await asyncio.to_thread(_fetch_hearing_data, hearing_id)
        )
        result.proposal_id = int(proposal.get("PROPOSAL_ID") or 0)
        pack = build_shadow_evidence_pack(
            hearing, snapshot, proposal, roles, artifacts,
            phase4_thesis=phase4_thesis,
            phase4_dossier=phase4_dossier,
        )
        await asyncio.to_thread(_stage_evidence_pack, pack, session_id)
        result.stage_reached = 0
        await _safe_checkpoint("stage0_progress", _checkpoint_session_progress_sync, session_id, 0, "RUNNING")
        logger.info("shadow_stage0: pack staged (hearing=%s session=%s)", hearing_id, session_id)

        # ------------------------------------------------------------------
        # Stage 1: Parallel specialist agents — but stream results in as they
        # land so the LPA poll can show specialist bubbles cascading in.
        # ------------------------------------------------------------------
        logger.info("shadow_stage1: launching %d specialists in parallel", len(_SPECIALIST_AGENTS))

        positions_dict: Dict[str, SpecialistPosition | DegradedPosition] = {}
        # Pre-seed degraded placeholders so EVERY specialist has a row from the
        # moment Stage 1 begins. As real results land we UPSERT in place. This
        # guarantees the user sees all 6 avatars immediately (with a "thinking"
        # state) and never ends up with only 3 visible just because the
        # background task got cancelled at the orchestrator-level timeout.
        for role in _SPECIALIST_AGENTS.keys():
            placeholder = DegradedPosition(
                role=role,
                degraded=True,
                degraded_reason="awaiting_specialist",
            )
            positions_dict[role] = placeholder
            await _safe_checkpoint(
                f"stage1_seed_{role}",
                _insert_specialist_position_sync,
                session_id, hearing_id, placeholder, 0,
            )

        tasks = [
            asyncio.create_task(_run_specialist(
                role=role,
                agent_name=agent_name,
                hearing_id=hearing_id,
                account=account,
                user=user,
                pk_path=pk_path,
                timeout=timeout_sec,
            ))
            for role, agent_name in _SPECIALIST_AGENTS.items()
        ]

        # Drain as each specialist completes — write its row immediately so
        # the frontend poll sees bubbles arriving one at a time instead of
        # all-at-once after a long silence.
        for fut in asyncio.as_completed(tasks):
            try:
                role, position, elapsed = await fut
            except Exception as exc:
                logger.warning("shadow_stage1: drain task failed: %s", exc)
                continue
            positions_dict[role] = position
            positions_elapsed[role] = elapsed
            # Replace the seeded placeholder row with the real result.
            try:
                await asyncio.to_thread(
                    _replace_specialist_row_sync,
                    session_id, hearing_id, position, elapsed,
                )
            except Exception as exc:
                logger.warning("shadow_stage1: row replace for %s failed: %s", role, exc)

        # Snapshot final position list in-result (degraded placeholders for any
        # specialist that never completed remain in positions_dict, so they
        # carry forward into result.positions for the final payload too).
        for role in _SPECIALIST_AGENTS.keys():
            result.positions.append(positions_dict[role])

        result.stage_reached = 1
        valid_count = sum(1 for p in result.positions if not getattr(p, "degraded", False))
        logger.info("shadow_stage1: %d/%d specialists succeeded", valid_count, len(_SPECIALIST_AGENTS))
        await _safe_checkpoint("stage1_progress", _checkpoint_session_progress_sync, session_id, 1, "RUNNING")

        # ------------------------------------------------------------------
        # Stage 2: Conflict detection (Python-side, no agent call)
        # ------------------------------------------------------------------
        valid_positions = [
            p for p in result.positions
            if isinstance(p, SpecialistPosition) and not getattr(p, "degraded", False)
        ]
        conflicts = detect_conflicts(valid_positions)
        result.conflicts = conflicts
        result.stage_reached = 2
        primary_conflict = pick_primary_conflict(conflicts)
        logger.info("shadow_stage2: %d conflicts detected (primary=%s)", len(conflicts),
                    primary_conflict.severity if primary_conflict else "none")
        for c in conflicts:
            await _safe_checkpoint(
                f"stage2_conflict_{c.role_a}_{c.role_b}",
                _insert_conflict_row_sync, session_id, hearing_id, c,
            )
        await _safe_checkpoint("stage2_progress", _checkpoint_session_progress_sync, session_id, 2, "RUNNING")

        # ------------------------------------------------------------------
        # Stage 3: Challenge turn (only if conflict exists)
        # ------------------------------------------------------------------
        challenge: Optional[ChallengeTurn] = None
        if primary_conflict and primary_conflict.challenger_role and primary_conflict.target_role:
            challenge, challenge_elapsed = await _run_challenge(
                conflict=primary_conflict,
                positions=positions_dict,
                hearing_id=hearing_id,
                account=account,
                user=user,
                pk_path=pk_path,
                timeout=timeout_sec,
            )
            result.challenge = challenge
            await _safe_checkpoint(
                "stage3_challenge",
                _insert_challenge_row_sync,
                session_id, hearing_id, challenge, challenge_elapsed,
            )
        result.stage_reached = 3
        await _safe_checkpoint("stage3_progress", _checkpoint_session_progress_sync, session_id, 3, "RUNNING")

        # ------------------------------------------------------------------
        # Stage 4: Revision turn (only if challenge exists and targets a valid specialist)
        # ------------------------------------------------------------------
        revisions: List[RevisionTurn] = []
        if challenge and not challenge.degraded and challenge.target_role in positions_dict:
            target_role = challenge.target_role
            original_pos = positions_dict[target_role]
            revision, rev_elapsed = await _run_revision(
                target_role=target_role,
                original_pos=original_pos,
                challenge=challenge,
                account=account,
                user=user,
                pk_path=pk_path,
                timeout=timeout_sec,
            )
            revisions.append(revision)
            revision_elapsed[target_role] = rev_elapsed
            await _safe_checkpoint(
                f"stage4_revision_{target_role}",
                _insert_revision_row_sync,
                session_id, hearing_id, revision, rev_elapsed,
            )

            # Update positions_dict with revised stance for chair context
            if not revision.degraded:
                revised_pos = SpecialistPosition(
                    role=target_role,
                    stance=revision.revised_stance,
                    confidence=getattr(original_pos, "confidence", 0.5),
                    rationale=revision.revision_note,
                    evidence_used=getattr(original_pos, "evidence_used", []),
                ) if isinstance(original_pos, SpecialistPosition) else original_pos
                positions_dict[target_role] = revised_pos

        result.revisions = revisions
        result.stage_reached = 4
        await _safe_checkpoint("stage4_progress", _checkpoint_session_progress_sync, session_id, 4, "RUNNING")

        # ------------------------------------------------------------------
        # Stage 5: Chair ruling
        # ------------------------------------------------------------------
        chair_ruling, chair_elapsed = await _run_chair(
            hearing_id=hearing_id,
            positions=positions_dict,
            revisions=revisions,
            conflicts=conflicts,
            account=account,
            user=user,
            pk_path=pk_path,
            timeout=timeout_sec,
        )
        result.chair = chair_ruling
        result.shadow_stance = chair_ruling.shadow_stance
        result.shadow_confidence = chair_ruling.shadow_confidence
        result.stage_reached = 5
        await _safe_checkpoint(
            "stage5_chair",
            _insert_chair_row_sync,
            session_id, hearing_id, chair_ruling, chair_elapsed,
        )
        await _safe_checkpoint("stage5_progress", _checkpoint_session_progress_sync, session_id, 5, "RUNNING")

        # Determine final status
        any_degraded = (
            any(getattr(p, "degraded", False) for p in result.positions)
            or (chair_ruling.degraded)
        )
        result.status = "DEGRADED" if any_degraded else "COMPLETE"
        if any_degraded:
            result.degraded = True
            result.degraded_reason = "One or more stages produced degraded output"

    except Exception as exc:
        logger.error("shadow_board: orchestration FAILED for hearing %s: %s", hearing_id, exc, exc_info=True)
        result.status = "FAILED"
        result.degraded = True
        result.degraded_reason = str(exc)[:500]
        if not result.shadow_stance:
            result.shadow_stance = "DEFER"
            result.shadow_confidence = 0.0

    finally:
        result.run_ms = int((time.monotonic() - run_start) * 1000)

        # Stage 6: Finalize. Child rows (positions/conflicts/challenge/
        # revision/chair) were written incrementally during the run via
        # _safe_checkpoint(...) calls, so the only thing left is the
        # terminal UPDATE on the session row (status, stance, confidence,
        # run_ms, completed_at). If incremental writes were skipped due to
        # an early exception, fall back to the legacy bulk persist so the
        # UI still gets *something*.
        try:
            await asyncio.to_thread(
                _finalize_session_sync,
                session_id,
                result.stage_reached,
                result.status,
                result.shadow_stance or "DEFER",
                float(result.shadow_confidence or 0.0),
                bool(result.degraded),
                (result.degraded_reason or ""),
                result.run_ms,
                result.proposal_id,
                snapshot_id,
                evidence_pack_hash,
            )
        except Exception as finalize_exc:
            logger.error("shadow_board: finalize FAILED for session %s: %s", session_id, finalize_exc)
            # Last-resort bulk write so we leave coherent rows behind.
            try:
                await asyncio.to_thread(
                    _persist_shadow_session,
                    result,
                    positions_elapsed,
                    challenge_elapsed,
                    revision_elapsed,
                    chair_elapsed,
                    snapshot_id,
                    evidence_pack_hash,
                )
            except Exception as persist_exc:
                logger.error("shadow_board: bulk fallback also FAILED for session %s: %s", session_id, persist_exc)

        # Stage 4b — best-effort auto-audit authority commit. Gated by
        # APP_CONFIG.AGENTIC_AUTO_AUDIT_ENABLED. Writes one AUTHORITY_MODE=AUTO_AUDIT
        # row into MIP.APP.AGENTIC_REVALIDATION_AUTHORITY. Never touches
        # LIVE_ACTIONS, Submit gating, COMMITTEE_FINAL_DECISION, or LPA. Any
        # failure here is swallowed and logged so it cannot degrade the shadow
        # board completion.
        try:
            await asyncio.to_thread(
                _auto_audit_authority_after_finalize,
                session_id,
                hearing_id,
                evidence_pack_hash,
                action_id,
            )
        except Exception as audit_exc:  # noqa: BLE001
            logger.warning(
                "shadow_board: auto-audit hook raised (swallowed) session=%s hearing=%s: %s",
                session_id, hearing_id, audit_exc,
            )

        # Expire the evidence pack cache entry
        try:
            await asyncio.to_thread(_expire_evidence_pack, hearing_id)
        except Exception as exp_exc:
            logger.warning("shadow_board: cache expiry failed for hearing %s: %s", hearing_id, exp_exc)

    return result


# ---------------------------------------------------------------------------
# Fetch cached session for GET endpoint (sync helper)
# ---------------------------------------------------------------------------

def _fetch_hearing_evidence_pack_hash_sync(hearing_id: str) -> Optional[str]:
    """Return COMMITTEE_HEARING.EVIDENCE_PACK_HASH for session binding."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT EVIDENCE_PACK_HASH
              FROM MIP.APP.COMMITTEE_HEARING
             WHERE HEARING_ID = %s
            """,
            (hearing_id,),
        )
        rows = fetch_all(cur)
        if not rows:
            return None
        raw = rows[0].get("EVIDENCE_PACK_HASH")
        return str(raw) if raw else None
    finally:
        conn.close()


def _select_shadow_session_row_sync(
    cur,
    hearing_id: str,
    evidence_pack_hash: Optional[str] = None,
) -> tuple[Optional[Dict[str, Any]], bool]:
    """
    Pick the shadow session row operators should see for a hearing.

    Priority:
      1. RUNNING session (any hash — a fresh run is in flight)
      2. Session whose EVIDENCE_PACK_HASH matches the hearing's current hash
         (or the explicit `evidence_pack_hash` query param when supplied)
      3. Most recent session (marked stale_session=True when hash mismatches)

    Returns (row, stale_session).
    """
    cur.execute(
        """
        SELECT *
          FROM MIP.APP.SHADOW_BOARD_SESSION
         WHERE HEARING_ID = %s
         ORDER BY CREATED_AT DESC
        """,
        (hearing_id,),
    )
    rows = fetch_all(cur)
    if not rows:
        return None, False

    running = next(
        (r for r in rows if str(r.get("STATUS") or "").upper() == "RUNNING"),
        None,
    )
    if running:
        return running, False

    target_hash = evidence_pack_hash
    if not target_hash:
        cur.execute(
            """
            SELECT EVIDENCE_PACK_HASH
              FROM MIP.APP.COMMITTEE_HEARING
             WHERE HEARING_ID = %s
            """,
            (hearing_id,),
        )
        hearing_rows = fetch_all(cur)
        if hearing_rows and hearing_rows[0].get("EVIDENCE_PACK_HASH"):
            target_hash = str(hearing_rows[0]["EVIDENCE_PACK_HASH"])
    if target_hash:
        for row in rows:
            if str(row.get("EVIDENCE_PACK_HASH") or "") == target_hash:
                return row, False

    return rows[0], True


def fetch_shadow_session(
    hearing_id: str,
    evidence_pack_hash: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    Retrieve the shadow board session operators should see for a hearing.
    Prefers RUNNING, then hash-matched, then most-recent (stale_session flag).
    Returns None if no session exists.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        session, stale_session = _select_shadow_session_row_sync(
            cur, hearing_id, evidence_pack_hash=evidence_pack_hash,
        )
        if not session:
            return None
        sid = session["SESSION_ID"]

        cur.execute(
            "SELECT * FROM MIP.APP.SHADOW_SPECIALIST_POSITION WHERE SESSION_ID = %s ORDER BY ROLE_NAME",
            (sid,),
        )
        specialist_rows = fetch_all(cur)

        cur.execute(
            "SELECT * FROM MIP.APP.SHADOW_CONFLICT_MAP WHERE SESSION_ID = %s",
            (sid,),
        )
        conflict_rows = fetch_all(cur)

        cur.execute(
            "SELECT * FROM MIP.APP.SHADOW_CHALLENGE_TURN WHERE SESSION_ID = %s",
            (sid,),
        )
        challenge_rows = fetch_all(cur)

        cur.execute(
            "SELECT * FROM MIP.APP.SHADOW_REVISION_TURN WHERE SESSION_ID = %s ORDER BY ROLE_NAME",
            (sid,),
        )
        revision_rows = fetch_all(cur)

        cur.execute(
            "SELECT * FROM MIP.APP.SHADOW_CHAIR_RULING WHERE SESSION_ID = %s",
            (sid,),
        )
        chair_rows = fetch_all(cur)

        def _v(row: Dict[str, Any], k: str) -> Any:
            v = row.get(k)
            if isinstance(v, str):
                try:
                    return json.loads(v)
                except Exception:
                    return v
            return v

        return {
            "ok": True,
            "session_id": sid,
            "hearing_id": hearing_id,
            "proposal_id": session.get("PROPOSAL_ID"),
            "snapshot_id": session.get("SNAPSHOT_ID"),
            "evidence_pack_hash": session.get("EVIDENCE_PACK_HASH"),
            "shadow_stance": session.get("SHADOW_STANCE"),
            "shadow_confidence": session.get("SHADOW_CONFIDENCE"),
            "stage_reached": session.get("STAGE_REACHED"),
            "status": session.get("STATUS"),
            "degraded": session.get("DEGRADED"),
            "degraded_reason": session.get("DEGRADED_REASON"),
            "stale_session": bool(stale_session),
            "run_ms": session.get("RUN_MS"),
            "created_at": str(session.get("CREATED_AT") or ""),
            "positions": [
                {
                    "role": r.get("ROLE_NAME"),
                    "stance": r.get("STANCE"),
                    "confidence": r.get("CONFIDENCE"),
                    "rationale": r.get("RATIONALE"),
                    "evidence_used": _v(r, "EVIDENCE_USED"),
                    "parse_ok": r.get("PARSE_OK"),
                    "degraded": r.get("DEGRADED"),
                    "degraded_reason": r.get("DEGRADED_REASON"),
                }
                for r in specialist_rows
            ],
            "conflicts": [
                {
                    "role_a": c.get("ROLE_A"),
                    "role_b": c.get("ROLE_B"),
                    "stance_a": c.get("STANCE_A"),
                    "stance_b": c.get("STANCE_B"),
                    "severity": c.get("SEVERITY"),
                    "challenger_role": c.get("CHALLENGER_ROLE"),
                    "target_role": c.get("TARGET_ROLE"),
                }
                for c in conflict_rows
            ],
            "challenge": (
                {
                    "challenger_role": challenge_rows[0].get("CHALLENGER_ROLE"),
                    "target_role": challenge_rows[0].get("TARGET_ROLE"),
                    "challenge_text": challenge_rows[0].get("CHALLENGE_TEXT"),
                    "parse_ok": challenge_rows[0].get("PARSE_OK"),
                    "degraded": challenge_rows[0].get("DEGRADED"),
                }
                if challenge_rows else None
            ),
            "revisions": [
                {
                    "role": r.get("ROLE_NAME"),
                    "revised_stance": r.get("REVISED_STANCE"),
                    "original_stance": r.get("ORIGINAL_STANCE"),
                    "stance_changed": r.get("STANCE_CHANGED"),
                    "revision_note": r.get("REVISION_NOTE"),
                    "parse_ok": r.get("PARSE_OK"),
                    "degraded": r.get("DEGRADED"),
                }
                for r in revision_rows
            ],
            "chair": (
                {
                    "shadow_stance": chair_rows[0].get("SHADOW_STANCE"),
                    "shadow_confidence": chair_rows[0].get("SHADOW_CONFIDENCE"),
                    "plurality_basis": chair_rows[0].get("PLURALITY_BASIS"),
                    "conflict_resolution": chair_rows[0].get("CONFLICT_RESOLUTION"),
                    "shadow_trade": _v(chair_rows[0], "SHADOW_TRADE_JSON"),
                    "top_supports": _v(chair_rows[0], "TOP_SUPPORTS"),
                    "top_tensions": _v(chair_rows[0], "TOP_TENSIONS"),
                    "parse_ok": chair_rows[0].get("PARSE_OK"),
                    "degraded": chair_rows[0].get("DEGRADED"),
                }
                if chair_rows else None
            ),
        }
    finally:
        conn.close()


def fetch_shadow_progress(
    hearing_id: str,
    evidence_pack_hash: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    Lightweight status-only fetch for the LPA polling loop. Avoids the full
    payload assembly (specialist rows, conflicts, chair) while a session is
    still RUNNING. Returns None if no session exists.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        row, stale_session = _select_shadow_session_row_sync(
            cur, hearing_id, evidence_pack_hash=evidence_pack_hash,
        )
        if not row:
            return None
        return {
            "ok": True,
            "session_id": row.get("SESSION_ID"),
            "status": row.get("STATUS"),
            "stage_reached": row.get("STAGE_REACHED"),
            "degraded": row.get("DEGRADED"),
            "evidence_pack_hash": row.get("EVIDENCE_PACK_HASH"),
            "snapshot_id": row.get("SNAPSHOT_ID"),
            "created_at": str(row.get("CREATED_AT") or ""),
            "stale_session": bool(stale_session),
        }
    finally:
        conn.close()
