"""
Stage 4a — Agentic revalidation authority mapping and persistence helpers.

Pure module: maps a completed shadow board session to a normalized
`AUTHORITY_STATUS`, builds the row payload, and provides a transactional
supersede-then-insert helper.

Stage 4a is observational only. None of these helpers gate Submit. Submit gating
is added in Stage 4d using the rules described in
`MIP/docs/mip_stage4_agentic_authority_design.md`.

Boundaries:
- This module does NOT call the shadow board.
- This module does NOT call any LPA / live execution / IBKR code.
- This module does NOT read from or write to LIVE_ACTIONS.
- It only reads provided session/c2-decision dicts and writes a row into
  MIP.APP.AGENTIC_REVALIDATION_AUTHORITY (via a caller-provided cursor).
"""
from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Explicit allow-list. New pack versions are admitted only via code review.
# Never use string less-than comparison here.
SUPPORTED_PACK_VERSIONS: frozenset[str] = frozenset({"2.0.0"})

# Normalized authority statuses. Mirrors the design report.
AGENTIC_APPROVE = "AGENTIC_APPROVE"
AGENTIC_APPROVE_REDUCED = "AGENTIC_APPROVE_REDUCED"
AGENTIC_WAIT_RECLAIM = "AGENTIC_WAIT_RECLAIM"
AGENTIC_DEFER = "AGENTIC_DEFER"
AGENTIC_REJECT = "AGENTIC_REJECT"
AGENTIC_DEGRADED_NO_AUTHORITY = "AGENTIC_DEGRADED_NO_AUTHORITY"
AGENTIC_FAILED_NO_AUTHORITY = "AGENTIC_FAILED_NO_AUTHORITY"

ALL_AUTHORITY_STATUSES = frozenset({
    AGENTIC_APPROVE,
    AGENTIC_APPROVE_REDUCED,
    AGENTIC_WAIT_RECLAIM,
    AGENTIC_DEFER,
    AGENTIC_REJECT,
    AGENTIC_DEGRADED_NO_AUTHORITY,
    AGENTIC_FAILED_NO_AUTHORITY,
})

POSITIVE_AUTHORITY_STATUSES = frozenset({
    AGENTIC_APPROVE,
    AGENTIC_APPROVE_REDUCED,
})

# Mapping from shadow stance → positive authority status (only valid when session
# is COMPLETE, not degraded, stage_reached == 5, and pack_version is supported).
_STANCE_TO_AUTHORITY: Dict[str, str] = {
    "APPROVE":         AGENTIC_APPROVE,
    "APPROVE_REDUCED": AGENTIC_APPROVE_REDUCED,
    "WAIT_RECLAIM":    AGENTIC_WAIT_RECLAIM,
    "DEFER":           AGENTIC_DEFER,
    "DENY":            AGENTIC_REJECT,
}

# Authority modes.
AUTHORITY_MODE_AUTO_AUDIT = "AUTO_AUDIT"
AUTHORITY_MODE_OPERATOR_COMMITTED = "OPERATOR_COMMITTED"

ALL_AUTHORITY_MODES = frozenset({
    AUTHORITY_MODE_AUTO_AUDIT,
    AUTHORITY_MODE_OPERATOR_COMMITTED,
})


# ---------------------------------------------------------------------------
# Pack-version validation
# ---------------------------------------------------------------------------

def is_pack_version_supported(pack_version: Optional[str]) -> bool:
    """Explicit allow-list lookup. No tuple/string comparison."""
    if not pack_version:
        return False
    return pack_version in SUPPORTED_PACK_VERSIONS


# ---------------------------------------------------------------------------
# Mapping logic
# ---------------------------------------------------------------------------

def _coerce_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _coerce_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _normalize_session(session: Dict[str, Any]) -> Dict[str, Any]:
    """Lower-case keys we read so callers can pass either DB rows (UPPER) or
    fetch_shadow_session output (lower)."""
    if not isinstance(session, dict):
        return {}
    if "status" in session or "shadow_stance" in session:
        return session
    # Looks like an UPPER-case DB row; lower-case the fields we use.
    return {
        "session_id":         session.get("SESSION_ID"),
        "status":             session.get("STATUS"),
        "shadow_stance":      session.get("SHADOW_STANCE"),
        "shadow_confidence":  session.get("SHADOW_CONFIDENCE"),
        "stage_reached":      session.get("STAGE_REACHED"),
        "degraded":           session.get("DEGRADED"),
        "degraded_reason":    session.get("DEGRADED_REASON"),
        "pack_version":       session.get("PACK_VERSION"),
        "hearing_id":         session.get("HEARING_ID"),
        "proposal_id":        session.get("PROPOSAL_ID"),
        "evidence_pack_hash": session.get("EVIDENCE_PACK_HASH"),
        "created_at":         session.get("CREATED_AT"),
        "run_ms":             session.get("RUN_MS"),
        "chair":              session.get("chair"),
    }


def map_shadow_to_authority_status(
    session: Dict[str, Any],
    *,
    min_confidence_threshold: float,
) -> Tuple[str, str]:
    """
    Map a shadow session dict to a normalized AUTHORITY_STATUS.

    Returns (authority_status, reason_code). The reason_code names the rule
    that produced the status, which is useful for the AUTHORITY_REASON_CODE
    column and for debugging.

    Strictly fail-closed: any non-positive condition maps to a
    `_NO_AUTHORITY` status.
    """
    s = _normalize_session(session)

    status = (s.get("status") or "").upper()
    degraded = bool(s.get("degraded"))
    stance = (s.get("shadow_stance") or "").upper()
    confidence = _coerce_float(s.get("shadow_confidence"))
    pack_version = s.get("pack_version")
    stage_reached = _coerce_int(s.get("stage_reached"))

    if status != "COMPLETE":
        return AGENTIC_FAILED_NO_AUTHORITY, f"SHADOW_STATUS_{status or 'MISSING'}"

    if degraded:
        return AGENTIC_DEGRADED_NO_AUTHORITY, "SHADOW_DEGRADED"

    if stage_reached < 5:
        return AGENTIC_DEGRADED_NO_AUTHORITY, f"STAGE_REACHED_{stage_reached}"

    if not is_pack_version_supported(pack_version):
        return AGENTIC_DEGRADED_NO_AUTHORITY, f"PACK_VERSION_UNSUPPORTED_{pack_version or 'NONE'}"

    if stance not in _STANCE_TO_AUTHORITY:
        return AGENTIC_FAILED_NO_AUTHORITY, f"SHADOW_STANCE_UNKNOWN_{stance or 'NONE'}"

    mapped = _STANCE_TO_AUTHORITY[stance]

    # Confidence floor only applies to positive stances. WAIT_RECLAIM / DEFER /
    # REJECT are intentional verdicts; their confidence is not a quality gate.
    if mapped in POSITIVE_AUTHORITY_STATUSES and confidence < min_confidence_threshold:
        return AGENTIC_DEGRADED_NO_AUTHORITY, f"CONFIDENCE_BELOW_THRESHOLD_{confidence:.2f}"

    return mapped, f"SHADOW_COMPLETE_{stance}"


# ---------------------------------------------------------------------------
# Staleness checks
# ---------------------------------------------------------------------------

def _coerce_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def build_staleness_check(
    session: Dict[str, Any],
    *,
    current_pack_hash: Optional[str],
    max_age_minutes: int,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    Determine whether a shadow session is stale relative to the current
    COMMITTEE_HEARING evidence pack and a maximum age.

    Returns {is_stale, stale_reason, session_age_minutes}.
    """
    s = _normalize_session(session)

    age_minutes: Optional[int] = None
    created = _coerce_datetime(s.get("created_at"))
    if created is not None:
        reference = (now or datetime.now(timezone.utc))
        if reference.tzinfo is None:
            reference = reference.replace(tzinfo=timezone.utc)
        if created.tzinfo is None:
            created = created.replace(tzinfo=timezone.utc)
        delta = reference - created
        age_minutes = int(delta.total_seconds() // 60)

    # Hash mismatch is the strongest stale signal.
    session_hash = s.get("evidence_pack_hash")
    if current_pack_hash and session_hash and session_hash != current_pack_hash:
        return {
            "is_stale": True,
            "stale_reason": "EVIDENCE_PACK_HASH_MISMATCH",
            "session_age_minutes": age_minutes,
        }

    if age_minutes is not None and max_age_minutes >= 0 and age_minutes > max_age_minutes:
        return {
            "is_stale": True,
            "stale_reason": f"SESSION_AGE_{age_minutes}_GT_MAX_{max_age_minutes}",
            "session_age_minutes": age_minutes,
        }

    return {
        "is_stale": False,
        "stale_reason": None,
        "session_age_minutes": age_minutes,
    }


# ---------------------------------------------------------------------------
# Row builder
# ---------------------------------------------------------------------------

def _extract_chair_field(session: Dict[str, Any], key: str) -> Any:
    chair = session.get("chair") if isinstance(session.get("chair"), dict) else None
    if not chair:
        return None
    return chair.get(key)


def _extract_size_posture(session: Dict[str, Any]) -> Optional[str]:
    chair = session.get("chair") if isinstance(session.get("chair"), dict) else None
    if not chair:
        return None
    trade = chair.get("shadow_trade") if isinstance(chair.get("shadow_trade"), dict) else None
    if not trade:
        return None
    posture = trade.get("size_posture")
    return str(posture).upper() if posture else None


def build_authority_row(
    *,
    action_id: str,
    session: Dict[str, Any],
    c2_final_decision: Optional[Dict[str, Any]],
    config: Dict[str, Any],
    authority_mode: str,
    committed_by: str,
    current_pack_hash: Optional[str] = None,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    Assemble a single authority row dict ready for INSERT into
    MIP.APP.AGENTIC_REVALIDATION_AUTHORITY.

    `session` may be either a DB row dict (UPPER-case keys) or a
    `fetch_shadow_session` result (lower-case keys with nested `chair`).
    `c2_final_decision` is the matching `COMMITTEE_FINAL_DECISION` row or
    None when no deterministic baseline exists.
    `config` is a dict containing at least `AGENTIC_MIN_CONFIDENCE_THRESHOLD`
    and `AGENTIC_MAX_SESSION_AGE_MINUTES`.

    Returns a flat dict whose keys match `MIP.APP.AGENTIC_REVALIDATION_AUTHORITY`
    columns plus an internal `_authority_id` field. No DB writes happen here.
    """
    if authority_mode not in ALL_AUTHORITY_MODES:
        raise ValueError(
            f"authority_mode must be one of {sorted(ALL_AUTHORITY_MODES)}, got {authority_mode!r}"
        )

    s = _normalize_session(session)

    min_conf = _coerce_float(config.get("AGENTIC_MIN_CONFIDENCE_THRESHOLD"), default=0.40)
    max_age = _coerce_int(config.get("AGENTIC_MAX_SESSION_AGE_MINUTES"), default=240)

    stale = build_staleness_check(
        s,
        current_pack_hash=current_pack_hash,
        max_age_minutes=max_age,
        now=now,
    )

    # Map. Then force fail-closed if stale.
    authority_status, reason_code = map_shadow_to_authority_status(
        s, min_confidence_threshold=min_conf
    )
    if stale["is_stale"]:
        authority_status = AGENTIC_FAILED_NO_AUTHORITY
        reason_code = "STALE_" + (stale["stale_reason"] or "UNKNOWN")

    pack_version = s.get("pack_version")
    pack_version_ok = is_pack_version_supported(pack_version)

    c2_stance = None
    if c2_final_decision:
        c2_stance = (
            c2_final_decision.get("STANCE")
            or c2_final_decision.get("stance")
        )
        if c2_stance:
            c2_stance = str(c2_stance).upper()
    disagrees = None
    raw_stance = (s.get("shadow_stance") or "").upper() or None
    if c2_stance and raw_stance:
        disagrees = bool(c2_stance != raw_stance)

    authority_id = str(uuid.uuid4())

    payload = {
        "session": s,
        "c2_final_decision_stance": c2_stance,
        "computed_at": (now or datetime.now(timezone.utc)).isoformat(),
        "supported_pack_versions": sorted(SUPPORTED_PACK_VERSIONS),
        "min_confidence_threshold": min_conf,
        "max_session_age_minutes": max_age,
    }

    row = {
        "_authority_id":           authority_id,
        "AUTHORITY_ID":            authority_id,
        "ACTION_ID":               str(action_id),
        "PROPOSAL_ID":             _coerce_int(s.get("proposal_id")),
        "HEARING_ID":              s.get("hearing_id"),
        "SHADOW_SESSION_ID":       s.get("session_id"),
        "PACK_VERSION":            pack_version or "UNKNOWN",
        "PACK_VERSION_OK":         bool(pack_version_ok),

        "AUTHORITY_MODE":          authority_mode,
        "COMMITTED_BY":            committed_by,
        "IS_LATEST":               True,
        "SUPERSEDED_AT":           None,
        "SUPERSEDED_BY":           None,

        "AUTHORITY_STATUS":        authority_status,
        "AUTHORITY_REASON_CODE":   reason_code,
        "AUTHORITY_CONFIDENCE":    _coerce_float(s.get("shadow_confidence"), default=0.0),

        "SHADOW_STANCE_RAW":       raw_stance,
        "SHADOW_STATUS_RAW":       (s.get("status") or None),
        "SHADOW_STAGE_REACHED":    _coerce_int(s.get("stage_reached")),
        "SHADOW_DEGRADED":         bool(s.get("degraded")),
        "SHADOW_DEGRADED_REASON":  s.get("degraded_reason"),
        "SHADOW_PLURALITY_BASIS":  _extract_chair_field(s, "plurality_basis"),
        "SHADOW_SIZE_POSTURE":     _extract_size_posture(s),

        "DETERMINISTIC_BASELINE_STANCE": c2_stance,
        "DISAGREES_WITH_BASELINE":       disagrees,

        "IS_STALE":                bool(stale["is_stale"]),
        "STALE_REASON":            stale["stale_reason"],
        "SESSION_AGE_MINUTES":     stale["session_age_minutes"],

        "IS_OPERATOR_OVERRIDDEN":  False,
        "OVERRIDE_BY":             None,
        "OVERRIDE_AT":             None,
        "OVERRIDE_REASON":         None,
        "OVERRIDE_ORIGINAL_STATUS": None,

        "AUTHORITY_PAYLOAD_JSON":  json.dumps(payload, default=str),
    }
    return row


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

_INSERT_SQL = """
INSERT INTO MIP.APP.AGENTIC_REVALIDATION_AUTHORITY (
    AUTHORITY_ID, ACTION_ID, PROPOSAL_ID, HEARING_ID, SHADOW_SESSION_ID,
    PACK_VERSION, PACK_VERSION_OK,
    AUTHORITY_MODE, COMMITTED_BY, IS_LATEST,
    AUTHORITY_STATUS, AUTHORITY_REASON_CODE, AUTHORITY_CONFIDENCE,
    SHADOW_STANCE_RAW, SHADOW_STATUS_RAW, SHADOW_STAGE_REACHED,
    SHADOW_DEGRADED, SHADOW_DEGRADED_REASON, SHADOW_PLURALITY_BASIS, SHADOW_SIZE_POSTURE,
    DETERMINISTIC_BASELINE_STANCE, DISAGREES_WITH_BASELINE,
    IS_STALE, STALE_REASON, SESSION_AGE_MINUTES,
    IS_OPERATOR_OVERRIDDEN,
    AUTHORITY_PAYLOAD_JSON,
    CREATED_AT, UPDATED_AT
)
SELECT
    %s, %s, %s, %s, %s,
    %s, %s,
    %s, %s, %s,
    %s, %s, %s,
    %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s,
    %s, %s, %s,
    %s,
    PARSE_JSON(%s),
    CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
"""

_SUPERSEDE_SQL = """
UPDATE MIP.APP.AGENTIC_REVALIDATION_AUTHORITY
   SET IS_LATEST = FALSE,
       SUPERSEDED_AT = CURRENT_TIMESTAMP(),
       SUPERSEDED_BY = %s,
       UPDATED_AT = CURRENT_TIMESTAMP()
 WHERE ACTION_ID = %s
   AND IS_LATEST = TRUE
"""


def insert_authority_row_with_supersession(conn, row: Dict[str, Any]) -> str:
    """
    Transactionally supersede any existing IS_LATEST row for this ACTION_ID,
    then insert the new row.

    Args:
        conn: A Snowflake connector connection (or any DB-API compatible
              connection that supports `.cursor()`, `.commit()`, `.rollback()`).
        row:  Output of `build_authority_row`.

    Returns:
        The new AUTHORITY_ID.

    Raises on DB error; caller decides whether to swallow or propagate.
    """
    authority_id = row.get("AUTHORITY_ID") or row.get("_authority_id")
    if not authority_id:
        raise ValueError("row missing AUTHORITY_ID")
    action_id = row.get("ACTION_ID")
    if not action_id:
        raise ValueError("row missing ACTION_ID")

    cur = conn.cursor()
    try:
        cur.execute("BEGIN")
        cur.execute(_SUPERSEDE_SQL, (authority_id, action_id))
        cur.execute(
            _INSERT_SQL,
            (
                row["AUTHORITY_ID"],
                row["ACTION_ID"],
                row["PROPOSAL_ID"],
                row["HEARING_ID"],
                row["SHADOW_SESSION_ID"],
                row["PACK_VERSION"],
                row["PACK_VERSION_OK"],
                row["AUTHORITY_MODE"],
                row["COMMITTED_BY"],
                row["IS_LATEST"],
                row["AUTHORITY_STATUS"],
                row["AUTHORITY_REASON_CODE"],
                row["AUTHORITY_CONFIDENCE"],
                row["SHADOW_STANCE_RAW"],
                row["SHADOW_STATUS_RAW"],
                row["SHADOW_STAGE_REACHED"],
                row["SHADOW_DEGRADED"],
                row["SHADOW_DEGRADED_REASON"],
                row["SHADOW_PLURALITY_BASIS"],
                row["SHADOW_SIZE_POSTURE"],
                row["DETERMINISTIC_BASELINE_STANCE"],
                row["DISAGREES_WITH_BASELINE"],
                row["IS_STALE"],
                row["STALE_REASON"],
                row["SESSION_AGE_MINUTES"],
                row["IS_OPERATOR_OVERRIDDEN"],
                row["AUTHORITY_PAYLOAD_JSON"],
            ),
        )
        cur.execute("COMMIT")
    except Exception:
        try:
            cur.execute("ROLLBACK")
        except Exception:  # noqa: BLE001
            logger.exception("rollback failed after authority insert error")
        raise
    finally:
        try:
            cur.close()
        except Exception:  # noqa: BLE001
            pass

    return authority_id


# ---------------------------------------------------------------------------
# Stage 4b — auto-audit commit from a completed shadow session
# ---------------------------------------------------------------------------

# Config keys used by the auto-audit hook.
CONFIG_KEY_AUTO_AUDIT_ENABLED = "AGENTIC_AUTO_AUDIT_ENABLED"
CONFIG_KEY_MIN_CONFIDENCE = "AGENTIC_MIN_CONFIDENCE_THRESHOLD"
CONFIG_KEY_MAX_AGE = "AGENTIC_MAX_SESSION_AGE_MINUTES"


def _is_flag_true(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"true", "1", "yes", "y", "on"}


def is_auto_audit_enabled(conn) -> bool:
    """Return True only if APP_CONFIG.AGENTIC_AUTO_AUDIT_ENABLED is truthy.

    Read-only. Never raises (returns False on any error)."""
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT CONFIG_VALUE FROM MIP.APP.APP_CONFIG WHERE CONFIG_KEY = %s",
                (CONFIG_KEY_AUTO_AUDIT_ENABLED,),
            )
            row = cur.fetchone()
            return _is_flag_true(row[0]) if row else False
        finally:
            try:
                cur.close()
            except Exception:  # noqa: BLE001
                pass
    except Exception:  # noqa: BLE001
        logger.exception("is_auto_audit_enabled: config read failed")
        return False


def _fetch_authority_config_sync(cur) -> Dict[str, Any]:
    cur.execute(
        """
        SELECT CONFIG_KEY, CONFIG_VALUE
        FROM MIP.APP.APP_CONFIG
        WHERE CONFIG_KEY IN (%s, %s)
        """,
        (CONFIG_KEY_MIN_CONFIDENCE, CONFIG_KEY_MAX_AGE),
    )
    out: Dict[str, Any] = {}
    for row in cur.fetchall():
        out[row[0]] = row[1]
    return out


def _fetch_shadow_session_sync(cur, session_id: str) -> Optional[Dict[str, Any]]:
    cur.execute(
        """
        SELECT
            SESSION_ID, HEARING_ID, PROPOSAL_ID, SNAPSHOT_ID, EVIDENCE_PACK_HASH,
            SHADOW_STANCE, SHADOW_CONFIDENCE, STAGE_REACHED, STATUS,
            DEGRADED, DEGRADED_REASON, AGENT_MODEL, PACK_VERSION, RUN_MS,
            CREATED_AT, COMPLETED_AT
        FROM MIP.APP.SHADOW_BOARD_SESSION
        WHERE SESSION_ID = %s
        """,
        (session_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    cols = [d[0] for d in cur.description]
    return dict(zip(cols, row))


def _fetch_chair_ruling_sync(cur, session_id: str) -> Optional[Dict[str, Any]]:
    cur.execute(
        """
        SELECT
            SHADOW_STANCE, SHADOW_CONFIDENCE,
            PLURALITY_BASIS, CONFLICT_RESOLUTION,
            SHADOW_TRADE_JSON, TOP_SUPPORTS, TOP_TENSIONS,
            PARSE_OK, DEGRADED, DEGRADED_REASON, AGENT_ELAPSED_MS, CREATED_AT
        FROM MIP.APP.SHADOW_CHAIR_RULING
        WHERE SESSION_ID = %s
        """,
        (session_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    cols = [d[0] for d in cur.description]
    return dict(zip(cols, row))


def _fetch_c2_final_for_action_sync(cur, action_id: str) -> Optional[Dict[str, Any]]:
    cur.execute(
        """
        SELECT FINAL_DECISION_ID, HEARING_ID, PROPOSAL_ID, STANCE, CONFIDENCE,
               ACTION_ID, DECISION_TS
        FROM MIP.APP.COMMITTEE_FINAL_DECISION
        WHERE ACTION_ID = %s
        ORDER BY DECISION_TS DESC NULLS LAST
        LIMIT 1
        """,
        (action_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    cols = [d[0] for d in cur.description]
    return dict(zip(cols, row))


def _resolve_action_id_from_hearing_sync(cur, hearing_id: str) -> Optional[str]:
    """Fallback: find ACTION_ID for a hearing via COMMITTEE_FINAL_DECISION.

    Returns the most recent ACTION_ID bound to this hearing, or None when no
    action has been bound (e.g. pure diagnostic replay)."""
    cur.execute(
        """
        SELECT ACTION_ID
        FROM MIP.APP.COMMITTEE_FINAL_DECISION
        WHERE HEARING_ID = %s
          AND ACTION_ID IS NOT NULL
        ORDER BY DECISION_TS DESC NULLS LAST
        LIMIT 1
        """,
        (hearing_id,),
    )
    row = cur.fetchone()
    return row[0] if row else None


def _normalize_session_with_chair(
    session_row: Dict[str, Any],
    chair_row: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """Lower-case shape build_authority_row expects, with chair nested."""
    chair_dict: Optional[Dict[str, Any]] = None
    if chair_row:
        shadow_trade = chair_row.get("SHADOW_TRADE_JSON")
        if isinstance(shadow_trade, str):
            try:
                shadow_trade = json.loads(shadow_trade)
            except Exception:  # noqa: BLE001
                shadow_trade = {}
        chair_dict = {
            "shadow_stance": chair_row.get("SHADOW_STANCE"),
            "shadow_confidence": chair_row.get("SHADOW_CONFIDENCE"),
            "plurality_basis": chair_row.get("PLURALITY_BASIS"),
            "conflict_resolution": chair_row.get("CONFLICT_RESOLUTION"),
            "shadow_trade": shadow_trade if isinstance(shadow_trade, dict) else {},
            "parse_ok": chair_row.get("PARSE_OK"),
            "degraded": chair_row.get("DEGRADED"),
            "degraded_reason": chair_row.get("DEGRADED_REASON"),
        }

    return {
        "session_id":         session_row.get("SESSION_ID"),
        "status":             session_row.get("STATUS"),
        "shadow_stance":      session_row.get("SHADOW_STANCE"),
        "shadow_confidence":  session_row.get("SHADOW_CONFIDENCE"),
        "stage_reached":      session_row.get("STAGE_REACHED"),
        "degraded":           session_row.get("DEGRADED"),
        "degraded_reason":    session_row.get("DEGRADED_REASON"),
        "pack_version":       session_row.get("PACK_VERSION"),
        "hearing_id":         session_row.get("HEARING_ID"),
        "proposal_id":        session_row.get("PROPOSAL_ID"),
        "evidence_pack_hash": session_row.get("EVIDENCE_PACK_HASH"),
        "created_at":         session_row.get("CREATED_AT"),
        "run_ms":             session_row.get("RUN_MS"),
        "chair":              chair_dict,
    }


def commit_auto_audit_authority_for_session(
    conn,
    *,
    session_id: str,
    hearing_id: str,
    evidence_pack_hash: Optional[str] = None,
    action_id: Optional[str] = None,
    enforce_config_flag: bool = True,
) -> Dict[str, Any]:
    """
    Stage 4b — write one AUTO_AUDIT authority row for a completed shadow session.

    Best-effort: this function is the safe path for the shadow board finalize
    hook to call. It NEVER touches LIVE_ACTIONS. It NEVER changes Submit gating.
    It ONLY writes AUTHORITY_MODE = AUTO_AUDIT rows.

    Behavior:
    - If `enforce_config_flag` is True (default) and APP_CONFIG.AGENTIC_AUTO_AUDIT_ENABLED
      is not truthy, returns {"committed": False, "skip_reason": "DISABLED"} and writes nothing.
    - If `action_id` is None, attempts a fallback lookup via
      COMMITTEE_FINAL_DECISION.HEARING_ID. If still None, returns
      {"committed": False, "skip_reason": "NO_ACTION_MAPPED"}.
    - Otherwise builds a row via `build_authority_row` and inserts via
      `insert_authority_row_with_supersession`.

    Returns a small dict describing what happened. Callers should not rely on
    exceptions — failures are caught at the caller boundary.

    Raises only programmer errors (e.g. missing required args). Never raises on
    DB I/O — wraps and returns {"committed": False, "skip_reason": "ERROR", "error": "..."}.
    """
    if not session_id:
        raise ValueError("session_id is required")
    if not hearing_id:
        raise ValueError("hearing_id is required")

    try:
        cur = conn.cursor()
        try:
            if enforce_config_flag:
                cur.execute(
                    "SELECT CONFIG_VALUE FROM MIP.APP.APP_CONFIG WHERE CONFIG_KEY = %s",
                    (CONFIG_KEY_AUTO_AUDIT_ENABLED,),
                )
                row = cur.fetchone()
                if not row or not _is_flag_true(row[0]):
                    return {"committed": False, "skip_reason": "DISABLED"}

            resolved_action_id = action_id
            if not resolved_action_id:
                resolved_action_id = _resolve_action_id_from_hearing_sync(cur, hearing_id)

            if not resolved_action_id:
                return {"committed": False, "skip_reason": "NO_ACTION_MAPPED"}

            session_row = _fetch_shadow_session_sync(cur, session_id)
            if not session_row:
                return {
                    "committed": False,
                    "skip_reason": "SESSION_NOT_FOUND",
                    "session_id": session_id,
                }

            chair_row = _fetch_chair_ruling_sync(cur, session_id)
            c2_final = _fetch_c2_final_for_action_sync(cur, resolved_action_id)
            config = _fetch_authority_config_sync(cur)
        finally:
            try:
                cur.close()
            except Exception:  # noqa: BLE001
                pass

        normalized = _normalize_session_with_chair(session_row, chair_row)

        # Prefer the explicit pack hash from the caller (truth-of-record for the
        # current evidence pack); fall back to the session row's hash.
        pack_hash_for_staleness = evidence_pack_hash or session_row.get("EVIDENCE_PACK_HASH")

        authority_row = build_authority_row(
            action_id=resolved_action_id,
            session=normalized,
            c2_final_decision=c2_final,
            config=config,
            authority_mode=AUTHORITY_MODE_AUTO_AUDIT,
            committed_by="system_shadow_audit",
            current_pack_hash=pack_hash_for_staleness,
        )

        new_authority_id = insert_authority_row_with_supersession(conn, authority_row)

        return {
            "committed": True,
            "authority_id": new_authority_id,
            "action_id": resolved_action_id,
            "authority_mode": AUTHORITY_MODE_AUTO_AUDIT,
            "authority_status": authority_row["AUTHORITY_STATUS"],
            "authority_reason_code": authority_row["AUTHORITY_REASON_CODE"],
            "is_stale": authority_row["IS_STALE"],
            "shadow_stance_raw": authority_row["SHADOW_STANCE_RAW"],
            "disagrees_with_baseline": authority_row["DISAGREES_WITH_BASELINE"],
        }
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "commit_auto_audit_authority_for_session FAILED session=%s hearing=%s",
            session_id, hearing_id,
        )
        return {
            "committed": False,
            "skip_reason": "ERROR",
            "error": str(exc)[:300],
            "session_id": session_id,
            "hearing_id": hearing_id,
        }


__all__ = [
    "SUPPORTED_PACK_VERSIONS",
    "AGENTIC_APPROVE",
    "AGENTIC_APPROVE_REDUCED",
    "AGENTIC_WAIT_RECLAIM",
    "AGENTIC_DEFER",
    "AGENTIC_REJECT",
    "AGENTIC_DEGRADED_NO_AUTHORITY",
    "AGENTIC_FAILED_NO_AUTHORITY",
    "POSITIVE_AUTHORITY_STATUSES",
    "AUTHORITY_MODE_AUTO_AUDIT",
    "AUTHORITY_MODE_OPERATOR_COMMITTED",
    "CONFIG_KEY_AUTO_AUDIT_ENABLED",
    "CONFIG_KEY_MIN_CONFIDENCE",
    "CONFIG_KEY_MAX_AGE",
    "is_pack_version_supported",
    "is_auto_audit_enabled",
    "map_shadow_to_authority_status",
    "build_staleness_check",
    "build_authority_row",
    "insert_authority_row_with_supersession",
    "commit_auto_audit_authority_for_session",
]
