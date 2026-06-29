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
        "age_minutes_at_fetch": session.get("AGE_MINUTES_AT_FETCH"),
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

    Strictly fail-closed on incomplete or failed runs. When the chair has
    finished (stage 5) with a recognized stance, a specialist-level
    degradation (e.g. SYMBOL_BEHAVIOR JSON parse fail) does not void the
    chair verdict — the mapped stance is still authoritative.
    """
    s = _normalize_session(session)

    status = (s.get("status") or "").upper()
    degraded = bool(s.get("degraded"))
    stance = (s.get("shadow_stance") or "").upper()
    confidence = _coerce_float(s.get("shadow_confidence"))
    pack_version = s.get("pack_version")
    stage_reached = _coerce_int(s.get("stage_reached"))

    if status == "FAILED":
        return AGENTIC_FAILED_NO_AUTHORITY, "SHADOW_STATUS_FAILED"

    if status not in ("COMPLETE", "DEGRADED"):
        return AGENTIC_FAILED_NO_AUTHORITY, f"SHADOW_STATUS_{status or 'MISSING'}"

    if stage_reached < 5:
        return AGENTIC_DEGRADED_NO_AUTHORITY, f"STAGE_REACHED_{stage_reached}"

    # Incomplete chair / unknown stance on a degraded run — no usable verdict.
    if stance not in _STANCE_TO_AUTHORITY:
        if degraded:
            return AGENTIC_DEGRADED_NO_AUTHORITY, "SHADOW_DEGRADED"
        return AGENTIC_FAILED_NO_AUTHORITY, f"SHADOW_STANCE_UNKNOWN_{stance or 'NONE'}"

    if not is_pack_version_supported(pack_version):
        return AGENTIC_DEGRADED_NO_AUTHORITY, f"PACK_VERSION_UNSUPPORTED_{pack_version or 'NONE'}"

    mapped = _STANCE_TO_AUTHORITY[stance]

    # Confidence floor only applies to positive stances. WAIT_RECLAIM / DEFER /
    # REJECT are intentional verdicts; their confidence is not a quality gate.
    if mapped in POSITIVE_AUTHORITY_STATUSES and confidence < min_confidence_threshold:
        return AGENTIC_DEGRADED_NO_AUTHORITY, f"CONFIDENCE_BELOW_THRESHOLD_{confidence:.2f}"

    if degraded and status == "DEGRADED":
        return mapped, f"SHADOW_DEGRADED_CHAIR_{stance}"

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

    Timezone safety:
        Snowflake `TIMESTAMP_NTZ` columns (including `SHADOW_BOARD_SESSION.CREATED_AT`)
        store wall-clock values without offset info. Comparing a naïve datetime
        from Snowflake against `datetime.now(timezone.utc)` is incorrect when the
        Snowflake session TIMEZONE is non-UTC (e.g. Europe/Berlin), producing
        spurious negative `session_age_minutes`.

        The robust source for the age is therefore an in-Snowflake delta
        (`DATEDIFF(minute, CREATED_AT, CURRENT_TIMESTAMP())`) computed inside
        the same Snowflake session as the fetch. Callers that go through
        `_fetch_shadow_session_sync` get this for free via `age_minutes_at_fetch`.

        For synthetic callers (unit tests / direct dict input) the Python
        datetime path remains, but is clamped at `max(0, …)` so a tz-mismatch
        can never report a negative age. The `now` kwarg is honored for tests
        that inject a fixed reference time.
    """
    s = _normalize_session(session)

    age_minutes: Optional[int] = None

    # Preferred: DB-computed delta. Always timezone-agnostic because both
    # CREATED_AT and CURRENT_TIMESTAMP() are evaluated inside the same
    # Snowflake session, so any TIMEZONE param simply cancels out in the diff.
    db_age = s.get("age_minutes_at_fetch")
    if db_age is not None:
        try:
            age_minutes = int(db_age)
        except (TypeError, ValueError):
            age_minutes = None

    # Fallback: Python datetime arithmetic. Only used when a caller assembled
    # the session dict by hand (no `age_minutes_at_fetch`).
    if age_minutes is None:
        created = _coerce_datetime(s.get("created_at"))
        if created is not None:
            reference = (now or datetime.now(timezone.utc))
            # If both sides are naïve, compare naïvely (caller is responsible
            # for keeping them in the same frame). If only one side is aware,
            # promote the naïve side to UTC — this is the historical behavior
            # and is still wrong for non-UTC Snowflake sessions, but the
            # `max(0, …)` clamp below prevents a negative output.
            if reference.tzinfo is None and created.tzinfo is None:
                delta = reference - created
            else:
                if reference.tzinfo is None:
                    reference = reference.replace(tzinfo=timezone.utc)
                if created.tzinfo is None:
                    created = created.replace(tzinfo=timezone.utc)
                delta = reference - created
            age_minutes = int(delta.total_seconds() // 60)

    # Defensive clamp — `session_age_minutes` is documented as "minutes since
    # CREATED_AT" and must never be negative.
    if age_minutes is not None and age_minutes < 0:
        age_minutes = 0

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

# Stage 4c — operator-commit flag. Gates the POST .../agentic-authority/commit
# endpoint. Default false on initial deploy so we can flip it on after smoke.
# Stage 4d Submit-gating uses a separate AGENTIC_AUTHORITY_ENABLED flag.
CONFIG_KEY_OPERATOR_COMMIT_ENABLED = "AGENTIC_OPERATOR_COMMIT_ENABLED"

# Stage 4d — Submit-gating flag. When true, the agentic authority gate fires
# inside `submission_allowed` (LPA pending-decisions builder) and inside
# `execute_live_action`. Stage 4a deploys with this OFF.
CONFIG_KEY_AUTHORITY_GATE_ENABLED = "AGENTIC_AUTHORITY_ENABLED"

# Stage 4e — Primary-materializer flag. When true:
#   * `orchestrate_committee2_structural_entry` SKIPS the deterministic C2
#     `_materialize_structural_entry_committee_apply` call (no COMMITTEE_RUN /
#     COMMITTEE_VERDICT writes, no LIVE_ACTIONS.STATUS transition driven by C2).
#   * C2 still refreshes COMMITTEE_HEARING and writes COMMITTEE_FINAL_DECISION
#     because position health + history readers depend on them.
#   * Operator-commit endpoint, after writing the OPERATOR_COMMITTED authority
#     row, fires the agentic materializer which becomes the source of truth
#     for LIVE_ACTIONS.STATUS / PROPOSED_QTY / PROPOSED_PRICE / REASON_CODES.
CONFIG_KEY_PRIMARY_MATERIALIZATION_ENABLED = "AGENTIC_PRIMARY_MATERIALIZATION_ENABLED"

# Phase 5C — Auto-commit flag. When true (default), the shadow-board completion
# hook auto-promotes a clean APPROVE / APPROVE_REDUCED AUTO_AUDIT row to
# OPERATOR_COMMITTED so the operator does not have to click "Apply Agentic
# Review" for the happy path. Auto-promote fails closed on any non-approving
# stance, degraded session, unsupported pack version, staleness, hearing
# mismatch, or DB error. The operator can always re-commit / override via the
# explicit endpoint.
CONFIG_KEY_AUTO_COMMIT_ENABLED = "AGENTIC_AUTO_COMMIT_ENABLED"

# The actor identifier used when the auto-commit hook promotes AUTO_AUDIT to
# OPERATOR_COMMITTED. UIs and audit readers use the `system_auto_commit_`
# prefix to distinguish system-promoted authority from operator-clicked
# authority.
AUTO_COMMIT_ACTOR = "system_auto_commit_v1"

# Approving stances that the auto-commit hook is allowed to promote. Any other
# status (WAIT_RECLAIM, DEFER, REJECT, DEGRADED, FAILED, ...) must remain in
# AUTO_AUDIT and require an explicit operator click to override.
AUTO_COMMIT_APPROVING_STATUSES = frozenset({
    "AGENTIC_APPROVE",
    "AGENTIC_APPROVE_REDUCED",
})


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
    """
    Fetch a single SHADOW_BOARD_SESSION row plus an in-Snowflake age delta.

    `AGE_MINUTES_AT_FETCH = DATEDIFF('minute', CREATED_AT, CURRENT_TIMESTAMP())`
    is computed Snowflake-side, in the same session as the read, so it is
    timezone-agnostic — both ends of the diff are evaluated in whatever
    session TIMEZONE is configured and the offset cancels out. This is the
    canonical source for staleness/freshness math; downstream Python code
    no longer has to reason about the wall-clock semantics of `CREATED_AT`
    (which is a naïve `TIMESTAMP_NTZ`).
    """
    cur.execute(
        """
        SELECT
            SESSION_ID, HEARING_ID, PROPOSAL_ID, SNAPSHOT_ID, EVIDENCE_PACK_HASH,
            SHADOW_STANCE, SHADOW_CONFIDENCE, STAGE_REACHED, STATUS,
            DEGRADED, DEGRADED_REASON, AGENT_MODEL, PACK_VERSION, RUN_MS,
            CREATED_AT, COMPLETED_AT,
            DATEDIFF('minute', CREATED_AT, CURRENT_TIMESTAMP()) AS AGE_MINUTES_AT_FETCH
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
    """Phase 5B: legacy deterministic baseline stance lookup.

    Returns the most recent COMMITTEE_FINAL_DECISION row for the action, or
    None when no row exists. Under Phase 5B+ the orchestrate path no longer
    writes COMMITTEE_FINAL_DECISION, so this returns None for actions that
    were revalidated under the agentic-only path. `build_authority_row`
    already handles None by setting DETERMINISTIC_BASELINE_STANCE and
    DISAGREES_WITH_BASELINE to None. This function is kept (rather than
    deleted) so historical CFD rows can still inform the baseline diff for
    actions that pre-date Phase 5B; it is NOT a runtime dependency for the
    Phase 5B operator path."""
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
    """Phase 5B: agentic-native hearing -> action mapping.

    Resolution order:
      1. AGENTIC_REVALIDATION_AUTHORITY by HEARING_ID (latest row).
         This is the agentic-primary anchor: every shadow session that
         completed via the AUTO_AUDIT hook writes its mapping here.
      2. LIVE_ACTIONS joined to COMMITTEE_HEARING by PROPOSAL_ID. CH is
         still written as an evidence container under Phase 5B, so this
         is the bootstrap fallback for cases where no authority row
         exists yet (first revalidation of an action).
      3. Legacy COMMITTEE_FINAL_DECISION lookup. Kept ONLY so that
         actions migrated mid-flight from the pre-Phase-5B era still
         resolve; it is NEVER a runtime dependency for new actions.

    Returns None when no mapping can be found via any path."""
    cur.execute(
        """
        SELECT ACTION_ID
        FROM MIP.APP.AGENTIC_REVALIDATION_AUTHORITY
        WHERE HEARING_ID = %s
          AND ACTION_ID IS NOT NULL
        ORDER BY CREATED_AT DESC NULLS LAST
        LIMIT 1
        """,
        (hearing_id,),
    )
    row = cur.fetchone()
    if row and row[0]:
        return row[0]

    cur.execute(
        """
        SELECT la.ACTION_ID
        FROM MIP.APP.COMMITTEE_HEARING ch
        JOIN MIP.LIVE.LIVE_ACTIONS la
          ON la.PROPOSAL_ID = ch.PROPOSAL_ID
        WHERE ch.HEARING_ID = %s
          AND la.ACTION_ID IS NOT NULL
        ORDER BY la.UPDATED_AT DESC NULLS LAST
        LIMIT 1
        """,
        (hearing_id,),
    )
    row = cur.fetchone()
    if row and row[0]:
        return row[0]

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
        # In-Snowflake age delta — timezone-agnostic. See
        # `_fetch_shadow_session_sync` and `build_staleness_check` for details.
        "age_minutes_at_fetch": session_row.get("AGE_MINUTES_AT_FETCH"),
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
            # Phase 5C: surface pack_version_ok + degraded so the auto-commit
            # hook can decide whether this AUTO_AUDIT row is safe to promote.
            "pack_version_ok": authority_row["PACK_VERSION_OK"],
            "pack_version": authority_row["PACK_VERSION"],
            "shadow_degraded": bool(normalized.get("DEGRADED")) if normalized else False,
            "session_id": session_id,
            "hearing_id": hearing_id,
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


# ---------------------------------------------------------------------------
# Phase 5C — auto-commit hook (system promotes AUTO_AUDIT -> OPERATOR_COMMITTED
# for clean APPROVE / APPROVE_REDUCED verdicts so the operator does not need to
# click "Apply Agentic Review" for the happy path)
# ---------------------------------------------------------------------------


def is_auto_commit_enabled(conn) -> bool:
    """Return True only if APP_CONFIG.AGENTIC_AUTO_COMMIT_ENABLED is truthy.

    Read-only. Never raises (returns False on any error)."""
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT CONFIG_VALUE FROM MIP.APP.APP_CONFIG WHERE CONFIG_KEY = %s",
                (CONFIG_KEY_AUTO_COMMIT_ENABLED,),
            )
            row = cur.fetchone()
            if not row:
                return False
            return _is_flag_true(row[0])
        finally:
            try:
                cur.close()
            except Exception:  # noqa: BLE001
                pass
    except Exception:  # noqa: BLE001
        return False


def _auto_promote_eligible(audit_result: Dict[str, Any]) -> Tuple[bool, str]:
    """Return (eligible, reason) for whether an AUTO_AUDIT row should be
    promoted to OPERATOR_COMMITTED by the system.

    Fail-closed: ANY non-approving status, ANY staleness, ANY pack-version
    failure, ANY degraded shadow, ANY missing field — returns (False, reason).
    """
    if not audit_result.get("committed"):
        return False, "AUDIT_NOT_COMMITTED"
    status = (audit_result.get("authority_status") or "").upper().strip()
    if status not in AUTO_COMMIT_APPROVING_STATUSES:
        return False, f"NON_APPROVING_STATUS:{status or 'EMPTY'}"
    if audit_result.get("is_stale"):
        return False, "AUDIT_IS_STALE"
    # Pack version check: build_authority_row puts the supported-version
    # decision in PACK_VERSION_OK. False means the audit row exists but the
    # pack version is outside the supported set; never promote those.
    if audit_result.get("pack_version_ok") is False:
        return False, "PACK_VERSION_UNSUPPORTED"
    if audit_result.get("shadow_degraded"):
        return False, "SHADOW_DEGRADED"
    if not audit_result.get("action_id"):
        return False, "NO_ACTION_ID"
    if not audit_result.get("session_id"):
        return False, "NO_SESSION_ID"
    if not audit_result.get("hearing_id"):
        return False, "NO_HEARING_ID"
    return True, "ELIGIBLE"


def auto_promote_authority_to_operator_committed(
    conn,
    audit_result: Dict[str, Any],
    *,
    enforce_config_flag: bool = True,
) -> Dict[str, Any]:
    """Phase 5C — promote a freshly-written AUTO_AUDIT row to OPERATOR_COMMITTED
    when the system can do so safely without operator interaction.

    Strict eligibility (see `_auto_promote_eligible`): APPROVE / APPROVE_REDUCED
    only, not stale, pack version supported, not degraded. Anything else stays
    in AUTO_AUDIT and the operator must explicitly click "Apply Agentic
    Review" (or equivalent override path).

    Calls into `commit_operator_authority_for_session` so the downstream
    hearing-mismatch / session-not-found / staleness gates remain authoritative.
    Uses `AUTO_COMMIT_ACTOR` as the committer.

    Returns a dict with shape compatible with `commit_operator_authority_for_session`,
    plus an `auto_promoted` boolean and a `skip_reason` when no-op.
    Never raises on DB I/O — wraps and logs.
    """
    eligible, reason = _auto_promote_eligible(audit_result)
    if not eligible:
        return {
            "committed": False,
            "auto_promoted": False,
            "skip_reason": reason,
            "action_id": audit_result.get("action_id"),
        }

    if enforce_config_flag and not is_auto_commit_enabled(conn):
        return {
            "committed": False,
            "auto_promoted": False,
            "skip_reason": "DISABLED",
            "action_id": audit_result.get("action_id"),
        }

    try:
        result = commit_operator_authority_for_session(
            conn,
            action_id=str(audit_result["action_id"]),
            shadow_session_id=str(audit_result["session_id"]),
            hearing_id=str(audit_result["hearing_id"]),
            committed_by=AUTO_COMMIT_ACTOR,
            enforce_config_flag=True,
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "auto_promote: commit_operator_authority_for_session raised action=%s",
            audit_result.get("action_id"),
        )
        return {
            "committed": False,
            "auto_promoted": False,
            "skip_reason": "ERROR",
            "error": str(exc)[:300],
            "action_id": audit_result.get("action_id"),
        }

    if result.get("committed"):
        result["auto_promoted"] = True
    else:
        result.setdefault("auto_promoted", False)
    return result


# ---------------------------------------------------------------------------
# Stage 4c — operator-committed authority
# ---------------------------------------------------------------------------

def is_operator_commit_enabled(conn) -> bool:
    """Return True only if APP_CONFIG.AGENTIC_OPERATOR_COMMIT_ENABLED is truthy.

    Read-only. Never raises (returns False on any error). This flag gates the
    operator-facing POST .../agentic-authority/commit endpoint — independently
    from AGENTIC_AUTO_AUDIT_ENABLED (Stage 4b) and AGENTIC_AUTHORITY_ENABLED
    (Stage 4d Submit gating)."""
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT CONFIG_VALUE FROM MIP.APP.APP_CONFIG WHERE CONFIG_KEY = %s",
                (CONFIG_KEY_OPERATOR_COMMIT_ENABLED,),
            )
            row = cur.fetchone()
            return _is_flag_true(row[0]) if row else False
        finally:
            try:
                cur.close()
            except Exception:  # noqa: BLE001
                pass
    except Exception:  # noqa: BLE001
        logger.exception("is_operator_commit_enabled: config read failed")
        return False


def _fetch_current_hearing_id_for_action_sync(cur, action_id: str) -> Optional[str]:
    """Phase 5B: agentic-native current-hearing anchor for an action.

    Resolution order (each step independent of deterministic C2 writes):

      1. AGENTIC_REVALIDATION_AUTHORITY by ACTION_ID + IS_LATEST=TRUE.
         Under Phase 5B every shadow session completion writes an AUTO_AUDIT
         row that becomes IS_LATEST; the operator commit path reads the
         hearing identity from here.
      2. COMMITTEE_HEARING joined via LIVE_ACTIONS.PROPOSAL_ID. CH is still
         written as the evidence-only container by the orchestrate path,
         so this is the bootstrap fallback when no authority row exists
         yet (first revalidation of an action, before the shadow finalize
         hook has fired).
      3. Legacy COMMITTEE_FINAL_DECISION. Retained ONLY so actions migrated
         mid-flight from the pre-Phase-5B era still resolve. New Phase 5B
         actions never reach this branch because step 2 always finds a CH
         row written by the agentic-only orchestrate path.

    Returns None when none of the three anchors resolves — caller must fail
    closed (do NOT silently approve)."""
    cur.execute(
        """
        SELECT HEARING_ID
        FROM MIP.APP.AGENTIC_REVALIDATION_AUTHORITY
        WHERE ACTION_ID = %s
          AND IS_LATEST = TRUE
        ORDER BY CREATED_AT DESC NULLS LAST
        LIMIT 1
        """,
        (action_id,),
    )
    row = cur.fetchone()
    if row and row[0]:
        return row[0]

    cur.execute(
        """
        SELECT ch.HEARING_ID
        FROM MIP.LIVE.LIVE_ACTIONS la
        JOIN MIP.APP.COMMITTEE_HEARING ch
          ON ch.PROPOSAL_ID = la.PROPOSAL_ID
        WHERE la.ACTION_ID = %s
        ORDER BY ch.UPDATED_AT DESC NULLS LAST
        LIMIT 1
        """,
        (action_id,),
    )
    row = cur.fetchone()
    if row and row[0]:
        return row[0]

    cur.execute(
        """
        SELECT HEARING_ID
        FROM MIP.APP.COMMITTEE_FINAL_DECISION
        WHERE ACTION_ID = %s
        ORDER BY DECISION_TS DESC NULLS LAST
        LIMIT 1
        """,
        (action_id,),
    )
    row = cur.fetchone()
    return row[0] if row else None


def _fetch_current_pack_hash_for_hearing_sync(cur, hearing_id: str) -> Optional[str]:
    """Phase 5B: agentic-native evidence-pack-hash lookup.

    Resolution order:
      1. COMMITTEE_HEARING.EVIDENCE_PACK_HASH — still the canonical
         per-proposal pack hash. Under Phase 5B this column is written by
         the new evidence-only orchestrate path (no deterministic chair
         needed). CH is the "container of last resort" — the user
         explicitly allows this as historical/container-only storage.
      2. SHADOW_BOARD_SESSION.EVIDENCE_PACK_HASH — the latest shadow
         session bound to this hearing carries its own copy of the pack
         hash. Used as the agentic-native fallback when the CH row is
         missing or has a NULL EVIDENCE_PACK_HASH (e.g. mid-migration
         actions).

    Returns None when neither lookup yields a hash."""
    cur.execute(
        "SELECT EVIDENCE_PACK_HASH FROM MIP.APP.COMMITTEE_HEARING WHERE HEARING_ID = %s",
        (hearing_id,),
    )
    row = cur.fetchone()
    if row and row[0]:
        return row[0]

    cur.execute(
        """
        SELECT EVIDENCE_PACK_HASH
        FROM MIP.APP.SHADOW_BOARD_SESSION
        WHERE HEARING_ID = %s
          AND EVIDENCE_PACK_HASH IS NOT NULL
        ORDER BY CREATED_AT DESC NULLS LAST
        LIMIT 1
        """,
        (hearing_id,),
    )
    row = cur.fetchone()
    return row[0] if row else None


# Skip-reason codes used by the operator commit helper. API layer maps these
# to HTTP responses.
SKIP_DISABLED = "DISABLED"
SKIP_OVERRIDE_NOT_ENABLED = "OVERRIDE_NOT_ENABLED"
SKIP_SESSION_NOT_FOUND = "SESSION_NOT_FOUND"
SKIP_HEARING_MISMATCH = "HEARING_MISMATCH"
SKIP_ACTION_HEARING_MISSING = "ACTION_HEARING_MISSING"
SKIP_EVIDENCE_PACK_STALE = "EVIDENCE_PACK_STALE"
SKIP_ERROR = "ERROR"


def commit_operator_authority_for_session(
    conn,
    *,
    action_id: str,
    shadow_session_id: str,
    hearing_id: str,
    committed_by: str,
    override_reason: Optional[str] = None,
    enforce_config_flag: bool = True,
) -> Dict[str, Any]:
    """
    Stage 4c — write one OPERATOR_COMMITTED authority row for a completed
    shadow session, triggered by an explicit operator click ("Apply Agentic
    Review").

    Differences from the AUTO_AUDIT helper:
      - Strict input validation (returns structured skip_reason so the API
        layer can map to HTTP 4xx). Never silently swallows.
      - The operator-supplied (shadow_session_id, hearing_id) MUST match each
        other AND must match the action's current COMMITTEE_FINAL_DECISION
        hearing — guards against committing a verdict that belongs to a
        different action or a stale hearing iteration.
      - `committed_by` is the operator/actor identifier from the request, not
        a system constant.
      - `override_reason` is reserved for Stage 4d+. In Stage 4c any non-null
        override_reason returns skip_reason='OVERRIDE_NOT_ENABLED'.
      - LIVE_ACTIONS / Submit gating are NOT touched. The new row simply
        becomes IS_LATEST=TRUE for the action, ready to be read by Stage 4d
        gating once that's enabled.

    Returns:
      On success:
        {
          "committed": True,
          "authority_id": "<uuid>",
          "action_id": ...,
          "authority_mode": "OPERATOR_COMMITTED",
          "authority_status": ...,
          "authority_reason_code": ...,
          "authority_confidence": ...,
          "is_stale": bool,
          "shadow_stance_raw": ...,
          "deterministic_baseline_stance": ...,
          "disagrees_with_baseline": bool|None,
          "pack_version": ...,
          "pack_version_ok": bool,
          "session_age_minutes": int|None,
          "superseded_authority_id": "<uuid>"|None,
        }
      On failure:
        {"committed": False, "skip_reason": "<CODE>", ...optional context...}

    Raises only on programmer errors (missing required args).
    """
    if not action_id:
        raise ValueError("action_id is required")
    if not shadow_session_id:
        raise ValueError("shadow_session_id is required")
    if not hearing_id:
        raise ValueError("hearing_id is required")
    if not committed_by:
        raise ValueError("committed_by is required")

    # Stage 4c does not implement operator override. Stage 4d+ will.
    if override_reason is not None and str(override_reason).strip() != "":
        return {
            "committed": False,
            "skip_reason": SKIP_OVERRIDE_NOT_ENABLED,
            "message": "Operator override is reserved for Stage 4d+; commit without override_reason.",
        }

    try:
        cur = conn.cursor()
        try:
            if enforce_config_flag:
                cur.execute(
                    "SELECT CONFIG_VALUE FROM MIP.APP.APP_CONFIG WHERE CONFIG_KEY = %s",
                    (CONFIG_KEY_OPERATOR_COMMIT_ENABLED,),
                )
                row = cur.fetchone()
                if not row or not _is_flag_true(row[0]):
                    return {
                        "committed": False,
                        "skip_reason": SKIP_DISABLED,
                        "message": (
                            f"APP_CONFIG.{CONFIG_KEY_OPERATOR_COMMIT_ENABLED} is not enabled. "
                            "Stage 4c operator commit is currently disabled."
                        ),
                    }

            session_row = _fetch_shadow_session_sync(cur, shadow_session_id)
            if not session_row:
                return {
                    "committed": False,
                    "skip_reason": SKIP_SESSION_NOT_FOUND,
                    "shadow_session_id": shadow_session_id,
                }

            session_hearing_id = session_row.get("HEARING_ID")
            if session_hearing_id and str(session_hearing_id) != str(hearing_id):
                return {
                    "committed": False,
                    "skip_reason": SKIP_HEARING_MISMATCH,
                    "message": (
                        "shadow_session.hearing_id does not match the hearing_id supplied with the request."
                    ),
                    "session_hearing_id": str(session_hearing_id),
                    "request_hearing_id": str(hearing_id),
                }

            current_hearing_for_action = _fetch_current_hearing_id_for_action_sync(cur, action_id)
            if current_hearing_for_action and str(current_hearing_for_action) != str(hearing_id):
                # The action has moved on to a newer hearing iteration (intraday
                # re-run). Refuse to commit a verdict bound to an older hearing.
                return {
                    "committed": False,
                    "skip_reason": SKIP_HEARING_MISMATCH,
                    "message": (
                        "Action has been re-orchestrated; the supplied hearing_id is not the action's "
                        "current hearing. Run a fresh Intelligence Review and commit the new verdict."
                    ),
                    "action_current_hearing_id": str(current_hearing_for_action),
                    "request_hearing_id": str(hearing_id),
                }
            if not current_hearing_for_action:
                return {
                    "committed": False,
                    "skip_reason": SKIP_ACTION_HEARING_MISSING,
                    "message": (
                        "No agentic authority anchor found for this action. "
                        "Run Intelligence Review first to build an evidence "
                        "dossier and run the Agentic Committee."
                    ),
                }

            chair_row = _fetch_chair_ruling_sync(cur, shadow_session_id)
            c2_final = _fetch_c2_final_for_action_sync(cur, action_id)
            config = _fetch_authority_config_sync(cur)

            # Phase 5B: pack-hash truth-of-record is sourced from the agentic-
            # native lookup chain (COMMITTEE_HEARING evidence container, then
            # SHADOW_BOARD_SESSION as the agentic fallback). This catches the
            # case where the operator is committing a session that was built
            # against an older pack hash.
            current_pack_hash = _fetch_current_pack_hash_for_hearing_sync(cur, hearing_id)
        finally:
            try:
                cur.close()
            except Exception:  # noqa: BLE001
                pass

        normalized = _normalize_session_with_chair(session_row, chair_row)

        authority_row = build_authority_row(
            action_id=action_id,
            session=normalized,
            c2_final_decision=c2_final,
            config=config,
            authority_mode=AUTHORITY_MODE_OPERATOR_COMMITTED,
            committed_by=committed_by,
            current_pack_hash=current_pack_hash,
        )

        # Look up the row we are about to supersede so callers can see the
        # prior authority_id in the response.
        prior_authority_id: Optional[str] = None
        try:
            tmp_cur = conn.cursor()
            try:
                tmp_cur.execute(
                    """
                    SELECT AUTHORITY_ID
                    FROM MIP.APP.AGENTIC_REVALIDATION_AUTHORITY
                    WHERE ACTION_ID = %s AND IS_LATEST = TRUE
                    """,
                    (action_id,),
                )
                row = tmp_cur.fetchone()
                if row:
                    prior_authority_id = row[0]
            finally:
                try:
                    tmp_cur.close()
                except Exception:  # noqa: BLE001
                    pass
        except Exception:  # noqa: BLE001
            logger.exception("commit_operator_authority: prior-row lookup failed (non-fatal)")

        new_authority_id = insert_authority_row_with_supersession(conn, authority_row)

        return {
            "committed": True,
            "authority_id": new_authority_id,
            "action_id": action_id,
            "authority_mode": AUTHORITY_MODE_OPERATOR_COMMITTED,
            "authority_status": authority_row["AUTHORITY_STATUS"],
            "authority_reason_code": authority_row["AUTHORITY_REASON_CODE"],
            "authority_confidence": authority_row["AUTHORITY_CONFIDENCE"],
            "is_stale": authority_row["IS_STALE"],
            "stale_reason": authority_row["STALE_REASON"],
            "shadow_stance_raw": authority_row["SHADOW_STANCE_RAW"],
            "shadow_size_posture": authority_row.get("SHADOW_SIZE_POSTURE"),
            "deterministic_baseline_stance": authority_row["DETERMINISTIC_BASELINE_STANCE"],
            "disagrees_with_baseline": authority_row["DISAGREES_WITH_BASELINE"],
            "pack_version": authority_row["PACK_VERSION"],
            "pack_version_ok": authority_row["PACK_VERSION_OK"],
            "session_age_minutes": authority_row["SESSION_AGE_MINUTES"],
            "superseded_authority_id": prior_authority_id,
            "committed_by": committed_by,
        }
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "commit_operator_authority_for_session FAILED action=%s session=%s hearing=%s",
            action_id, shadow_session_id, hearing_id,
        )
        return {
            "committed": False,
            "skip_reason": SKIP_ERROR,
            "error": str(exc)[:300],
            "action_id": action_id,
            "shadow_session_id": shadow_session_id,
            "hearing_id": hearing_id,
        }


# ---------------------------------------------------------------------------
# Stage 4d — Submit gating evaluation
# ---------------------------------------------------------------------------

# Reason-code taxonomy emitted by `evaluate_authority_gate`. These strings
# flow into `submission_gate_hints` (LPA) and `reason_codes` (execute_live_action)
# and are stable identifiers — UI / clients may switch on them.
GATE_REASON_NOT_COMMITTED       = "AGENTIC_AUTHORITY_NOT_COMMITTED"
GATE_REASON_STALE               = "AGENTIC_AUTHORITY_STALE"
GATE_REASON_BLOCKED_WAIT_RECLAIM = "AGENTIC_AUTHORITY_BLOCKED_WAIT_RECLAIM"
GATE_REASON_BLOCKED_DEFER       = "AGENTIC_AUTHORITY_BLOCKED_DEFER"
GATE_REASON_BLOCKED_REJECT      = "AGENTIC_AUTHORITY_BLOCKED_REJECT"
GATE_REASON_DEGRADED            = "AGENTIC_AUTHORITY_DEGRADED"
GATE_REASON_FAILED              = "AGENTIC_AUTHORITY_FAILED"
GATE_REASON_UNKNOWN_STATUS      = "AGENTIC_AUTHORITY_UNKNOWN_STATUS"
# Stage 4f — used when the authority row cannot be evaluated due to a DB /
# infra error AND agentic-primary materialization is the intended mode.
# This is the explicit fail-closed reason code.
GATE_REASON_EVAL_ERROR          = "AGENTIC_AUTHORITY_EVAL_ERROR"

# Human-readable submit-button tooltips per design table (LPA UX).
_GATE_TOOLTIP_BY_REASON: Dict[str, str] = {
    GATE_REASON_NOT_COMMITTED:        "Agentic review not committed — apply review first",
    GATE_REASON_STALE:                "Agentic authority stale — re-commit after new review",
    GATE_REASON_EVAL_ERROR:           "Agentic authority could not be evaluated — submit blocked (fail-closed)",
    GATE_REASON_BLOCKED_WAIT_RECLAIM: "Agentic board: Wait / Reclaim — submit blocked",
    GATE_REASON_BLOCKED_DEFER:        "Agentic board: Defer — submit blocked",
    GATE_REASON_BLOCKED_REJECT:       "Agentic board: Reject — submit blocked",
    GATE_REASON_DEGRADED:             "Agentic review degraded — run new review",
    GATE_REASON_FAILED:               "Agentic review unavailable — run new review",
    GATE_REASON_UNKNOWN_STATUS:       "Agentic review unrecognized — run new review",
}

_STATUS_TO_BLOCK_REASON: Dict[str, str] = {
    AGENTIC_WAIT_RECLAIM:           GATE_REASON_BLOCKED_WAIT_RECLAIM,
    AGENTIC_DEFER:                  GATE_REASON_BLOCKED_DEFER,
    AGENTIC_REJECT:                 GATE_REASON_BLOCKED_REJECT,
    AGENTIC_DEGRADED_NO_AUTHORITY:  GATE_REASON_DEGRADED,
    AGENTIC_FAILED_NO_AUTHORITY:    GATE_REASON_FAILED,
}


# ---------------------------------------------------------------------------
# Agentic reason-code sync (LPA display / LIVE_ACTIONS persistence)
# ---------------------------------------------------------------------------

_AGENTIC_SYNC_STRIP_EXACT = frozenset({
    "STRUCTURAL_AGENTIC_REVIEWED",
    "AGENTIC_SIZE_POSTURE_REDUCED",
})


def is_agentic_authority_sync_reason_code(code: str) -> bool:
    """True for agentic tags owned by the latest authority row (safe to replace)."""
    u = str(code or "").strip().upper()
    if not u:
        return False
    if u in _AGENTIC_SYNC_STRIP_EXACT:
        return True
    return u.startswith("AGENTIC_AUTHORITY_")


def _dedupe_reason_codes(reason_codes: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for rc in reason_codes or []:
        text = str(rc).strip()
        if text and text not in seen:
            seen.add(text)
            out.append(text)
    return out


def reconcile_agentic_authority_reason_codes(
    reason_codes: list[str],
    *,
    authority_status: str | None,
    is_stale: bool,
    gate_ok: bool | None = None,
) -> list[str]:
    """Replace stale agentic tags with tags matching the latest authority truth."""
    kept = [
        str(rc)
        for rc in (reason_codes or [])
        if not is_agentic_authority_sync_reason_code(rc)
    ]
    status = str(authority_status or "").upper().strip()
    if not status:
        return _dedupe_reason_codes(kept)

    positive = status in POSITIVE_AUTHORITY_STATUSES and not is_stale
    if gate_ok is None:
        gate_ok = positive
    elif gate_ok and not positive:
        gate_ok = False

    if gate_ok:
        kept.append("STRUCTURAL_AGENTIC_REVIEWED")
        kept.append(f"AGENTIC_AUTHORITY_{status}")
        if status == AGENTIC_APPROVE_REDUCED:
            kept.append("AGENTIC_SIZE_POSTURE_REDUCED")
    else:
        kept.append(f"AGENTIC_AUTHORITY_{status}")
        if is_stale:
            kept.append(GATE_REASON_STALE)

    return _dedupe_reason_codes(kept)


def sync_live_action_agentic_reason_codes(
    conn,
    action_id: str,
    *,
    authority_status: str | None,
    is_stale: bool,
    gate_ok: bool | None = None,
) -> list[str] | None:
    """Persist reconciled agentic tags onto LIVE_ACTIONS.REASON_CODES."""
    from app.routers.live import _fetch_live_action, _parse_list_variant, _write_reason_codes

    cur = conn.cursor()
    try:
        action = _fetch_live_action(cur, action_id)
        if not action:
            return None
        existing = _parse_list_variant(action.get("REASON_CODES"))
        merged = reconcile_agentic_authority_reason_codes(
            existing,
            authority_status=authority_status,
            is_stale=is_stale,
            gate_ok=gate_ok,
        )
        _write_reason_codes(cur, action_id, merged)
        return merged
    finally:
        try:
            cur.close()
        except Exception:  # noqa: BLE001
            pass


def is_authority_gate_enabled(conn) -> bool:
    """Return True only if APP_CONFIG.AGENTIC_AUTHORITY_ENABLED is truthy.

    This is the Stage 4d Submit-gating circuit breaker. When False the
    `evaluate_authority_gate` consumer paths in `live.py` must short-circuit
    to gate_ok=True so behavior is identical to pre-Stage-4d.

    Read-only. Never raises (returns False on any error)."""
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT CONFIG_VALUE FROM MIP.APP.APP_CONFIG WHERE CONFIG_KEY = %s",
                (CONFIG_KEY_AUTHORITY_GATE_ENABLED,),
            )
            row = cur.fetchone()
            return _is_flag_true(row[0]) if row else False
        finally:
            try:
                cur.close()
            except Exception:  # noqa: BLE001
                pass
    except Exception:  # noqa: BLE001
        logger.exception("is_authority_gate_enabled: config read failed")
        return False


def is_agentic_primary_materialization_enabled(conn) -> bool:
    """Return True only if APP_CONFIG.AGENTIC_PRIMARY_MATERIALIZATION_ENABLED
    is truthy.

    Stage 4e circuit breaker. When False the orchestrate path remains
    pre-Stage-4e: C2 owns the LIVE_ACTIONS materialization. When True, C2
    materialize is skipped and the operator-commit endpoint becomes the
    source of truth for LIVE_ACTIONS.STATUS / sizing.

    Read-only. Never raises (returns False on any error). For
    fail-CLOSED behavior at deterministic-materialization sites, use
    `should_block_deterministic_materialization` instead — that helper
    treats a config-read failure as "block C2" so a DB blip cannot
    silently re-enable the deterministic path."""
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT CONFIG_VALUE FROM MIP.APP.APP_CONFIG WHERE CONFIG_KEY = %s",
                (CONFIG_KEY_PRIMARY_MATERIALIZATION_ENABLED,),
            )
            row = cur.fetchone()
            return _is_flag_true(row[0]) if row else False
        finally:
            try:
                cur.close()
            except Exception:  # noqa: BLE001
                pass
    except Exception:  # noqa: BLE001
        logger.exception(
            "is_agentic_primary_materialization_enabled: config read failed",
        )
        return False


def should_block_deterministic_materialization(conn) -> bool:
    """Stage 4f hard-guard helper. Returns True when the deterministic C2
    materializer must be blocked from running.

    Semantics:

    * True when `AGENTIC_PRIMARY_MATERIALIZATION_ENABLED` is truthy.
    * True when the flag CANNOT be read (config row missing, DB error,
      cursor failure). Stage 4f explicitly requires fail-CLOSED behavior
      for the deterministic path: a config-read blip must not silently
      re-enable C2 materialization while agentic-primary is the intended
      operating mode.

    The only way this returns False is an explicit, successfully-read
    `'false'` / `'0'` / `'no'` / empty value. That preserves the
    documented rollback semantics (set the flag to `'false'`, the
    deterministic path returns).
    """
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT CONFIG_VALUE FROM MIP.APP.APP_CONFIG WHERE CONFIG_KEY = %s",
                (CONFIG_KEY_PRIMARY_MATERIALIZATION_ENABLED,),
            )
            row = cur.fetchone()
        finally:
            try:
                cur.close()
            except Exception:  # noqa: BLE001
                pass
        if not row:
            # No config row at all -> safer assumption is "block" because the
            # default for a freshly-provisioned environment running Stage 4f
            # code is agentic-primary.
            logger.warning(
                "should_block_deterministic_materialization: APP_CONFIG row "
                "for %s missing; failing closed (blocking C2 materialize).",
                CONFIG_KEY_PRIMARY_MATERIALIZATION_ENABLED,
            )
            return True
        return _is_flag_true(row[0])
    except Exception:  # noqa: BLE001
        logger.exception(
            "should_block_deterministic_materialization: config read failed; "
            "failing closed (blocking C2 materialize).",
        )
        return True


def _gate_from_latest_row(latest_row: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Pure evaluator: given the latest AGENTIC_REVALIDATION_AUTHORITY row for
    an action (or None), return the Stage 4d gate verdict.

    Result shape:
        {
          "gate_ok":      bool,                    # all four conditions met?
          "reason_code":  str | None,              # primary blocking reason
          "reason_codes": list[str],               # full list (may have extras)
          "tooltip":      str | None,              # submit-button hint
          "authority_mode":    str | None,
          "authority_status":  str | None,
          "is_stale":          bool | None,
          "is_latest":         bool | None,
          "size_posture":      str | None,         # 'REDUCED' for REDUCED approval
          "authority_id":      str | None,
        }

    This helper makes no DB calls — pass it the row dict you already have.
    """
    if not latest_row:
        return {
            "gate_ok": False,
            "reason_code": GATE_REASON_NOT_COMMITTED,
            "reason_codes": [GATE_REASON_NOT_COMMITTED],
            "tooltip": _GATE_TOOLTIP_BY_REASON[GATE_REASON_NOT_COMMITTED],
            "authority_mode": None,
            "authority_status": None,
            "is_stale": None,
            "is_latest": None,
            "size_posture": None,
            "authority_id": None,
        }

    mode = (latest_row.get("AUTHORITY_MODE") or "").upper()
    status = (latest_row.get("AUTHORITY_STATUS") or "").upper()
    is_stale = bool(latest_row.get("IS_STALE"))
    is_latest = bool(latest_row.get("IS_LATEST", True))  # view default
    size_posture = latest_row.get("SHADOW_SIZE_POSTURE")
    authority_id = latest_row.get("AUTHORITY_ID")

    common = {
        "authority_mode": mode or None,
        "authority_status": status or None,
        "is_stale": is_stale,
        "is_latest": is_latest,
        "size_posture": (str(size_posture).upper() if size_posture else None),
        "authority_id": authority_id,
    }

    # The Stage 4d gate ONLY trusts the LATEST OPERATOR_COMMITTED row. If a
    # newer AUTO_AUDIT row has been written (e.g. after a fresh shadow run)
    # the operator must re-apply review even if their previous OPERATOR_COMMITTED
    # row was already approving.
    if mode != AUTHORITY_MODE_OPERATOR_COMMITTED:
        return {
            **common,
            "gate_ok": False,
            "reason_code": GATE_REASON_NOT_COMMITTED,
            "reason_codes": [GATE_REASON_NOT_COMMITTED],
            "tooltip": _GATE_TOOLTIP_BY_REASON[GATE_REASON_NOT_COMMITTED],
        }

    if is_stale:
        return {
            **common,
            "gate_ok": False,
            "reason_code": GATE_REASON_STALE,
            "reason_codes": [GATE_REASON_STALE],
            "tooltip": _GATE_TOOLTIP_BY_REASON[GATE_REASON_STALE],
        }

    if status in POSITIVE_AUTHORITY_STATUSES:
        return {
            **common,
            "gate_ok": True,
            "reason_code": None,
            "reason_codes": [],
            "tooltip": None,
        }

    blocking_reason = _STATUS_TO_BLOCK_REASON.get(status, GATE_REASON_UNKNOWN_STATUS)
    return {
        **common,
        "gate_ok": False,
        "reason_code": blocking_reason,
        "reason_codes": [blocking_reason],
        "tooltip": _GATE_TOOLTIP_BY_REASON[blocking_reason],
    }


def evaluate_authority_gate(conn, action_id: str) -> Dict[str, Any]:
    """
    Stage 4d — evaluate the Submit gate for a single ACTION_ID.

    Reads the latest authority row from `V_AGENTIC_AUTHORITY_LATEST` (the
    `IS_LATEST = TRUE` filtered view), evaluates the four gate conditions:

        IS_LATEST        = TRUE
        AUTHORITY_MODE   = 'OPERATOR_COMMITTED'
        IS_STALE         = FALSE
        AUTHORITY_STATUS IN ('AGENTIC_APPROVE', 'AGENTIC_APPROVE_REDUCED')

    and returns the verdict per `_gate_from_latest_row`.

    Always also returns `"gate_enabled"`: True only when
    `AGENTIC_AUTHORITY_ENABLED='true'` in `APP_CONFIG`. Callers must use this
    flag to decide whether to enforce the gate — when False, the gate verdict
    is informational only.

    Never raises. Stage 4f fail-mode policy:

    * If `AGENTIC_PRIMARY_MATERIALIZATION_ENABLED` is true (or its config
      row cannot be read), an evaluation error fails CLOSED:
      gate_enabled=True, gate_ok=False, reason=AGENTIC_AUTHORITY_EVAL_ERROR.
      No silent fallback to the deterministic path is possible.
    * If agentic-primary is explicitly off, the historical Stage 4d behavior
      applies: an evaluation error returns gate_enabled=False, gate_ok=True
      so the gate does not become a unique blocker while the system is in
      the legacy operating mode.
    """
    if not action_id:
        return {
            "gate_enabled": False,
            "gate_ok": True,
            "reason_code": None,
            "reason_codes": [],
            "tooltip": None,
            "authority_mode": None,
            "authority_status": None,
            "is_stale": None,
            "is_latest": None,
            "size_posture": None,
            "authority_id": None,
            "error": "MISSING_ACTION_ID",
        }
    try:
        gate_enabled = is_authority_gate_enabled(conn)
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT AUTHORITY_ID, AUTHORITY_MODE, AUTHORITY_STATUS,
                       AUTHORITY_REASON_CODE, AUTHORITY_CONFIDENCE,
                       IS_STALE, STALE_REASON,
                       SHADOW_SIZE_POSTURE, SHADOW_STANCE_RAW,
                       DETERMINISTIC_BASELINE_STANCE, DISAGREES_WITH_BASELINE,
                       COMMITTED_BY, CREATED_AT
                FROM MIP.APP.V_AGENTIC_AUTHORITY_LATEST
                WHERE ACTION_ID = %s
                """,
                (action_id,),
            )
            row = cur.fetchone()
            cols = [d[0] for d in cur.description] if cur.description else []
        finally:
            try:
                cur.close()
            except Exception:  # noqa: BLE001
                pass
        latest = dict(zip(cols, row)) if row else None
        # `V_AGENTIC_AUTHORITY_LATEST` already filters IS_LATEST=TRUE; force
        # the flag in the dict so _gate_from_latest_row's invariant holds.
        if latest is not None:
            latest["IS_LATEST"] = True
        verdict = _gate_from_latest_row(latest)
        verdict["gate_enabled"] = gate_enabled
        return verdict
    except Exception as exc:  # noqa: BLE001
        logger.exception("evaluate_authority_gate FAILED action=%s", action_id)
        # Stage 4f — fail-CLOSED when agentic-primary is the operating mode
        # (or when we cannot read the flag). Otherwise preserve the historical
        # Stage 4d fail-open default so legacy environments are not affected.
        try:
            fail_closed = should_block_deterministic_materialization(conn)
        except Exception:  # noqa: BLE001
            fail_closed = True
        if fail_closed:
            return {
                "gate_enabled": True,
                "gate_ok": False,
                "reason_code": GATE_REASON_EVAL_ERROR,
                "reason_codes": [GATE_REASON_EVAL_ERROR],
                "tooltip": _GATE_TOOLTIP_BY_REASON[GATE_REASON_EVAL_ERROR],
                "authority_mode": None,
                "authority_status": None,
                "is_stale": None,
                "is_latest": None,
                "size_posture": None,
                "authority_id": None,
                "error": str(exc)[:300],
            }
        return {
            "gate_enabled": False,
            "gate_ok": True,
            "reason_code": None,
            "reason_codes": [],
            "tooltip": None,
            "authority_mode": None,
            "authority_status": None,
            "is_stale": None,
            "is_latest": None,
            "size_posture": None,
            "authority_id": None,
            "error": str(exc)[:300],
        }


def evaluate_authority_gate_bulk(conn, action_ids: list) -> Dict[str, Dict[str, Any]]:
    """
    Bulk variant of `evaluate_authority_gate` — one query covering up to a few
    hundred action_ids in a single round-trip. Used by the LPA pending-decisions
    builder which evaluates every visible row.

    Returns a dict keyed by ACTION_ID. Missing actions get the "no row" verdict.
    Includes `gate_enabled` (same flag value) on every entry so callers don't
    have to plumb it separately.

    Never raises; an infrastructure failure returns an open-gate verdict for
    every requested action_id (with `error` populated) — see the rationale on
    `evaluate_authority_gate`.
    """
    out: Dict[str, Dict[str, Any]] = {}
    unique_ids = [a for a in {str(x) for x in (action_ids or []) if x} if a]
    if not unique_ids:
        return out
    try:
        gate_enabled = is_authority_gate_enabled(conn)
        # Build a parametrized IN clause; Snowflake supports up to ~16k params.
        placeholders = ",".join(["%s"] * len(unique_ids))
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                SELECT AUTHORITY_ID, ACTION_ID, AUTHORITY_MODE, AUTHORITY_STATUS,
                       AUTHORITY_REASON_CODE, AUTHORITY_CONFIDENCE,
                       IS_STALE, STALE_REASON,
                       SHADOW_SIZE_POSTURE, SHADOW_STANCE_RAW,
                       DETERMINISTIC_BASELINE_STANCE, DISAGREES_WITH_BASELINE,
                       COMMITTED_BY, CREATED_AT
                FROM MIP.APP.V_AGENTIC_AUTHORITY_LATEST
                WHERE ACTION_ID IN ({placeholders})
                """,
                unique_ids,
            )
            cols = [d[0] for d in cur.description] if cur.description else []
            rows = cur.fetchall()
        finally:
            try:
                cur.close()
            except Exception:  # noqa: BLE001
                pass

        by_action: Dict[str, Dict[str, Any]] = {}
        for r in rows:
            d = dict(zip(cols, r))
            d["IS_LATEST"] = True  # the view filters this, force the flag
            by_action[str(d.get("ACTION_ID"))] = d

        for action_id in unique_ids:
            verdict = _gate_from_latest_row(by_action.get(action_id))
            verdict["gate_enabled"] = gate_enabled
            out[action_id] = verdict
        return out
    except Exception as exc:  # noqa: BLE001
        logger.exception("evaluate_authority_gate_bulk FAILED")
        err = str(exc)[:300]
        for action_id in unique_ids:
            out[action_id] = {
                "gate_enabled": False,
                "gate_ok": True,
                "reason_code": None,
                "reason_codes": [],
                "tooltip": None,
                "authority_mode": None,
                "authority_status": None,
                "is_stale": None,
                "is_latest": None,
                "size_posture": None,
                "authority_id": None,
                "error": err,
            }
        return out


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
    "CONFIG_KEY_OPERATOR_COMMIT_ENABLED",
    "CONFIG_KEY_AUTHORITY_GATE_ENABLED",
    "CONFIG_KEY_PRIMARY_MATERIALIZATION_ENABLED",
    "CONFIG_KEY_AUTO_COMMIT_ENABLED",
    "AUTO_COMMIT_ACTOR",
    "AUTO_COMMIT_APPROVING_STATUSES",
    "CONFIG_KEY_MIN_CONFIDENCE",
    "CONFIG_KEY_MAX_AGE",
    "SKIP_DISABLED",
    "SKIP_OVERRIDE_NOT_ENABLED",
    "SKIP_SESSION_NOT_FOUND",
    "SKIP_HEARING_MISMATCH",
    "SKIP_ACTION_HEARING_MISSING",
    "SKIP_EVIDENCE_PACK_STALE",
    "SKIP_ERROR",
    "GATE_REASON_NOT_COMMITTED",
    "GATE_REASON_STALE",
    "GATE_REASON_BLOCKED_WAIT_RECLAIM",
    "GATE_REASON_BLOCKED_DEFER",
    "GATE_REASON_BLOCKED_REJECT",
    "GATE_REASON_DEGRADED",
    "GATE_REASON_FAILED",
    "GATE_REASON_UNKNOWN_STATUS",
    "GATE_REASON_EVAL_ERROR",
    "is_pack_version_supported",
    "is_auto_audit_enabled",
    "is_operator_commit_enabled",
    "is_authority_gate_enabled",
    "is_agentic_primary_materialization_enabled",
    "should_block_deterministic_materialization",
    "map_shadow_to_authority_status",
    "build_staleness_check",
    "build_authority_row",
    "insert_authority_row_with_supersession",
    "commit_auto_audit_authority_for_session",
    "commit_operator_authority_for_session",
    "auto_promote_authority_to_operator_committed",
    "is_auto_commit_enabled",
    "evaluate_authority_gate",
    "evaluate_authority_gate_bulk",
]
