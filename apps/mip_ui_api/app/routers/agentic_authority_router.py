"""
Stage 4c — Agentic Authority operator-facing endpoints.

This router provides the LPA-side surface area for the agentic revalidation
authority introduced in Stage 4a/4b:

    POST /live/trades/actions/{action_id}/agentic-authority/commit
        Operator explicitly "applies" the latest shadow-board verdict by
        writing an AUTHORITY_MODE='OPERATOR_COMMITTED' row into
        MIP.APP.AGENTIC_REVALIDATION_AUTHORITY. Stage 4d Submit gating will
        eventually trust this row; in Stage 4c the row is observed but has no
        gating effect — Submit logic is unchanged.

    GET  /live/trades/actions/{action_id}/agentic-authority
        Returns the latest authority row for the action (any mode) plus a
        derived `gate_eligible` flag the UI uses to render chips/buttons.

    GET  /live/trades/actions/{action_id}/agentic-authority/history
        Returns the full history of authority commits for the action
        (diagnostic only).

Boundaries / non-goals for Stage 4c:
  - This router does NOT touch LIVE_ACTIONS.
  - This router does NOT change Submit / revalidate behavior.
  - This router does NOT trigger the shadow board (existing orchestrate path
    still does that).
  - Operator override (`override_reason`) is rejected with 400 OVERRIDE_NOT_ENABLED.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, Body, HTTPException, Path
from pydantic import BaseModel, Field

from app.committee.agentic_authority import (
    AUTHORITY_MODE_OPERATOR_COMMITTED,
    POSITIVE_AUTHORITY_STATUSES,
    SKIP_ACTION_HEARING_MISSING,
    SKIP_DISABLED,
    SKIP_ERROR,
    SKIP_HEARING_MISMATCH,
    SKIP_OVERRIDE_NOT_ENABLED,
    SKIP_SESSION_NOT_FOUND,
    _gate_from_latest_row,
    commit_operator_authority_for_session,
    is_agentic_primary_materialization_enabled,
    is_authority_gate_enabled,
)
from app.db import fetch_all, get_connection, serialize_row, serialize_rows

router = APIRouter(prefix="/live", tags=["agentic-authority"])
_log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------

class AgenticAuthorityCommitRequest(BaseModel):
    shadow_session_id: str = Field(
        ..., min_length=8, max_length=64,
        description="UUID of the SHADOW_BOARD_SESSION whose verdict is being committed.",
    )
    hearing_id: str = Field(
        ..., min_length=8, max_length=64,
        description=(
            "COMMITTEE_HEARING.HEARING_ID that the session was built against. "
            "Must match the session's HEARING_ID and the action's current hearing."
        ),
    )
    actor: str = Field(
        default="agentic_operator", min_length=1, max_length=100,
        description="Who clicked Apply Agentic Review (operator identifier).",
    )
    override_reason: Optional[str] = Field(
        default=None, max_length=500,
        description=(
            "Reserved for Stage 4d+. Any non-null value in Stage 4c returns 400 "
            "OVERRIDE_NOT_ENABLED."
        ),
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _gate_eligible_from_row(row: Dict[str, Any]) -> bool:
    """A row gates Submit only when it is OPERATOR_COMMITTED, not stale, and
    has a positive authority status. Mirrors the rule documented for Stage 4d
    so the UI can preview what will/won't gate."""
    if not row:
        return False
    mode = (row.get("AUTHORITY_MODE") or "").upper()
    if mode != AUTHORITY_MODE_OPERATOR_COMMITTED:
        return False
    if bool(row.get("IS_STALE")):
        return False
    status = (row.get("AUTHORITY_STATUS") or "").upper()
    return status in POSITIVE_AUTHORITY_STATUSES


def _status_to_display(status: str) -> Dict[str, Any]:
    """Lightweight UI hints. The frontend has its own copy of labels; this is
    just a server echo so curl users can see the intended meaning."""
    s = (status or "").upper()
    table = {
        "AGENTIC_APPROVE":                  ("Agentic: Approved", "approve"),
        "AGENTIC_APPROVE_REDUCED":          ("Agentic: Approve (reduced size)", "approve-reduced"),
        "AGENTIC_WAIT_RECLAIM":             ("Agentic: Wait / Reclaim", "block"),
        "AGENTIC_DEFER":                    ("Agentic: Defer", "block"),
        "AGENTIC_REJECT":                   ("Agentic: Reject", "reject"),
        "AGENTIC_DEGRADED_NO_AUTHORITY":    ("Agentic: Degraded — no authority", "degraded"),
        "AGENTIC_FAILED_NO_AUTHORITY":      ("Agentic: Not available", "degraded"),
    }
    label, css = table.get(s, ("Agentic: Unknown", "degraded"))
    return {"label": label, "css_tone": css}


_SKIP_TO_HTTP: Dict[str, int] = {
    SKIP_DISABLED:                400,
    SKIP_OVERRIDE_NOT_ENABLED:    400,
    SKIP_SESSION_NOT_FOUND:       404,
    SKIP_HEARING_MISMATCH:        409,
    SKIP_ACTION_HEARING_MISSING:  409,
    SKIP_ERROR:                   500,
}


# Stage 4e — statuses where the agentic materializer is allowed to drive the
# action forward. Beyond these (PM_ACCEPTED, COMPLIANCE_APPROVED, INTENT_*,
# REVALIDATED_*) the operator commit remains a pure audit snapshot — we do
# not re-materialize a late-stage action.
_AGENTIC_MATERIALIZER_ELIGIBLE_STATUSES = {
    "OPEN_BLOCKED",
    "OPEN_ELIGIBLE",
    "OPEN_CAUTION",
    "PENDING_OPEN_STABILITY_REVIEW",
    "READY_FOR_APPROVAL_FLOW",
}

# Stage 1.5 — late-stage recovery when executable protection was never materialized.
_RECOVERY_LATE_STAGE_STATUSES = frozenset({"REVALIDATED_PASS", "REVALIDATED_FAIL"})

_RECOVERY_INCOMPLETE_BRACKET_REASON_CODES = frozenset({
    "STRUCT_SUBMIT_CONTRACT_INCOMPLETE",
    "LIVE_TP_REQUIRED_MISSING",
    "LIVE_SL_REQUIRED_MISSING",
    "LIVE_BRACKET_REQUIRED",
})


def _committee_bracket_baseline_incomplete(param_snapshot: dict) -> bool:
    baseline = param_snapshot.get("committee_bracket_baseline")
    if not isinstance(baseline, dict):
        return True
    for key in (
        "acceptable_early_exit_target_return",
        "realistic_target_return",
        "stop_loss_pct",
    ):
        if baseline.get(key) is not None:
            return False
    return True


def _executable_bracket_incomplete(param_snapshot: dict) -> bool:
    eb = param_snapshot.get("executable_bracket")
    if eb is None:
        return True
    if not isinstance(eb, dict):
        return True
    if eb.get("blocked"):
        return True
    if eb.get("target_return") is None or eb.get("stop_loss_pct") is None:
        return True
    return False


def _action_needs_recovery_materialization(action: dict) -> bool:
    """True when a late-stage row still lacks a viable executable protection contract."""
    from app.routers.live import _parse_list_variant, _parse_variant

    ps = _parse_variant(action.get("PARAM_SNAPSHOT"))
    if not isinstance(ps, dict):
        ps = {}

    diag = ps.get("structural_diagnostics_v1")
    contract_incomplete = True
    if isinstance(diag, dict):
        contract_incomplete = not bool(diag.get("structural_contract_complete", False))

    eb_incomplete = _executable_bracket_incomplete(ps)
    baseline_incomplete = _committee_bracket_baseline_incomplete(ps)

    reason_codes = {
        str(rc).strip().upper()
        for rc in _parse_list_variant(action.get("REASON_CODES"))
        if rc
    }
    has_incomplete_reason = bool(reason_codes & _RECOVERY_INCOMPLETE_BRACKET_REASON_CODES)

    structural_incomplete = contract_incomplete or eb_incomplete or baseline_incomplete
    if structural_incomplete:
        return True
    return has_incomplete_reason and (
        contract_incomplete or eb_incomplete or baseline_incomplete
    )


def agentic_materializer_status_eligible(
    action: dict,
) -> tuple[bool, str | None]:
    """Return (eligible, reason) for agentic-primary materialization on commit."""
    status = str(action.get("STATUS") or "").upper()
    if status in _AGENTIC_MATERIALIZER_ELIGIBLE_STATUSES:
        return True, "forward_eligible_status"
    if status in _RECOVERY_LATE_STAGE_STATUSES:
        if _action_needs_recovery_materialization(action):
            return True, "recovery_incomplete_contract"
        return False, "status_not_eligible"
    return False, "status_not_eligible"


def _build_authority_row_from_commit(
    action_id: str, result: Dict[str, Any]
) -> Dict[str, Any]:
    """Synthesize the dict shape expected by the agentic materializer from a
    commit_operator_authority_for_session result. Keeps the materializer's
    contract narrow and decoupled from the live AGENTIC_REVALIDATION_AUTHORITY
    table schema."""
    return {
        "AUTHORITY_ID":          result.get("authority_id"),
        "ACTION_ID":             action_id,
        "AUTHORITY_MODE":        result.get("authority_mode"),
        "AUTHORITY_STATUS":      (result.get("authority_status") or "").upper(),
        "AUTHORITY_CONFIDENCE":  result.get("authority_confidence"),
        "COMMITTED_BY":          result.get("committed_by") or "operator",
        "IS_STALE":              bool(result.get("is_stale")),
        "IS_LATEST":             True,
        "SHADOW_SIZE_POSTURE":   result.get("shadow_size_posture"),
        "SESSION_AGE_MINUTES":   result.get("session_age_minutes"),
    }


def _maybe_run_agentic_materializer(
    action_id: str, commit_result: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    """If AGENTIC_PRIMARY_MATERIALIZATION_ENABLED is true and the action is in
    a forward-eligible status, run `_materialize_structural_entry_agentic_apply`.

    Returns the materializer result dict on success, None when the flag is
    off / the action is not eligible, or a `{"error": ..., "ran": False}` shape
    when something failed. Failure never propagates to the caller — the commit
    has already succeeded and the operator's audit row is preserved regardless.
    """
    materializer_conn = None
    try:
        materializer_conn = get_connection()
        if not is_agentic_primary_materialization_enabled(materializer_conn):
            return {"ran": False, "reason": "flag_off"}

        cur = materializer_conn.cursor()
        try:
            from app.routers.committee import _underlying_sf_conn
            from app.routers.live import (
                _fetch_live_action,
                _materialize_structural_entry_agentic_apply,
                is_structural_live_action,
            )
            action_row = _fetch_live_action(cur, action_id)
            if not action_row:
                return {"ran": False, "reason": "action_not_found"}
            if not is_structural_live_action(action_row):
                return {"ran": False, "reason": "non_structural"}
            eligible, eligibility_reason = agentic_materializer_status_eligible(dict(action_row))
            status = str(action_row.get("STATUS") or "").upper()
            if not eligible:
                return {"ran": False, "reason": eligibility_reason, "status": status}

            authority_row = _build_authority_row_from_commit(action_id, commit_result)
            recovery_late_stage = (
                eligibility_reason == "recovery_incomplete_contract"
                and status in _RECOVERY_LATE_STAGE_STATUSES
            )
            raw = _underlying_sf_conn(materializer_conn)
            raw.autocommit(False)
            try:
                out = _materialize_structural_entry_agentic_apply(
                    cur,
                    action_id,
                    dict(action_row),
                    authority_row,
                    apply_detail_source="AGENTIC_OPERATOR_COMMIT",
                    recovery_late_stage=recovery_late_stage,
                )
                raw.commit()
            except Exception:  # noqa: BLE001
                raw.rollback()
                raise
            finally:
                raw.autocommit(True)
            return {
                "ran": True,
                "eligibility_reason": eligibility_reason,
                "recovery_late_stage": recovery_late_stage,
                **out,
            }
        finally:
            try:
                cur.close()
            except Exception:  # noqa: BLE001
                pass
    except Exception as exc:  # noqa: BLE001 — never let materializer kill the commit
        _log.exception("agentic materializer failed for action %s", action_id)
        return {"ran": False, "reason": "error", "error": str(exc)}
    finally:
        if materializer_conn is not None:
            try:
                materializer_conn.close()
            except Exception:  # noqa: BLE001
                pass


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("/trades/actions/{action_id}/agentic-authority/commit")
def commit_agentic_authority(
    action_id: str = Path(..., min_length=8, max_length=64),
    body: AgenticAuthorityCommitRequest = Body(...),
):
    """Stage 4c — operator commits an AGENTIC authority row for this action.

    Writes AUTHORITY_MODE='OPERATOR_COMMITTED' to MIP.APP.AGENTIC_REVALIDATION_AUTHORITY.
    Submit gating is NOT changed by this endpoint. Stage 4d will add that gate.
    """
    conn = None
    try:
        conn = get_connection()
        result = commit_operator_authority_for_session(
            conn,
            action_id=action_id,
            shadow_session_id=body.shadow_session_id,
            hearing_id=body.hearing_id,
            committed_by=body.actor,
            override_reason=body.override_reason,
            enforce_config_flag=True,
        )
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                _log.exception("commit_agentic_authority: conn.close failed")

    if not result.get("committed"):
        skip = result.get("skip_reason") or SKIP_ERROR
        status_code = _SKIP_TO_HTTP.get(skip, 500)
        detail = {
            "error_code": skip,
            "action_id": action_id,
            **{k: v for k, v in result.items() if k != "skip_reason"},
        }
        raise HTTPException(status_code=status_code, detail=detail)

    auth_status = (result.get("authority_status") or "").upper()

    # Stage 4e — fire the agentic materializer once the operator row is
    # safely written. Runs in its own connection so a materializer failure
    # never undoes the commit. Returns None or a diagnostic dict so the UI
    # can surface why STATUS did/didn't move.
    agentic_materializer = _maybe_run_agentic_materializer(action_id, result)

    # Build the Stage 4d gate verdict using the row we just wrote so the
    # client doesn't have to re-fetch. Keep the verdict shape identical to
    # the GET endpoint so the UI can use one rendering path.
    synthetic_row = {
        "AUTHORITY_ID": result.get("authority_id"),
        "AUTHORITY_MODE": result.get("authority_mode"),
        "AUTHORITY_STATUS": auth_status,
        "IS_STALE": bool(result.get("is_stale")),
        "IS_LATEST": True,
        "SHADOW_SIZE_POSTURE": result.get("shadow_size_posture"),
    }
    gate_eval = _gate_from_latest_row(synthetic_row)
    # Pop a fresh flag read so the response is self-describing.
    gate_eval_conn = None
    agentic_primary_enabled = False
    try:
        gate_eval_conn = get_connection()
        gate_eval["gate_enabled"] = is_authority_gate_enabled(gate_eval_conn)
        agentic_primary_enabled = is_agentic_primary_materialization_enabled(gate_eval_conn)
    except Exception:  # noqa: BLE001
        gate_eval["gate_enabled"] = False
    finally:
        if gate_eval_conn is not None:
            try:
                gate_eval_conn.close()
            except Exception:  # noqa: BLE001
                pass

    return {
        "committed": True,
        "authority_id": result.get("authority_id"),
        "action_id": result.get("action_id"),
        "authority_mode": result.get("authority_mode"),
        "authority_status": auth_status,
        "authority_reason_code": result.get("authority_reason_code"),
        "authority_confidence": result.get("authority_confidence"),
        "shadow_stance_raw": result.get("shadow_stance_raw"),
        "deterministic_baseline_stance": result.get("deterministic_baseline_stance"),
        "disagrees_with_baseline": result.get("disagrees_with_baseline"),
        "is_stale": bool(result.get("is_stale")),
        "stale_reason": result.get("stale_reason"),
        "pack_version": result.get("pack_version"),
        "pack_version_ok": bool(result.get("pack_version_ok")),
        "session_age_minutes": result.get("session_age_minutes"),
        "superseded_authority_id": result.get("superseded_authority_id"),
        "gate_eligible": (
            auth_status in POSITIVE_AUTHORITY_STATUSES
            and not bool(result.get("is_stale"))
        ),
        "display": _status_to_display(auth_status),
        "gate_evaluation": gate_eval,
        "agentic_primary_enabled": agentic_primary_enabled,
        "agentic_materializer": agentic_materializer,
    }


@router.get("/trades/actions/{action_id}/agentic-authority")
def get_agentic_authority_latest(
    action_id: str = Path(..., min_length=8, max_length=64),
):
    """Return the latest authority row for the action (any AUTHORITY_MODE).

    Returns 200 with `{authority: null, gate_eligible: false}` when no authority
    row exists yet (e.g. shadow board has never completed for this action)."""
    conn = None
    gate_enabled = False
    try:
        conn = get_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT
                    AUTHORITY_ID, ACTION_ID, PROPOSAL_ID, HEARING_ID, SHADOW_SESSION_ID,
                    PACK_VERSION, PACK_VERSION_OK, AUTHORITY_MODE, COMMITTED_BY,
                    AUTHORITY_STATUS, AUTHORITY_REASON_CODE, AUTHORITY_CONFIDENCE,
                    SHADOW_STANCE_RAW, SHADOW_STATUS_RAW, SHADOW_STAGE_REACHED,
                    SHADOW_DEGRADED, SHADOW_DEGRADED_REASON, SHADOW_PLURALITY_BASIS,
                    SHADOW_SIZE_POSTURE,
                    DETERMINISTIC_BASELINE_STANCE, DISAGREES_WITH_BASELINE,
                    IS_STALE, STALE_REASON, SESSION_AGE_MINUTES,
                    IS_OPERATOR_OVERRIDDEN, OVERRIDE_BY, OVERRIDE_AT,
                    OVERRIDE_REASON, OVERRIDE_ORIGINAL_STATUS,
                    LIVE_ACTION_STATUS, SYMBOL, C2_STANCE, C2_CONFIDENCE,
                    CREATED_AT, UPDATED_AT
                FROM MIP.APP.V_AGENTIC_AUTHORITY_LATEST
                WHERE ACTION_ID = %s
                """,
                (action_id,),
            )
            rows = fetch_all(cur)
        finally:
            try:
                cur.close()
            except Exception:  # noqa: BLE001
                pass
        # Read the Stage 4d gate flag on the same connection so the response
        # is self-describing without a second pool acquire.
        gate_enabled = is_authority_gate_enabled(conn)
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                _log.exception("get_agentic_authority_latest: conn.close failed")

    if not rows:
        verdict = _gate_from_latest_row(None)
        verdict["gate_enabled"] = gate_enabled
        return {
            "action_id": action_id,
            "authority": None,
            "gate_eligible": False,
            "display": None,
            "gate_evaluation": verdict,
        }

    row = rows[0]
    serialized = serialize_row(row)
    # `V_AGENTIC_AUTHORITY_LATEST` already filters IS_LATEST=TRUE; force the
    # flag on the dict view so the pure evaluator's invariant holds.
    row_for_eval = dict(row)
    row_for_eval["IS_LATEST"] = True
    verdict = _gate_from_latest_row(row_for_eval)
    verdict["gate_enabled"] = gate_enabled
    return {
        "action_id": action_id,
        "authority": serialized,
        "gate_eligible": _gate_eligible_from_row(row),
        "display": _status_to_display(row.get("AUTHORITY_STATUS") or ""),
        "gate_evaluation": verdict,
    }


@router.get("/trades/actions/{action_id}/agentic-authority/history")
def get_agentic_authority_history(
    action_id: str = Path(..., min_length=8, max_length=64),
    limit: int = 50,
):
    """Return full history of authority commits for the action.

    The list is ordered newest-first. Includes both AUTO_AUDIT and
    OPERATOR_COMMITTED rows, including superseded ones (IS_LATEST=FALSE).
    """
    safe_limit = max(1, min(int(limit or 50), 200))

    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                SELECT
                    ACTION_ID, AUTHORITY_ID, AUTHORITY_MODE, COMMITTED_BY,
                    AUTHORITY_STATUS, AUTHORITY_REASON_CODE, AUTHORITY_CONFIDENCE,
                    SHADOW_STANCE_RAW, SHADOW_STATUS_RAW, SHADOW_SESSION_ID,
                    PACK_VERSION, PACK_VERSION_OK,
                    IS_STALE, STALE_REASON, IS_LATEST,
                    SUPERSEDED_AT, SUPERSEDED_BY,
                    IS_OPERATOR_OVERRIDDEN, OVERRIDE_BY, OVERRIDE_REASON,
                    CREATED_AT
                FROM MIP.APP.V_AGENTIC_AUTHORITY_HISTORY
                WHERE ACTION_ID = %s
                ORDER BY CREATED_AT DESC NULLS LAST
                LIMIT {safe_limit}
                """,
                (action_id,),
            )
            rows = fetch_all(cur)
        finally:
            try:
                cur.close()
            except Exception:  # noqa: BLE001
                pass
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                _log.exception("get_agentic_authority_history: conn.close failed")

    return {
        "action_id": action_id,
        "count": len(rows),
        "history": serialize_rows(rows),
    }
