"""
Live intent classification and LIVE_STRUCTURAL_ONLY deployment policy.
"""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Callable
from typing import Any

from fastapi import HTTPException

_log = logging.getLogger(__name__)

LIVE_INTENT_STRUCTURAL = "STRUCTURAL"
LIVE_INTENT_LEGACY_PATTERN = "LEGACY_PATTERN"
LIVE_INTENT_OPERATOR_EXIT = "OPERATOR_EXIT"
LIVE_INTENT_UNKNOWN = "UNKNOWN"

CONFIG_LIVE_STRUCTURAL_ONLY = "LIVE_STRUCTURAL_ONLY"
ENV_LIVE_STRUCTURAL_ONLY = "MIP_LIVE_STRUCTURAL_ONLY"


def _parse_snapshot_dict(param_snapshot: Any) -> dict:
    if param_snapshot is None:
        return {}
    if isinstance(param_snapshot, dict):
        return param_snapshot
    if isinstance(param_snapshot, str):
        try:
            v = json.loads(param_snapshot)
            return v if isinstance(v, dict) else {}
        except Exception:
            return {}
    return {}


def live_intent_kind_from_row(row: dict | None) -> str:
    if not row:
        return LIVE_INTENT_UNKNOWN
    col = row.get("LIVE_INTENT_KIND")
    if col is not None and str(col).strip():
        return str(col).strip().upper()
    ps = _parse_snapshot_dict(row.get("PARAM_SNAPSHOT"))
    src = str(ps.get("source") or "").upper()
    if src == "BROKER_POSITION_EXIT":
        return LIVE_INTENT_OPERATOR_EXIT
    if src == "ORDER_PROPOSALS":
        return LIVE_INTENT_LEGACY_PATTERN
    if ps.get("structural_source") is True:
        return LIVE_INTENT_STRUCTURAL
    if row.get("SETUP_EVENT_ID") is not None and str(row.get("SETUP_EVENT_ID")).strip() != "":
        return LIVE_INTENT_STRUCTURAL
    if row.get("SETUP_FAMILY"):
        return LIVE_INTENT_STRUCTURAL
    sec = ps.get("structural_execution_contract_v1")
    if isinstance(sec, dict) and sec.get("routing", {}).get("committee_model") == "STRUCTURAL_V1":
        return LIVE_INTENT_STRUCTURAL
    return LIVE_INTENT_UNKNOWN


def parse_structural_only_config(value: str | None, *, default: bool = True) -> bool:
    if value is None or str(value).strip() == "":
        return default
    raw = str(value).strip().lower()
    if raw in ("1", "true", "yes", "on", "y"):
        return True
    if raw in ("0", "false", "no", "off", "n"):
        return False
    return default


def env_structural_only_flag() -> bool | None:
    raw = os.getenv(ENV_LIVE_STRUCTURAL_ONLY)
    if raw is None or str(raw).strip() == "":
        return None
    return parse_structural_only_config(raw, default=True)


def live_structural_only_enabled_cur(cur) -> bool:
    """
    Read LIVE_STRUCTURAL_ONLY from APP_CONFIG (default true if missing).
    Safe to call from any router; does not depend on live.py.
    """
    try:
        cur.execute(
            """
            select CONFIG_VALUE
            from MIP.APP.APP_CONFIG
            where CONFIG_KEY = %s
            limit 1
            """,
            (CONFIG_LIVE_STRUCTURAL_ONLY,),
        )
        row = cur.fetchone()
        val = row[0] if row else None
    except Exception:
        val = None
    app_val = parse_structural_only_config(str(val) if val is not None else None, default=True)
    env_val = env_structural_only_flag()
    if env_val is not None and env_val != app_val:
        _log.warning(
            "%s=%s disagrees with APP_CONFIG %s=%s; using APP_CONFIG value.",
            ENV_LIVE_STRUCTURAL_ONLY,
            env_val,
            CONFIG_LIVE_STRUCTURAL_ONLY,
            app_val,
        )
    return app_val


def assert_legacy_order_proposals_import_allowed(app_structural_only: bool) -> None:
    if not app_structural_only:
        return
    debug_legacy = os.getenv("MIP_DEBUG_LEGACY_LIVE", "").strip().lower() in ("1", "true", "yes", "on")
    if debug_legacy:
        raise HTTPException(
            status_code=403,
            detail={
                "message": "MIP_DEBUG_LEGACY_LIVE is not allowed when LIVE_STRUCTURAL_ONLY is true.",
                "reason_codes": ["DEBUG_LEGACY_LIVE_FORBIDDEN_UNDER_STRUCTURAL_ONLY"],
            },
        )
    raise HTTPException(
        status_code=403,
        detail={
            "message": "Legacy ORDER_PROPOSALS import is disabled under LIVE_STRUCTURAL_ONLY. Use POST /live/trades/actions/import-structural-proposals.",
            "reason_codes": [
                "STRUCTURAL_ONLY_LIVE_POLICY",
                "LEGACY_ORDER_PROPOSALS_IMPORT_FORBIDDEN",
                "LIVE_STRUCTURAL_ONLY_ENFORCED",
            ],
        },
    )


def assert_live_committee_policy(
    row: dict | None,
    app_structural_only: bool,
    *,
    is_structural_fn: Callable[[dict | None], bool],
) -> None:
    """Under LIVE_STRUCTURAL_ONLY, only structural entry rows may use the live committee pipeline."""
    if not app_structural_only or not row:
        return
    kind = live_intent_kind_from_row(row)
    committee_required = bool(row.get("COMMITTEE_REQUIRED")) if row.get("COMMITTEE_REQUIRED") is not None else True
    if not committee_required:
        return
    if kind == LIVE_INTENT_OPERATOR_EXIT:
        raise HTTPException(
            status_code=409,
            detail={
                "message": "Operator exit rows do not use committee.",
                "reason_codes": ["OPERATOR_EXIT_COMMITTEE_NOT_APPLICABLE"],
            },
        )
    if kind in (LIVE_INTENT_LEGACY_PATTERN, LIVE_INTENT_UNKNOWN):
        raise HTTPException(
            status_code=409,
            detail={
                "message": "Legacy or unknown live intent rows cannot use committee under LIVE_STRUCTURAL_ONLY.",
                "reason_codes": ["LEGACY_LIVE_ROW_COMMITTEE_FORBIDDEN", "LIVE_STRUCTURAL_ONLY_ENFORCED"],
                "live_intent_kind": kind,
            },
        )
    if kind == LIVE_INTENT_STRUCTURAL and not is_structural_fn(row):
        raise HTTPException(
            status_code=409,
            detail={
                "message": "Row marked STRUCTURAL but missing structural identity fields.",
                "reason_codes": ["STRUCTURAL_INTENT_ROW_INCOMPLETE", "LIVE_STRUCTURAL_ONLY_ENFORCED"],
            },
        )


def assert_legacy_execute_forbidden(row: dict | None, app_structural_only: bool) -> None:
    if not app_structural_only or not row:
        return
    kind = live_intent_kind_from_row(row)
    if kind in (LIVE_INTENT_LEGACY_PATTERN, LIVE_INTENT_UNKNOWN):
        raise HTTPException(
            status_code=409,
            detail={
                "message": "Execution is blocked for legacy or unknown live intent rows under LIVE_STRUCTURAL_ONLY.",
                "reason_codes": ["LEGACY_LIVE_EXECUTE_FORBIDDEN", "LIVE_STRUCTURAL_ONLY_ENFORCED"],
                "live_intent_kind": kind,
            },
        )


def structural_proposal_minimum_contract_violations(p: dict) -> list[str]:
    violations: list[str] = []
    if p.get("SETUP_EVENT_ID") is None:
        violations.append("MISSING_SETUP_EVENT_ID")
    if not (p.get("SETUP_FAMILY") and str(p.get("SETUP_FAMILY")).strip()):
        violations.append("MISSING_SETUP_FAMILY")
    if not (p.get("DIRECTION") and str(p.get("DIRECTION")).strip()):
        violations.append("MISSING_DIRECTION")
    for fld in ("ENTRY_ZONE_LOW", "ENTRY_ZONE_HIGH"):
        if p.get(fld) is None:
            violations.append(f"MISSING_{fld}")
    if p.get("INVALIDATION_LEVEL") is None:
        violations.append("MISSING_INVALIDATION_LEVEL")
    if not (p.get("INVALIDATION_RULE") and str(p.get("INVALIDATION_RULE")).strip()):
        violations.append("MISSING_INVALIDATION_RULE")

    # Trailing fields are only required when the resolved EXIT_POLICY is
    # TRAIL_BRACKET. Trailing Stop Phase 1 requires the import path to
    # resolve EXIT_POLICY (from EXIT_PROFILE on the proposal/policy row)
    # BEFORE invoking this validator. If EXIT_POLICY is absent here, treat
    # as FIXED_BRACKET so we never raise premature MISSING_TRAIL_*
    # violations on rows that are effectively fixed by default.
    exit_policy = str(p.get("EXIT_POLICY") or "").strip().upper()
    if exit_policy == "TRAIL_BRACKET":
        if not (p.get("TRAIL_STYLE") and str(p.get("TRAIL_STYLE")).strip()):
            violations.append("MISSING_TRAIL_STYLE")
        tp_raw = p.get("TRAIL_PARAMS")
        if tp_raw is None:
            violations.append("MISSING_TRAIL_PARAMS")
        elif isinstance(tp_raw, dict) and not tp_raw:
            violations.append("MISSING_TRAIL_PARAMS")
        elif isinstance(tp_raw, str) and not tp_raw.strip():
            violations.append("MISSING_TRAIL_PARAMS")

    if not (p.get("EXIT_STYLE") and str(p.get("EXIT_STYLE")).strip()):
        violations.append("MISSING_EXIT_STYLE")
    if not (p.get("TRUST_LABEL") and str(p.get("TRUST_LABEL")).strip()):
        violations.append("MISSING_TRUST_LABEL")
    if not (p.get("STRUCTURAL_STATE") and str(p.get("STRUCTURAL_STATE")).strip()):
        violations.append("MISSING_STRUCTURAL_STATE")
    if not (p.get("REGIME_COMPAT") and str(p.get("REGIME_COMPAT")).strip()):
        violations.append("MISSING_REGIME_COMPAT")
    if not (p.get("RISK_CLASS") and str(p.get("RISK_CLASS")).strip()):
        violations.append("MISSING_RISK_CLASS")
    if not (p.get("SETUP_NARRATIVE") and str(p.get("SETUP_NARRATIVE")).strip()):
        violations.append("MISSING_SETUP_NARRATIVE")
    if not (p.get("PROPOSAL_RATIONALE") and str(p.get("PROPOSAL_RATIONALE")).strip()):
        violations.append("MISSING_PROPOSAL_RATIONALE")
    return violations


def overview_excluded_intent_kinds(*, include_legacy: bool) -> set[str]:
    if include_legacy:
        return set()
    return {LIVE_INTENT_LEGACY_PATTERN, LIVE_INTENT_UNKNOWN}
