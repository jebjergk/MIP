"""
Structural vs legacy live-action routing and persistence helpers.

Single source for: what counts as a structural row, payload hashing for contracts,
and version constants for audit/diagnostics.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

# Bump when structural committee logic or contract schema meaningfully changes.
STRUCTURAL_COMMITTEE_LOGIC_VERSION = "1.2.0"

STRUCTURAL_CONTRACT_VERSION = "1.0"

# Columns included in structural_execution_contract_v1.payload_hash (deterministic audit).
STRUCTURAL_PAYLOAD_HASH_KEYS: tuple[str, ...] = (
    "SETUP_EVENT_ID",
    "MARKET_TYPE",
    "SETUP_FAMILY",
    "DIRECTION",
    "SYMBOL",
    "SIDE",
    "ACTION_INTENT",
    "ENTRY_ZONE_LOW",
    "ENTRY_ZONE_HIGH",
    "SUPPORTING_LEVEL",
    "INVALIDATION_LEVEL",
    "INVALIDATION_RULE",
    "STRUCTURE_CONFIDENCE",
    "LEVEL_SIGNIFICANCE",
    "STRUCTURAL_STATE",
    "REGIME_TAGS",
    "REGIME_COMPAT",
    "TRUST_LABEL",
    "MEANINGFUL_HIT_RATE",
    "PATH_SURVIVAL_RATE",
    "MFE_MAE_RATIO",
    "AVG_BARS_TO_THRESHOLD",
    "DOMINANT_FAILURE_MODE",
    "BEST_WINDOW",
    "RISK_CLASS",
    "EXIT_STYLE",
    "TRAIL_STYLE",
    "TRAIL_PARAMS",
    "TRAIL_ACTIVATION_TYPE",
    "TRAIL_ACTIVATION_PARAM",
    "EXPECTED_HOLD_CHARACTER",
    "MAX_HOLD_BARS",
    "SETUP_NARRATIVE",
    "LATEST_BAR_DATE",
    "CURRENT_PRICE",
    "DISTANCE_TO_ENTRY_ZONE",
    "SETUP_STILL_VALID",
    "PRICE_MOVED_TOO_FAR",
    "FRESHNESS_ASSESSMENT",
)


def is_structural_live_action(action: dict | None) -> bool:
    """
    Structural rows must never use the legacy multi-agent committee.

    Predicate (any):
    - LIVE_INTENT_KIND = STRUCTURAL when column is populated
    - SETUP_EVENT_ID present (structural import always sets this)
    - SETUP_FAMILY present
    - PARAM_SNAPSHOT.structural_source or structural_execution_contract_v1 marks structural
    """
    if not action:
        return False
    lik = str(action.get("LIVE_INTENT_KIND") or "").strip().upper()
    if lik == "STRUCTURAL":
        return True
    if action.get("SETUP_EVENT_ID") is not None and str(action.get("SETUP_EVENT_ID")).strip() != "":
        return True
    if action.get("SETUP_FAMILY"):
        return True
    ps = action.get("PARAM_SNAPSHOT")
    if isinstance(ps, str):
        try:
            ps = json.loads(ps)
        except Exception:
            ps = None
    if isinstance(ps, dict):
        if ps.get("structural_source") is True:
            return True
        sec = ps.get("structural_execution_contract_v1")
        if isinstance(sec, dict) and sec.get("routing", {}).get("committee_model") == "STRUCTURAL_V1":
            return True
    return False


def structural_payload_hash(action: dict) -> str:
    """Stable short hash of canonical structural fields for contract versioning."""
    payload = {k: action.get(k) for k in STRUCTURAL_PAYLOAD_HASH_KEYS}
    raw = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def build_structural_verdict_envelope_v1(
    *,
    action: dict,
    verdict: dict,
    reason_codes: list[str],
    committee_run_id: str,
    outputs_wrapper: list | dict | None,
) -> dict:
    """Replaces legacy alpha Phase-3 + tier-C packaging for structural COMMITTEE_VERDICT JSON."""
    return {
        "structural_verdict_envelope_v1": {
            "committee_logic_version": STRUCTURAL_COMMITTEE_LOGIC_VERSION,
            "committee_run_id": committee_run_id,
            "payload_hash": structural_payload_hash(action),
            "routing": {
                "committee_model": "STRUCTURAL_V1",
                "legacy_phase3_envelope": False,
                "legacy_tier_c_evaluated": False,
            },
            "reason_codes_snapshot": list(reason_codes),
            "verdict_recommendation": verdict.get("recommendation"),
            "verdict_blocked": verdict.get("blocked"),
            "outputs": outputs_wrapper,
        }
    }


def build_structural_execution_contract_v1(
    *,
    action: dict,
    verdict: dict,
    joint_decision: dict | None,
    committee_run_id: str,
    param_snapshot_executable_bracket: dict | None,
    diagnostics: dict | None = None,
) -> dict:
    """Canonical contract object merged into PARAM_SNAPSHOT."""
    jd = joint_decision if isinstance(joint_decision, dict) else {}
    rec = str(verdict.get("recommendation") or "BLOCK").upper()
    blocked = bool(verdict.get("blocked"))
    decision = "DENY" if blocked else ("DEFER" if rec == "DEFER" else "APPROVE")
    trail = jd.get("trail") if isinstance(jd.get("trail"), dict) else {}
    contract: dict[str, Any] = {
        "contract_version": STRUCTURAL_CONTRACT_VERSION,
        "committee_logic_version": STRUCTURAL_COMMITTEE_LOGIC_VERSION,
        "committee_run_id": committee_run_id,
        "payload_hash": structural_payload_hash(action),
        "routing": {"committee_model": "STRUCTURAL_V1", "legacy_committee_forbidden": True},
        "decision": decision,
        "verdict_recommendation": rec,
        "verdict_blocked": blocked,
        "joint_decision": jd,
        "executable_bracket": param_snapshot_executable_bracket,
        "trail": {
            "style": trail.get("style") or action.get("TRAIL_STYLE"),
            "params": action.get("TRAIL_PARAMS"),
            "activation_type": action.get("TRAIL_ACTIVATION_TYPE"),
            "activation_param": action.get("TRAIL_ACTIVATION_PARAM"),
        },
        "structural_identity": {
            "setup_event_id": action.get("SETUP_EVENT_ID"),
            "setup_family": action.get("SETUP_FAMILY"),
            "direction": action.get("DIRECTION"),
            "market_type": action.get("MARKET_TYPE"),
        },
    }
    if diagnostics:
        contract["diagnostics_at_commit"] = diagnostics
    return contract


def contract_executable_bracket(contract: dict | None) -> dict | None:
    """Extract executable bracket dict from structural_execution_contract_v1 if present."""
    if not isinstance(contract, dict):
        return None
    eb = contract.get("executable_bracket")
    return eb if isinstance(eb, dict) else None
