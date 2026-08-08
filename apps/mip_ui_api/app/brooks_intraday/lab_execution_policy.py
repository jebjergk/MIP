"""Brooks Lab execution modes — block obsolete bulk pipeline unless explicitly opted in."""

from __future__ import annotations

import os
from typing import Any

from .errors import BrooksIntradayError
from .learning_constants import (
    PHASE_E1_CONTEXT_ATTEMPT_ID,
    PHASE_E1_SIMULATION_ATTEMPT_ID,
    PM_V01_CERT_SIMULATION_ATTEMPT_ID,
)

from .adviser_baseline_v01 import ADVISER_VERSION

# Target architecture profile (Adviser foundation).
LAB_PIPELINE_PROFILE_ADVISER_V1_0 = ADVISER_VERSION
LAB_EXECUTION_MODE_ADVISER_FOUNDATION = "ADVISER_FOUNDATION"
LAB_EXECUTION_MODE_DIAGNOSTIC_LEGACY = "DIAGNOSTIC_LEGACY"

# Frozen diagnostic baselines — never delete/rerun; Learning View review-only.
PRESERVED_CONTEXT_ATTEMPT_IDS = frozenset(
    {
        PHASE_E1_CONTEXT_ATTEMPT_ID,  # Week 1 V0.3 context
        "e35b6713-ee67-42f6-bc30-f18aa5bee56d",  # V0.4 W1 context (disposable)
        "e912b33d-59cf-4c4c-a95c-352e6b39935a",  # V0.4 W2 context (disposable)
    }
)
PRESERVED_SIMULATION_ATTEMPT_IDS = frozenset(
    {
        PHASE_E1_SIMULATION_ATTEMPT_ID,  # Baseline V0.1 simulation
        PM_V01_CERT_SIMULATION_ATTEMPT_ID,  # Canonical PM certification
        "03eaf144-8744-4637-81ea-cec9e1b0cef7",  # V0.4 W1 sim
        "622321da-4d28-4e42-8345-d22ea3b0c3ea",  # V0.4 W2 sim
    }
)

DIAGNOSTIC_LEGACY_RULESETS = frozenset(
    {
        "BROOKS_CONTEXT_RULESET_V0_1",
        "BROOKS_CONTEXT_RULESET_V0_2",
        "BROOKS_CONTEXT_RULESET_V0_3",
        "BROOKS_CONTEXT_RULESET_V0_4",
        "BROOKS_PATTERN_RULESET_V0_1",
        "BROOKS_PATTERN_RULESET_V0_2",
        "BROOKS_PATTERN_RULESET_V0_3",
    }
)

_LEGACY_PIPELINE_ENV = "BROOKS_LAB_LEGACY_PIPELINE_ENABLED"


def legacy_pipeline_env_enabled() -> bool:
    return os.environ.get(_LEGACY_PIPELINE_ENV, "").strip().lower() in ("1", "true", "yes")


def get_lab_execution_mode(state: dict[str, Any]) -> str:
    cfg = state.get("configuration") or {}
    return str(
        cfg.get("lab_execution_mode")
        or state.get("lab_execution_mode")
        or LAB_EXECUTION_MODE_ADVISER_FOUNDATION
    )


def get_lab_pipeline_profile(state: dict[str, Any]) -> str:
    cfg = state.get("configuration") or {}
    return str(
        cfg.get("lab_pipeline_profile")
        or state.get("lab_pipeline_profile")
        or LAB_PIPELINE_PROFILE_ADVISER_V1_0
    )


def adviser_foundation_defaults() -> dict[str, str]:
    return {
        "lab_execution_mode": LAB_EXECUTION_MODE_ADVISER_FOUNDATION,
        "lab_pipeline_profile": LAB_PIPELINE_PROFILE_ADVISER_V1_0,
    }


def is_diagnostic_legacy_mode(state: dict[str, Any]) -> bool:
    return get_lab_execution_mode(state) == LAB_EXECUTION_MODE_DIAGNOSTIC_LEGACY


def require_diagnostic_legacy(
    state: dict[str, Any],
    operation: str,
    *,
    allow_diagnostic_legacy: bool = False,
) -> None:
    """
    Raise when an obsolete bulk/diagnostic stage is invoked on an Adviser-foundation run.

    Allowed when any of:
    - run lab_execution_mode is DIAGNOSTIC_LEGACY
    - allow_diagnostic_legacy=True (API query param or vetted script)
    - BROOKS_LAB_LEGACY_PIPELINE_ENABLED=1 (operator env for Phase 9 / scripts)
    """
    if is_diagnostic_legacy_mode(state):
        return
    if allow_diagnostic_legacy or legacy_pipeline_env_enabled():
        return
    run_id = state.get("run_id")
    raise BrooksIntradayError(
        "DIAGNOSTIC_LEGACY_REQUIRED",
        (
            f"{operation} is classified DIAGNOSTIC_LEGACY and is disabled for "
            f"lab_execution_mode={get_lab_execution_mode(state)}. "
            "Use ?diagnostic_legacy=true on the API, create a run with "
            "lab_execution_mode=DIAGNOSTIC_LEGACY, or set "
            f"{_LEGACY_PIPELINE_ENV}=1 for controlled scripts."
        ),
        run_id=run_id,
        status_code=403,
        details={
            "operation": operation,
            "lab_execution_mode": get_lab_execution_mode(state),
            "lab_pipeline_profile": get_lab_pipeline_profile(state),
            "classification": "DIAGNOSTIC_LEGACY",
        },
    )


def meta_execution_policy() -> dict[str, Any]:
    return {
        "lab_pipeline_profile": LAB_PIPELINE_PROFILE_ADVISER_V1_0,
        "default_lab_execution_mode": LAB_EXECUTION_MODE_ADVISER_FOUNDATION,
        "diagnostic_legacy_rulesets": sorted(DIAGNOSTIC_LEGACY_RULESETS),
        "legacy_pipeline_env_var": _LEGACY_PIPELINE_ENV,
        "legacy_pipeline_env_enabled": legacy_pipeline_env_enabled(),
        "preserved_context_attempt_ids": sorted(PRESERVED_CONTEXT_ATTEMPT_IDS),
        "preserved_simulation_attempt_ids": sorted(PRESERVED_SIMULATION_ATTEMPT_IDS),
    }
