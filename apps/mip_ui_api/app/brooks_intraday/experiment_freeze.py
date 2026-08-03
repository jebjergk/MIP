"""Phase 9 — frozen ruleset chain for unseen-week validation (immutable)."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone

from .constants import (
    COMPILER_VERSION_DEFAULT,
    DOSSIER_VERSION_DEFAULT,
    RECONSTRUCTION_VERSION_DEFAULT,
)
from .learning_constants import PILOT_RUN_ID
from .objective_ruleset_v01 import DEFAULT_PARAMETERS as OBJECTIVE_PARAMS
from .objective_ruleset_v01 import RULESET_VERSION as OBJECTIVE_RULESET
from .pattern_ruleset_v03 import DEFAULT_PARAMETERS as PATTERN_PARAMS
from .pattern_ruleset_v03 import RULESET_VERSION as PATTERN_RULESET
from .context_ruleset_v02 import DEFAULT_PARAMETERS as CONTEXT_PARAMS
from .context_ruleset_v02 import RULESET_VERSION as CONTEXT_RULESET
from .simulation_ruleset_v01 import DEFAULT_PARAMETERS as SIM_PARAMS
from .simulation_ruleset_v01 import RULESET_VERSION as SIMULATION_RULESET

FREEZE_ID = "BROOKS_EXPERIMENT_RULESET_FREEZE_V1"
BASELINE_WEEK_START = "2026-07-20"
BASELINE_RUN_ID = PILOT_RUN_ID

EXPERIMENT_ROLE = "UNSEEN_VALIDATION"
PILOT_SYMBOLS = ("AAPL", "AMZN", "JPM", "MCD")


def build_freeze_record(*, source_hashes: dict[str, str] | None = None) -> dict:
    """Create the experiment freeze payload (no outcome data)."""
    chain = {
        "objective_ruleset": OBJECTIVE_RULESET,
        "pattern_ruleset": PATTERN_RULESET,
        "context_ruleset": CONTEXT_RULESET,
        "simulation_ruleset": SIMULATION_RULESET,
        "reconstruction_version": RECONSTRUCTION_VERSION_DEFAULT,
        "dossier_compiler_version": COMPILER_VERSION_DEFAULT,
        "dossier_version": DOSSIER_VERSION_DEFAULT,
    }
    params_blob = {
        "objective_parameters": OBJECTIVE_PARAMS,
        "pattern_parameters": PATTERN_PARAMS,
        "context_parameters": CONTEXT_PARAMS,
        "simulation_parameters": SIM_PARAMS,
    }
    source_hashes = source_hashes or {}
    canonical = json.dumps({**chain, "parameters": params_blob, "source_hashes": source_hashes}, sort_keys=True)
    chain_hash = hashlib.sha256(canonical.encode()).hexdigest()
    return {
        "freeze_id": FREEZE_ID,
        "freeze_timestamp_utc": datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec="seconds"),
        "baseline_run_id": BASELINE_RUN_ID,
        "baseline_week_start": BASELINE_WEEK_START,
        "ruleset_chain": chain,
        "parameters": params_blob,
        "source_hashes": source_hashes,
        "chain_hash": chain_hash,
        "experiment_role": EXPERIMENT_ROLE,
        "note": "Any rule change requires a new freeze version and new validation runs.",
    }


def assert_run_uses_freeze(cfg: dict, freeze: dict | None = None) -> None:
    freeze = freeze or build_freeze_record()
    expected = freeze["freeze_id"]
    if cfg.get("experiment_freeze_id") != expected:
        raise ValueError(f"Run must use experiment freeze {expected}")
