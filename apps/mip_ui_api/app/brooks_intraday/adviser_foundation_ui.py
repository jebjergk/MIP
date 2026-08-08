"""Normal Lab UX vs diagnostic legacy review (Adviser foundation — V1.0 only)."""

from __future__ import annotations

from typing import Any

from .adviser_baseline_v01 import ADVISER_VERSION, is_canonical_adviser_foundation

V1_NO_ACTIVE_SESSIONS_MESSAGE = (
    "No active V1.0 validation sessions (archived attempts are kept in Snowflake). "
    "Use New Validation to run a symbol/day again."
)

ADVISER_EMPTY_STATE_MESSAGE = (
    "Brooks Adviser has not yet been run for this session. "
    "Prepare a run and replay bars; Adviser thesis and watch conditions will appear here when available."
)

FOUNDATION_CONTEXT_RULESET = ADVISER_VERSION
FOUNDATION_SIMULATION_RULESET = "BROOKS_ADVISER_SIMULATION_V0_1"

_LEGACY_CONTEXT_PREFIXES = (
    "BROOKS_CONTEXT_RULESET_V0_",
    "BROOKS_CONTEXT_RULESET_V0",
)


def is_adviser_foundation_chain(chain: dict[str, Any]) -> bool:
    return is_canonical_adviser_foundation(chain)


def is_legacy_review_chain(chain: dict[str, Any]) -> bool:
    """Forensic V0.x / official baseline / PM cert chains (hidden from normal UX)."""
    if chain.get("official"):
        return True
    if chain.get("canonical_pm_certification"):
        return True
    if chain.get("inactive_pm_ruleset"):
        return True
    ctx_rs = str(chain.get("context_ruleset") or "")
    if any(ctx_rs.startswith(p) or p in ctx_rs for p in _LEGACY_CONTEXT_PREFIXES):
        return True
    if "BROOKS_CONTEXT_RULESET_V0_" in ctx_rs:
        return True
    sim_rs = str(chain.get("simulation_ruleset") or "").upper()
    if "BROOKS_SIMULATION_RULESET_V0_" in sim_rs:
        return True
    if "POSITION_MANAGEMENT" in sim_rs and not is_adviser_foundation_chain(chain):
        return True
    if "BROOKS_PATTERN" in str(chain.get("pattern_ruleset") or ""):
        return True
    # Disposable V0.3/V0.4 and Phase E chains are legacy diagnostic.
    notes = str(chain.get("notes") or "").lower()
    if "v0.3" in notes or "v0.4" in notes or "phase e" in notes or "phase 6" in notes:
        return True
    label = str(chain.get("primary_label") or chain.get("label") or "").lower()
    if "official baseline" in label or "official pinned" in label:
        return True
    if "v0.2" in label or "v0.3" in label or "v0.4" in label:
        return True
    if "position-management" in label or "position management" in label:
        return True
    if "context_ruleset_v0" in label.replace(" ", ""):
        return True
    return False


def filter_chains_for_ui(
    chains: list[dict[str, Any]],
    *,
    diagnostic_legacy: bool,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for chain in chains:
        if chain.get("disabled"):
            continue
        if chain.get("adviser_attempt_id") and not is_canonical_adviser_foundation(chain):
            continue
        if diagnostic_legacy:
            out.append(chain)
        elif is_adviser_foundation_chain(chain):
            out.append(chain)
    return out


def foundation_catalog_defaults(run_entry: dict[str, Any] | None) -> dict[str, Any]:
    """Session-first selection; no legacy chain or trade pins."""
    if not run_entry:
        return {}
    sym_sessions = run_entry.get("available_symbol_sessions") or []
    first = sym_sessions[0] if sym_sessions else {}
    symbols = run_entry.get("symbols") or []
    return {
        "run_id": run_entry.get("run_id"),
        "symbol": first.get("symbol") or (symbols[0] if symbols else None),
        "trading_date": first.get("trading_date"),
        "review_mode": "sessions",
        "lab_mode": "adviser_foundation",
    }


def assert_legacy_review_access(
    *,
    diagnostic_legacy: bool,
    context_attempt_id: str | None,
    simulation_attempt_id: str | None,
    context_ruleset: str | None = None,
) -> None:
    """Raise ValueError when normal UI tries to load a legacy attempt chain."""
    if diagnostic_legacy:
        return
    if not context_attempt_id and not simulation_attempt_id:
        return
    if is_adviser_foundation_chain(
        {
            "context_ruleset": context_ruleset or FOUNDATION_CONTEXT_RULESET,
            "context_attempt_id": context_attempt_id,
            "simulation_attempt_id": simulation_attempt_id,
        }
    ):
        return
    # Any explicit legacy UUID load in foundation mode is forbidden.
    from .lab_execution_policy import (
        PRESERVED_CONTEXT_ATTEMPT_IDS,
        PRESERVED_SIMULATION_ATTEMPT_IDS,
    )

    ctx = str(context_attempt_id or "")
    sim = str(simulation_attempt_id or "")
    if ctx in PRESERVED_CONTEXT_ATTEMPT_IDS or sim in PRESERVED_SIMULATION_ATTEMPT_IDS:
        raise ValueError(
            "Legacy review chains are not available in Adviser foundation mode. "
            "Add ?diagnostic_legacy=true for forensic review."
        )
    if context_ruleset and is_legacy_review_chain({"context_ruleset": context_ruleset}):
        raise ValueError(
            "Legacy context ruleset review is disabled in foundation mode. "
            "Use ?diagnostic_legacy=true for forensic access."
        )
    # Unknown attempt pair — still block if not adviser ruleset (fail closed for foundation UX).
    if context_ruleset and context_ruleset != FOUNDATION_CONTEXT_RULESET:
        raise ValueError(
            "Only Adviser foundation review is enabled in normal Lab mode. "
            "Use ?diagnostic_legacy=true to inspect legacy attempts."
        )
