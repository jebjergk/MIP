"""
Trailing Stop Phase 1 — Exit Policy Service.

EXIT_POLICY is a first-class execution contract on LIVE_ACTIONS. This module
defines the bounded set of exit profiles, resolves a profile into an
executable policy, validates broker-executable trail params, and emits the
broker arguments consumed by place_ibkr_order.py.

Phase 1 scope:
  - PCT trail mode only (ABS deferred)
  - Reference = ENTRY_FILL only
  - Four bounded profiles: FIXED_STANDARD, TRAIL_TIGHT, TRAIL_STANDARD, TRAIL_WIDE
  - Override precedence is extensible but Phase 1 only uses
    action.EXIT_PROFILE -> default FIXED_STANDARD.

Hard rules:
  - Unknown profile raises ValueError. Never silently coerces to default.
  - Validation never silently downgrades a TRAIL_BRACKET to FIXED_BRACKET.
"""
from __future__ import annotations

from typing import Any

POLICY_VERSION = "v1"

EXIT_POLICY_FIXED = "FIXED_BRACKET"
EXIT_POLICY_TRAIL = "TRAIL_BRACKET"

TRAIL_STATUS_NOT_REQUESTED = "NOT_REQUESTED"
TRAIL_STATUS_REQUESTED = "REQUESTED"

TRAIL_MODE_PCT = "PCT"
TRAIL_MODE_ABS = "ABS"

TRAIL_REFERENCE_ENTRY_FILL = "ENTRY_FILL"

TP_MODE_LIMIT = "LIMIT"

SUPPORTED_TRAIL_MODES: frozenset[str] = frozenset({TRAIL_MODE_PCT})
SUPPORTED_TRAIL_REFERENCES: frozenset[str] = frozenset({TRAIL_REFERENCE_ENTRY_FILL})

TRAIL_VALUE_BOUNDS: dict[str, tuple[float, float]] = {
    TRAIL_MODE_PCT: (0.5, 10.0),
}

PROFILES: dict[str, dict[str, Any]] = {
    "FIXED_STANDARD": {
        "exit_policy": EXIT_POLICY_FIXED,
        "trail_status": TRAIL_STATUS_NOT_REQUESTED,
        "trail_style": None,
        "trail_params": None,
    },
    "TRAIL_TIGHT": {
        "exit_policy": EXIT_POLICY_TRAIL,
        "trail_status": TRAIL_STATUS_REQUESTED,
        "trail_style": TRAIL_MODE_PCT,
        "trail_params": {
            "trail_mode": TRAIL_MODE_PCT,
            "trail_value": 1.5,
            "reference": TRAIL_REFERENCE_ENTRY_FILL,
            "tp_mode": TP_MODE_LIMIT,
            "profile": "TRAIL_TIGHT",
            "policy_version": POLICY_VERSION,
        },
    },
    "TRAIL_STANDARD": {
        "exit_policy": EXIT_POLICY_TRAIL,
        "trail_status": TRAIL_STATUS_REQUESTED,
        "trail_style": TRAIL_MODE_PCT,
        "trail_params": {
            "trail_mode": TRAIL_MODE_PCT,
            "trail_value": 2.5,
            "reference": TRAIL_REFERENCE_ENTRY_FILL,
            "tp_mode": TP_MODE_LIMIT,
            "profile": "TRAIL_STANDARD",
            "policy_version": POLICY_VERSION,
        },
    },
    "TRAIL_WIDE": {
        "exit_policy": EXIT_POLICY_TRAIL,
        "trail_status": TRAIL_STATUS_REQUESTED,
        "trail_style": TRAIL_MODE_PCT,
        "trail_params": {
            "trail_mode": TRAIL_MODE_PCT,
            "trail_value": 4.0,
            "reference": TRAIL_REFERENCE_ENTRY_FILL,
            "tp_mode": TP_MODE_LIMIT,
            "profile": "TRAIL_WIDE",
            "policy_version": POLICY_VERSION,
        },
    },
}

DEFAULT_PROFILE = "FIXED_STANDARD"


def known_profiles() -> tuple[str, ...]:
    return tuple(PROFILES.keys())


def _clone_profile(profile: str) -> dict[str, Any]:
    src = PROFILES[profile]
    cloned: dict[str, Any] = {
        "exit_policy": src["exit_policy"],
        "trail_status": src["trail_status"],
        "trail_style": src["trail_style"],
        "trail_params": dict(src["trail_params"]) if src["trail_params"] else None,
        "resolved_profile": profile,
    }
    return cloned


def resolve_exit_policy(profile: str | None) -> dict[str, Any]:
    """Direct profile lookup. Raises ValueError on unknown profile."""
    if not profile or not str(profile).strip():
        raise ValueError("EXIT_PROFILE is empty")
    name = str(profile).strip().upper()
    if name not in PROFILES:
        raise ValueError(f"Unknown EXIT_PROFILE: {profile!r}")
    return _clone_profile(name)


def resolve_exit_policy_for_action(
    action: dict[str, Any],
    *,
    explicit_override: str | None = None,
) -> dict[str, Any]:
    """
    Resolve a profile for a single action with extensible precedence.

    Precedence (Phase 1 actively uses steps 3-4; 1-2 are future hooks):
      1. explicit_override argument (caller-supplied, e.g. UI override)
      2. action["EXIT_POLICY_OVERRIDE"]      (future per-proposal override)
      3. action["EXIT_PROFILE"]              (from policy/proposal table)
      4. DEFAULT_PROFILE                     (FIXED_STANDARD)

    Raises ValueError on unknown profile.
    """
    candidate = (
        explicit_override
        or action.get("EXIT_POLICY_OVERRIDE")
        or action.get("EXIT_PROFILE")
        or DEFAULT_PROFILE
    )
    return resolve_exit_policy(candidate)


def validate_trail_params(params: Any) -> list[str]:
    """
    Validate broker-executable TRAIL_PARAMS shape. Returns a list of
    violation reason codes (empty list = valid).

    Never raises. Caller is responsible for blocking execution on
    non-empty violation list.
    """
    violations: list[str] = []
    if not isinstance(params, dict) or not params:
        return ["TRAIL_PARAMS_NOT_OBJECT"]

    mode_raw = params.get("trail_mode")
    mode = str(mode_raw).strip().upper() if mode_raw is not None else ""
    if not mode:
        violations.append("MISSING_TRAIL_MODE")
    elif mode not in SUPPORTED_TRAIL_MODES:
        violations.append("UNSUPPORTED_TRAIL_MODE")

    val_raw = params.get("trail_value")
    if val_raw is None:
        violations.append("MISSING_TRAIL_VALUE")
    else:
        try:
            val = float(val_raw)
        except (TypeError, ValueError):
            violations.append("TRAIL_VALUE_NOT_NUMERIC")
        else:
            if mode in TRAIL_VALUE_BOUNDS:
                lo, hi = TRAIL_VALUE_BOUNDS[mode]
                if not (lo <= val <= hi):
                    violations.append("TRAIL_VALUE_OUT_OF_BOUNDS")

    reference = str(params.get("reference") or "").strip().upper()
    if not reference:
        violations.append("MISSING_TRAIL_REFERENCE")
    elif reference not in SUPPORTED_TRAIL_REFERENCES:
        violations.append("UNSUPPORTED_TRAIL_REFERENCE")

    policy_version = str(params.get("policy_version") or "").strip()
    if not policy_version:
        violations.append("MISSING_POLICY_VERSION")
    elif policy_version != POLICY_VERSION:
        violations.append("UNSUPPORTED_POLICY_VERSION")

    return violations


def broker_trail_args(params: dict[str, Any]) -> tuple[float | None, float | None]:
    """
    Convert broker-executable TRAIL_PARAMS into (trail_amount, trail_percent)
    for place_ibkr_order.py.

    PCT mode → (None, trail_value)
    ABS mode → (trail_value, None)   [reserved for Phase 2; raises today]

    Raises ValueError on unsupported mode. Caller MUST validate first via
    validate_trail_params() to avoid an exception here.
    """
    if not isinstance(params, dict):
        raise ValueError("trail_params must be a dict")
    mode = str(params.get("trail_mode") or "").strip().upper()
    val_raw = params.get("trail_value")
    if val_raw is None:
        raise ValueError("trail_value missing")
    val = float(val_raw)
    if mode == TRAIL_MODE_PCT:
        return (None, val)
    if mode == TRAIL_MODE_ABS:
        raise ValueError("ABS trail_mode is not supported in Phase 1")
    raise ValueError(f"Unsupported trail_mode: {mode!r}")
