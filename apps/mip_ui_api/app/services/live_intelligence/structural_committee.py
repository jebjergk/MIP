"""
Structural Committee — rule-based validator for structural trade proposals.

Consumes the canonical structural live-intent object and produces a shaped
decision with explicit ownership:
  - Approve / Deny / Defer
  - Freshness assessment
  - Risk note
  - Adjusted parameters (within policy bounds only)
  - Hold-character interpretation

Does NOT invent structural thesis — only validates and shapes.
"""

from __future__ import annotations

import json
from typing import Any


# ── Trust gate thresholds ─────────────────────────────────────────────
TRUST_GATE = {
    "TRUSTED":     {"min_mhr": 0.0,  "max_size_factor": 1.0,  "allow_entry": True},
    "PROVISIONAL": {"min_mhr": 0.30, "max_size_factor": 0.7,  "allow_entry": True},
    "RESEARCH":    {"min_mhr": 0.25, "max_size_factor": 0.4,  "allow_entry": True},
    "REJECTED":    {"min_mhr": 0.0,  "max_size_factor": 0.0,  "allow_entry": False},
    "UNKNOWN":     {"min_mhr": 0.25, "max_size_factor": 0.3,  "allow_entry": True},
}

# ── Regime compatibility scoring ──────────────────────────────────────
REGIME_SIZE_MULT = {
    "GOOD":    1.0,
    "NEUTRAL": 0.7,
    "POOR":    0.0,
}

# ── Freshness thresholds ─────────────────────────────────────────────
MAX_DISTANCE_FOR_CURRENT = 1.0       # % from entry zone
MAX_DISTANCE_FOR_STALE_VALID = 3.0   # % from entry zone

# ── Policy adjustment bounds (committee may adjust within these) ─────
ENTRY_ZONE_ADJUST_MAX_PCT = 5.0      # max % widening of entry zone
INVALIDATION_ADJUST_MAX_PCT = 10.0   # max % tightening of invalidation
TRAIL_ACTIVATION_ADJUST_RANGE = 0.5  # +/- 50% of policy default


def _safe_float(v, default=None) -> float | None:
    if v is None:
        return default
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _evaluate_freshness(action: dict) -> dict:
    """Re-evaluate freshness at committee time using action's stored freshness fields."""
    stored = (action.get("FRESHNESS_ASSESSMENT") or "").upper()
    still_valid = action.get("SETUP_STILL_VALID")
    moved_too_far = action.get("PRICE_MOVED_TOO_FAR")
    distance = _safe_float(action.get("DISTANCE_TO_ENTRY_ZONE"))

    if stored == "STALE_INVALID" or moved_too_far or not still_valid:
        return {
            "freshness": "STALE_INVALID",
            "reason": "Setup invalidated or price moved beyond threshold",
            "distance_pct": distance,
        }
    if distance is not None and distance > MAX_DISTANCE_FOR_STALE_VALID:
        return {
            "freshness": "STALE_INVALID",
            "reason": f"Price {distance:.1f}% from entry zone exceeds {MAX_DISTANCE_FOR_STALE_VALID}% limit",
            "distance_pct": distance,
        }
    if distance is not None and distance > MAX_DISTANCE_FOR_CURRENT:
        return {
            "freshness": "STALE_BUT_VALID",
            "reason": f"Price {distance:.1f}% from entry zone — valid but not ideal",
            "distance_pct": distance,
        }
    return {
        "freshness": "CURRENT",
        "reason": "Price within entry zone",
        "distance_pct": distance,
    }


def _evaluate_trust(action: dict) -> dict:
    """Evaluate trust label and meaningful hit rate against gate thresholds."""
    trust_label = (action.get("TRUST_LABEL") or "UNKNOWN").upper()
    mhr = _safe_float(action.get("MEANINGFUL_HIT_RATE"), 0.0)
    gate = TRUST_GATE.get(trust_label, TRUST_GATE["UNKNOWN"])

    if not gate["allow_entry"]:
        return {
            "passed": False,
            "trust_label": trust_label,
            "mhr": mhr,
            "max_size_factor": 0.0,
            "reason": f"Trust label {trust_label} blocks entry",
        }
    if mhr < gate["min_mhr"]:
        return {
            "passed": False,
            "trust_label": trust_label,
            "mhr": mhr,
            "max_size_factor": 0.0,
            "reason": f"MHR {mhr:.1%} below {gate['min_mhr']:.0%} threshold for {trust_label}",
        }
    return {
        "passed": True,
        "trust_label": trust_label,
        "mhr": mhr,
        "max_size_factor": gate["max_size_factor"],
        "reason": f"{trust_label} at {mhr:.1%} MHR — acceptable",
    }


def _evaluate_regime(action: dict) -> dict:
    """Evaluate regime compatibility."""
    compat = (action.get("REGIME_COMPAT") or "NEUTRAL").upper()
    mult = REGIME_SIZE_MULT.get(compat, 0.5)

    if compat == "POOR":
        return {
            "passed": False,
            "regime_compat": compat,
            "size_mult": mult,
            "reason": "POOR regime compatibility blocks entry",
        }
    return {
        "passed": True,
        "regime_compat": compat,
        "size_mult": mult,
        "reason": f"Regime {compat} — size multiplier {mult}",
    }


def _evaluate_path_quality(action: dict) -> dict:
    """Evaluate path statistics quality."""
    mfe_mae = _safe_float(action.get("MFE_MAE_RATIO"))
    survival = _safe_float(action.get("PATH_SURVIVAL_RATE"))
    confidence = _safe_float(action.get("STRUCTURE_CONFIDENCE"))

    flags: list[str] = []
    if mfe_mae is not None and mfe_mae < 1.0:
        flags.append(f"MFE/MAE ratio {mfe_mae:.2f} < 1.0 — adverse paths dominate")
    if survival is not None and survival < 0.25:
        flags.append(f"Path survival {survival:.0%} is very low")
    if confidence is not None and confidence < 0.5:
        flags.append(f"Structure confidence {confidence:.2f} is weak")

    return {
        "mfe_mae_ratio": mfe_mae,
        "path_survival": survival,
        "structure_confidence": confidence,
        "flags": flags,
        "quality_ok": len(flags) == 0,
    }


def _compute_size_factor(
    trust_eval: dict,
    regime_eval: dict,
    freshness_eval: dict,
    path_eval: dict,
) -> float:
    """Compute composite size factor from committee evaluations."""
    base = trust_eval.get("max_size_factor", 1.0)
    regime_mult = regime_eval.get("size_mult", 1.0)

    freshness_mult = 1.0
    if freshness_eval["freshness"] == "STALE_BUT_VALID":
        freshness_mult = 0.6

    path_mult = 1.0
    if not path_eval["quality_ok"]:
        path_mult = 0.7

    return round(base * regime_mult * freshness_mult * path_mult, 4)


def _build_risk_note(
    action: dict,
    trust_eval: dict,
    regime_eval: dict,
    freshness_eval: dict,
    path_eval: dict,
) -> str:
    """Build a concise risk note from evaluations."""
    parts: list[str] = []
    symbol = action.get("SYMBOL") or "?"
    direction = action.get("DIRECTION") or "?"
    family = action.get("SETUP_FAMILY") or "?"

    parts.append(f"{family} {direction} on {symbol}.")

    if not trust_eval["passed"]:
        parts.append(f"BLOCKED: {trust_eval['reason']}.")
    if not regime_eval["passed"]:
        parts.append(f"BLOCKED: {regime_eval['reason']}.")
    if freshness_eval["freshness"] == "STALE_INVALID":
        parts.append(f"BLOCKED: {freshness_eval['reason']}.")
    elif freshness_eval["freshness"] == "STALE_BUT_VALID":
        parts.append(f"CAUTION: {freshness_eval['reason']}.")

    for flag in path_eval.get("flags", []):
        parts.append(f"WARNING: {flag}.")

    if not parts[1:]:
        parts.append("No issues detected.")

    return " ".join(parts)


def _validate_trail_style(action: dict) -> dict:
    """Validate and potentially adjust trail style."""
    trail_style = action.get("TRAIL_STYLE")
    trail_activation = action.get("TRAIL_ACTIVATION_TYPE")
    freshness = (action.get("FRESHNESS_ASSESSMENT") or "CURRENT").upper()

    recommendation = trail_style
    adjusted = False
    note = None

    if freshness == "STALE_BUT_VALID" and trail_activation != "IMMEDIATE":
        note = "Stale freshness — recommend deferring trailing activation"

    return {
        "intended_trail_style": trail_style,
        "committee_trail_style": recommendation,
        "adjusted": adjusted,
        "note": note,
    }


def run_structural_committee(action: dict) -> dict:
    """
    Run the structural committee evaluation on a canonical live-intent action.

    Input: action dict with all structural columns from LIVE_ACTIONS.
    Output: committee verdict with recommendation, size_factor, evaluations, risk note.
    """
    # 1. Evaluate each dimension
    freshness_eval = _evaluate_freshness(action)
    trust_eval = _evaluate_trust(action)
    regime_eval = _evaluate_regime(action)
    path_eval = _evaluate_path_quality(action)
    trail_eval = _validate_trail_style(action)

    # 2. Determine recommendation
    blocked = False
    block_reasons: list[str] = []
    reason_codes: list[str] = []

    if freshness_eval["freshness"] == "STALE_INVALID":
        blocked = True
        block_reasons.append(freshness_eval["reason"])
        reason_codes.append("SETUP_STALE_INVALID")

    if not trust_eval["passed"]:
        blocked = True
        block_reasons.append(trust_eval["reason"])
        reason_codes.append("TRUST_GATE_FAILED")

    if not regime_eval["passed"]:
        blocked = True
        block_reasons.append(regime_eval["reason"])
        reason_codes.append("REGIME_INCOMPATIBLE")

    # Path quality is a warning, not a hard block
    if not path_eval["quality_ok"]:
        reason_codes.append("PATH_QUALITY_DEGRADED")

    if freshness_eval["freshness"] == "STALE_BUT_VALID":
        reason_codes.append("FRESHNESS_STALE_BUT_VALID")

    if trail_eval.get("note"):
        reason_codes.append("TRAIL_DEFER_RECOMMENDED")

    # 3. Compute size factor
    size_factor = 0.0 if blocked else _compute_size_factor(
        trust_eval, regime_eval, freshness_eval, path_eval
    )

    recommendation = "BLOCK" if blocked else (
        "PROCEED_REDUCED" if size_factor < 0.9 else "PROCEED"
    )

    # 4. Build risk note
    risk_note = _build_risk_note(action, trust_eval, regime_eval, freshness_eval, path_eval)

    # 5. Build hold character interpretation
    max_hold = action.get("MAX_HOLD_BARS")
    hold_character = action.get("EXPECTED_HOLD_CHARACTER") or "MEDIUM_SWING"

    # 6. Derive entry/exit parameters (committee shapes, does not invent)
    entry_zone_low = _safe_float(action.get("ENTRY_ZONE_LOW"))
    entry_zone_high = _safe_float(action.get("ENTRY_ZONE_HIGH"))
    invalidation = _safe_float(action.get("INVALIDATION_LEVEL"))
    direction = (action.get("DIRECTION") or "LONG").upper()

    # Committee does not adjust entry zone or invalidation by default —
    # only passes through the proposal values. Adjustments would be added here
    # if policy-driven tightening rules are implemented.
    committee_entry_zone_low = entry_zone_low
    committee_entry_zone_high = entry_zone_high
    committee_invalidation = invalidation

    # 7. Joint decision (compatible with existing committee verdict schema)
    jd = {
        "should_enter": not blocked,
        "recommendation": recommendation,
        "size_factor": size_factor,
        "risk_note": risk_note,
        "hold_bars": max_hold,
        "hold_character": hold_character,
        "realistic_target_return": None,
        "stop_loss_pct": None,
        "acceptable_early_exit_target_return": None,
        "entry_zone": {
            "low": committee_entry_zone_low,
            "high": committee_entry_zone_high,
            "adjusted": False,
        },
        "invalidation": {
            "level": committee_invalidation,
            "rule": action.get("INVALIDATION_RULE"),
            "adjusted": False,
        },
        "trail": {
            "style": trail_eval.get("committee_trail_style"),
            "activation_type": action.get("TRAIL_ACTIVATION_TYPE"),
            "activation_param": _safe_float(action.get("TRAIL_ACTIVATION_PARAM")),
            "adjusted": trail_eval.get("adjusted", False),
            "note": trail_eval.get("note"),
        },
    }

    # Derive stop_loss_pct from invalidation distance
    current_price = _safe_float(action.get("CURRENT_PRICE"))
    if current_price and invalidation and current_price > 0:
        if direction == "LONG":
            jd["stop_loss_pct"] = round(abs(current_price - invalidation) / current_price, 6)
        else:
            jd["stop_loss_pct"] = round(abs(invalidation - current_price) / current_price, 6)

    # 8. Build role outputs (structural roles, not legacy agent stubs)
    role_outputs = _build_structural_role_outputs(
        action, freshness_eval, trust_eval, regime_eval, path_eval, trail_eval
    )

    return {
        "recommendation": recommendation,
        "size_factor": size_factor,
        "confidence": _compute_confidence(trust_eval, regime_eval, path_eval),
        "blocked": blocked,
        "block_reasons": block_reasons,
        "reason_codes": reason_codes,
        "risk_note": risk_note,
        "joint_decision": jd,
        "evaluations": {
            "freshness": freshness_eval,
            "trust": trust_eval,
            "regime": regime_eval,
            "path_quality": path_eval,
            "trail": trail_eval,
        },
        "structural_source": True,
    }


def _compute_confidence(trust_eval: dict, regime_eval: dict, path_eval: dict) -> float:
    """Compute aggregate confidence 0-1."""
    trust_conf = 0.8 if trust_eval["passed"] else 0.2
    regime_conf = {"GOOD": 0.9, "NEUTRAL": 0.6, "POOR": 0.2}.get(
        regime_eval.get("regime_compat", "NEUTRAL"), 0.5
    )
    path_conf = 0.8 if path_eval.get("quality_ok") else 0.4
    return round((trust_conf * 0.4 + regime_conf * 0.3 + path_conf * 0.3), 4)


def _build_structural_role_outputs(
    action: dict,
    freshness_eval: dict,
    trust_eval: dict,
    regime_eval: dict,
    path_eval: dict,
    trail_eval: dict,
) -> list[dict[str, Any]]:
    """Build committee role outputs using structural evaluations."""
    symbol = action.get("SYMBOL") or "?"
    family = action.get("SETUP_FAMILY") or "?"
    direction = action.get("DIRECTION") or "?"

    roles = []

    # StructuralValidator
    roles.append({
        "role": "StructuralValidator",
        "stance": "SUPPORT" if freshness_eval["freshness"] != "STALE_INVALID" else "BLOCK",
        "confidence": 0.9 if freshness_eval["freshness"] == "CURRENT" else 0.5,
        "summary": f"Freshness: {freshness_eval['freshness']}. {freshness_eval['reason']}",
    })

    # TrustGatekeeper
    roles.append({
        "role": "TrustGatekeeper",
        "stance": "SUPPORT" if trust_eval["passed"] else "BLOCK",
        "confidence": min(trust_eval.get("mhr", 0.5) + 0.3, 1.0),
        "summary": trust_eval["reason"],
    })

    # RegimeAssessor
    roles.append({
        "role": "RegimeAssessor",
        "stance": "SUPPORT" if regime_eval["passed"] else "BLOCK",
        "confidence": regime_eval.get("size_mult", 0.5),
        "summary": regime_eval["reason"],
    })

    # PathAnalyst
    roles.append({
        "role": "PathAnalyst",
        "stance": "SUPPORT" if path_eval["quality_ok"] else "CONDITIONAL",
        "confidence": 0.8 if path_eval["quality_ok"] else 0.4,
        "summary": "Path quality acceptable" if path_eval["quality_ok"] else "; ".join(path_eval.get("flags", [])),
    })

    # ProtectionAdvisor
    trail_stance = "SUPPORT"
    trail_summary = f"Trail style: {trail_eval.get('intended_trail_style') or 'N/A'}"
    if trail_eval.get("note"):
        trail_stance = "CONDITIONAL"
        trail_summary += f". {trail_eval['note']}"
    roles.append({
        "role": "ProtectionAdvisor",
        "stance": trail_stance,
        "confidence": 0.8,
        "summary": trail_summary,
    })

    return roles
