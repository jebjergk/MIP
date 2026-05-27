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

Phase C — fields consumed beyond the original core (and how they affect output)
================================================================================
All of the following are read from the LIVE_ACTIONS / import payload dict.

Newly wired (this module) — identity / context
- MARKET_TYPE: informational reason code MARKET_TYPE_NOTE (CRYPTO/FUTURES); no hard block.
- SETUP_NARRATIVE: if missing/short and STRUCTURE_CONFIDENCE < 0.45, adds
  NARRATIVE_THIN_WITH_LOW_CONFIDENCE and applies size_mult 0.82 (deterministic).
- STRUCTURAL_STATE: values BROKEN / INVALID / INVALIDATED (case-insensitive) hard-block with
  STRUCTURAL_STATE_INVALID.
- REGIME_TAGS: JSON or list parsed; tag CRISIS_HARD applies size_mult 0.55 and
  REGIME_TAG_CRISIS_HARD; EXTREME_VOL applies size_mult 0.75 and reason tag.

Newly wired — level / path timing (layer on top of path_quality)
- LEVEL_SIGNIFICANCE: if present and < 0.35, applies size_mult 0.88 and
  LEVEL_SIGNIFICANCE_LOW (warning tier; contributes to PROCEED_REDUCED).
- AVG_BARS_TO_THRESHOLD: if > 80 and PATH_SURVIVAL_RATE < 0.35, adds
  SLOW_PATH_HIGH_THRESHOLD and size_mult 0.85.
- DOMINANT_FAILURE_MODE: if in {GAP, NEWS, LIQUIDITY} adds soft reason
  DOMINANT_FAILURE_{MODE} (size_mult 0.92).
- BEST_WINDOW: recorded in structural_context_eval for audit only (no block).

Newly wired — risk / hold
- RISK_CLASS: AGGRESSIVE with trust label not TRUSTED applies size_mult 0.85 and
  RISK_CLASS_AGGRESSIVE_SIZE_CLAMP; SPECULATIVE applies 0.9 and reason tag.
  GAP_AWARE applies size_mult 0.5 and RISK_CLASS_GAP_AWARE_SIZE_CLAMP
  (used by BREAKOUT_RETEST_LONG to enforce the C3 gap-risk-aware sizing intent
  documented in 520_sp_propose_structural_trades.sql; binds the previously
  informational sizing_multiplier=0.5 carried in COMMITTEE_PAYLOAD; token kept
  short to fit STRUCTURAL_TRADE_PROPOSALS.RISK_CLASS TEXT(10) constraint).
- EXIT_STYLE: recorded on joint_decision.exit_style for submit/diagnostics coherence.
- MAX_HOLD_BARS / EXPECTED_HOLD_CHARACTER: already on joint_decision; hold used in risk note.

Existing core (unchanged semantics, still consumed)
- Freshness: FRESHNESS_ASSESSMENT, SETUP_STILL_VALID, PRICE_MOVED_TOO_FAR,
  DISTANCE_TO_ENTRY_ZONE.
- Trust: TRUST_LABEL, MEANINGFUL_HIT_RATE.
- Regime: REGIME_COMPAT.
- Path: MFE_MAE_RATIO, PATH_SURVIVAL_RATE, STRUCTURE_CONFIDENCE.
- Trail: TRAIL_STYLE, TRAIL_ACTIVATION_TYPE, FRESHNESS_ASSESSMENT interplay.
- Bracket: ENTRY_ZONE_*, INVALIDATION_*, DIRECTION, CURRENT_PRICE / bars, TARGET_EXPECTATION_SNAPSHOT.

Structural EXIT uses ``build_structural_exit_execution_only_verdict`` (broker position qty only) — no
freshness/trust/regime committee layer; see live router.
"""

from __future__ import annotations

import json
import math
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

# Must match execute_live_action / LIVE_MIN_R_MULTIPLE default (IB bracket gates).
_MIN_RR_FOR_LIVE_ENTRY = 1.10
_DEFAULT_STRUCTURAL_SL_PCT = 0.05
_MAX_STRUCTURAL_TP_PCT = 0.50


def _safe_float(v, default=None) -> float | None:
    if v is None:
        return default
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _structural_reference_price(action: dict) -> float | None:
    """Best-effort price for invalidation-distance and TP/SL % (materialize/execute anchor)."""
    for key in ("REVALIDATION_PRICE", "PROPOSED_PRICE", "CURRENT_PRICE", "ONE_MIN_BAR_CLOSE"):
        px = _safe_float(action.get(key))
        if px is not None and px > 0:
            return px
    lo = _safe_float(action.get("ENTRY_ZONE_LOW"))
    hi = _safe_float(action.get("ENTRY_ZONE_HIGH"))
    if lo is not None and hi is not None and lo > 0 and hi > 0:
        return (lo + hi) / 2.0
    return None


def _target_return_meeting_min_rr(
    sl_pct: float,
    candidate: float,
    min_rr: float = _MIN_RR_FOR_LIVE_ENTRY,
) -> float:
    """Return target_return (6dp) with target_return / stop_loss_pct >= min_rr after rounding."""
    sl = float(sl_pct)
    if sl <= 0:
        return round(min(max(float(candidate), 0.0), _MAX_STRUCTURAL_TP_PCT), 6)
    needed = sl * float(min_rr)
    tr = round(min(max(float(candidate), needed), _MAX_STRUCTURAL_TP_PCT), 6)
    if tr / sl < float(min_rr) - 1e-12:
        tr = min(math.ceil(needed * 1_000_000) / 1_000_000, _MAX_STRUCTURAL_TP_PCT)
    return tr


def _target_return_from_expectation_snapshot(action: dict) -> float | None:
    raw = action.get("TARGET_EXPECTATION_SNAPSHOT")
    if raw is None:
        return None
    if isinstance(raw, str):
        try:
            te = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return None
    elif isinstance(raw, dict):
        te = raw
    else:
        return None
    if not isinstance(te, dict):
        return None
    bands = te.get("bands")
    if isinstance(bands, dict):
        for k in ("base", "mid", "strong", "median"):
            v = _safe_float(bands.get(k))
            if v is not None and v > 0:
                return float(v)
    for k in ("base_return", "expected_return", "realistic_target_return", "target_return"):
        v = _safe_float(te.get(k))
        if v is not None and v > 0:
            return float(v)
    return None


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


def _parse_regime_tags(action: dict) -> list[str]:
    raw = action.get("REGIME_TAGS")
    if raw is None:
        return []
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return []
    if isinstance(raw, list):
        return [str(x).upper() for x in raw if x is not None]
    if isinstance(raw, dict):
        return [str(k).upper() for k, v in raw.items() if v]
    return []


def _evaluate_structural_extended_context(action: dict) -> dict:
    """
    Deterministic structural thesis / risk context beyond core gates.
    Returns size_mult in (0, 1], reason_fragments, optional hard block.
    """
    mult = 1.0
    reasons: list[str] = []
    tags_add: list[str] = []
    hard_block = False
    block_msg = ""

    mt = str(action.get("MARKET_TYPE") or "").upper()
    if mt in ("CRYPTO", "FUTURE", "FUTURES"):
        tags_add.append("MARKET_TYPE_NOTE")

    state = str(action.get("STRUCTURAL_STATE") or "").upper()
    if state in ("BROKEN", "INVALID", "INVALIDATED", "VOID"):
        hard_block = True
        block_msg = f"Structural state {state} blocks execution"
        tags_add.append("STRUCTURAL_STATE_INVALID")

    sig = _safe_float(action.get("LEVEL_SIGNIFICANCE"))
    if sig is not None and sig < 0.35:
        mult *= 0.88
        tags_add.append("LEVEL_SIGNIFICANCE_LOW")

    avg_bt = _safe_float(action.get("AVG_BARS_TO_THRESHOLD"))
    surv = _safe_float(action.get("PATH_SURVIVAL_RATE"))
    if avg_bt is not None and avg_bt > 80 and surv is not None and surv < 0.35:
        mult *= 0.85
        tags_add.append("SLOW_PATH_HIGH_THRESHOLD")

    dfm = str(action.get("DOMINANT_FAILURE_MODE") or "").upper()
    if dfm in ("GAP", "NEWS", "LIQUIDITY"):
        mult *= 0.92
        tags_add.append(f"DOMINANT_FAILURE_{dfm}")

    risk_c = str(action.get("RISK_CLASS") or "").upper()
    trust = str(action.get("TRUST_LABEL") or "").upper()
    if risk_c == "AGGRESSIVE" and trust != "TRUSTED":
        mult *= 0.85
        tags_add.append("RISK_CLASS_AGGRESSIVE_SIZE_CLAMP")
    elif risk_c == "SPECULATIVE":
        mult *= 0.90
        tags_add.append("RISK_CLASS_SPECULATIVE_NOTE")
    elif risk_c == "GAP_AWARE":
        mult *= 0.50
        tags_add.append("RISK_CLASS_GAP_AWARE_SIZE_CLAMP")

    narrative = str(action.get("SETUP_NARRATIVE") or "").strip()
    conf = _safe_float(action.get("STRUCTURE_CONFIDENCE"), 1.0)
    if (not narrative or len(narrative) < 12) and conf is not None and conf < 0.45:
        mult *= 0.82
        tags_add.append("NARRATIVE_THIN_WITH_LOW_CONFIDENCE")

    for rt in _parse_regime_tags(action):
        if rt in ("CRISIS_HARD", "CRISIS", "BLACK_SWAN"):
            mult *= 0.55
            tags_add.append("REGIME_TAG_CRISIS_HARD")
        elif rt in ("EXTREME_VOL", "EXTREME_VOLATILITY"):
            mult *= 0.75
            tags_add.append("REGIME_TAG_EXTREME_VOL")

    mult = max(0.15, min(1.0, float(mult)))
    return {
        "size_mult": mult,
        "reason_tags": tags_add,
        "notes": reasons,
        "hard_block": hard_block,
        "hard_block_message": block_msg,
        "market_type": mt or None,
        "best_window": action.get("BEST_WINDOW"),
        "latest_bar_date": action.get("LATEST_BAR_DATE"),
    }


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


def build_structural_exit_execution_only_verdict(
    action: dict, *, exit_position_qty: float | None
) -> dict:
    """
    Deterministic structural EXIT verdict for LIVE materialization only.
    Blocks solely on broker-truth position quantity (flat symbol); no entry-style structural gates.
    """
    try:
        qty = float(exit_position_qty) if exit_position_qty is not None else 0.0
    except (TypeError, ValueError):
        qty = 0.0
    blocked = abs(qty) <= 0
    recommendation = "BLOCK" if blocked else "PROCEED"
    flat_msg = "No broker position quantity for this symbol — exit not executable."
    proceed_msg = "Structural exit — execution-only path (broker position gate)."
    risk_note = flat_msg if blocked else ""
    jd: dict[str, Any] = {
        "should_enter": False,
        "should_execute_exit": not blocked,
        "recommendation": recommendation,
        "size_factor": 0.0 if blocked else 1.0,
        "position_size_factor": 0.0 if blocked else 1.0,
        "risk_note": risk_note,
        "hold_bars": action.get("MAX_HOLD_BARS"),
        "hold_character": action.get("EXPECTED_HOLD_CHARACTER") or "MEDIUM_SWING",
        "exit_style": action.get("EXIT_STYLE"),
        "realistic_target_return": None,
        "stop_loss_pct": None,
        "acceptable_early_exit_target_return": None,
    }
    reason_codes: list[str] = ["STRUCTURAL_EXIT_EXECUTION_ONLY"]
    if blocked:
        reason_codes.append("EXIT_POSITION_MISSING")
    summary = flat_msg if blocked else proceed_msg
    role_outputs = [
        {
            "role": "Execution",
            "stance": "BLOCK" if blocked else "PROCEED",
            "confidence": 1.0,
            "summary": summary[:500],
        }
    ]
    return {
        "recommendation": recommendation,
        "size_factor": 1.0 if not blocked else 0.0,
        "confidence": 1.0 if not blocked else 0.0,
        "blocked": blocked,
        "block_reasons": [] if not blocked else [flat_msg],
        "reason_codes": reason_codes,
        "risk_note": risk_note,
        "joint_decision": jd,
        "role_outputs": role_outputs,
        "structural_source": True,
        "committee_model": "STRUCTURAL_EXIT_EXECUTION_ONLY",
    }


def build_structural_entry_joint_decision(action: dict) -> dict:
    """
    Build joint_decision for structural ENTRY from LIVE_ACTIONS only (TP/SL, zones, trail shell).
    Used by Committee 2.0 bridge and execute-time bracket self-heal — not a second opinion committee.
    """
    trail_eval = _validate_trail_style(action)
    max_hold = action.get("MAX_HOLD_BARS")
    hold_character = action.get("EXPECTED_HOLD_CHARACTER") or "MEDIUM_SWING"
    entry_zone_low = _safe_float(action.get("ENTRY_ZONE_LOW"))
    entry_zone_high = _safe_float(action.get("ENTRY_ZONE_HIGH"))
    invalidation = _safe_float(action.get("INVALIDATION_LEVEL"))
    direction = (action.get("DIRECTION") or "LONG").upper()
    committee_entry_zone_low = entry_zone_low
    committee_entry_zone_high = entry_zone_high
    committee_invalidation = invalidation
    jd: dict[str, Any] = {
        "should_enter": True,
        "recommendation": "PROCEED",
        "size_factor": 1.0,
        "position_size_factor": 1.0,
        "risk_note": "",
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
            "style": trail_eval.get("committee_trail_style") or action.get("TRAIL_STYLE"),
            "activation_type": action.get("TRAIL_ACTIVATION_TYPE"),
            "activation_param": _safe_float(action.get("TRAIL_ACTIVATION_PARAM")),
            "adjusted": trail_eval.get("adjusted", False),
            "note": trail_eval.get("note"),
        },
    }
    ref_px = _structural_reference_price(action)
    if ref_px and invalidation and ref_px > 0 and float(invalidation) > 0:
        if direction == "LONG":
            jd["stop_loss_pct"] = round(abs(ref_px - float(invalidation)) / ref_px, 6)
        else:
            jd["stop_loss_pct"] = round(abs(float(invalidation) - ref_px) / ref_px, 6)
    sl_pct = _safe_float(jd.get("stop_loss_pct"))
    if sl_pct is None or sl_pct <= 0:
        jd["stop_loss_pct"] = float(_DEFAULT_STRUCTURAL_SL_PCT)
        sl_pct = float(_DEFAULT_STRUCTURAL_SL_PCT)
    tp_floor = float(sl_pct) * _MIN_RR_FOR_LIVE_ENTRY
    te_tp = _target_return_from_expectation_snapshot(action)
    candidate = max(tp_floor, float(te_tp) if te_tp is not None else 0.0, 0.02)
    jd["realistic_target_return"] = _target_return_meeting_min_rr(sl_pct, candidate)
    if te_tp is not None and float(te_tp) > 0:
        jd["acceptable_early_exit_target_return"] = round(
            min(float(te_tp) * 0.85, jd["realistic_target_return"]), 6
        )
    return jd


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
    ctx_eval: dict | None = None,
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

    if isinstance(ctx_eval, dict) and ctx_eval.get("reason_tags"):
        roles.append({
            "role": "ThesisContext",
            "stance": "CONDITIONAL" if float(ctx_eval.get("size_mult") or 1.0) < 0.95 else "SUPPORT",
            "confidence": round(float(ctx_eval.get("size_mult") or 1.0), 2),
            "summary": "Context tags: " + ", ".join(ctx_eval.get("reason_tags") or [])[:400],
        })

    return roles
