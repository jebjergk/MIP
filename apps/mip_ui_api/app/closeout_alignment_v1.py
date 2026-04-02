"""Deterministic closeout alignment ALIGN_RULE_V1 — pure functions (no Snowflake I/O)."""

from __future__ import annotations

# Returns are fractional (0.0005 = 0.05% = 5 bps noise band for FLAT).
EPSILON_FLAT_PCT = 0.0005
COMPARISON_RULE_VERSION = "ALIGN_RULE_V1"


def recommended_action_from_alpha_spec(alpha: dict | None) -> str | None:
    """Mirrors live._effective_alpha_baseline_action for closeout without importing live."""
    if not isinstance(alpha, dict) or not alpha:
        return None
    if alpha.get("stub") is True and alpha.get("phase") == 1:
        return None
    if not alpha.get("alpha_schema_version") and not alpha.get("recommended_action"):
        return None
    act = str(alpha.get("recommended_action") or "").upper()
    if act in ("ENTER", "REDUCE", "SKIP"):
        return act
    return None


def compute_position_return_pct(
    entry_avg: float | None,
    exit_avg: float | None,
    entry_side: str | None,
) -> float | None:
    """
    Signed economic return on the position (positive = favorable).
    Long (BUY entry): (exit - entry) / entry.
    Short (SELL entry): (entry - exit) / entry.
    """
    if entry_avg is None or exit_avg is None or entry_avg == 0:
        return None
    es = (entry_side or "").upper()
    if es == "SELL":
        return float((entry_avg - exit_avg) / entry_avg)
    return float((exit_avg - entry_avg) / entry_avg)


def classify_realized_outcome_class(
    position_return_pct: float | None,
) -> tuple[str | None, list[str]]:
    if position_return_pct is None:
        return None, ["MISSING_REALIZED_RETURN"]
    pr = float(position_return_pct)
    if abs(pr) <= EPSILON_FLAT_PCT:
        return "FLAT", ["REALIZED_FLAT"]
    if pr > EPSILON_FLAT_PCT:
        return "FAVORABLE", ["REALIZED_FAVORABLE"]
    return "UNFAVORABLE", ["REALIZED_UNFAVORABLE"]


def exit_type_reason_code(exit_type: str) -> str:
    m = {
        "SL": "EXIT_STOP_LOSS",
        "TP": "EXIT_TAKE_PROFIT",
        "EARLY": "EXIT_EARLY",
        "MANUAL": "EXIT_MANUAL",
        "REVALIDATION": "EXIT_REVALIDATION",
        "OTHER": "EXIT_OTHER",
    }
    return m.get((exit_type or "OTHER").upper(), "EXIT_OTHER")


def _dedupe_preserve(seq: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for x in seq:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def compute_alignment_rule_v1(
    *,
    recommended_action: str | None,
    alpha_override_class: str | None,
    realized_outcome_class: str | None,
    exit_type: str,
    missing_entry_fill: bool,
    missing_exit_fill: bool,
) -> dict:
    """
    Returns ALIGNMENT_JSON-shaped dict (comparison_rule_version, alignment_class,
    realized_outcome_class, alignment_reason_codes, summary, epsilon_flat_pct).
    """
    reasons: list[str] = [COMPARISON_RULE_VERSION, exit_type_reason_code(exit_type)]

    if missing_entry_fill:
        reasons.append("MISSING_ENTRY_FILL")
    if missing_exit_fill:
        reasons.append("MISSING_EXIT_FILL")

    roc = realized_outcome_class
    if roc == "FAVORABLE":
        reasons.append("REALIZED_FAVORABLE")
    elif roc == "FLAT":
        reasons.append("REALIZED_FLAT")
    elif roc == "UNFAVORABLE":
        reasons.append("REALIZED_UNFAVORABLE")
    elif roc is None:
        reasons.append("MISSING_REALIZED_OUTCOME")

    aoc = (alpha_override_class or "").upper()
    ra = (recommended_action or "").upper() if recommended_action else None

    if aoc == "INCREASE_VS_ALPHA":
        if ra == "SKIP":
            reasons.append("ENTRY_ALPHA_SKIP_OVERRIDDEN")
        elif ra == "REDUCE":
            reasons.append("ENTRY_ALPHA_REDUCE_OVERRIDDEN")
    elif aoc == "REDUCE_VS_ALPHA":
        reasons.append("ENTRY_COMMITTEE_REDUCED_VS_ENTER")
    elif aoc == "BLOCK_DESPITE_ALPHA":
        reasons.append("ENTRY_ALPHA_BLOCK_DESPITE_ENTER")
    elif aoc == "ACCEPT_ALPHA":
        reasons.append("ENTRY_COMMITTEE_ACCEPT_ALPHA")
    elif aoc == "NO_ALPHA_BASELINE":
        reasons.append("NO_ALPHA_BASELINE")
    elif aoc == "UNKNOWN_OVERRIDE":
        reasons.append("UNKNOWN_OVERRIDE_CLASS")

    # alignment_class
    if missing_entry_fill or missing_exit_fill or roc is None:
        alignment = "NEUTRAL"
        reasons.append("ALIGN_INSUFFICIENT_BROKER_DATA")
    elif ra is None:
        alignment = "NEUTRAL"
        reasons.append("ALIGN_NO_ACTIONABLE_ALPHA")
    elif ra == "SKIP":
        reasons.append("SKIP_BASELINE_TRADE_CLOSED")
        if roc == "UNFAVORABLE":
            alignment = "ADVERSE"
        else:
            alignment = "NEUTRAL"
    elif ra in ("ENTER", "REDUCE"):
        if roc == "FAVORABLE":
            alignment = "ALIGNED"
        elif roc == "FLAT":
            alignment = "NEUTRAL"
        else:
            alignment = "ADVERSE"
    else:
        alignment = "NEUTRAL"
        reasons.append("ALIGN_UNKNOWN_BASELINE_ACTION")

    reasons = _dedupe_preserve(reasons)
    summary = (
        f"{COMPARISON_RULE_VERSION} baseline={ra or 'NONE'} override={aoc or 'NONE'} "
        f"outcome={roc or 'NONE'} alignment={alignment} exit={exit_type}"
    )
    if len(summary) > 512:
        summary = summary[:509] + "..."

    return {
        "alignment_class": alignment,
        "realized_outcome_class": roc,
        "comparison_rule_version": COMPARISON_RULE_VERSION,
        "alignment_reason_codes": reasons,
        "summary": summary,
        "epsilon_flat_pct": EPSILON_FLAT_PCT,
    }
