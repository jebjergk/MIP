"""
Training Status v1: deterministic scoring and reasons.

Uses only RECOMMENDATION_LOG, RECOMMENDATION_OUTCOMES, optional PATTERN_DEFINITION,
optional TRAINING_GATE_PARAMS. Scoring: sample (0–30), coverage (0–40), horizons (0–30).
Stage: INSUFFICIENT / WARMING_UP / LEARNING / CONFIDENT. reasons[] in plain language.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

# Default thresholds when TRAINING_GATE_PARAMS is not used
DEFAULT_MIN_SIGNALS = 40
MAX_HORIZONS = 5  # 1, 3, 5, 10, 20
POINTS_SAMPLE = 30
POINTS_COVERAGE = 40
POINTS_HORIZONS = 30


@dataclass
class TrainingStatusScore:
    """Result of deterministic scoring for one (market_type, symbol, pattern_id, interval_minutes) row."""
    maturity_score: float  # 0–100
    maturity_stage: str   # INSUFFICIENT | WARMING_UP | LEARNING | CONFIDENT
    reasons: list[str]


def compute_maturity_score(
    recs_total: int,
    outcomes_total: int,
    horizons_covered: int,
    min_signals: int = DEFAULT_MIN_SIGNALS,
) -> tuple[float, float, float]:
    """
    Compute the three component scores (sample, coverage, horizons).
    Returns (score_sample, score_coverage, score_horizons) each in [0, max].
    """
    # Sample: 0→30, cap at min_signals
    ratio_sample = min(1.0, recs_total / min_signals) if min_signals else 1.0
    score_sample = POINTS_SAMPLE * ratio_sample

    # Coverage: outcomes / (recs * 5) capped at 1
    possible_outcomes = recs_total * MAX_HORIZONS if recs_total else 0
    coverage_ratio = (outcomes_total / possible_outcomes) if possible_outcomes else 0.0
    coverage_ratio = min(1.0, coverage_ratio)
    score_coverage = POINTS_COVERAGE * coverage_ratio

    # Horizons: 0..5 → 0..30
    score_horizons = POINTS_HORIZONS * (horizons_covered / MAX_HORIZONS) if MAX_HORIZONS else 0.0

    return (score_sample, score_coverage, score_horizons)


def get_maturity_stage(score: float) -> str:
    """Map 0–100 score to stage."""
    if score < 25:
        return "INSUFFICIENT"
    if score < 50:
        return "WARMING_UP"
    if score < 75:
        return "LEARNING"
    return "CONFIDENT"


def build_reasons(
    recs_total: int,
    outcomes_total: int,
    horizons_covered: int,
    coverage_ratio: float,
    score_sample: float,
    score_coverage: float,
    score_horizons: float,
    total_score: float,
    min_signals: int = DEFAULT_MIN_SIGNALS,
) -> list[str]:
    """Plain-language reasons for the score (non-trader language)."""
    reasons: list[str] = []

    if recs_total < min_signals:
        reasons.append("Not enough recommendations yet; more data will improve the score.")
    else:
        reasons.append("Enough recommendations to start judging quality.")

    if coverage_ratio < 0.5:
        reasons.append("Many recommendations are still waiting for outcome data.")
    elif coverage_ratio < 1.0:
        reasons.append("Most recommendations have been evaluated; some are still pending.")
    else:
        reasons.append("All recommendations have outcome data for the horizons evaluated.")

    if horizons_covered < MAX_HORIZONS:
        reasons.append(
            f"Outcome data is available for {horizons_covered} of {MAX_HORIZONS} time windows; "
            "more windows will strengthen the score."
        )
    else:
        reasons.append("All time windows have outcome data.")

    stage = get_maturity_stage(total_score)
    if stage == "INSUFFICIENT":
        reasons.append("Overall: not enough data yet to be confident.")
    elif stage == "WARMING_UP":
        reasons.append("Overall: data is building; confidence is growing.")
    elif stage == "LEARNING":
        reasons.append("Overall: enough data to learn from; score is meaningful.")
    else:
        reasons.append("Overall: strong data coverage and outcome completeness.")

    return reasons


def score_training_status_row(
    recs_total: int,
    outcomes_total: int,
    horizons_covered: int,
    min_signals: int = DEFAULT_MIN_SIGNALS,
) -> TrainingStatusScore:
    """
    Deterministic scoring for one row. coverage_ratio derived from outcomes_total and recs_total.
    """
    possible_outcomes = recs_total * MAX_HORIZONS if recs_total else 0
    coverage_ratio = (outcomes_total / possible_outcomes) if possible_outcomes else 0.0
    coverage_ratio = min(1.0, coverage_ratio)

    score_sample, score_coverage, score_horizons = compute_maturity_score(
        recs_total, outcomes_total, horizons_covered, min_signals
    )
    total_score = score_sample + score_coverage + score_horizons
    total_score = min(100.0, max(0.0, total_score))
    stage = get_maturity_stage(total_score)
    reasons = build_reasons(
        recs_total,
        outcomes_total,
        horizons_covered,
        coverage_ratio,
        score_sample,
        score_coverage,
        score_horizons,
        total_score,
        min_signals,
    )
    return TrainingStatusScore(maturity_score=round(total_score, 1), maturity_stage=stage, reasons=reasons)


def _get_int(row: dict[str, Any], *keys: str) -> int:
    """Get first present key (case-insensitive); return 0 if missing."""
    for k in keys:
        v = row.get(k)
        if v is None and k.islower():
            v = row.get(k.upper())
        if v is not None:
            try:
                return int(v)
            except (TypeError, ValueError):
                pass
    return 0


def apply_scoring_to_rows(
    rows: list[dict[str, Any]],
    min_signals: int = DEFAULT_MIN_SIGNALS,
) -> list[dict[str, Any]]:
    """
    For each row from the training-status SQL, add maturity_score, maturity_stage, reasons.
    Expects keys: recs_total, outcomes_total, horizons_covered (and any others passed through).
    """
    result = []
    for r in rows:
        recs = _get_int(r, "recs_total")
        outcomes = _get_int(r, "outcomes_total")
        horizons = _get_int(r, "horizons_covered")
        score_result = score_training_status_row(recs, outcomes, horizons, min_signals)
        out = {**r, "maturity_score": score_result.maturity_score, "maturity_stage": score_result.maturity_stage, "reasons": score_result.reasons}
        result.append(out)
    return result
