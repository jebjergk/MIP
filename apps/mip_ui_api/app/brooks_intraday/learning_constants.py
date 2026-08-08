"""Official attempt chain for the pilot historical week (Phase 8 provenance)."""

from __future__ import annotations

PILOT_RUN_ID = "4eababc8-88fe-4ebc-b47c-b65f6ec10c74"

# Phase E1 validation week — review-only PM certification (does not change run pins).
PHASE_E1_VALIDATION_RUN_ID = "6d5e883b-eacf-4730-aed7-f9dd3bb1eb0e"
PHASE_E1_CONTEXT_ATTEMPT_ID = "3defa3de-d699-424a-8ceb-78020b453284"
PHASE_E1_SIMULATION_ATTEMPT_ID = "8de2e63f-99f8-46ed-a2ec-f5b8c321c651"
PM_V01_CERT_SIMULATION_ATTEMPT_ID = "125eb282-3dc7-41a7-8fbf-602f75ad6b51"
PM_V01_OBSOLETE_CERT_SIMULATION_ATTEMPT_ID = "5aa3cd99-b11d-4ba6-b061-4a4f098d4140"

REVIEW_CHAIN_SUPPLEMENTS: dict[str, list[dict[str, str | bool | float | int | None]]] = {
    PHASE_E1_VALIDATION_RUN_ID: [
        {
            "label": (
                "BROOKS_POSITION_MANAGEMENT_RULESET_V0_1 · canonical PM certification · "
                "sim 125eb282… · P/L +$6.88"
            ),
            "context_attempt_id": PHASE_E1_CONTEXT_ATTEMPT_ID,
            "simulation_attempt_id": PM_V01_CERT_SIMULATION_ATTEMPT_ID,
            "context_ruleset": "BROOKS_CONTEXT_RULESET_V0_3",
            "simulation_ruleset": "BROOKS_POSITION_MANAGEMENT_RULESET_V0_1",
            "trade_count": 1,
            "realized_pnl": 6.88,
            "canonical_pm_certification": True,
            "notes": "Inactive PM ruleset; V0_1 entries only.",
        },
    ],
}

OFFICIAL_ATTEMPT_CHAIN: dict[str, dict[str, str]] = {
    PILOT_RUN_ID: {
        "objective_attempt_id": "53a502f5-dec4-4bff-8106-f6637574163e",
        "objective_ruleset": "BROOKS_OBJECTIVE_RULESET_V0_1",
        "pattern_attempt_id": "d83d5bab-4e02-4aec-9bd7-0da54b1395d3",
        "pattern_ruleset": "BROOKS_PATTERN_RULESET_V0_3",
        "context_attempt_id": "63ecc779-3dbb-4468-8bc1-a8bbd0f17342",
        "context_ruleset": "BROOKS_CONTEXT_RULESET_V0_2",
        "simulation_attempt_id": "4bd264d3-c061-4bee-8534-c4eb99532438",
        "simulation_ruleset": "BROOKS_SIMULATION_RULESET_V0_1",
    },
}

SIMULATION_ONLY_BANNER = "SIMULATION ONLY — NO REAL ORDERS OR PORTFOLIO CONNECTION"
RECONSTRUCTED_DOSSIER_BADGE = "HISTORICAL PAA RECONSTRUCTION — POINT-IN-TIME DATA ONLY"

ZERO_TRADE_EXPLANATION = (
    "The approved context attempt produced no CONSIDER_ENTRY events. "
    "The simulation therefore made no trades and preserved the full $1,000 balance."
)

PILOT_ZERO_TRADE_DETAIL = (
    "No CONSIDER_ENTRY advisory events occurred in the approved context attempt, "
    "so the simulation correctly remained in cash."
)

BARS_PER_SYMBOL_WEEK = 390
