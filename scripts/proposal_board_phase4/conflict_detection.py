"""
Phase 4 Cortex Agentic Proposal Board — conflict detection.

Inputs: dict[role_name -> parsed specialist position] for ONE dossier.
Outputs: list of structured ConflictEntry objects describing pairwise
disagreements between specialists.

Detection rules (deterministic, auditable, written for INTERACTION_V2):

  1. THESIS direction vs LEVEL_PRICE_ACTION location
     - THESIS LONG_THESIS  + LEVEL SHORT_LOCATION  -> LONG_VS_SHORT  (DIRECTION_DISAGREEMENT)
     - THESIS LONG_THESIS  + LEVEL NO_EDGE         -> LONG_VS_NO_TRADE (LEVEL_PROXIMITY_DISPUTE)
     - THESIS SHORT_THESIS + LEVEL LONG_LOCATION   -> LONG_VS_SHORT  (DIRECTION_DISAGREEMENT)
     - THESIS SHORT_THESIS + LEVEL NO_EDGE         -> SHORT_VS_NO_TRADE (LEVEL_PROXIMITY_DISPUTE)

  2. THESIS direction vs RISK_EXECUTION feasibility
     - THESIS directional + RISK HARD_BLOCK   -> EVIDENCE_CONFLICT (RISK_FEASIBILITY_DISPUTE)
     - THESIS directional + RISK NO_TRADE     -> EVIDENCE_CONFLICT (RISK_FEASIBILITY_DISPUTE)

  3. THESIS direction vs HISTORICAL_EVIDENCE
     - THESIS directional + HISTORY MIXED_DIRECTIONAL -> EVIDENCE_CONFLICT (HISTORY_INTERPRETATION)
     - THESIS directional + HISTORY WEAK_BOTH_SIDES   -> EVIDENCE_CONFLICT (HISTORY_INTERPRETATION)

  4. STANCE_DRIFT (post-revision):
     - Detected separately by orchestrator when revision changes
       a verdict and creates a NEW pairwise conflict that did not
       exist before.

The orchestrator persists each conflict pair as a separate
INTERACTION_V2 row (one challenge row, then one revision row).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# Verdict / stance vocabulary (kept in sync with agent system prompts)
# ---------------------------------------------------------------------------

_THESIS_DIRECTIONAL = {"LONG_THESIS", "SHORT_THESIS"}
_THESIS_LONG = {"LONG_THESIS"}
_THESIS_SHORT = {"SHORT_THESIS"}

_LEVEL_LONG = {"LONG_LOCATION"}
_LEVEL_SHORT = {"SHORT_LOCATION"}
_LEVEL_NO_EDGE = {"NO_EDGE"}

_RISK_HARD_BLOCK = {"HARD_BLOCK"}
_RISK_NO_TRADE = {"NO_TRADE"}

_HISTORY_MIXED = {"MIXED_DIRECTIONAL", "WEAK_BOTH_SIDES"}


@dataclass(frozen=True)
class ConflictEntry:
    source_role: str            # role raising the challenge (challenger)
    target_role: str            # role being challenged
    topic: str                  # one of TOPIC enum values
    disagreement_type: str      # one of DISAGREEMENT_TYPE enum values
    challenge_text: str         # human-readable explanation of the conflict


def _verdict(positions: Dict[str, Dict[str, Any]], role: str) -> Optional[str]:
    pos = positions.get(role) or {}
    v = pos.get("verdict")
    if isinstance(v, str):
        return v.strip().upper()
    return None


def _short(s: Optional[str], n: int = 200) -> str:
    if not s:
        return ""
    s = str(s).strip()
    return s if len(s) <= n else s[:n].rstrip() + "..."


def detect_conflicts(positions: Dict[str, Dict[str, Any]]) -> List[ConflictEntry]:
    """
    Inspect the 5 specialist positions for ONE dossier and return a list
    of structured conflicts. Empty list = no conflicts found.

    `positions` keys are role names: MARKET_STRUCTURE, LEVEL_PRICE_ACTION,
    THESIS, HISTORICAL_EVIDENCE, RISK_EXECUTION.
    """
    out: List[ConflictEntry] = []

    thesis_v = _verdict(positions, "THESIS")
    level_v = _verdict(positions, "LEVEL_PRICE_ACTION")
    risk_v = _verdict(positions, "RISK_EXECUTION")
    history_v = _verdict(positions, "HISTORICAL_EVIDENCE")

    # 1) THESIS direction vs LEVEL_PRICE_ACTION location
    if thesis_v in _THESIS_LONG and level_v in _LEVEL_SHORT:
        out.append(ConflictEntry(
            source_role="LEVEL_PRICE_ACTION",
            target_role="THESIS",
            topic="DIRECTION_DISAGREEMENT",
            disagreement_type="LONG_VS_SHORT",
            challenge_text=(
                "THESIS argues LONG but LEVEL_PRICE_ACTION reads price location as "
                "SHORT_LOCATION. Reconcile: defend why a long thesis is justified "
                "from current price relative to the dossier's nearest support and "
                "resistance, or revise."
            ),
        ))
    elif thesis_v in _THESIS_LONG and level_v in _LEVEL_NO_EDGE:
        out.append(ConflictEntry(
            source_role="LEVEL_PRICE_ACTION",
            target_role="THESIS",
            topic="LEVEL_PROXIMITY_DISPUTE",
            disagreement_type="LONG_VS_NO_TRADE",
            challenge_text=(
                "THESIS argues LONG but LEVEL_PRICE_ACTION reads NO_EDGE. "
                "Reconcile: defend why a long thesis is justified despite no clean "
                "level edge, or revise to WATCH_LONG / NO_TRADE."
            ),
        ))
    elif thesis_v in _THESIS_SHORT and level_v in _LEVEL_LONG:
        out.append(ConflictEntry(
            source_role="LEVEL_PRICE_ACTION",
            target_role="THESIS",
            topic="DIRECTION_DISAGREEMENT",
            disagreement_type="LONG_VS_SHORT",
            challenge_text=(
                "THESIS argues SHORT but LEVEL_PRICE_ACTION reads price location as "
                "LONG_LOCATION. Reconcile: defend why a short thesis is justified, "
                "or revise."
            ),
        ))
    elif thesis_v in _THESIS_SHORT and level_v in _LEVEL_NO_EDGE:
        out.append(ConflictEntry(
            source_role="LEVEL_PRICE_ACTION",
            target_role="THESIS",
            topic="LEVEL_PROXIMITY_DISPUTE",
            disagreement_type="SHORT_VS_NO_TRADE",
            challenge_text=(
                "THESIS argues SHORT but LEVEL_PRICE_ACTION reads NO_EDGE. "
                "Reconcile: defend why a short thesis is justified despite no clean "
                "level edge, or revise to WATCH_SHORT / NO_TRADE."
            ),
        ))

    # 2) THESIS direction vs RISK_EXECUTION feasibility
    if thesis_v in _THESIS_DIRECTIONAL and risk_v in (_RISK_HARD_BLOCK | _RISK_NO_TRADE):
        out.append(ConflictEntry(
            source_role="RISK_EXECUTION",
            target_role="THESIS",
            topic="RISK_FEASIBILITY_DISPUTE",
            disagreement_type="EVIDENCE_CONFLICT",
            challenge_text=(
                f"THESIS verdict={thesis_v} is directional but RISK_EXECUTION "
                f"verdict={risk_v} blocks actionability. Defend the directional "
                "thesis given operational risk constraints, or revise."
            ),
        ))

    # 3) THESIS direction vs HISTORICAL_EVIDENCE
    if thesis_v in _THESIS_DIRECTIONAL and history_v in _HISTORY_MIXED:
        out.append(ConflictEntry(
            source_role="HISTORICAL_EVIDENCE",
            target_role="THESIS",
            topic="HISTORY_INTERPRETATION",
            disagreement_type="EVIDENCE_CONFLICT",
            challenge_text=(
                f"THESIS verdict={thesis_v} is directional but HISTORICAL_EVIDENCE "
                f"verdict={history_v}. Defend the directional thesis given the "
                "mixed/weak history, or revise."
            ),
        ))

    return out


def detect_stance_drift(
    before: Dict[str, Dict[str, Any]],
    after: Dict[str, Dict[str, Any]],
) -> List[str]:
    """
    Return list of role names whose verdict changed between rounds.
    Used by the orchestrator to decide whether to re-run conflict
    detection after a revision round.
    """
    drifted: List[str] = []
    for role in after.keys():
        b = _verdict(before, role)
        a = _verdict(after, role)
        if b != a and b is not None and a is not None:
            drifted.append(role)
    return drifted
