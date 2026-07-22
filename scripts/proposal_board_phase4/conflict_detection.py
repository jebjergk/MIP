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


def detect_conflicts(
    positions: Dict[str, Dict[str, Any]],
    evidence_json: Optional[Dict[str, Any]] = None,
) -> List[ConflictEntry]:
    """
    Inspect the 5 specialist positions for ONE dossier and return a list
    of structured conflicts. Empty list = no conflicts found.

    `positions` keys are role names: MARKET_STRUCTURA, LEVEL_PRICE_ACTION,
    THESIS, HISTORICAL_EVIDENCE, RISK_EXECUTION.

    `evidence_json` is the dossier payload (optional). When provided,
    additional evidence-level conflicts are detected — currently the
    OPPOSING_SETUP_UNRESOLVED rule, which fires when THESIS points one
    way but the dossier carries an eligible opposite-direction setup with
    meaningful structure confidence.
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

    # 4) THESIS direction vs eligible opposite-direction setup in dossier
    if thesis_v in _THESIS_DIRECTIONAL and isinstance(evidence_json, dict):
        thesis_dir = "LONG" if thesis_v in _THESIS_LONG else "SHORT"
        opposing = _find_eligible_opposing_setup(evidence_json, thesis_dir)
        if opposing:
            out.append(ConflictEntry(
                source_role="EVIDENCE",
                target_role="THESIS",
                topic="OPPOSING_SETUP_UNRESOLVED",
                disagreement_type="EVIDENCE_CONFLICT",
                challenge_text=(
                    f"THESIS argues {thesis_dir} but the dossier carries an eligible "
                    f"opposite-direction setup: setup_event_id="
                    f"{opposing.get('setup_event_id')} "
                    f"{opposing.get('setup_family')} "
                    f"({opposing.get('setup_status')}) with structure_confidence="
                    f"{opposing.get('structure_confidence')}. Defend why the "
                    f"{thesis_dir} thesis outweighs this opposing evidence, or "
                    "revise to WATCH."
                ),
            ))

    return out


# Structure_confidence and bar-window thresholds for the OPPOSING_SETUP gate.
# Kept in sync with the deterministic UNRESOLVED_OPPOSING_SETUP publish gate in
# orchestrator._publish_to_structural / 566 SQL SP.
_OPPOSING_CONFIDENCE_THRESHOLD = 0.65
_OPPOSING_MAX_AGE_BARS = 3
_ELIGIBLE_STATUSES = {"DETECTED", "ELIGIBLE", "WAITING"}


def _find_eligible_opposing_setup(
    evidence_json: Dict[str, Any],
    thesis_direction: str,
) -> Optional[Dict[str, Any]]:
    """Return the freshest eligible opposite-direction setup, or None.

    Reads EVIDENCE_JSON.setup_events_evidence_only (list) and picks the row
    with DIRECTION != thesis_direction, an active-eligible status, structure
    confidence >= threshold, within the recent-bars window.
    """
    setup_events = evidence_json.get("setup_events_evidence_only") or []
    if not isinstance(setup_events, list):
        return None

    opposite = "SHORT" if thesis_direction == "LONG" else "LONG"
    best: Optional[Dict[str, Any]] = None
    best_setup_id: int = -1

    for se in setup_events:
        if not isinstance(se, dict):
            continue
        if str(se.get("event_direction") or se.get("direction") or "").upper() != opposite:
            continue
        status = str(se.get("setup_status") or "").upper()
        if status not in _ELIGIBLE_STATUSES:
            continue
        conf = se.get("structure_confidence")
        try:
            if conf is None or float(conf) < _OPPOSING_CONFIDENCE_THRESHOLD:
                continue
        except (TypeError, ValueError):
            continue
        setup_id_raw = se.get("setup_event_id")
        try:
            setup_id = int(setup_id_raw) if setup_id_raw is not None else -1
        except (TypeError, ValueError):
            setup_id = -1
        if setup_id > best_setup_id:
            best = se
            best_setup_id = setup_id

    return best


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
