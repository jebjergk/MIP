"""
Map Committee 2.0 COMMITTEE_FINAL_DECISION rows into the legacy LIVE committee verdict shape
so execute / submit paths stay stable. ENTRY structural only; EXIT uses execution-only pass-through in live.py.
"""
from __future__ import annotations

import json
from typing import Any

from app.db import fetch_all
from app.services.live_intelligence.structural_committee import build_structural_entry_joint_decision


def _v(x: Any) -> Any:
    if x is None:
        return None
    if isinstance(x, (dict, list)):
        return x
    if isinstance(x, str):
        try:
            return json.loads(x)
        except json.JSONDecodeError:
            return x
    return x


def fetch_committee2_final_decision_for_action(cur, action_id: str) -> dict | None:
    cur.execute(
        """
        SELECT *
        FROM MIP.APP.COMMITTEE_FINAL_DECISION
        WHERE ACTION_ID = %s
        ORDER BY DECISION_TS DESC
        LIMIT 1
        """,
        (action_id,),
    )
    rows = fetch_all(cur)
    return rows[0] if rows else None


def _stance_to_blocked_and_rec(stance: str) -> tuple[bool, str]:
    s = (stance or "").upper()
    if s in ("DENY", "DEFER", "WAIT_RECLAIM"):
        return True, "BLOCK"
    if s == "APPROVE_REDUCED":
        return False, "PROCEED_REDUCED"
    if s == "APPROVE":
        return False, "PROCEED"
    return True, "BLOCK"


def _size_from_posture(posture: dict, stance: str, blocked: bool) -> float:
    if blocked:
        return 0.0
    sp = (posture or {}).get("size_posture")
    if sp == "REDUCED":
        return 0.5
    if sp == "FULL":
        return 1.0
    if sp == "ZERO":
        return 0.0
    st = (stance or "").upper()
    if st == "APPROVE_REDUCED":
        return 0.5
    if st == "APPROVE":
        return 1.0
    return 0.0


def structural_entry_verdict_from_committee2_final(fd: dict, action: dict) -> dict:
    """
    Build the same top-level shape as historical run_structural_committee(ENTRY) for live.py.
    """
    stance = str(fd.get("STANCE") or "").upper()
    blocked, recommendation = _stance_to_blocked_and_rec(stance)
    posture = _v(fd.get("POSTURE_JSON")) or {}
    chair = _v(fd.get("CHAIR_OUTPUT_JSON")) or {}
    size_factor = _size_from_posture(posture, stance, blocked)
    conf = fd.get("CONFIDENCE")
    try:
        confidence = float(conf) if conf is not None else 0.65
    except (TypeError, ValueError):
        confidence = 0.65

    jd = build_structural_entry_joint_decision(dict(action))
    jd["should_enter"] = not blocked and stance in ("APPROVE", "APPROVE_REDUCED")
    jd["recommendation"] = recommendation
    jd["size_factor"] = size_factor
    jd["position_size_factor"] = size_factor
    risk_parts = [
        str(chair.get("stance") or stance),
        "Committee 2.0 hearing commit",
    ]
    tensions = chair.get("top_tensions") or []
    if tensions:
        risk_parts.append("; ".join(str(t) for t in tensions[:3]))
    jd["risk_note"] = " | ".join(risk_parts)[:1500]
    trail = jd.get("trail") if isinstance(jd.get("trail"), dict) else {}
    tp = posture.get("trail_posture")
    if tp:
        note = str(trail.get("note") or "")
        trail["note"] = (f"C2 posture {tp}. " + note).strip()
        jd["trail"] = trail

    reason_codes: list[str] = ["COMMITTEE2_SYNCED", f"COMMITTEE2_STANCE_{stance}"]
    if blocked:
        reason_codes.append("COMMITTEE2_STANCE_BLOCKS_ENTRY")
    if recommendation == "PROCEED_REDUCED":
        reason_codes.append("COMMITTEE_REDUCED_SIZE")

    block_reasons: list[str] = []
    if blocked:
        block_reasons.append(f"Committee 2.0 stance {stance}")
        # Phase 4: add named primary block reason from operational payload for traceability
        operational_payload = _v(fd.get("OPERATIONAL_JSON")) or {}
        primary_reason = str(operational_payload.get("block_primary_reason") or "BLOCK_REASON_UNKNOWN")
        reason_codes.append(f"BLOCK_PRIMARY_{primary_reason}")
        block_reasons.append(f"Primary: {primary_reason}")

    role_payloads = _v(fd.get("ROLE_OUTPUTS_JSON")) or {}
    role_outputs: list[dict[str, Any]] = []
    if isinstance(role_payloads, dict):
        for role_name, payload in role_payloads.items():
            p = _v(payload) if not isinstance(payload, dict) else payload
            if not isinstance(p, dict):
                p = {}
            role_outputs.append(
                {
                    "role": str(role_name),
                    "stance": str(p.get("stance_badge") or p.get("stance") or "NEUTRAL")[:40],
                    "confidence": confidence,
                    "summary": str(p.get("one_liner") or "")[:500],
                }
            )

    conf_eval = round(confidence, 4)
    evaluations = {
        "freshness": {"passed": not blocked, "freshness": "COMMITTEE2", "reason": "COMMITTEE2_FINAL", "confidence": conf_eval},
        "trust": {"passed": not blocked, "passed_gate": not blocked, "reason": "COMMITTEE2_FINAL", "confidence": conf_eval},
        "regime": {"passed": not blocked, "passed": not blocked, "regime_compat": "GOOD" if not blocked else "POOR", "confidence": conf_eval},
        "path_quality": {"quality_ok": not blocked, "confidence": conf_eval, "flags": []},
        "trail": {"note": (posture.get("trail_posture") or "")},
    }

    return {
        "recommendation": recommendation,
        "size_factor": size_factor,
        "confidence": confidence,
        "blocked": blocked,
        "block_reasons": block_reasons,
        "reason_codes": reason_codes,
        "risk_note": jd["risk_note"],
        "joint_decision": jd,
        "role_outputs": role_outputs,
        "evaluations": evaluations,
        "structural_source": True,
        "committee2": {
            "hearing_id": fd.get("HEARING_ID"),
            "final_decision_id": fd.get("FINAL_DECISION_ID"),
            "stance": stance,
        },
    }
