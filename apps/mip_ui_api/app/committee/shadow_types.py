"""
Shadow Board Phase 1 — Pydantic types, evidence pack builder, and output contracts.

All types here are shadow-only. Nothing here references COMMITTEE_FINAL_DECISION
or the real board stance/confidence/chair fields.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, model_validator

logger = logging.getLogger(__name__)

STANCE_ORDER = ("DENY", "DEFER", "WAIT_RECLAIM", "APPROVE_REDUCED", "APPROVE")
ALLOWED_STANCES = set(STANCE_ORDER)

SHADOW_ROLES = (
    "STRUCTURAL_THESIS",
    "ENTRY_GEOMETRY",
    "REGIME",
    "PATH_TRADEABILITY",
    "PROTECTION_EXIT",
    "SYMBOL_BEHAVIOR",
)

# Role → allowed slice names (closed-world; mirrors GET_SHADOW_EVIDENCE_SLICE)
# Phase 4 slices (phase4_thesis_verdict, phase4_dossier_context) are structural
# prior from proposal night. intraday_session_picture is live RTH substantiation.
ROLE_SLICE_MAP: Dict[str, set] = {
    "STRUCTURAL_THESIS": {
        "proposal_meta", "structural_state", "thesis_summary",
        "phase4_thesis_verdict", "phase4_dossier_context",
    },
    "ENTRY_GEOMETRY": {
        "proposal_meta", "entry_zone", "live_price",
        "phase4_dossier_context", "intraday_session_picture",
    },
    "REGIME": {
        "proposal_meta", "regime_state", "live_bars",
        "phase4_dossier_context", "intraday_session_picture",
    },
    "PATH_TRADEABILITY": {
        "proposal_meta", "path_metrics", "mfe_mae",
        "phase4_thesis_verdict", "phase4_dossier_context",
        "intraday_session_picture",
    },
    "PROTECTION_EXIT": {
        "proposal_meta", "invalidation", "live_price",
        "phase4_thesis_verdict", "phase4_dossier_context",
    },
    "SYMBOL_BEHAVIOR": {
        "proposal_meta", "trust_label", "path_metrics", "live_bars",
        "phase4_dossier_context", "intraday_session_picture",
    },
    "SHADOW_CHAIR": {
        "proposal_meta", "structural_state", "thesis_summary",
        "entry_zone", "live_price", "regime_state", "live_bars",
        "path_metrics", "mfe_mae", "invalidation", "trust_label",
        "deltas_summary", "artifacts_summary",
        "phase4_thesis_verdict", "phase4_dossier_context",
        "intraday_session_picture",
    },
}


def _stance_idx(s: str) -> int:
    try:
        return STANCE_ORDER.index(s.upper())
    except ValueError:
        return -1


# ---------------------------------------------------------------------------
# Evidence pack types
# ---------------------------------------------------------------------------

class ShadowEvidencePack(BaseModel):
    """
    De-verdicted evidence pack staged for agent tool access.
    Real board stance, confidence, chair narrative, and posture are excluded.
    Only raw evidence: snapshot, live context, deltas, artifacts (no role stances).
    """
    hearing_id: str
    proposal_id: int
    pack_version: str = "2.1.0"

    # Pre-computed slices — keyed by slice_name, consumed by GET_SHADOW_EVIDENCE_SLICE
    slices: Dict[str, Any] = Field(default_factory=dict)

    def to_cache_dict(self) -> Dict[str, Any]:
        """Serialize for storage in SHADOW_EVIDENCE_PACK_CACHE.PACK_JSON."""
        return self.model_dump()


# ---------------------------------------------------------------------------
# Stage 1 — Specialist position
# ---------------------------------------------------------------------------

class SpecialistPosition(BaseModel):
    role: str
    stance: str
    confidence: float = Field(ge=0.0, le=1.0)
    rationale: str = ""
    evidence_used: List[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _normalise(self) -> "SpecialistPosition":
        self.stance = self.stance.upper()
        self.role = self.role.upper()
        return self

    def is_valid_stance(self) -> bool:
        return self.stance in ALLOWED_STANCES


class DegradedPosition(BaseModel):
    """Used when an agent fails to produce a parseable position."""
    role: str
    stance: str = "DEFER"
    confidence: float = 0.0
    rationale: str = ""
    evidence_used: List[str] = Field(default_factory=list)
    degraded: bool = True
    degraded_reason: str = ""
    parse_ok: bool = False


# ---------------------------------------------------------------------------
# Stage 2 — Conflict detection
# ---------------------------------------------------------------------------

class ConflictEntry(BaseModel):
    role_a: str
    role_b: str
    stance_a: str
    stance_b: str
    severity: str  # MINOR | MAJOR | CRITICAL
    challenger_role: Optional[str] = None
    target_role: Optional[str] = None


def detect_conflicts(positions: List[SpecialistPosition]) -> List[ConflictEntry]:
    """
    Python-side conflict detection. Returns all pairs with stance disagreements.
    severity:
      CRITICAL — APPROVE vs DENY (index distance >= 4)
      MAJOR    — APPROVE vs WAIT_RECLAIM or DEFER (index distance >= 2)
      MINOR    — adjacent stances (index distance == 1)

    Challenger: the DENY/DEFER side (more conservative).
    Target: the APPROVE/APPROVE_REDUCED side.
    """
    conflicts: List[ConflictEntry] = []
    valid = [p for p in positions if p.stance in ALLOWED_STANCES]

    for i in range(len(valid)):
        for j in range(i + 1, len(valid)):
            a, b = valid[i], valid[j]
            ia, ib = _stance_idx(a.stance), _stance_idx(b.stance)
            dist = abs(ia - ib)
            if dist == 0:
                continue
            severity = "MINOR" if dist == 1 else "MAJOR" if dist < 4 else "CRITICAL"
            # Challenger is the more conservative specialist (lower stance index)
            if ia <= ib:
                challenger, target = a, b
            else:
                challenger, target = b, a
            conflicts.append(ConflictEntry(
                role_a=a.role,
                role_b=b.role,
                stance_a=a.stance,
                stance_b=b.stance,
                severity=severity,
                challenger_role=challenger.role,
                target_role=target.role,
            ))
    return conflicts


def pick_primary_conflict(conflicts: List[ConflictEntry]) -> Optional[ConflictEntry]:
    """Return the most severe conflict for Stage 3 (CRITICAL first, then MAJOR, then MINOR)."""
    order = {"CRITICAL": 0, "MAJOR": 1, "MINOR": 2}
    if not conflicts:
        return None
    return min(conflicts, key=lambda c: order.get(c.severity, 9))


# ---------------------------------------------------------------------------
# Stage 3 — Challenge turn
# ---------------------------------------------------------------------------

class ChallengeTurn(BaseModel):
    challenger_role: str
    target_role: str
    challenge_text: str
    parse_ok: bool = True
    degraded: bool = False
    degraded_reason: str = ""


# ---------------------------------------------------------------------------
# Stage 4 — Revision turn
# ---------------------------------------------------------------------------

class RevisionTurn(BaseModel):
    role_name: str
    revised_stance: str
    original_stance: str
    stance_changed: bool = False
    revision_note: str = ""
    parse_ok: bool = True
    degraded: bool = False
    degraded_reason: str = ""

    @model_validator(mode="after")
    def _set_changed(self) -> "RevisionTurn":
        self.stance_changed = (
            self.revised_stance.upper() != self.original_stance.upper()
        )
        return self


# ---------------------------------------------------------------------------
# Stage 5 — Shadow chair ruling
# ---------------------------------------------------------------------------

class ShadowTradeArtifact(BaseModel):
    entry_zone: str = ""
    size_posture: str = "REDUCED"
    trail_posture: str = "NORMAL"
    key_condition: str = ""
    advisory_only: bool = True
    # Trailing Stop Phase 1: shadow chair must also recommend an exit
    # contract using the same bounded profile registry as the primary
    # path. Empty string = no recommendation (treat as FIXED_STANDARD).
    exit_profile: str = ""
    exit_policy: str = ""


class ShadowChairRuling(BaseModel):
    shadow_stance: str
    shadow_confidence: float = Field(ge=0.0, le=1.0)
    plurality_basis: str = ""
    conflict_resolution: str = ""
    top_supports: List[str] = Field(default_factory=list)
    top_tensions: List[str] = Field(default_factory=list)
    shadow_trade: Optional[ShadowTradeArtifact] = None
    parse_ok: bool = True
    degraded: bool = False
    degraded_reason: str = ""

    @model_validator(mode="after")
    def _normalise(self) -> "ShadowChairRuling":
        self.shadow_stance = self.shadow_stance.upper()
        return self


# ---------------------------------------------------------------------------
# Full session result (assembled post-run for API response)
# ---------------------------------------------------------------------------

class ShadowBoardResult(BaseModel):
    session_id: str
    hearing_id: str
    proposal_id: int
    status: str  # COMPLETE | DEGRADED | FAILED
    stage_reached: int = 0

    positions: List[Any] = Field(default_factory=list)      # SpecialistPosition | DegradedPosition
    conflicts: List[ConflictEntry] = Field(default_factory=list)
    challenge: Optional[ChallengeTurn] = None
    revisions: List[RevisionTurn] = Field(default_factory=list)
    chair: Optional[ShadowChairRuling] = None

    shadow_stance: Optional[str] = None
    shadow_confidence: Optional[float] = None
    degraded: bool = False
    degraded_reason: Optional[str] = None
    run_ms: Optional[int] = None


# ---------------------------------------------------------------------------
# Evidence pack builder
# ---------------------------------------------------------------------------

def _safe_variant(v: Any) -> Any:
    if v is None:
        return {}
    if isinstance(v, str):
        try:
            return json.loads(v)
        except Exception:
            return {}
    if hasattr(v, "as_dict"):
        return dict(v)
    if isinstance(v, (dict, list)):
        return v
    return {}


def build_shadow_evidence_pack(
    hearing: Dict[str, Any],
    snapshot: Dict[str, Any],
    proposal: Dict[str, Any],
    roles: List[Dict[str, Any]],
    artifacts: List[Dict[str, Any]],
    phase4_thesis: Optional[Dict[str, Any]] = None,
    phase4_dossier: Optional[Dict[str, Any]] = None,
    intraday_session_picture: Optional[Dict[str, Any]] = None,
) -> ShadowEvidencePack:
    """
    Build the ShadowEvidencePack from Committee 2.0 DB rows.

    EXCLUDED (real board verdict):
      - hearing.STANCE
      - hearing.CONFIDENCE
      - hearing.CHAIR_OUTPUT_JSON
      - role output stance_badges / one_liners / influence fields
      - any COMMITTEE_FINAL_DECISION data

    INCLUDED:
      - Raw snapshot fields (structural_state, entry_zone, regime, path_metrics, etc.)
      - Raw live context (latest_price, structural_state_now, trend_regime_now, etc.)
      - Delta categories (drift buckets, no final stance)
      - Artifact existence + kind (not raw geometry payloads)
      - Phase 4 thesis verdict slice (phase4_thesis_verdict) — null-safe
      - Phase 4 dossier context slice (phase4_dossier_context) — null-safe

    phase4_thesis and phase4_dossier may be empty dicts for pre-Phase-4 proposals.
    In that case the new slices carry phase4_available=False and all fields are None.
    """
    hid = str(hearing.get("HEARING_ID") or "")
    pid = int(proposal.get("PROPOSAL_ID") or 0)

    ev = _safe_variant(hearing.get("EVIDENCE_JSON")) or {}
    deltas_raw = _safe_variant(hearing.get("DELTAS_JSON")) or {}
    entry_zone = _safe_variant(snapshot.get("ENTRY_ZONE_JSON")) or {}
    inv_json = _safe_variant(snapshot.get("INVALIDATION_JSON")) or {}
    path_json = _safe_variant(snapshot.get("PATH_METRICS_JSON")) or {}
    mfe_mae_json = _safe_variant(snapshot.get("MFE_MAE_JSON")) or {}
    prop_summary = _safe_variant(snapshot.get("PROPOSAL_SUMMARY_JSON")) or {}

    latest_price = ev.get("latest_price")
    open_price = ev.get("open_price")
    prior_close = ev.get("prior_close")
    struct_now = ev.get("structural_state_now")
    trend_now = ev.get("trend_regime_now")
    vol_now = ev.get("vol_regime_now")
    bar_dates = ev.get("bar_dates") or []
    bar_trace = ev.get("recent_bar_trace") or []
    inv_level = ev.get("invalidation_level")
    inv_breached = bool(ev.get("invalidation_breached"))

    symbol = str(proposal.get("SYMBOL") or snapshot.get("SYMBOL") or "")
    side = str(proposal.get("DIRECTION") or snapshot.get("SIDE") or "")
    setup_family = str(snapshot.get("SETUP_FAMILY") or "")
    proposal_ts = str(snapshot.get("PROPOSAL_TS") or "")
    struct_snap = str(snapshot.get("STRUCTURAL_STATE") or "")
    regime_snap = str(snapshot.get("REGIME_STATE") or "")
    trust_label = str(snapshot.get("TRUST_LABEL") or "")
    trailing_style = str(snapshot.get("TRAILING_STYLE") or "")

    # Trailing Stop Phase 1 — surface the real action's exit-policy contract
    # to the shadow chair so its shadow_trade recommendation can be compared
    # against (and dissent from) the executed policy. EXIT_PROFILE lives on
    # STRUCTURAL_TRADE_PROPOSALS / STRUCTURAL_RISK_POLICY (resolved into the
    # proposal SELECT). EXIT_POLICY is materialized on LIVE_ACTIONS but we
    # also derive it here from the profile so shadow runs that fire before a
    # LIVE_ACTIONS row exists still see the intended bracket type.
    real_exit_profile = str(
        proposal.get("EXIT_PROFILE")
        or snapshot.get("EXIT_PROFILE")
        or proposal.get("EXIT_POLICY_REASON")
        or ""
    ).upper()
    real_exit_policy = str(
        proposal.get("EXIT_POLICY") or snapshot.get("EXIT_POLICY") or ""
    ).upper()
    if not real_exit_policy and real_exit_profile:
        try:
            from app.services.live_intelligence import (
                exit_policy as _exit_policy_service,
            )
            real_exit_policy = _exit_policy_service.resolve_exit_policy(
                real_exit_profile
            )["exit_policy"]
        except Exception:
            real_exit_policy = ""

    # Compute zone distance (entry geometry)
    zone_low = entry_zone.get("low") or entry_zone.get("zone_low")
    zone_high = entry_zone.get("high") or entry_zone.get("zone_high")
    dist_pct = None
    if zone_low is not None and zone_high is not None and latest_price is not None:
        try:
            mid = (float(zone_low) + float(zone_high)) / 2.0
            if mid > 0:
                dist_pct = round((float(latest_price) - mid) / mid * 100, 3)
        except (TypeError, ValueError):
            pass

    # Invalidation cushion
    inv_cushion_pct = None
    if inv_level is not None and latest_price is not None:
        try:
            cushion = abs(float(latest_price) - float(inv_level))
            if float(latest_price) > 0:
                inv_cushion_pct = round(cushion / float(latest_price) * 100, 3)
        except (TypeError, ValueError):
            pass

    # Delta categories (drift buckets only, no verdict)
    delta_categories = []
    if isinstance(deltas_raw, dict):
        cats = deltas_raw.get("categories") or []
        for cat in cats:
            if isinstance(cat, dict):
                delta_categories.append({
                    "category": cat.get("category"),
                    "detail": cat.get("detail"),
                    "direction": cat.get("direction"),
                })

    # Artifact summary (kind only, not raw payloads)
    artifact_kinds = [a.get("artifact_kind") or a.get("ARTIFACT_KIND") for a in artifacts]

    # Role evidence_refs only (no stance badges)
    role_evidence_refs = {}
    for r in roles:
        rname = str(r.get("role_name") or r.get("ROLE_NAME") or "")
        refs = r.get("evidence_refs") or r.get("EVIDENCE_REFS") or []
        if rname:
            role_evidence_refs[rname] = refs if isinstance(refs, list) else []

    slices = {
        "proposal_meta": {
            "proposal_id": pid,
            "symbol": symbol,
            "side": side,
            "setup_family": setup_family,
            "proposal_ts": proposal_ts,
            "trailing_style": trailing_style,
            "exit_policy": real_exit_policy,
            "exit_profile": real_exit_profile,
        },
        "structural_state": {
            "structural_state_at_proposal": struct_snap,
            "structural_state_now": struct_now,
            "state_changed": (struct_snap != struct_now) if struct_snap and struct_now else None,
        },
        "thesis_summary": {
            "proposal_summary": prop_summary,
            "structural_state_at_proposal": struct_snap,
            "structural_state_now": struct_now,
            "setup_family": setup_family,
            "trust_label": trust_label,
        },
        "entry_zone": {
            "zone_low": zone_low,
            "zone_high": zone_high,
            "latest_price": latest_price,
            "zone_distance_pct": dist_pct,
            "side": side,
        },
        "live_price": {
            "latest_price": latest_price,
            "open_price": open_price,
            "prior_close": prior_close,
            "symbol": symbol,
        },
        "regime_state": {
            "regime_at_proposal": regime_snap,
            "trend_regime_now": trend_now,
            "vol_regime_now": vol_now,
        },
        "live_bars": {
            "bar_dates": bar_dates,
            "recent_bar_trace": bar_trace,
            "vol_regime_now": vol_now,
        },
        "path_metrics": {
            "path_metrics": path_json,
            "mfe_mae": mfe_mae_json,
        },
        "mfe_mae": {
            "mfe_mae": mfe_mae_json,
        },
        "invalidation": {
            "invalidation_json": inv_json,
            "invalidation_level": inv_level,
            "invalidation_breached": inv_breached,
            "latest_price": latest_price,
            "cushion_pct": inv_cushion_pct,
        },
        "trust_label": {
            "trust_label": trust_label,
            "setup_family": setup_family,
            "symbol": symbol,
        },
        "deltas_summary": {
            "delta_categories": delta_categories,
            "count": len(delta_categories),
        },
        "artifacts_summary": {
            "artifact_kinds": artifact_kinds,
        },
    }

    # -----------------------------------------------------------------------
    # Phase 4 slices — null-safe for pre-Phase-4 proposals.
    # phase4_thesis: row from PROPOSAL_BOARD_THESIS_VERDICT (may be empty dict).
    # phase4_dossier: row from PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT (may be empty).
    # Mirroring the field projection from board/explanation.py _phase4_chair_view.
    # -----------------------------------------------------------------------
    _p4t = phase4_thesis or {}
    _p4d = phase4_dossier or {}
    _p4t_available = bool(_p4t)
    _p4d_available = bool(_p4d)

    chair_json = _safe_variant(_p4t.get("CHAIR_OUTPUT_JSON")) if _p4t else {}
    dossier_payload = _safe_variant(_p4d.get("DOSSIER_PAYLOAD_JSON")) if _p4d else {}
    act_ctx  = dossier_payload.get("actionability_context") or {} if dossier_payload else {}
    candle_ps = dossier_payload.get("candle_psychology") or {} if dossier_payload else {}
    levels    = dossier_payload.get("levels") or {} if dossier_payload else {}
    timeline  = dossier_payload.get("structural_timeline_summary") or {} if dossier_payload else {}

    slices["phase4_thesis_verdict"] = {
        "phase4_available": _p4t_available,
        "board_run_id": proposal.get("BOARD_RUN_ID"),
        "board_dossier_id": proposal.get("BOARD_DOSSIER_ID"),
        "final_action":         _p4t.get("FINAL_ACTION"),
        "final_direction":      _p4t.get("FINAL_DIRECTION"),
        "final_thesis":         _p4t.get("FINAL_THESIS"),
        "thesis_health":        chair_json.get("thesis_health"),
        "prior_thesis_reference": chair_json.get("prior_thesis_reference"),
        "actionability_summary": chair_json.get("actionability_summary"),
        "primary_reason_code":  _p4t.get("PRIMARY_REASON_CODE"),
        "secondary_reason_code": _p4t.get("SECONDARY_REASON_CODE"),
        "why_not_opposite":     _p4t.get("WHY_NOT_OPPOSITE"),
        "why_not_no_trade":     _p4t.get("WHY_NOT_NO_TRADE"),
        "risk_treatment":       _p4t.get("RISK_TREATMENT"),
    }

    slices["phase4_dossier_context"] = {
        "phase4_available": _p4d_available,
        "temporal_authority": "STRUCTURAL_PRIOR",
        "as_of": "proposal_night",
        "continuation_quality":              act_ctx.get("continuation_quality"),
        "resistance_overhead_risk":          act_ctx.get("resistance_overhead_risk"),
        "broken_resistance_support_confidence": act_ctx.get("broken_resistance_support_confidence"),
        "candle_psychology":                 candle_ps,
        "recent_cluster":                    candle_ps.get("recent_cluster"),
        "broken_resistance_as_support":      levels.get("broken_resistance_as_support"),
        "nearest_support":                   (levels.get("nearest_support") or {}).get("level_price"),
        "nearest_resistance":                (levels.get("nearest_resistance") or {}).get("level_price"),
        "current_range_position_pct":        timeline.get("current_range_position_pct"),
        "trend_shape_class":                 timeline.get("trend_shape_class"),
        "structural_timeline_summary":       timeline,
        "levels":                            levels,
        "actionability_context":             act_ctx,
    }

    slices["intraday_session_picture"] = intraday_session_picture or {
        "session_available": False,
        "temporal_authority": "EXECUTION_SUBSTANTIATION",
        "reason": "NOT_COMPUTED",
        "operator_line": "Intraday session picture was not computed for this run.",
    }

    return ShadowEvidencePack(
        hearing_id=hid,
        proposal_id=pid,
        slices=slices,
    )


def parse_specialist_position(raw_text: str, role: str) -> SpecialistPosition | DegradedPosition:
    """
    Parse agent response text into a SpecialistPosition.
    Returns DegradedPosition on any parse failure.
    """
    text = (raw_text or "").strip()
    # Strip markdown fences if present
    if text.startswith("```"):
        lines = text.splitlines()
        lines = [l for l in lines if not l.startswith("```")]
        text = "\n".join(lines).strip()
    try:
        data = json.loads(text)
        stance = str(data.get("stance") or "DEFER").upper()
        if stance not in ALLOWED_STANCES:
            stance = "DEFER"
        return SpecialistPosition(
            role=str(data.get("role") or role).upper(),
            stance=stance,
            confidence=float(data.get("confidence") or 0.5),
            rationale=str(data.get("rationale") or ""),
            evidence_used=list(data.get("evidence_used") or []),
        )
    except Exception as exc:
        logger.warning("parse_specialist_position failed for %s: %s", role, exc)
        return DegradedPosition(
            role=role.upper(),
            stance="DEFER",
            degraded=True,
            degraded_reason=f"JSON parse failed: {exc}",
        )


def parse_chair_ruling(raw_text: str) -> ShadowChairRuling:
    """Parse chair agent response. Returns degraded ruling on failure."""
    text = (raw_text or "").strip()
    if text.startswith("```"):
        lines = text.splitlines()
        lines = [l for l in lines if not l.startswith("```")]
        text = "\n".join(lines).strip()
    try:
        data = json.loads(text)
        stance = str(data.get("shadow_stance") or "DEFER").upper()
        if stance not in ALLOWED_STANCES:
            stance = "DEFER"
        trade_data = data.get("shadow_trade") or {}
        trade = ShadowTradeArtifact(
            entry_zone=str(trade_data.get("entry_zone") or ""),
            size_posture=str(trade_data.get("size_posture") or "REDUCED"),
            trail_posture=str(trade_data.get("trail_posture") or "NORMAL"),
            key_condition=str(trade_data.get("key_condition") or ""),
            advisory_only=True,
            exit_profile=str(trade_data.get("exit_profile") or "").upper(),
            exit_policy=str(trade_data.get("exit_policy") or "").upper(),
        ) if trade_data else None
        return ShadowChairRuling(
            shadow_stance=stance,
            shadow_confidence=float(data.get("shadow_confidence") or 0.5),
            plurality_basis=str(data.get("plurality_basis") or ""),
            conflict_resolution=str(data.get("conflict_resolution") or ""),
            top_supports=list(data.get("top_supports") or []),
            top_tensions=list(data.get("top_tensions") or []),
            shadow_trade=trade,
        )
    except Exception as exc:
        logger.warning("parse_chair_ruling failed: %s", exc)
        return ShadowChairRuling(
            shadow_stance="DEFER",
            shadow_confidence=0.0,
            plurality_basis="",
            conflict_resolution="",
            parse_ok=False,
            degraded=True,
            degraded_reason=f"JSON parse failed: {exc}",
        )


def _final_stance_for_role(
    role: str,
    pos: Any,
    revisions: List["RevisionTurn"],
) -> str:
    stance = str(getattr(pos, "stance", "DEFER") or "DEFER").upper()
    rev = next(
        (r for r in revisions if r.role_name == role and not r.degraded and r.parse_ok),
        None,
    )
    if rev is not None:
        stance = str(rev.revised_stance or stance).upper()
    if stance not in ALLOWED_STANCES:
        stance = "DEFER"
    return stance


def specialists_ready_for_plurality_fallback(positions: Dict[str, Any]) -> bool:
    """True when every specialist produced a parseable, non-degraded position."""
    if len(positions) < len(SHADOW_ROLES):
        return False
    for role in SHADOW_ROLES:
        pos = positions.get(role)
        if pos is None:
            return False
        if getattr(pos, "degraded", False):
            return False
        if getattr(pos, "parse_ok", True) is False:
            return False
        stance = str(getattr(pos, "stance", "") or "").upper()
        if stance not in ALLOWED_STANCES:
            return False
    return True


def _plurality_stance_from_counts(counts: Dict[str, int]) -> str:
    """Pick plurality stance; break ties conservatively (DENY > DEFER > ... > APPROVE)."""
    if not counts:
        return "DEFER"
    max_count = max(counts.values())
    tied = [stance for stance, count in counts.items() if count == max_count]
    if len(tied) == 1:
        return tied[0]
    for stance in STANCE_ORDER:
        if stance in tied:
            return stance
    return tied[0]


def synthesize_chair_plurality_fallback(
    positions: Dict[str, Any],
    revisions: List["RevisionTurn"],
    conflicts: List["ConflictEntry"],
) -> ShadowChairRuling:
    """
    Deterministic chair fallback when SHADOW_CHAIR_AGENT returns no parseable JSON
    but all specialists produced valid positions.
    """
    from collections import Counter

    stance_counts: Counter[str] = Counter()
    conf_by_stance: Dict[str, List[float]] = {}
    for role, pos in positions.items():
        stance = _final_stance_for_role(role, pos, revisions)
        stance_counts[stance] += 1
        conf = float(getattr(pos, "confidence", 0.5) or 0.5)
        conf_by_stance.setdefault(stance, []).append(conf)

    plurality = _plurality_stance_from_counts(dict(stance_counts))
    confs = conf_by_stance.get(plurality) or [0.5]
    avg_conf = sum(confs) / len(confs)
    basis_parts = [f"{stance} x{count}" for stance, count in stance_counts.most_common()]
    basis = f"Plurality fallback (chair agent returned no JSON): {', '.join(basis_parts)}."

    conflict_note = ""
    if conflicts:
        top = pick_primary_conflict(conflicts)
        if top is not None:
            conflict_note = (
                f"Primary conflict {top.role_a}[{top.stance_a}] vs "
                f"{top.role_b}[{top.stance_b}] ({top.severity}); "
                "resolved by conservative plurality tie-break."
            )

    size_posture = "REDUCED" if plurality == "APPROVE_REDUCED" else (
        "MINIMAL" if plurality in ("WAIT_RECLAIM", "DEFER", "DENY") else "FULL"
    )
    exit_profile = "TRAIL_STANDARD" if plurality in ("APPROVE", "APPROVE_REDUCED") else "FIXED_STANDARD"
    exit_policy = "TRAIL_BRACKET" if exit_profile.startswith("TRAIL_") else "FIXED_BRACKET"

    return ShadowChairRuling(
        shadow_stance=plurality,
        shadow_confidence=round(min(max(avg_conf, 0.4), 0.85), 2),
        plurality_basis=basis[:500],
        conflict_resolution=conflict_note[:2000],
        top_supports=[
            f"{role}: {_final_stance_for_role(role, pos, revisions)}"
            for role, pos in positions.items()
        ],
        top_tensions=[f"{c.role_a} vs {c.role_b} ({c.severity})" for c in conflicts[:3]],
        shadow_trade=ShadowTradeArtifact(
            size_posture=size_posture,
            trail_posture="TIGHT" if plurality == "APPROVE_REDUCED" else "NORMAL",
            exit_profile=exit_profile,
            exit_policy=exit_policy,
            advisory_only=True,
        ),
        parse_ok=True,
        degraded=False,
        degraded_reason="",
    )
