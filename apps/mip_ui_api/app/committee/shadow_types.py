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
ROLE_SLICE_MAP: Dict[str, set] = {
    "STRUCTURAL_THESIS":  {"proposal_meta", "structural_state", "thesis_summary"},
    "ENTRY_GEOMETRY":     {"proposal_meta", "entry_zone", "live_price"},
    "REGIME":             {"proposal_meta", "regime_state", "live_bars"},
    "PATH_TRADEABILITY":  {"proposal_meta", "path_metrics", "mfe_mae"},
    "PROTECTION_EXIT":    {"proposal_meta", "invalidation", "live_price"},
    "SYMBOL_BEHAVIOR":    {"proposal_meta", "trust_label", "path_metrics", "live_bars"},
    "SHADOW_CHAIR": {
        "proposal_meta", "structural_state", "thesis_summary",
        "entry_zone", "live_price", "regime_state", "live_bars",
        "path_metrics", "mfe_mae", "invalidation", "trust_label",
        "deltas_summary", "artifacts_summary",
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
    pack_version: str = "1.0.0"

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
