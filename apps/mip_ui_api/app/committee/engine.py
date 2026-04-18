"""
Deterministic Committee 2.0 hearing engine: snapshot vs live evidence, deltas, roles, chair, caps.
Operational output is JSON-safe dicts; explanatory strings are separate.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

# Stance ordering: worst (0) .. best (4)
STANCE_ORDER = ("DENY", "DEFER", "WAIT_RECLAIM", "APPROVE_REDUCED", "APPROVE")

ROLE_NAMES = (
    "STRUCTURAL_THESIS",
    "ENTRY_GEOMETRY",
    "REGIME",
    "PATH_TRADEABILITY",
    "PROTECTION_EXIT",
    "SYMBOL_BEHAVIOR",
)

ARTIFACT_KINDS = (
    "STRUCTURE_MAP",
    "GEOMETRY_METER",
    "REGIME_GAUGE",
    "PATH_STRIP",
    "PROTECTION_STRIP",
    "SYMBOL_FINGERPRINT",
)


def _stance_idx(s: str) -> int:
    return STANCE_ORDER.index(s.upper())


def cap_stance_worse(current: str, cap: str) -> str:
    """Apply cap: return the worse (lower index) of the two stances."""
    a, b = current.upper(), cap.upper()
    return a if _stance_idx(a) <= _stance_idx(b) else b


def _f(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _regime_bucket(label: Optional[str]) -> str:
    if not label:
        return "NEUTRAL"
    u = str(label).upper()
    if u in ("GOOD", "FAVORABLE", "SUPPORTIVE"):
        return "GOOD"
    if u in ("BAD", "HOSTILE", "ADVERSE", "POOR"):
        return "BAD"
    return "NEUTRAL"


@dataclass
class LiveContext:
    latest_price: float
    open_price: Optional[float]
    prior_close: Optional[float]
    structural_state_now: Optional[str]
    trend_regime_now: Optional[str]
    vol_regime_now: Optional[str]
    bar_dates: List[str]


def _entry_zone(snapshot: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    z = snapshot.get("ENTRY_ZONE_JSON") or {}
    if hasattr(z, "as_dict"):
        z = dict(z)
    if not isinstance(z, dict):
        return None, None
    return _f(z.get("low")), _f(z.get("high"))


def _invalidation(snapshot: Dict[str, Any]) -> Tuple[Optional[float], Optional[str]]:
    inv = snapshot.get("INVALIDATION_JSON") or {}
    if hasattr(inv, "as_dict"):
        inv = dict(inv)
    if not isinstance(inv, dict):
        return None, None
    return _f(inv.get("level")), inv.get("rule")


def _path_metrics(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    pm = snapshot.get("PATH_METRICS_JSON") or {}
    if hasattr(pm, "as_dict"):
        pm = dict(pm)
    return pm if isinstance(pm, dict) else {}


def invalidation_breached(side: str, price: float, inv_level: Optional[float]) -> bool:
    if inv_level is None:
        return False
    su = (side or "").upper()
    if su == "LONG":
        return price < inv_level
    if su == "SHORT":
        return price > inv_level
    return False


def compute_hearing_bundle(
    snapshot: Dict[str, Any],
    live: LiveContext,
    evidence_pack_version: str = "1.0.0",
) -> Dict[str, Any]:
    """
    Returns dict with keys: evidence, deltas, roles, chair, operational, explanatory,
    artifacts, evidence_refs_aggregate, stance, confidence.
    """
    side = (snapshot.get("SIDE") or "").upper()
    sym = snapshot.get("SYMBOL") or ""
    zone_low, zone_high = _entry_zone(snapshot)
    inv_level, inv_rule = _invalidation(snapshot)
    pm = _path_metrics(snapshot)
    pct_adv = _f(pm.get("pct_adverse_before_favorable"))
    mhr = _f(pm.get("meaningful_hit_rate"))
    snap_regime = _regime_bucket(snapshot.get("REGIME_STATE"))
    trend_now = (live.trend_regime_now or "").upper()
    struct_now = (live.structural_state_now or "").upper()
    struct_snap = (snapshot.get("STRUCTURAL_STATE") or "").upper()

    price = float(live.latest_price)
    breach = invalidation_breached(side, price, inv_level)

    # --- Geometry distance (mid zone) ---
    mid = None
    if zone_low is not None and zone_high is not None:
        mid = (zone_low + zone_high) / 2.0
    dist_pct = None
    if mid and mid > 0:
        dist_pct = abs(price - mid) / mid * 100.0

    chase_severe = False
    stretch = False
    if side == "LONG" and zone_high is not None:
        if price > zone_high * 1.02:
            chase_severe = True
        elif price > zone_high * 1.008:
            stretch = True
    elif side == "SHORT" and zone_low is not None:
        if price < zone_low * 0.98:
            chase_severe = True
        elif price < zone_low * 0.992:
            stretch = True

    # --- Regime drift heuristic ---
    regime_hostile = snap_regime == "GOOD" and ("DOWN" in trend_now or "BEAR" in trend_now)
    if side == "SHORT":
        regime_hostile = snap_regime == "GOOD" and ("UP" in trend_now or "BULL" in trend_now)

    # --- Path ugliness ---
    path_ugly = pct_adv is not None and pct_adv > 0.42
    path_weak = mhr is not None and mhr < 0.38

    # --- Structure health ---
    thesis_broken = bool(struct_snap) and bool(struct_now) and struct_snap != struct_now

    # --- Nominal stance before caps ---
    nominal = "APPROVE"
    if chase_severe:
        nominal = "WAIT_RECLAIM"
    elif stretch:
        nominal = "APPROVE_REDUCED"
    elif path_ugly or path_weak:
        nominal = "APPROVE_REDUCED"
    elif regime_hostile:
        nominal = "DEFER"

    # --- Apply caps (explicit) ---
    stance = nominal
    if breach:
        stance = cap_stance_worse(stance, "DENY")
    if chase_severe:
        stance = cap_stance_worse(stance, "WAIT_RECLAIM")
    if thesis_broken:
        stance = cap_stance_worse(stance, "DEFER")
    if regime_hostile:
        stance = cap_stance_worse(stance, "DEFER")
    if path_ugly:
        stance = cap_stance_worse(stance, "APPROVE_REDUCED")

    # --- Confidence ---
    conf = 0.72
    if stance == "DENY":
        conf = 0.9
    elif stance in ("DEFER", "WAIT_RECLAIM"):
        conf = 0.55
    elif stance == "APPROVE_REDUCED":
        conf = 0.62
    if path_weak:
        conf *= 0.92
    conf = max(0.05, min(0.95, conf))

    evidence = {
        "latest_price": price,
        "open_price": live.open_price,
        "prior_close": live.prior_close,
        "gap_pct": (
            ((live.open_price - live.prior_close) / live.prior_close * 100.0)
            if live.open_price and live.prior_close and live.prior_close != 0
            else None
        ),
        "structural_state_now": live.structural_state_now,
        "trend_regime_now": live.trend_regime_now,
        "vol_regime_now": live.vol_regime_now,
        "recent_bar_dates": live.bar_dates[:5],
        "zone_distance_pct": dist_pct,
        "invalidation_level": inv_level,
        "invalidation_breached": breach,
    }

    deltas: List[Dict[str, Any]] = [
        {
            "category": "price",
            "summary": "Price vs proposal-time context",
            "detail": f"Latest {price:.4f} vs zone mid {mid:.4f}" if mid else f"Latest {price:.4f}",
            "execution_implication": "Expression quality changes with distance from preferred zone.",
            "evidence_refs": ["hearing.latest_price", "snapshot.ENTRY_ZONE_JSON"],
        },
        {
            "category": "entry_geometry",
            "summary": "Entry geometry delta",
            "detail": (
                "Severe chase above zone high" if chase_severe else "Stretch vs zone" if stretch else "Within normal geometry tolerance"
            ),
            "execution_implication": "Size / wait posture" if (chase_severe or stretch) else "Standard expression",
            "evidence_refs": ["hearing.zone_distance_pct", "snapshot.ENTRY_ZONE_JSON"],
        },
        {
            "category": "structure",
            "summary": "Structural state vs snapshot",
            "detail": f"Snapshot {struct_snap or 'n/a'} vs now {struct_now or 'n/a'}",
            "execution_implication": "Thesis health" + (" — state shift" if thesis_broken else " — aligned"),
            "evidence_refs": ["snapshot.STRUCTURAL_STATE", "hearing.structural_state_now"],
        },
        {
            "category": "regime",
            "summary": "Regime backdrop",
            "detail": f"Proposal regime {snapshot.get('REGIME_STATE')}; trend now {live.trend_regime_now}",
            "execution_implication": "Defer / reduce if backdrop hostile to side",
            "evidence_refs": ["snapshot.REGIME_STATE", "hearing.trend_regime_now"],
        },
        {
            "category": "path",
            "summary": "Path / tradeability",
            "detail": f"Adverse-before-favorable {pct_adv}; MHR {mhr}",
            "execution_implication": "Caution on path-heavy profiles",
            "evidence_refs": ["snapshot.PATH_METRICS_JSON"],
        },
        {
            "category": "freshness",
            "summary": "Evidence freshness",
            "detail": f"Based on latest daily bar {live.bar_dates[0] if live.bar_dates else 'n/a'}",
            "execution_implication": "Refresh if stale vs session needs",
            "evidence_refs": ["hearing.recent_bar_dates"],
        },
        {
            "category": "execution_implication",
            "summary": "Net execution posture",
            "detail": f"Deterministic stance {stance} after policy caps",
            "execution_implication": stance,
            "evidence_refs": ["operational.stance", "deltas.*"],
        },
    ]

    posture = {
        "thesis_health": "BROKEN" if thesis_broken else ("STRESSED" if regime_hostile else "INTACT"),
        "entry_geometry": "SEVERE_CHASE" if chase_severe else ("STRETCH" if stretch else "OK"),
        "regime_fit": "HOSTILE" if regime_hostile else "OK",
        "path_quality": "POOR" if path_ugly or path_weak else "OK",
        "protection_fit": "AT_RISK" if breach else "OK",
        "size_posture": "ZERO" if stance == "DENY" else ("REDUCED" if stance in ("DEFER", "WAIT_RECLAIM", "APPROVE_REDUCED") else "FULL"),
        "trail_posture": "DEFENSIVE" if stance in ("DENY", "DEFER", "WAIT_RECLAIM") else "STANDARD",
    }

    artifacts: List[Dict[str, Any]] = [
        {
            "artifact_kind": "STRUCTURE_MAP",
            "schema_version": "1",
            "payload": {"snapshot_state": struct_snap, "now_state": struct_now, "thesis": posture["thesis_health"]},
            "evidence_refs": ["snapshot.STRUCTURAL_STATE", "hearing.structural_state_now"],
        },
        {
            "artifact_kind": "GEOMETRY_METER",
            "schema_version": "1",
            "payload": {
                "zone_low": zone_low,
                "zone_high": zone_high,
                "price": price,
                "dist_pct": dist_pct,
                "label": posture["entry_geometry"],
            },
            "evidence_refs": ["snapshot.ENTRY_ZONE_JSON", "hearing.latest_price"],
        },
        {
            "artifact_kind": "REGIME_GAUGE",
            "schema_version": "1",
            "payload": {"proposal": snapshot.get("REGIME_STATE"), "trend": live.trend_regime_now, "vol": live.vol_regime_now},
            "evidence_refs": ["snapshot.REGIME_STATE", "hearing.trend_regime_now"],
        },
        {
            "artifact_kind": "PATH_STRIP",
            "schema_version": "1",
            "payload": {"pct_adverse": pct_adv, "mhr": mhr, "label": posture["path_quality"]},
            "evidence_refs": ["snapshot.PATH_METRICS_JSON"],
        },
        {
            "artifact_kind": "PROTECTION_STRIP",
            "schema_version": "1",
            "payload": {"invalidation": inv_level, "breached": breach, "rule": inv_rule},
            "evidence_refs": ["snapshot.INVALIDATION_JSON", "hearing.latest_price"],
        },
        {
            "artifact_kind": "SYMBOL_FINGERPRINT",
            "schema_version": "1",
            "payload": {"symbol": sym, "setup_family": snapshot.get("SETUP_FAMILY"), "note": "V1 deterministic placeholder"},
            "evidence_refs": ["snapshot.SYMBOL", "snapshot.SETUP_FAMILY"],
        },
    ]

    roles: List[Dict[str, Any]] = [
        {
            "role_name": "STRUCTURAL_THESIS",
            "stance_badge": posture["thesis_health"],
            "one_liner": "Thesis " + ("broken vs snapshot" if thesis_broken else "intact"),
            "bullets": [
                f"State {struct_snap} → {struct_now}",
                f"Trust {snapshot.get('TRUST_LABEL')}",
            ],
            "influence": "Thesis strength / defer tone",
            "output": {"thesis_health": posture["thesis_health"]},
            "evidence_refs": ["snapshot.STRUCTURAL_STATE", "hearing.structural_state_now"],
        },
        {
            "role_name": "ENTRY_GEOMETRY",
            "stance_badge": posture["entry_geometry"],
            "one_liner": "Expression geometry " + posture["entry_geometry"].lower().replace("_", " "),
            "bullets": [
                f"Zone {zone_low}–{zone_high}",
                f"Price {price} vs mid distance {dist_pct:.2f}%" if dist_pct is not None else f"Price {price}",
            ],
            "influence": "Wait / reclaim / reduced size",
            "output": {"geometry_stance": posture["entry_geometry"], "dist_pct": dist_pct},
            "evidence_refs": ["snapshot.ENTRY_ZONE_JSON", "hearing.latest_price"],
        },
        {
            "role_name": "REGIME",
            "stance_badge": posture["regime_fit"],
            "one_liner": "Regime fit " + posture["regime_fit"].lower(),
            "bullets": [f"Proposal {snapshot.get('REGIME_STATE')}", f"Trend now {live.trend_regime_now}"],
            "influence": "Confidence shade / defer",
            "output": {"regime_fit": posture["regime_fit"]},
            "evidence_refs": ["snapshot.REGIME_STATE", "hearing.trend_regime_now"],
        },
        {
            "role_name": "PATH_TRADEABILITY",
            "stance_badge": posture["path_quality"],
            "one_liner": "Path quality " + posture["path_quality"].lower(),
            "bullets": [
                f"P(adverse before favorable)={pct_adv}",
                f"MHR={mhr}",
            ],
            "influence": "Reduced size / defer",
            "output": {"path_quality": posture["path_quality"], "pct_adverse": pct_adv, "mhr": mhr},
            "evidence_refs": ["snapshot.PATH_METRICS_JSON"],
        },
        {
            "role_name": "PROTECTION_EXIT",
            "stance_badge": posture["protection_fit"],
            "one_liner": "Invalidation " + ("breached" if breach else "holding"),
            "bullets": [f"Level {inv_level}", f"Rule {inv_rule or 'n/a'}"],
            "influence": "Trail / management caution",
            "output": {"protection_fit": posture["protection_fit"], "breach": breach},
            "evidence_refs": ["snapshot.INVALIDATION_JSON", "hearing.latest_price"],
        },
        {
            "role_name": "SYMBOL_BEHAVIOR",
            "stance_badge": "NEUTRAL",
            "one_liner": "Symbol behavior — V1 baseline",
            "bullets": [f"{sym} / {snapshot.get('SETUP_FAMILY')}", "No adverse fingerprint in V1"],
            "influence": "Patience / nuance only",
            "output": {"symbol_stance": "NEUTRAL"},
            "evidence_refs": ["snapshot.SYMBOL", "snapshot.SETUP_FAMILY"],
        },
    ]

    chair = {
        "stance": stance,
        "confidence": conf,
        "top_supports": [
            s for s in [
                "Geometry tolerable" if not chase_severe and not stretch else None,
                "Invalidation holding" if not breach else None,
                "Path not worst-in-class" if not path_ugly else None,
            ]
            if s
        ],
        "top_tensions": [
            t for t in [
                "Chase / stretch vs zone" if (chase_severe or stretch) else None,
                "Regime hostile to side" if regime_hostile else None,
                "Path stats weak" if path_ugly or path_weak else None,
                "Structural state shifted" if thesis_broken else None,
                "Invalidation breached" if breach else None,
            ]
            if t
        ],
        "execution_shaping": {
            "stance": stance,
            "size_posture": posture["size_posture"],
            "trail_posture": posture["trail_posture"],
            "notes": "Deterministic Committee 2.0 — does not replace broker gates.",
        },
        "what_changed_since_proposal": [d["detail"] for d in deltas if d["category"] != "execution_implication"][:5],
        "evidence_refs": ["roles.*", "deltas.*", "snapshot.*", "hearing.*"],
    }

    operational = {
        "stance": stance,
        "confidence": conf,
        "posture": posture,
        "evidence_pack_version": evidence_pack_version,
        "symbol": sym,
        "side": side,
        "proposal_id": snapshot.get("PROPOSAL_ID"),
        "snapshot_id": snapshot.get("SNAPSHOT_ID"),
    }

    explanatory = {
        "header_subtitle": f"{sym} {side} — deterministic hearing v1",
        "stance_label": stance.replace("_", " "),
    }

    agg_refs = {
        "snapshot_fields": [
            "PROPOSAL_ID",
            "SNAPSHOT_ID",
            "ENTRY_ZONE_JSON",
            "INVALIDATION_JSON",
            "PATH_METRICS_JSON",
            "REGIME_STATE",
            "STRUCTURAL_STATE",
        ],
        "hearing_fields": list(evidence.keys()),
    }

    return {
        "evidence": evidence,
        "deltas": deltas,
        "roles": roles,
        "chair": chair,
        "artifacts": artifacts,
        "operational": operational,
        "explanatory": explanatory,
        "stance": stance,
        "confidence": conf,
        "posture": posture,
        "evidence_refs_aggregate": agg_refs,
    }


def bundle_to_db_json(bundle: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    """Split bundle into EVIDENCE_JSON, DELTAS_JSON, CHAIR_OUTPUT_JSON, OPERATIONAL_JSON."""
    evidence = bundle["evidence"]
    deltas = {"categories": bundle["deltas"]}
    chair = bundle["chair"]
    operational = bundle["operational"]
    return evidence, deltas, chair, operational
