"""
Shared structural-proposal priority computation.

The proposal pipeline (MIP/SQL/app/520_sp_propose_structural_trades.sql)
ranks setups by a deterministic composite score, but the score is *not*
persisted on STRUCTURAL_TRADE_PROPOSALS. This module reproduces the
same composite locally so downstream UI surfaces (cockpit Trade Proposals
panel, LPA Committee 2 Exhibits masthead) can:

  1. Order proposals strongest-first by ranking strength rather than
     by CREATED_AT, which is just a clock.
  2. Show an explicit priority indicator (rank + band) so the operator
     can see *which* proposal the system considers strongest.
  3. Surface the dominant reason that proposal ranks highly
     (stronger structure / better regime / higher trust / fresher
     setup, etc.) without forcing the operator to read the whole
     evidence pack.

Design rule: priority is **comparative strength** only. Whether a
proposal is actionable right now (READY_NOW / NEAR_READY / WAIT /
DO_NOT_ENTER) is a separate dimension and is computed elsewhere
(`_entry_readiness` in trade_proposals.py). The two dimensions are
intentionally orthogonal: a #1 proposal can be WAIT and a #5 proposal
can be READY_NOW; the operator reads them independently.

Composite formula (mirrors 520_sp_propose_structural_trades.sql):
    structure   = STRUCTURE_CONFIDENCE  * 0.25
    significance = LEVEL_SIGNIFICANCE   * 0.17
    regime      = 0.20 if GOOD; 0.10 if NEUTRAL; 0 otherwise
    mhr         = MEANINGFUL_HIT_RATE   * 0.18
    pshr        = PATH_SURVIVAL_HIT_RATE * 0.05
                  (BRL override: GREATEST(MHR, PSHR) * 0.05)
    trust       = 0.15 if TRUSTED;
                  0.08 if PROVISIONAL;
                  0.04 if BREAKOUT_RETEST_LONG (calibrated trust bonus);
                  0    otherwise
    fresh       = 0.05 if SETUP_DATE == AS_OF, else 0

Priority bands are rank-position based (top of slate, mid, bottom).
This stays stable when the composite cluster is tight (one bp gap)
and avoids "everything is High" when the slate is uniformly strong.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, List, Optional


# Composite weights — kept in sync with 520_sp_propose_structural_trades.sql.
# If 520 changes its weights, update here and add a regression test.
_W_STRUCTURE      = 0.25
_W_SIGNIFICANCE   = 0.17
_W_REGIME_GOOD    = 0.20
_W_REGIME_NEUTRAL = 0.10
_W_MHR            = 0.18
_W_PSHR           = 0.05
_W_TRUST_TRUSTED  = 0.15
_W_TRUST_PROV     = 0.08
_W_BRL_BONUS      = 0.04
_W_FRESHNESS      = 0.05

# Default fallbacks used when a component is missing — match 520 SP COALESCEs
# so the local composite tracks the SP-side composite as closely as possible.
_DEFAULT_STRUCTURE    = 0.5
_DEFAULT_SIGNIFICANCE = 0.3
_DEFAULT_MHR          = 0.3
_DEFAULT_PSHR         = 0.2

_BRL_FAMILY = "BREAKOUT_RETEST_LONG"


# Reason codes — single source of truth for both the API payload and the UI.
# Deliberately limited to the four operator-meaningful dimensions the user
# called out (trust / fresh / regime / structure). The composite score
# also blends MHR / PSHR / level significance, but those numbers move in
# tight clusters across the slate (every surviving proposal has high MHR
# by construction) so they rarely *differentiate* one proposal from the
# next. Surfacing them as the "main reason" would produce
# uninformative tooltips like "Stronger MHR" on 30 of 37 cards.
REASON_LABELS: Dict[str, str] = {
    "TRUST":     "Higher trust family",
    "FRESH":     "Fresher setup (today)",
    "REGIME":    "Better regime alignment",
    "STRUCTURE": "Stronger structure",
    "BASELINE":  "Balanced across all dimensions",
}

# Per-reason baselines used to compute "gap above baseline". A proposal
# whose contribution beats these baselines is *differentiated* on that
# dimension; whoever has the biggest gap wins. Baselines reflect the
# typical surviving-proposal value, not the absolute floor:
#   structure: 0.5 * 0.25 — same default the SP uses when missing
#   regime:    0.10       — NEUTRAL is the common case; GOOD differentiates
#   trust:     0.0        — RESEARCH is the floor; PROVISIONAL/TRUSTED differentiate
#   fresh:     0.0        — older setups are the common case; today differentiates
_BASELINE_STRUCTURE = 0.5 * _W_STRUCTURE
_BASELINE_REGIME    = _W_REGIME_NEUTRAL
_BASELINE_TRUST     = 0.0
_BASELINE_FRESH     = 0.0
_REASON_GAP_EPSILON = 1e-3

# Band labels — separate from band codes so the API stays terse and the
# UI can still show plain English.
BAND_LABELS: Dict[str, str] = {
    "HIGH":   "High",
    "MEDIUM": "Medium",
    "LOW":    "Low",
}


@dataclass(frozen=True)
class PriorityComponents:
    """Per-proposal composite contributions, rounded to 4 dp.

    Each value already includes its own weight, so the composite score
    is just the sum.
    """
    structure:    float
    significance: float
    regime:       float
    mhr:          float
    pshr:         float
    trust:        float
    fresh:        float

    @property
    def composite(self) -> float:
        return round(
            self.structure + self.significance + self.regime
            + self.mhr + self.pshr + self.trust + self.fresh,
            4,
        )

    def primary_reason(self) -> str:
        """Pick the most *differentiating* reason from the user-meaningful set.

        Contributions like MHR and structure_confidence are correlated
        across the slate (every surviving proposal has them high by
        construction), so the absolute-largest contributor is almost
        always "structure". We instead measure each reason as a *gap
        above its baseline* — how much does this proposal stand out on
        this dimension compared to a typical surviving proposal? The
        biggest gap wins.

        Returns one of: TRUST | FRESH | REGIME | STRUCTURE | BASELINE.
        """
        gaps = [
            ("TRUST",     self.trust     - _BASELINE_TRUST),
            ("FRESH",     self.fresh     - _BASELINE_FRESH),
            ("REGIME",    self.regime    - _BASELINE_REGIME),
            ("STRUCTURE", self.structure - _BASELINE_STRUCTURE),
        ]
        gaps.sort(key=lambda kv: kv[1], reverse=True)
        if gaps[0][1] <= _REASON_GAP_EPSILON:
            return "BASELINE"
        return gaps[0][0]


def compute_components(
    *,
    setup_family:           Optional[str],
    structure_confidence:   Optional[float],
    level_significance:     Optional[float],
    regime_compat:          Optional[str],
    meaningful_hit_rate:    Optional[float],
    path_survival_hit_rate: Optional[float],
    trust_label:            Optional[str],
    setup_date:             Optional[date],
    as_of_date:             date,
) -> PriorityComponents:
    """Reproduce the 520 SP composite locally.

    Inputs may be None / NaN / wrong-typed; we COALESCE to the same
    defaults the SP uses so a missing column still produces a stable
    composite (rather than NaN or a crash).
    """
    fam = (setup_family or "").upper()
    is_brl = fam == _BRL_FAMILY

    sc   = _safe_float(structure_confidence,   _DEFAULT_STRUCTURE)
    ls   = _safe_float(level_significance,     _DEFAULT_SIGNIFICANCE)
    mhr  = _safe_float(meaningful_hit_rate,    _DEFAULT_MHR)
    pshr = _safe_float(path_survival_hit_rate, _DEFAULT_PSHR)

    regime = (regime_compat or "").upper()
    if regime == "GOOD":
        regime_w = _W_REGIME_GOOD
    elif regime == "NEUTRAL":
        regime_w = _W_REGIME_NEUTRAL
    else:
        regime_w = 0.0

    trust = (trust_label or "RESEARCH").upper()
    if trust == "TRUSTED":
        trust_w = _W_TRUST_TRUSTED
    elif trust == "PROVISIONAL":
        trust_w = _W_TRUST_PROV
    elif is_brl:
        # Calibrated BRL bonus — mirrors 520 SP C1 trust treatment.
        trust_w = _W_BRL_BONUS
    else:
        trust_w = 0.0

    # BRL override: path component uses GREATEST(MHR, PSHR) per SP C1.
    pshr_value = max(mhr, pshr) if is_brl else pshr

    fresh_w = _W_FRESHNESS if (setup_date is not None and setup_date == as_of_date) else 0.0

    return PriorityComponents(
        structure    = round(sc          * _W_STRUCTURE,    4),
        significance = round(ls          * _W_SIGNIFICANCE, 4),
        regime       = round(regime_w,                       4),
        mhr          = round(mhr         * _W_MHR,           4),
        pshr         = round(pshr_value  * _W_PSHR,          4),
        trust        = round(trust_w,                        4),
        fresh        = round(fresh_w,                        4),
    )


def assign_band(rank: int, total: int) -> str:
    """Position-based band: top third / middle third / bottom third.

    For very small slates (≤ 3) we just return HIGH for #1, MEDIUM for
    #2, LOW for the rest — `total/3` rounding would otherwise give
    everything HIGH on a 3-item slate.
    """
    if total <= 0 or rank < 1:
        return "LOW"
    if total <= 3:
        if rank == 1:
            return "HIGH"
        if rank == 2:
            return "MEDIUM"
        return "LOW"
    third = max(1, total // 3)
    if rank <= third:
        return "HIGH"
    if rank <= third * 2:
        return "MEDIUM"
    return "LOW"


@dataclass(frozen=True)
class PriorityVerdict:
    rank: int
    band: str
    band_label: str
    reason_code: str
    reason_label: str
    composite_score: float
    components: PriorityComponents


def rank_proposals(
    rows: List[Dict[str, Any]],
    *,
    as_of_date: date,
) -> List[Dict[str, Any]]:
    """Sort `rows` strongest-first by composite, attach priority verdict.

    Each input row must include the fields used by `compute_components`
    plus a stable id (we use `PROPOSAL_ID`). Returns a list of NEW dicts
    in priority order, each carrying:

        priority_rank:        int (1-based)
        priority_band:        'HIGH' | 'MEDIUM' | 'LOW'
        priority_band_label:  'High' | 'Medium' | 'Low'
        priority_reason_code: see REASON_LABELS keys
        priority_reason_label: human-readable
        composite_score:      float
        composite_components: dict of all 7 contributions
        + every original key passed through unchanged

    Tie-break rules (deterministic): higher composite first; on tie,
    lower PROPOSAL_ID wins (older proposals beat newer ones with the
    same score so refreshes don't reshuffle the slate).
    """
    enriched: List[tuple[float, int, Dict[str, Any]]] = []
    for r in rows:
        comps = compute_components(
            setup_family           = r.get("SETUP_FAMILY"),
            structure_confidence   = r.get("STRUCTURE_CONFIDENCE"),
            level_significance     = r.get("LEVEL_SIGNIFICANCE"),
            regime_compat          = r.get("REGIME_COMPAT"),
            meaningful_hit_rate    = r.get("MEANINGFUL_HIT_RATE"),
            path_survival_hit_rate = r.get("PATH_SURVIVAL_HIT_RATE"),
            trust_label            = r.get("TRUST_LABEL"),
            setup_date             = _coerce_date(r.get("SETUP_DATE")),
            as_of_date             = as_of_date,
        )
        try:
            pid = int(r.get("PROPOSAL_ID") or 0)
        except (TypeError, ValueError):
            pid = 0
        enriched.append((comps.composite, pid, _attach(r, comps)))

    # Sort: composite DESC, then PROPOSAL_ID ASC (stable tie-break).
    enriched.sort(key=lambda t: (-t[0], t[1]))

    total = len(enriched)
    out: List[Dict[str, Any]] = []
    for idx, (_, _, row_with_comps) in enumerate(enriched, start=1):
        comps: PriorityComponents = row_with_comps["__components__"]
        band = assign_band(idx, total)
        reason_code = comps.primary_reason()
        verdict_keys = {
            "priority_rank":         idx,
            "priority_band":         band,
            "priority_band_label":   BAND_LABELS.get(band, band),
            "priority_reason_code":  reason_code,
            "priority_reason_label": REASON_LABELS.get(reason_code, reason_code),
            "composite_score":       comps.composite,
            "composite_components": {
                "structure":    comps.structure,
                "significance": comps.significance,
                "regime":       comps.regime,
                "mhr":          comps.mhr,
                "pshr":         comps.pshr,
                "trust":        comps.trust,
                "fresh":        comps.fresh,
            },
        }
        merged = {**row_with_comps, **verdict_keys}
        merged.pop("__components__", None)
        out.append(merged)
    return out


# --- Internal helpers -------------------------------------------------------


def _safe_float(v: Any, default: float) -> float:
    if v is None:
        return default
    try:
        f = float(v)
    except (TypeError, ValueError):
        return default
    if f != f:  # NaN
        return default
    return f


def _coerce_date(v: Any) -> Optional[date]:
    """Accept date / datetime / 'YYYY-MM-DD' / None."""
    if v is None:
        return None
    if isinstance(v, date):
        # date includes datetime; strip time component to match SETUP_DATE.
        return date(v.year, v.month, v.day)
    s = str(v)[:10]
    try:
        y, m, d = s.split("-")
        return date(int(y), int(m), int(d))
    except (ValueError, AttributeError):
        return None


def _attach(row: Dict[str, Any], comps: PriorityComponents) -> Dict[str, Any]:
    new = dict(row)
    new["__components__"] = comps
    return new
