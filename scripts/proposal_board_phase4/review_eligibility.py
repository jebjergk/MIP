"""
Phase 4 Cortex Agentic Proposal Board — direction-neutral review eligibility.

Purpose
=======
Decide whether a symbol deserves a *full* Cortex Agent review on a given run.
This is purely a budget / coverage filter. It MUST NOT decide:

  * trade direction (long / short)
  * trade vs no-trade
  * any policy verdict
  * which specialist or chair would say what

Those remain the job of the agents. This filter only answers:
"Is there enough new structural evidence here today to spend agent
budget on this symbol, or should we record a skip?"

Inputs
======
The dossier payload as already produced by
`MIP.MART.V_PROPOSAL_BOARD_SYMBOL_DOSSIER`. We deliberately read only
structural/training/outcome evidence and recent proposal/live-action/trade
memory — no horizontal-signal or candidate-review paths.

Output
======
EligibilityDecision describing whether the symbol is eligible, the
primary include or skip reason code, optional secondary reason, the
structural flags considered, and a compact evidence summary. The
orchestrator persists one row per symbol to
``MIP.APP.PROPOSAL_BOARD_REVIEW_ELIGIBILITY``.

Tuning
======
Thresholds are intentionally conservative. We err on the side of
running the agents — the goal is to skip clearly-stale symbols, not to
gate the board. A per-run summary is logged so we can ratchet the
thresholds later if needed.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date as _date
from datetime import datetime as _dt
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Reason-code allowlist (mirrors MIP/SQL/app/571_phase4_review_eligibility.sql)
# ---------------------------------------------------------------------------

INCLUDE_REASONS = {
    "ELIG_OPEN_POSITION",
    "ELIG_ACTIVE_LIVE_ACTION",
    "ELIG_RECENT_SETUP_EVENT",
    "ELIG_STATE_OR_REGIME_CHANGE",
    "ELIG_NEAR_KEY_LEVEL",
    "ELIG_ABNORMAL_CANDLE",
    "ELIG_RECENT_PROPOSAL_OR_TRADE_MEMORY",
    "ELIG_STRONG_STRUCTURAL_TRUST",
    "ELIG_OVERRIDE_SYMBOL_FILTER",
}

SKIP_REASONS = {
    "NO_MATERIAL_NEW_EVIDENCE",
    "INSUFFICIENT_STRUCTURAL_HISTORY",
    "FAR_FROM_ACTIONABLE_LEVELS",
    "NO_RECENT_STATE_CHANGE",
}

# ---------------------------------------------------------------------------
# Tuning thresholds (conservative defaults)
# ---------------------------------------------------------------------------

# A setup event qualifies as "recent" if its setup_date is within this many
# days of the run's as_of date.
SETUP_RECENT_DAYS = 10

# A structural state transition qualifies as "recent" if state_entered_date
# is within this many days of the run's as_of date.
STATE_RECENT_DAYS = 14

# Price counts as "near a key level" if either nearest_support or
# nearest_resistance is within this percent distance.
NEAR_LEVEL_PCT = 3.0

# A daily candle counts as "abnormal" if its body or range is at or above
# this percent of the latest close, OR the wick ratio is at or above
# WICK_RATIO_THRESHOLD.
ABNORMAL_BODY_PCT = 3.0
ABNORMAL_RANGE_PCT = 4.5
WICK_RATIO_THRESHOLD = 0.55

# History counts as strong if the meaningful_hit_rate >= STRONG_HIT_RATE
# and n_setups >= STRONG_MIN_N for at least one setup family in either
# long_history or short_history.
STRONG_HIT_RATE = 0.65
STRONG_MIN_N = 100

# Recent-memory windows.
RECENT_PROPOSAL_DAYS = 14


# ---------------------------------------------------------------------------
# Result shape
# ---------------------------------------------------------------------------

@dataclass
class EligibilityDecision:
    symbol: str
    market_type: str
    eligible: bool
    primary_reason_code: str
    secondary_reason_code: Optional[str] = None
    signal_flags: Dict[str, bool] = field(default_factory=dict)
    evidence_summary: Dict[str, Any] = field(default_factory=dict)
    notes: Optional[str] = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _to_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _parse_date(v: Any) -> Optional[_date]:
    if v is None:
        return None
    if isinstance(v, _date) and not isinstance(v, _dt):
        return v
    if isinstance(v, _dt):
        return v.date()
    s = str(v).strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
        try:
            return _dt.strptime(s[: len(fmt) + 3] if "." in fmt else s[: len(fmt)], fmt).date()
        except ValueError:
            continue
    return None


def _days_between(a: Optional[_date], b: Optional[_date]) -> Optional[int]:
    if a is None or b is None:
        return None
    try:
        return (a - b).days
    except Exception:  # noqa: BLE001
        return None


# ---------------------------------------------------------------------------
# Individual signal evaluators
# ---------------------------------------------------------------------------

def _has_open_position(memory: Dict[str, Any]) -> bool:
    """memory.open_position_context is non-empty / non-zero."""
    if not isinstance(memory, dict):
        return False
    op = memory.get("open_position_context")
    if not isinstance(op, dict) or not op:
        return False
    qty_keys = ("quantity", "qty", "shares", "open_quantity", "net_quantity")
    for k in qty_keys:
        v = _to_float(op.get(k))
        if v is not None and abs(v) > 0:
            return True
    # fall back to "any positive integer-like value"
    for v in op.values():
        f = _to_float(v)
        if f is not None and abs(f) > 0:
            return True
    return False


def _has_active_live_action(memory: Dict[str, Any]) -> bool:
    if not isinstance(memory, dict):
        return False
    la = memory.get("live_action_context")
    if not isinstance(la, dict) or not la:
        return False
    open_n = _to_int(la.get("open_structural_actions")) or 0
    pre_n = _to_int(la.get("pre_broker_actions")) or 0
    exec_n = _to_int(la.get("execution_requested_actions")) or 0
    return (open_n + pre_n + exec_n) > 0


def _has_recent_setup_event(
    setup_events: List[Dict[str, Any]], as_of: _date
) -> Tuple[bool, Optional[Dict[str, Any]]]:
    if not isinstance(setup_events, list):
        return False, None
    cutoff = SETUP_RECENT_DAYS
    best: Optional[Dict[str, Any]] = None
    best_age: Optional[int] = None
    for ev in setup_events:
        if not isinstance(ev, dict):
            continue
        sdate = _parse_date(ev.get("setup_date"))
        age = _days_between(as_of, sdate)
        if age is None or age < 0:
            continue
        status = str(ev.get("setup_status") or "").upper()
        if age <= cutoff and status in {"DETECTED", "ACTIVE", "CONFIRMED"}:
            if best_age is None or age < best_age:
                best_age = age
                best = ev
    return (best is not None), best


def _has_recent_state_or_regime_change(
    structure: Dict[str, Any], regime: Dict[str, Any], as_of: _date
) -> Tuple[bool, Optional[str]]:
    if isinstance(structure, dict):
        sdate = _parse_date(structure.get("state_entered_date"))
        age = _days_between(as_of, sdate)
        if age is not None and 0 <= age <= STATE_RECENT_DAYS:
            return True, "structure.state_entered_date"
        bars = _to_int(structure.get("bars_in_state"))
        if bars is not None and bars <= STATE_RECENT_DAYS:
            return True, "structure.bars_in_state"
    if isinstance(regime, dict):
        rdate = _parse_date(regime.get("regime_entered_date"))
        age = _days_between(as_of, rdate)
        if age is not None and 0 <= age <= STATE_RECENT_DAYS:
            return True, "regime.regime_entered_date"
    return False, None


def _is_near_key_level(levels: Dict[str, Any]) -> Tuple[bool, Optional[float]]:
    if not isinstance(levels, dict):
        return False, None
    candidates: List[float] = []
    for key in ("distance_to_resistance_pct", "distance_to_support_pct"):
        v = _to_float(levels.get(key))
        if v is not None:
            candidates.append(abs(v))
    for nested_key in ("nearest_resistance", "nearest_support"):
        block = levels.get(nested_key)
        if isinstance(block, dict):
            v = _to_float(block.get("distance_pct"))
            if v is not None:
                candidates.append(abs(v))
    if not candidates:
        return False, None
    nearest = min(candidates)
    return (nearest <= NEAR_LEVEL_PCT), nearest


def _has_abnormal_candle(candle_sequence: List[Dict[str, Any]], price: Dict[str, Any]) -> bool:
    if not isinstance(candle_sequence, list) or not candle_sequence:
        return False
    last_close = _to_float(price.get("current_price")) if isinstance(price, dict) else None
    # consider the most recent candle (index 0 — view orders DESC)
    cand = candle_sequence[0]
    if not isinstance(cand, dict):
        return False
    # wick ratios are often pre-computed in the dossier
    upper = _to_float(cand.get("upper_wick_ratio"))
    lower = _to_float(cand.get("lower_wick_ratio"))
    if upper is not None and upper >= WICK_RATIO_THRESHOLD:
        return True
    if lower is not None and lower >= WICK_RATIO_THRESHOLD:
        return True
    # If full OHLC present, check body/range vs latest close
    o = _to_float(cand.get("open"))
    h = _to_float(cand.get("high"))
    l = _to_float(cand.get("low"))
    c = _to_float(cand.get("close"))
    ref = last_close or c
    if ref and ref > 0:
        if o is not None and c is not None:
            body_pct = abs(c - o) / ref * 100.0
            if body_pct >= ABNORMAL_BODY_PCT:
                return True
        if h is not None and l is not None:
            range_pct = abs(h - l) / ref * 100.0
            if range_pct >= ABNORMAL_RANGE_PCT:
                return True
    return False


def _has_recent_proposal_or_trade_memory(memory: Dict[str, Any]) -> bool:
    if not isinstance(memory, dict):
        return False
    rpm = memory.get("recent_proposal_memory")
    if isinstance(rpm, dict):
        for key in ("proposals_14d", "active_proposals", "recent_agentic_proposals_14d"):
            n = _to_int(rpm.get(key))
            if n is not None and n > 0:
                return True
    rtm = memory.get("recent_trade_memory")
    if isinstance(rtm, dict) and rtm:
        for v in rtm.values():
            n = _to_int(v)
            if n is not None and n > 0:
                return True
    return False


def _has_strong_structural_trust(history: Dict[str, Any]) -> bool:
    if not isinstance(history, dict):
        return False
    for side_key in ("long_history", "short_history"):
        side = history.get(side_key)
        if not isinstance(side, list):
            continue
        for entry in side:
            if not isinstance(entry, dict):
                continue
            n = _to_int(entry.get("n_setups")) or 0
            mhr = _to_float(entry.get("meaningful_hit_rate")) or 0.0
            trust = str(entry.get("trust_label") or "").upper()
            if trust == "TRUSTED":
                return True
            if n >= STRONG_MIN_N and mhr >= STRONG_HIT_RATE:
                return True
    return False


def _has_any_history(history: Dict[str, Any]) -> bool:
    if not isinstance(history, dict):
        return False
    for side_key in ("long_history", "short_history"):
        side = history.get(side_key)
        if isinstance(side, list) and any(isinstance(e, dict) for e in side):
            return True
    return False


# ---------------------------------------------------------------------------
# Top-level decision
# ---------------------------------------------------------------------------

def evaluate_dossier_eligibility(
    *,
    symbol: str,
    market_type: str,
    payload: Dict[str, Any],
    as_of: _date,
    operator_symbol_override: bool = False,
) -> EligibilityDecision:
    """Compute a direction-neutral eligibility decision for one dossier."""
    payload = payload if isinstance(payload, dict) else {}

    # Operator override always wins — preserves --symbols testing UX.
    if operator_symbol_override:
        return EligibilityDecision(
            symbol=symbol,
            market_type=market_type,
            eligible=True,
            primary_reason_code="ELIG_OVERRIDE_SYMBOL_FILTER",
            signal_flags={"override": True},
            evidence_summary={"operator_symbol_override": True},
            notes="Operator passed --symbols; eligibility filter bypassed.",
        )

    memory = payload.get("memory") or {}
    structure = payload.get("structure") or {}
    regime = payload.get("regime") or {}
    levels = payload.get("levels") or {}
    setup_events = payload.get("setup_events_evidence_only") or []
    candle_sequence = payload.get("candle_sequence") or []
    price = payload.get("price") or {}
    history = payload.get("history") or {}

    open_pos = _has_open_position(memory)
    active_live = _has_active_live_action(memory)
    recent_setup, recent_setup_event = _has_recent_setup_event(setup_events, as_of)
    state_change, state_change_source = _has_recent_state_or_regime_change(structure, regime, as_of)
    near_level, nearest_pct = _is_near_key_level(levels)
    abnormal_candle = _has_abnormal_candle(candle_sequence, price)
    recent_memory = _has_recent_proposal_or_trade_memory(memory)
    strong_trust = _has_strong_structural_trust(history)
    has_history = _has_any_history(history)

    flags = {
        "open_position": bool(open_pos),
        "active_live_action": bool(active_live),
        "recent_setup_event": bool(recent_setup),
        "recent_state_or_regime_change": bool(state_change),
        "near_key_level": bool(near_level),
        "abnormal_candle": bool(abnormal_candle),
        "recent_proposal_or_trade_memory": bool(recent_memory),
        "strong_structural_trust": bool(strong_trust),
        "has_any_history": bool(has_history),
    }

    summary: Dict[str, Any] = {
        "nearest_level_distance_pct": nearest_pct,
        "recent_state_change_source": state_change_source,
    }
    if recent_setup_event is not None:
        summary["recent_setup_family"] = recent_setup_event.get("setup_family")
        summary["recent_setup_status"] = recent_setup_event.get("setup_status")
        summary["recent_setup_date"] = recent_setup_event.get("setup_date")
    if isinstance(memory, dict):
        rpm = memory.get("recent_proposal_memory") or {}
        summary["recent_proposals_14d"] = _to_int(rpm.get("proposals_14d"))
        summary["recent_agentic_proposals_14d"] = _to_int(rpm.get("recent_agentic_proposals_14d"))
        la = memory.get("live_action_context") or {}
        summary["latest_action_status"] = la.get("latest_action_status")

    # ---- INCLUDE branches ----
    # Priority order: highest-importance first so the audit trail is useful.
    primary = secondary = None
    if open_pos:
        primary = "ELIG_OPEN_POSITION"
    elif active_live:
        primary = "ELIG_ACTIVE_LIVE_ACTION"
    elif recent_setup:
        primary = "ELIG_RECENT_SETUP_EVENT"
    elif state_change:
        primary = "ELIG_STATE_OR_REGIME_CHANGE"
    elif near_level:
        primary = "ELIG_NEAR_KEY_LEVEL"
    elif abnormal_candle:
        primary = "ELIG_ABNORMAL_CANDLE"
    elif recent_memory:
        primary = "ELIG_RECENT_PROPOSAL_OR_TRADE_MEMORY"
    elif strong_trust:
        primary = "ELIG_STRONG_STRUCTURAL_TRUST"

    if primary is not None:
        # populate a useful secondary if we have multiple positive signals
        for cand_flag, cand_code in [
            (recent_setup, "ELIG_RECENT_SETUP_EVENT"),
            (state_change, "ELIG_STATE_OR_REGIME_CHANGE"),
            (near_level, "ELIG_NEAR_KEY_LEVEL"),
            (abnormal_candle, "ELIG_ABNORMAL_CANDLE"),
            (recent_memory, "ELIG_RECENT_PROPOSAL_OR_TRADE_MEMORY"),
            (strong_trust, "ELIG_STRONG_STRUCTURAL_TRUST"),
        ]:
            if cand_flag and cand_code != primary:
                secondary = cand_code
                break
        return EligibilityDecision(
            symbol=symbol,
            market_type=market_type,
            eligible=True,
            primary_reason_code=primary,
            secondary_reason_code=secondary,
            signal_flags=flags,
            evidence_summary=summary,
        )

    # ---- SKIP branches ----
    # Pick the most informative skip reason. Order is important.
    if not has_history:
        skip_primary = "INSUFFICIENT_STRUCTURAL_HISTORY"
    elif (nearest_pct is None or nearest_pct > NEAR_LEVEL_PCT) and not abnormal_candle:
        skip_primary = "FAR_FROM_ACTIONABLE_LEVELS"
    elif not state_change and not recent_setup:
        skip_primary = "NO_RECENT_STATE_CHANGE"
    else:
        skip_primary = "NO_MATERIAL_NEW_EVIDENCE"

    return EligibilityDecision(
        symbol=symbol,
        market_type=market_type,
        eligible=False,
        primary_reason_code=skip_primary,
        secondary_reason_code=None,
        signal_flags=flags,
        evidence_summary=summary,
        notes="No open position, no active live action, no recent setup, no recent state/regime change, no abnormal candle, no recent memory, and no strong structural trust.",
    )
