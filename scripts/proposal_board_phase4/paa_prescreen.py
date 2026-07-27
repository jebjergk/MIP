"""
Price Action Analyser proposal-panel pre-screen (Stage 0.6 replacement).

Deterministic ranking and top-N selection from bounded PAA scans.
No changes to specialist/chair/LPA/execution paths.
"""
from __future__ import annotations

import json
import logging
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_API_ROOT = _PROJECT_ROOT / "apps" / "mip_ui_api"
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from app.db import fetch_all, get_connection  # noqa: E402
from app.price_action.models import AnalyseRequest  # noqa: E402
from app.price_action.service import (  # noqa: E402
    MAX_LLM_CALLS,
    MAX_RAG_RETRIES,
    AnalyseResponse,
    RuntimeConfig,
    analyse,
    read_runtime_config,
)

DEFAULT_TOP_N = 30
DEFAULT_MAX_SYMBOLS = 80
DEFAULT_MAX_LLM_CALLS = 80
DEFAULT_MAX_USD_ESTIMATE = 3.00
DEFAULT_LOOKBACK_BARS = 120
DEFAULT_AI_CREDITS_PER_CALL = 0.0157476375
DEFAULT_AI_CREDIT_USD = 2.00

_PRIMARY_VERDICTS = frozenset({"LONG_APPROVE", "LONG_APPROVE_REDUCED"})
_SECONDARY_VERDICTS = frozenset({"WAIT_PULLBACK", "WAIT_RECLAIM"})
_GEOMETRY_FILL_VERDICTS = _PRIMARY_VERDICTS | _SECONDARY_VERDICTS
_EXCLUDED_VERDICTS = frozenset({"DEFER", "NO_CLEAR_LONG", "REJECT"})
_METHODOLOGIST_OK = frozenset({"OK"})
_GEOMETRY_STATUSES = frozenset({"GEOMETRY_ONLY", "METHODOLOGIST_UNAVAILABLE"})

_CONFIG_KEYS = (
    "PROPOSAL_BOARD_PAA_PRESCREEN_ENABLED",
    "PROPOSAL_BOARD_PAA_TOP_N",
    "PROPOSAL_BOARD_PAA_MAX_SYMBOLS",
    "PROPOSAL_BOARD_PAA_MAX_LLM_CALLS",
    "PROPOSAL_BOARD_PAA_MAX_USD_ESTIMATE",
    "PROPOSAL_BOARD_PAA_LOOKBACK_BARS",
)

AUDIT_VERIFY_SQL = """
SELECT COUNT(*) AS N
FROM MIP.KNOWLEDGE.PRICE_ACTION_ANALYSER_AUDIT
WHERE ANALYSIS_ID = %s
  AND SYMBOL = %s
  AND SIDE = 'LONG'
  AND LOOKBACK_BARS = %s
"""


class AuditVerificationError(RuntimeError):
    pass


class PaaPrescreenConfigError(RuntimeError):
    pass


@dataclass(frozen=True)
class PaaPrescreenConfig:
    enabled: bool = False
    top_n: int = DEFAULT_TOP_N
    max_symbols: int = DEFAULT_MAX_SYMBOLS
    max_llm_calls: int = DEFAULT_MAX_LLM_CALLS
    max_usd_estimate: float = DEFAULT_MAX_USD_ESTIMATE
    lookback_bars: int = DEFAULT_LOOKBACK_BARS


@dataclass
class PaaScanResult:
    symbol: str
    response: AnalyseResponse | None = None
    error: str | None = None
    elapsed_seconds: float = 0.0
    audit_verified: bool = False


@dataclass
class PaaRankedCandidate:
    symbol: str
    paa_rank: int
    verdict: str
    confidence: float
    analysis_status: str
    analysis_id: str | None
    opportunity_score: float
    select_reason: str
    selected_for_panel: bool
    score_breakdown: Dict[str, Any] = field(default_factory=dict)
    methodologist_ok: bool = False
    geometry_fill: bool = False


@dataclass
class PaaSelectionResult:
    selected_symbols: List[str]
    ranked: List[PaaRankedCandidate]
    primary_count: int
    secondary_count: int
    geometry_fill_count: int
    scan_errors: int
    estimated_llm_calls: int
    estimated_usd: float


@dataclass
class PaaPrescreenSummary:
    enabled: bool
    top_n: int
    scanned: int
    selected: int
    primary_count: int
    secondary_count: int
    geometry_fill_count: int
    scan_errors: int
    estimated_llm_calls: int
    estimated_usd: float
    newly_included: List[str] = field(default_factory=list)
    displaced: List[str] = field(default_factory=list)
    downstream_blocked: List[Dict[str, Any]] = field(default_factory=list)


def _as_bool(value: Any, default: bool = False) -> bool:
    if value is None or str(value).strip() == "":
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _as_int(value: Any, default: int) -> int:
    if value is None or str(value).strip() == "":
        return default
    try:
        return int(str(value).strip())
    except ValueError:
        return default


def _as_float(value: Any, default: float) -> float:
    if value is None or str(value).strip() == "":
        return default
    try:
        return float(str(value).strip())
    except ValueError:
        return default


def read_paa_prescreen_config(cur) -> PaaPrescreenConfig:
    placeholders = ",".join(["'" + k.replace("'", "''") + "'" for k in _CONFIG_KEYS])
    cur.execute(
        f"SELECT CONFIG_KEY, CONFIG_VALUE FROM MIP.APP.APP_CONFIG "
        f"WHERE CONFIG_KEY IN ({placeholders})"
    )
    values = {str(r[0]): r[1] for r in (cur.fetchall() or [])}
    lookback = _as_int(values.get("PROPOSAL_BOARD_PAA_LOOKBACK_BARS"), DEFAULT_LOOKBACK_BARS)
    if lookback not in {90, 120, 180, 250}:
        lookback = DEFAULT_LOOKBACK_BARS
    return PaaPrescreenConfig(
        enabled=_as_bool(values.get("PROPOSAL_BOARD_PAA_PRESCREEN_ENABLED"), default=False),
        top_n=max(1, _as_int(values.get("PROPOSAL_BOARD_PAA_TOP_N"), DEFAULT_TOP_N)),
        max_symbols=max(1, _as_int(values.get("PROPOSAL_BOARD_PAA_MAX_SYMBOLS"), DEFAULT_MAX_SYMBOLS)),
        max_llm_calls=max(1, _as_int(values.get("PROPOSAL_BOARD_PAA_MAX_LLM_CALLS"), DEFAULT_MAX_LLM_CALLS)),
        max_usd_estimate=max(0.0, _as_float(
            values.get("PROPOSAL_BOARD_PAA_MAX_USD_ESTIMATE"), DEFAULT_MAX_USD_ESTIMATE
        )),
        lookback_bars=lookback,
    )


def validate_analyser_runtime(config: RuntimeConfig) -> None:
    problems: List[str] = []
    if not config.enabled:
        problems.append("PRICE_ACTION_ANALYSER_ENABLED must be true")
    if not config.audit_enabled:
        problems.append("PRICE_ACTION_ANALYSER_AUDIT_ENABLED must be true")
    if config.rag_max_retries != 0 or MAX_RAG_RETRIES != 0:
        problems.append("RAG retries must remain zero")
    if MAX_LLM_CALLS != 1:
        problems.append("Price Action Analyser must remain one LLM call per symbol")
    if config.rag_enabled and config.rag_max_calls > 3:
        problems.append("RAG calls exceed hard maximum of three per symbol")
    if problems:
        raise PaaPrescreenConfigError("Unsafe analyser runtime: " + "; ".join(problems))


def verify_analysis_audit(analysis_id: str, symbol: str, lookback_bars: int) -> bool:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(AUDIT_VERIFY_SQL, (analysis_id, symbol, lookback_bars))
        row = fetch_all(cur)
        return bool(row and int(row[0]["N"]) == 1)
    finally:
        conn.close()


def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def _lookup(mapping: Dict[str, float], key: str, default: float = 0.0) -> float:
    return mapping.get(str(key or "").upper(), default)


def opportunity_score(response: AnalyseResponse) -> Tuple[float, Dict[str, Any]]:
    geometry = response.detected_geometry or {}
    situation = response.situation_model or {}
    verdict = response.methodologist.verdict

    loc_q = str(situation.get("long_entry_quality") or "UNCLEAR").upper()
    cycle = str(situation.get("market_cycle") or "UNCLEAR").upper()
    trend = str((geometry.get("structure") or {}).get("trend") or "UNCLEAR").upper()
    swing = str(situation.get("swing_structure") or "MIXED").upper()
    ema = geometry.get("ema20") or {}
    ema_ok = bool(ema.get("price_above"))
    entry = geometry.get("entry_location") or {}
    pullback = geometry.get("pullback") or {}
    support_zones = geometry.get("support_zones") or []
    resistance_zones = geometry.get("resistance_zones") or []

    d_sup = entry.get("distance_to_nearest_support_pct")
    d_res = entry.get("distance_to_nearest_resistance_pct")
    chase = str(entry.get("risk_of_chasing") or "MODERATE").upper()
    pb_q = str(situation.get("pullback_quality") or "SHARP").upper()
    bt = str(situation.get("breakout_followthrough") or "NONE").upper()
    wedge = str(situation.get("wedge_risk") or "NONE").upper()
    loc = str(situation.get("current_location") or "NEUTRAL").upper()

    long_quality = _lookup(
        {"GOOD": 1.0, "ACCEPTABLE": 0.7, "UNCLEAR": 0.4, "CHASING": 0.25, "POOR": 0.1},
        loc_q,
        0.4,
    )
    long_quality += 0.15 if trend == "UPTREND" else 0.0
    long_quality += 0.10 if swing == "HH_HL" else 0.0
    long_quality += 0.10 if ema_ok else 0.0
    long_quality += _lookup(
        {
            "TREND_PULLBACK": 0.15,
            "BREAKOUT": 0.10,
            "UNCLEAR": 0.0,
            "TRADING_RANGE": -0.10,
            "FAILED_BREAKOUT": -0.25,
        },
        cycle,
        0.0,
    )
    long_quality = _clamp(long_quality)

    if support_zones:
        sup_t = float((support_zones[0] or {}).get("touch_count") or 0)
        sup_pct = abs(float(d_sup)) if d_sup is not None else 8.0
        support_quality = _clamp(
            0.35 * min(sup_t, 4.0) / 4.0
            + 0.40 * (1.0 - min(sup_pct, 8.0) / 8.0)
            + 0.25 * (1.0 if pullback.get("near_support") else 0.0)
        )
    else:
        support_quality = 0.0

    if d_sup is None or not support_zones:
        invalidation_room = 0.2
    else:
        invalidation_room = _clamp(min(abs(float(d_sup)), 6.0) / 6.0)
        invalidation_room *= _lookup({"LOW": 1.0, "MODERATE": 0.75, "HIGH": 0.4}, chase, 0.75)
        invalidation_room *= _lookup(
            {"ORDERLY": 1.0, "SHARP": 0.7, "DAMAGING": 0.35}, pb_q, 0.7
        )
        invalidation_room = _clamp(invalidation_room)

    if not resistance_zones:
        resistance_room = 1.0
    else:
        res_t = float((resistance_zones[0] or {}).get("touch_count") or 0)
        res_pct = abs(float(d_res)) if d_res is not None else 0.0
        resistance_room = _clamp(
            0.7 * min(res_pct, 12.0) / 12.0 + 0.3 * (1.0 - min(res_t, 4.0) / 4.0)
        )

    confirmation_maturity = _lookup({"HELD": 0.85, "NONE": 0.55, "FAILED": 0.15}, bt, 0.55)
    if swing == "HH_HL" and ema_ok:
        confirmation_maturity += 0.15
    confirmation_maturity += _lookup(
        {"ORDERLY": 0.10, "SHARP": 0.05, "DAMAGING": -0.10}, pb_q, 0.05
    )
    if wedge == "POSSIBLE":
        confirmation_maturity -= 0.20
    confirmation_maturity = _clamp(confirmation_maturity)

    warning_severity = 0.0
    if bt == "FAILED" or cycle == "FAILED_BREAKOUT":
        warning_severity += 0.30
    if wedge == "POSSIBLE":
        warning_severity += 0.20
    if loc in {"UNFAVOURABLE", "EXTENDED_NEAR_HIGHS"}:
        warning_severity += 0.20
    if chase == "HIGH":
        warning_severity += 0.15
    if pb_q == "DAMAGING":
        warning_severity += 0.15
    if trend == "DOWNTREND" or swing == "LH_LL":
        warning_severity += 0.15
    if cycle == "TRADING_RANGE":
        warning_severity += 0.10
    warning_score = _clamp(1.0 - _clamp(warning_severity))

    raw = 100.0 * (
        0.28 * long_quality
        + 0.18 * support_quality
        + 0.16 * invalidation_room
        + 0.14 * resistance_room
        + 0.14 * confirmation_maturity
        + 0.10 * warning_score
    )
    gate = _lookup(
        {
            "LONG_APPROVE": 1.00,
            "LONG_APPROVE_REDUCED": 0.90,
            "WAIT_PULLBACK": 0.70,
            "WAIT_RECLAIM": 0.65,
            "DEFER": 0.40,
            "NO_CLEAR_LONG": 0.25,
            "REJECT": 0.10,
        },
        verdict.decision,
        0.25,
    )
    confidence = float(verdict.confidence or 0.0)
    score = round(raw * gate * (0.85 + 0.15 * confidence), 2)
    breakdown = {
        "long_quality": round(long_quality, 4),
        "support_quality": round(support_quality, 4),
        "invalidation_room": round(invalidation_room, 4),
        "resistance_room": round(resistance_room, 4),
        "confirmation_maturity": round(confirmation_maturity, 4),
        "warning_score": round(warning_score, 4),
        "verdict_gate": gate,
        "raw_score": round(raw, 2),
    }
    return score, breakdown


def _verdict_class_tier(
    verdict: str,
    analysis_status: str,
    *,
    methodologist_ok: bool,
) -> int:
    decision = str(verdict or "").upper()
    if methodologist_ok:
        if decision == "LONG_APPROVE":
            return 400
        if decision == "LONG_APPROVE_REDUCED":
            return 390
        if decision == "WAIT_PULLBACK":
            return 370
        if decision == "WAIT_RECLAIM":
            return 365
    else:
        if decision == "LONG_APPROVE":
            return 300
        if decision == "LONG_APPROVE_REDUCED":
            return 290
        if decision == "WAIT_PULLBACK":
            return 270
        if decision == "WAIT_RECLAIM":
            return 265
    return 0


def _classify_candidate(response: AnalyseResponse) -> Tuple[str | None, str | None]:
    verdict = str(response.methodologist.verdict.decision or "").upper()
    status = str(response.methodologist.analysis_status or "").upper()

    if verdict in _EXCLUDED_VERDICTS or status == "ERROR":
        return None, f"NOT_SELECTED_{verdict or status}"

    methodologist_ok = status in _METHODOLOGIST_OK
    geometry_fill = status in _GEOMETRY_STATUSES

    if methodologist_ok:
        if verdict in _PRIMARY_VERDICTS:
            return "PRIMARY_APPROVE", None
        if verdict in _SECONDARY_VERDICTS:
            return "SECONDARY_WAIT", None
        return None, f"NOT_SELECTED_{verdict}"

    if geometry_fill and verdict in _GEOMETRY_FILL_VERDICTS:
        return "GEOMETRY_ONLY_FILL", None

    return None, f"NOT_SELECTED_{status or verdict}"


def _sort_key(candidate: PaaRankedCandidate) -> Tuple[Any, ...]:
    bd = candidate.score_breakdown
    return (
        -bd.get("verdict_tier", 0),
        -candidate.confidence,
        -candidate.opportunity_score,
        -bd.get("support_quality", 0.0),
        -bd.get("invalidation_room", 0.0),
        bd.get("resistance_room", 0.0),
        bd.get("confirmation_maturity", 0.0),
        -bd.get("warning_score", 0.0),
        candidate.symbol,
    )


def rank_candidates(
    scans: Dict[str, PaaScanResult],
    *,
    top_n: int,
    llm_enabled: bool = False,
) -> PaaSelectionResult:
    eligible: List[PaaRankedCandidate] = []
    scan_errors = 0

    for symbol, scan in scans.items():
        if scan.error or scan.response is None:
            scan_errors += 1
            continue
        response = scan.response
        select_reason, _reject_reason = _classify_candidate(response)
        if select_reason is None:
            continue
        verdict = str(response.methodologist.verdict.decision or "").upper()
        status = str(response.methodologist.analysis_status or "").upper()
        methodologist_ok = status in _METHODOLOGIST_OK
        opp_score, breakdown = opportunity_score(response)
        breakdown["verdict_tier"] = _verdict_class_tier(
            verdict, status, methodologist_ok=methodologist_ok
        )
        eligible.append(
            PaaRankedCandidate(
                symbol=symbol,
                paa_rank=0,
                verdict=verdict,
                confidence=float(response.methodologist.verdict.confidence or 0.0),
                analysis_status=status,
                analysis_id=response.analysis_id,
                opportunity_score=opp_score,
                select_reason=select_reason,
                selected_for_panel=False,
                score_breakdown=breakdown,
                methodologist_ok=methodologist_ok,
                geometry_fill=select_reason == "GEOMETRY_ONLY_FILL",
            )
        )

    eligible.sort(key=_sort_key)

    primary_pool = [c for c in eligible if c.select_reason == "PRIMARY_APPROVE"]
    secondary_pool = [c for c in eligible if c.select_reason == "SECONDARY_WAIT"]
    geometry_pool = [c for c in eligible if c.select_reason == "GEOMETRY_ONLY_FILL"]

    selected: List[PaaRankedCandidate] = []
    for pool in (primary_pool, secondary_pool, geometry_pool):
        for candidate in pool:
            if len(selected) >= top_n:
                break
            selected.append(candidate)
        if len(selected) >= top_n:
            break

    selected_symbols = {c.symbol for c in selected}
    ranked: List[PaaRankedCandidate] = []
    rank_idx = 0
    for candidate in eligible:
        rank_idx += 1
        candidate.paa_rank = rank_idx
        candidate.selected_for_panel = candidate.symbol in selected_symbols
        ranked.append(candidate)

    primary_count = sum(1 for c in selected if c.select_reason == "PRIMARY_APPROVE")
    secondary_count = sum(1 for c in selected if c.select_reason == "SECONDARY_WAIT")
    geometry_fill_count = sum(1 for c in selected if c.select_reason == "GEOMETRY_ONLY_FILL")

    llm_calls = (
        sum(1 for scan in scans.values() if scan.response is not None and not scan.error)
        if llm_enabled
        else 0
    )
    estimated_usd = llm_calls * DEFAULT_AI_CREDITS_PER_CALL * DEFAULT_AI_CREDIT_USD

    return PaaSelectionResult(
        selected_symbols=[c.symbol for c in selected],
        ranked=ranked,
        primary_count=primary_count,
        secondary_count=secondary_count,
        geometry_fill_count=geometry_fill_count,
        scan_errors=scan_errors,
        estimated_llm_calls=llm_calls,
        estimated_usd=round(estimated_usd, 4),
    )


def estimate_scan_cost(symbol_count: int, *, llm_enabled: bool) -> Tuple[int, float]:
    llm_calls = symbol_count if llm_enabled else 0
    usd = llm_calls * DEFAULT_AI_CREDITS_PER_CALL * DEFAULT_AI_CREDIT_USD
    return llm_calls, round(usd, 4)


def preflight_scan_budget(
    *,
    symbol_count: int,
    config: PaaPrescreenConfig,
    analyser_config: RuntimeConfig,
    allow_override: bool = False,
) -> None:
    if symbol_count > config.max_symbols:
        raise PaaPrescreenConfigError(
            f"PAA symbol cap exceeded: {symbol_count} > {config.max_symbols}"
        )
    projected_calls, projected_usd = estimate_scan_cost(
        symbol_count, llm_enabled=analyser_config.llm_enabled
    )
    if projected_calls > config.max_llm_calls and not allow_override:
        raise PaaPrescreenConfigError(
            f"PAA LLM call budget exceeded: projected={projected_calls} "
            f"cap={config.max_llm_calls}"
        )
    if projected_usd > config.max_usd_estimate and not allow_override:
        raise PaaPrescreenConfigError(
            f"PAA USD estimate exceeded: projected=${projected_usd:.4f} "
            f"cap=${config.max_usd_estimate:.2f}"
        )


def scan_universe(
    symbols: Sequence[str],
    *,
    lookback_bars: int,
    config: RuntimeConfig,
    analyse_fn: Callable[..., AnalyseResponse] = analyse,
    audit_verify_fn: Callable[[str, str, int], bool] = verify_analysis_audit,
) -> Dict[str, PaaScanResult]:
    normalized = [s.strip().upper() for s in symbols if s and s.strip()]
    if len(normalized) != len(set(normalized)):
        raise ValueError("Duplicate symbols violate one-call-per-symbol boundary.")

    if lookback_bars not in {90, 120, 180, 250}:
        raise PaaPrescreenConfigError(f"Unsupported lookback_bars={lookback_bars}")

    results: Dict[str, PaaScanResult] = {}
    for index, symbol in enumerate(normalized, 1):
        logger.info("paa_prescreen progress=%d/%d symbol=%s", index, len(normalized), symbol)
        started = time.monotonic()
        try:
            response = analyse_fn(
                AnalyseRequest(symbol=symbol, lookback_bars=lookback_bars, side="LONG"),
                config=config,
            )
            elapsed = time.monotonic() - started
            audit_ok = audit_verify_fn(response.analysis_id, symbol, lookback_bars)
            if not audit_ok:
                raise AuditVerificationError(
                    f"Analysis {response.analysis_id} for {symbol} missing from audit table"
                )
            results[symbol] = PaaScanResult(
                symbol=symbol,
                response=response,
                elapsed_seconds=elapsed,
                audit_verified=True,
            )
        except AuditVerificationError:
            raise
        except Exception as exc:  # noqa: BLE001 — per-symbol error excludes and continues
            results[symbol] = PaaScanResult(
                symbol=symbol,
                error=f"{type(exc).__name__}: {str(exc)[:500]}",
                elapsed_seconds=time.monotonic() - started,
            )
            logger.error(
                "paa_prescreen symbol_failed symbol=%s error_class=%s",
                symbol, type(exc).__name__,
            )
    return results


def build_paa_audit_fields(candidate: PaaRankedCandidate) -> Dict[str, Any]:
    return {
        "paa_rank": candidate.paa_rank,
        "paa_verdict": candidate.verdict,
        "paa_confidence": candidate.confidence,
        "paa_opportunity_score": candidate.opportunity_score,
        "paa_analysis_id": candidate.analysis_id,
        "paa_analysis_status": candidate.analysis_status,
        "paa_selected_for_panel": candidate.selected_for_panel,
        "paa_select_reason": candidate.select_reason,
        "paa_methodologist_ok": candidate.methodologist_ok,
        "paa_geometry_fill": candidate.geometry_fill,
        "paa_score_breakdown": candidate.score_breakdown,
    }


def compare_old_vs_paa(
    old_panel_symbols: Sequence[str],
    paa_selected_symbols: Sequence[str],
) -> Tuple[List[str], List[str]]:
    old_set = {s.upper() for s in old_panel_symbols}
    paa_set = {s.upper() for s in paa_selected_symbols}
    newly_included = sorted(paa_set - old_set)
    displaced = sorted(old_set - paa_set)
    return newly_included, displaced


def collect_downstream_blocks(
    cur,
    run_id: str,
    selected_symbols: Sequence[str],
) -> List[Dict[str, Any]]:
    if not selected_symbols:
        return []
    in_list = ",".join("'" + s.replace("'", "''").upper() + "'" for s in selected_symbols)
    cur.execute(
        f"""
        SELECT
            UPPER(TRIM(s.SYMBOL)) AS SYMBOL,
            COALESCE(p.EXECUTION_POLICY_STATUS, 'UNKNOWN') AS EXECUTION_POLICY_STATUS,
            COALESCE(p.EXECUTION_POLICY_REASON, '') AS EXECUTION_POLICY_REASON,
            COALESCE(p.IS_RESEARCH_ONLY, FALSE) AS IS_RESEARCH_ONLY,
            COALESCE(fs.FINAL_ACTION, '') AS FINAL_ACTION,
            COALESCE(fs.PUBLICATION_STATUS, '') AS PUBLICATION_STATUS,
            COALESCE(e.EVIDENCE_SUMMARY_JSON:paa_rank::STRING, '') AS PAA_RANK
        FROM MIP.APP.PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT s
        LEFT JOIN MIP.APP.PROPOSAL_BOARD_FINAL_SLATE_V2 fs
          ON fs.RUN_ID = s.RUN_ID AND fs.DOSSIER_ID = s.DOSSIER_ID
        LEFT JOIN MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
          ON p.BOARD_RUN_ID = s.RUN_ID AND p.BOARD_DOSSIER_ID = s.DOSSIER_ID
        LEFT JOIN MIP.APP.PROPOSAL_BOARD_REVIEW_ELIGIBILITY e
          ON e.RUN_ID = s.RUN_ID AND UPPER(TRIM(e.SYMBOL)) = UPPER(TRIM(s.SYMBOL))
        WHERE s.RUN_ID = %(run_id)s
          AND UPPER(TRIM(s.SYMBOL)) IN ({in_list})
          AND (
              COALESCE(p.IS_RESEARCH_ONLY, FALSE) = TRUE
              OR COALESCE(p.EXECUTION_POLICY_STATUS, 'EXECUTABLE') <> 'EXECUTABLE'
              OR COALESCE(fs.PUBLICATION_STATUS, '') IN ('SKIPPED', 'BLOCKED')
              OR COALESCE(e.PRIMARY_REASON_CODE, '') LIKE '%TRUST%'
              OR COALESCE(e.PRIMARY_REASON_CODE, '') LIKE '%RESEARCH%'
              OR COALESCE(e.PRIMARY_REASON_CODE, '') LIKE '%STRUCTURAL%'
          )
        ORDER BY TRY_TO_NUMBER(PAA_RANK), SYMBOL
        """,
        {"run_id": run_id},
    )
    rows = cur.fetchall() or []
    blocked: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        symbol = str(row[0]).upper()
        if symbol in seen:
            continue
        seen.add(symbol)
        blocked.append({
            "symbol": symbol,
            "execution_policy_status": str(row[1] or ""),
            "execution_policy_reason": str(row[2] or ""),
            "is_research_only": bool(row[3]),
            "final_action": str(row[4] or ""),
            "publication_status": str(row[5] or ""),
            "paa_rank": str(row[6] or ""),
        })
    return blocked


def write_prescreen_artifact(
    *,
    run_id: str,
    summary: PaaPrescreenSummary,
    ranked: Sequence[PaaRankedCandidate],
) -> Path | None:
    try:
        artifact_root = _PROJECT_ROOT.parent / "artifacts" / "paa_prescreen"
        artifact_root.mkdir(parents=True, exist_ok=True)
        path = artifact_root / f"{run_id}.json"
        payload = {
            "run_id": run_id,
            "summary": {
                "enabled": summary.enabled,
                "top_n": summary.top_n,
                "scanned": summary.scanned,
                "selected": summary.selected,
                "primary_count": summary.primary_count,
                "secondary_count": summary.secondary_count,
                "geometry_fill_count": summary.geometry_fill_count,
                "scan_errors": summary.scan_errors,
                "estimated_llm_calls": summary.estimated_llm_calls,
                "estimated_usd": summary.estimated_usd,
                "newly_included": summary.newly_included,
                "displaced": summary.displaced,
                "downstream_blocked": summary.downstream_blocked,
            },
            "ranked": [
                {
                    "symbol": c.symbol,
                    "paa_rank": c.paa_rank,
                    "selected_for_panel": c.selected_for_panel,
                    "verdict": c.verdict,
                    "confidence": c.confidence,
                    "opportunity_score": c.opportunity_score,
                    "select_reason": c.select_reason,
                }
                for c in ranked
            ],
        }
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return path
    except Exception as exc:  # noqa: BLE001
        logger.warning("paa_prescreen artifact write failed run=%s: %s", run_id, exc)
        return None
