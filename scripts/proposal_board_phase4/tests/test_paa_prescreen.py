"""Unit tests for PAA proposal-panel pre-screen ranking and selection."""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[3] / "apps" / "mip_ui_api"
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from app.price_action.models import (  # noqa: E402
    AnalyseResponse,
    DailyBar,
    ExpertRead,
    LongVerdictModel,
    MethodologistOutput,
    PlainExplanation,
)
from app.price_action.service import RuntimeConfig  # noqa: E402

from MIP.scripts.proposal_board_phase4.paa_prescreen import (  # noqa: E402
    PaaPrescreenConfig,
    PaaPrescreenConfigError,
    PaaScanResult,
    _classify_candidate,
    compare_old_vs_paa,
    opportunity_score,
    preflight_scan_budget,
    rank_candidates,
)


def _make_response(
    *,
    symbol: str = "AAPL",
    decision: str = "LONG_APPROVE",
    analysis_status: str = "OK",
    confidence: float = 0.8,
) -> AnalyseResponse:
    expert = ExpertRead(
        market_context="uptrend",
        trend_state="up",
        current_pattern="pullback",
        support_resistance_read="support nearby",
        long_location_quality="good",
        continuation_vs_failure_risk="continuation",
        what_supports_the_long=["support"],
        what_weakens_the_long=[],
        confirmation_needed="hold",
        invalidation_logic="break support",
    )
    plain = PlainExplanation(
        summary="summary",
        what_the_chart_is_doing="pullback",
        why_it_matters="matters",
        what_to_wait_for="wait",
        simple_risk_warning="risk",
    )
    methodologist = MethodologistOutput(
        symbol=symbol,
        analysis_status=analysis_status,
        expert_read=expert,
        plain_explanation=plain,
        verdict=LongVerdictModel(
            decision=decision,
            confidence=confidence,
            reason_summary="reason",
            not_a_short_recommendation=True,
        ),
        annotations=[],
    )
    geometry = {
        "structure": {"trend": "UPTREND"},
        "ema20": {"price_above": True},
        "entry_location": {
            "distance_to_nearest_support_pct": 2.0,
            "distance_to_nearest_resistance_pct": 5.0,
            "risk_of_chasing": "LOW",
        },
        "pullback": {"near_support": True},
        "support_zones": [{"touch_count": 3, "lower": 100.0, "upper": 101.0}],
        "resistance_zones": [{"touch_count": 2, "lower": 110.0, "upper": 111.0}],
    }
    situation = {
        "long_entry_quality": "GOOD",
        "market_cycle": "TREND_PULLBACK",
        "swing_structure": "HH_HL",
        "current_location": "CONSTRUCTIVE_PULLBACK",
        "pullback_quality": "ORDERLY",
        "breakout_followthrough": "NONE",
        "wedge_risk": "NONE",
    }
    bar = DailyBar(
        date=date(2026, 7, 1), open=100, high=101, low=99, close=100.5, ema20=99.5,
    )
    return AnalyseResponse(
        analysis_id="test-id",
        symbol=symbol,
        lookback_bars=120,
        as_of_date=date(2026, 7, 1),
        bar_count=1,
        bars=[bar],
        current_price=100.5,
        detected_geometry=geometry,
        situation_model=situation,
        annotations=[],
        rag_status="DISABLED",
        retrieval_count=0,
        card_count=0,
        total_chars=0,
        methodologist_knowledge=[],
        methodologist=methodologist,
        plain_language_read="plain",
        key_takeaways_simple=["one"],
        important_levels=[],
        chart_callouts=[],
        already_hold_guidance="hold",
        why_not_stronger="none",
        watch_next="watch",
        invalidation="invalid",
    )


def test_opportunity_score_is_deterministic():
    response = _make_response()
    score_a, bd_a = opportunity_score(response)
    score_b, bd_b = opportunity_score(response)
    assert score_a == score_b
    assert bd_a == bd_b
    assert 0 <= score_a <= 100


def test_classify_primary_secondary_geometry_fill():
    primary, _ = _classify_candidate(_make_response(decision="LONG_APPROVE", analysis_status="OK"))
    assert primary == "PRIMARY_APPROVE"

    secondary, _ = _classify_candidate(_make_response(decision="WAIT_PULLBACK", analysis_status="OK"))
    assert secondary == "SECONDARY_WAIT"

    geo, _ = _classify_candidate(
        _make_response(decision="LONG_APPROVE", analysis_status="GEOMETRY_ONLY")
    )
    assert geo == "GEOMETRY_ONLY_FILL"

    excluded, reason = _classify_candidate(_make_response(decision="REJECT", analysis_status="OK"))
    assert excluded is None
    assert "REJECT" in reason


def test_geometry_only_never_outranks_methodologist_peer():
    ok = _make_response(symbol="AAA", decision="LONG_APPROVE", analysis_status="OK", confidence=0.5)
    geo = _make_response(symbol="BBB", decision="LONG_APPROVE", analysis_status="GEOMETRY_ONLY", confidence=0.99)
    scans = {
        "AAA": PaaScanResult(symbol="AAA", response=ok),
        "BBB": PaaScanResult(symbol="BBB", response=geo),
    }
    result = rank_candidates(scans, top_n=1)
    assert result.selected_symbols == ["AAA"]


def test_primary_then_secondary_then_geometry_fill():
    primary = _make_response(symbol="P1", decision="LONG_APPROVE", analysis_status="OK")
    secondary = _make_response(symbol="S1", decision="WAIT_PULLBACK", analysis_status="OK")
    geo = _make_response(symbol="G1", decision="LONG_APPROVE", analysis_status="GEOMETRY_ONLY")
    scans = {
        "P1": PaaScanResult(symbol="P1", response=primary),
        "S1": PaaScanResult(symbol="S1", response=secondary),
        "G1": PaaScanResult(symbol="G1", response=geo),
    }
    result = rank_candidates(scans, top_n=2)
    assert result.selected_symbols == ["P1", "S1"]
    assert result.primary_count == 1
    assert result.secondary_count == 1
    assert result.geometry_fill_count == 0

    result2 = rank_candidates(scans, top_n=3)
    assert result2.selected_symbols == ["P1", "S1", "G1"]
    assert result2.geometry_fill_count == 1


def test_excluded_verdicts_never_selected():
    reject = _make_response(symbol="R1", decision="REJECT", analysis_status="OK")
    defer = _make_response(symbol="D1", decision="DEFER", analysis_status="OK")
    scans = {
        "R1": PaaScanResult(symbol="R1", response=reject),
        "D1": PaaScanResult(symbol="D1", response=defer),
    }
    result = rank_candidates(scans, top_n=30)
    assert result.selected_symbols == []


def test_compare_old_vs_paa_displacement():
    newly, displaced = compare_old_vs_paa(["AAPL", "MSFT"], ["MSFT", "NVDA"])
    assert newly == ["NVDA"]
    assert displaced == ["AAPL"]


def test_preflight_scan_budget_aborts_on_symbol_cap():
    config = PaaPrescreenConfig(max_symbols=5)
    analyser = RuntimeConfig(enabled=True, audit_enabled=True)
    with pytest.raises(PaaPrescreenConfigError, match="symbol cap"):
        preflight_scan_budget(symbol_count=10, config=config, analyser_config=analyser)


def test_preflight_scan_budget_aborts_on_usd_cap():
    config = PaaPrescreenConfig(max_usd_estimate=0.001, max_llm_calls=100)
    analyser = RuntimeConfig(enabled=True, audit_enabled=True, llm_enabled=True)
    with pytest.raises(PaaPrescreenConfigError, match="USD"):
        preflight_scan_budget(symbol_count=10, config=config, analyser_config=analyser)
