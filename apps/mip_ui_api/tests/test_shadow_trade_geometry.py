"""Unit tests for shadow trade geometry normalization."""

from app.committee.shadow_trade_geometry import (
    format_executable_entry_zone_text,
    normalize_shadow_trade_artifact,
    resolve_execution_reclaim_level,
)
from app.committee.shadow_types import ShadowTradeArtifact


def test_resolve_reclaim_prefers_invalidation_over_stale_dossier():
    level, source, legacy = resolve_execution_reclaim_level(
        "LONG",
        zone_low=254.42,
        zone_high=260.2,
        inv_level=251.71,
        dossier_payload={"levels": {"broken_resistance_as_support": 245.49}},
        last_price=262.0,
    )
    assert level == 251.71
    assert source == "invalidation"
    assert legacy == 245.49


def test_format_entry_zone_chase_message():
    text = format_executable_entry_zone_text(
        side="LONG",
        zone_low=254.42,
        zone_high=260.2,
        inv_level=251.71,
        latest_price=262.33,
    )
    assert "254.42" in text
    assert "260.20" in text
    assert "262.33" in text
    assert "Do not chase" in text


def test_normalize_shadow_trade_rewrites_llm_stale_text():
    trade = ShadowTradeArtifact(
        entry_zone="Upon reclaim of 245.49 target 246-250",
        key_condition="Two bars above 245.49",
        advisory_only=True,
    )
    out = normalize_shadow_trade_artifact(
        trade,
        side="LONG",
        zone_low=254.42,
        zone_high=260.2,
        inv_level=251.71,
        latest_price=262.0,
        reclaim_level=251.71,
        reclaim_status="HELD",
    )
    assert out is not None
    assert "245.49" not in (out.entry_zone or "")
    assert "254.42" in (out.entry_zone or "")
    assert "Do not chase" in (out.entry_zone or "")
