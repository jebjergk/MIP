"""Unit tests for Phase 4 chair geometry validation (no Snowflake required)."""
from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[4]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from MIP.scripts.proposal_board_phase4.orchestrator import (  # noqa: E402
    _sanitize_evidence_used,
    _validate_chair_trade_geometry,
)


def test_sanitize_evidence_used_from_dict():
    out = _sanitize_evidence_used({"history": [], "price": {}})
    assert out == ["history", "price"]


def test_propose_long_requires_full_geometry():
    dossier = {"price": {"current_price": 100.0}}
    cfg = {"thesis_label": "AGENTIC_LONG", "entry_zone_low": 99.0}
    reason = _validate_chair_trade_geometry("PROPOSE_LONG", "LONG", cfg, dossier)
    assert reason and reason.startswith("PROPOSED_TRADE_CONFIG_MISSING:")


def test_propose_long_rejects_stale_zone():
    dossier = {"price": {"current_price": 16.81}}
    cfg = {
        "thesis_label": "AGENTIC_LONG",
        "entry_zone_low": 15.63,
        "entry_zone_high": 15.72,
        "invalidation_level": 15.40,
        "invalidation_rule": "LOSS_OF_SUPPORT",
        "exit_profile": "TRAIL_STANDARD",
        "size_treatment": "HALF",
        "risk_class": "MEDIUM",
        "time_horizon": "SWING",
    }
    reason = _validate_chair_trade_geometry("PROPOSE_LONG", "LONG", cfg, dossier)
    assert reason and reason.startswith("ENTRY_ZONE_STALE_VS_DAILY_CLOSE:")


def test_propose_long_accepts_zone_near_close():
    dossier = {"price": {"current_price": 214.69}}
    cfg = {
        "thesis_label": "AGENTIC_LONG",
        "entry_zone_low": 213.5,
        "entry_zone_high": 215.5,
        "invalidation_level": 211.0,
        "invalidation_rule": "LOSS_OF_SUPPORT",
        "exit_profile": "TRAIL_STANDARD",
        "size_treatment": "HALF",
        "risk_class": "MEDIUM",
        "time_horizon": "SWING",
    }
    assert _validate_chair_trade_geometry("PROPOSE_LONG", "LONG", cfg, dossier) is None
