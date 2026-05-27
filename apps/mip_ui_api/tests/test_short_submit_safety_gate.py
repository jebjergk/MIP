"""Stage 1 — short submit safety gate tests."""
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.routers.live import (
    _is_entry_bracket_hard_block_code,
    _live_ib_entry_risk_reason_codes,
    _strip_recomputable_entry_bracket_codes,
    _structural_ref_price_for_bracket,
)
from app.services.live_intelligence.structural_committee import (
    _MIN_RR_FOR_LIVE_ENTRY,
    _structural_reference_price,
    _target_return_meeting_min_rr,
    build_structural_entry_joint_decision,
)


def _fee_params():
    return {
        "slippage_bps": 2.0,
        "fee_bps": 1.0,
        "spread_bps": 0.0,
        "min_net_tp_bps": 5.0,
        "min_rr": 1.10,
    }


class TestLiveIbEntryRiskReasonCodesSell(unittest.TestCase):
    def test_sell_passes_when_rr_meets_floor(self):
        codes = _live_ib_entry_risk_reason_codes(
            side="SELL",
            is_exit=False,
            entry_price=30.0,
            target_return=0.04,
            stop_loss_pct=0.033,
            fee_params=_fee_params(),
        )
        self.assertNotIn("LIVE_RISK_REWARD_TOO_LOW", codes)
        self.assertEqual(codes, [])

    def test_sell_fails_when_rr_below_floor(self):
        codes = _live_ib_entry_risk_reason_codes(
            side="SELL",
            is_exit=False,
            entry_price=30.0,
            target_return=0.03,
            stop_loss_pct=0.033,
            fee_params=_fee_params(),
        )
        self.assertIn("LIVE_RISK_REWARD_TOO_LOW", codes)


class TestJdLikeRoundingEdge(unittest.TestCase):
    """JD SHORT: ref=29.99, invalidation=30.99 — must not round to RR 1.09999."""

    def test_target_return_meeting_min_rr_after_rounding(self):
        ref = 29.99
        inv = 30.99
        sl = round(abs(inv - ref) / ref, 6)
        self.assertAlmostEqual(sl, 0.033344, places=5)
        tp_floor = sl * _MIN_RR_FOR_LIVE_ENTRY
        tr = _target_return_meeting_min_rr(sl, max(tp_floor, 0.02))
        self.assertGreaterEqual(tr / sl, _MIN_RR_FOR_LIVE_ENTRY - 1e-12)

    def test_build_structural_joint_decision_jd_geometry(self):
        action = {
            "DIRECTION": "SHORT",
            "INVALIDATION_LEVEL": 30.99,
            "CURRENT_PRICE": 29.99,
            "REVALIDATION_PRICE": 29.88,
            "PROPOSED_PRICE": 30.06,
            "TARGET_EXPECTATION_SNAPSHOT": {
                "bands": {"base": 0.02, "strong": 0.026},
            },
        }
        jd = build_structural_entry_joint_decision(action)
        sl = float(jd["stop_loss_pct"])
        tr = float(jd["realistic_target_return"])
        self.assertGreater(sl, 0)
        self.assertGreater(tr, 0)
        self.assertGreaterEqual(tr / sl, _MIN_RR_FOR_LIVE_ENTRY - 1e-12)

    def test_revalidation_price_wins_over_current_for_reference(self):
        action = {
            "REVALIDATION_PRICE": 29.88,
            "CURRENT_PRICE": 29.99,
            "PROPOSED_PRICE": 30.06,
        }
        self.assertEqual(_structural_reference_price(action), 29.88)
        self.assertEqual(_structural_ref_price_for_bracket(action), 29.88)


class TestBracketHardBlockHelpers(unittest.TestCase):
    def test_strip_removes_stale_rr_code(self):
        existing = [
            "REVALIDATION_PRICE_FROM_IBKR_DIRECT",
            "LIVE_RISK_REWARD_TOO_LOW",
            "AGENTIC_AUTHORITY_AGENTIC_APPROVE_REDUCED",
        ]
        stripped = _strip_recomputable_entry_bracket_codes(existing)
        self.assertIn("REVALIDATION_PRICE_FROM_IBKR_DIRECT", stripped)
        self.assertIn("AGENTIC_AUTHORITY_AGENTIC_APPROVE_REDUCED", stripped)
        self.assertNotIn("LIVE_RISK_REWARD_TOO_LOW", stripped)

    def test_is_entry_bracket_hard_block_code(self):
        self.assertTrue(_is_entry_bracket_hard_block_code("LIVE_RISK_REWARD_TOO_LOW"))
        self.assertTrue(_is_entry_bracket_hard_block_code("LIVE_BRACKET_NOT_VIABLE_WITHIN_GUARDRAILS"))
        self.assertFalse(_is_entry_bracket_hard_block_code("REVALIDATION_PRICE_FROM_IBKR_DIRECT"))


class TestAgenticMaterializeBracketSeed(unittest.TestCase):
    """Verify agentic materializer seeds joint_decision from structural builder."""

    @patch("app.routers.live._merge_structural_contract_and_diagnostics")
    @patch("app.routers.live._apply_post_committee_entry_viability_and_qty")
    @patch("app.routers.live._fetch_ibkr_mart_reference_close")
    @patch("app.routers.live.build_structural_verdict_envelope_v1")
    @patch("app.routers.live.fetch_all")
    def test_materialize_seeds_structural_joint_decision_for_short(
        self,
        mock_fetch_all,
        mock_envelope,
        mock_mart_close,
        mock_viability,
        mock_merge_contract,
    ):
        from app.routers.live import _materialize_structural_entry_agentic_apply

        mock_mart_close.return_value = 30.0
        mock_fetch_all.return_value = [{"NET_LIQUIDATION_EUR": 10000.0, "MAX_POSITION_PCT": 0.05}]
        mock_envelope.return_value = {"structural_verdict_envelope_v1": {}}

        captured_jd = {}

        def _capture_viability(cur, **kwargs):
            captured_jd.update(kwargs.get("joint_decision") or {})
            return 20.0, kwargs.get("reason_codes") or []

        mock_viability.side_effect = _capture_viability

        action = {
            "ACTION_ID": "test-short-action",
            "PORTFOLIO_ID": 1,
            "SYMBOL": "JD",
            "SIDE": "SELL",
            "DIRECTION": "SHORT",
            "INVALIDATION_LEVEL": 30.99,
            "PROPOSED_PRICE": 30.06,
            "REVALIDATION_PRICE": 29.88,
            "CURRENT_PRICE": 29.99,
            "ENTRY_ZONE_LOW": 29.55625,
            "ENTRY_ZONE_HIGH": 29.999,
            "TARGET_EXPECTATION_SNAPSHOT": {"bands": {"base": 0.02}},
            "PARAM_SNAPSHOT": {},
        }
        authority_row = {
            "AUTHORITY_STATUS": "AGENTIC_APPROVE_REDUCED",
            "AUTHORITY_CONFIDENCE": 0.7,
            "IS_STALE": False,
            "AUTHORITY_ID": "auth-1",
        }

        cur = MagicMock()
        cur.execute = MagicMock()

        def _fetch_after_viability(cur_in, action_id):
            return {
                **action,
                "PARAM_SNAPSHOT": {
                    "executable_bracket": {
                        "target_return": 0.036679,
                        "stop_loss_pct": 0.033344,
                        "calibrated": False,
                        "blocked": False,
                    },
                    "committee_bracket_baseline": {
                        "realistic_target_return": 0.036679,
                        "stop_loss_pct": 0.033344,
                    },
                },
            }

        with patch("app.routers.live._fetch_live_action", side_effect=_fetch_after_viability):
            out = _materialize_structural_entry_agentic_apply(
                cur,
                "test-short-action",
                action,
                authority_row,
            )

        self.assertTrue(out.get("ok"))
        self.assertIn("realistic_target_return", captured_jd)
        self.assertIn("stop_loss_pct", captured_jd)
        self.assertTrue(captured_jd.get("agentic_source"))
        tr = float(captured_jd["realistic_target_return"])
        sl = float(captured_jd["stop_loss_pct"])
        self.assertGreaterEqual(tr / sl, _MIN_RR_FOR_LIVE_ENTRY - 1e-12)

    @patch("app.routers.live._merge_structural_contract_and_diagnostics")
    @patch("app.routers.live._apply_post_committee_entry_viability_and_qty")
    @patch("app.routers.live._fetch_ibkr_mart_reference_close")
    @patch("app.routers.live.build_structural_verdict_envelope_v1")
    @patch("app.routers.live.fetch_all")
    def test_incomplete_contract_blocks_ready_status(
        self,
        mock_fetch_all,
        mock_envelope,
        mock_mart_close,
        mock_viability,
        mock_merge_contract,
    ):
        from app.routers.live import _materialize_structural_entry_agentic_apply

        mock_mart_close.return_value = 30.0
        mock_fetch_all.return_value = [{"NET_LIQUIDATION_EUR": 10000.0, "MAX_POSITION_PCT": 0.05}]
        mock_envelope.return_value = {"structural_verdict_envelope_v1": {}}
        mock_viability.return_value = (
            20.0,
            ["LIVE_RISK_REWARD_TOO_LOW", "STRUCT_SUBMIT_CONTRACT_INCOMPLETE"],
        )

        action = {
            "ACTION_ID": "test-blocked",
            "PORTFOLIO_ID": 1,
            "SYMBOL": "JD",
            "SIDE": "SELL",
            "DIRECTION": "SHORT",
            "PARAM_SNAPSHOT": {},
        }
        authority_row = {
            "AUTHORITY_STATUS": "AGENTIC_APPROVE",
            "AUTHORITY_CONFIDENCE": 0.8,
            "IS_STALE": False,
        }

        cur = MagicMock()
        cur.execute = MagicMock()

        with patch("app.routers.live._fetch_live_action", return_value=action):
            out = _materialize_structural_entry_agentic_apply(
                cur,
                "test-blocked",
                action,
                authority_row,
            )

        self.assertEqual(out.get("action_status"), "OPEN_BLOCKED")


class TestPreflightRecomputeClearsStale(unittest.TestCase):
    @patch("app.routers.live._live_bracket_realism_reason_codes", return_value=[])
    @patch("app.routers.live._load_bracket_realism_config")
    @patch("app.routers.live._load_live_entry_fee_params")
    @patch("app.routers.live._live_execution_requires_ib_risk_gates", return_value=True)
    @patch("app.routers.live._read_app_config")
    @patch("app.routers.live.fetch_all")
    @patch("app.routers.live.is_structural_live_action", return_value=True)
    def test_preflight_no_rr_block_after_fix(
        self,
        _mock_structural,
        mock_fetch_all,
        mock_read_cfg,
        _mock_ib_gates,
        mock_fee,
        mock_realism,
        _mock_bracket_realism,
    ):
        from app.routers.live import _preflight_entry_bracket_hard_block_reason_codes

        mock_read_cfg.return_value = {
            "LIVE_ENFORCE_LONG_ONLY": "false",
            "LIVE_BLOCK_ON_BROKER_SHORT": "true",
        }
        mock_fetch_all.return_value = [
            {"IBKR_ACCOUNT_ID": "DU123", "ADAPTER_MODE": "LIVE", "BUST_PCT": 0.2}
        ]
        mock_fee.return_value = _fee_params()
        mock_realism.return_value = {"enabled": False}

        action = {
            "PORTFOLIO_ID": 1,
            "SIDE": "SELL",
            "DIRECTION": "SHORT",
            "ACTION_INTENT": "ENTRY",
            "LIVE_INTENT_KIND": "STRUCTURAL",
            "SETUP_FAMILY": "AGENTIC_SHORT_THESIS",
            "INVALIDATION_LEVEL": 30.99,
            "REVALIDATION_PRICE": 29.88,
            "PROPOSED_PRICE": 30.06,
            "PROPOSED_QTY": 20,
            "PARAM_SNAPSHOT": {
                "executable_bracket": {
                    "target_return": 0.036679,
                    "stop_loss_pct": 0.033344,
                    "blocked": False,
                },
                "structural_diagnostics_v1": {"structural_contract_complete": True},
            },
        }

        cur = MagicMock()
        codes = _preflight_entry_bracket_hard_block_reason_codes(cur, action)
        self.assertNotIn("LIVE_RISK_REWARD_TOO_LOW", codes)


if __name__ == "__main__":
    unittest.main()
