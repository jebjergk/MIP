"""Stage 1.5 — recovery materialization + executable target selection."""
from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.routers.agentic_authority_router import (
    _action_needs_recovery_materialization,
    agentic_materializer_status_eligible,
)
from app.routers.live import (
    _live_ib_entry_risk_reason_codes,
    _live_rr_meets_min_floor,
    _select_executable_target_from_joint_decision,
    _strip_recomputable_entry_bracket_codes,
)
from app.services.live_intelligence.structural_committee import (
    _MIN_RR_FOR_LIVE_ENTRY,
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


def _jd_short_jd_like() -> dict:
    action = {
        "DIRECTION": "SHORT",
        "INVALIDATION_LEVEL": 30.99,
        "REVALIDATION_PRICE": 29.76,
        "PROPOSED_PRICE": 30.06,
        "CURRENT_PRICE": 29.99,
        "TARGET_EXPECTATION_SNAPSHOT": {"bands": {"base": 0.02, "strong": 0.026}},
    }
    return build_structural_entry_joint_decision(action)


class TestExecutableTargetSelection(unittest.TestCase):
    def test_jd_early_exit_fails_rr_realistic_passes(self):
        jd = _jd_short_jd_like()
        early = float(jd["acceptable_early_exit_target_return"])
        realistic = float(jd["realistic_target_return"])
        sl = float(jd["stop_loss_pct"])
        self.assertLess(early / sl, _MIN_RR_FOR_LIVE_ENTRY)
        self.assertGreaterEqual(realistic / sl, _MIN_RR_FOR_LIVE_ENTRY - 1e-12)

    def test_selector_chooses_realistic_when_early_exit_fails(self):
        jd = _jd_short_jd_like()
        tr, sl, source = _select_executable_target_from_joint_decision(jd, None)
        self.assertEqual(source, "realistic_target_return")
        self.assertAlmostEqual(tr, float(jd["realistic_target_return"]), places=6)
        self.assertTrue(_live_rr_meets_min_floor(tr, sl, _MIN_RR_FOR_LIVE_ENTRY))

    def test_selector_uses_early_exit_when_it_passes(self):
        jd = {
            "acceptable_early_exit_target_return": 0.05,
            "realistic_target_return": 0.06,
            "stop_loss_pct": 0.04,
        }
        tr, sl, source = _select_executable_target_from_joint_decision(jd, None)
        self.assertEqual(source, "acceptable_early_exit_target_return")
        self.assertAlmostEqual(tr, 0.05, places=6)

    def test_long_buy_realistic_fallback(self):
        jd = {
            "acceptable_early_exit_target_return": 0.01,
            "realistic_target_return": 0.045,
            "stop_loss_pct": 0.04,
        }
        tr, sl, source = _select_executable_target_from_joint_decision(jd, None)
        self.assertEqual(source, "realistic_target_return")
        codes = _live_ib_entry_risk_reason_codes(
            side="BUY",
            is_exit=False,
            entry_price=100.0,
            target_return=tr,
            stop_loss_pct=sl,
            fee_params=_fee_params(),
        )
        self.assertNotIn("LIVE_RISK_REWARD_TOO_LOW", codes)

    def test_short_sell_realistic_fallback(self):
        jd = _jd_short_jd_like()
        tr, sl, _ = _select_executable_target_from_joint_decision(jd, None)
        codes = _live_ib_entry_risk_reason_codes(
            side="SELL",
            is_exit=False,
            entry_price=29.76,
            target_return=tr,
            stop_loss_pct=sl,
            fee_params=_fee_params(),
        )
        self.assertNotIn("LIVE_RISK_REWARD_TOO_LOW", codes)

    def test_no_candidate_passes_keeps_blocking_target(self):
        jd = {
            "acceptable_early_exit_target_return": 0.01,
            "realistic_target_return": 0.02,
            "stop_loss_pct": 0.04,
        }
        tr, sl, source = _select_executable_target_from_joint_decision(jd, None)
        self.assertEqual(source, "realistic_target_return")
        codes = _live_ib_entry_risk_reason_codes(
            side="SELL",
            is_exit=False,
            entry_price=30.0,
            target_return=tr,
            stop_loss_pct=sl,
            fee_params=_fee_params(),
        )
        self.assertIn("LIVE_RISK_REWARD_TOO_LOW", codes)


class TestRevalidateReasonCodeClearing(unittest.TestCase):
    @patch("app.routers.live._live_bracket_realism_reason_codes", return_value=[])
    @patch("app.routers.live._load_bracket_realism_config")
    @patch("app.routers.live._load_live_entry_fee_params")
    @patch("app.routers.live._live_execution_requires_ib_risk_gates", return_value=True)
    @patch("app.routers.live._read_app_config")
    @patch("app.routers.live.fetch_all")
    @patch("app.routers.live.is_structural_live_action", return_value=True)
    def test_preflight_clears_rr_when_realistic_selected(
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

        jd = _jd_short_jd_like()
        action = {
            "PORTFOLIO_ID": 1,
            "SIDE": "SELL",
            "DIRECTION": "SHORT",
            "ACTION_INTENT": "ENTRY",
            "LIVE_INTENT_KIND": "STRUCTURAL",
            "SETUP_FAMILY": "AGENTIC_SHORT_THESIS",
            "INVALIDATION_LEVEL": 30.99,
            "REVALIDATION_PRICE": 29.76,
            "PROPOSED_PRICE": 30.06,
            "PROPOSED_QTY": 20,
            "PARAM_SNAPSHOT": {
                "structural_diagnostics_v1": {"structural_contract_complete": True},
            },
        }

        cur = MagicMock()
        codes = _preflight_entry_bracket_hard_block_reason_codes(cur, action)
        self.assertNotIn("LIVE_RISK_REWARD_TOO_LOW", codes)

        existing = [
            "REVALIDATION_PRICE_FROM_IBKR_DIRECT",
            "LIVE_RISK_REWARD_TOO_LOW",
            "STRUCT_SUBMIT_CONTRACT_INCOMPLETE",
        ]
        stripped = _strip_recomputable_entry_bracket_codes(existing)
        self.assertNotIn("LIVE_RISK_REWARD_TOO_LOW", stripped)
        merged = stripped + codes
        self.assertNotIn("LIVE_RISK_REWARD_TOO_LOW", merged)


class TestRecoveryMaterializationEligibility(unittest.TestCase):
    def _incomplete_jd_action(self) -> dict:
        return {
            "STATUS": "REVALIDATED_PASS",
            "REASON_CODES": [
                "LIVE_RISK_REWARD_TOO_LOW",
                "STRUCT_SUBMIT_CONTRACT_INCOMPLETE",
            ],
            "PARAM_SNAPSHOT": {
                "executable_bracket": None,
                "committee_bracket_baseline": {
                    "acceptable_early_exit_target_return": None,
                    "realistic_target_return": None,
                    "stop_loss_pct": None,
                },
                "structural_diagnostics_v1": {"structural_contract_complete": False},
            },
        }

    def test_incomplete_revalidated_pass_is_recovery_eligible(self):
        action = self._incomplete_jd_action()
        self.assertTrue(_action_needs_recovery_materialization(action))
        eligible, reason = agentic_materializer_status_eligible(action)
        self.assertTrue(eligible)
        self.assertEqual(reason, "recovery_incomplete_contract")

    def test_healthy_revalidated_pass_not_eligible(self):
        action = {
            "STATUS": "REVALIDATED_PASS",
            "REASON_CODES": ["REVALIDATION_PRICE_FROM_IBKR_DIRECT"],
            "PARAM_SNAPSHOT": {
                "executable_bracket": {
                    "target_return": 0.045,
                    "stop_loss_pct": 0.04,
                    "blocked": False,
                },
                "committee_bracket_baseline": {
                    "realistic_target_return": 0.045,
                    "stop_loss_pct": 0.04,
                },
                "structural_diagnostics_v1": {"structural_contract_complete": True},
            },
        }
        self.assertFalse(_action_needs_recovery_materialization(action))
        eligible, reason = agentic_materializer_status_eligible(action)
        self.assertFalse(eligible)
        self.assertEqual(reason, "status_not_eligible")

    def test_revalidated_fail_incomplete_is_recovery_eligible(self):
        action = self._incomplete_jd_action()
        action["STATUS"] = "REVALIDATED_FAIL"
        eligible, reason = agentic_materializer_status_eligible(action)
        self.assertTrue(eligible)
        self.assertEqual(reason, "recovery_incomplete_contract")

    def test_ready_for_approval_still_forward_eligible(self):
        action = {"STATUS": "READY_FOR_APPROVAL_FLOW", "PARAM_SNAPSHOT": {}}
        eligible, reason = agentic_materializer_status_eligible(action)
        self.assertTrue(eligible)
        self.assertEqual(reason, "forward_eligible_status")


class TestRecoveryMaterializerPreservesStatus(unittest.TestCase):
    @patch("app.routers.live._merge_structural_contract_and_diagnostics")
    @patch("app.routers.live._apply_post_committee_entry_viability_and_qty")
    @patch("app.routers.live._fetch_ibkr_mart_reference_close")
    @patch("app.routers.live.build_structural_verdict_envelope_v1")
    @patch("app.routers.live.fetch_all")
    def test_recovery_preserves_revalidated_pass(
        self,
        mock_fetch_all,
        mock_envelope,
        mock_mart_close,
        mock_viability,
        mock_merge_contract,
    ):
        from app.routers.live import _materialize_structural_entry_agentic_apply

        mock_mart_close.return_value = 29.76
        mock_fetch_all.return_value = [{"NET_LIQUIDATION_EUR": 10000.0, "MAX_POSITION_PCT": 0.05}]
        mock_envelope.return_value = {"structural_verdict_envelope_v1": {}}

        jd = _jd_short_jd_like()
        tr = float(jd["realistic_target_return"])
        sl = float(jd["stop_loss_pct"])

        mock_viability.return_value = (20.0, ["STRUCTURAL_AGENTIC_REVIEWED"])

        action = {
            "ACTION_ID": "recovery-jd",
            "PORTFOLIO_ID": 1,
            "SYMBOL": "JD",
            "SIDE": "SELL",
            "DIRECTION": "SHORT",
            "STATUS": "REVALIDATED_PASS",
            "INVALIDATION_LEVEL": 30.99,
            "REVALIDATION_PRICE": 29.76,
            "PARAM_SNAPSHOT": {},
        }
        authority_row = {
            "AUTHORITY_STATUS": "AGENTIC_APPROVE_REDUCED",
            "AUTHORITY_CONFIDENCE": 0.7,
            "IS_STALE": False,
        }

        cur = MagicMock()
        cur.execute = MagicMock()

        def _fetch_after(cur_in, action_id):
            return {
                **action,
                "PARAM_SNAPSHOT": {
                    "executable_bracket": {
                        "target_return": tr,
                        "stop_loss_pct": sl,
                        "blocked": False,
                    },
                    "committee_bracket_baseline": {
                        "realistic_target_return": tr,
                        "stop_loss_pct": sl,
                    },
                },
            }

        with patch("app.routers.live._fetch_live_action", side_effect=_fetch_after):
            out = _materialize_structural_entry_agentic_apply(
                cur,
                "recovery-jd",
                action,
                authority_row,
                recovery_late_stage=True,
            )

        self.assertEqual(out.get("action_status"), "REVALIDATED_PASS")
        self.assertTrue(out.get("recovery_late_stage"))

    @patch("app.routers.live._merge_structural_contract_and_diagnostics")
    @patch("app.routers.live._apply_post_committee_entry_viability_and_qty")
    @patch("app.routers.live._fetch_ibkr_mart_reference_close")
    @patch("app.routers.live.build_structural_verdict_envelope_v1")
    @patch("app.routers.live.fetch_all")
    def test_recovery_preserves_revalidated_pass_when_thesis_blocks(
        self,
        mock_fetch_all,
        mock_envelope,
        mock_mart_close,
        mock_viability,
        mock_merge_contract,
    ):
        from app.routers.live import _materialize_structural_entry_agentic_apply

        mock_mart_close.return_value = 29.76
        mock_fetch_all.return_value = [{"NET_LIQUIDATION_EUR": 10000.0, "MAX_POSITION_PCT": 0.05}]
        mock_envelope.return_value = {"structural_verdict_envelope_v1": {}}
        mock_viability.return_value = (None, ["STRUCTURAL_AGENTIC_REVIEWED"])

        action = {
            "ACTION_ID": "recovery-jd-blocked",
            "PORTFOLIO_ID": 1,
            "SYMBOL": "JD",
            "SIDE": "SELL",
            "DIRECTION": "SHORT",
            "STATUS": "REVALIDATED_PASS",
            "PARAM_SNAPSHOT": {},
        }
        authority_row = {
            "AUTHORITY_STATUS": "AGENTIC_FAILED_NO_AUTHORITY",
            "AUTHORITY_CONFIDENCE": 0.0,
            "IS_STALE": False,
        }

        cur = MagicMock()
        cur.execute = MagicMock()

        with patch("app.routers.live._fetch_live_action", return_value=action):
            out = _materialize_structural_entry_agentic_apply(
                cur,
                "recovery-jd-blocked",
                action,
                authority_row,
                recovery_late_stage=True,
            )

        self.assertEqual(out.get("action_status"), "REVALIDATED_PASS")

    @patch("app.routers.live._merge_structural_contract_and_diagnostics")
    @patch("app.routers.live._apply_post_committee_entry_viability_and_qty")
    @patch("app.routers.live._fetch_ibkr_mart_reference_close")
    @patch("app.routers.live.build_structural_verdict_envelope_v1")
    @patch("app.routers.live.fetch_all")
    def test_recovery_seeds_bracket_even_when_thesis_blocks(
        self,
        mock_fetch_all,
        mock_envelope,
        mock_mart_close,
        mock_viability,
        mock_merge_contract,
    ):
        from app.routers.live import _materialize_structural_entry_agentic_apply

        mock_mart_close.return_value = 29.76
        mock_fetch_all.return_value = [{"NET_LIQUIDATION_EUR": 10000.0, "MAX_POSITION_PCT": 0.05}]
        mock_envelope.return_value = {"structural_verdict_envelope_v1": {}}
        mock_viability.return_value = (20.0, ["STRUCTURAL_AGENTIC_REVIEWED"])

        action = {
            "ACTION_ID": "recovery-bracket-seed",
            "PORTFOLIO_ID": 1,
            "SYMBOL": "JD",
            "SIDE": "SELL",
            "DIRECTION": "SHORT",
            "STATUS": "REVALIDATED_PASS",
            "PROPOSED_QTY": 20,
            "PARAM_SNAPSHOT": {},
        }
        authority_row = {
            "AUTHORITY_STATUS": "AGENTIC_FAILED_NO_AUTHORITY",
            "AUTHORITY_CONFIDENCE": 0.0,
            "IS_STALE": False,
        }

        cur = MagicMock()
        cur.execute = MagicMock()

        with patch("app.routers.live._fetch_live_action", return_value=action):
            _materialize_structural_entry_agentic_apply(
                cur,
                "recovery-bracket-seed",
                action,
                authority_row,
                recovery_late_stage=True,
            )

        mock_viability.assert_called_once()
        self.assertFalse(mock_viability.call_args.kwargs["is_committee_blocked"])


if __name__ == "__main__":
    unittest.main()
