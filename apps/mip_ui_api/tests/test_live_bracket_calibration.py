import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.routers.live import _live_bracket_realism_codes_pure
from app.routers.live_bracket_calibration import _calibrate_live_entry_bracket_to_min_viable


def _frozen_rcfg():
    return {
        "enabled": True,
        "mode": "BLOCK",
        "apply_to_paper": False,
        "abs_min_gross_tp_usd": 1.0,
        "abs_min_gross_sl_usd": 1.0,
        "min_gross_tp_pct_notional": 0.015,
        "min_net_tp_pct_notional": 0.01,
        "min_gross_sl_pct_notional": 0.01,
        "min_gross_tp_bps_of_nav": 12.0,
        "nav_rule_cap_mult": 5.0,
        "small_pos_max_pct_nav": 0.10,
        "small_pos_strict_mult": 1.2,
        "min_bracket_width_bps": 25.0,
    }


def _fee_params():
    return {"slippage_bps": 2.0, "fee_bps": 1.0, "spread_bps": 0.0}


class TestLiveBracketCalibration(unittest.TestCase):
    def test_sbux_like_calibrates_when_guardrails_allow(self):
        """Same geometry as test_scenario_1_sbux_like_fails; wider caps yield empty realism codes."""
        rcfg = _frozen_rcfg()
        fee = _fee_params()
        ep = 94.93
        q = 2.0
        tr = 0.0065
        sl = (ep - 94.46) / ep
        c = _calibrate_live_entry_bracket_to_min_viable(
            side="BUY",
            entry_price=ep,
            qty=q,
            nav_scale=3000.0,
            baseline_target_return=tr,
            baseline_stop_loss_pct=sl,
            bust_pct=0.08,
            fee_params=fee,
            rcfg=rcfg,
            calib_cfg={"enabled": True, "max_tp_mult": 3.5, "max_sl_mult": 3.0, "max_tp_abs_add": 0.05},
        )
        self.assertTrue(c.ok, msg=c.meta)
        self.assertIsNotNone(c.target_return)
        self.assertIsNotNone(c.stop_loss_pct)
        tp_p = ep * (1 + float(c.target_return))
        sl_p = max(ep * (1 - float(c.stop_loss_pct)), 0.0001)
        codes = _live_bracket_realism_codes_pure(
            nav_scale=3000.0,
            side="BUY",
            entry_price=ep,
            qty=q,
            tp_price=tp_p,
            sl_price=sl_p,
            target_return=float(c.target_return),
            fee_params=fee,
            rcfg=rcfg,
        )
        self.assertEqual(codes, [])

    def test_sbux_like_fails_within_tight_guardrails(self):
        rcfg = _frozen_rcfg()
        fee = _fee_params()
        ep = 94.93
        q = 2.0
        tr = 0.0065
        sl = (ep - 94.46) / ep
        c = _calibrate_live_entry_bracket_to_min_viable(
            side="BUY",
            entry_price=ep,
            qty=q,
            nav_scale=3000.0,
            baseline_target_return=tr,
            baseline_stop_loss_pct=sl,
            bust_pct=0.08,
            fee_params=fee,
            rcfg=rcfg,
            calib_cfg={"enabled": True, "max_tp_mult": 2.0, "max_sl_mult": 2.0, "max_tp_abs_add": 0.01},
        )
        self.assertFalse(c.ok)
        self.assertIn("LIVE_BRACKET_NOT_VIABLE_WITHIN_GUARDRAILS", c.reason_codes)

    def test_mid_line_no_op_when_already_passes(self):
        rcfg = _frozen_rcfg()
        fee = _fee_params()
        ep = 100.0
        q = 4.0
        tr = 0.015
        sl = 0.01
        c = _calibrate_live_entry_bracket_to_min_viable(
            side="BUY",
            entry_price=ep,
            qty=q,
            nav_scale=3000.0,
            baseline_target_return=tr,
            baseline_stop_loss_pct=sl,
            bust_pct=0.08,
            fee_params=fee,
            rcfg=rcfg,
            calib_cfg={"enabled": True, "max_tp_mult": 2.0, "max_sl_mult": 2.0, "max_tp_abs_add": 0.02},
        )
        self.assertTrue(c.ok)
        self.assertFalse(c.calibrated)
        self.assertAlmostEqual(float(c.target_return), tr, places=6)
        self.assertAlmostEqual(float(c.stop_loss_pct), sl, places=6)

    def test_respects_bust_cap(self):
        rcfg = _frozen_rcfg()
        fee = _fee_params()
        c = _calibrate_live_entry_bracket_to_min_viable(
            side="BUY",
            entry_price=50.0,
            qty=10.0,
            nav_scale=5000.0,
            baseline_target_return=0.02,
            baseline_stop_loss_pct=0.01,
            bust_pct=0.015,
            fee_params=fee,
            rcfg=rcfg,
            calib_cfg={"enabled": True, "max_tp_mult": 5.0, "max_sl_mult": 5.0, "max_tp_abs_add": 0.1},
        )
        self.assertTrue(c.ok)
        self.assertLessEqual(float(c.stop_loss_pct), 0.015 + 1e-9)


class TestLoadExecutableBracketSnapshot(unittest.TestCase):
    def test_snapshot_preferred_over_verdict_fields(self):
        from app.routers.live import _load_executable_entry_bracket_for_action

        class _Cur:
            def execute(self, *args, **kwargs):
                raise AssertionError("verdict SQL should not run when snapshot supplies bracket")

        action = {
            "PARAM_SNAPSHOT": {
                "executable_bracket": {
                    "target_return": 0.02,
                    "stop_loss_pct": 0.012,
                    "calibrated": True,
                    "blocked": False,
                    "meta": {},
                }
            }
        }
        tr, sl, src = _load_executable_entry_bracket_for_action(
            _Cur(),
            action=action,
            committee_run_id="any-run",
            bust_pct_default=0.05,
        )
        self.assertEqual(src, "snapshot")
        self.assertAlmostEqual(tr, 0.02)
        self.assertAlmostEqual(sl, 0.012)


if __name__ == "__main__":
    unittest.main()
