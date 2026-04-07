import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.routers.live import _live_bracket_realism_codes_pure


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


class TestLiveBracketRealismPure(unittest.TestCase):
    def test_scenario_1_sbux_like_fails(self):
        """Tuning matrix row 1: small line, tight %."""
        rcfg = _frozen_rcfg()
        fee = _fee_params()
        codes = _live_bracket_realism_codes_pure(
            nav_scale=3000.0,
            side="BUY",
            entry_price=94.93,
            qty=2.0,
            tp_price=95.55,
            sl_price=94.46,
            target_return=0.0065,
            fee_params=fee,
            rcfg=rcfg,
        )
        self.assertIn("LIVE_BRACKET_ABS_GROSS_SL_BELOW_MIN_USD", codes)
        self.assertIn("LIVE_BRACKET_REL_GROSS_TP_BELOW_PCT_NOTIONAL", codes)
        self.assertIn("LIVE_BRACKET_REL_GROSS_TP_BELOW_BPS_NAV", codes)
        self.assertIn("LIVE_BRACKET_REL_NET_TP_BELOW_PCT_NOTIONAL", codes)
        self.assertIn("LIVE_BRACKET_REL_GROSS_SL_BELOW_PCT_NOTIONAL", codes)

    def test_scenario_5_mid_line_passes(self):
        rcfg = _frozen_rcfg()
        fee = _fee_params()
        codes = _live_bracket_realism_codes_pure(
            nav_scale=3000.0,
            side="BUY",
            entry_price=100.0,
            qty=4.0,
            tp_price=101.5,
            sl_price=99.0,
            target_return=0.015,
            fee_params=fee,
            rcfg=rcfg,
        )
        self.assertEqual(codes, [])

    def test_scenario_7_width_fail(self):
        rcfg = _frozen_rcfg()
        fee = _fee_params()
        ep = 100.0
        q = 5.0
        codes = _live_bracket_realism_codes_pure(
            nav_scale=3000.0,
            side="BUY",
            entry_price=ep,
            qty=q,
            tp_price=ep * 1.002,
            sl_price=ep * 0.999,
            target_return=0.002,
            fee_params=fee,
            rcfg=rcfg,
        )
        self.assertIn("LIVE_BRACKET_WIDTH_BELOW_MIN_BPS", codes)

    def test_warn_mode_returns_empty(self):
        rcfg = _frozen_rcfg()
        rcfg["mode"] = "WARN"
        fee = _fee_params()
        codes = _live_bracket_realism_codes_pure(
            nav_scale=3000.0,
            side="BUY",
            entry_price=94.93,
            qty=2.0,
            tp_price=95.55,
            sl_price=94.46,
            target_return=0.0065,
            fee_params=fee,
            rcfg=rcfg,
        )
        self.assertEqual(codes, [])


if __name__ == "__main__":
    unittest.main()
