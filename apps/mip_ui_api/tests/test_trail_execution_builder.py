"""
Trailing Stop Phase 1 — execution builder + validation contract tests.

These tests exercise the policy/validation seams of the entry-time TRAIL
rollout WITHOUT spinning up Snowflake or the FastAPI router. They prove:

  1. structural_proposal_minimum_contract_violations is EXIT_POLICY-aware:
     - FIXED_BRACKET (or unset) does NOT raise MISSING_TRAIL_*.
     - TRAIL_BRACKET requires TRAIL_STYLE + TRAIL_PARAMS.

  2. The exit_policy service produces a broker-executable trail spec end to
     end (profile -> resolve -> validate -> broker_trail_args).

  3. ShadowTradeArtifact carries exit_profile / exit_policy fields so the
     shadow chair can recommend an exit contract that mirrors the primary
     path.

  4. parse_chair_ruling extracts exit_profile / exit_policy from chair JSON.

  5. build_shadow_evidence_pack puts the resolved exit_profile / exit_policy
     into proposal_meta so the chair agent has context.

  6. Paper-leg shape contract: TRAIL_PAPER intent rows must keep
     LIMIT_PRICE/STOP_PRICE NULL and rely on TRAIL_PERCENT/TRAIL_AMOUNT
     (asserted via a small payload simulator that mirrors the production
     branch logic).
"""
from __future__ import annotations

import unittest

from app.committee.shadow_types import (
    ShadowTradeArtifact,
    build_shadow_evidence_pack,
    parse_chair_ruling,
)
from app.services.live_intelligence import exit_policy as ep
from app.services.live_intelligence.live_intent_policy import (
    structural_proposal_minimum_contract_violations,
)


def _baseline_proposal() -> dict:
    """A proposal-shaped dict with all non-trail required fields populated."""
    return {
        "SETUP_EVENT_ID": 12345,
        "SETUP_FAMILY": "BREAKOUT_RECLAIM",
        "DIRECTION": "LONG",
        "ENTRY_ZONE_LOW": 100.0,
        "ENTRY_ZONE_HIGH": 102.0,
        "INVALIDATION_LEVEL": 95.0,
        "INVALIDATION_RULE": "CLOSE_BELOW",
        "EXIT_STYLE": "STRUCTURAL_TARGET",
        "TRUST_LABEL": "TRUSTED",
        "STRUCTURAL_STATE": "RECLAIM",
        "REGIME_COMPAT": "NEUTRAL",
        "RISK_CLASS": "STANDARD",
        "SETUP_NARRATIVE": "Reclaim above prior breakdown level.",
        "PROPOSAL_RATIONALE": "Path metrics show favorable MFE/MAE.",
    }


class StructuralValidationExitPolicyAwareTests(unittest.TestCase):
    def test_fixed_bracket_does_not_require_trail_fields(self):
        p = _baseline_proposal()
        p["EXIT_POLICY"] = "FIXED_BRACKET"
        viols = structural_proposal_minimum_contract_violations(p)
        self.assertEqual(viols, [], f"unexpected violations for FIXED_BRACKET: {viols}")

    def test_unset_exit_policy_treated_as_fixed(self):
        # Phase 1 import resolves EXIT_POLICY before this validator runs.
        # If somehow absent, validator must NOT explode with MISSING_TRAIL_*
        # for what is effectively a fixed-bracket row.
        p = _baseline_proposal()
        viols = structural_proposal_minimum_contract_violations(p)
        self.assertNotIn("MISSING_TRAIL_STYLE", viols)
        self.assertNotIn("MISSING_TRAIL_PARAMS", viols)

    def test_trail_bracket_requires_style_and_params(self):
        p = _baseline_proposal()
        p["EXIT_POLICY"] = "TRAIL_BRACKET"
        viols = structural_proposal_minimum_contract_violations(p)
        self.assertIn("MISSING_TRAIL_STYLE", viols)
        self.assertIn("MISSING_TRAIL_PARAMS", viols)

    def test_trail_bracket_with_complete_fields_passes(self):
        p = _baseline_proposal()
        p["EXIT_POLICY"] = "TRAIL_BRACKET"
        p["TRAIL_STYLE"] = "PCT"
        p["TRAIL_PARAMS"] = {"trail_mode": "PCT", "trail_value": 2.5}
        viols = structural_proposal_minimum_contract_violations(p)
        self.assertEqual(viols, [], f"unexpected violations: {viols}")


class EndToEndProfileResolutionTests(unittest.TestCase):
    def test_profile_to_broker_args_roundtrip_pct(self):
        resolved = ep.resolve_exit_policy("TRAIL_STANDARD")
        violations = ep.validate_trail_params(resolved["trail_params"])
        self.assertEqual(violations, [])
        amt, pct = ep.broker_trail_args(resolved["trail_params"])
        self.assertIsNone(amt)
        self.assertEqual(pct, 2.5)

    def test_fixed_profile_yields_no_broker_trail_args(self):
        resolved = ep.resolve_exit_policy("FIXED_STANDARD")
        self.assertIsNone(resolved["trail_params"])
        # broker_trail_args is only called when EXIT_POLICY = TRAIL_BRACKET
        self.assertEqual(resolved["exit_policy"], ep.EXIT_POLICY_FIXED)


class ShadowExitPolicyContractTests(unittest.TestCase):
    def test_shadow_trade_artifact_carries_exit_fields(self):
        art = ShadowTradeArtifact(
            entry_zone="105-107",
            exit_profile="TRAIL_STANDARD",
            exit_policy="TRAIL_BRACKET",
        )
        self.assertEqual(art.exit_profile, "TRAIL_STANDARD")
        self.assertEqual(art.exit_policy, "TRAIL_BRACKET")

    def test_parse_chair_ruling_extracts_exit_fields(self):
        raw = (
            '{'
            '"shadow_stance":"APPROVE",'
            '"shadow_confidence":0.7,'
            '"plurality_basis":"4 APPROVE",'
            '"conflict_resolution":"none",'
            '"top_supports":[],'
            '"top_tensions":[],'
            '"shadow_trade":{'
            '"entry_zone":"105-107",'
            '"size_posture":"FULL",'
            '"trail_posture":"NORMAL",'
            '"key_condition":"reclaim holds",'
            '"exit_profile":"TRAIL_STANDARD",'
            '"exit_policy":"TRAIL_BRACKET"'
            '}'
            '}'
        )
        ruling = parse_chair_ruling(raw)
        self.assertTrue(ruling.parse_ok)
        self.assertIsNotNone(ruling.shadow_trade)
        self.assertEqual(ruling.shadow_trade.exit_profile, "TRAIL_STANDARD")
        self.assertEqual(ruling.shadow_trade.exit_policy, "TRAIL_BRACKET")

    def test_evidence_pack_proposal_meta_has_exit_policy_from_profile(self):
        hearing = {
            "HEARING_ID": "hear-1",
            "EVIDENCE_JSON": "{}",
            "DELTAS_JSON": "{}",
        }
        snapshot = {
            "ENTRY_ZONE_JSON": "{}",
            "INVALIDATION_JSON": "{}",
            "PATH_METRICS_JSON": "{}",
            "MFE_MAE_JSON": "{}",
            "PROPOSAL_SUMMARY_JSON": "{}",
            "SETUP_FAMILY": "BREAKOUT",
            "PROPOSAL_TS": "2026-04-20T10:00:00",
            "STRUCTURAL_STATE": "RECLAIM",
            "REGIME_STATE": "RANGE",
            "TRUST_LABEL": "TRUSTED",
            "TRAILING_STYLE": "PCT",
        }
        proposal = {
            "PROPOSAL_ID": 42,
            "SYMBOL": "AAPL",
            "DIRECTION": "LONG",
            "EXIT_PROFILE": "TRAIL_STANDARD",
        }
        pack = build_shadow_evidence_pack(hearing, snapshot, proposal, [], [])
        meta = pack.slices["proposal_meta"]
        self.assertEqual(meta["exit_profile"], "TRAIL_STANDARD")
        self.assertEqual(meta["exit_policy"], "TRAIL_BRACKET")

    def test_evidence_pack_proposal_meta_fixed_profile(self):
        hearing = {"HEARING_ID": "h", "EVIDENCE_JSON": "{}", "DELTAS_JSON": "{}"}
        snapshot = {
            "ENTRY_ZONE_JSON": "{}",
            "INVALIDATION_JSON": "{}",
            "PATH_METRICS_JSON": "{}",
            "MFE_MAE_JSON": "{}",
            "PROPOSAL_SUMMARY_JSON": "{}",
        }
        proposal = {"PROPOSAL_ID": 1, "EXIT_PROFILE": "FIXED_STANDARD"}
        pack = build_shadow_evidence_pack(hearing, snapshot, proposal, [], [])
        meta = pack.slices["proposal_meta"]
        self.assertEqual(meta["exit_profile"], "FIXED_STANDARD")
        self.assertEqual(meta["exit_policy"], "FIXED_BRACKET")


class PaperLegShapeContractTests(unittest.TestCase):
    """
    Mirrors the live.py paper branch logic: when EXIT_POLICY = TRAIL_BRACKET
    a TRAIL_PAPER leg is appended with limit_price=None and stop_price=None,
    carrying trail_percent / trail_amount instead. The fixed STP_SL_PAPER
    leg MUST be suppressed.
    """

    def _build_paper_legs(self, *, exit_policy, sl_price, tp_price, trail_percent):
        legs = [{"order_type": "MKT_PAPER", "limit_price": 100.0}]
        if tp_price is not None:
            legs.append({"order_type": "LMT_TP_PAPER", "limit_price": tp_price})
        if exit_policy == ep.EXIT_POLICY_TRAIL:
            legs.append(
                {
                    "order_type": "TRAIL_PAPER",
                    "limit_price": None,
                    "stop_price": None,
                    "trail_style": "PCT",
                    "trail_amount": None,
                    "trail_percent": trail_percent,
                }
            )
        elif sl_price is not None:
            legs.append(
                {
                    "order_type": "STP_SL_PAPER",
                    "limit_price": sl_price,
                    "stop_price": sl_price,
                }
            )
        return legs

    def test_trail_bracket_emits_trail_paper_no_fixed_stp(self):
        legs = self._build_paper_legs(
            exit_policy=ep.EXIT_POLICY_TRAIL,
            sl_price=95.0,            # invalidation level still set upstream
            tp_price=110.0,
            trail_percent=2.5,
        )
        types = [leg["order_type"] for leg in legs]
        self.assertIn("MKT_PAPER", types)
        self.assertIn("LMT_TP_PAPER", types)
        self.assertIn("TRAIL_PAPER", types)
        self.assertNotIn(
            "STP_SL_PAPER", types,
            "TRAIL_BRACKET must not emit a fixed STP paper leg",
        )

    def test_trail_paper_leg_has_null_prices_and_pct_fields(self):
        legs = self._build_paper_legs(
            exit_policy=ep.EXIT_POLICY_TRAIL,
            sl_price=None,
            tp_price=110.0,
            trail_percent=2.5,
        )
        trail = next(leg for leg in legs if leg["order_type"] == "TRAIL_PAPER")
        self.assertIsNone(trail["limit_price"])
        self.assertIsNone(trail["stop_price"])
        self.assertEqual(trail["trail_percent"], 2.5)
        self.assertIsNone(trail["trail_amount"])
        self.assertEqual(trail["trail_style"], "PCT")

    def test_fixed_bracket_emits_stp_no_trail_paper(self):
        legs = self._build_paper_legs(
            exit_policy=ep.EXIT_POLICY_FIXED,
            sl_price=95.0,
            tp_price=110.0,
            trail_percent=None,
        )
        types = [leg["order_type"] for leg in legs]
        self.assertIn("STP_SL_PAPER", types)
        self.assertNotIn("TRAIL_PAPER", types)


class NoFixedSTPOnTrailBracketProofTests(unittest.TestCase):
    """
    Proof scaffolding: simulates the broker_sl_price guard from execute_live_action.
    """

    def _broker_sl_price(self, *, structural_exit_policy, sl_price, is_exit):
        if structural_exit_policy == ep.EXIT_POLICY_TRAIL and not is_exit:
            return None
        return sl_price

    def test_trail_bracket_clears_sl_price_for_broker(self):
        self.assertIsNone(
            self._broker_sl_price(
                structural_exit_policy=ep.EXIT_POLICY_TRAIL,
                sl_price=95.0,
                is_exit=False,
            )
        )

    def test_fixed_bracket_keeps_sl_price_for_broker(self):
        self.assertEqual(
            self._broker_sl_price(
                structural_exit_policy=ep.EXIT_POLICY_FIXED,
                sl_price=95.0,
                is_exit=False,
            ),
            95.0,
        )

    def test_exit_action_keeps_sl_price_logic_unaffected(self):
        # is_exit short-circuits all entry-time bracket logic upstream.
        self.assertEqual(
            self._broker_sl_price(
                structural_exit_policy=ep.EXIT_POLICY_TRAIL,
                sl_price=95.0,
                is_exit=True,
            ),
            95.0,
        )


if __name__ == "__main__":
    unittest.main()
