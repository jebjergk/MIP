"""
Trailing Stop Phase 1 — exit_policy service tests.

Covers:
  - Profile registry shape (FIXED_STANDARD has no trail params; TRAIL_*
    profiles are PCT with bounded trail values and policy_version=v1).
  - resolve_exit_policy_for_action override precedence (caller > per-action
    override > EXIT_PROFILE > FIXED_STANDARD default).
  - validate_trail_params bounds, mode/reference whitelist, version check.
  - broker_trail_args returns (trail_amount=None, trail_percent=N) for PCT
    and rejects ABS in Phase 1.
"""
from __future__ import annotations

import unittest

from app.services.live_intelligence import exit_policy as ep


class ProfileRegistryTests(unittest.TestCase):
    def test_known_profiles_includes_all_four(self):
        self.assertEqual(
            set(ep.known_profiles()),
            {"FIXED_STANDARD", "TRAIL_TIGHT", "TRAIL_STANDARD", "TRAIL_WIDE"},
        )

    def test_fixed_standard_has_no_trail_params(self):
        resolved = ep.resolve_exit_policy("FIXED_STANDARD")
        self.assertEqual(resolved["exit_policy"], ep.EXIT_POLICY_FIXED)
        self.assertEqual(resolved["trail_status"], ep.TRAIL_STATUS_NOT_REQUESTED)
        self.assertIsNone(resolved["trail_style"])
        self.assertIsNone(resolved["trail_params"])
        self.assertEqual(resolved["resolved_profile"], "FIXED_STANDARD")

    def test_trail_profiles_are_pct_with_bounded_values(self):
        for name, expected_value in (
            ("TRAIL_TIGHT", 1.5),
            ("TRAIL_STANDARD", 2.5),
            ("TRAIL_WIDE", 4.0),
        ):
            resolved = ep.resolve_exit_policy(name)
            self.assertEqual(resolved["exit_policy"], ep.EXIT_POLICY_TRAIL, name)
            self.assertEqual(resolved["trail_status"], ep.TRAIL_STATUS_REQUESTED, name)
            self.assertEqual(resolved["trail_style"], "PCT", name)
            params = resolved["trail_params"]
            self.assertEqual(params["trail_mode"], "PCT", name)
            self.assertEqual(params["trail_value"], expected_value, name)
            self.assertEqual(params["reference"], "ENTRY_FILL", name)
            self.assertEqual(params["policy_version"], ep.POLICY_VERSION, name)

    def test_unknown_profile_raises(self):
        with self.assertRaises(ValueError):
            ep.resolve_exit_policy("UNKNOWN_PROFILE")

    def test_empty_profile_raises(self):
        with self.assertRaises(ValueError):
            ep.resolve_exit_policy("")
        with self.assertRaises(ValueError):
            ep.resolve_exit_policy(None)

    def test_resolve_returns_independent_dicts(self):
        a = ep.resolve_exit_policy("TRAIL_STANDARD")
        b = ep.resolve_exit_policy("TRAIL_STANDARD")
        a["trail_params"]["trail_value"] = 99.0
        self.assertEqual(b["trail_params"]["trail_value"], 2.5)


class OverridePrecedenceTests(unittest.TestCase):
    def test_explicit_override_wins(self):
        action = {
            "EXIT_PROFILE": "FIXED_STANDARD",
            "EXIT_POLICY_OVERRIDE": "TRAIL_TIGHT",
        }
        resolved = ep.resolve_exit_policy_for_action(
            action, explicit_override="TRAIL_WIDE"
        )
        self.assertEqual(resolved["resolved_profile"], "TRAIL_WIDE")

    def test_action_override_beats_profile(self):
        action = {
            "EXIT_PROFILE": "FIXED_STANDARD",
            "EXIT_POLICY_OVERRIDE": "TRAIL_TIGHT",
        }
        resolved = ep.resolve_exit_policy_for_action(action)
        self.assertEqual(resolved["resolved_profile"], "TRAIL_TIGHT")

    def test_profile_used_when_no_override(self):
        resolved = ep.resolve_exit_policy_for_action({"EXIT_PROFILE": "TRAIL_STANDARD"})
        self.assertEqual(resolved["resolved_profile"], "TRAIL_STANDARD")

    def test_default_when_nothing_set(self):
        resolved = ep.resolve_exit_policy_for_action({})
        self.assertEqual(resolved["resolved_profile"], "FIXED_STANDARD")
        self.assertEqual(resolved["exit_policy"], ep.EXIT_POLICY_FIXED)


class ValidateTrailParamsTests(unittest.TestCase):
    def _good(self):
        return {
            "trail_mode": "PCT",
            "trail_value": 2.5,
            "reference": "ENTRY_FILL",
            "tp_mode": "LIMIT",
            "profile": "TRAIL_STANDARD",
            "policy_version": "v1",
        }

    def test_valid_params_no_violations(self):
        self.assertEqual(ep.validate_trail_params(self._good()), [])

    def test_non_dict_violation(self):
        for bad in (None, "", "not a dict", 42, []):
            self.assertEqual(ep.validate_trail_params(bad), ["TRAIL_PARAMS_NOT_OBJECT"])

    def test_missing_mode(self):
        p = self._good(); del p["trail_mode"]
        self.assertIn("MISSING_TRAIL_MODE", ep.validate_trail_params(p))

    def test_unsupported_mode(self):
        p = self._good(); p["trail_mode"] = "ABS"
        self.assertIn("UNSUPPORTED_TRAIL_MODE", ep.validate_trail_params(p))

    def test_missing_value(self):
        p = self._good(); del p["trail_value"]
        self.assertIn("MISSING_TRAIL_VALUE", ep.validate_trail_params(p))

    def test_value_below_lower_bound(self):
        p = self._good(); p["trail_value"] = 0.1
        self.assertIn("TRAIL_VALUE_OUT_OF_BOUNDS", ep.validate_trail_params(p))

    def test_value_above_upper_bound(self):
        p = self._good(); p["trail_value"] = 50.0
        self.assertIn("TRAIL_VALUE_OUT_OF_BOUNDS", ep.validate_trail_params(p))

    def test_unsupported_reference(self):
        p = self._good(); p["reference"] = "PRIOR_DAY_CLOSE"
        self.assertIn("UNSUPPORTED_TRAIL_REFERENCE", ep.validate_trail_params(p))

    def test_missing_policy_version(self):
        p = self._good(); del p["policy_version"]
        self.assertIn("MISSING_POLICY_VERSION", ep.validate_trail_params(p))

    def test_unsupported_policy_version(self):
        p = self._good(); p["policy_version"] = "v999"
        self.assertIn("UNSUPPORTED_POLICY_VERSION", ep.validate_trail_params(p))

    def test_non_numeric_value(self):
        p = self._good(); p["trail_value"] = "two-point-five"
        self.assertIn("TRAIL_VALUE_NOT_NUMERIC", ep.validate_trail_params(p))


class BrokerTrailArgsTests(unittest.TestCase):
    def test_pct_returns_amount_none_percent_value(self):
        params = {"trail_mode": "PCT", "trail_value": 2.5}
        amt, pct = ep.broker_trail_args(params)
        self.assertIsNone(amt)
        self.assertEqual(pct, 2.5)

    def test_abs_rejected_in_phase1(self):
        with self.assertRaises(ValueError):
            ep.broker_trail_args({"trail_mode": "ABS", "trail_value": 1.5})

    def test_unknown_mode_raises(self):
        with self.assertRaises(ValueError):
            ep.broker_trail_args({"trail_mode": "FOOBAR", "trail_value": 1.0})

    def test_missing_value_raises(self):
        with self.assertRaises(ValueError):
            ep.broker_trail_args({"trail_mode": "PCT"})


if __name__ == "__main__":
    unittest.main()
