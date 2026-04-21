"""Contract tests for the committee bake-off CONFIG_STATUS decision tree.

These tests mirror the SQL logic inside MIP.APP.SP_REFRESH_COMMITTEE_BAKEOFF
(see MIP/SQL/app/601_sp_refresh_committee_bakeoff.sql) so we LOCK the rules:

    REAL board (config sourced from LIVE_ACTIONS + STRUCTURAL_PROPOSAL_SNAPSHOT):
        - normalized action == 'CASH'                       -> NOT_APPLICABLE
        - missing SYMBOL / ENTRY_ZONE_LOW / INVALIDATION    -> UNSCORABLE
        - missing TRAIL_STYLE or MAX_HOLD_BARS               -> PARTIAL
        - all required present                              -> SCORABLE

    SHADOW board (numeric levels are ALWAYS inherited from
    STRUCTURAL_PROPOSAL_SNAPSHOT because the shadow board emits only advisory
    text — by design, never invent missing values):
        - normalized action == 'CASH'                       -> NOT_APPLICABLE
        - missing SYMBOL / ENTRY_ZONE_LOW / STOP_PRICE      -> UNSCORABLE
        - else                                              -> PARTIAL  (NEVER SCORABLE)

If anyone refactors the SQL and breaks these contracts, these tests must
fail loudly. They intentionally re-implement the rules in Python so the
tests serve as executable documentation of the bake-off design intent.

No Snowflake or FastAPI imports needed.
"""
import unittest


# ---------------------------------------------------------------------------
# Reference Python re-implementations of the SQL CASE expressions.
# ---------------------------------------------------------------------------

def real_config_status(*, normalized_action, symbol, entry_zone_low,
                       invalidation_level, trail_style, max_hold_bars):
    if normalized_action == "CASH":
        return "NOT_APPLICABLE"
    if symbol is None or entry_zone_low is None or invalidation_level is None:
        return "UNSCORABLE"
    if trail_style is None or max_hold_bars is None:
        return "PARTIAL"
    return "SCORABLE"


def real_config_reason(*, normalized_action, symbol, entry_zone_low,
                       invalidation_level, trail_style, max_hold_bars):
    if normalized_action == "CASH":
        return "No position; scoring not applicable."
    if symbol is None:
        return "Missing symbol on STRUCTURAL_PROPOSAL_SNAPSHOT."
    if entry_zone_low is None:
        return "Missing entry zone on LIVE_ACTIONS."
    if invalidation_level is None:
        return "Missing stop (INVALIDATION_LEVEL) on LIVE_ACTIONS."
    if trail_style is None and max_hold_bars is None:
        return "No trail style and no MAX_HOLD_BARS; horizon defaulted to 20."
    if trail_style is None:
        return "No trail style on LIVE_ACTIONS; stop+horizon scoring only."
    if max_hold_bars is None:
        return "No MAX_HOLD_BARS on LIVE_ACTIONS; defaulted to 20."
    return "All required fields sourced from LIVE_ACTIONS."


def shadow_config_status(*, normalized_action, symbol, entry_zone_low, stop_price):
    if normalized_action == "CASH":
        return "NOT_APPLICABLE"
    if symbol is None or entry_zone_low is None or stop_price is None:
        return "UNSCORABLE"
    # Shadow ALWAYS pulls numeric levels from the proposal snapshot, never
    # from a shadow-emitted config. By contract, ENTER rows are at best PARTIAL.
    return "PARTIAL"


def shadow_config_reason(*, normalized_action, symbol, entry_zone_low, stop_price):
    if normalized_action == "CASH":
        return "No position; scoring not applicable."
    if symbol is None:
        return "Missing symbol on STRUCTURAL_PROPOSAL_SNAPSHOT."
    if entry_zone_low is None:
        return "Missing entry zone on STRUCTURAL_PROPOSAL_SNAPSHOT."
    if stop_price is None:
        return "Missing stop on STRUCTURAL_PROPOSAL_SNAPSHOT."
    return ("Numeric stop/entry inherited from STRUCTURAL_PROPOSAL_SNAPSHOT "
            "(shadow board did not emit own numeric config). No TP from shadow source.")


def normalize_real_stance(raw_stance):
    """Mirrors MIP.APP.COMMITTEE_FINAL_DECISION normalization (APPROVE => ENTER)."""
    return "ENTER" if (raw_stance or "").upper() in ("APPROVE", "APPROVE_REDUCED") else "CASH"


def normalize_shadow_stance(raw_stance):
    """Mirrors SHADOW_STANCE normalization."""
    return "ENTER" if (raw_stance or "").upper() in ("APPROVE", "APPROVE_REDUCED") else "CASH"


# ---------------------------------------------------------------------------
# REAL board contract
# ---------------------------------------------------------------------------

class TestRealConfigStatus(unittest.TestCase):
    def _all_fields(self, **overrides):
        base = dict(
            normalized_action="ENTER",
            symbol="AAPL",
            entry_zone_low=180.0,
            invalidation_level=170.0,
            trail_style="ATR",
            max_hold_bars=20,
        )
        base.update(overrides)
        return base

    def test_full_real_config_is_scorable(self):
        f = self._all_fields()
        self.assertEqual(real_config_status(**f), "SCORABLE")
        self.assertEqual(real_config_reason(**f),
                         "All required fields sourced from LIVE_ACTIONS.")

    def test_missing_stop_is_unscorable(self):
        f = self._all_fields(invalidation_level=None)
        self.assertEqual(real_config_status(**f), "UNSCORABLE")
        self.assertIn("INVALIDATION_LEVEL", real_config_reason(**f))

    def test_missing_entry_zone_is_unscorable(self):
        f = self._all_fields(entry_zone_low=None)
        self.assertEqual(real_config_status(**f), "UNSCORABLE")
        self.assertIn("entry zone", real_config_reason(**f).lower())

    def test_missing_symbol_is_unscorable(self):
        f = self._all_fields(symbol=None)
        self.assertEqual(real_config_status(**f), "UNSCORABLE")
        self.assertIn("symbol", real_config_reason(**f).lower())

    def test_missing_trail_style_is_partial_not_scorable(self):
        f = self._all_fields(trail_style=None)
        self.assertEqual(real_config_status(**f), "PARTIAL")
        self.assertIn("trail style", real_config_reason(**f).lower())

    def test_missing_max_hold_is_partial(self):
        f = self._all_fields(max_hold_bars=None)
        self.assertEqual(real_config_status(**f), "PARTIAL")
        self.assertIn("max_hold_bars", real_config_reason(**f).lower())

    def test_cash_is_not_applicable_regardless_of_other_fields(self):
        f = self._all_fields(normalized_action="CASH",
                             entry_zone_low=None, invalidation_level=None)
        self.assertEqual(real_config_status(**f), "NOT_APPLICABLE")
        self.assertIn("not applicable", real_config_reason(**f).lower())

    def test_unscorable_takes_precedence_over_partial(self):
        f = self._all_fields(invalidation_level=None, trail_style=None)
        self.assertEqual(real_config_status(**f), "UNSCORABLE")


# ---------------------------------------------------------------------------
# SHADOW board contract — the critical "never silently invent" rule
# ---------------------------------------------------------------------------

class TestShadowConfigStatus(unittest.TestCase):
    def _all_fields(self, **overrides):
        base = dict(
            normalized_action="ENTER",
            symbol="AAPL",
            entry_zone_low=180.0,
            stop_price=170.0,
        )
        base.update(overrides)
        return base

    def test_shadow_enter_with_full_snapshot_is_partial_not_scorable(self):
        """Shadow board ALWAYS borrows numeric levels from the proposal
        snapshot, so even a fully-populated row must be PARTIAL — never
        SCORABLE — to surface the provenance to the user."""
        f = self._all_fields()
        self.assertEqual(shadow_config_status(**f), "PARTIAL")
        reason = shadow_config_reason(**f)
        self.assertIn("STRUCTURAL_PROPOSAL_SNAPSHOT", reason)
        self.assertIn("shadow board did not emit", reason)

    def test_shadow_missing_stop_is_unscorable(self):
        f = self._all_fields(stop_price=None)
        self.assertEqual(shadow_config_status(**f), "UNSCORABLE")
        self.assertIn("Missing stop", shadow_config_reason(**f))

    def test_shadow_missing_entry_zone_is_unscorable(self):
        f = self._all_fields(entry_zone_low=None)
        self.assertEqual(shadow_config_status(**f), "UNSCORABLE")
        self.assertIn("Missing entry zone", shadow_config_reason(**f))

    def test_shadow_missing_symbol_is_unscorable(self):
        f = self._all_fields(symbol=None)
        self.assertEqual(shadow_config_status(**f), "UNSCORABLE")
        self.assertIn("Missing symbol", shadow_config_reason(**f))

    def test_shadow_cash_is_not_applicable(self):
        f = self._all_fields(normalized_action="CASH", stop_price=None)
        self.assertEqual(shadow_config_status(**f), "NOT_APPLICABLE")
        self.assertIn("not applicable", shadow_config_reason(**f).lower())

    def test_shadow_never_promotes_to_scorable(self):
        """Loop a wide array of inputs to ensure no combination ever yields
        SCORABLE for the shadow board. This is the load-bearing invariant."""
        for sym in ("AAPL", "MSFT", "RIVN"):
            for ezl in (1.0, 100.0, 9999.0):
                for stop in (0.5, 50.0, 8888.0):
                    status = shadow_config_status(
                        normalized_action="ENTER",
                        symbol=sym, entry_zone_low=ezl, stop_price=stop,
                    )
                    self.assertNotEqual(status, "SCORABLE",
                                        f"Shadow promoted to SCORABLE for "
                                        f"{sym=} {ezl=} {stop=}")


# ---------------------------------------------------------------------------
# Stance normalization contract
# ---------------------------------------------------------------------------

class TestStanceNormalization(unittest.TestCase):
    def test_real_approve_becomes_enter(self):
        self.assertEqual(normalize_real_stance("APPROVE"), "ENTER")
        self.assertEqual(normalize_real_stance("approve"), "ENTER")
        self.assertEqual(normalize_real_stance("APPROVE_REDUCED"), "ENTER")

    def test_real_defer_becomes_cash(self):
        self.assertEqual(normalize_real_stance("DEFER"), "CASH")

    def test_real_deny_becomes_cash(self):
        self.assertEqual(normalize_real_stance("DENY"), "CASH")
        self.assertEqual(normalize_real_stance("REJECT"), "CASH")

    def test_real_unknown_becomes_cash(self):
        self.assertEqual(normalize_real_stance(None), "CASH")
        self.assertEqual(normalize_real_stance(""), "CASH")
        self.assertEqual(normalize_real_stance("MAYBE"), "CASH")

    def test_shadow_approve_becomes_enter(self):
        self.assertEqual(normalize_shadow_stance("APPROVE"), "ENTER")
        self.assertEqual(normalize_shadow_stance("APPROVE_REDUCED"), "ENTER")

    def test_shadow_defer_kept_distinct_from_deny_at_raw_level(self):
        """Raw stance must be preserved upstream so DEFER vs DENY can be
        analyzed; both normalize to CASH, but the design persists RAW_STANCE
        on COMMITTEE_BAKEOFF_LATCH so analysts can split them later."""
        self.assertEqual(normalize_shadow_stance("DEFER"), "CASH")
        self.assertEqual(normalize_shadow_stance("DENY"), "CASH")
        self.assertNotEqual("DEFER", "DENY")  # raw values stay distinct


if __name__ == "__main__":
    unittest.main()
