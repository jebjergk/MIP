import unittest

from app.brooks_intraday.objective_ruleset_v01 import (
    DEFAULT_PARAMETERS,
    compute_geometry,
    evaluate_brooks_terms,
    prior_bar_relationship,
    relative_metrics,
)


class Phase4GeometryTests(unittest.TestCase):
    def test_bull_bar(self):
        g = compute_geometry(open_=100, high=110, low=99, close=108, volume=1000)
        self.assertEqual(g.direction, "BULLISH")
        self.assertAlmostEqual(g.body_fraction, 8 / 11, places=4)

    def test_bear_bar(self):
        g = compute_geometry(open_=110, high=111, low=100, close=101, volume=500)
        self.assertEqual(g.direction, "BEARISH")

    def test_doji(self):
        g = compute_geometry(open_=100, high=110, low=90, close=100.005, volume=100)
        self.assertLessEqual(g.body_fraction or 1, DEFAULT_PARAMETERS["doji_body_max_fraction"])

    def test_zero_range(self):
        g = compute_geometry(open_=50, high=50, low=50, close=50, volume=10)
        self.assertEqual(g.data_quality, "INVALID_ZERO_RANGE")

    def test_close_near_high(self):
        g = compute_geometry(open_=100, high=110, low=100, close=109.5, volume=1)
        rel = relative_metrics(g, [], DEFAULT_PARAMETERS)
        relship = prior_bar_relationship(g, None, params=DEFAULT_PARAMETERS)
        facts, terms, _rules = evaluate_brooks_terms(
            geometry=g,
            relative=rel,
            relationship=relship,
            session_state={"consecutive_bull": 0, "consecutive_bear": 0, "prior_bar_flags": {}},
            params=DEFAULT_PARAMETERS,
        )
        self.assertIn("CLOSE_NEAR_HIGH", facts)


class Phase4RelationshipTests(unittest.TestCase):
    def test_inside_bar(self):
        prior = compute_geometry(open_=100, high=105, low=95, close=102, volume=1)
        cur = compute_geometry(open_=101, high=104, low=96, close=103, volume=1)
        rel = prior_bar_relationship(cur, prior, params=DEFAULT_PARAMETERS)
        self.assertTrue(rel["inside_bar"])

    def test_session_open_no_prior(self):
        g = compute_geometry(open_=1, high=2, low=0.5, close=1.5, volume=1)
        rel = prior_bar_relationship(g, None, params=DEFAULT_PARAMETERS)
        self.assertFalse(rel["has_prior_intraday_bar"])


class Phase4RelativeTests(unittest.TestCase):
    def test_not_enough_history(self):
        g = compute_geometry(open_=10, high=11, low=9, close=10.5, volume=100)
        rel = relative_metrics(g, [], DEFAULT_PARAMETERS)
        self.assertEqual(rel["relative_range_class"], "NOT_ENOUGH_HISTORY")


class Phase4FreezePersistenceTests(unittest.TestCase):
    def test_reconstruction_sets_flags(self):
        from app.brooks_intraday import paa_reconstruction as mod

        source = open(mod.__file__).read()
        self.assertIn('configuration", {})["dossiers_frozen"] = True', source)
        self.assertIn('configuration", {})["bars_frozen"]', source)


if __name__ == "__main__":
    unittest.main()
