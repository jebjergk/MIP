"""Phase B — dossier unwrap, levels, blocker diagnostics, week-card progress."""

from __future__ import annotations

import unittest
from datetime import date
from unittest.mock import patch

from app.brooks_intraday.dossier_access import unwrap_dossier
from app.brooks_intraday.experiment_phase9_service import _week_card
from app.brooks_intraday.experiment_progress import build_stage_progress
from app.brooks_intraday.learning_view import (
    _daily_levels_from_dossier,
    _pick_dossier,
    build_blocker_diagnostics,
    build_run_learning_payload,
    _blocker_display_label,
)


class UnwrapDossierTests(unittest.TestCase):
    def test_flat_dossier(self):
        d = {"paa_verdict": "NO_CLEAR_LONG", "reclaim_level": 100.0}
        self.assertEqual(unwrap_dossier(d)["paa_verdict"], "NO_CLEAR_LONG")

    def test_wrapped_dossier(self):
        row = {
            "symbol": "AAPL",
            "trading_date": "2026-06-22",
            "frozen_dossier_json": {
                "paa_verdict": "NO_CLEAR_LONG",
                "resistance_zones": [{"low": 316.94, "high": 317.4}],
                "support_zones": [{"low": 243.42, "high": 246}],
                "reclaim_level": 317.4,
                "do_not_chase_level": 317.4,
                "invalidation_level": 244.71,
            },
        }
        body = unwrap_dossier(row)
        self.assertEqual(body["paa_verdict"], "NO_CLEAR_LONG")
        self.assertEqual(body["reclaim_level"], 317.4)

    def test_missing_nested(self):
        self.assertEqual(unwrap_dossier(None), {})
        self.assertEqual(unwrap_dossier({}), {})


class DailyLevelsTests(unittest.TestCase):
    def test_wrapped_daily_levels_jpm_resistance(self):
        row = {
            "symbol": "JPM",
            "trading_date": date(2026, 6, 22),
            "frozen_dossier_json": {
                "trading_date": "2026-06-22",
                "paa_verdict": "LONG_APPROVE",
                "resistance_zones": [{"low": 326.4, "high": 326.4, "lower": 326.4, "upper": 326.4}],
                "support_zones": [{"low": 301.37, "high": 308.88}],
                "reclaim_level": 316.42,
                "do_not_chase_level": 338.09,
                "invalidation_level": 305.125,
            },
        }
        levels = _daily_levels_from_dossier(row)
        self.assertEqual(levels["resistance"], 326.4)
        self.assertEqual(levels["reclaim_level"], 316.42)
        self.assertEqual(levels["do_not_chase_level"], 338.09)
        self.assertEqual(levels["daily_thesis_invalidation"], 305.125)
        self.assertEqual(levels["trading_date"], "2026-06-22")
        self.assertEqual(levels["source"], "frozen daily dossier")
        res_entry = next(x for x in levels["levels"] if x["name"] == "Resistance")
        self.assertEqual(res_entry["value"], 326.4)
        self.assertEqual(res_entry["display"], 326.4)

    def test_missing_nested_shows_not_available(self):
        levels = _daily_levels_from_dossier({"symbol": "AAPL"})
        # Wrapper without frozen body treats row as flat empty analytical fields
        self.assertIsNone(levels.get("resistance"))
        for entry in levels["levels"]:
            self.assertEqual(entry["display"], "Not available")


class BlockerDiagnosticsTests(unittest.TestCase):
    def test_at_resistance_display_label(self):
        pj = {
            "blockers_json": ["LIMITED_ROOM_TO_RESISTANCE"],
            "room_class": "AT_RESISTANCE",
            "active_levels_json": {"primary_resistance": 326.4, "do_not_chase": 338.09},
        }
        self.assertEqual(_blocker_display_label("LIMITED_ROOM_TO_RESISTANCE", pj), "AT_RESISTANCE")
        diag = build_blocker_diagnostics(pj, close=328.7, session_range=2.95)
        self.assertEqual(diag["display_blocker"], "AT_RESISTANCE")
        self.assertEqual(diag["stored_blocker"], "LIMITED_ROOM_TO_RESISTANCE")
        self.assertEqual(diag["resistance"], 326.4)
        self.assertAlmostEqual(diag["distance"], -2.3, places=5)
        self.assertEqual(diag["session_range"], 2.95)
        self.assertAlmostEqual(diag["room_fraction"], -2.3 / 2.95, places=5)
        self.assertEqual(diag["min_room_acceptable_fraction"], 0.35)
        self.assertEqual(diag["min_room_ample_fraction"], 0.55)

    def test_limited_room_fraction_below_threshold(self):
        pj = {
            "blockers_json": ["LIMITED_ROOM_TO_RESISTANCE"],
            "room_class": "LIMITED_ROOM",
            "active_levels_json": {"primary_resistance": 100.0},
        }
        # distance 1.0 / session_range 4.0 = 0.25 < 0.35 acceptable
        diag = build_blocker_diagnostics(pj, close=99.0, session_range=4.0)
        self.assertEqual(diag["display_blocker"], "LIMITED_ROOM_TO_RESISTANCE")
        self.assertAlmostEqual(diag["room_fraction"], 0.25, places=5)
        self.assertLess(diag["room_fraction"], diag["min_room_acceptable_fraction"])


class StageProgressTests(unittest.TestCase):
    def test_stage_progress_payload(self):
        sp = build_stage_progress(stage="OBJECTIVE_REPLAY", completed=939, total=1560)
        self.assertEqual(sp["completed"], 939)
        self.assertEqual(sp["total"], 1560)
        self.assertEqual(sp["unit"], "observations")
        self.assertIn("updated_at_utc", sp)
        self.assertLessEqual(sp["completed"], sp["total"])


class WeekCardProgressTests(unittest.TestCase):
    def test_partial_week_uses_progress_json_bars(self):
        progress = {
            "weeks": {},
            "bars": {
                "complete": 11,
                "total": 20,
                "current_symbol": "JPM",
                "current_trading_date": "2026-06-03",
            },
        }
        with patch("app.brooks_intraday.experiment_phase9_service.list_stages", return_value=[]), patch(
            "app.brooks_intraday.experiment_phase9_service._is_week_complete_in_db",
            return_value=(False, None),
        ):
            card = _week_card(
                "exec-1",
                date(2026, 6, 1),
                progress,
                current_week_start="2026-06-01",
                overall_status="PAUSED",
            )
        self.assertEqual(card["week_status"], "PAUSED")
        self.assertEqual(card["bar_sessions_complete"], 11)
        self.assertEqual(card["bar_sessions_expected"], 20)
        self.assertEqual(card["current_session"]["symbol"], "JPM")
        self.assertEqual(card["current_session"]["trading_date"], "2026-06-03")


class OverviewVerdictTests(unittest.TestCase):
    def test_overview_paa_verdict_from_wrapped_dossier(self):
        dossiers = [
            {
                "symbol": "AAPL",
                "trading_date": date(2026, 6, 22),
                "frozen_dossier_json": {"paa_verdict": "NO_CLEAR_LONG", "support_zones": [], "resistance_zones": []},
            },
            {
                "symbol": "JPM",
                "trading_date": date(2026, 6, 22),
                "frozen_dossier_json": {
                    "paa_verdict": "LONG_APPROVE",
                    "resistance_zones": [{"low": 326.4, "high": 326.4}],
                },
            },
        ]
        with patch("app.brooks_intraday.learning_view.load_context_observations", return_value=[]), patch(
            "app.brooks_intraday.learning_view.load_sim_trades", return_value=[]
        ), patch(
            "app.brooks_intraday.learning_view.load_blocked_signals", return_value=[]
        ), patch(
            "app.brooks_intraday.learning_view.load_pattern_snapshot_index", return_value={}
        ), patch(
            "app.brooks_intraday.learning_view.load_simulation_attempt", return_value=None
        ):
            payload = build_run_learning_payload(
                run_id="894e32f9-6add-4ba9-8564-824889090bee",
                state={"symbols": ["AAPL", "JPM"], "selected_week_start": date(2026, 6, 22)},
                cfg={},
                symbols=["AAPL", "JPM"],
                dossiers=dossiers,
                mode="full",
                active_trading_date=date(2026, 6, 22),
            )
        by_sym = {o["symbol"]: o for o in payload["symbol_overviews"]}
        self.assertEqual(by_sym["AAPL"]["paa_verdict"], "NO_CLEAR_LONG")
        self.assertNotEqual(by_sym["AAPL"]["paa_verdict"], None)
        self.assertEqual(by_sym["JPM"]["paa_verdict"], "LONG_APPROVE")

    def test_pick_dossier_prefers_requested_trading_date(self):
        dossiers = [
            {"symbol": "AAPL", "trading_date": date(2026, 6, 22), "frozen_dossier_json": {"paa_verdict": "NO_CLEAR_LONG"}},
            {"symbol": "AAPL", "trading_date": date(2026, 6, 23), "frozen_dossier_json": {"paa_verdict": "WAIT_PULLBACK"}},
        ]
        picked = _pick_dossier(dossiers, "AAPL", date(2026, 6, 23))
        self.assertTrue(str(picked.get("trading_date")).startswith("2026-06-23"))


class ReviewWeekResetContractTests(unittest.TestCase):
    def test_reset_selects_first_matrix_date_and_clears_selection(self):
        """Mirrors BrooksIntradayLab.resetReviewStateForRun behaviour."""
        matrix = [
            {"trading_date": "2026-06-24", "symbol": "AAPL"},
            {"trading_date": "2026-06-22", "symbol": "AAPL"},
            {"trading_date": "2026-06-23", "symbol": "JPM"},
        ]
        dates = sorted({r["trading_date"] for r in matrix if r.get("trading_date")})
        selected_bar_ts = None
        matrix_date = dates[0] if dates else ""
        self.assertEqual(matrix_date, "2026-06-22")
        self.assertIsNone(selected_bar_ts)


if __name__ == "__main__":
    unittest.main()
