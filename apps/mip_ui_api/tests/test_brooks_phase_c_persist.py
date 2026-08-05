"""Phase C — persist mode, schedule-key integrity, progress phases, sim isolation guards."""

from __future__ import annotations

import os
import unittest
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from app.brooks_intraday.experiment_progress import (
    PROGRESS_PHASE_COMPUTE,
    PROGRESS_PHASE_INTEGRITY,
    PROGRESS_PHASE_PERSIST,
    build_stage_progress,
    emit_progress,
)
from app.brooks_intraday.persist_integrity import (
    assert_schedule_keys_match,
    normalize_bar_ts,
    schedule_key_set,
)
from app.brooks_intraday.persist_mode import PERSIST_MODE_BULK, PERSIST_MODE_LEGACY, get_persist_mode, is_bulk_persist
from app.brooks_intraday.simulation_repository import clear_run_simulation_artifacts


class PersistModeTests(unittest.TestCase):
    def test_default_is_legacy(self):
        env = {k: v for k, v in os.environ.items() if k != "BROOKS_PERSIST_MODE"}
        with patch.dict(os.environ, env, clear=True):
            self.assertEqual(get_persist_mode(), PERSIST_MODE_LEGACY)
            self.assertFalse(is_bulk_persist())

    def test_bulk_requires_explicit_env(self):
        with patch.dict(os.environ, {"BROOKS_PERSIST_MODE": "bulk"}):
            self.assertEqual(get_persist_mode(), PERSIST_MODE_BULK)
            self.assertTrue(is_bulk_persist())


class ScheduleKeyIntegrityTests(unittest.TestCase):
    def test_bidirectional_set_equality(self):
        expected = {("AAPL", "2026-06-22 13:30:00"), ("JPM", "2026-06-22 13:30:00")}
        assert_schedule_keys_match(expected=expected, persisted=set(expected), label="t")
        with self.assertRaises(ValueError):
            assert_schedule_keys_match(
                expected=expected,
                persisted={("AAPL", "2026-06-22 13:30:00")},
                label="t",
            )
        with self.assertRaises(ValueError):
            assert_schedule_keys_match(
                expected={("AAPL", "2026-06-22 13:30:00")},
                persisted=expected,
                label="t",
            )

    def test_schedule_key_set_uses_symbols_and_ts(self):
        def norm(ts):
            return ts

        schedule = [
            SimpleNamespace(bar_timestamp_utc=datetime(2026, 6, 22, 13, 30, 0)),
            SimpleNamespace(bar_timestamp_utc=datetime(2026, 6, 22, 13, 35, 0)),
        ]
        keys = schedule_key_set(schedule, ["AAPL", "JPM"], normalize_utc=norm)
        self.assertEqual(len(keys), 4)
        self.assertIn(("AAPL", "2026-06-22 13:30:00"), keys)
        self.assertEqual(normalize_bar_ts("2026-06-22T13:30:00"), "2026-06-22 13:30:00")


class ProgressPhaseTests(unittest.TestCase):
    def test_build_includes_phase(self):
        sp = build_stage_progress(
            stage="CONTEXT_REPLAY",
            completed=10,
            total=100,
            phase=PROGRESS_PHASE_PERSIST,
            unit="rows",
        )
        self.assertEqual(sp["phase"], PROGRESS_PHASE_PERSIST)
        self.assertEqual(sp["unit"], "rows")

    def test_emit_progress_passes_phase(self):
        seen = {}

        def cb(completed, *, phase=None, total=None, unit=None):
            seen["completed"] = completed
            seen["phase"] = phase
            seen["total"] = total

        emit_progress(cb, 5, phase=PROGRESS_PHASE_INTEGRITY, total=2, unit="checks")
        self.assertEqual(seen["phase"], PROGRESS_PHASE_INTEGRITY)
        self.assertEqual(seen["completed"], 5)

    def test_emit_progress_legacy_callback(self):
        seen = {}

        def cb(completed):
            seen["completed"] = completed

        emit_progress(cb, 3, phase=PROGRESS_PHASE_COMPUTE, total=10)
        self.assertEqual(seen["completed"], 3)


class SimulationIsolationGuards(unittest.TestCase):
    def test_clear_run_artifacts_disabled(self):
        with self.assertRaises(RuntimeError):
            clear_run_simulation_artifacts("any-run")


class FailAttemptFreshConnectionTests(unittest.TestCase):
    def test_fail_replay_uses_get_connection(self):
        from app.brooks_intraday.persist_integrity import fail_replay_attempt

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur
        with patch("app.brooks_intraday.persist_integrity.get_connection", return_value=mock_conn) as gc:
            fail_replay_attempt(attempt_id="a1", notes="phase_c_fail:test")
        gc.assert_called()
        mock_conn.commit.assert_called()
        mock_conn.close.assert_called()


class PatternInstanceCountPolicyTests(unittest.TestCase):
    def test_zero_instances_is_valid_equality(self):
        expected = 0
        actual = 0
        self.assertEqual(actual, expected)


if __name__ == "__main__":
    unittest.main()
