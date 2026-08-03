import threading
import unittest
import uuid
from datetime import date, datetime
from unittest.mock import patch

from app.brooks_intraday.bars import HistoricalBar
from app.brooks_intraday.errors import BrooksIntradayError
from app.brooks_intraday.replay_bar_view import ReplayBarView
from app.brooks_intraday.replay_engine import process_next_bar, replay_reset, replay_start
from app.brooks_intraday.replay_schedule import build_replay_schedule, total_steps_for_week


def _bar(sym: str, ny_h: int, ny_m: int, utc_h: int, utc_m: int, td: date) -> HistoricalBar:
    return HistoricalBar(
        symbol=sym,
        ts_utc=datetime(2026, 7, td.day, utc_h, utc_m),
        ts_ny=datetime(2026, 7, td.day, ny_h, ny_m),
        trading_date=td,
        open=1.0,
        high=2.0,
        low=0.5,
        close=1.5,
        volume=100.0,
        source="T",
        bar_size_minutes=5,
        rth=True,
    )


class BrooksPhase3ScheduleTests(unittest.TestCase):
    def test_five_sessions_390_steps(self):
        self.assertEqual(total_steps_for_week(date(2026, 7, 20)), 390)

    def test_timestamp_order(self):
        steps = build_replay_schedule(date(2026, 7, 20))
        self.assertEqual(len(steps), 390)
        self.assertEqual(steps[0].bar_timestamp_ny, datetime(2026, 7, 20, 9, 30))
        self.assertEqual(steps[77].bar_timestamp_ny, datetime(2026, 7, 20, 15, 55))
        self.assertEqual(steps[78].bar_timestamp_ny, datetime(2026, 7, 21, 9, 30))
        self.assertEqual(steps[-1].bar_timestamp_ny, datetime(2026, 7, 24, 15, 55))

    def test_monday_close_to_tuesday_open(self):
        steps = build_replay_schedule(date(2026, 7, 20))
        self.assertEqual(steps[77].bar_timestamp_ny, datetime(2026, 7, 20, 15, 55))
        self.assertEqual(steps[78].bar_timestamp_ny, datetime(2026, 7, 21, 9, 30))


class BrooksPhase3FutureBarTests(unittest.TestCase):
    def _bars(self):
        b1 = HistoricalBar(
            symbol="AAPL",
            ts_utc=datetime(2026, 7, 20, 13, 30),
            ts_ny=datetime(2026, 7, 20, 9, 30),
            trading_date=date(2026, 7, 20),
            open=1,
            high=2,
            low=0.5,
            close=1.5,
            volume=10,
            source="T",
            bar_size_minutes=5,
            rth=True,
        )
        b2 = HistoricalBar(
            symbol="AAPL",
            ts_utc=datetime(2026, 7, 20, 13, 35),
            ts_ny=datetime(2026, 7, 20, 9, 35),
            trading_date=date(2026, 7, 20),
            open=1,
            high=2,
            low=0.5,
            close=1.5,
            volume=10,
            source="T",
            bar_size_minutes=5,
            rth=True,
        )
        return {"AAPL": [b1, b2]}

    def test_visible_only_through_cursor(self):
        view = ReplayBarView(
            bars_by_symbol=self._bars(),
            cursor_timestamp_utc=datetime(2026, 7, 20, 13, 30),
            symbols=["AAPL"],
        )
        vis = view.visible_bars("AAPL")
        self.assertEqual(len(vis), 1)

    def test_future_denied(self):
        view = ReplayBarView(
            bars_by_symbol=self._bars(),
            cursor_timestamp_utc=datetime(2026, 7, 20, 13, 30),
            symbols=["AAPL"],
        )
        with self.assertRaises(BrooksIntradayError) as ctx:
            view.assert_timestamp_allowed(datetime(2026, 7, 20, 13, 35))
        self.assertEqual(ctx.exception.code, "FUTURE_BAR_ACCESS_DENIED")


class BrooksPhase3EngineMockTests(unittest.TestCase):
    def _mock_state(self):
        week = date(2026, 7, 20)
        steps = build_replay_schedule(week)
        td = date(2026, 7, 20)
        symbols = ["AAPL", "AMZN", "JPM", "MCD"]
        index = {}
        for step in steps[:2]:
            for sym in symbols:
                index[(sym, step.trading_date, step.bar_timestamp_utc)] = HistoricalBar(
                    symbol=sym,
                    ts_utc=step.bar_timestamp_utc,
                    ts_ny=step.bar_timestamp_ny,
                    trading_date=step.trading_date,
                    open=1.0,
                    high=2.0,
                    low=0.5,
                    close=1.5,
                    volume=100.0,
                    source="T",
                    bar_size_minutes=5,
                    rth=True,
                )
        return {
            "run_id": "r1",
            "symbols": symbols,
            "selected_week_start": week,
            "status": "PAUSED",
            "bars_frozen": True,
            "dossiers_frozen": True,
            "playback_speed": "manual",
            "readiness": {"replay_readiness": "READY"},
            "replay_cursor": {
                "run_id": "r1",
                "completed_steps": 0,
                "total_steps": 390,
                "trading_date": td.isoformat(),
                "active_dossiers": {s: {} for s in symbols},
            },
            "_frozen_bars_index": index,
            "_replay_schedule": steps,
        }

    @patch("app.brooks_intraday.replay_engine.insert_objective_observation")
    @patch("app.brooks_intraday.replay_engine.ensure_replay_attempt", return_value="attempt-test")
    @patch("app.brooks_intraday.replay_engine.slice_observation_count", return_value=0)
    @patch("app.brooks_intraday.replay_engine.count_observations", return_value=0)
    @patch("app.brooks_intraday.replay_engine.verify_cursor_obs_consistency")
    @patch("app.brooks_intraday.replay_engine.save_cursor")
    def test_one_next_bar_four_symbols(self, *_mocks):
        state = self._mock_state()
        state["active_replay_attempt_id"] = "attempt-test"
        out = process_next_bar(state, request_token=str(uuid.uuid4()))
        self.assertEqual(len(out["bars"]), 4)
        symbols = [b["symbol"] for b in out["bars"]]
        self.assertEqual(symbols, ["AAPL", "AMZN", "JPM", "MCD"])
        self.assertEqual(state["replay_cursor"]["completed_steps"], 1)

    def test_completed_rejects(self):
        state = {
            "run_id": "r1",
            "symbols": ["AAPL", "AMZN", "JPM", "MCD"],
            "selected_week_start": date(2026, 7, 20),
            "status": "COMPLETED",
            "bars_frozen": True,
            "dossiers_frozen": True,
            "readiness": {"replay_readiness": "READY"},
        }
        with self.assertRaises(BrooksIntradayError) as ctx:
            process_next_bar(state)
        self.assertEqual(ctx.exception.code, "REPLAY_ALREADY_COMPLETED")

    @patch("app.brooks_intraday.replay_engine.create_replay_attempt", return_value="new-attempt")
    @patch("app.brooks_intraday.replay_engine._load_session_dossiers", return_value={s: {} for s in ["AAPL", "AMZN", "JPM", "MCD"]})
    @patch("app.brooks_intraday.replay_engine.persist_replay_cursor")
    def test_reset_cursor_zero(self, _persist, _doss, _create):
        state = self._mock_state()
        state["replay_cursor"]["completed_steps"] = 5
        replay_reset(state)
        self.assertEqual(state["replay_cursor"]["completed_steps"], 0)
        self.assertEqual(state["status"], "READY")

    @patch("app.brooks_intraday.replay_engine.verify_schedule_bars")
    @patch("app.brooks_intraday.replay_engine._load_session_dossiers", return_value={"AAPL": {}, "AMZN": {}, "JPM": {}, "MCD": {}})
    @patch("app.brooks_intraday.replay_engine.save_cursor")
    def test_start_from_stopped_requires_reset(self, *_):
        state = self._mock_state()
        state["status"] = "STOPPED"
        with self.assertRaises(BrooksIntradayError) as ctx:
            replay_start(state)
        self.assertEqual(ctx.exception.code, "REPLAY_NOT_READY")

    @patch("app.brooks_intraday.replay_engine.insert_objective_observation")
    @patch("app.brooks_intraday.replay_engine.ensure_replay_attempt", return_value="attempt-test")
    @patch("app.brooks_intraday.replay_engine.slice_observation_count", return_value=0)
    @patch("app.brooks_intraday.replay_engine.count_observations", return_value=0)
    @patch("app.brooks_intraday.replay_engine.verify_cursor_obs_consistency")
    @patch("app.brooks_intraday.replay_engine.save_cursor")
    def test_idempotent_request_token(self, *_mocks):
        state = self._mock_state()
        state["active_replay_attempt_id"] = "attempt-test"
        token = str(uuid.uuid4())
        out1 = process_next_bar(state, request_token=token)
        out2 = process_next_bar(state, request_token=token)
        self.assertEqual(out1["global_step_index"], out2["global_step_index"])
        self.assertEqual(state["replay_cursor"]["completed_steps"], 1)


if __name__ == "__main__":
    unittest.main()
