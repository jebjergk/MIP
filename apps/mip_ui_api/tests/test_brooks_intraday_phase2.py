import unittest
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from app.brooks_intraday.bars import (
    HistoricalBar,
    aggregate_1m_to_5m,
    validate_bars,
)
from app.brooks_intraday.calendar import (
    TradingSession,
    berlin_offset_hours_at,
    resolve_week_sessions,
    rth_open_utc_naive,
)
from app.brooks_intraday.constants import EXPECTED_RTH_5M_BARS_FULL_SESSION
from app.brooks_intraday.dossier_compiler import compile_frozen_dossier
from app.brooks_intraday.level_derivation import derive_levels
from app.brooks_intraday.paa_selection import PaaSelectionResult, select_pre_rth_paa


class BrooksCalendarTests(unittest.TestCase):
    def test_five_day_week_july_2026(self):
        week = resolve_week_sessions(
            date(2026, 7, 27),
            holidays={},
        )
        self.assertEqual(week.status, "READY")
        self.assertEqual(len(week.trading_dates), 5)

    def test_holiday_short_week(self):
        week = resolve_week_sessions(
            date(2026, 7, 27),
            holidays={date(2026, 7, 28): {"full_day_close": True, "name": "Test"}},
        )
        self.assertEqual(week.status, "WEEK_HAS_FEWER_THAN_FIVE_SESSIONS")

    def test_early_close_blocked(self):
        week = resolve_week_sessions(
            date(2026, 7, 27),
            holidays={date(2026, 7, 28): {"full_day_close": False, "name": "Half day"}},
        )
        self.assertEqual(week.status, "EARLY_CLOSE_REQUIRES_EXPLICIT_SUPPORT")

    def test_dst_berlin_offset_differs(self):
        winter = datetime(2026, 1, 5, 9, 30)
        summer = datetime(2026, 7, 6, 9, 30)
        self.assertNotEqual(
            berlin_offset_hours_at(winter),
            berlin_offset_hours_at(summer),
        )


class BrooksBarTests(unittest.TestCase):
    def _session(self, d: date) -> TradingSession:
        return TradingSession(
            trading_date=d,
            session_open_utc=rth_open_utc_naive(d),
            session_close_utc=datetime.combine(d, time(16, 0)),
            session_open_ny=datetime.combine(d, time(9, 30)),
            session_close_ny=datetime.combine(d, time(16, 0)),
            is_early_close=False,
            expected_5m_bars=EXPECTED_RTH_5M_BARS_FULL_SESSION,
        )

    def _bar(self, d: date, i: int, price: float = 100.0) -> HistoricalBar:
        from app.brooks_intraday.interval_validation import expected_interval_starts_ny

        ny = expected_interval_starts_ny(d)[i]
        return HistoricalBar(
            symbol="AAPL",
            ts_utc=ny,
            ts_ny=ny,
            trading_date=d,
            open=price,
            high=price + 0.5,
            low=price - 0.5,
            close=price,
            volume=1000,
            source="TEST",
            bar_size_minutes=5,
            rth=True,
        )

    def test_complete_78_bar_session(self):
        d = date(2026, 7, 27)
        session = self._session(d)
        bars = [self._bar(d, i) for i in range(78)]
        result = validate_bars(bars, session, source="TEST")
        self.assertEqual(result.status, "COMPLETE")
        self.assertEqual(result.bar_count, 78)

    def test_missing_bar_detected(self):
        d = date(2026, 7, 27)
        session = self._session(d)
        bars = [self._bar(d, i) for i in range(10)]
        result = validate_bars(bars, session, source="TEST")
        self.assertEqual(result.status, "MISSING_BARS")

    def test_duplicate_bar_detected(self):
        d = date(2026, 7, 27)
        session = self._session(d)
        bars = [self._bar(d, 0), self._bar(d, 0)]
        result = validate_bars(bars, session, source="TEST")
        self.assertEqual(result.status, "DUPLICATE_BARS")

    def test_invalid_ohlc(self):
        d = date(2026, 7, 27)
        session = self._session(d)
        bad = self._bar(d, 0)
        bad = HistoricalBar(**{**bad.__dict__, "high": 1.0})
        result = validate_bars([bad], session, source="TEST")
        self.assertEqual(result.status, "INVALID_OHLC")

    def test_aggregate_1m_to_5m(self):
        d = date(2026, 7, 27)
        session = self._session(d)
        rows = []
        base = datetime.combine(d, time(9, 30))
        for i in range(10):
            ts = base + timedelta(minutes=i)
            rows.append({"TS": ts, "OPEN": 100, "HIGH": 101, "LOW": 99, "CLOSE": 100.5, "VOLUME": 10})
        bars = aggregate_1m_to_5m(rows, "AAPL", d, session)
        self.assertEqual(len(bars), 2)
        self.assertEqual(bars[0].bar_size_minutes, 5)


class BrooksPaaSelectionTests(unittest.TestCase):
    def test_newest_pre_rth_selected(self):
        trading_date = date(2026, 7, 28)
        cutoff = rth_open_utc_naive(trading_date)

        def fake_query(sql, params=()):
            return [
                {
                    "analysis_id": "old",
                    "symbol": "AAPL",
                    "created_at": cutoff - timedelta(hours=2),
                    "as_of_date": date(2026, 7, 27),
                    "verdict": "WAIT_PULLBACK",
                    "confidence": 0.7,
                    "geometry_summary_json": {},
                    "situation_model_json": {},
                    "methodologist_output_json": {"plain_explanation": "test"},
                    "card_ids": [],
                    "lookback_bars": 120,
                    "error_class": None,
                },
                {
                    "analysis_id": "newer",
                    "symbol": "AAPL",
                    "created_at": cutoff - timedelta(minutes=30),
                    "as_of_date": date(2026, 7, 27),
                    "verdict": "WAIT_PULLBACK",
                    "confidence": 0.8,
                    "geometry_summary_json": {},
                    "situation_model_json": {},
                    "methodologist_output_json": {"plain_explanation": "test"},
                    "card_ids": [],
                    "lookback_bars": 120,
                    "error_class": None,
                },
            ]

        import app.brooks_intraday.paa_selection as mod

        original = mod.query_rows
        mod.query_rows = fake_query
        try:
            sel = select_pre_rth_paa("AAPL", trading_date, rth_open_utc=cutoff)
            self.assertEqual(sel.analysis_id, "newer")
        finally:
            mod.query_rows = original

    def test_post_rth_rejected(self):
        trading_date = date(2026, 7, 28)
        cutoff = rth_open_utc_naive(trading_date)

        def fake_query(sql, params=()):
            return [
                {
                    "analysis_id": "late",
                    "symbol": "AAPL",
                    "created_at": cutoff + timedelta(minutes=5),
                    "as_of_date": date(2026, 7, 28),
                    "verdict": "WAIT_PULLBACK",
                    "confidence": 0.8,
                    "geometry_summary_json": {},
                    "situation_model_json": {},
                    "methodologist_output_json": {},
                    "card_ids": [],
                    "lookback_bars": 120,
                }
            ]

        import app.brooks_intraday.paa_selection as mod

        original = mod.query_rows
        mod.query_rows = fake_query
        try:
            sel = select_pre_rth_paa("AAPL", trading_date, rth_open_utc=cutoff)
            self.assertEqual(sel.status, "PAA_DOSSIER_NOT_AVAILABLE_BEFORE_RTH")
        finally:
            mod.query_rows = original


class BrooksCompilerTests(unittest.TestCase):
    def test_stable_source_hash(self):
        selection = PaaSelectionResult(
            symbol="AAPL",
            trading_date=date(2026, 7, 28),
            status="SELECTED",
            analysis_id="abc",
            scanned_at_utc=datetime(2026, 7, 28, 8, 0),
            as_of_date=date(2026, 7, 27),
            board_run_id=None,
            selection_rule="TEST",
            candidate_count=1,
            rejected=[],
            record={
                "verdict": "WAIT_PULLBACK",
                "confidence": 0.7,
                "geometry_summary_json": {
                    "support_zones": [{"low": 99, "high": 101}],
                    "resistance_zones": [{"low": 109, "high": 111}],
                    "structure": {"trend": "UPTREND", "highs": "HIGHER_HIGH", "lows": "HIGHER_LOW"},
                    "latest": {"close": 105},
                    "swings": [{"kind": "high", "price": 108}],
                },
                "situation_model_json": {"long_location": "CONSTRUCTIVE_PULLBACK"},
                "methodologist_output_json": {"plain_explanation": "Wait for pullback."},
                "card_ids": [],
            },
        )
        d1 = compile_frozen_dossier(run_id="r1", selection=selection, compiled_at_utc=datetime(2026, 7, 28, 8, 5))
        d2 = compile_frozen_dossier(run_id="r1", selection=selection, compiled_at_utc=datetime(2026, 7, 28, 8, 5))
        self.assertEqual(d1["source_hash"], d2["source_hash"])
        self.assertTrue(d1["observation_ready"])


class BrooksSafetyTests(unittest.TestCase):
    def test_no_live_imports_in_package(self):
        import app.brooks_intraday as pkg
        import app.brooks_intraday.service as svc

        self.assertFalse(hasattr(svc, "live"))
        self.assertIn("brooks_intraday", pkg.__name__)


if __name__ == "__main__":
    unittest.main()
