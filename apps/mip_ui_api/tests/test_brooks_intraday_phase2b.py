import ast
import unittest
from datetime import date, datetime, time
from pathlib import Path
from unittest.mock import MagicMock, patch

from app.brooks_intraday.acquisition import acquire_historical_bars
from app.brooks_intraday.bars import HistoricalBar, validate_bars
from app.brooks_intraday.calendar import TradingSession
from app.brooks_intraday.constants import EXPECTED_RTH_5M_BARS_FULL_SESSION
from app.brooks_intraday.ib_historical_provider import IbHistoricalProviderError
from app.brooks_intraday.interval_validation import (
    TIMESTAMP_CONVENTION,
    expected_interval_starts_ny,
)
from app.brooks_intraday.readiness import compute_readiness_labels


class BrooksPhase2BValidationTests(unittest.TestCase):
    def _session(self, d: date) -> TradingSession:
        return TradingSession(
            trading_date=d,
            session_open_utc=datetime.combine(d, time(13, 30)),
            session_close_utc=datetime.combine(d, time(20, 0)),
            session_open_ny=datetime.combine(d, time(9, 30)),
            session_close_ny=datetime.combine(d, time(16, 0)),
            is_early_close=False,
            expected_5m_bars=EXPECTED_RTH_5M_BARS_FULL_SESSION,
        )

    def _bars(self, d: date) -> list[HistoricalBar]:
        out = []
        for ny in expected_interval_starts_ny(d):
            out.append(
                HistoricalBar(
                    symbol="AAPL",
                    ts_utc=ny,
                    ts_ny=ny,
                    trading_date=d,
                    open=100,
                    high=101,
                    low=99,
                    close=100.5,
                    volume=100,
                    source="TEST",
                    bar_size_minutes=5,
                    rth=True,
                )
            )
        return out

    def test_complete_interval_set(self):
        d = date(2026, 7, 27)
        result = validate_bars(self._bars(d), self._session(d), source="TEST")
        self.assertEqual(result.status, "COMPLETE")
        self.assertEqual(result.bar_count, 78)
        self.assertIsNotNone(result.dataset_hash)
        self.assertEqual(result.timestamp_convention, TIMESTAMP_CONVENTION)

    def test_78_rows_with_missing_and_duplicate_interval(self):
        d = date(2026, 7, 27)
        bars = self._bars(d)
        dup = bars[1]
        bars[10] = HistoricalBar(
            symbol=dup.symbol,
            ts_utc=dup.ts_utc,
            ts_ny=dup.ts_ny,
            trading_date=dup.trading_date,
            open=dup.open,
            high=dup.high,
            low=dup.low,
            close=dup.close,
            volume=dup.volume,
            source=dup.source,
            bar_size_minutes=dup.bar_size_minutes,
            rth=dup.rth,
        )
        bars = bars[:77]
        result = validate_bars(bars, self._session(d), source="TEST")
        self.assertNotEqual(result.status, "COMPLETE")

    def test_interval_start_convention_documented(self):
        d = date(2026, 7, 27)
        starts = expected_interval_starts_ny(d)
        self.assertEqual(starts[0], datetime.combine(d, time(9, 30)))
        self.assertEqual(starts[-1], datetime.combine(d, time(15, 55)))
        self.assertEqual(len(starts), 78)

    def test_wrong_session_timestamps_rejected(self):
        d = date(2026, 7, 27)
        wrong_day = date(2026, 7, 28)
        bars = []
        for ny in expected_interval_starts_ny(wrong_day):
            bars.append(
                HistoricalBar(
                    symbol="AAPL",
                    ts_utc=ny,
                    ts_ny=ny,
                    trading_date=d,
                    open=100,
                    high=101,
                    low=99,
                    close=100.5,
                    volume=100,
                    source="TEST",
                    bar_size_minutes=5,
                    rth=True,
                )
            )
        result = validate_bars(bars, self._session(d), source="TEST")
        self.assertNotEqual(result.status, "COMPLETE")

    def test_dossier_ready_replay_blocked(self):
        state = {
            "preparation": {
                "counts": {
                    "dossiers_expected": 20,
                    "dossiers_compiled": 20,
                    "bar_sessions_expected": 20,
                    "bar_sessions_complete": 0,
                },
                "fatal_errors": [],
            }
        }
        labels = compute_readiness_labels(state)
        self.assertEqual(labels["dossier_readiness"], "READY")
        self.assertEqual(labels["historical_bar_readiness"], "INCOMPLETE")
        self.assertEqual(labels["replay_readiness"], "BLOCKED")

    def test_all_sessions_complete_replay_ready(self):
        state = {
            "bars_frozen": True,
            "preparation": {
                "counts": {
                    "dossiers_expected": 20,
                    "dossiers_compiled": 20,
                    "bar_sessions_expected": 20,
                    "bar_sessions_complete": 20,
                },
                "fatal_errors": [],
            },
        }
        labels = compute_readiness_labels(state)
        self.assertEqual(labels["replay_readiness"], "READY")


class BrooksPhase2BAcquisitionTests(unittest.TestCase):
    def _minimal_state(self) -> dict:
        return {
            "symbols": ["AAPL"],
            "selected_week_start": date(2026, 7, 27),
            "preparation": {"counts": {}, "matrix": []},
        }

    def test_ib_provider_error_recorded(self):
        state = self._minimal_state()

        def fail_fetch(sym, td):
            raise IbHistoricalProviderError("IB_HISTORICAL_FAILED", "connection refused")

        with patch("app.brooks_intraday.acquisition.validate_stored_session") as vss:
            vss.return_value = {"status": "NO_DATA", "bar_count": 0}
            with patch("app.brooks_intraday.acquisition.persist_bars") as persist:
                out = acquire_historical_bars(state, ib_fetch=fail_fetch)
                persist.assert_not_called()
        self.assertEqual(out["sessions_requested"], 5)
        self.assertEqual(out["sessions_acquired"], 0)
        self.assertEqual(len(out["sessions_failed"]), 5)

    def test_partial_ib_response_not_persisted(self):
        state = self._minimal_state()
        d = date(2026, 7, 27)

        def partial(sym, td):
            return {
                "status": "SUCCESS",
                "symbol": sym,
                "bars": [{"ts": "2026-07-27 09:30:00", "open": 1, "high": 2, "low": 0.5, "close": 1.5, "volume": 10}],
                "source_request_id": "test",
            }

        with patch("app.brooks_intraday.acquisition.validate_stored_session") as vss:
            vss.return_value = {"status": "NO_DATA", "bar_count": 0}
            with patch("app.brooks_intraday.acquisition.persist_bars") as persist:
                out = acquire_historical_bars(state, ib_fetch=partial)
                persist.assert_not_called()
        self.assertEqual(out["sessions_acquired"], 0)

    def test_idempotent_skips_complete_sessions(self):
        state = self._minimal_state()
        fetch = MagicMock()

        with patch("app.brooks_intraday.acquisition.validate_stored_session") as vss:
            vss.return_value = {"status": "COMPLETE", "bar_count": 78}
            out = acquire_historical_bars(state, ib_fetch=fetch)
        fetch.assert_not_called()
        self.assertEqual(out["sessions_requested"], 0)

    def test_frozen_dataset_blocks_acquire(self):
        state = self._minimal_state()
        state["bars_frozen"] = True
        out = acquire_historical_bars(state, ib_fetch=MagicMock())
        self.assertEqual(out["status"], "ALREADY_FROZEN")


class BrooksPhase2BSafetyTests(unittest.TestCase):
    FORBIDDEN_IB_IMPORTS = (
        "ib_insync.Order",
        "ib_insync.MarketOrder",
        "ib_insync.LimitOrder",
        "placeOrder",
        "reqOpenOrders",
        "reqPositions",
        "reqAccountSummary",
    )

    def test_brooks_modules_avoid_ib_order_portfolio_apis(self):
        root = Path(__file__).resolve().parents[1] / "app" / "brooks_intraday"
        offenders: list[str] = []
        for path in root.glob("*.py"):
            text = path.read_text(encoding="utf-8")
            for token in self.FORBIDDEN_IB_IMPORTS:
                if token in text:
                    offenders.append(f"{path.name}: {token}")
        self.assertEqual(offenders, [], msg=f"Forbidden IB trading APIs referenced: {offenders}")

    def test_ib_historical_provider_uses_subprocess_only(self):
        path = Path(__file__).resolve().parents[1] / "app" / "brooks_intraday" / "ib_historical_provider.py"
        tree = ast.parse(path.read_text(encoding="utf-8"))
        imports = []
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                imports.append(node.module)
        self.assertIn("app.services.ibkr_live_bars", imports)
        self.assertNotIn("ib_insync", imports)


if __name__ == "__main__":
    unittest.main()
