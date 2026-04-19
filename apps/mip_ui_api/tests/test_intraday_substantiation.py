import unittest
import unittest.mock
from datetime import datetime, timedelta

from app.committee.engine import LiveContext
from app.committee.intraday_substantiation import (
    MARKER_CAP,
    VERDICT_CHALLENGES,
    build_intraday_substantiation_artifact,
    fetch_intraday_bars_15m_ib,
)


def _bar(ts_off: int, o, h, l, c, v=1e6):
    base = datetime(2026, 4, 18, 14, 0, 0)
    ts = base + timedelta(minutes=15 * ts_off)
    return {
        "TS": ts,
        "OPEN": o,
        "HIGH": h,
        "LOW": l,
        "CLOSE": c,
        "VOLUME": v,
        "SOURCE": "TEST",
    }


class IntradaySubstantiationTests(unittest.TestCase):
    def test_returns_none_when_insufficient_bars(self):
        snap = {
            "SYMBOL": "TEST",
            "SIDE": "LONG",
            "SETUP_FAMILY": "TREND_PULLBACK_LONG",
            "ENTRY_ZONE_JSON": {"low": 99.0, "high": 101.0},
            "INVALIDATION_JSON": {"level": 95.0, "rule": "BELOW"},
        }
        live = LiveContext(
            latest_price=100.0,
            open_price=99.0,
            prior_close=98.0,
            structural_state_now="TREND_UP",
            trend_regime_now="UPTREND",
            vol_regime_now="NORMAL",
            bar_dates=["2026-04-18"],
        )
        self.assertIsNone(build_intraday_substantiation_artifact(snap, live, [], "2026-04-18T14:30:00"))
        self.assertIsNone(build_intraday_substantiation_artifact(snap, live, [_bar(0, 100, 100.5, 99.5, 100)], "2026-04-18T14:30:00"))

    def test_marker_cap_and_proposal_marker_in_window(self):
        snap = {
            "SYMBOL": "TEST",
            "SIDE": "LONG",
            "SETUP_FAMILY": "TREND_PULLBACK_LONG",
            "ENTRY_ZONE_JSON": {"low": 99.0, "high": 101.0},
            "INVALIDATION_JSON": {"level": 90.0, "rule": "BELOW"},
            "PROPOSAL_TS": datetime(2026, 4, 18, 14, 45, 0),
        }
        live = LiveContext(
            latest_price=100.5,
            open_price=99.0,
            prior_close=98.0,
            structural_state_now="TREND_UP",
            trend_regime_now="UPTREND",
            vol_regime_now="NORMAL",
            bar_dates=["2026-04-18"],
        )
        rows = [_bar(i, 100 + i * 0.05, 100.2 + i * 0.05, 99.8 + i * 0.05, 100 + i * 0.05) for i in range(12)]
        art = build_intraday_substantiation_artifact(snap, live, rows, snap["PROPOSAL_TS"])
        self.assertIsNotNone(art)
        payload = art["payload"]
        self.assertEqual(payload["headline"], "Expected vs observed today")
        self.assertLessEqual(len(payload["markers"]), MARKER_CAP)
        pm = payload.get("proposal_marker")
        self.assertIsNotNone(pm)
        self.assertIn("bar_index", pm)

    def test_breach_yields_challenge_verdict(self):
        snap = {
            "SYMBOL": "TEST",
            "SIDE": "LONG",
            "SETUP_FAMILY": "X",
            "ENTRY_ZONE_JSON": {"low": 99.0, "high": 101.0},
            "INVALIDATION_JSON": {"level": 100.5, "rule": "BELOW"},
        }
        live = LiveContext(
            latest_price=99.0,
            open_price=100.0,
            prior_close=99.0,
            structural_state_now="TREND_UP",
            trend_regime_now="UPTREND",
            vol_regime_now="NORMAL",
            bar_dates=["2026-04-18"],
        )
        rows = [_bar(0, 101, 101, 100, 101), _bar(1, 101, 101, 99, 99)]
        art = build_intraday_substantiation_artifact(snap, live, rows, None)
        self.assertIsNotNone(art)
        self.assertEqual(art["payload"]["captions"]["verdict_bucket"], VERDICT_CHALLENGES)

    def test_fetch_ib_maps_bars_oldest_first(self):
        ib_out = {
            "status": "SUCCESS",
            "symbols": [
                {
                    "symbol": "ABC",
                    "status": "SUCCESS",
                    "bars": [
                        {"ts": "2026-04-18T15:00:00", "open": 1, "high": 1.1, "low": 0.9, "close": 1.05, "volume": 100},
                        {"ts": "2026-04-18T15:15:00", "open": 1.05, "high": 1.06, "low": 1.0, "close": 1.02, "volume": 110},
                    ],
                }
            ],
        }
        with unittest.mock.patch(
            "app.committee.intraday_substantiation.run_agent_ibkr_live_bars",
            return_value=ib_out,
        ):
            out = fetch_intraday_bars_15m_ib("ABC", "STOCK")
        self.assertEqual(len(out), 2)
        self.assertEqual(out[0]["CLOSE"], 1.05)
        self.assertEqual(out[1]["CLOSE"], 1.02)
        self.assertEqual(out[0]["SOURCE"], "IBKR_DIRECT")

    def test_fetch_ib_returns_empty_on_http_exception(self):
        from fastapi import HTTPException

        with unittest.mock.patch(
            "app.committee.intraday_substantiation.run_agent_ibkr_live_bars",
            side_effect=HTTPException(status_code=502, detail="ib down"),
        ):
            out = fetch_intraday_bars_15m_ib("ABC")
        self.assertEqual(out, [])


if __name__ == "__main__":
    unittest.main()
