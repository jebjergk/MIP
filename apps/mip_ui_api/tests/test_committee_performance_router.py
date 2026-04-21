"""Read-only router tests for /committee-performance/*.

We patch `get_connection` and `fetch_all` inside
`app.routers.committee_performance` so no Snowflake connection is required.
A bare FastAPI app with just the router is mounted to keep imports light.
"""
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from app.routers import committee_performance  # noqa: E402


def _make_client():
    app = FastAPI()
    app.include_router(committee_performance.router)
    return TestClient(app)


class _FakeCursor:
    """Captures executed SQL + params and returns canned rows."""

    def __init__(self, rows):
        self._rows = rows
        self.last_sql = None
        self.last_params = None

    def execute(self, sql, params=None):
        self.last_sql = sql
        self.last_params = params

    def close(self):
        pass


def _stub_connection(cursor):
    conn = MagicMock()
    conn.cursor.return_value = cursor
    conn.close.return_value = None
    return conn


class TestScorecardEndpoint(unittest.TestCase):
    def test_returns_first_row_payload(self):
        row = {
            "TOTAL_OPPORTUNITIES": 23,
            "SCORED_OPPORTUNITIES": 23,
            "EXCLUDED_OPPORTUNITIES": 0,
            "REAL_HIT_RATE": 0.5,
            "SHADOW_HIT_RATE": 0.4,
        }
        cursor = _FakeCursor([row])
        with patch.object(committee_performance, "get_connection",
                          return_value=_stub_connection(cursor)), \
             patch.object(committee_performance, "fetch_all", return_value=[row]):
            client = _make_client()
            resp = client.get("/committee-performance/scorecard")
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body["TOTAL_OPPORTUNITIES"], 23)
        self.assertEqual(body["REAL_HIT_RATE"], 0.5)

    def test_returns_empty_when_no_rows(self):
        cursor = _FakeCursor([])
        with patch.object(committee_performance, "get_connection",
                          return_value=_stub_connection(cursor)), \
             patch.object(committee_performance, "fetch_all", return_value=[]):
            client = _make_client()
            resp = client.get("/committee-performance/scorecard")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json(), {})


class TestMtdEndpoint(unittest.TestCase):
    def test_returns_recommendation(self):
        row = {
            "MTD_RECOMMENDATION": "PREFER_REAL",
            "REAL_AVG_RETURN": 0.012,
            "SHADOW_AVG_RETURN": 0.009,
            "AVG_RETURN_DELTA_REAL_MINUS_SHADOW": 0.003,
        }
        cursor = _FakeCursor([row])
        with patch.object(committee_performance, "get_connection",
                          return_value=_stub_connection(cursor)), \
             patch.object(committee_performance, "fetch_all", return_value=[row]):
            client = _make_client()
            resp = client.get("/committee-performance/mtd")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["MTD_RECOMMENDATION"], "PREFER_REAL")


class TestLabelsEndpoint(unittest.TestCase):
    def test_wraps_rows(self):
        rows = [
            {"COMPARISON_LABEL": "REAL_WIN__SHADOW_CASH", "N": 4},
            {"COMPARISON_LABEL": "REAL_LOSS__SHADOW_LOSS", "N": 2},
        ]
        cursor = _FakeCursor(rows)
        with patch.object(committee_performance, "get_connection",
                          return_value=_stub_connection(cursor)), \
             patch.object(committee_performance, "fetch_all", return_value=rows):
            client = _make_client()
            resp = client.get("/committee-performance/labels")
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertIn("rows", body)
        self.assertEqual(len(body["rows"]), 2)
        self.assertEqual(body["rows"][0]["COMPARISON_LABEL"], "REAL_WIN__SHADOW_CASH")


class TestOpportunitiesEndpoint(unittest.TestCase):
    def _run(self, query):
        cursor = _FakeCursor([])
        with patch.object(committee_performance, "get_connection",
                          return_value=_stub_connection(cursor)), \
             patch.object(committee_performance, "fetch_all", return_value=[]):
            client = _make_client()
            resp = client.get("/committee-performance/opportunities" + query)
        return resp, cursor

    def test_no_filters_no_where_clause(self):
        resp, cursor = self._run("")
        self.assertEqual(resp.status_code, 200)
        self.assertNotIn(" WHERE ", cursor.last_sql)
        self.assertEqual(cursor.last_params, [])
        self.assertIn("ORDER BY FIRST_DECISION_TS DESC", cursor.last_sql)

    def test_disagreements_only_adds_clause(self):
        _, cursor = self._run("?disagreements_only=true")
        self.assertIn("BOARDS_DISAGREE_NORMALIZED", cursor.last_sql)
        self.assertIn("BOARDS_DISAGREE_RAW", cursor.last_sql)

    def test_excluded_only_adds_clause(self):
        _, cursor = self._run("?excluded_only=true")
        self.assertIn("REAL_EXCLUDED", cursor.last_sql)
        self.assertIn("SHADOW_EXCLUDED", cursor.last_sql)

    def test_symbol_param_uppercased(self):
        _, cursor = self._run("?symbol=rivn")
        self.assertIn("SYMBOL = %s", cursor.last_sql)
        self.assertEqual(cursor.last_params, ["RIVN"])

    def test_label_filter_uppercased(self):
        _, cursor = self._run("?comparison_label=real_win__shadow_cash")
        self.assertIn("COMPARISON_LABEL = %s", cursor.last_sql)
        self.assertEqual(cursor.last_params, ["REAL_WIN__SHADOW_CASH"])

    def test_combined_filters(self):
        _, cursor = self._run("?disagreements_only=true&symbol=msft&comparison_label=tie_case")
        self.assertIn("BOARDS_DISAGREE_NORMALIZED", cursor.last_sql)
        self.assertIn("SYMBOL = %s", cursor.last_sql)
        self.assertIn("COMPARISON_LABEL = %s", cursor.last_sql)
        self.assertEqual(cursor.last_params, ["MSFT", "TIE_CASE"])


class TestOpportunityDetailEndpoint(unittest.TestCase):
    def test_404_when_no_rows(self):
        cursor = _FakeCursor([])
        with patch.object(committee_performance, "get_connection",
                          return_value=_stub_connection(cursor)), \
             patch.object(committee_performance, "fetch_all", return_value=[]):
            client = _make_client()
            resp = client.get("/committee-performance/opportunity/9999")
        self.assertEqual(resp.status_code, 404)

    def test_groups_rows_under_boards(self):
        rows = [
            {
                "LATCH_ID": 1, "PROPOSAL_ID": 304, "BOARD_KIND": "REAL",
                "RAW_STANCE": "APPROVE", "NORMALIZED_ACTION": "ENTER",
                "CONFIG_STATUS": "SCORABLE", "RAW_OUTCOME": "TP_HIT",
                "REALIZED_RETURN_PCT": 0.024,
            },
            {
                "LATCH_ID": 2, "PROPOSAL_ID": 304, "BOARD_KIND": "SHADOW",
                "RAW_STANCE": "DEFER", "NORMALIZED_ACTION": "CASH",
                "CONFIG_STATUS": "NOT_APPLICABLE", "RAW_OUTCOME": "CASH",
                "REALIZED_RETURN_PCT": None,
            },
        ]
        cursor = _FakeCursor(rows)
        with patch.object(committee_performance, "get_connection",
                          return_value=_stub_connection(cursor)), \
             patch.object(committee_performance, "fetch_all", return_value=rows):
            client = _make_client()
            resp = client.get("/committee-performance/opportunity/304")
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body["proposal_id"], 304)
        self.assertEqual(len(body["boards"]), 2)
        kinds = sorted(b["BOARD_KIND"] for b in body["boards"])
        self.assertEqual(kinds, ["REAL", "SHADOW"])


class TestDisagreementsEndpoint(unittest.TestCase):
    def test_returns_rows(self):
        rows = [{"PROPOSAL_ID": 1, "COMPARISON_LABEL": "REAL_WIN__SHADOW_CASH"}]
        cursor = _FakeCursor(rows)
        with patch.object(committee_performance, "get_connection",
                          return_value=_stub_connection(cursor)), \
             patch.object(committee_performance, "fetch_all", return_value=rows):
            client = _make_client()
            resp = client.get("/committee-performance/disagreements")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(resp.json()["rows"]), 1)


if __name__ == "__main__":
    unittest.main()
