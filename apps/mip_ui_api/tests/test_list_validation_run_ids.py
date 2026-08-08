"""Validation run listing for review catalog (CONFIG_JSON variant path)."""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from app.brooks_intraday.experiment_analytics import list_validation_run_ids


class ListValidationRunIdsTests(unittest.TestCase):
    @patch("app.brooks_intraday.experiment_analytics.get_connection")
    def test_uses_variant_experiment_role_predicate(self, mock_conn):
        cur = MagicMock()
        cur.description = [
            ("RUN_ID",),
            ("SELECTED_WEEK_START",),
            ("STATUS",),
            ("CONFIG_JSON",),
            ("STARTING_CASH",),
            ("ENDING_CASH",),
            ("REALIZED_PNL",),
        ]
        cur.fetchall.return_value = []
        conn = MagicMock()
        conn.cursor.return_value = cur
        mock_conn.return_value = conn

        list_validation_run_ids()

        sql = cur.execute.call_args[0][0]
        self.assertIn("experiment_role", sql)
        self.assertNotIn("LIKE %s", sql)
        self.assertIn("UNSEEN_VALIDATION", sql)


if __name__ == "__main__":
    unittest.main()
