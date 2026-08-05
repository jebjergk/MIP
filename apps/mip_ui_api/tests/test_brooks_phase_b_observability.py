"""Phase B — activity ordering and stage-progress helpers."""

from __future__ import annotations

import unittest
from datetime import datetime
from unittest.mock import patch

from app.brooks_intraday.experiment_phase9_service import get_phase9_events
from app.brooks_intraday.experiment_progress import build_stage_progress


class ActivityOrderingTests(unittest.TestCase):
    def test_events_sorted_by_event_timestamp_utc(self):
        raw = [
            {
                "event_ts": datetime(2026, 8, 3, 20, 46, 11),
                "severity": "ERROR",
                "stage": "SIMULATION_REPLAY",
                "message": "later",
                "detail_json": {},
            },
            {
                "event_ts": datetime(2026, 8, 3, 19, 37, 38),
                "severity": "ERROR",
                "stage": "CONTEXT_REPLAY",
                "message": "earlier",
                "detail_json": {},
            },
        ]
        with patch(
            "app.brooks_intraday.experiment_phase9_service.ensure_execution_record",
            return_value={"execution_id": "e1"},
        ), patch(
            "app.brooks_intraday.experiment_phase9_service.list_events",
            return_value=raw,
        ):
            out = get_phase9_events(limit=10)
        events = out["events"]
        self.assertEqual(len(events), 2)
        self.assertTrue(all("event_timestamp_utc" in e for e in events))
        self.assertLessEqual(events[0]["event_timestamp_utc"], events[1]["event_timestamp_utc"])
        self.assertEqual(events[0]["message"], "earlier")
        self.assertEqual(events[1]["message"], "later")


class ProgressMonotonicTests(unittest.TestCase):
    def test_progress_completed_not_above_total(self):
        sp = build_stage_progress(stage="CONTEXT_REPLAY", completed=1560, total=1560)
        self.assertEqual(sp["completed"], sp["total"])
        sp2 = build_stage_progress(stage="CONTEXT_REPLAY", completed=100, total=1560)
        self.assertLess(sp2["completed"], sp2["total"])


if __name__ == "__main__":
    unittest.main()
