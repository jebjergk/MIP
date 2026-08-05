"""Phase 9A — check-tws endpoint and subprocess preflight isolation."""

from __future__ import annotations

import asyncio
import concurrent.futures
import json
import unittest
from unittest.mock import MagicMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.brooks_intraday.experiment_ib_preflight import run_ib_connection_preflight
from app.brooks_intraday.experiment_phase9_constants import TWS_CONNECTED, TWS_NOT_RUNNING, TWS_UNKNOWN
from app.brooks_intraday.experiment_phase9_tws import check_tws_connection
from app.brooks_intraday.router import router

_test_app = FastAPI()
_test_app.include_router(router)


def _preflight_ok_payload() -> dict:
    return {
        "status": "SUCCESS",
        "connected": True,
        "server_version": 176,
        "historical_data_capability": "contract_qualified",
        "managed_accounts_masked": ["****1234"],
        "managed_accounts_count": 1,
        "ib_messages": [],
        "ib_error_codes": [],
        "client_id_collision_suspected": False,
    }


class CheckTwsSubprocessTests(unittest.TestCase):
    @patch("app.brooks_intraday.experiment_ib_preflight.subprocess.run")
    @patch("app.brooks_intraday.experiment_ib_preflight._preflight_subprocess_paths")
    def test_preflight_uses_subprocess_not_ib_insync_in_caller_thread(self, mock_paths, mock_run):
        py = MagicMock()
        py.exists.return_value = True
        script = MagicMock()
        script.exists.return_value = True
        mock_paths.return_value = (MagicMock(), py, script)
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=json.dumps(_preflight_ok_payload()),
            stderr="",
        )

        def _worker_no_loop():
            asyncio.set_event_loop(None)
            return run_ib_connection_preflight()

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            result = pool.submit(_worker_no_loop).result(timeout=30)

        self.assertTrue(result.get("connected"))
        self.assertEqual(result.get("execution_path"), "subprocess")
        mock_run.assert_called_once()
        mod = __import__("app.brooks_intraday.experiment_ib_preflight", fromlist=["x"])
        src = open(mod.__file__, encoding="utf-8").read()
        self.assertNotIn("from ib_insync import", src)

    @patch("app.brooks_intraday.experiment_ib_preflight.subprocess.run")
    @patch("app.brooks_intraday.experiment_ib_preflight._preflight_subprocess_paths")
    def test_tws_unavailable_structured(self, mock_paths, mock_run):
        py = MagicMock()
        py.exists.return_value = True
        script = MagicMock()
        script.exists.return_value = True
        mock_paths.return_value = (MagicMock(), py, script)
        mock_run.return_value = MagicMock(
            returncode=1,
            stdout=json.dumps(
                {
                    "status": "SUCCESS",
                    "connected": False,
                    "ib_messages": ["Couldn't connect to TWS"],
                    "connection_refused": True,
                }
            ),
            stderr="",
        )
        tws = check_tws_connection()
        self.assertEqual(tws["readiness"], TWS_NOT_RUNNING)
        self.assertFalse(tws["connected"])
        self.assertIn("message", tws)
        self.assertTrue(tws.get("recoverable"))

    @patch("app.brooks_intraday.experiment_ib_preflight._preflight_subprocess_paths")
    def test_ib_runtime_missing(self, mock_paths):
        py = MagicMock()
        py.exists.return_value = False
        script = MagicMock()
        script.exists.return_value = False
        mock_paths.return_value = (MagicMock(), py, script)
        tws = check_tws_connection()
        self.assertFalse(tws["connected"])
        self.assertIn("message", tws)
        self.assertTrue(tws.get("recoverable"))

    @patch("app.brooks_intraday.experiment_ib_preflight.subprocess.run")
    @patch("app.brooks_intraday.experiment_ib_preflight._preflight_subprocess_paths")
    def test_no_raw_account_in_response(self, mock_paths, mock_run):
        py = MagicMock()
        py.exists.return_value = True
        script = MagicMock()
        script.exists.return_value = True
        mock_paths.return_value = (MagicMock(), py, script)
        payload = _preflight_ok_payload()
        payload["managed_accounts"] = ["DU1234567"]
        mock_run.return_value = MagicMock(returncode=0, stdout=json.dumps(payload), stderr="")
        tws = check_tws_connection()
        masked = tws.get("managed_accounts_masked") or []
        self.assertTrue(all("DU1234567" != m for m in masked))
        self.assertTrue(any(m.endswith("4567") or m == "****1234" for m in masked) or not masked)


class CheckTwsFastApiTests(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(_test_app)

    @patch("app.brooks_intraday.experiment_phase9_service.build_phase9_status")
    @patch("app.brooks_intraday.experiment_phase9_service.update_execution")
    @patch("app.brooks_intraday.experiment_phase9_service.append_event")
    @patch("app.brooks_intraday.experiment_phase9_service.ensure_execution_record")
    @patch("app.brooks_intraday.experiment_phase9_tws.run_ib_connection_preflight")
    def test_endpoint_returns_json_from_threadpool(
        self,
        mock_preflight,
        mock_ensure,
        _append,
        _update,
        mock_status,
    ):
        mock_ensure.return_value = {"execution_id": "e1", "overall_status": "READY"}
        mock_status.return_value = {"overall_status": "READY"}
        mock_preflight.return_value = _preflight_ok_payload()

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            fut = pool.submit(
                self.client.post,
                "/research/brooks-intraday/experiments/phase9/check-tws",
            )
            resp = fut.result(timeout=30)

        self.assertEqual(resp.headers.get("content-type", "").split(";")[0], "application/json")
        body = resp.json()
        self.assertIn("tws", body)
        self.assertEqual(body["tws"]["readiness"], TWS_CONNECTED)
        self.assertIn("host", body["tws"])

    @patch("app.brooks_intraday.experiment_phase9_service.post_check_tws")
    def test_endpoint_json_on_unhandled_service_error(self, mock_post):
        mock_post.side_effect = RuntimeError("There is no current event loop in thread 'AnyIO worker thread'")
        resp = self.client.post("/research/brooks-intraday/experiments/phase9/check-tws")
        self.assertEqual(resp.status_code, 503)
        body = resp.json()
        self.assertEqual(body["tws"]["readiness"], TWS_UNKNOWN)
        self.assertFalse(body["tws"]["connected"])
        self.assertIn("message", body["tws"])
        self.assertTrue(body["tws"].get("recoverable"))

    def test_preflight_modules_no_place_order(self):
        import app.brooks_intraday.experiment_ib_preflight as pre
        import app.brooks_intraday.experiment_phase9_tws as tws

        for mod in (pre, tws):
            text = open(mod.__file__, encoding="utf-8").read()
            self.assertNotIn("placeOrder", text)
            self.assertNotIn("reqOpenOrders", text)


if __name__ == "__main__":
    unittest.main()
