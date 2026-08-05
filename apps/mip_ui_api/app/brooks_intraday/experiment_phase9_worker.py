"""Phase 9A background worker — polls Snowflake execution queue (historical data only)."""

from __future__ import annotations

import logging
import threading
import time

from .experiment_execution_repository import (
    get_active_execution,
    release_lease,
    try_claim_lease,
)
from .experiment_phase9_constants import (
    ACTION_PAUSE,
    LEASE_SEC,
    OVERALL_COMPLETED,
    OVERALL_FAILED_RECOVERABLE,
    OVERALL_PAUSED,
    OVERALL_READY,
    OVERALL_RUNNING,
    OVERALL_WAITING_TWS,
    WORKER_POLL_SEC,
)
from .experiment_phase9_engine import _apply_pause_if_requested, execute_work_unit
from .experiment_phase9_service import startup_recovery, worker_owner_id

logger = logging.getLogger(__name__)

_worker_thread: threading.Thread | None = None
_stop = threading.Event()


def _worker_loop() -> None:
    owner = worker_owner_id()
    while not _stop.is_set():
        try:
            ex = get_active_execution()
            if not ex:
                time.sleep(WORKER_POLL_SEC)
                continue
            status = ex.get("overall_status")
            if status in (OVERALL_READY, OVERALL_PAUSED, OVERALL_COMPLETED, OVERALL_FAILED_RECOVERABLE) and not ex.get(
                "requested_action"
            ):
                time.sleep(WORKER_POLL_SEC)
                continue
            if status == OVERALL_WAITING_TWS:
                time.sleep(WORKER_POLL_SEC)
                continue
            eid = ex["execution_id"]
            if not try_claim_lease(eid, owner, LEASE_SEC):
                time.sleep(WORKER_POLL_SEC)
                continue
            try:
                ex = get_active_execution() or ex
                if _apply_pause_if_requested(ex, owner):
                    continue
                if ex.get("requested_action") == ACTION_PAUSE:
                    from .experiment_execution_repository import update_execution

                    update_execution(eid, requested_action=None)
                    ex = get_active_execution() or ex
                    if _apply_pause_if_requested(ex, owner):
                        continue
                elif ex.get("requested_action"):
                    from .experiment_execution_repository import update_execution

                    update_execution(eid, requested_action=None, overall_status=OVERALL_RUNNING)
                    ex = get_active_execution() or ex
                if ex.get("overall_status") in (OVERALL_RUNNING, OVERALL_FAILED_RECOVERABLE) or ex.get(
                    "requested_action"
                ):
                    execute_work_unit(ex, owner)
            finally:
                release_lease(eid, owner)
        except Exception as exc:
            logger.exception("phase9 worker tick failed: %s", exc)
        time.sleep(WORKER_POLL_SEC)


def start_phase9_worker() -> None:
    global _worker_thread
    if _worker_thread and _worker_thread.is_alive():
        return
    startup_recovery()
    _stop.clear()
    _worker_thread = threading.Thread(target=_worker_loop, name="brooks-phase9-worker", daemon=True)
    _worker_thread.start()
    logger.info("Brooks Phase 9A experiment worker started")


def stop_phase9_worker() -> None:
    _stop.set()
