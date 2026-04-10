"""Phase 3 — append-only JSONL replay (optional)."""

from __future__ import annotations

import json
import os
import threading
import time
from typing import Any


class ReplayStore:
    def __init__(self) -> None:
        self.path = (os.getenv("TAPE_REPLAY_JSONL") or "").strip()
        self.anomaly_path = (os.getenv("TAPE_REPLAY_ANOMALY_JSONL") or "").strip()
        self._lock = threading.Lock()
        self._last_periodic: dict[str, float] = {}
        self._last_anomaly: dict[str, float] = {}
        try:
            self.interval = float((os.getenv("TAPE_REPLAY_INTERVAL_SEC") or "5").strip() or "5")
        except ValueError:
            self.interval = 5.0
        try:
            self.anomaly_cooldown = float((os.getenv("TAPE_REPLAY_ANOMALY_COOLDOWN_SEC") or "12").strip() or "12")
        except ValueError:
            self.anomaly_cooldown = 12.0

    def record(self, sym: str, snap: dict[str, Any]) -> None:
        if not self.path and not self.anomaly_path:
            return
        now_ts = time.time()
        mq = str(snap.get("move_quality") or "")
        burst = float(snap.get("burst_score") or 0.0)

        if self.path:
            last = self._last_periodic.get(sym, 0.0)
            if now_ts - last >= self.interval:
                self._last_periodic[sym] = now_ts
                row = {
                    "ts": snap.get("snapshot_ts"),
                    "symbol": sym,
                    "move_quality": mq,
                    "burst_score": snap.get("burst_score"),
                    "tape_pressure_score": snap.get("tape_pressure_score"),
                    "session_regime": snap.get("session_regime"),
                }
                with self._lock:
                    with open(self.path, "a", encoding="utf-8") as fh:
                        fh.write(json.dumps(row, default=str) + "\n")

        if self.anomaly_path:
            anomaly = (
                burst > 0.72
                or mq.startswith("vacuum")
                or mq.startswith("absorption")
                or mq.startswith("exhaustion")
            )
            if anomaly and now_ts - self._last_anomaly.get(sym, 0.0) >= self.anomaly_cooldown:
                self._last_anomaly[sym] = now_ts
                with self._lock:
                    with open(self.anomaly_path, "a", encoding="utf-8") as fh:
                        fh.write(json.dumps({"ts": snap.get("snapshot_ts"), "symbol": sym, "snapshot": snap}, default=str) + "\n")


replay_store = ReplayStore()
