"""Phase 9A — TWS readiness for historical data only (no orders)."""

from __future__ import annotations

import re
import time
from datetime import datetime, timezone
from typing import Any

from .experiment_ib_preflight import phase9_effective_ib_config, run_ib_connection_preflight
from .experiment_phase9_constants import (
    TWS_API_DISABLED,
    TWS_AUTH_REQUIRED,
    TWS_CLIENT_IN_USE,
    TWS_CONNECTED,
    TWS_HIST_UNAVAILABLE,
    TWS_NOT_RUNNING,
    TWS_PORT_UNAVAILABLE,
    TWS_UNKNOWN,
)


def classify_tws_preflight(preflight: dict[str, Any]) -> str:
    if preflight.get("connected") and preflight.get("historical_data_capability") == "contract_qualified":
        return TWS_CONNECTED
    msgs = " ".join(str(m) for m in (preflight.get("ib_messages") or []))
    if preflight.get("client_id_collision_suspected") or "326" in msgs or "already in use" in msgs.lower():
        return TWS_CLIENT_IN_USE
    if preflight.get("connection_refused") or "502" in msgs or "Couldn't connect" in msgs:
        return TWS_NOT_RUNNING
    if "504" in msgs or "Not connected" in msgs:
        return TWS_NOT_RUNNING
    if "port" in msgs.lower() and "refused" in msgs.lower():
        return TWS_PORT_UNAVAILABLE
    if "10197" in msgs or "competing" in msgs.lower():
        return TWS_CLIENT_IN_USE
    if "10168" in msgs or "market data" in msgs.lower() and "disabled" in msgs.lower():
        return TWS_API_DISABLED
    if "auth" in msgs.lower() or "login" in msgs.lower():
        return TWS_AUTH_REQUIRED
    if preflight.get("connected") and preflight.get("historical_data_capability") != "contract_qualified":
        return TWS_HIST_UNAVAILABLE
    if not preflight.get("connected"):
        code_match = re.search(r"error\s*(\d+)", msgs, re.I)
        if code_match and code_match.group(1) == "502":
            return TWS_NOT_RUNNING
        return TWS_NOT_RUNNING
    return TWS_UNKNOWN


def check_tws_connection(*, persist_probe: bool = True) -> dict[str, Any]:
    cfg = phase9_effective_ib_config()
    t0 = time.perf_counter()
    preflight = run_ib_connection_preflight()
    readiness = classify_tws_preflight(preflight)
    out: dict[str, Any] = {
        "readiness": readiness,
        "host": cfg.get("host"),
        "port": cfg.get("port"),
        "historical_client_id": cfg.get("historical_subprocess_client_id"),
        "server_version": preflight.get("server_version"),
        "managed_accounts_masked": preflight.get("managed_accounts_masked") or [],
        "connected": bool(preflight.get("connected")),
        "historical_data_capability": preflight.get("historical_data_capability"),
        "ib_error_codes": preflight.get("ib_error_codes") or [],
        "ib_messages": preflight.get("ib_messages") or [],
        "client_id_collision_suspected": preflight.get("client_id_collision_suspected"),
        "duration_sec": round(time.perf_counter() - t0, 3),
        "checked_at_utc": datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec="seconds"),
    }
    if readiness == TWS_CONNECTED:
        out["last_successful_probe_at_utc"] = out["checked_at_utc"]
    return out
