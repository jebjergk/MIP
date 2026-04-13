"""
Bridge repo `cursorfiles/ibkr_host_config` into mip_ui_api (adds cursorfiles to sys.path once).
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

_CURSORFILES: Path | None = None


def _cursorfiles_dir() -> Path:
    global _CURSORFILES
    if _CURSORFILES is not None:
        return _CURSORFILES
    # .../MIP/apps/mip_ui_api/app/integrations/this.py -> parents[5] = repo root (mip_0.7)
    here = Path(__file__).resolve()
    root = here.parents[5]
    cf = root / "cursorfiles"
    if str(cf) not in sys.path:
        sys.path.insert(0, str(cf))
    _CURSORFILES = cf
    return cf


def _load_config():
    _cursorfiles_dir()
    import ibkr_host_config as _c  # type: ignore

    return _c


def ensure_read_client_ids_valid() -> None:
    c = _load_config()
    c.validate_read_client_ids_no_collision()


def get_snapshot_sync_params() -> dict[str, Any]:
    c = _load_config()
    c.validate_read_client_ids_no_collision()
    ep = c.resolve_snapshot_read()
    return {"host": ep.host, "port": ep.port, "client_id": ep.client_id}


def get_live_bars_subprocess_args() -> dict[str, Any]:
    c = _load_config()
    c.validate_read_client_ids_no_collision()
    ep = c.resolve_live_bars_read()
    return {
        "host": ep.host,
        "port": ep.port,
        "client_id": ep.client_id,
        "connect_timeout_sec": 10,
    }


def diagnostics_template(surface: str) -> dict[str, Any]:
    c = _load_config()
    return c.ib_host_diagnostics_template(surface)


def merge_diagnostics(base: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
    c = _load_config()
    return c.merge_diagnostics(base, **kwargs)


def diagnostics_live_bars_subprocess_result(
    *,
    surface_name: str,
    subprocess_ok: bool,
    payload_status: str | None,
    endpoint: Any | None = None,
) -> dict[str, Any]:
    """§5.2 LI / Living Charts: freshness live iff current IB fetch succeeded."""
    c = _load_config()
    base = c.ib_host_diagnostics_template(surface_name)
    ep = endpoint or c.resolve_live_bars_read()
    ok = subprocess_ok and (payload_status or "").upper() in ("SUCCESS", "PARTIAL_FAILURE")
    # PARTIAL_FAILURE still had a connection; treat surface as stale for strict live
    strict_live = subprocess_ok and (payload_status or "").upper() == "SUCCESS"
    fresh = "live" if strict_live else ("stale" if subprocess_ok else "unavailable")
    reason = None
    if not subprocess_ok:
        reason = "subprocess_failed_or_timeout"
    elif not strict_live:
        reason = "partial_or_non_success_bar_status"
    return c.merge_diagnostics(
        base,
        effective_host=ep.host,
        effective_port=ep.port,
        effective_client_id=ep.client_id,
        transport_state="connected" if subprocess_ok else "disconnected",
        socket_connected=subprocess_ok,
        api_ready=subprocess_ok,
        surface_freshness=fresh,
        surface_freshness_reason=reason,
        tape_transport_state="not_applicable",
        tape_operational_validity=None,
        tape_operational_reason_code=None,
    )


def diagnostics_live_portfolio_overview(
    *,
    snapshot_state: str,
    latest_snapshot_ts: Any,
    threshold_sec: Any,
) -> dict[str, Any]:
    """§5.2 Live Portfolio: live iff snapshot sync data fresh vs threshold (uses existing snapshot_state)."""
    c = _load_config()
    base = c.ib_host_diagnostics_template(c.SURFACE_LIVE_PORTFOLIO)
    ep = c.resolve_snapshot_read()
    fresh_states = ("FRESH", "AGING")
    fresh = snapshot_state in fresh_states
    surface_fresh = "live" if fresh else ("unavailable" if snapshot_state == "BLOCKED" else "stale")
    reason = None
    if snapshot_state not in fresh_states:
        reason = f"snapshot_state={snapshot_state}"
    return c.merge_diagnostics(
        base,
        effective_host=ep.host,
        effective_port=ep.port,
        effective_client_id=ep.client_id,
        transport_state="unknown",
        socket_connected=False,
        api_ready=False,
        surface_freshness=surface_fresh,
        surface_freshness_reason=reason,
        tape_transport_state="not_applicable",
        tape_operational_validity=None,
        tape_operational_reason_code=None,
    )
