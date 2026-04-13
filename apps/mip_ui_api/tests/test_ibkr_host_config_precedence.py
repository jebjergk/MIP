"""Gateway backward-compat + precedence for ibkr_host_config (via repo cursorfiles)."""

from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[4]
_CF = _REPO / "cursorfiles"
if str(_CF) not in sys.path:
    sys.path.insert(0, str(_CF))


def _reload_config(monkeypatch, **env: str):
    for k in list(os.environ.keys()):
        if k.startswith("IB_") or k.startswith("IBKR_") or k.startswith("TAPE_"):
            monkeypatch.delenv(k, raising=False)
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    import ibkr_host_config as mod

    importlib.reload(mod)
    return mod


def test_legacy_snapshot_only_matches_prior_defaults(monkeypatch):
    mod = _reload_config(
        monkeypatch,
        IBKR_SNAPSHOT_HOST="127.0.0.1",
        IBKR_SNAPSHOT_PORT="4002",
        IBKR_SNAPSHOT_CLIENT_ID="9402",
    )
    ep = mod.resolve_snapshot_read()
    assert ep.host == "127.0.0.1"
    assert ep.port == 4002
    assert ep.client_id == 9402


def test_ib_api_overrides_snapshot_host_port(monkeypatch):
    mod = _reload_config(
        monkeypatch,
        IB_API_HOST="10.0.0.5",
        IB_API_PORT="7497",
        IBKR_SNAPSHOT_CLIENT_ID="9402",
    )
    ep = mod.resolve_snapshot_read()
    assert ep.host == "10.0.0.5"
    assert ep.port == 7497


def test_live_bars_default_client_9436(monkeypatch):
    mod = _reload_config(monkeypatch)
    ep = mod.resolve_live_bars_read()
    assert ep.client_id == 9436


def test_tape_legacy_ibkr_host(monkeypatch):
    mod = _reload_config(
        monkeypatch,
        IBKR_HOST="192.168.1.1",
        IBKR_PORT="4001",
        TAPE_IB_CLIENT_ID="991",
    )
    ep = mod.resolve_tape_read()
    assert ep.host == "192.168.1.1"
    assert ep.port == 4001
    assert ep.client_id == 991


def test_client_id_collision_raises(monkeypatch):
    mod = _reload_config(
        monkeypatch,
        IB_CLIENT_ID_SNAPSHOT="42",
        IB_CLIENT_ID_LIVE_BARS="42",
        IB_CLIENT_ID_TAPE="99",
    )
    with pytest.raises(mod.IbkrClientIdCollisionError):
        mod.validate_read_client_ids_no_collision()


def test_diagnostics_template_has_all_keys(monkeypatch):
    mod = _reload_config(monkeypatch)
    d = mod.ib_host_diagnostics_template(mod.SURFACE_LIVE_PORTFOLIO)
    required = {
        "schema_version",
        "surface_name",
        "ib_host_mode",
        "socket_connected",
        "api_ready",
        "transport_state",
        "surface_freshness",
        "tape_transport_state",
        "effective_client_id",
        "effective_host",
        "effective_port",
    }
    for k in required:
        assert k in d
