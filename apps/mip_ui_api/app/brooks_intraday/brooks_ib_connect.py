"""Brooks Lab IB socket settings — dedicated client id; does not reuse live-bars client id + 1."""

from __future__ import annotations

import os
from typing import Any

from app.services.ibkr_live_bars import resolve_live_bars_connect

# Separate from live bars (9436) and live+1 historical (9437) used elsewhere in MIP.
DEFAULT_BROOKS_PHASE9_CLIENT_ID = 9447


def resolve_brooks_phase9_ib_connect() -> dict[str, Any]:
    """
    Host/port follow market-data gateway (same as live bars) unless BROOKS_PHASE9_IB_HOST/PORT are set.
    Client id is always BROOKS_PHASE9_IB_CLIENT_ID (default 9447) so Phase 9 validation never steals 9436/9437.
    """
    live = resolve_live_bars_connect(portfolio_id=None)
    client_id = int(
        os.environ.get("BROOKS_PHASE9_IB_CLIENT_ID", str(DEFAULT_BROOKS_PHASE9_CLIENT_ID))
    )
    host = (os.environ.get("BROOKS_PHASE9_IB_HOST") or live.get("host") or "127.0.0.1").strip()
    port = int(os.environ.get("BROOKS_PHASE9_IB_PORT") or live.get("port") or 7497)
    return {
        "host": host,
        "port": port,
        "client_id": client_id,
        "connect_timeout_sec": int(live.get("connect_timeout_sec") or 10),
        "historical_subprocess_client_id": client_id,
        "live_bars_client_id": int(live.get("client_id", 9436)),
    }
