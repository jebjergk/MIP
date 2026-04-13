"""Tape observer HTTP API."""

from __future__ import annotations

import asyncio
import logging
import os
import random
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query

_cf = Path(__file__).resolve().parents[4] / "cursorfiles"
if _cf.is_dir() and str(_cf) not in sys.path:
    sys.path.insert(0, str(_cf))
from fastapi.middleware.cors import CORSMiddleware

from app.collector.ibkr_bridge import IbkrTapeBridge
from app.tape_coordinator import coordinator
_log = logging.getLogger(__name__)

_bridge: IbkrTapeBridge | None = None


async def _idle_gc_loop() -> None:
    while True:
        await asyncio.sleep(20)
        dropped = coordinator.drop_symbol_if_idle()
        if dropped:
            _log.info("Tape idle GC dropped: %s", dropped)


async def _simulate_loop() -> None:
    coordinator.simulate_mode = True
    coordinator.set_ib_connected(True)
    coordinator.set_ib_transport_state("connected")
    while True:
        await asyncio.sleep(2.0)
        now = datetime.now(timezone.utc)
        for sym in coordinator.list_active_symbols():
            mid = 100.0 + random.random() * 0.5
            coordinator.ingest_quote(
                sym,
                now,
                mid - 0.01,
                mid + 0.01,
                800.0 + random.random() * 400,
                800.0 + random.random() * 400,
            )
            side = random.choice(["buy", "sell", "buy", "sell", "buy"])
            px = mid + (0.015 if side == "buy" else -0.015)
            coordinator.ingest_trade(sym, now, px, 25 + random.randint(0, 80), aggressor=side)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _bridge
    sim = (os.getenv("TAPE_OBSERVER_SIMULATE") or "").strip().lower() in ("1", "true", "yes")
    if sim:
        _log.info("Tape observer SIMULATE mode (no IBKR)")
        asyncio.create_task(_simulate_loop())
    else:
        try:
            from ibkr_host_config import resolve_tape_read, validate_read_client_ids_no_collision

            validate_read_client_ids_no_collision()
            tape_ep = resolve_tape_read()
            _bridge = IbkrTapeBridge(
                host=tape_ep.host,
                port=tape_ep.port,
                client_id=tape_ep.client_id,
            )
        except Exception as exc:
            _log.warning("ibkr_host_config unavailable or invalid (%s); legacy tape env.", exc)
            _bridge = IbkrTapeBridge(
                host=(os.getenv("IBKR_HOST") or "127.0.0.1").strip(),
                port=int((os.getenv("IBKR_PORT") or "4002").strip() or "4002"),
                client_id=int((os.getenv("TAPE_IB_CLIENT_ID") or "991").strip() or "991"),
            )
        _bridge.start()

    asyncio.create_task(_idle_gc_loop())
    yield

    if _bridge:
        _bridge.stop()
        _bridge = None


app = FastAPI(title="MIP Tape Observer", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def _tape_ib_host_diagnostics() -> dict:
    import ibkr_host_config as ic

    base = ic.ib_host_diagnostics_template(ic.SURFACE_TAPE_OBSERVER)
    ep = ic.resolve_tape_read()
    tr_raw = coordinator.get_ib_transport_state()
    tr = tr_raw if tr_raw in ("disconnected", "reconnecting", "connected") else "unknown"
    conn = tr == "connected"
    valid, reason = coordinator.tape_operational_sample()
    if coordinator.simulate_mode:
        valid, reason = True, None
        fresh = "live"
    elif conn and valid:
        fresh = "live"
        reason = None
    elif conn:
        fresh = "stale"
        if reason is None:
            reason = "tape_not_live_ready"
    elif tr == "disconnected":
        fresh = "unavailable"
    else:
        fresh = "unknown"

    return ic.merge_diagnostics(
        base,
        effective_host=ep.host,
        effective_port=ep.port,
        effective_client_id=ep.client_id,
        socket_connected=conn or bool(coordinator.simulate_mode),
        api_ready=conn or bool(coordinator.simulate_mode),
        transport_state=tr,
        tape_transport_state=tr,
        tape_operational_validity=valid,
        tape_operational_reason_code=reason,
        surface_freshness=fresh,
        surface_freshness_reason=reason,
        last_message_ts=None,
    )


@app.get("/health")
def health():
    return {
        "ok": True,
        "service": "mip_tape_observer",
        "ib_connected": coordinator.ib_connected,
        "ib_transport_state": coordinator.get_ib_transport_state(),
        "ib_host_diagnostics": _tape_ib_host_diagnostics(),
    }


@app.get("/tape/v1/snapshot")
def tape_snapshot(symbol: str = Query(..., min_length=1, max_length=32)):
    sym = symbol.strip().upper()
    coordinator.touch(sym)
    return coordinator.build_snapshot(sym)


@app.get("/tape/v1/snapshot/debug")
def tape_snapshot_debug(symbol: str = Query(..., min_length=1, max_length=32)):
    """Phase 3 — same snapshot without replay write; gated by TAPE_DEBUG_ENDPOINT=1."""
    if (os.getenv("TAPE_DEBUG_ENDPOINT") or "").strip().lower() not in ("1", "true", "yes"):
        raise HTTPException(status_code=404, detail="Tape debug endpoint disabled.")
    sym = symbol.strip().upper()
    coordinator.touch(sym)
    snap = coordinator.build_snapshot(sym, record_replay=False)
    return {
        "snapshot": snap,
        "replay": {
            "periodic_path": bool((os.getenv("TAPE_REPLAY_JSONL") or "").strip()),
            "anomaly_path": bool((os.getenv("TAPE_REPLAY_ANOMALY_JSONL") or "").strip()),
        },
    }


@app.get("/")
def root():
    return {"service": "MIP Tape Observer", "docs": "/docs"}
