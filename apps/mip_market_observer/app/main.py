"""Tape observer HTTP API."""

from __future__ import annotations

import asyncio
import logging
import os
import random
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, Query
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


@app.get("/health")
def health():
    return {"ok": True, "service": "mip_tape_observer", "ib_connected": coordinator.ib_connected}


@app.get("/tape/v1/snapshot")
def tape_snapshot(symbol: str = Query(..., min_length=1, max_length=32)):
    sym = symbol.strip().upper()
    coordinator.touch(sym)
    return coordinator.build_snapshot(sym)


@app.get("/")
def root():
    return {"service": "MIP Tape Observer", "docs": "/docs"}
