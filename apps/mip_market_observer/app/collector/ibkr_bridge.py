"""IBKR market data → tape_coordinator (daemon thread). Uses reqMktData only (Phase 1)."""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone

from app.tape_coordinator import coordinator

_log = logging.getLogger(__name__)

try:
    from ib_insync import IB, Stock, util
except ImportError:
    IB = None  # type: ignore
    Stock = None  # type: ignore
    util = None  # type: ignore


class IbkrTapeBridge:
    def __init__(
        self,
        *,
        host: str = "127.0.0.1",
        port: int = 4002,
        client_id: int = 991,
    ) -> None:
        self.host = host
        self.port = port
        self.client_id = client_id
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._ib: IB | None = None
        self._subscribed: dict[str, object] = {}

    def start(self) -> None:
        if IB is None:
            _log.warning("ib_insync not installed — Tape IB bridge disabled.")
            coordinator.set_ib_connected(False)
            return
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="ibkr-tape", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._ib and self._ib.isConnected():
            try:
                self._ib.disconnect()
            except Exception:
                pass
        coordinator.set_ib_connected(False)

    def _run(self) -> None:
        assert IB is not None and Stock is not None and util is not None
        util.patchAsyncio()
        ib = IB()
        self._ib = ib
        try:
            ib.connect(self.host, self.port, clientId=self.client_id, readonly=True, timeout=15)
        except Exception as exc:
            _log.warning("IBKR connect failed: %s", exc)
            coordinator.set_ib_connected(False)
            return

        coordinator.set_ib_connected(True)
        _log.info("Tape IBKR connected %s:%s", self.host, self.port)

        while not self._stop.is_set():
            try:
                self._sync_subscriptions(ib)
                ib.sleep(0.25)
            except Exception as exc:
                _log.exception("IBKR loop error: %s", exc)
                time.sleep(1.0)

        for c in list(self._subscribed.values()):
            try:
                ib.cancelMktData(c)
            except Exception:
                pass
        self._subscribed.clear()
        try:
            ib.disconnect()
        except Exception:
            pass
        coordinator.set_ib_connected(False)

    def _attach(self, ib: IB, sym: str, c) -> None:
        ticker = ib.reqMktData(c, "", False, False)

        def on_update(t, symbol=sym):
            try:
                now = datetime.now(timezone.utc)
                if t.bid is not None and t.ask is not None:
                    bid = float(t.bid)
                    ask = float(t.ask)
                    if ask >= bid:
                        bs = float(t.bidSize) if t.bidSize is not None else None
                        asz = float(t.askSize) if t.askSize is not None else None
                        coordinator.ingest_quote(symbol, now, bid, ask, bs, asz)
                if t.last is not None and t.lastSize is not None:
                    px = float(t.last)
                    sz = float(t.lastSize)
                    if px > 0 and sz > 0:
                        coordinator.ingest_trade(symbol, now, px, sz, aggressor=None)
            except Exception:
                pass

        ticker.updateEvent += on_update
        self._subscribed[sym] = c

    def _sync_subscriptions(self, ib: IB) -> None:
        wanted = set(coordinator.list_active_symbols())
        current = set(self._subscribed.keys())

        for sym in current - wanted:
            c = self._subscribed.pop(sym, None)
            if c is not None:
                try:
                    ib.cancelMktData(c)
                except Exception:
                    pass

        for sym in wanted - current:
            if "/" in sym:
                continue
            c = Stock(sym, "SMART", "USD")
            try:
                ib.qualifyContracts(c)
            except Exception as exc:
                _log.warning("qualifyContracts %s: %s", sym, exc)
                continue
            self._attach(ib, sym, c)
