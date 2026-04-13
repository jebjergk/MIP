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

try:
    from ibkr_host_config import ResolvedIbEndpoint
    from ibkr_host_session import IBHostSession
except ImportError:
    ResolvedIbEndpoint = None  # type: ignore
    IBHostSession = None  # type: ignore


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
        self._session: IBHostSession | None = None
        self._subscribed: dict[str, object] = {}

    def start(self) -> None:
        if IB is None:
            _log.warning("ib_insync not installed — Tape IB bridge disabled.")
            coordinator.set_ib_connected(False)
            coordinator.set_ib_transport_state("disconnected")
            return
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="ibkr-tape", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._session:
            try:
                self._session.disconnect()
            except Exception:
                pass
            self._session = None
        if self._ib and self._ib.isConnected():
            try:
                self._ib.disconnect()
            except Exception:
                pass
        self._ib = None
        coordinator.set_ib_connected(False)
        coordinator.set_ib_transport_state("disconnected")

    def _run(self) -> None:
        assert IB is not None and Stock is not None and util is not None
        util.patchAsyncio()

        if IBHostSession is None or ResolvedIbEndpoint is None:
            self._run_legacy()
            return

        endpoint = ResolvedIbEndpoint("tape_read", self.host, self.port, self.client_id)
        session = IBHostSession(
            endpoint,
            on_disconnect=lambda: coordinator.set_ib_connected(False),
        )
        self._session = session

        while not self._stop.is_set():
            coordinator.set_ib_transport_state("reconnecting")
            if not session.connect_readonly(15.0):
                coordinator.set_ib_connected(False)
                coordinator.set_ib_transport_state("disconnected")
                if self._stop.is_set():
                    break
                session.reconnect_loop_sleep(1.0, 30.0)
                continue

            session.reset_reconnect_count_on_success()
            coordinator.set_ib_transport_state("connected")
            ib = session.ib
            self._ib = ib
            coordinator.set_ib_connected(True)
            _log.info("Tape IBKR connected %s:%s client=%s", self.host, self.port, self.client_id)

            while not self._stop.is_set() and session.is_connected:
                try:
                    self._sync_subscriptions(ib)
                    ib.sleep(0.25)
                except Exception as exc:
                    _log.exception("IBKR loop error: %s", exc)
                    time.sleep(1.0)
                    break

            coordinator.set_ib_connected(False)
            for c in list(self._subscribed.values()):
                try:
                    ib.cancelMktData(c)
                except Exception:
                    pass
            self._subscribed.clear()
            session.disconnect()
            coordinator.set_ib_transport_state("disconnected")
            self._ib = None
            if self._stop.is_set():
                break
            session.reconnect_loop_sleep(1.0, 15.0)

        self._session = None

    def _run_legacy(self) -> None:
        """Fallback if cursorfiles ibkr_* modules are not on PYTHONPATH."""
        assert IB is not None and Stock is not None and util is not None
        ib = IB()
        self._ib = ib
        coordinator.set_ib_transport_state("reconnecting")
        try:
            ib.connect(self.host, self.port, clientId=self.client_id, readonly=True, timeout=15)
        except Exception as exc:
            _log.warning("IBKR connect failed: %s", exc)
            coordinator.set_ib_connected(False)
            coordinator.set_ib_transport_state("disconnected")
            return

        coordinator.set_ib_transport_state("connected")
        coordinator.set_ib_connected(True)
        _log.info("Tape IBKR connected %s:%s (legacy session)", self.host, self.port)

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
        coordinator.set_ib_transport_state("disconnected")

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
