from __future__ import annotations

from datetime import datetime
from typing import Iterable

from .bars import HistoricalBar
from .errors import BrooksIntradayError


class ReplayBarView:
    """Restricted view of frozen bars — no access beyond replay cursor."""

    def __init__(
        self,
        *,
        bars_by_symbol: dict[str, list[HistoricalBar]],
        cursor_timestamp_utc: datetime | None,
        symbols: list[str],
    ) -> None:
        self._bars_by_symbol = bars_by_symbol
        self._cursor_ts = cursor_timestamp_utc
        self._symbols = symbols

    @property
    def cursor_timestamp_utc(self) -> datetime | None:
        return self._cursor_ts

    def _assert_not_future(self, bar: HistoricalBar) -> None:
        if self._cursor_ts is None:
            raise BrooksIntradayError(
                "FUTURE_BAR_ACCESS_DENIED",
                "No bar has been revealed yet.",
                status_code=403,
            )
        if bar.ts_utc > self._cursor_ts:
            raise BrooksIntradayError(
                "FUTURE_BAR_ACCESS_DENIED",
                f"Bar {bar.ts_utc.isoformat()} is after cursor {self._cursor_ts.isoformat()}.",
                status_code=403,
            )

    def visible_bars(self, symbol: str) -> list[HistoricalBar]:
        sym = symbol.upper()
        out: list[HistoricalBar] = []
        for bar in self._bars_by_symbol.get(sym, []):
            if self._cursor_ts is None:
                continue
            if bar.ts_utc <= self._cursor_ts:
                out.append(bar)
            else:
                break
        return out

    def current_slice(self, slice_bars: Iterable[HistoricalBar]) -> list[HistoricalBar]:
        revealed = []
        for bar in slice_bars:
            self._assert_not_future(bar)
            revealed.append(bar)
        return revealed

    def assert_timestamp_allowed(self, ts_utc: datetime) -> None:
        if self._cursor_ts is None:
            if ts_utc is not None:
                raise BrooksIntradayError(
                    "FUTURE_BAR_ACCESS_DENIED",
                    "Cursor has not advanced to any timestamp yet.",
                    status_code=403,
                )
            return
        if ts_utc > self._cursor_ts:
            raise BrooksIntradayError(
                "FUTURE_BAR_ACCESS_DENIED",
                f"Requested timestamp {ts_utc.isoformat()} is after cursor.",
                status_code=403,
            )
