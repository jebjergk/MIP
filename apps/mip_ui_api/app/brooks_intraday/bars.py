from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta
from typing import Protocol

from zoneinfo import ZoneInfo

from app.brooks_intraday.db_util import query_rows

from .interval_validation import (
    BAR_VALIDATION_VERSION,
    TIMESTAMP_CONVENTION,
    expected_interval_starts_for_session,
    interval_sets_match,
)
from .calendar import NY_TZ, TradingSession
from .readiness import dataset_hash

UTC = ZoneInfo("UTC")
BAR_SIZE_MINUTES = 5


@dataclass(frozen=True)
class HistoricalBar:
    symbol: str
    ts_utc: datetime
    ts_ny: datetime
    trading_date: date
    open: float
    high: float
    low: float
    close: float
    volume: float | None
    source: str
    bar_size_minutes: int
    rth: bool


@dataclass
class BarValidationResult:
    status: str
    bar_count: int
    expected_bar_count: int
    source: str
    messages: list[str]
    first_bar_ts: str | None = None
    last_bar_ts: str | None = None
    missing_timestamps_count: int = 0
    missing_timestamps: list[str] | None = None
    extra_timestamps: list[str] | None = None
    timestamp_convention: str = "INTERVAL_START_NY_WALL_V0_1"
    validation_version: str = "BROOKS_BAR_INTERVAL_VALIDATION_V0_1"
    dataset_hash: str | None = None


class BrooksHistoricalBarProvider(Protocol):
    def get_rth_bars(self, symbol: str, trading_date: date, session: TradingSession) -> tuple[list[HistoricalBar], BarValidationResult]:
        ...


def _parse_ts(value) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value).replace("Z", "")[:26])


def _ny_wall_from_mart_ts(ts: datetime, trading_date: date) -> datetime:
    """MART MARKET_BARS.TS is stored as NY session wall time (NTZ)."""
    if ts.tzinfo is not None:
        ts = ts.astimezone(NY_TZ).replace(tzinfo=None)
    return ts


def _to_utc_naive(ny_wall: datetime) -> datetime:
    return ny_wall.replace(tzinfo=NY_TZ).astimezone(NY_TZ).astimezone(__import__("zoneinfo").ZoneInfo("UTC")).replace(tzinfo=None)


def _validate_ohlc(o: float, h: float, l: float, c: float) -> bool:
    if min(o, h, l, c) <= 0:
        return False
    if h < max(o, c, l):
        return False
    if l > min(o, c, h):
        return False
    return True


def _align_5m_bucket(ny_ts: datetime, session_open: time = time(9, 30)) -> datetime | None:
    if ny_ts.date() != ny_ts.date():
        return None
    open_dt = datetime.combine(ny_ts.date(), session_open)
    if ny_ts < open_dt:
        return None
    minutes = int((ny_ts - open_dt).total_seconds() // 60)
    bucket_min = (minutes // BAR_SIZE_MINUTES) * BAR_SIZE_MINUTES
    return open_dt + timedelta(minutes=bucket_min)


def aggregate_1m_to_5m(rows: list[dict], symbol: str, trading_date: date, session: TradingSession) -> list[HistoricalBar]:
    buckets: dict[datetime, dict] = {}
    open_dt = datetime.combine(trading_date, time(9, 30))
    close_dt = datetime.combine(trading_date, session.session_close_ny.time())

    for row in rows:
        ts = _parse_ts(row.get("ts") or row.get("TS"))
        ts = _ny_wall_from_mart_ts(ts, trading_date)
        if ts.date() != trading_date or ts < open_dt or ts >= close_dt:
            continue
        bucket = _align_5m_bucket(ts)
        if bucket is None:
            continue
        o = float(row.get("open") or row.get("OPEN"))
        h = float(row.get("high") or row.get("HIGH"))
        l = float(row.get("low") or row.get("LOW"))
        c = float(row.get("close") or row.get("CLOSE"))
        vol = row.get("volume") if row.get("volume") is not None else row.get("VOLUME")
        vol_f = float(vol) if vol is not None else None
        slot = buckets.get(bucket)
        if not slot:
            buckets[bucket] = {
                "open": o,
                "high": h,
                "low": l,
                "close": c,
                "volume": vol_f or 0.0,
            }
        else:
            slot["high"] = max(slot["high"], h)
            slot["low"] = min(slot["low"], l)
            slot["close"] = c
            slot["volume"] = (slot["volume"] or 0) + (vol_f or 0)

    bars: list[HistoricalBar] = []
    for bucket in sorted(buckets):
        slot = buckets[bucket]
        ts_ny = bucket
        ts_utc = ts_ny.replace(tzinfo=NY_TZ).astimezone(UTC).replace(tzinfo=None)
        bars.append(
            HistoricalBar(
                symbol=symbol,
                ts_utc=ts_utc,
                ts_ny=ts_ny,
                trading_date=trading_date,
                open=slot["open"],
                high=slot["high"],
                low=slot["low"],
                close=slot["close"],
                volume=slot["volume"],
                source="MART_MARKET_BARS_1M_AGGREGATED_TO_5M_V0_1",
                bar_size_minutes=BAR_SIZE_MINUTES,
                rth=True,
            )
        )
    return bars


def validate_bars(bars: list[HistoricalBar], session: TradingSession, *, source: str) -> BarValidationResult:
    expected_ny = expected_interval_starts_for_session(session)
    expected = len(expected_ny)
    messages: list[str] = []
    if not bars:
        return BarValidationResult(
            status="NO_DATA",
            bar_count=0,
            expected_bar_count=expected,
            source=source,
            messages=["No RTH bars returned."],
            missing_timestamps_count=expected,
            missing_timestamps=[d.isoformat(timespec="minutes") for d in expected_ny[:20]],
            timestamp_convention=TIMESTAMP_CONVENTION,
            validation_version=BAR_VALIDATION_VERSION,
        )

    seen_ts: set[datetime] = set()
    prev: datetime | None = None
    actual_ny: list[datetime] = []
    for bar in bars:
        if bar.bar_size_minutes != BAR_SIZE_MINUTES:
            return BarValidationResult(
                status="INVALID_OHLC",
                bar_count=len(bars),
                expected_bar_count=expected,
                source=source,
                messages=[f"Unexpected bar size {bar.bar_size_minutes}."],
                timestamp_convention=TIMESTAMP_CONVENTION,
                validation_version=BAR_VALIDATION_VERSION,
            )
        if bar.ts_ny in seen_ts:
            return BarValidationResult(
                status="DUPLICATE_BARS",
                bar_count=len(bars),
                expected_bar_count=expected,
                source=source,
                messages=["Duplicate bar timestamp detected."],
                timestamp_convention=TIMESTAMP_CONVENTION,
                validation_version=BAR_VALIDATION_VERSION,
            )
        seen_ts.add(bar.ts_ny)
        actual_ny.append(bar.ts_ny)
        if prev and bar.ts_ny <= prev:
            return BarValidationResult(
                status="OUT_OF_ORDER",
                bar_count=len(bars),
                expected_bar_count=expected,
                source=source,
                messages=["Bars are not strictly increasing."],
                timestamp_convention=TIMESTAMP_CONVENTION,
                validation_version=BAR_VALIDATION_VERSION,
            )
        prev = bar.ts_ny
        if not _validate_ohlc(bar.open, bar.high, bar.low, bar.close):
            return BarValidationResult(
                status="INVALID_OHLC",
                bar_count=len(bars),
                expected_bar_count=expected,
                source=source,
                messages=[f"Invalid OHLC at {bar.ts_ny}."],
                timestamp_convention=TIMESTAMP_CONVENTION,
                validation_version=BAR_VALIDATION_VERSION,
            )
        open_dt = datetime.combine(session.trading_date, time(9, 30))
        close_dt = session.session_close_ny
        if bar.ts_ny < open_dt or bar.ts_ny >= close_dt:
            return BarValidationResult(
                status="OUTSIDE_RTH",
                bar_count=len(bars),
                expected_bar_count=expected,
                source=source,
                messages=[f"Bar {bar.ts_ny} outside RTH window."],
                timestamp_convention=TIMESTAMP_CONVENTION,
                validation_version=BAR_VALIDATION_VERSION,
            )
        if bar.trading_date != session.trading_date:
            return BarValidationResult(
                status="OUTSIDE_RTH",
                bar_count=len(bars),
                expected_bar_count=expected,
                source=source,
                messages=[f"Unexpected trading date on bar {bar.trading_date}."],
                timestamp_convention=TIMESTAMP_CONVENTION,
                validation_version=BAR_VALIDATION_VERSION,
            )

    ok, missing, extra = interval_sets_match(actual_ny, expected_ny)
    status = "COMPLETE" if ok else "MISSING_BARS"
    if not ok:
        messages.append(
            f"Interval set mismatch: missing={len(missing)} extra={len(extra)} row_count={len(bars)} expected={expected}."
        )
    dhash = dataset_hash(bars) if ok else None
    return BarValidationResult(
        status=status,
        bar_count=len(bars),
        expected_bar_count=expected,
        source=source,
        messages=messages,
        first_bar_ts=bars[0].ts_utc.isoformat() if bars else None,
        last_bar_ts=bars[-1].ts_utc.isoformat() if bars else None,
        missing_timestamps_count=len(missing),
        missing_timestamps=missing[:50],
        extra_timestamps=extra[:50],
        timestamp_convention=TIMESTAMP_CONVENTION,
        validation_version=BAR_VALIDATION_VERSION,
        dataset_hash=dhash,
    )


class SnowflakeMartHistoricalBarProvider:
    """Read-only bars from MIP.MART.MARKET_BARS (native 5m or aggregated 1m)."""

    def get_rth_bars(
        self,
        symbol: str,
        trading_date: date,
        session: TradingSession,
    ) -> tuple[list[HistoricalBar], BarValidationResult]:
        sym = symbol.upper()
        native = query_rows(
            """
            SELECT TS, OPEN, HIGH, LOW, CLOSE, VOLUME
            FROM MIP.MART.MARKET_BARS
            WHERE SYMBOL = %s
              AND MARKET_TYPE IN ('STOCK', 'ETF')
              AND INTERVAL_MINUTES = 5
              AND TS::DATE = %s
            ORDER BY TS
            """,
            (sym, trading_date),
        )
        if native:
            bars: list[HistoricalBar] = []
            for row in native:
                ts_ny = _ny_wall_from_mart_ts(_parse_ts(row["ts"]), trading_date)
                ts_utc = ts_ny.replace(tzinfo=NY_TZ).astimezone(UTC).replace(tzinfo=None)
                bars.append(
                    HistoricalBar(
                        symbol=sym,
                        ts_utc=ts_utc,
                        ts_ny=ts_ny,
                        trading_date=trading_date,
                        open=float(row["open"]),
                        high=float(row["high"]),
                        low=float(row["low"]),
                        close=float(row["close"]),
                        volume=float(row["volume"]) if row.get("volume") is not None else None,
                        source="MART_MARKET_BARS_5M",
                        bar_size_minutes=5,
                        rth=True,
                    )
                )
            result = validate_bars(bars, session, source="MART_MARKET_BARS_5M")
            return bars, result

        one_min = query_rows(
            """
            SELECT TS, OPEN, HIGH, LOW, CLOSE, VOLUME
            FROM MIP.MART.MARKET_BARS
            WHERE SYMBOL = %s
              AND MARKET_TYPE IN ('STOCK', 'ETF')
              AND INTERVAL_MINUTES = 1
              AND TS::DATE = %s
            ORDER BY TS
            """,
            (sym, trading_date),
        )
        bars = aggregate_1m_to_5m(one_min, sym, trading_date, session)
        result = validate_bars(bars, session, source="MART_MARKET_BARS_1M_AGGREGATED_TO_5M_V0_1")
        return bars, result


def bar_validation_to_dict(result: BarValidationResult) -> dict:
    return asdict(result)
