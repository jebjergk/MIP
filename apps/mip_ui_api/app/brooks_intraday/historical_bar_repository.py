from __future__ import annotations

import json
import logging
from datetime import date, datetime, timezone
from typing import Any

from app.db import get_connection

from .bars import HistoricalBar, BAR_SIZE_MINUTES
from .calendar import resolve_week_sessions

logger = logging.getLogger(__name__)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def persist_bars(
    bars: list[HistoricalBar],
    *,
    source_request_id: str | None,
    source_metadata: dict | None,
    data_quality_status: str,
) -> int:
    if not bars:
        return 0
    retrieved = _utc_now()
    conn = get_connection()
    inserted = 0
    try:
        cur = conn.cursor()
        for bar in bars:
            cur.execute(
                """
                MERGE INTO MIP.APP.BROOKS_INTRADAY_HISTORICAL_BAR t
                USING (
                    SELECT
                        %s AS SYMBOL,
                        %s AS BAR_TS_UTC,
                        %s AS BAR_TS_NY,
                        %s AS TRADING_DATE,
                        %s AS BAR_SIZE_MINUTES,
                        %s AS OPEN,
                        %s AS HIGH,
                        %s AS LOW,
                        %s AS CLOSE,
                        %s AS VOLUME,
                        %s AS RTH_FLAG,
                        %s AS SOURCE,
                        %s AS SOURCE_REQUEST_ID,
                        %s AS RETRIEVED_AT_UTC,
                        %s AS DATA_QUALITY_STATUS,
                        PARSE_JSON(%s) AS SOURCE_METADATA_JSON
                ) s
                ON t.SYMBOL = s.SYMBOL AND t.BAR_TS_UTC = s.BAR_TS_UTC AND t.BAR_SIZE_MINUTES = s.BAR_SIZE_MINUTES
                WHEN NOT MATCHED THEN INSERT (
                    SYMBOL, BAR_TS_UTC, BAR_TS_NY, TRADING_DATE, BAR_SIZE_MINUTES,
                    OPEN, HIGH, LOW, CLOSE, VOLUME, RTH_FLAG, SOURCE,
                    SOURCE_REQUEST_ID, RETRIEVED_AT_UTC, DATA_QUALITY_STATUS, SOURCE_METADATA_JSON
                ) VALUES (
                    s.SYMBOL, s.BAR_TS_UTC, s.BAR_TS_NY, s.TRADING_DATE, s.BAR_SIZE_MINUTES,
                    s.OPEN, s.HIGH, s.LOW, s.CLOSE, s.VOLUME, s.RTH_FLAG, s.SOURCE,
                    s.SOURCE_REQUEST_ID, s.RETRIEVED_AT_UTC, s.DATA_QUALITY_STATUS, s.SOURCE_METADATA_JSON
                )
                """,
                (
                    bar.symbol,
                    bar.ts_utc,
                    bar.ts_ny,
                    bar.trading_date,
                    bar.bar_size_minutes,
                    bar.open,
                    bar.high,
                    bar.low,
                    bar.close,
                    bar.volume,
                    bar.rth,
                    bar.source,
                    source_request_id,
                    retrieved,
                    data_quality_status,
                    json.dumps(source_metadata or {}),
                ),
            )
            inserted += 1
        conn.commit()
    finally:
        conn.close()
    return inserted


def load_bars_from_store(symbol: str, trading_date: date) -> list[HistoricalBar]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT SYMBOL, BAR_TS_UTC, BAR_TS_NY, TRADING_DATE, BAR_SIZE_MINUTES,
                   OPEN, HIGH, LOW, CLOSE, VOLUME, RTH_FLAG, SOURCE
            FROM MIP.APP.BROOKS_INTRADAY_HISTORICAL_BAR
            WHERE SYMBOL = %s AND TRADING_DATE = %s AND BAR_SIZE_MINUTES = %s
            ORDER BY BAR_TS_UTC
            """,
            (symbol.upper(), trading_date, BAR_SIZE_MINUTES),
        )
        cols = [d[0].lower() for d in cur.description]
        bars: list[HistoricalBar] = []
        for raw in cur.fetchall():
            row = dict(zip(cols, raw))
            bars.append(
                HistoricalBar(
                    symbol=row["symbol"],
                    ts_utc=row["bar_ts_utc"],
                    ts_ny=row["bar_ts_ny"],
                    trading_date=row["trading_date"],
                    open=float(row["open"]),
                    high=float(row["high"]),
                    low=float(row["low"]),
                    close=float(row["close"]),
                    volume=float(row["volume"]) if row.get("volume") is not None else None,
                    source=row["source"],
                    bar_size_minutes=int(row["bar_size_minutes"]),
                    rth=bool(row["rth_flag"]),
                )
            )
        return bars
    finally:
        conn.close()


def load_bars_for_symbol_week(symbol: str, week_start: date) -> list[HistoricalBar]:
    """All RTH 5m bars for one symbol across the resolved trading week (one query)."""
    week = resolve_week_sessions(week_start)
    if not week.trading_dates:
        return []
    start = week.trading_dates[0]
    end = week.trading_dates[-1]
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT SYMBOL, BAR_TS_UTC, BAR_TS_NY, TRADING_DATE, BAR_SIZE_MINUTES,
                   OPEN, HIGH, LOW, CLOSE, VOLUME, RTH_FLAG, SOURCE
            FROM MIP.APP.BROOKS_INTRADAY_HISTORICAL_BAR
            WHERE SYMBOL = %s
              AND TRADING_DATE >= %s AND TRADING_DATE <= %s
              AND BAR_SIZE_MINUTES = %s
            ORDER BY BAR_TS_UTC
            """,
            (symbol.upper(), start, end, BAR_SIZE_MINUTES),
        )
        cols = [d[0].lower() for d in cur.description]
        bars: list[HistoricalBar] = []
        for raw in cur.fetchall():
            row = dict(zip(cols, raw))
            bars.append(
                HistoricalBar(
                    symbol=row["symbol"],
                    ts_utc=row["bar_ts_utc"],
                    ts_ny=row["bar_ts_ny"],
                    trading_date=row["trading_date"],
                    open=float(row["open"]),
                    high=float(row["high"]),
                    low=float(row["low"]),
                    close=float(row["close"]),
                    volume=float(row["volume"]) if row.get("volume") is not None else None,
                    source=row["source"],
                    bar_size_minutes=int(row["bar_size_minutes"]),
                    rth=bool(row["rth_flag"]),
                )
            )
        return bars
    finally:
        conn.close()
