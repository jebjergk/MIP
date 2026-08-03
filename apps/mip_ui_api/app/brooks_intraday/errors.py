from __future__ import annotations

from typing import Any


class BrooksIntradayError(Exception):
    def __init__(
        self,
        code: str,
        message: str,
        *,
        status_code: int = 400,
        run_id: str | None = None,
        symbol: str | None = None,
        trading_date: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code
        self.run_id = run_id
        self.symbol = symbol
        self.trading_date = trading_date
        self.details = details or {}

    def to_payload(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "run_id": self.run_id,
            "symbol": self.symbol,
            "trading_date": self.trading_date,
            "details": self.details,
        }
