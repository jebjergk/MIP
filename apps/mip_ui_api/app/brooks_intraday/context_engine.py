"""Dispatch Phase 6 context engine by ruleset version."""

from __future__ import annotations

from datetime import date
from typing import Any

from .bars import HistoricalBar
from .context_engine_v01 import (
    ContextResult,
    SessionContextState,
    advance_context_v01_for_bar,
    get_context_session,
    reset_context_session,
)
from .context_ruleset_v01 import RULESET_VERSION as RULESET_V01
from .context_ruleset_v02 import RULESET_VERSION as RULESET_V02
from .context_ruleset_v03 import RULESET_VERSION as RULESET_V03
from .context_ruleset_v04 import RULESET_VERSION as RULESET_V04

__all__ = [
    "ContextResult",
    "SessionContextState",
    "advance_context_for_bar",
    "advance_context_v01_for_bar",
    "get_context_session",
    "reset_context_session",
    "context_sequence_hash",
]


def advance_context_for_bar(
    *,
    state: dict[str, Any],
    dossier: dict[str, Any],
    symbol: str,
    trading_date: date,
    bar: HistoricalBar,
    bar_index_in_session: int,
    objective_obs: dict[str, Any],
    patterns: list[dict[str, Any]],
    params: dict[str, Any] | None = None,
    ruleset_version: str | None = None,
    open_position_symbol: str | None = None,
) -> ContextResult:
    # Default remains V0.2 — V0.3 is explicit opt-in only (not Freeze V1).
    rs = ruleset_version or (params or {}).get("ruleset_version") or RULESET_V02
    if rs == RULESET_V01:
        return advance_context_v01_for_bar(
            state=state,
            dossier=dossier,
            symbol=symbol,
            trading_date=trading_date,
            bar=bar,
            bar_index_in_session=bar_index_in_session,
            objective_obs=objective_obs,
            patterns=patterns,
            params=params,
        )
    if rs == RULESET_V04:
        from .context_engine_v04 import advance_context_v04_for_bar

        return advance_context_v04_for_bar(
            state=state,
            dossier=dossier,
            symbol=symbol,
            trading_date=trading_date,
            bar=bar,
            bar_index_in_session=bar_index_in_session,
            objective_obs=objective_obs,
            patterns=patterns,
            params=params,
            open_position_symbol=open_position_symbol,
        )
    if rs == RULESET_V03:
        from .context_engine_v03 import advance_context_v03_for_bar

        return advance_context_v03_for_bar(
            state=state,
            dossier=dossier,
            symbol=symbol,
            trading_date=trading_date,
            bar=bar,
            bar_index_in_session=bar_index_in_session,
            objective_obs=objective_obs,
            patterns=patterns,
            params=params,
            open_position_symbol=open_position_symbol,
        )
    from .context_engine_v02 import advance_context_v02_for_bar

    return advance_context_v02_for_bar(
        state=state,
        dossier=dossier,
        symbol=symbol,
        trading_date=trading_date,
        bar=bar,
        bar_index_in_session=bar_index_in_session,
        objective_obs=objective_obs,
        patterns=patterns,
        params=params,
    )


def context_sequence_hash(run_id: str, *, context_attempt_id: str) -> str:
    from .context_repository import context_sequence_hash as repo_hash

    return repo_hash(run_id, context_attempt_id=context_attempt_id)
