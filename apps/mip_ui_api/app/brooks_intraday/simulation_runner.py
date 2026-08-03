"""In-memory simulation stepping for fixtures and bulk replay."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from .bars import HistoricalBar
from .context_ruleset_v01 import ACTION_CONSIDER_ENTRY
from .simulation_engine import (
    BLOCK_REASON_POSITION_OPEN,
    BLOCK_REASON_TIE_BREAK,
    BLOCK_REASON_NOT_ENTRY_SIGNAL,
    PortfolioSimState,
    entry_permitted,
    execute_entry,
    fill_pending_exit,
    process_symbol_bar,
    record_blocked_entry,
    tie_break_pick,
)
from .simulation_ruleset_v01 import resolve_params


@dataclass
class FixtureStep:
    """One global timestamp slice (may include multiple symbols)."""

    bar_index_in_session: int
    is_last_bar_in_session: bool
    is_last_bar_of_week: bool = False
    bars: dict[str, HistoricalBar] = field(default_factory=dict)
    context_by_symbol: dict[str, dict[str, Any]] = field(default_factory=dict)
    dossiers_by_symbol: dict[str, dict[str, Any]] = field(default_factory=dict)
    step_key: str | None = None

    def idempotency_key(self) -> str:
        if self.step_key:
            return self.step_key
        ts_parts = sorted(f"{sym}:{str(b.ts_utc)[:19]}" for sym, b in self.bars.items())
        return "|".join(ts_parts)


@dataclass
class FixtureRunResult:
    portfolio: PortfolioSimState
    step_log: list[dict[str, Any]] = field(default_factory=list)
    skipped_duplicate_steps: int = 0


def _tie_break_detail(winner: dict, losers: list[dict], params: dict) -> dict:
    from .simulation_engine import _entry_score, _room_class, _paa_rank

    def row(c: dict) -> dict:
        return {
            "symbol": c["symbol"],
            "entry_score": _entry_score(c["context"]),
            "room_class": _room_class(c["context"]),
            "paa_verdict": c["dossier"].get("paa_verdict"),
            "paa_rank": _paa_rank(str(c["dossier"].get("paa_verdict") or ""), params),
        }

    return {
        "winner": row(winner),
        "rejected": [row(x) for x in losers],
        "tie_break_reason": "BROOKS_TIEBREAK_V0_1_ORDER",
    }


def run_simulation_fixture(
    steps: list[FixtureStep],
    *,
    symbol_order: list[str],
    starting_cash: float = 1000.0,
    params: dict[str, Any] | None = None,
    processed_step_keys: set[str] | None = None,
) -> FixtureRunResult:
    """
    Run simulation on deterministic fixture steps (no Snowflake, no pilot run).

    If processed_step_keys is provided, steps whose idempotency key is already present
    are skipped entirely (no cash/trade mutation).
    """
    params = resolve_params(params)
    portfolio = PortfolioSimState(cash=float(starting_cash))
    entry_action = str(params["entry_signal_action"])
    log: list[dict[str, Any]] = []
    skipped = 0
    seen = processed_step_keys if processed_step_keys is not None else set()

    for step in steps:
        ikey = step.idempotency_key()
        if ikey in seen:
            skipped += 1
            log.append({"step_key": ikey, "skipped_duplicate": True})
            continue
        seen.add(ikey)

        step_notes: dict[str, Any] = {"step_key": ikey, "actions": {}, "events": []}

        for sym in symbol_order:
            bar = step.bars.get(sym)
            if bar:
                res = fill_pending_exit(portfolio, bar=bar)
                if res:
                    step_notes["events"].append(f"{sym}:exit_filled_at_open")

        entry_candidates: list[dict[str, Any]] = []
        for sym in symbol_order:
            bar = step.bars.get(sym)
            ctx = step.context_by_symbol.get(sym)
            dossier = step.dossiers_by_symbol.get(sym) or {"trade_simulation_ready": True}
            if not bar or not ctx:
                continue
            step_res = process_symbol_bar(
                portfolio=portfolio,
                ctx=ctx,
                bar=bar,
                dossier=dossier,
                bar_index_in_session=step.bar_index_in_session,
                is_last_bar_in_session=step.is_last_bar_in_session,
                params=params,
            )
            step_notes["actions"][sym] = ctx.get("selected_action")
            if step_res.exit_scheduled:
                step_notes["events"].append(f"{sym}:exit_scheduled")
            if step_res.exit_filled:
                step_notes["events"].append(f"{sym}:exit_filled_same_bar")

            act = str(ctx.get("selected_action") or "")
            if act == entry_action and not step_res.entry_candidate:
                _ok, block = entry_permitted(
                    ctx=ctx,
                    dossier=dossier,
                    bar_index_in_session=step.bar_index_in_session,
                    params=params,
                )
                if block and block != BLOCK_REASON_NOT_ENTRY_SIGNAL:
                    record_blocked_entry(
                        portfolio,
                        symbol=sym,
                        signal_ts=bar.ts_utc,
                        reason=block,
                        candidate_action=entry_action,
                    )
                    step_notes["events"].append(f"{sym}:blocked:{block}")

            if step_res.entry_candidate:
                entry_candidates.append({"symbol": sym, "bar": bar, "context": ctx, "dossier": dossier})

        if entry_candidates:
            if portfolio.open_position is not None:
                for c in entry_candidates:
                    record_blocked_entry(
                        portfolio,
                        symbol=c["symbol"],
                        signal_ts=c["bar"].ts_utc,
                        reason=BLOCK_REASON_POSITION_OPEN,
                        candidate_action=entry_action,
                    )
                    step_notes["events"].append(f"{c['symbol']}:blocked:{BLOCK_REASON_POSITION_OPEN}")
            else:
                winner, losers = tie_break_pick(entry_candidates, params=params, symbol_order=symbol_order)
                detail = _tie_break_detail(winner, losers, params)
                if execute_entry(portfolio, candidate=winner, bar=winner["bar"], params=params):
                    step_notes["events"].append(f"{winner['symbol']}:entry@{winner['bar'].close}")
                for loser in losers:
                    record_blocked_entry(
                        portfolio,
                        symbol=loser["symbol"],
                        signal_ts=loser["bar"].ts_utc,
                        reason=BLOCK_REASON_TIE_BREAK,
                        candidate_action=entry_action,
                        tie_break_json=detail,
                    )
                    step_notes["events"].append(f"{loser['symbol']}:blocked:{BLOCK_REASON_TIE_BREAK}")

        if step.is_last_bar_of_week and portfolio.open_position and not portfolio.pending_exit:
            pos = portfolio.open_position
            bar = step.bars.get(pos.symbol)
            if bar:
                fill_price = float(bar.close)
                pnl = (fill_price - pos.entry_price) * pos.quantity
                portfolio.cash += fill_price * pos.quantity
                portfolio.realized_pnl += pnl
                portfolio.closed_trades.append(
                    {
                        "trade_id": pos.trade_id,
                        "symbol": pos.symbol,
                        "direction": "LONG",
                        "quantity": pos.quantity,
                        "signal_ts": pos.signal_ts,
                        "entry_ts": pos.entry_ts,
                        "entry_price": pos.entry_price,
                        "exit_decision_ts": bar.ts_utc,
                        "exit_ts": bar.ts_utc,
                        "exit_price": fill_price,
                        "exit_reason": "FORCED_WEEK_END_FLATTEN",
                        "initial_cash": pos.initial_cash,
                        "remaining_cash": portfolio.cash,
                        "realized_pnl": round(pnl, 4),
                    }
                )
                portfolio.open_position = None
                step_notes["events"].append(f"{pos.symbol}:week_end_flatten")

        step_notes["cash_after"] = portfolio.cash
        step_notes["open_position"] = portfolio.open_position.symbol if portfolio.open_position else None
        step_notes["pending_exit"] = portfolio.pending_exit.reason if portfolio.pending_exit else None
        log.append(step_notes)
        portfolio.bars_processed += 1

    return FixtureRunResult(portfolio=portfolio, step_log=log, skipped_duplicate_steps=skipped)


def make_ctx(action: str, *, entry_score: float = 5.0, room: str = "AMPLE_ROOM") -> dict:
    return {
        "selected_action": action,
        "payload_json": {
            "layers_json": {
                "contextual_interpretation": {"room_class": room},
                "diagnostics": {"entry_score": entry_score},
            }
        },
    }


def make_bar(
    sym: str,
    ts: datetime,
    *,
    o: float,
    h: float,
    l: float,
    c: float,
    trading_date=None,
) -> HistoricalBar:
    td = trading_date or ts.date()
    return HistoricalBar(
        symbol=sym,
        trading_date=td,
        ts_utc=ts,
        ts_ny=ts,
        open=o,
        high=h,
        low=l,
        close=c,
        volume=1000,
        source="CERTIFICATION_FIXTURE",
        bar_size_minutes=5,
        rth=True,
    )
