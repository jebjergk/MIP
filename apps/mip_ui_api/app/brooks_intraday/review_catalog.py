"""Lightweight review catalog for Learning / Technical lab selection (read-only)."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from . import store
from .experiment_analytics import list_validation_run_ids
from .learning_constants import (
    PHASE_E1_VALIDATION_RUN_ID,
    PM_V01_CERT_SIMULATION_ATTEMPT_ID,
)
from .learning_view import list_review_chain_options
from .repository import load_dossiers_for_run
from .simulation_repository import load_sim_trades
from .adviser_foundation_ui import (
    ADVISER_EMPTY_STATE_MESSAGE,
    V1_NO_ACTIVE_SESSIONS_MESSAGE,
    filter_chains_for_ui,
    foundation_catalog_defaults,
)
from .adviser_v1_learning_view import AMZN_V1_STATUS, V1_LEARNING_DEFAULTS, list_v1_validation_sessions
from .trade_learning_g1 import CANONICAL_G1_DEFAULTS

# PM V0_1 certification trade is review-only (not always in SIM_TRADE).
_PM_CERT_CATALOG_TRADE: dict[str, Any] = {
    "trade_id": "e0d236d5-c281-4c5c-8479-4c6a10f67356",
    "symbol": "AMZN",
    "trading_date": "2026-07-13",
    "entry_ts": "2026-07-13T14:25:00",
    "exit_ts": "2026-07-13T15:45:00",
    "realized_pnl": 6.88,
}


def _parse_date(val: Any) -> date | None:
    if val is None:
        return None
    if isinstance(val, date):
        return val
    s = str(val)[:10]
    try:
        return date.fromisoformat(s)
    except ValueError:
        return None


def _week_bounds(run) -> tuple[str | None, str | None]:
    ws = _parse_date(run.selected_week_start)
    we = None
    prep = run.preparation or {}
    matrix = prep.get("matrix") or []
    dates = sorted({str(r.get("trading_date") or "")[:10] for r in matrix if r.get("trading_date")})
    if dates:
        we = dates[-1]
    elif ws:
        we = (ws + timedelta(days=4)).isoformat()
    return (ws.isoformat() if ws else None, we)


def _chain_primary_label(chain: dict[str, Any]) -> str:
    if chain.get("official"):
        return "Official baseline"
    if chain.get("canonical_pm_certification"):
        pnl = chain.get("realized_pnl")
        if pnl is not None:
            sign = "+" if float(pnl) >= 0 else ""
            return f"Canonical position-management review · {sign}${float(pnl):.2f}"
        return "Canonical position-management review"
    if chain.get("obsolete_certification"):
        return "Obsolete certification — do not use"
    sim = str(chain.get("simulation_ruleset") or "")
    if "POSITION_MANAGEMENT" in sim.upper():
        return "Position-management review"
    ctx = str(chain.get("context_ruleset") or "Context review")
    return ctx.replace("BROOKS_", "").replace("_", " ").title()


def _chain_sort_key(chain: dict[str, Any]) -> tuple[int, str]:
    if chain.get("official"):
        return (0, "")
    if chain.get("canonical_pm_certification"):
        return (1, "")
    if chain.get("obsolete_certification"):
        return (9, str(chain.get("simulation_attempt_id") or ""))
    if chain.get("inactive_pm_ruleset"):
        return (3, "")
    return (2, str(chain.get("simulation_attempt_id") or ""))


def _flatten_chains(chain_payload: dict[str, Any]) -> list[dict[str, Any]]:
    official = dict(chain_payload.get("official") or {})
    official["official"] = True
    rows = [official]
    for alt in chain_payload.get("alternatives") or []:
        rows.append(dict(alt))
    rows.sort(key=_chain_sort_key)
    out: list[dict[str, Any]] = []
    for ch in rows:
        out.append(
            {
                "context_attempt_id": ch.get("context_attempt_id"),
                "simulation_attempt_id": ch.get("simulation_attempt_id"),
                "primary_label": _chain_primary_label(ch),
                "technical_label": ch.get("label"),
                "official": bool(ch.get("official")),
                "canonical_pm_certification": bool(ch.get("canonical_pm_certification")),
                "obsolete_certification": bool(ch.get("obsolete_certification")),
                "disabled": bool(ch.get("obsolete_certification")),
                "trade_count": int(ch.get("trade_count") or 0),
                "realized_pnl": ch.get("realized_pnl"),
                "context_ruleset": ch.get("context_ruleset"),
                "simulation_ruleset": ch.get("simulation_ruleset"),
            }
        )
    return out


def _ts_iso(val: Any) -> str | None:
    if val is None:
        return None
    if hasattr(val, "isoformat"):
        return val.isoformat(sep="T", timespec="seconds")
    return str(val).replace(" ", "T")[:19]


def _catalog_trades_for_chain(run_id: str, sim_id: str | None) -> list[dict[str, Any]]:
    if not sim_id:
        return []
    trades = load_sim_trades(run_id, simulation_attempt_id=sim_id, allow_legacy_fallback=False)
    if (
        not trades
        and run_id == PHASE_E1_VALIDATION_RUN_ID
        and sim_id == PM_V01_CERT_SIMULATION_ATTEMPT_ID
    ):
        trades = [dict(_PM_CERT_CATALOG_TRADE)]
    out: list[dict[str, Any]] = []
    for t in trades:
        td = str(t.get("entry_ts") or t.get("signal_ts") or "")[:10]
        if not td and t.get("trading_date"):
            td = str(t["trading_date"])[:10]
        out.append(
            {
                "trade_id": t.get("trade_id"),
                "symbol": str(t.get("symbol") or "").upper(),
                "trading_date": td,
                "entry_ts": _ts_iso(t.get("entry_ts")),
                "exit_ts": _ts_iso(t.get("exit_ts")),
                "realized_pnl": float(t.get("realized_pnl") or 0),
            }
        )
    out.sort(key=lambda x: (x.get("trading_date") or "", x.get("entry_ts") or ""))
    return out


def _symbol_sessions(run_id: str, symbols: list[str]) -> list[dict[str, str]]:
    dossiers = load_dossiers_for_run(run_id)
    seen: set[tuple[str, str]] = set()
    rows: list[dict[str, str]] = []
    for d in dossiers:
        sym = str(d.get("symbol") or "").upper()
        td = str(d.get("trading_date") or "")[:10]
        if not sym or not td:
            continue
        key = (sym, td)
        if key in seen:
            continue
        seen.add(key)
        rows.append({"symbol": sym, "trading_date": td})
    if not rows and symbols:
        ws = None
        run = store.get_run(run_id)
        if run:
            ws = _parse_date(run.selected_week_start)
        if ws:
            for i in range(5):
                td = (ws + timedelta(days=i)).isoformat()
                for sym in symbols:
                    rows.append({"symbol": sym.upper(), "trading_date": td})
    rows.sort(key=lambda r: (r["trading_date"], r["symbol"]))
    return rows


def _default_chain(chains: list[dict[str, Any]]) -> dict[str, Any] | None:
    for ch in chains:
        if ch.get("canonical_pm_certification") and not ch.get("disabled"):
            return ch
    for ch in chains:
        if ch.get("official"):
            return ch
    for ch in chains:
        if not ch.get("disabled"):
            return ch
    return chains[0] if chains else None


def _run_has_learning_data(chains: list[dict[str, Any]]) -> bool:
    return any(
        (c.get("trade_count") or 0) > 0 or c.get("canonical_pm_certification")
        for c in chains
        if not c.get("obsolete_certification")
    )


def build_run_catalog_entry(run_id: str, *, diagnostic_legacy: bool = False) -> dict[str, Any] | None:
    run = store.get_run(run_id)
    if not run:
        return None
    with store._lock:
        state = dict(store._runs.get(run_id) or {})
    cfg = dict(run.configuration or {})
    week_start, week_end = _week_bounds(run)
    chain_payload = list_review_chain_options(run_id, state, cfg, diagnostic_legacy=diagnostic_legacy)
    chains = _flatten_chains(chain_payload)
    chains = filter_chains_for_ui(chains, diagnostic_legacy=diagnostic_legacy)
    default_chain = _default_chain(chains) if diagnostic_legacy else None
    if not diagnostic_legacy and chains:
        default_chain = chains[0]
    chains_out: list[dict[str, Any]] = []
    for ch in chains:
        sim_id = str(ch.get("simulation_attempt_id") or "")
        trades = _catalog_trades_for_chain(run_id, sim_id) if diagnostic_legacy else []
        chains_out.append({**ch, "available_trades": trades})
    symbols = [str(s).upper() for s in (run.symbols or [])]
    entry = {
        "run_id": run_id,
        "week_start": week_start,
        "week_end": week_end,
        "status": run.status,
        "symbols": symbols,
        "completed_trade_count": sum(len(c.get("available_trades") or []) for c in chains_out),
        "has_learning_view_data": bool(chains_out) and (
            diagnostic_legacy
            or any(is_adviser_foundation_chain(c) for c in chains_out)
            or (not diagnostic_legacy and bool(list_v1_validation_sessions()))
        ),
        "available_chains": chains_out,
        "default_chain": {
            "context_attempt_id": default_chain.get("context_attempt_id"),
            "simulation_attempt_id": default_chain.get("simulation_attempt_id"),
        }
        if default_chain
        else None,
        "available_symbol_sessions": _symbol_sessions(run_id, symbols),
        "adviser_empty_state": ADVISER_EMPTY_STATE_MESSAGE if not chains_out and not diagnostic_legacy else None,
    }
    return entry


def is_adviser_foundation_chain(chain: dict[str, Any]) -> bool:
    from .adviser_foundation_ui import is_adviser_foundation_chain as _is

    return _is(chain)


def build_review_catalog(*, run_id: str | None = None, diagnostic_legacy: bool = False) -> dict[str, Any]:
    """Aggregate validation runs with chains, trades, and symbol sessions."""
    v1_sessions: list[dict[str, Any]] = []
    if not diagnostic_legacy:
        v1_sessions = list_v1_validation_sessions()
        runs: list[dict[str, Any]] = []
    elif run_id:
        entry = build_run_catalog_entry(run_id, diagnostic_legacy=diagnostic_legacy)
        runs = [entry] if entry else []
    else:
        ids = [str(r.get("run_id")) for r in list_validation_run_ids() if r.get("run_id")]
        runs = []
        for rid in ids:
            entry = build_run_catalog_entry(rid, diagnostic_legacy=diagnostic_legacy)
            if entry:
                runs.append(entry)
        runs.sort(key=lambda r: r.get("week_start") or "", reverse=True)
    foundation_defaults = foundation_catalog_defaults(runs[0] if runs else None)
    if not diagnostic_legacy:
        fd = dict(V1_LEARNING_DEFAULTS)
        fd["review_mode"] = "sessions"
        fd["context_attempt_id"] = fd["adviser_attempt_id"]
        defaults = fd
        if v1_sessions:
            pin = next(
                (s for s in v1_sessions if s.get("adviser_attempt_id") == fd.get("adviser_attempt_id")),
                v1_sessions[0],
            )
            defaults["run_id"] = pin.get("run_id") or fd["run_id"]
            defaults["symbol"] = pin.get("symbol") or fd["symbol"]
            defaults["trading_date"] = pin.get("trading_date") or fd["trading_date"]
            defaults["adviser_attempt_id"] = pin.get("adviser_attempt_id")
            defaults["simulation_attempt_id"] = pin.get("simulation_attempt_id")
        else:
            defaults["adviser_attempt_id"] = None
            defaults["simulation_attempt_id"] = None
            defaults["context_attempt_id"] = None
    else:
        defaults = dict(CANONICAL_G1_DEFAULTS)
    empty_msg = (
        V1_NO_ACTIVE_SESSIONS_MESSAGE
        if not diagnostic_legacy and not v1_sessions
        else ADVISER_EMPTY_STATE_MESSAGE
    )
    return {
        "runs": runs,
        "defaults": defaults,
        "v1_validation_sessions": v1_sessions,
        "amzn_v1_status": AMZN_V1_STATUS,
        "canonical_trade_id": _PM_CERT_CATALOG_TRADE["trade_id"] if diagnostic_legacy else None,
        "catalog_mode": "diagnostic_legacy" if diagnostic_legacy else "adviser_foundation",
        "diagnostic_legacy": diagnostic_legacy,
        "adviser_empty_state": empty_msg,
        "note": (
            "Forensic legacy chains available only with ?diagnostic_legacy=true."
            if not diagnostic_legacy
            else "Read-only diagnostic catalog; does not mutate pins or rerun simulations."
        ),
    }
