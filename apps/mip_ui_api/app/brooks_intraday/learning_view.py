"""Phase 8 — aggregated learning / simulation review payloads (read-only)."""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo

from .constants import SIMULATION_BANNER
from .context_repository import load_context_observations, load_pattern_snapshot_index
from .historical_bar_repository import load_bars_for_symbol_week
from .learning_constants import (
    BARS_PER_SYMBOL_WEEK,
    OFFICIAL_ATTEMPT_CHAIN,
    PILOT_RUN_ID,
    PILOT_ZERO_TRADE_DETAIL,
    RECONSTRUCTED_DOSSIER_BADGE,
    SIMULATION_ONLY_BANNER,
    ZERO_TRADE_EXPLANATION,
)
from .observation_repository import load_observations
from .pattern_repository import load_bar_pattern_links, load_patterns
from .simulation_certification import HISTORICAL_SIMULATION, run_certification
from .simulation_repository import load_blocked_signals, load_sim_trades, load_simulation_attempt


def _ts_key(ts: Any) -> str:
    if isinstance(ts, datetime):
        return ts.strftime("%Y-%m-%dT%H:%M:%S")
    s = str(ts).replace(" ", "T")
    return s[:19]


def _berlin_ts(ts: Any) -> str | None:
    if ts is None:
        return None
    if isinstance(ts, str):
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00")[:19])
        except ValueError:
            return None
    elif isinstance(ts, datetime):
        dt = ts
    else:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo("America/New_York"))
    return dt.astimezone(ZoneInfo("Europe/Berlin")).strftime("%Y-%m-%d %H:%M:%S")


def resolve_attempt_chain(
    run_id: str,
    state: dict[str, Any],
    cfg: dict[str, Any],
) -> dict[str, str | None]:
    official = OFFICIAL_ATTEMPT_CHAIN.get(run_id, {})
    objective_id = (
        state.get("phase4_review_baseline_attempt_id")
        or cfg.get("phase4_review_baseline_attempt_id")
        or official.get("objective_attempt_id")
    )
    pattern_id = (
        state.get("phase5_pattern_v03_attempt_id")
        or cfg.get("phase5_pattern_v03_attempt_id")
        or state.get("phase5_pattern_attempt_id")
        or cfg.get("phase5_pattern_attempt_id")
        or official.get("pattern_attempt_id")
    )
    context_id = (
        state.get("phase6b_context_attempt_id")
        or cfg.get("phase6b_context_attempt_id")
        or state.get("phase6_context_attempt_id")
        or cfg.get("phase6_context_attempt_id")
        or official.get("context_attempt_id")
    )
    simulation_id = (
        state.get("phase7_simulation_attempt_id")
        or cfg.get("phase7_simulation_attempt_id")
        or official.get("simulation_attempt_id")
    )
    return {
        "objective_attempt_id": str(objective_id) if objective_id else None,
        "objective_ruleset": official.get("objective_ruleset") or "BROOKS_OBJECTIVE_RULESET_V0_1",
        "pattern_attempt_id": str(pattern_id) if pattern_id else None,
        "pattern_ruleset": official.get("pattern_ruleset") or "BROOKS_PATTERN_RULESET_V0_3",
        "context_attempt_id": str(context_id) if context_id else None,
        "context_ruleset": official.get("context_ruleset") or "BROOKS_CONTEXT_RULESET_V0_2",
        "simulation_attempt_id": str(simulation_id) if simulation_id else None,
        "simulation_ruleset": official.get("simulation_ruleset") or "BROOKS_SIMULATION_RULESET_V0_1",
    }


def _bar_freeze_hash(cfg: dict[str, Any]) -> str | None:
    freeze = cfg.get("bar_dataset_freeze") or {}
    if not freeze:
        return None
    parts = sorted(f"{k}={json.dumps(v, sort_keys=True, default=str)}" for k, v in freeze.items())
    return hashlib.sha256("\n".join(parts).encode()).hexdigest()


def _dossier_provenance_summary(run_id: str, dossiers: list[dict[str, Any]]) -> dict[str, Any]:
    origins: dict[str, int] = {}
    hashes: list[str] = []
    for d in dossiers:
        origin = d.get("dossier_origin") or d.get("origin") or "UNKNOWN"
        origins[origin] = origins.get(origin, 0) + 1
        sh = d.get("source_hash") or d.get("source_daily_bar_hash")
        if sh:
            hashes.append(str(sh))
    dossier_hash = hashlib.sha256("\n".join(sorted(hashes)).encode()).hexdigest() if hashes else None
    any_reconstructed = any(
        (d.get("dossier_origin") or d.get("origin")) == "HISTORICAL_RECONSTRUCTION" for d in dossiers
    )
    return {
        "dossier_count": len(dossiers),
        "origins": origins,
        "dossier_aggregate_hash": dossier_hash,
        "any_reconstructed": any_reconstructed,
        "reconstructed_badge": RECONSTRUCTED_DOSSIER_BADGE if any_reconstructed else None,
    }


def build_provenance_header(
    *,
    run_id: str,
    attempts: dict[str, str | None],
    cfg: dict[str, Any],
    dossiers: list[dict[str, Any]],
) -> dict[str, Any]:
    prov = _dossier_provenance_summary(run_id, dossiers)
    return {
        "run_id": run_id,
        "attempt_chain": attempts,
        "bar_dataset_freeze_hash": _bar_freeze_hash(cfg),
        "dossier_provenance": prov,
        "simulation_only_banner": SIMULATION_ONLY_BANNER,
        "simulation_only_status": True,
        "meta_simulation_banner": SIMULATION_BANNER,
    }


def extract_state_transitions(rows: list[dict[str, Any]], *, symbol: str | None = None) -> list[dict[str, Any]]:
    """Meaningful state/action changes only (not every duration bar)."""
    filtered = rows
    if symbol:
        filtered = [r for r in rows if str(r.get("symbol", "")).upper() == symbol.upper()]
    ordered = sorted(filtered, key=lambda r: (_ts_key(r.get("bar_ts")), str(r.get("symbol", ""))))
    out: list[dict[str, Any]] = []
    prev_state: str | None = None
    prev_action: str | None = None
    for row in ordered:
        st = row.get("state_after")
        act = row.get("selected_action")
        if st != prev_state or act != prev_action:
            ts_ny = row.get("bar_ts_ny") or row.get("bar_ts")
            out.append(
                {
                    "bar_ts": row.get("bar_ts"),
                    "bar_ts_ny": ts_ny,
                    "symbol": row.get("symbol"),
                    "state_before": row.get("state_before"),
                    "state_after": st,
                    "selected_action": act,
                    "thesis_effect": row.get("thesis_effect"),
                    "label": st,
                    "time_ny_short": _format_ny_short(ts_ny),
                }
            )
            prev_state = st
            prev_action = act
    return out


def _format_ny_short(ts: Any) -> str:
    s = str(ts)
    if "T" in s:
        part = s.split("T", 1)[1][:5]
        return part
    if " " in s:
        return s.split(" ", 1)[1][:5]
    return s[:5]


def transition_context_markers(context_row: dict[str, Any], *, is_transition: bool) -> list[str]:
    if not is_transition:
        return []
    pj = context_row.get("payload_json") or {}
    return list(pj.get("marker_flags_json") or [])


def _objective_summary(obs: dict[str, Any] | None) -> str:
    if not obs:
        return ""
    terms = obs.get("brooks_obs_json") or []
    names = [t.get("term") for t in terms if isinstance(t, dict) and t.get("term")]
    return ", ".join(names[:8]) + ("…" if len(names) > 8 else "")


def _ohlcv_from_obs_or_bar(obs: dict[str, Any] | None, bar: dict[str, Any] | None) -> dict[str, Any]:
    if obs and obs.get("ohlcv_json"):
        o = obs["ohlcv_json"]
        return {
            "open": o.get("open"),
            "high": o.get("high"),
            "low": o.get("low"),
            "close": o.get("close"),
            "volume": o.get("volume"),
        }
    if bar:
        return {
            "open": bar.get("open"),
            "high": bar.get("high"),
            "low": bar.get("low"),
            "close": bar.get("close"),
            "volume": bar.get("volume"),
        }
    return {}


def _simulation_effect_for_bar(
    bar_ts: str,
    trades: list[dict[str, Any]],
    blocked: list[dict[str, Any]],
) -> str:
    for t in trades:
        if _ts_key(t.get("entry_ts")) == bar_ts:
            return f"ENTRY {t.get('symbol')} qty={t.get('quantity')}"
        if _ts_key(t.get("exit_ts")) == bar_ts:
            return f"EXIT {t.get('exit_reason') or 'CLOSE'}"
    for b in blocked:
        if _ts_key(b.get("signal_ts")) == bar_ts:
            return f"BLOCKED {b.get('block_reason')}"
    return "—"


def build_explanation_sections(
    *,
    obs: dict[str, Any] | None,
    ctx: dict[str, Any] | None,
    patterns: list[dict[str, Any]],
    simulation_effect: str,
    zero_trade_week: bool,
) -> dict[str, Any]:
    pj = (ctx or {}).get("payload_json") or {}
    layers = pj.get("layers_json") or {}
    return {
        "objective_facts": obs.get("explanation") if obs else "No objective observation for this bar.",
        "brooks_patterns": [
            {
                "pattern_family": p.get("pattern_family"),
                "lifecycle": p.get("lifecycle") or p.get("lifecycle_status"),
                "explanation": p.get("explanation"),
            }
            for p in patterns
        ],
        "daily_context": layers.get("contextual_interpretation") or pj.get("daily_context_summary") or "",
        "supporting_evidence": pj.get("supporting_evidence_json") or layers.get("supporting_evidence") or [],
        "blocking_evidence": pj.get("blocking_evidence_json") or layers.get("blocking_evidence") or [],
        "advisory_conclusion": {
            "state_after": (ctx or {}).get("state_after"),
            "selected_action": (ctx or {}).get("selected_action"),
            "thesis_effect": (ctx or {}).get("thesis_effect"),
            "explanation": (ctx or {}).get("explanation"),
        },
        "simulation_result": simulation_effect,
        "zero_trade_note": (
            "No simulated entry occurred because the advisory action did not reach CONSIDER_ENTRY."
            if zero_trade_week and (ctx or {}).get("selected_action") != "CONSIDER_ENTRY"
            else None
        ),
    }


def _apply_filters(
    rows: list[dict[str, Any]],
    *,
    trading_date: date | None,
    pattern_family: str | None,
    lifecycle: str | None,
    thesis_effect: str | None,
    advisory_state: str | None,
    advisory_action: str | None,
    transition_events_only: bool,
    blockers: bool,
    simulated_trade_events: bool,
    action_changed_only: bool,
    meaningful_pattern_no_entry: bool,
) -> list[dict[str, Any]]:
    out = rows
    if trading_date:
        td = trading_date.isoformat()
        out = [r for r in out if str(r.get("trading_date", "")).startswith(td)]
    if pattern_family:
        pf = pattern_family.upper()
        out = [
            r
            for r in out
            if any(pf in str(p.get("pattern_family", "")).upper() for p in (r.get("active_patterns") or []))
        ]
    if lifecycle:
        lc = lifecycle.upper()
        out = [
            r
            for r in out
            if any(str(p.get("lifecycle", "")).upper() == lc for p in (r.get("active_patterns") or []))
        ]
    if thesis_effect:
        out = [r for r in out if str(r.get("thesis_effect", "")).upper() == thesis_effect.upper()]
    if advisory_state:
        out = [r for r in out if str(r.get("state_after", "")).upper() == advisory_state.upper()]
    if advisory_action:
        out = [r for r in out if str(r.get("selected_action", "")).upper() == advisory_action.upper()]
    if transition_events_only:
        out = [r for r in out if r.get("is_state_transition")]
    if blockers:
        out = [r for r in out if r.get("blockers")]
    if simulated_trade_events:
        out = [r for r in out if r.get("simulation_effect") and r.get("simulation_effect") != "—"]
    if action_changed_only:
        out = [r for r in out if r.get("is_action_transition")]
    if meaningful_pattern_no_entry:
        out = [
            r
            for r in out
            if (r.get("active_patterns") or [])
            and str(r.get("selected_action", "")).upper()
            not in ("CONSIDER_ENTRY", "ENTRY_ARMED")
        ]
    return out


def _merge_symbol_grid(
    *,
    bars: list[dict[str, Any]],
    observations: list[dict[str, Any]],
    context_rows: list[dict[str, Any]],
    pattern_links: list[dict[str, Any]],
    trades: list[dict[str, Any]],
    blocked: list[dict[str, Any]],
    symbol: str,
) -> list[dict[str, Any]]:
    obs_by_ts = {_ts_key(o.get("bar_ts")): o for o in observations}
    ctx_by_ts = {_ts_key(c.get("bar_ts")): c for c in context_rows}
    pat_by_ts = {_ts_key(p.get("bar_ts")): p for p in pattern_links}

    bar_list = bars if bars else []
    if not bar_list and (observations or context_rows):
        seen: set[str] = set()
        for src in observations + context_rows:
            k = _ts_key(src.get("bar_ts"))
            if k in seen:
                continue
            seen.add(k)
            bar_list.append({"ts_utc": src.get("bar_ts"), "trading_date": src.get("trading_date")})

    ordered = sorted(bar_list, key=lambda b: _ts_key(b.get("ts_utc")))
    grid: list[dict[str, Any]] = []
    prev_state: str | None = None
    prev_action: str | None = None
    sym_upper = symbol.upper()

    for bar in ordered:
        ts = _ts_key(bar.get("ts_utc"))
        obs = obs_by_ts.get(ts)
        ctx = ctx_by_ts.get(ts)
        plink = pat_by_ts.get(ts)
        patterns = (plink or {}).get("pattern_snapshot_json") or []
        st_after = (ctx or {}).get("state_after")
        action = (ctx or {}).get("selected_action")
        is_state_tr = st_after != prev_state or prev_state is None
        is_action_tr = action != prev_action or prev_action is None
        pj = (ctx or {}).get("payload_json") or {}
        blockers = pj.get("blockers_json") or []
        sim_eff = _simulation_effect_for_bar(ts, trades, blocked)
        dm = (obs or {}).get("derived_metrics_json") or {}
        ohlcv = _ohlcv_from_obs_or_bar(obs, bar)
        grid.append(
            {
                "bar_ts": bar.get("ts_utc") or (obs or {}).get("bar_ts"),
                "bar_ts_ny": bar.get("ts_ny") or (obs or {}).get("bar_ts_ny"),
                "bar_ts_berlin": _berlin_ts(bar.get("ts_ny") or bar.get("ts_utc") or (obs or {}).get("bar_ts")),
                "trading_date": bar.get("trading_date") or (obs or {}).get("trading_date"),
                "symbol": sym_upper,
                "ohlcv": ohlcv,
                "direction": dm.get("direction"),
                "objective_summary": _objective_summary(obs),
                "active_patterns": patterns,
                "thesis_effect": (ctx or {}).get("thesis_effect"),
                "state_before": (ctx or {}).get("state_before"),
                "state_after": st_after,
                "selected_action": action,
                "blockers": blockers,
                "simulation_effect": sim_eff,
                "explanation": (ctx or {}).get("explanation") or (obs or {}).get("explanation"),
                "is_state_transition": is_state_tr and prev_state is not None,
                "is_action_transition": is_action_tr and prev_action is not None,
                "context_transition_markers": transition_context_markers(ctx or {}, is_transition=is_action_tr),
                "objective_terms": obs.get("brooks_obs_json") if obs else [],
                "payload_json": pj,
                "explanation_sections": build_explanation_sections(
                    obs=obs,
                    ctx=ctx,
                    patterns=patterns,
                    simulation_effect=sim_eff,
                    zero_trade_week=not trades,
                ),
            }
        )
        prev_state = st_after
        prev_action = action
    return grid


def _historical_account_summary(
    run_id: str,
    simulation_attempt_id: str | None,
    trades: list[dict[str, Any]],
) -> dict[str, Any]:
    attempt = load_simulation_attempt(simulation_attempt_id) if simulation_attempt_id else None
    starting = float(attempt.get("starting_cash") or 1000) if attempt else 1000.0
    ending = float(attempt.get("ending_cash") or starting) if attempt else starting
    realized = float(attempt.get("realized_pnl") or 0) if attempt else 0.0
    trade_count = int(attempt.get("trade_count") or len(trades)) if attempt else len(trades)
    wins = sum(1 for t in trades if float(t.get("realized_pnl") or 0) > 0)
    losses = sum(1 for t in trades if float(t.get("realized_pnl") or 0) < 0)
    zero_trades = trade_count == 0
    return {
        "starting_cash": starting,
        "ending_cash": ending,
        "available_cash": ending,
        "position_value": 0.0,
        "realized_pnl": realized,
        "unrealized_pnl": 0.0,
        "total_account_value": ending,
        "open_position": None,
        "trades": trade_count,
        "wins": wins,
        "losses": losses,
        "blocked_entry_signals": load_blocked_signals(run_id) if run_id else [],
        "zero_trade_explanation": ZERO_TRADE_EXPLANATION if zero_trades else None,
        "pilot_week_detail": PILOT_ZERO_TRADE_DETAIL if zero_trades and run_id == PILOT_RUN_ID else None,
        "simulation_attempt_id": simulation_attempt_id,
    }


def _cursor_ts(state: dict[str, Any]) -> datetime | None:
    from .replay_engine import get_replay_state as _rs

    rs = _rs(state)
    ts_raw = rs.get("cursor_timestamp_utc") or rs.get("bar_timestamp_utc")
    if not ts_raw:
        return None
    return datetime.fromisoformat(str(ts_raw)[:19])


def _cap_grid_to_replay(grid: list[dict[str, Any]], cursor: datetime | None) -> list[dict[str, Any]]:
    if cursor is None:
        return grid
    ck = _ts_key(cursor)
    return [r for r in grid if _ts_key(r.get("bar_ts")) <= ck]


def build_symbol_learning_payload(
    *,
    run_id: str,
    symbol: str,
    state: dict[str, Any],
    cfg: dict[str, Any],
    attempts: dict[str, str | None],
    mode: str = "full",
    trading_date: date | None = None,
    offset: int = 0,
    limit: int = 500,
    filters: dict[str, Any] | None = None,
    dossier: dict[str, Any] | None = None,
) -> dict[str, Any]:
    sym = symbol.upper()
    week_start = state.get("selected_week_start") or cfg.get("selected_week_start")
    if week_start and not isinstance(week_start, date):
        week_start = date.fromisoformat(str(week_start)[:10])

    bars_raw = []
    if week_start:
        bars_raw = load_bars_for_symbol_week(sym, week_start)
    bars = [
        {
            "ts_utc": b.ts_utc,
            "ts_ny": b.ts_ny,
            "trading_date": b.trading_date,
            "open": b.open,
            "high": b.high,
            "low": b.low,
            "close": b.close,
            "volume": b.volume,
        }
        for b in bars_raw
    ]

    obj_id = attempts.get("objective_attempt_id")
    ctx_id = attempts.get("context_attempt_id")
    pat_id = attempts.get("pattern_attempt_id")

    observations = (
        load_observations(run_id, sym, replay_attempt_id=obj_id, limit=5000) if obj_id else []
    )
    context_rows = (
        load_context_observations(run_id, context_attempt_id=str(ctx_id), symbol=sym, limit=5000)
        if ctx_id
        else []
    )
    pattern_links = (
        load_bar_pattern_links(run_id, replay_attempt_id=str(pat_id), symbol=sym, limit=5000) if pat_id else []
    )
    trades = load_sim_trades(run_id)
    blocked = load_blocked_signals(run_id)

    grid = _merge_symbol_grid(
        bars=bars,
        observations=observations,
        context_rows=context_rows,
        pattern_links=pattern_links,
        trades=trades,
        blocked=blocked,
        symbol=sym,
    )

    if mode == "replay":
        grid = _cap_grid_to_replay(grid, _cursor_ts(state))

    flt = filters or {}
    grid = _apply_filters(
        grid,
        trading_date=trading_date,
        pattern_family=flt.get("pattern_family"),
        lifecycle=flt.get("lifecycle"),
        thesis_effect=flt.get("thesis_effect"),
        advisory_state=flt.get("advisory_state"),
        advisory_action=flt.get("advisory_action"),
        transition_events_only=bool(flt.get("transition_events_only")),
        blockers=bool(flt.get("blockers")),
        simulated_trade_events=bool(flt.get("simulated_trade_events")),
        action_changed_only=bool(flt.get("action_changed_only")),
        meaningful_pattern_no_entry=bool(flt.get("meaningful_pattern_no_entry")),
    )

    total = len(grid)
    chart_bars = [_chart_bar(r) for r in grid]
    page = grid[offset : offset + limit]

    transitions = extract_state_transitions(context_rows, symbol=sym)
    if mode == "replay":
        cursor = _cursor_ts(state)
        if cursor:
            ck = _ts_key(cursor)
            transitions = [t for t in transitions if _ts_key(t.get("bar_ts")) <= ck]

    daily_levels = _daily_levels_from_dossier(dossier)
    patterns_catalog = (
        load_patterns(run_id, replay_attempt_id=str(pat_id), symbol=sym, limit=5000) if pat_id else []
    )

    return {
        "run_id": run_id,
        "symbol": sym,
        "mode": mode,
        "attempt_chain": attempts,
        "bars_per_symbol_expected": BARS_PER_SYMBOL_WEEK,
        "grid_total": total,
        "grid_offset": offset,
        "grid_limit": limit,
        "grid_rows": page,
        "state_transitions": transitions,
        "patterns": patterns_catalog,
        "daily_levels": daily_levels,
        "chart_bars": chart_bars,
    }


def _chart_bar(row: dict[str, Any]) -> dict[str, Any]:
    o = row.get("ohlcv") or {}
    return {
        "ts_utc": row.get("bar_ts"),
        "ts_ny": row.get("bar_ts_ny"),
        "open": o.get("open"),
        "high": o.get("high"),
        "low": o.get("low"),
        "close": o.get("close"),
        "volume": o.get("volume"),
        "transition_markers": row.get("context_transition_markers") or [],
        "active_patterns": row.get("active_patterns") or [],
        "objective_terms": row.get("objective_terms") or [],
        "selected_action": row.get("selected_action"),
        "simulation_effect": row.get("simulation_effect"),
    }


def _daily_levels_from_dossier(dossier: dict[str, Any] | None) -> dict[str, Any]:
    if not dossier:
        return {}
    derived = dossier.get("derived_levels") or {}
    inv = dossier.get("invalidation_level")
    return {
        "primary_support": _zone_mid(dossier.get("support_zones")),
        "secondary_support": _secondary_support(dossier),
        "resistance": _zone_mid(dossier.get("resistance_zones")),
        "reclaim_level": dossier.get("reclaim_level"),
        "do_not_chase_level": dossier.get("do_not_chase_level"),
        "daily_thesis_invalidation": inv,
        "intraday_setup_invalidation": derived.get("intraday_setup_invalidation", {}).get("level")
        if isinstance(derived.get("intraday_setup_invalidation"), dict)
        else derived.get("intraday_setup_invalidation"),
        "entry_zone": derived.get("entry_zone"),
        "simulated_trade_stop": None,
    }


def _zone_mid(zones: Any) -> float | None:
    if not zones or not isinstance(zones, list):
        return None
    z0 = zones[0]
    if isinstance(z0, dict):
        lo, hi = z0.get("low"), z0.get("high")
        if lo is not None and hi is not None:
            return (float(lo) + float(hi)) / 2
    return None


def _secondary_support(dossier: dict[str, Any]) -> float | None:
    zones = dossier.get("support_zones") or []
    if len(zones) < 2:
        return None
    z1 = zones[1]
    if isinstance(z1, dict):
        lo, hi = z1.get("low"), z1.get("high")
        if lo is not None and hi is not None:
            return (float(lo) + float(hi)) / 2
    return None


def build_run_learning_payload(
    *,
    run_id: str,
    state: dict[str, Any],
    cfg: dict[str, Any],
    symbols: list[str],
    dossiers: list[dict[str, Any]],
    mode: str = "full",
    active_symbol: str | None = None,
    active_trading_date: date | None = None,
) -> dict[str, Any]:
    attempts = resolve_attempt_chain(run_id, state, cfg)
    provenance = build_provenance_header(run_id=run_id, attempts=attempts, cfg=cfg, dossiers=dossiers)
    trades = load_sim_trades(run_id)
    account = _historical_account_summary(run_id, attempts.get("simulation_attempt_id"), trades)

    week_start = state.get("selected_week_start") or cfg.get("selected_week_start")
    replay_progress = None
    try:
        from .replay_engine import get_replay_state as _rs

        rs = _rs(state)
        replay_progress = {
            "completed_steps": rs.get("completed_steps"),
            "total_steps": rs.get("total_steps"),
            "cursor_timestamp_utc": rs.get("cursor_timestamp_utc"),
            "replay_timestamp_ny": rs.get("replay_timestamp_ny"),
        }
    except Exception:
        pass

    symbol_overviews = []
    ctx_id = attempts.get("context_attempt_id")
    pat_id = attempts.get("pattern_attempt_id")
    all_context = (
        load_context_observations(run_id, context_attempt_id=str(ctx_id), limit=6000) if ctx_id else []
    )
    pat_index = (
        load_pattern_snapshot_index(run_id, pattern_attempt_id=str(pat_id)) if pat_id else {}
    )

    for sym in symbols:
        sym_u = sym.upper()
        sym_ctx = [c for c in all_context if str(c.get("symbol", "")).upper() == sym_u]
        if mode == "replay":
            cursor = _cursor_ts(state)
            if cursor:
                ck = _ts_key(cursor)
                sym_ctx = [c for c in sym_ctx if _ts_key(c.get("bar_ts")) <= ck]
        latest = sym_ctx[-1] if sym_ctx else {}
        ts_key = _ts_key(latest.get("bar_ts")) if latest else ""
        pats = pat_index.get((sym_u, ts_key), []) if ts_key else []
        meaningful = [p for p in pats if str(p.get("lifecycle", "")).upper() in ("CONFIRMED", "DEVELOPING")]
        dossier_match = _pick_dossier(dossiers, sym_u, active_trading_date)
        symbol_overviews.append(
            {
                "symbol": sym_u,
                "paa_verdict": dossier_match.get("paa_verdict") if dossier_match else None,
                "current_state": latest.get("state_after"),
                "latest_advisory_action": latest.get("selected_action"),
                "thesis_effect": latest.get("thesis_effect"),
                "active_meaningful_patterns": meaningful,
                "nearest_support": dossier_match.get("support_zones") if dossier_match else None,
                "nearest_resistance": dossier_match.get("resistance_zones") if dossier_match else None,
                "reclaim_state": _reclaim_from_payload(latest),
                "do_not_chase_status": latest.get("selected_action") == "DO_NOT_CHASE",
                "simulated_position": None,
            }
        )

    return {
        "provenance": provenance,
        "mode": mode,
        "selected_week_start": str(week_start)[:10] if week_start else None,
        "replay_progress": replay_progress,
        "account_summary": account,
        "symbol_overviews": symbol_overviews,
        "active_symbol": active_symbol,
        "certification_view_label": "SIMULATION ENGINE CERTIFICATION — SYNTHETIC FIXTURES",
    }


def _pick_dossier(dossiers: list[dict[str, Any]], symbol: str, td: date | None) -> dict[str, Any]:
    if not dossiers:
        return {}
    if td:
        for d in dossiers:
            if str(d.get("symbol", "")).upper() == symbol and str(d.get("trading_date", "")).startswith(td.isoformat()):
                return d
    for d in dossiers:
        if str(d.get("symbol", "")).upper() == symbol:
            return d
    return {}


def _reclaim_from_payload(ctx_row: dict[str, Any]) -> str | None:
    pj = ctx_row.get("payload_json") or {}
    layers = pj.get("layers_json") or {}
    ci = layers.get("contextual_interpretation") or {}
    return ci.get("reclaim_state") or ci.get("reclaim_status")


def build_account_timeline(run_id: str, simulation_attempt_id: str | None) -> dict[str, Any]:
    trades = load_sim_trades(run_id)
    blocked = load_blocked_signals(run_id)
    account = _historical_account_summary(run_id, simulation_attempt_id, trades)
    events: list[dict[str, Any]] = []
    for t in trades:
        events.append(
            {
                "kind": "TRADE_ENTRY",
                "ts": t.get("entry_ts"),
                "symbol": t.get("symbol"),
                "detail": t,
            }
        )
        if t.get("exit_ts"):
            events.append(
                {
                    "kind": "TRADE_EXIT",
                    "ts": t.get("exit_ts"),
                    "symbol": t.get("symbol"),
                    "detail": t,
                }
            )
    for b in blocked:
        events.append(
            {
                "kind": "BLOCKED_ENTRY",
                "ts": b.get("signal_ts"),
                "symbol": b.get("symbol"),
                "detail": b,
            }
        )
    events.sort(key=lambda e: _ts_key(e.get("ts")))
    return {
        "run_id": run_id,
        "simulation_attempt_id": simulation_attempt_id,
        "account": account,
        "events": events,
        "fixture_isolation": "Certification fixture trades are never persisted to BROOKS_INTRADAY_SIM_TRADE.",
    }


def build_certification_summary(*, include_fixtures: bool = False) -> dict[str, Any]:
    report = run_certification()
    fixtures = report.get("fixtures") or []
    summary = {
        "label": "SIMULATION ENGINE CERTIFICATION — SYNTHETIC FIXTURES",
        "scenarios_passed": sum(1 for f in fixtures if f.get("pass")),
        "scenarios_total": len(fixtures),
        "all_pass": report.get("all_pass"),
        "fixture_sequence_hash": report.get("fixture_sequence_hash"),
        "historical_simulation_attempt_id": HISTORICAL_SIMULATION,
        "persistence_isolation": report.get("persistence_isolation"),
        "checks": [
            "fill_model",
            "cash_accounting",
            "one_position",
            "deterministic_tie_break",
        ],
        "must_not": [
            "add fixture P/L to historical account",
            "show fixture entries on historical charts",
            "mix fixture trades into historical trade APIs",
        ],
    }
    if include_fixtures:
        summary["fixtures"] = fixtures
    return summary


def build_state_transitions_payload(
    run_id: str,
    attempts: dict[str, str | None],
    *,
    symbol: str | None = None,
    mode: str = "full",
    state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    ctx_id = attempts.get("context_attempt_id")
    if not ctx_id:
        return {"run_id": run_id, "transitions": [], "total": 0}
    rows = load_context_observations(run_id, context_attempt_id=str(ctx_id), symbol=symbol, limit=6000)
    transitions = extract_state_transitions(rows, symbol=symbol)
    if mode == "replay" and state:
        cursor = _cursor_ts(state)
        if cursor:
            ck = _ts_key(cursor)
            transitions = [t for t in transitions if _ts_key(t.get("bar_ts")) <= ck]
    return {"run_id": run_id, "symbol": symbol, "transitions": transitions, "total": len(transitions)}
