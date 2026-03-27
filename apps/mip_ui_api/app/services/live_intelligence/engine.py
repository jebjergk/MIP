"""Deterministic intelligence step — no Snowflake, no AI."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.live_intelligence.analog import match_analogs
from app.services.live_intelligence.portfolio_regime import detect_portfolio_regime, merge_pairwise_from_bootstrap
from app.services.live_intelligence.simulator import simulate_actions
from app.services.live_intelligence.worlds import build_scenario_worlds

EXIT_RANK = {"HOLD": 0, "MONITOR": 1, "PREPARE": 2, "EXIT_NOW": 3}
THESIS_RANK = {"THESIS_INTACT": 0, "THESIS_STRETCHED": 1, "THESIS_DAMAGED": 2, "THESIS_BROKEN": 3}
NOVELTY_RANK = {"NORMAL": 0, "STRETCHED": 1, "OUTSIDE_COMFORT_ZONE": 2}


def _to_f(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _pct_change(curr: float | None, prev: float | None) -> float | None:
    if curr is None or prev is None or prev == 0:
        return None
    return (curr / prev) - 1


def _live_features(tile: dict[str, Any]) -> dict[str, Any]:
    bars = (tile.get("chart") or {}).get("bars") or []
    if not bars:
        return {
            "ret_5m": None,
            "ret_15m": None,
            "vol_15m": 0.01,
            "pattern_label": "WEAK_DRIFT",
            "inside_cone": None,
            "deviation_median": None,
            "deviation_lower": None,
        }
    step_sec = 3600
    bs = (tile.get("chart") or {}).get("bar_seconds")
    im = (tile.get("chart") or {}).get("interval_minutes")
    if _to_f(bs):
        step_sec = int(float(bs))
    elif _to_f(im):
        step_sec = int(float(im) * 60)
    last = bars[-1]
    close = _to_f(last.get("close")) or _to_f(tile.get("current_price"))
    n5 = max(1, round((5 * 60) / step_sec))
    n15 = max(1, round((15 * 60) / step_sec))
    prev5 = bars[max(0, len(bars) - 1 - n5)]
    prev15 = bars[max(0, len(bars) - 1 - n15)]
    ret5 = _pct_change(close, _to_f(prev5.get("close")))
    ret15 = _pct_change(close, _to_f(prev15.get("close")))
    vol_window = min(len(bars), max(16, round((16 * 3600) / step_sec)))
    slice_bars = bars[-vol_window:]
    closes = [_to_f(b.get("close")) for b in slice_bars if _to_f(b.get("close")) is not None]
    rets = []
    for i in range(1, len(closes)):
        if closes[i - 1] and closes[i - 1] != 0:
            rets.append((closes[i] / closes[i - 1]) - 1)
    mean_r = sum(rets) / len(rets) if rets else 0.0
    var = sum((r - mean_r) ** 2 for r in rets) / len(rets) if rets else 0.0
    vol15 = var**0.5

    pat = "WEAK_DRIFT"
    if vol15 > 0.022:
        pat = "VOLATILITY_SPIKE"
    elif (ret15 or 0) < -0.012:
        pat = "RISK_OFF_BREAKDOWN"
    elif (ret15 or 0) > 0.009:
        pat = "TREND_CONTINUATION"

    exp = tile.get("expectation") or {}
    center_path = exp.get("center_path") or []
    lower_path = exp.get("lower_path") or []
    upper_path = exp.get("upper_path") or []
    c0 = _to_f(center_path[0].get("price")) if center_path else None
    l0 = _to_f(lower_path[0].get("price")) if lower_path else None
    u0 = _to_f(upper_path[0].get("price")) if upper_path else None
    current = close
    inside = None
    if current is not None and l0 is not None and u0 is not None:
        lo, hi = min(l0, u0), max(l0, u0)
        inside = lo <= current <= hi
    dev_med = ((current / c0) - 1) if current and c0 else None
    dev_lo = ((current / l0) - 1) if current and l0 else None

    return {
        "ret_5m": ret5,
        "ret_15m": ret15,
        "vol_15m": vol15,
        "pattern_label": pat,
        "inside_cone": inside,
        "deviation_median": dev_med,
        "deviation_lower": dev_lo,
    }


def _map_legacy_thesis_to_fracture(legacy_status: str, feats: dict[str, Any], dist_sl_pct: float | None) -> str:
    s = str(legacy_status or "WEAKENING").upper()
    if s == "INVALIDATED":
        return "THESIS_BROKEN"
    if s == "THESIS_INTACT":
        if feats.get("inside_cone") is False or (feats.get("deviation_lower") or 0) < -0.02:
            return "THESIS_STRETCHED"
        return "THESIS_INTACT"
    # WEAKENING
    d_sl = dist_sl_pct if dist_sl_pct is not None else 0.1
    if d_sl < 0.02:
        return "THESIS_DAMAGED"
    if feats.get("inside_cone") is False:
        return "THESIS_DAMAGED"
    return "THESIS_STRETCHED"


def _compute_exit_urgency(tile: dict[str, Any], feats: dict[str, Any], thesis_fracture: str) -> str:
    side = str(tile.get("side") or "LONG").upper()
    current = _to_f(tile.get("current_price"))
    sl = _to_f((tile.get("overlays") or {}).get("stop_loss"))
    tp = _to_f((tile.get("overlays") or {}).get("take_profit"))
    urgency = "HOLD"

    def max_u(cand: str) -> None:
        nonlocal urgency
        if EXIT_RANK.get(cand, 0) > EXIT_RANK.get(urgency, 0):
            urgency = cand

    if sl is not None and current:
        dist_sl = (current - sl) / current if side == "LONG" else (sl - current) / current
        if dist_sl < 0:
            max_u("EXIT_NOW")
        elif dist_sl < 0.005:
            max_u("EXIT_NOW")
        elif dist_sl < 0.015:
            max_u("PREPARE")

    if tp is not None and current:
        dist_tp = (tp - current) / current if side == "LONG" else (current - tp) / current
        if dist_tp <= 0 or dist_tp < 0.005:
            max_u("PREPARE")

    if thesis_fracture == "THESIS_BROKEN":
        max_u("PREPARE")
    elif thesis_fracture == "THESIS_DAMAGED":
        max_u("MONITOR")

    if feats.get("pattern_label") == "RISK_OFF_BREAKDOWN":
        max_u("PREPARE")
    elif feats.get("pattern_label") == "VOLATILITY_SPIKE":
        max_u("MONITOR")

    if (feats.get("deviation_lower") or 0) < -0.01:
        max_u("MONITOR")

    return urgency


def _compute_novelty(tile: dict[str, Any], feats: dict[str, Any], analog_quality: float) -> str:
    vol_ctx = tile.get("volatility_context") or {}
    st = str(vol_ctx.get("status") or "")
    if st == "LIVE_VOL_ABOVE_TRAINED_REGIME" and (feats.get("vol_15m") or 0) > 0.025:
        return "OUTSIDE_COMFORT_ZONE"
    if analog_quality < 0.12 and (feats.get("vol_15m") or 0) > 0.018:
        return "STRETCHED"
    if st == "LIVE_VOL_BELOW_TRAINED_REGIME":
        return "NORMAL"
    return "NORMAL"


def _attention_score(
    exit_urgency: str,
    novelty: str,
    thesis_fracture: str,
    prior: dict[str, Any] | None,
    dist_sl_pct: float | None,
) -> float:
    base = 20.0
    base += EXIT_RANK.get(exit_urgency, 0) * 18
    base += NOVELTY_RANK.get(novelty, 0) * 12
    base += THESIS_RANK.get(thesis_fracture, 0) * 10
    if dist_sl_pct is not None and dist_sl_pct < 0.03:
        base += 15
    if prior:
        pu = prior.get("exit_urgency")
        if pu != exit_urgency:
            base += 8
    return float(_clamp(base, 0, 100))


def _material(
    symbol: str,
    prior: dict[str, Any] | None,
    next_intel: dict[str, Any],
) -> bool:
    if not prior:
        return True
    keys = ["exit_urgency", "thesis_fracture", "novelty_state", "pattern_label"]
    for k in keys:
        if prior.get(k) != next_intel.get(k):
            return True
    if abs(float(prior.get("attention_score") or 0) - float(next_intel.get("attention_score") or 0)) >= 12:
        return True
    return False


def _why_now(prior: dict[str, Any] | None, nxt: dict[str, Any]) -> dict[str, Any]:
    crossed = []
    if not prior:
        return {
            "human": "Initial intelligence baseline for this session.",
            "machine": {"initial": True},
            "crossed_thresholds": crossed,
        }
    if prior.get("exit_urgency") != nxt.get("exit_urgency"):
        crossed.append(f"exit_urgency:{prior.get('exit_urgency')}->{nxt.get('exit_urgency')}")
    if prior.get("thesis_fracture") != nxt.get("thesis_fracture"):
        crossed.append(f"thesis_fracture:{prior.get('thesis_fracture')}->{nxt.get('thesis_fracture')}")
    if prior.get("novelty_state") != nxt.get("novelty_state"):
        crossed.append(f"novelty:{prior.get('novelty_state')}->{nxt.get('novelty_state')}")
    human = (
        f"State change vs prior cycle: {', '.join(crossed) if crossed else 'No threshold crossings; values refreshed.'}"
    )
    return {"human": human, "machine": {"deltas": crossed}, "crossed_thresholds": crossed}


def run_deterministic_step(body: dict[str, Any]) -> dict[str, Any]:
    positions: list[dict[str, Any]] = list(body.get("positions") or [])
    prior_map: dict[str, dict[str, Any]] = dict(body.get("prior_intelligence") or {})
    peaks: dict[str, float] = {k.upper(): float(v) for k, v in (body.get("session_peak_pnl_by_symbol") or {}).items()}
    analog_by_sym: dict[str, list[dict[str, Any]]] = {
        k.upper(): list(v) for k, v in (body.get("analog_episodes_by_symbol") or {}).items()
    }
    portfolio_ctx_in = body.get("portfolio_context") or {}

    intelligence: dict[str, dict[str, Any]] = {}
    feed_events: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc).isoformat()

    for tile in positions:
        sym = str(tile.get("symbol") or "").upper()
        if not sym:
            continue
        prior = prior_map.get(sym)
        feats = _live_features(tile)
        legacy_thesis = (tile.get("thesis") or {}).get("status") or "WEAKENING"
        dist_sl = _to_f((tile.get("progress_metrics") or {}).get("distance_to_sl_pct"))
        thesis_fracture = _map_legacy_thesis_to_fracture(str(legacy_thesis), feats, dist_sl)

        analog_summary = match_analogs(tile, analog_by_sym.get(sym) or [])
        novelty = _compute_novelty(tile, feats, float(analog_summary.get("match_quality") or 0))

        exit_urg = _compute_exit_urgency(tile, feats, thesis_fracture)
        attn = _attention_score(exit_urg, novelty, thesis_fracture, prior, dist_sl)

        worlds = build_scenario_worlds(tile, thesis_fracture, exit_urg)
        sim = simulate_actions(tile, exit_urg, thesis_fracture)

        peak = peaks.get(sym)
        pnl = _to_f(tile.get("unrealized_pnl"))
        giveback_note = ""
        if peak is not None and pnl is not None and peak > 0 and pnl < peak * 0.5:
            giveback_note = "Giveback from session PnL peak detected."

        next_intel = {
            "exit_urgency": exit_urg,
            "thesis_fracture": thesis_fracture,
            "novelty_state": novelty,
            "pattern_label": feats.get("pattern_label"),
            "attention_score": attn,
        }
        is_material = _material(sym, prior, next_intel)
        why = _why_now(prior, next_intel)

        intel = {
            "symbol": sym,
            "position_state": {
                "side": tile.get("side"),
                "quantity": tile.get("quantity"),
                "unrealized_pnl": pnl,
                "entry_price": tile.get("entry_price"),
                "current_price": tile.get("current_price"),
            },
            "thesis_fracture": thesis_fracture,
            "legacy_thesis_status": legacy_thesis,
            "exit_urgency": exit_urg,
            "attention_score": round(attn, 2),
            "novelty_state": novelty,
            "why_now_delta": why,
            "scenario_worlds": worlds,
            "analog_summary": analog_summary,
            "action_simulation": sim,
            "committee_state": {},
            "position_story": f"{sym} {tile.get('side')}: urgency {exit_urg}, thesis {thesis_fracture}, novelty {novelty}. {giveback_note}".strip(),
            "materiality_state": "MATERIAL" if is_material else "STABLE",
            "derived_features": feats,
            "last_ai_refresh_at": prior.get("last_ai_refresh_at") if prior else None,
            "last_material_change_at": now if is_material else prior.get("last_material_change_at"),
        }
        intelligence[sym] = intel

        if is_material:
            feed_events.append(
                {
                    "ts": now,
                    "symbol": sym,
                    "transition": f"{prior.get('exit_urgency') if prior else 'INIT'}->{exit_urg}",
                    "why_now_human": why["human"],
                    "why_now_machine": why["machine"],
                    "action_implication": sim.get("preferred_ranking", ["hold"])[0],
                    "urgency": exit_urg,
                }
            )

    pw = merge_pairwise_from_bootstrap(portfolio_ctx_in if isinstance(portfolio_ctx_in, dict) else None)
    regime = detect_portfolio_regime(positions, pairwise_correlation=pw)

    return {
        "intelligence_by_symbol": intelligence,
        "feed_events": feed_events,
        "portfolio_regime": regime,
    }
