"""Deterministic intelligence step - no Snowflake, no AI."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.live_intelligence.analog import match_analogs
from app.services.live_intelligence.portfolio_regime import detect_portfolio_regime, merge_pairwise_from_bootstrap
from app.services.live_intelligence.resolver import (
    analog_tier_key,
    build_case_file_signature,
    build_delta_fields,
    build_feed_fingerprint,
    build_why_now_bullets,
    confidence_block,
    dominant_world_key,
    novelty_explanation_one_liner,
    regret_bucket_from_sim,
    regret_tilt_label,
    resolve_final_recommendation,
    should_emit_feed_event,
    urgency_to_final_band,
)
from app.services.live_intelligence import lic_display
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


def _attention_with_components(
    exit_urgency: str,
    novelty: str,
    thesis_fracture: str,
    prior: dict[str, Any] | None,
    dist_sl_pct: float | None,
    *,
    regime_active: bool,
    analog_quality: float,
    giveback: bool,
) -> tuple[float, dict[str, float]]:
    components: dict[str, float] = {}
    components["exit_band"] = float(EXIT_RANK.get(exit_urgency, 0) * 18)
    components["novelty"] = float(NOVELTY_RANK.get(novelty, 0) * 12)
    components["thesis_damage"] = float(THESIS_RANK.get(thesis_fracture, 0) * 10)
    if dist_sl_pct is not None and dist_sl_pct < 0.03:
        components["stop_proximity"] = 15.0
    else:
        components["stop_proximity"] = 0.0
    if regime_active:
        components["portfolio_stress"] = 18.0
    else:
        components["portfolio_stress"] = 0.0
    if prior and prior.get("exit_urgency") != exit_urgency:
        components["urgency_flip"] = 8.0
    else:
        components["urgency_flip"] = 0.0
    if analog_quality < 0.25 and analog_quality > 0:
        components["analog_deterioration"] = min(15.0, (0.25 - analog_quality) * 40)
    else:
        components["analog_deterioration"] = 0.0
    if giveback:
        components["giveback_from_peak"] = 12.0
    else:
        components["giveback_from_peak"] = 0.0

    base = 15.0
    total = base + sum(components.values())
    return float(_clamp(total, 0, 100)), components


def _material_state(
    prior: dict[str, Any] | None,
    next_intel: dict[str, Any],
) -> bool:
    if not prior:
        return True
    keys = ["exit_urgency", "thesis_fracture", "novelty_state", "pattern_label", "final_recommendation"]
    for k in keys:
        if prior.get(k) != next_intel.get(k):
            return True
    if abs(float(prior.get("attention_score") or 0) - float(next_intel.get("attention_score") or 0)) >= 12:
        return True
    return False


def _hot_news(tile: dict[str, Any]) -> bool:
    for e in tile.get("events") or []:
        if str(e.get("type") or "").upper() != "NEWS":
            continue
        badge = str((e.get("meta") or {}).get("badge") or "").upper()
        if badge in {"HOT", "RISK"}:
            return True
    return False


def run_deterministic_step(body: dict[str, Any]) -> dict[str, Any]:
    positions: list[dict[str, Any]] = list(body.get("positions") or [])
    prior_map: dict[str, dict[str, Any]] = dict(body.get("prior_intelligence") or {})
    peaks: dict[str, float] = {k.upper(): float(v) for k, v in (body.get("session_peak_pnl_by_symbol") or {}).items()}
    analog_by_sym: dict[str, list[dict[str, Any]]] = {
        k.upper(): list(v) for k, v in (body.get("analog_episodes_by_symbol") or {}).items()
    }
    portfolio_ctx_in = body.get("portfolio_context") or {}
    prior_pf = body.get("prior_portfolio_regime") or {}
    prior_regime_hyp = str(prior_pf.get("hypothesis") or "")

    pw = merge_pairwise_from_bootstrap(portfolio_ctx_in if isinstance(portfolio_ctx_in, dict) else None)
    regime = detect_portfolio_regime(positions, pairwise_correlation=pw)
    regime_active = bool(regime.get("active"))
    regime_hyp = str(regime.get("hypothesis") or "NONE")

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
        dist_tp = _to_f((tile.get("progress_metrics") or {}).get("distance_to_tp_pct"))
        thesis_fracture = _map_legacy_thesis_to_fracture(str(legacy_thesis), feats, dist_sl)

        analog_summary = match_analogs(tile, analog_by_sym.get(sym) or [])
        mq = float(analog_summary.get("match_quality") or 0)
        novelty = _compute_novelty(tile, feats, mq)

        exit_urg = _compute_exit_urgency(tile, feats, thesis_fracture)
        final_band, reason_summary, supporting, opposing = resolve_final_recommendation(
            exit_urg,
            thesis_fracture,
            feats,
            tile,
            regime_active=regime_active,
            analog_match_quality=mq,
        )

        peak = peaks.get(sym)
        pnl = _to_f(tile.get("unrealized_pnl"))
        giveback = peak is not None and pnl is not None and peak > 0 and pnl < peak * 0.5

        attn, attn_components = _attention_with_components(
            exit_urg,
            novelty,
            thesis_fracture,
            prior,
            dist_sl,
            regime_active=regime_active,
            analog_quality=mq,
            giveback=giveback,
        )

        worlds = build_scenario_worlds(tile, thesis_fracture, exit_urg)
        sim = simulate_actions(tile, exit_urg, thesis_fracture, final_band=final_band)
        analog_ui = lic_display.build_analog_ui(analog_summary)
        pf_sym = lic_display.portfolio_factor_for_symbol(regime, sym)

        sl_near = dist_sl is not None and dist_sl < 0.02
        tp_near = dist_tp is not None and abs(dist_tp) < 0.02
        hot = _hot_news(tile)
        tier_k = analog_tier_key(mq)
        dominant_w = dominant_world_key(worlds)
        regret_b = regret_bucket_from_sim(sim)

        why_bullets = build_why_now_bullets(
            feats=feats,
            thesis_fracture=thesis_fracture,
            novelty=novelty,
            final_band=final_band,
            dist_sl_pct=dist_sl,
            regime_active=regime_active,
            analog_summary=analog_summary,
            giveback=giveback,
            tile=tile,
        )
        delta_fields = build_delta_fields(
            prior,
            final_band=final_band,
            thesis_fracture=thesis_fracture,
            novelty=novelty,
            feats=feats,
            why_bullets=why_bullets,
            prior_regime_hypothesis=prior_regime_hyp or None,
            regime_hypothesis=regime_hyp,
            sl_near=sl_near,
            analog_tier_key=tier_k,
            dominant_world=dominant_w,
            regret_bucket=regret_b,
        )

        fingerprint = build_feed_fingerprint(
            final_band=final_band,
            thesis_fracture=thesis_fracture,
            novelty_state=novelty,
            analog_match_quality=mq,
            regime_hypothesis=regime_hyp,
            sl_near=sl_near,
            tp_near=tp_near,
            hot_news=hot,
            analog_tier=tier_k,
            dominant_world=dominant_w,
            regret_bucket=regret_b,
        )

        crossed = delta_fields.get("trigger_crossed") or []
        emit_feed = bool(
            should_emit_feed_event(prior, fingerprint) and len(crossed) > 0,
        )

        next_intel_compare = {
            "exit_urgency": exit_urg,
            "thesis_fracture": thesis_fracture,
            "novelty_state": novelty,
            "pattern_label": feats.get("pattern_label"),
            "attention_score": attn,
            "final_recommendation": final_band,
        }
        is_material = _material_state(prior, next_intel_compare)

        conf_block = confidence_block(feats, tile, mq, thesis_fracture)
        case_file_signature = build_case_file_signature(
            final_band=final_band,
            sim=sim,
            attention_score=attn,
            confidence_headline=float(conf_block.get("headline") or 0),
            thesis_fracture=thesis_fracture,
            analog_tier=tier_k,
            sl_near=sl_near,
            regret_bucket=regret_b,
        )
        evidence_sections = lic_display.build_evidence_sections(
            feats=feats,
            thesis_fracture=thesis_fracture,
            novelty=novelty,
            analog_ui=analog_ui,
            portfolio_plain=pf_sym["localized_plain"],
            supporting=supporting,
            opposing=opposing,
            regime_active=regime_active,
            dist_sl_pct=dist_sl,
        )
        decision_drivers = lic_display.decision_drivers_from_bullets(why_bullets)
        ba = sim.get("best_action") or {}
        analog_short = (
            f"{analog_ui.get('chip_verdict') or analog_ui['confidence_plain']} · {analog_ui['bias_plain']}"
            if analog_ui.get("analog_count", 0)
            else (analog_ui.get("low_similarity_note") or "No historical cluster for this snapshot.")
        )
        recommendation_display = {
            "headline": lic_display.recommendation_headline(final_band),
            "confidence": conf_block["headline"],
            "confidence_caption": lic_display.confidence_caption_from_block(conf_block),
            "primary_action": ba.get("label") or lic_display.primary_action_plain(ba.get("action")),
            "primary_action_key": ba.get("action"),
            "why_this_action": ba.get("why", ""),
            "supporting_reasons": supporting,
            "counterarguments": opposing,
        }
        intel = {
            "symbol": sym,
            "position_state": {
                "side": tile.get("side"),
                "quantity": tile.get("quantity"),
                "unrealized_pnl": pnl,
                "entry_price": tile.get("entry_price"),
                "current_price": tile.get("current_price"),
                "opened_at": tile.get("opened_at"),
            },
            "thesis_fracture": thesis_fracture,
            "thesis_plain": lic_display.plain_thesis_fracture(thesis_fracture),
            "legacy_thesis_status": legacy_thesis,
            "exit_urgency": exit_urg,
            "pattern_label": feats.get("pattern_label"),
            "final_recommendation": final_band,
            "final_recommendation_reason_summary": reason_summary,
            "recommendation_display": recommendation_display,
            "supporting_signals": supporting,
            "opposing_signals": opposing,
            "attention_score": round(attn, 2),
            "attention_band": "High" if attn >= 70 else ("Medium" if attn >= 40 else "Low"),
            "attention_components": {k: round(v, 2) for k, v in attn_components.items()},
            "novelty_state": novelty,
            "novelty_plain": lic_display.plain_novelty(novelty),
            "novelty_explanation_one_liner": novelty_explanation_one_liner(novelty, tile, feats, mq),
            "decision_drivers": decision_drivers,
            "why_now_bullets": why_bullets,
            "delta_label": delta_fields.get("delta_label"),
            "delta_reason": delta_fields.get("delta_reason"),
            "trigger_crossed": crossed,
            "new_vs_persistent": delta_fields.get("new_vs_persistent") or {},
            "why_now_delta": {
                "human": delta_fields.get("delta_reason", ""),
                "bullets": why_bullets,
                "machine": {"fingerprint": fingerprint},
            },
            "scenario_worlds": worlds,
            "analog_summary": analog_summary,
            "analog_ui": analog_ui,
            "analog_tile_line": analog_short,
            "action_simulation": sim,
            "regret_tilt_label": regret_tilt_label(sim),
            "confidence": conf_block,
            "confidence_decomposition": conf_block,
            "portfolio_factor_chip": pf_sym["chip_short"],
            "portfolio_factor_local": pf_sym,
            "evidence_sections": evidence_sections,
            "committee_state": {},
            "position_story": f"{sym} {tile.get('side')}: {final_band}. {reason_summary}",
            "materiality_state": "MATERIAL" if is_material else "STABLE",
            "derived_features": feats,
            "feed_fingerprint": fingerprint,
            "case_file_signature": case_file_signature,
            "sl_near": sl_near,
            "analog_tier_key": tier_k,
            "dominant_world_id": dominant_w,
            "regret_bucket": regret_b,
            "last_ai_refresh_at": prior.get("last_ai_refresh_at") if prior else None,
            "last_material_change_at": now if is_material else prior.get("last_material_change_at"),
        }
        intelligence[sym] = intel

        if emit_feed and prior:
            prior_case_sig = str(prior.get("case_file_signature") or "")
            if prior_case_sig != case_file_signature:
                prior_band = prior.get("final_recommendation") or urgency_to_final_band(prior.get("exit_urgency"))
                transition_plain = (
                    f"{lic_display.recommendation_headline(prior_band)} → {lic_display.recommendation_headline(final_band)}"
                )
                reason_plain = lic_display.build_feed_reason_plain(crossed)
                act_label = ba.get("label") or lic_display.primary_action_plain(ba.get("action"))
                sev = lic_display.feed_event_severity(final_band, thesis_fracture, sl_near)
                feed_events.append(
                    {
                        "timestamp": now,
                        "ts": now,
                        "symbol": sym,
                        "scope": "symbol",
                        "transition": transition_plain,
                        "state_transition": transition_plain,
                        "reason": reason_plain,
                        "what_changed": reason_plain,
                        "action_implication": act_label,
                        "final_recommendation": final_band,
                        "severity": sev,
                    }
                )

    return {
        "intelligence_by_symbol": intelligence,
        "feed_events": feed_events,
        "portfolio_regime": regime,
    }
