"""Single resolved recommendation, feed dedupe, and UX-oriented copy helpers."""

from __future__ import annotations

from typing import Any

# Aligned with exit urgency ladder (higher = more defensive)
BAND_ORDER = {"STAY_COURSE": 0, "WATCH_CLOSELY": 1, "PREPARE_EXIT": 2, "EXIT_NOW": 3}


def urgency_to_final_band(exit_urgency: str | None) -> str:
    u = str(exit_urgency or "HOLD").upper()
    if u == "EXIT_NOW":
        return "EXIT_NOW"
    if u == "PREPARE":
        return "PREPARE_EXIT"
    if u == "MONITOR":
        return "WATCH_CLOSELY"
    return "STAY_COURSE"


def _escalate(band: str, steps: int) -> str:
    order = list(BAND_ORDER.keys())
    idx = BAND_ORDER.get(band, 0)
    idx = min(len(order) - 1, idx + steps)
    return order[idx]


def resolve_final_recommendation(
    exit_urgency: str,
    thesis_fracture: str,
    feats: dict[str, Any],
    tile: dict[str, Any],
    *,
    regime_active: bool,
    analog_match_quality: float,
) -> tuple[str, str, list[str], list[str]]:
    """Return (final_band, reason_summary, supporting_signals, opposing_signals)."""
    base = urgency_to_final_band(exit_urgency)
    band = base
    supporting: list[str] = []
    opposing: list[str] = []

    if exit_urgency == "EXIT_NOW":
        supporting.append("Price/stop geometry implies immediate risk of stop touch.")
    elif exit_urgency == "PREPARE":
        supporting.append("Multiple paths converge on needing an exit plan soon.")
    elif exit_urgency == "MONITOR":
        supporting.append("No immediate forced exit, but conditions warrant closer monitoring.")

    if thesis_fracture == "THESIS_BROKEN":
        band = _escalate(band, 2)
        supporting.append("Thesis fracture reads as broken or invalidated.")
    elif thesis_fracture == "THESIS_DAMAGED":
        band = _escalate(band, 1)
        supporting.append("Thesis is damaged relative to expectation path.")
    elif thesis_fracture == "THESIS_STRETCHED":
        supporting.append("Thesis still plausible but stretched vs median path.")
        opposing.append("Could normalize if price re-enters the expectation cone.")

    if feats.get("pattern_label") == "RISK_OFF_BREAKDOWN":
        band = _escalate(band, 1)
        supporting.append("Short-horizon tape pattern skews risk-off.")
    if feats.get("pattern_label") == "VOLATILITY_SPIKE":
        supporting.append("Realized vol elevated vs recent baseline.")
        if band == "STAY_COURSE":
            band = "WATCH_CLOSELY"

    if regime_active:
        band = _escalate(band, 1)
        supporting.append("Portfolio-level stress hypothesis is active (co-movement / book shock).")

    if analog_match_quality > 0 and analog_match_quality < 0.18:
        supporting.append("Historical analog match is weak - path less validated.")
        if band == "STAY_COURSE":
            band = "WATCH_CLOSELY"

    if exit_urgency == "HOLD" and thesis_fracture == "THESIS_INTACT" and not regime_active:
        opposing.append("No strong exit trigger; default bias is patience within risk limits.")

    # Monotonic: never softer than urgency-derived floor
    floor = BAND_ORDER[urgency_to_final_band(exit_urgency)]
    if BAND_ORDER.get(band, 0) < floor:
        band = urgency_to_final_band(exit_urgency)

    reason = f"Resolved to {band.replace('_', ' ').lower()} given urgency {exit_urgency}, thesis {thesis_fracture}."
    return band, reason, supporting[:6], opposing[:4]


def build_feed_fingerprint(
    *,
    final_band: str,
    thesis_fracture: str,
    novelty_state: str,
    analog_match_quality: float,
    regime_hypothesis: str,
    sl_near: bool,
    tp_near: bool,
    hot_news: bool,
) -> str:
    mq = round(float(analog_match_quality or 0), 3)
    parts = [
        final_band,
        thesis_fracture,
        novelty_state,
        str(mq),
        regime_hypothesis,
        "1" if sl_near else "0",
        "1" if tp_near else "0",
        "1" if hot_news else "0",
    ]
    return "|".join(parts)


def should_emit_feed_event(prior: dict[str, Any] | None, fingerprint: str) -> bool:
    if not prior:
        return False
    return str(prior.get("feed_fingerprint") or "") != fingerprint


def analog_tile_line(analog_summary: dict[str, Any]) -> str:
    w = int(analog_summary.get("winners") or 0)
    l = int(analog_summary.get("losers") or 0)
    n = w + l
    mq = analog_summary.get("match_quality")
    avg = analog_summary.get("avg_forward_return")
    if n == 0:
        return "No close analog episodes in bootstrap set for this feature snapshot."
    ret_txt = f"avg forward return ~{float(avg):.3f}" if avg is not None else "forward return mixed"
    return (
        f"Nearest {n} analogs: {w} winners / {l} losers; match quality {mq}; {ret_txt}."
    )


def novelty_explanation_one_liner(
    novelty: str,
    tile: dict[str, Any],
    feats: dict[str, Any],
    analog_quality: float,
) -> str:
    vol_ctx = tile.get("volatility_context") or {}
    st = str(vol_ctx.get("status") or "")
    if novelty == "OUTSIDE_COMFORT_ZONE":
        return "Live volatility and tape behavior sit outside the trained comfort band."
    if novelty == "STRETCHED":
        return "Conditions are stretched: weaker analog fit plus elevated short-horizon vol."
    if st == "LIVE_VOL_BELOW_TRAINED_REGIME":
        return "Volatility is subdued vs training - moves may be slower than modeled."
    return "Novelty is normal vs recent training and analog context."


def portfolio_factor_chip(regime: dict[str, Any]) -> str:
    if regime.get("active"):
        hyp = str(regime.get("hypothesis") or "STRESS")
        conf = regime.get("confidence")
        return f"Portfolio factor: {hyp.replace('_', ' ')} (confidence {conf})."
    return "Portfolio factor: idiosyncratic mix - no book-wide shock flag."


def regret_tilt_label(sim: dict[str, Any]) -> str:
    alts = sim.get("alternatives") or []
    exit_alt = next((a for a in alts if a.get("action") == "exit_now"), None)
    hold_alt = next((a for a in alts if a.get("action") == "hold"), None)
    if not exit_alt or not hold_alt:
        return "Regret tilt: balanced - compare trim vs hold in simulator table."
    er_exit = float(exit_alt.get("expected_reward") or 0) - float(exit_alt.get("expected_risk") or 0)
    er_hold = float(hold_alt.get("expected_reward") or 0) - float(hold_alt.get("expected_risk") or 0)
    if er_exit > er_hold + 0.02:
        return "Regret tilt: leaning exit - scenario mass favors cutting tail risk."
    if er_hold > er_exit + 0.02:
        return "Regret tilt: leaning hold - mean-reversion path still competes."
    return "Regret tilt: neutral - exit and hold scenarios are close on net score."


def confidence_block(
    feats: dict[str, Any],
    tile: dict[str, Any],
    analog_match_quality: float,
    thesis_fracture: str,
) -> dict[str, Any]:
    pattern = str(feats.get("pattern_label") or "")
    inside = feats.get("inside_cone")
    vol = float(feats.get("vol_15m") or 0)
    thesis_conf = 0.72
    if thesis_fracture == "THESIS_INTACT":
        thesis_conf = 0.88
    elif thesis_fracture in {"THESIS_DAMAGED", "THESIS_BROKEN"}:
        thesis_conf = 0.45
    analog_conf = min(0.95, 0.35 + float(analog_match_quality or 0) * 2)
    tape_conf = 0.7
    if pattern == "WEAK_DRIFT":
        tape_conf = 0.55
    if vol > 0.025:
        tape_conf -= 0.08
    if inside is False:
        tape_conf -= 0.1
    tape_conf = max(0.35, min(0.92, tape_conf))
    return {
        "headline": round((thesis_conf + analog_conf + tape_conf) / 3, 3),
        "thesis_confidence": round(thesis_conf, 3),
        "analog_confidence": round(analog_conf, 3),
        "tape_confidence": round(tape_conf, 3),
    }


def build_why_now_bullets(
    *,
    feats: dict[str, Any],
    thesis_fracture: str,
    novelty: str,
    final_band: str,
    dist_sl_pct: float | None,
    regime_active: bool,
    analog_summary: dict[str, Any],
    giveback: bool,
    tile: dict[str, Any],
) -> list[str]:
    bullets: list[str] = []
    pat = feats.get("pattern_label")
    if pat:
        bullets.append(f"Tape pattern: {pat}.")
    if dist_sl_pct is not None and dist_sl_pct < 0.03:
        bullets.append("Stop distance is tight - path errors convert quickly into loss.")
    if thesis_fracture not in {"THESIS_INTACT", ""}:
        bullets.append(f"Thesis state: {thesis_fracture.replace('_', ' ').lower()}.")
    if novelty != "NORMAL":
        bullets.append(f"Novelty: {novelty.replace('_', ' ').lower()}.")
    if regime_active:
        bullets.append("Portfolio regime flag is on - shared factor risk.")
    mq = float(analog_summary.get("match_quality") or 0)
    if mq < 0.2:
        bullets.append("Analog match is weak - historical playbook less informative.")
    if giveback:
        bullets.append("Session PnL has given back materially from peak.")
    events = tile.get("events") or []
    if any(str(e.get("type") or "").upper() == "NEWS" for e in events):
        bullets.append("News or event items are attached to this symbol.")
    bullets.append(f"Resolved stance: {final_band.replace('_', ' ').lower()}.")
    return bullets[:8]


def build_delta_fields(
    prior: dict[str, Any] | None,
    *,
    final_band: str,
    thesis_fracture: str,
    novelty: str,
    feats: dict[str, Any],
    why_bullets: list[str],
) -> dict[str, Any]:
    crossed: list[str] = []
    if not prior:
        return {
            "delta_label": "Baseline",
            "delta_reason": "First intelligence cycle for this session - establishing baseline.",
            "trigger_crossed": crossed,
            "new_vs_persistent": {"stance": "new", "drivers": "initial"},
        }
    pb = prior.get("final_recommendation") or urgency_to_final_band(prior.get("exit_urgency"))
    if pb != final_band:
        crossed.append(f"stance:{pb}->{final_band}")
    if prior.get("thesis_fracture") != thesis_fracture:
        crossed.append(f"thesis:{prior.get('thesis_fracture')}->{thesis_fracture}")
    if prior.get("novelty_state") != novelty:
        crossed.append(f"novelty:{prior.get('novelty_state')}->{novelty}")
    if prior.get("pattern_label") != feats.get("pattern_label"):
        crossed.append(
            f"pattern:{prior.get('pattern_label')}->{feats.get('pattern_label')}",
        )
    persistent = len(crossed) == 0
    label = "Stable refresh" if persistent else "Material shift"
    human = (
        "; ".join(crossed)
        if crossed
        else "No threshold crossings; prices and features refreshed only."
    )
    return {
        "delta_label": label,
        "delta_reason": human,
        "trigger_crossed": crossed,
        "new_vs_persistent": {
            "stance": "persistent" if persistent else "new",
            "drivers": "refreshed_only" if persistent else "crossed",
        },
        "why_now_bullets_snapshot": why_bullets[:4],
    }
