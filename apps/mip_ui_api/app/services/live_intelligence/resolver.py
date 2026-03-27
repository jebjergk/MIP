"""Single resolved recommendation, feed dedupe, and UX-oriented copy helpers."""

from __future__ import annotations

from typing import Any

from app.services.live_intelligence import lic_display

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
        supporting.append("Price is very close to the stop — immediate protection priority.")
    elif exit_urgency == "PREPARE":
        supporting.append("Several signals line up for having an exit plan ready soon.")
    elif exit_urgency == "MONITOR":
        supporting.append("No forced exit yet, but this deserves closer monitoring.")

    if thesis_fracture == "THESIS_BROKEN":
        band = _escalate(band, 2)
        supporting.append("The original thesis no longer holds up well enough to justify full risk.")
    elif thesis_fracture == "THESIS_DAMAGED":
        band = _escalate(band, 1)
        supporting.append("The thesis is damaged versus the path you modeled.")
    elif thesis_fracture == "THESIS_STRETCHED":
        supporting.append("The thesis is still possible but price has drifted from the core story.")
        opposing.append("Could calm down if price snaps back inside the expected band.")

    if feats.get("pattern_label") == "RISK_OFF_BREAKDOWN":
        band = _escalate(band, 1)
        supporting.append("Short-term tape is leaning risk-off.")
    if feats.get("pattern_label") == "VOLATILITY_SPIKE":
        supporting.append("Recent swings are larger than usual — room for surprise moves.")
        if band == "STAY_COURSE":
            band = "WATCH_CLOSELY"

    if regime_active:
        band = _escalate(band, 1)
        supporting.append("Other positions are moving against you together — possible shared shock.")

    if analog_match_quality > 0 and analog_match_quality < 0.18:
        supporting.append("Historical parallels are weak — backward-looking confidence is limited.")
        if band == "STAY_COURSE":
            band = "WATCH_CLOSELY"

    if exit_urgency == "HOLD" and thesis_fracture == "THESIS_INTACT" and not regime_active:
        opposing.append("No strong exit trigger — patience inside your risk limits is reasonable.")

    # Monotonic: never softer than urgency-derived floor
    floor = BAND_ORDER[urgency_to_final_band(exit_urgency)]
    if BAND_ORDER.get(band, 0) < floor:
        band = urgency_to_final_band(exit_urgency)

    reason = (
        f"{lic_display.recommendation_headline(band)} — synthesized from tape, thesis, portfolio context, and history match."
    )
    return band, reason, supporting[:6], opposing[:4]


def analog_tier_key(mq: float) -> str:
    x = float(mq or 0)
    if x >= 0.45:
        return "strong"
    if x >= 0.22:
        return "moderate"
    return "weak"


def dominant_world_key(worlds: list[dict[str, Any]]) -> str:
    if not worlds:
        return "unknown"
    best = max(worlds, key=lambda w: float(w.get("probability") or 0))
    return str(best.get("id") or "unknown")


def regret_bucket_from_sim(sim: dict[str, Any]) -> str:
    alts = sim.get("alternatives") or []
    exit_alt = next((a for a in alts if a.get("action") == "exit_now"), None)
    hold_alt = next((a for a in alts if a.get("action") == "hold"), None)
    if not exit_alt or not hold_alt:
        return "balanced"
    ne = float(exit_alt.get("net_score") or 0)
    nh = float(hold_alt.get("net_score") or 0)
    if ne > nh + 0.02:
        return "exit_favored"
    if nh > ne + 0.02:
        return "hold_favored"
    return "balanced"


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
    analog_tier: str,
    dominant_world: str,
    regret_bucket: str,
) -> str:
    mq = round(float(analog_match_quality or 0), 2)
    parts = [
        final_band,
        thesis_fracture,
        novelty_state,
        str(mq),
        regime_hypothesis,
        "1" if sl_near else "0",
        "1" if tp_near else "0",
        "1" if hot_news else "0",
        str(analog_tier or ""),
        str(dominant_world or ""),
        str(regret_bucket or ""),
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
    mq_f = analog_summary.get("match_quality")
    mq = float(mq_f) if mq_f is not None else 0.0
    tier_word = "strong" if mq >= 0.45 else ("moderate" if mq >= 0.22 else "weak")
    avg = analog_summary.get("avg_forward_return")
    if n == 0:
        return "No close analog episodes in bootstrap set for this feature snapshot."
    ret_txt = (
        f"typical follow-on near {float(avg) * 100:.1f}%"
        if avg is not None
        else "follow-on outcomes were mixed across winners and losers"
    )
    return (
        f"Nearest {n} similar episodes: {w} winners / {l} losers; historical match is {tier_word}; {ret_txt}."
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
    b = regret_bucket_from_sim(sim)
    if b == "exit_favored":
        return "Exiting now hurts less if wrong"
    if b == "hold_favored":
        return "Holding still favored"
    return "Balanced regret"


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
    """Concrete drivers, ranked: stop proximity, tape, thesis, shared factor, analogs, giveback, news."""
    _ = final_band  # headline not restated as a driver bullet
    bullets: list[str] = []
    if dist_sl_pct is not None and dist_sl_pct < 0.03:
        bullets.append("Stop is close — small adverse moves can hit risk quickly.")
    if feats.get("pattern_label"):
        bullets.append(lic_display.plain_pattern(feats.get("pattern_label")))
    if thesis_fracture not in {"THESIS_INTACT", ""}:
        bullets.append(lic_display.plain_thesis_fracture(thesis_fracture))
    if regime_active:
        bullets.append("Other positions are stressed together — shared-factor risk matters.")
    mq = float(analog_summary.get("match_quality") or 0)
    if mq < 0.2:
        bullets.append("Historical parallels are thin — lean less on backward-looking stats.")
    if giveback:
        bullets.append("Session profit has given back meaningfully from its peak.")
    if novelty != "NORMAL":
        bullets.append(lic_display.plain_novelty(novelty))
    events = tile.get("events") or []
    if any(str(e.get("type") or "").upper() == "NEWS" for e in events):
        bullets.append("There is news flow on this name — read headlines before sizing changes.")
    return bullets[:8]


def build_delta_fields(
    prior: dict[str, Any] | None,
    *,
    final_band: str,
    thesis_fracture: str,
    novelty: str,
    feats: dict[str, Any],
    why_bullets: list[str],
    prior_regime_hypothesis: str | None = None,
    regime_hypothesis: str | None = None,
    sl_near: bool = False,
    analog_tier_key: str = "",
    dominant_world: str = "",
    regret_bucket: str = "",
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
    prh = str(prior_regime_hypothesis or "").strip()
    crh = str(regime_hypothesis or "").strip()
    if prh and crh and prh != crh:
        crossed.append(f"portfolio_regime:{prh}->{crh}")
    if "sl_near" in prior and bool(prior.get("sl_near")) != bool(sl_near):
        crossed.append(f"stop_danger:{int(bool(prior.get('sl_near')))}->{int(sl_near)}")
    pt = prior.get("analog_tier_key")
    if pt is not None and str(pt) != str(analog_tier_key):
        crossed.append(f"analog_tier:{pt}->{analog_tier_key}")
    pw = prior.get("dominant_world_id")
    if pw is not None and str(pw) != str(dominant_world):
        crossed.append(f"dominant_world:{pw}->{dominant_world}")
    prb = prior.get("regret_bucket")
    if prb is not None and str(prb) != str(regret_bucket):
        crossed.append(f"regret_bucket:{prb}->{regret_bucket}")
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
