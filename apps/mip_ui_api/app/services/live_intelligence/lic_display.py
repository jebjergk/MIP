"""Plain-English copy and structured display payloads for the Live Intelligence Cockpit."""

from __future__ import annotations

from typing import Any


def plain_thesis_fracture(code: str) -> str:
    m = {
        "THESIS_INTACT": "Thesis is intact versus the expected path.",
        "THESIS_STRETCHED": "Thesis is still possible but price has drifted from the core scenario.",
        "THESIS_DAMAGED": "Thesis is damaged: multiple signals disagree with the original idea.",
        "THESIS_BROKEN": "Thesis looks broken or invalidated for practical risk management.",
    }
    return m.get(str(code).upper(), "Thesis state is unclear; treat with extra care.")


def plain_pattern(code: str | None) -> str:
    p = str(code or "WEAK_DRIFT").upper()
    m = {
        "WEAK_DRIFT": "Quiet tape: small moves, no strong directional conviction.",
        "VOLATILITY_SPIKE": "Volatility has picked up; swings are wider than recent norms.",
        "RISK_OFF_BREAKDOWN": "Short-term price action is breaking down in a risk-off way.",
        "TREND_CONTINUATION": "Price is pushing in the direction of the existing trend.",
    }
    return m.get(p, "Tape is mixed; read stops and size carefully.")


def plain_novelty(code: str) -> str:
    n = str(code or "NORMAL").upper()
    m = {
        "NORMAL": "Conditions feel familiar versus recent training and history.",
        "STRETCHED": "Setup is stretched: history matches weakly and vol is elevated.",
        "OUTSIDE_COMFORT_ZONE": "Behavior sits outside the usual comfort band for this model.",
    }
    return m.get(n, "Novelty is elevated; trust analogs less.")


def recommendation_headline(band: str) -> str:
    b = str(band or "").upper()
    m = {
        "EXIT_NOW": "Exit or cut now",
        "PREPARE_EXIT": "Prepare to exit",
        "WATCH_CLOSELY": "Watch closely",
        "STAY_COURSE": "Stay the course",
    }
    return m.get(b, "Review position")


def primary_action_plain(action_key: str) -> str:
    k = str(action_key or "hold").lower()
    m = {
        "hold": "Hold the full position",
        "tighten_stop": "Tighten the stop",
        "trim_25": "Trim about a quarter",
        "trim_50": "Trim about half",
        "exit_now": "Exit the position",
    }
    return m.get(k, "Review with your plan")


def build_analog_ui(analog_summary: dict[str, Any]) -> dict[str, Any]:
    w = int(analog_summary.get("winners") or 0)
    l = int(analog_summary.get("losers") or 0)
    n = w + l
    mq = float(analog_summary.get("match_quality") or 0)
    avg = analog_summary.get("avg_forward_return")
    hint = analog_summary.get("best_exit_hint_bars")

    if mq >= 0.45:
        tier, tier_plain = "strong", "Strong historical match"
    elif mq >= 0.22:
        tier, tier_plain = "moderate", "Moderate historical match"
    else:
        tier, tier_plain = "weak", "Weak historical match"

    if n == 0:
        bias, bias_plain = "inconclusive", "Inconclusive - no close analogs in bootstrap"
    elif w >= l + 3:
        bias, bias_plain = "winner_leaning", "History leans toward favorable outcomes in similar setups"
    elif l >= w + 3:
        bias, bias_plain = "loser_leaning", "History leans toward unfavorable outcomes in similar setups"
    else:
        bias, bias_plain = "mixed", "Mixed historical outcomes - no clear winner/loser tilt"

    if avg is not None:
        fo = f"Across {n} similar past episodes, average follow-on return was about {float(avg) * 100:.1f}%."
    else:
        fo = f"Across {n} similar past episodes, forward returns were mixed."

    if hint and n > 0:
        exit_txt = f"In similar episodes, a typical review horizon was about {hint} bars."
    else:
        exit_txt = "Exit timing hint is thin until analog match improves."

    low_sim = None
    if mq < 0.15 and n > 0:
        low_sim = "Similarity to stored history is low - treat analog statistics as exploratory, not definitive."
    elif n == 0:
        low_sim = "No analog cluster for this snapshot; lean more on tape, thesis, and risk limits."

    return {
        "confidence_tier": tier,
        "confidence_plain": tier_plain,
        "bias": bias,
        "bias_plain": bias_plain,
        "analog_count": n,
        "winners": w,
        "losers": l,
        "forward_outcome_summary": fo if n else "No analog sample to summarize.",
        "exit_timing_hint_plain": exit_txt,
        "low_similarity_note": low_sim,
    }


def portfolio_factor_for_symbol(regime: dict[str, Any], symbol: str) -> dict[str, Any]:
    sym = str(symbol or "").upper()
    active = bool(regime.get("active"))
    affected = [str(x).upper() for x in (regime.get("affected_symbols") or [])]

    if active and sym in affected:
        localized = "portfolio_wide"
        localized_plain = (
            "This symbol is in the portfolio-wide stress cluster (shared factor risk with other positions)."
        )
    elif active and sym not in affected:
        localized = "mixed"
        localized_plain = (
            "Portfolio stress is flagged, but this symbol is outside the core affected list - watch spillover."
        )
    else:
        localized = "symbol_specific"
        localized_plain = "Stress looks mostly idiosyncratic to individual names right now."

    chip = (
        "Shared book stress"
        if active and localized == "portfolio_wide"
        else ("Possible spillover risk" if active else "Mostly idiosyncratic")
    )

    return {
        "localized": localized,
        "localized_plain": localized_plain,
        "chip_short": chip,
        "hypothesis_plain": str(regime.get("hypothesis") or "NONE").replace("_", " ").lower(),
        "confidence": regime.get("confidence"),
    }


def build_feed_reason_plain(crossed: list[str]) -> str:
    if not crossed:
        return ""
    parts: list[str] = []
    for c in crossed:
        if c.startswith("stance:"):
            parts.append(f"Recommended stance changed ({c.replace('stance:', '').replace('->', ' to ')}).")
        elif c.startswith("thesis:"):
            parts.append("Thesis assessment shifted.")
        elif c.startswith("novelty:"):
            parts.append("Novelty regime changed.")
        elif c.startswith("pattern:"):
            parts.append("Short-term tape pattern changed.")
        elif c.startswith("portfolio_regime:"):
            parts.append("Portfolio-wide risk hypothesis changed.")
        else:
            parts.append(c)
    return " ".join(parts)


def build_evidence_sections(
    *,
    feats: dict[str, Any],
    thesis_fracture: str,
    novelty: str,
    analog_ui: dict[str, Any],
    portfolio_plain: str,
    supporting: list[str],
    opposing: list[str],
    regime_active: bool,
    dist_sl_pct: float | None,
) -> dict[str, Any]:
    tape_sup: list[str] = []
    tape_opp: list[str] = []
    pat_plain = plain_pattern(feats.get("pattern_label"))
    tape_sup.append(pat_plain)
    if feats.get("pattern_label") == "RISK_OFF_BREAKDOWN":
        tape_sup.append("Supports a more defensive stance.")
    if feats.get("inside_cone") is False:
        tape_opp.append("Price is outside the median expectation band - challenges a relaxed stance.")
    if feats.get("pattern_label") == "TREND_CONTINUATION":
        tape_opp.append("Trend strength can argue against rushing out if thesis still fits.")

    thesis_sup = [plain_thesis_fracture(thesis_fracture)]
    if thesis_fracture in {"THESIS_DAMAGED", "THESIS_BROKEN"}:
        thesis_sup.append("Supports de-risking or exit planning.")
    thesis_opp: list[str] = []
    if thesis_fracture == "THESIS_INTACT":
        thesis_opp.append("Intact thesis argues against over-trading on noise.")

    risk_sup: list[str] = []
    risk_opp: list[str] = []
    if dist_sl_pct is not None and dist_sl_pct < 0.03:
        risk_sup.append("Stop distance is tight - mistakes convert quickly into loss.")
    if dist_sl_pct is not None and dist_sl_pct > 0.08:
        risk_opp.append("More room to stop gives breathing space if thesis holds.")

    analog_sup = [analog_ui.get("forward_outcome_summary") or ""]
    if analog_ui.get("bias") == "loser_leaning":
        analog_sup.append("Historical bias in similar setups tilts unfavorable.")
    analog_opp: list[str] = []
    if analog_ui.get("bias") == "winner_leaning":
        analog_opp.append("Historical bias tilts favorable - could oppose rushing out.")
    if analog_ui.get("low_similarity_note"):
        analog_sup.append(str(analog_ui["low_similarity_note"]))

    pf_sup = [portfolio_plain]
    if regime_active:
        pf_sup.append("Shared-factor stress can justify extra caution even if this name looks fine alone.")
    pf_opp: list[str] = []
    if not regime_active:
        pf_opp.append("No book-wide flag reduces the case for panic across unrelated names.")

    nov_sup = [plain_novelty(novelty)]
    if novelty != "NORMAL":
        nov_sup.append("Unfamiliar conditions warrant smaller size or tighter risk.")

    def clean(xs: list[str]) -> list[str]:
        return [x for x in xs if x]

    return {
        "tape": {"summary": pat_plain, "supports": clean(tape_sup), "opposes": clean(tape_opp)},
        "thesis": {
            "summary": plain_thesis_fracture(thesis_fracture),
            "supports": clean(thesis_sup),
            "opposes": clean(thesis_opp),
        },
        "risk": {
            "summary": "Stop distance, volatility, and giveback shape the risk picture.",
            "supports": clean(risk_sup),
            "opposes": clean(risk_opp),
        },
        "analog": {
            "summary": f"{analog_ui.get('confidence_plain', '')} - {analog_ui.get('bias_plain', '')}",
            "supports": clean(analog_sup),
            "opposes": clean(analog_opp),
        },
        "portfolio_factor": {"summary": portfolio_plain, "supports": clean(pf_sup), "opposes": clean(pf_opp)},
        "novelty": {"summary": plain_novelty(novelty), "supports": clean(nov_sup), "opposes": []},
        "recommendation_bridge": {"supporting_reasons": supporting, "counterarguments": opposing},
    }


def decision_drivers_from_bullets(bullets: list[str], limit: int = 4) -> list[str]:
    out: list[str] = []
    for b in bullets:
        if "Resolved stance" in b:
            continue
        out.append(b)
        if len(out) >= limit:
            break
    return out
