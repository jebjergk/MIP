"""Plain-English copy and structured display payloads for the Live Intelligence Cockpit."""

from __future__ import annotations

from typing import Any


def plain_thesis_fracture(code: str) -> str:
    m = {
        "THESIS_INTACT": "Thesis is intact versus the expected path.",
        "THESIS_STRETCHED": "Thesis is stretched: setup still alive, but risk is rising.",
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
    """Public tile/feed vocabulary (exact strings for UX contract)."""
    b = str(band or "").upper()
    m = {
        "EXIT_NOW": "EXIT OR CUT NOW",
        "PREPARE_EXIT": "PREPARE EXIT",
        "WATCH_CLOSELY": "WATCH CLOSELY",
        "STAY_COURSE": "HOLD",
    }
    return m.get(b, "HOLD")


def primary_action_plain(action_key: str) -> str:
    k = str(action_key or "hold").lower()
    m = {
        "hold": "Hold",
        "tighten_stop": "Tighten stop",
        "trim_25": "Trim 25%",
        "trim_50": "Trim 50%",
        "exit_now": "Exit now",
    }
    return m.get(k, "Hold")


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
        bias, bias_plain = "inconclusive", "Inconclusive — no close analogs in bootstrap"
    elif w >= l + 3:
        bias, bias_plain = "winner_leaning", "History leans toward favorable outcomes in similar setups"
    elif l >= w + 3:
        bias, bias_plain = "loser_leaning", "History leans toward unfavorable outcomes in similar setups"
    else:
        bias, bias_plain = "mixed", "Mixed historical outcomes - no clear winner/loser tilt"

    if n == 0:
        chip_verdict = "Historical analogs thin"
    elif tier == "strong":
        chip_verdict = "Strong historical match"
    elif tier == "moderate":
        chip_verdict = "Moderate historical match"
    elif bias == "mixed":
        chip_verdict = "Mixed historical match"
    else:
        chip_verdict = "Weak historical match"

    if avg is not None:
        fo = f"Across {n} similar past episodes, average follow-on return was about {float(avg) * 100:.1f}%."
    elif n > 0:
        fo = (
            f"Across {n} similar episodes, outcomes were mixed (winners {w} vs losers {l}); "
            f"treat the average as directional only, not a forecast."
        )
    else:
        fo = "No analog sample to summarize."

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
        "chip_verdict": chip_verdict,
        "bias": bias,
        "bias_plain": bias_plain,
        "analog_count": n,
        "winners": w,
        "losers": l,
        "forward_outcome_summary": fo,
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

    hyp_u = str(regime.get("hypothesis") or "").upper()
    hyp_chip = {
        "RISK_OFF_BOOK_SHOCK": "Risk-off factor",
        "USD_SHOCK": "USD factor",
        "RATES_SHOCK": "Rates factor",
        "SECTOR_SHOCK": "Sector factor",
    }.get(hyp_u, "Shared book stress")

    if not active:
        chip = "Mostly symbol-specific"
    elif localized == "portfolio_wide":
        chip = "Shared book stress" if hyp_u == "RISK_OFF_BOOK_SHOCK" else hyp_chip
    else:
        chip = "Possible spillover risk"

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
        elif c.startswith("regret_bucket:"):
            parts.append("Least-regret balance between hold and exit shifted.")
        elif c.startswith("analog_tier:"):
            parts.append("Historical match strength changed.")
        elif c.startswith("dominant_world:"):
            parts.append("Dominant scenario outlook changed.")
        elif c.startswith("stop_danger:"):
            parts.append("Stop proximity crossed a material threshold.")
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

    def strength(sup: list[str], opp: list[str]) -> str:
        ns, no = len(sup), len(opp)
        if ns >= 3 and no <= 1:
            return "Strong"
        if ns <= 1 and no >= 3:
            return "Weak"
        return "Medium"

    tape_c, tape_o = clean(tape_sup), clean(tape_opp)
    ths_c, tho_c = clean(thesis_sup), clean(thesis_opp)
    rs_c, ro_c = clean(risk_sup), clean(risk_opp)
    an_c, ao_c = clean(analog_sup), clean(analog_opp)
    pf_c, pfo_c = clean(pf_sup), clean(pf_opp)
    nv_c = clean(nov_sup)

    return {
        "tape": {
            "summary": pat_plain,
            "supports": tape_c,
            "opposes": tape_o,
            "status": strength(tape_c, tape_o),
        },
        "thesis": {
            "summary": plain_thesis_fracture(thesis_fracture),
            "supports": ths_c,
            "opposes": tho_c,
            "status": strength(ths_c, tho_c),
        },
        "risk": {
            "summary": "Stop distance, volatility, and giveback shape the risk picture.",
            "supports": rs_c,
            "opposes": ro_c,
            "status": strength(rs_c, ro_c),
        },
        "analog": {
            "summary": f"{analog_ui.get('confidence_plain', '')} — {analog_ui.get('bias_plain', '')}",
            "supports": an_c,
            "opposes": ao_c,
            "status": strength(an_c, ao_c),
        },
        "portfolio_factor": {
            "summary": portfolio_plain,
            "supports": pf_c,
            "opposes": pfo_c,
            "status": strength(pf_c, pfo_c),
        },
        "novelty": {
            "summary": plain_novelty(novelty),
            "supports": nv_c,
            "opposes": [],
            "status": "Strong" if novelty != "NORMAL" else "Medium",
        },
        "recommendation_bridge": {"supporting_reasons": supporting, "counterarguments": opposing},
    }


def feed_event_severity(final_band: str, thesis_fracture: str, sl_near: bool) -> str:
    """Case-file severity vocabulary (INFO / WATCH / ALERT / CRITICAL)."""
    b = str(final_band or "").upper()
    th = str(thesis_fracture or "").upper()
    if b == "EXIT_NOW" or (sl_near and th == "THESIS_BROKEN"):
        return "CRITICAL"
    if b == "PREPARE_EXIT" or sl_near:
        return "ALERT"
    if b == "WATCH_CLOSELY" or th in {"THESIS_DAMAGED", "THESIS_STRETCHED"}:
        return "WATCH"
    return "INFO"


def confidence_caption_from_block(conf: dict[str, Any]) -> str:
    """One sentence: decision confidence (not probability of being right)."""
    tc = float(conf.get("tape_confidence") or 0)
    ac = float(conf.get("analog_confidence") or 0)
    th = float(conf.get("thesis_confidence") or 0)
    parts = []
    if th >= 0.82:
        parts.append("thesis is clear")
    elif th <= 0.5:
        parts.append("thesis is shaky")
    else:
        parts.append("thesis is middling")
    if tc >= 0.75:
        parts.append("tape is steady")
    elif tc <= 0.52:
        parts.append("tape is noisy or stressed")
    else:
        parts.append("tape is mixed")
    if ac >= 0.65:
        parts.append("history matches reasonably well")
    elif ac <= 0.42:
        parts.append("history matches weakly")
    else:
        parts.append("history is only a moderate guide")
    inner = ", ".join(parts)
    return (
        f"This score blends how clear the thesis is, how calm the tape looks, and how much similar past setups help — "
        f"right now: {inner}. It is decision confidence, not odds of being correct."
    )


def decision_drivers_from_bullets(bullets: list[str], limit: int = 4) -> list[str]:
    out: list[str] = []
    seen_lower: set[str] = set()
    for b in bullets:
        if not b or not str(b).strip():
            continue
        s = str(b).strip()
        if "Resolved stance" in s or "Bottom line:" in s:
            continue
        key = s.lower()[:120]
        if key in seen_lower:
            continue
        seen_lower.add(key)
        out.append(s)
        if len(out) >= limit:
            break
    return out
