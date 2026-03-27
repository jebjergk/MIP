"""Heuristic action simulator - no AI."""

from __future__ import annotations

from typing import Any

_BAND_PREF_ACTION: dict[str, str] = {
    "EXIT_NOW": "exit_now",
    "PREPARE_EXIT": "trim_50",
    "WATCH_CLOSELY": "tighten_stop",
    "STAY_COURSE": "hold",
}


def simulate_actions(
    tile: dict[str, Any],
    exit_urgency: str,
    thesis_fracture: str,
    *,
    final_band: str | None = None,
) -> dict[str, Any]:
    side = str(tile.get("side") or "LONG").upper()
    current = tile.get("current_price") or 0
    entry = tile.get("entry_price") or 0
    sl = (tile.get("overlays") or {}).get("stop_loss")
    tp = (tile.get("overlays") or {}).get("take_profit")
    pnl = float(tile.get("unrealized_pnl") or 0)

    def score(label: str, ev: float, risk: float, giveback: float, regret: float) -> dict[str, Any]:
        return {
            "action": label,
            "expected_reward": round(ev, 4),
            "expected_risk": round(risk, 4),
            "giveback_probability": round(giveback, 4),
            "regret_probability": round(regret, 4),
        }

    base_risk = 0.25 if exit_urgency in {"PREPARE", "EXIT_NOW"} else 0.45
    alts = [
        score("hold", 0.05, base_risk, 0.35, 0.2),
        score("trim_25", 0.12, base_risk * 0.85, 0.28, 0.25),
        score("trim_50", 0.18, base_risk * 0.75, 0.22, 0.32),
        score("tighten_stop", 0.08, base_risk * 0.7, 0.3, 0.28),
        score("exit_now", 0.0 if pnl <= 0 else 0.15, 0.15, 0.1, 0.45),
        score("time_stop_next_bar", 0.04, base_risk * 0.9, 0.33, 0.22),
    ]
    if thesis_fracture == "THESIS_BROKEN":
        alts[4]["expected_reward"] = 0.2
    ranked = sorted(alts, key=lambda a: a["expected_reward"] - a["expected_risk"], reverse=True)

    preferred = [a["action"] for a in ranked]
    band = final_band or "STAY_COURSE"
    anchor = _BAND_PREF_ACTION.get(band)
    if anchor and anchor in preferred:
        preferred = [anchor] + [a for a in preferred if a != anchor]

    top = preferred[0] if preferred else "hold"
    regret_framing = (
        f"If {top} is wrong and price mean-reverts, regret risk is captured in regret_probability; "
        "if tape breaks, early trim reduces giveback exposure."
    )
    return {
        "alternatives": alts,
        "preferred_ranking": preferred,
        "preferred_action_aligned_with_final_band": anchor,
        "regret_framing": regret_framing,
        "context": {
            "side": side,
            "current_price": current,
            "entry_price": entry,
            "stop_loss": sl,
            "take_profit": tp,
            "final_band": band,
        },
    }
