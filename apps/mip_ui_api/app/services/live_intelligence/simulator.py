"""Heuristic action simulator - five concrete actions, plain-English rationale."""

from __future__ import annotations

from typing import Any

_BAND_PREF_ACTION: dict[str, str] = {
    "EXIT_NOW": "exit_now",
    "PREPARE_EXIT": "trim_50",
    "WATCH_CLOSELY": "tighten_stop",
    "STAY_COURSE": "hold",
}


def _regret_word(regret_p: float) -> str:
    if regret_p >= 0.38:
        return "Higher regret risk if the move is wrong"
    if regret_p >= 0.28:
        return "Moderate regret risk"
    return "Lower regret risk versus alternatives"


def simulate_actions(
    tile: dict[str, Any],
    exit_urgency: str,
    thesis_fracture: str,
    *,
    final_band: str | None = None,
) -> dict[str, Any]:
    side = str(tile.get("side") or "LONG").upper()
    current = float(tile.get("current_price") or 0)
    entry = float(tile.get("entry_price") or 0)
    sl = tile.get("overlays", {}).get("stop_loss")
    tp = tile.get("overlays", {}).get("take_profit")
    pnl = float(tile.get("unrealized_pnl") or 0)
    sl_f = float(sl) if sl is not None else None
    tp_f = float(tp) if tp is not None else None

    stress = exit_urgency in {"PREPARE", "EXIT_NOW"}
    thesis_bad = thesis_fracture in {"THESIS_DAMAGED", "THESIS_BROKEN"}

    def row(
        key: str,
        label: str,
        up: float,
        down: float,
        give: float,
        regret_p: float,
        rationale: str,
    ) -> dict[str, Any]:
        net = up - down
        return {
            "action": key,
            "label": label,
            "expected_upside": round(up, 4),
            "expected_downside": round(down, 4),
            "giveback_risk": round(give, 4),
            "regret_tilt": _regret_word(regret_p),
            "regret_probability": round(regret_p, 4),
            "rationale": rationale,
            "net_score": round(net, 4),
        }

    base_down = 0.25 if stress else 0.42
    rows = [
        row(
            "hold",
            "Hold full size",
            0.06 if not thesis_bad else 0.02,
            base_down,
            0.36,
            0.22,
            "Keeps full upside if the thesis is still valid and liquidity is normal.",
        ),
        row(
            "tighten_stop",
            "Tighten stop",
            0.05,
            base_down * 0.72,
            0.32,
            0.28,
            "Cuts tail risk while staying in the trade; good when tape is noisy but thesis is not dead.",
        ),
        row(
            "trim_25",
            "Trim 25%",
            0.11,
            base_down * 0.82,
            0.27,
            0.26,
            "Locks partial gain or reduces loss surface while keeping core exposure.",
        ),
        row(
            "trim_50",
            "Trim 50%",
            0.17,
            base_down * 0.7,
            0.21,
            0.31,
            "Meaningful de-risk when exit preparation is warranted but you want optionality.",
        ),
        row(
            "exit_now",
            "Exit now",
            0.0 if pnl <= 0 else 0.14,
            0.14,
            0.11,
            0.44,
            "Closes ambiguity when stops are close, thesis is broken, or execution risk dominates.",
        ),
    ]

    if thesis_fracture == "THESIS_BROKEN":
        rows[4] = row(
            "exit_now",
            "Exit now",
            0.18,
            0.12,
            0.09,
            0.36,
            "With a broken thesis, finishing the trade often dominates small continuation bets.",
        )

    by_net = sorted(rows, key=lambda r: r["net_score"], reverse=True)
    band = final_band or "STAY_COURSE"
    anchor = _BAND_PREF_ACTION.get(band)
    best = None
    if anchor:
        best = next((r for r in rows if r["action"] == anchor), None)
    if best is None:
        best = by_net[0]

    why = (
        f"Aligned with the resolved stance ({band.replace('_', ' ').title()}) and highest net score "
        f"after risk adjustment among the five actions."
    )

    return {
        "alternatives": rows,
        "ranked_by_net_score": [r["action"] for r in by_net],
        "best_action": {
            "action": best["action"],
            "label": best["label"],
            "why": why,
            "net_score": best["net_score"],
        },
        "preferred_ranking": [best["action"]] + [r["action"] for r in by_net if r["action"] != best["action"]],
        "preferred_action_aligned_with_final_band": anchor,
        "context": {
            "side": side,
            "current_price": current,
            "entry_price": entry,
            "stop_loss": sl_f,
            "take_profit": tp_f,
            "final_band": band,
        },
    }
