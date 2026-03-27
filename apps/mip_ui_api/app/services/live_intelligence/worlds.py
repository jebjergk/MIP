"""Deterministic parallel worlds (formulaic probabilities, plain-English UI fields)."""

from __future__ import annotations

from typing import Any


def build_scenario_worlds(tile: dict[str, Any], thesis_fracture: str, exit_urgency: str) -> list[dict[str, Any]]:
    side = str(tile.get("side") or "LONG").upper()
    vol_st = str((tile.get("volatility_context") or {}).get("status") or "UNKNOWN")
    prog = (tile.get("progress_metrics") or {}).get("expected_progress_pct")
    prog_f = float(prog) if prog is not None else 0.5
    dist_sl = (tile.get("progress_metrics") or {}).get("distance_to_sl_pct")
    d_sl = float(dist_sl) if dist_sl is not None else 0.2

    base_p = 0.35
    bull_p = 0.2 + (0.15 if side == "LONG" and prog_f < 0.8 else 0.05)
    risk_p = 0.2 + (0.2 if thesis_fracture in {"THESIS_DAMAGED", "THESIS_BROKEN"} else 0.05)
    shock_p = 0.1 + (0.15 if vol_st == "LIVE_VOL_ABOVE_TRAINED_REGIME" else 0.0)
    decay_p = 0.15 + (0.1 if d_sl < 0.02 else 0.0)

    s = bull_p + base_p + risk_p + shock_p + decay_p
    bull_p, base_p, risk_p, shock_p, decay_p = (bull_p / s, base_p / s, risk_p / s, shock_p / s, decay_p / s)

    def world(
        wid: str,
        title: str,
        p: float,
        explanation: str,
        triggers: list[str],
        action_if_dominant: str,
    ) -> dict[str, Any]:
        return {
            "id": wid,
            "title": title,
            "probability": round(p, 4),
            "probability_pct": round(p * 100, 1),
            "explanation": explanation,
            "trigger_conditions": triggers,
            "action_if_dominant": action_if_dominant,
        }

    long_up = "Price works higher with benign vol and no thesis break."

    return [
        world(
            "bull",
            "Bull",
            bull_p,
            long_up if side == "LONG" else "Price continues against a short in a squeeze-style path.",
            [
                "Tape holds constructive higher lows" if side == "LONG" else "Tape holds constructive lower highs",
                "Volatility does not spike into a shock regime",
                "Thesis remains at least stretched-not-broken",
            ],
            "Add or hold only per plan; avoid chasing if size is already full.",
        ),
        world(
            "base",
            "Base",
            base_p,
            "Choppy, mean-reverting session: neither clean trend nor clean failure.",
            [
                "Intraday range holds around recent value",
                "No synchronized book shock across holdings",
            ],
            "Default to your plan levels; prefer patience over reactive trading.",
        ),
        world(
            "risk",
            "Risk",
            risk_p,
            "Gradual risk-off: thesis softens, correlations pick up, or progress stalls.",
            [
                "Thesis moves toward damaged or broken",
                "Multiple positions deteriorate together",
                "Distance to stop compresses without reward",
            ],
            "Reduce size or tighten risk; prepare an explicit exit ladder.",
        ),
        world(
            "shock",
            "Shock",
            shock_p,
            "A gap or vol event moves price faster than normal adjustment.",
            [
                "Live vol materially above trained regime",
                "Liquidity thins or headline risk spikes",
            ],
            "Protect first: assume execution slippage; favor decisive trims or flat.",
        ),
        world(
            "time_decay",
            "Time decay",
            decay_p,
            "Theta and opportunity cost bite: edge fades even if price is flat.",
            [
                "Time passes without thesis progress",
                "Stop proximity rises as range compresses",
            ],
            "Re-underwrite the hold: either refresh thesis or free capital.",
        ),
    ]
