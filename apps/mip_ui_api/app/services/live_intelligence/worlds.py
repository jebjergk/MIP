"""Deterministic parallel worlds (formulaic probabilities)."""

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

    def world(name: str, p: float, bias: str) -> dict[str, Any]:
        return {
            "world": name,
            "probability": round(p, 4),
            "short_horizon_bias": bias,
            "rest_of_day_bias": bias,
            "triggers": [],
            "preferred_action_if_dominant": "HOLD" if name == "BASE" else ("PREPARE" if name in {"RISK", "SHOCK"} else "MONITOR"),
        }

    return [
        world("BULL", bull_p, "up" if side == "LONG" else "down"),
        world("BASE", base_p, "chop"),
        world("RISK", risk_p, "down" if side == "LONG" else "up"),
        world("SHOCK", shock_p, "gap_against"),
        world("TIME_DECAY", decay_p, "theta_bleed"),
    ]
