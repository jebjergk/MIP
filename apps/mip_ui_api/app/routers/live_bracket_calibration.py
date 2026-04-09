"""
Deterministic post-committee live bracket calibration: raise TP/SL within guardrails
so committee-sized entries pass IB risk + bracket-relativity checks, or fail with
clear reason codes before submit.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any


@dataclass
class LiveBracketCalibrationResult:
    ok: bool
    target_return: float | None
    stop_loss_pct: float | None
    reason_codes: list[str] = field(default_factory=list)
    calibrated: bool = False
    meta: dict[str, Any] = field(default_factory=dict)


def _entry_tp_sl_prices(
    side: str,
    entry_price: float,
    target_return: float | None,
    stop_loss_pct: float | None,
) -> tuple[float | None, float | None]:
    side_u = str(side or "").upper()
    ep = float(entry_price)
    tp_price = None
    sl_price = None
    if target_return is not None:
        tr = float(target_return)
        if side_u == "BUY":
            tp_price = math.fsum([ep, ep * tr])
        elif side_u == "SELL":
            tp_price = max(math.fsum([ep, -ep * tr]), 0.0001)
    if stop_loss_pct is not None:
        sl = float(stop_loss_pct)
        if side_u == "BUY":
            sl_price = max(math.fsum([ep, -ep * sl]), 0.0001)
        elif side_u == "SELL":
            sl_price = math.fsum([ep, ep * sl])
    return tp_price, sl_price


def _fee_return_floor(fee_params: dict) -> float:
    slippage_bps = float(fee_params.get("slippage_bps") or 2.0)
    fee_bps = float(fee_params.get("fee_bps") or 1.0)
    spread_bps = float(fee_params.get("spread_bps") or 0.0)
    return (slippage_bps + fee_bps + (spread_bps / 2.0)) / 10000.0


def _bracket_realism_lower_bounds_tr_sl(
    *,
    entry_price: float,
    qty: float,
    nav_scale: float,
    fee_params: dict,
    rcfg: dict,
) -> tuple[float, float]:
    """
    Lower bounds on (target_return, stop_loss_pct) implied by LIVE_BRACKET_* thresholds,
    evaluated as if mode were BLOCK (numeric floors only; BUY/SELL symmetric in % space).
    """
    ep = float(entry_price)
    qv = float(qty)
    notional = abs(ep * qv)
    if ep <= 0 or qv <= 0 or notional <= 0:
        return 0.0, 0.0

    nav = float(nav_scale)
    small_line = bool(nav > 0 and (notional / nav) < float(rcfg["small_pos_max_pct_nav"]))
    smult = float(rcfg["small_pos_strict_mult"]) if small_line else 1.0
    strict_tp_pct = float(rcfg["min_gross_tp_pct_notional"]) * smult
    strict_net_pct = float(rcfg["min_net_tp_pct_notional"]) * smult
    strict_sl_pct = float(rcfg["min_gross_sl_pct_notional"]) * smult

    abs_tp = float(rcfg["abs_min_gross_tp_usd"])
    abs_sl = float(rcfg["abs_min_gross_sl_usd"])
    bps_nav = float(rcfg["min_gross_tp_bps_of_nav"])
    cap_mult = float(rcfg["nav_rule_cap_mult"])
    base_tp_pct = float(rcfg["min_gross_tp_pct_notional"])
    nav_tp_floor = (
        min(nav * (bps_nav / 10000.0), cap_mult * base_tp_pct * notional) if nav > 0 else 0.0
    )
    req_net = strict_net_pct * notional
    fee_floor = _fee_return_floor(fee_params)
    min_width = float(rcfg["min_bracket_width_bps"]) / 10000.0
    rel_eps = 1.0 + 1e-8

    gross_tp_floor_usd = max(abs_tp, strict_tp_pct * notional, nav_tp_floor)
    min_tr_gross = (gross_tp_floor_usd / notional) * rel_eps
    min_tr_net = (fee_floor + req_net / notional) * rel_eps
    min_tr = max(min_tr_gross, min_tr_net, min_width)

    gross_sl_floor_usd = max(abs_sl, strict_sl_pct * notional)
    min_sl = max((gross_sl_floor_usd / notional) * rel_eps, min_width)

    return min_tr, min_sl


def _bracket_realism_constraints_active(rcfg: dict) -> bool:
    if not rcfg.get("enabled"):
        return False
    mode = str(rcfg.get("mode") or "BLOCK").upper()
    return mode == "BLOCK"


# Baseline bracket pass/fail at this notional (EUR) distinguishes tiny-line NAV floor vs weak committee %s.
BLOCKED_BRACKET_PROBE_MIN_NOTIONAL_EUR = 250.0


def classify_blocked_bracket_for_diagnostics(
    *,
    side: str,
    entry_price: float,
    qty: float,
    nav_scale: float,
    baseline_target_return: float,
    baseline_stop_loss_pct: float,
    fee_params: dict,
    rcfg: dict,
    last_bracket: list[str] | None,
    calibration_reason_codes: list[str] | None,
) -> tuple[str, dict[str, Any]]:
    """
    Classify blocked executable-bracket outcomes for diagnostics (PARAM_SNAPSHOT / SQL).

    - TINY_LINE_NAV_FLOOR: small line vs NAV and NAV-based gross TP stress at TP-cap failure
      (calibration ended with EXCEEDS_MAX_TP and BPS_NAV signal).
    - WEAK_BASELINE_BRACKET: baseline TR/SL still fail realism/risk at probe notional (dominant).
    - MIXED: both tiny-line NAV TP-cap pattern and weak baseline at probe.
    """
    from app.routers.live import _live_bracket_realism_codes_pure, _live_ib_entry_risk_reason_codes

    side_u = str(side or "").upper()
    ep = float(entry_price)
    qv = float(qty)
    nav = float(nav_scale)
    notional = abs(ep * qv)
    small_pos_max = float(rcfg.get("small_pos_max_pct_nav") or 0.10)
    pos_pct_nav = (notional / nav) if nav > 0 else None
    small_line = pos_pct_nav is not None and pos_pct_nav < small_pos_max

    lb_upper = [str(x).strip().upper() for x in (last_bracket or [])]
    has_bps_nav = "LIVE_BRACKET_REL_GROSS_TP_BELOW_BPS_NAV" in lb_upper
    calib_u = [str(x).strip().upper() for x in (calibration_reason_codes or [])]

    tiny_line_nav_floor = bool(
        small_line
        and (
            ("LIVE_BRACKET_CALIBRATION_EXCEEDS_MAX_TP" in calib_u and has_bps_nav)
            or ("LIVE_BRACKET_CALIBRATION_EXCEEDS_MAX_TP" in calib_u and not lb_upper)
        )
    )

    probe_notional = max(notional, BLOCKED_BRACKET_PROBE_MIN_NOTIONAL_EUR)
    q_probe = probe_notional / max(ep, 1e-9)
    b_tr = float(baseline_target_return)
    b_sl = float(baseline_stop_loss_pct)

    tp_p, sl_p = _entry_tp_sl_prices(side_u, ep, b_tr, b_sl)
    probe_risk: list[str] = []
    probe_bracket: list[str] = []
    if tp_p is None or sl_p is None:
        weak_baseline = True
    else:
        probe_risk = _live_ib_entry_risk_reason_codes(
            side=side_u,
            is_exit=False,
            entry_price=ep,
            target_return=b_tr,
            stop_loss_pct=b_sl,
            fee_params=fee_params,
        )
        if _bracket_realism_constraints_active(rcfg):
            probe_bracket = _live_bracket_realism_codes_pure(
                nav_scale=nav,
                side=side_u,
                entry_price=ep,
                qty=q_probe,
                tp_price=float(tp_p),
                sl_price=float(sl_p),
                target_return=b_tr,
                fee_params=fee_params,
                rcfg=rcfg,
            )
        weak_baseline = bool(probe_risk or probe_bracket)

    detail: dict[str, Any] = {
        "small_line_vs_config": small_line,
        "position_pct_nav": pos_pct_nav,
        "has_bps_nav_in_last_bracket": has_bps_nav,
        "probe_min_notional_eur": BLOCKED_BRACKET_PROBE_MIN_NOTIONAL_EUR,
        "probe_notional_eur": probe_notional,
        "probe_qty": q_probe,
        "baseline_passes_bracket_risk_at_probe_notional": not weak_baseline,
        "probe_bracket_codes": probe_bracket,
        "probe_risk_codes": probe_risk,
    }

    if tiny_line_nav_floor and weak_baseline:
        label = "MIXED"
    elif tiny_line_nav_floor:
        label = "TINY_LINE_NAV_FLOOR"
    elif weak_baseline:
        label = "WEAK_BASELINE_BRACKET"
    else:
        label = "MIXED"

    return label, detail


def _calibrate_live_entry_bracket_to_min_viable(
    *,
    side: str,
    entry_price: float,
    qty: float,
    nav_scale: float,
    baseline_target_return: float,
    baseline_stop_loss_pct: float,
    bust_pct: float | None,
    fee_params: dict,
    rcfg: dict,
    calib_cfg: dict,
) -> LiveBracketCalibrationResult:
    """
    Bounded calibration vs committee baseline: only widen TP/SL (raise target_return,
    raise stop_loss_pct up to bust and max multiples). Reuses live bracket/risk pure checks.
    """
    from app.routers.live import _live_bracket_realism_codes_pure, _live_ib_entry_risk_reason_codes

    side_u = str(side or "").upper()
    meta: dict[str, Any] = {
        "baseline_target_return": baseline_target_return,
        "baseline_stop_loss_pct": baseline_stop_loss_pct,
    }

    if side_u not in ("BUY", "SELL"):
        return LiveBracketCalibrationResult(
            ok=True,
            target_return=baseline_target_return,
            stop_loss_pct=baseline_stop_loss_pct,
            calibrated=False,
            meta=meta,
        )

    enabled = bool(calib_cfg.get("enabled", True))
    max_tp_mult = max(1.0, float(calib_cfg.get("max_tp_mult", 2.5)))
    max_sl_mult = max(1.0, float(calib_cfg.get("max_sl_mult", 2.0)))
    max_tp_abs_add = max(0.0, float(calib_cfg.get("max_tp_abs_add", 0.03)))

    ep = float(entry_price)
    qv = float(qty)
    if ep <= 0 or qv <= 0:
        return LiveBracketCalibrationResult(
            ok=False,
            target_return=None,
            stop_loss_pct=None,
            reason_codes=["LIVE_BRACKET_NOT_VIABLE_WITHIN_GUARDRAILS"],
            meta={**meta, "detail": "invalid_entry_price_or_qty"},
        )

    bust_limit = float(bust_pct) if bust_pct is not None else 1.0
    bust_limit = min(max(bust_limit, 1e-9), 1.0)

    b_tr = float(baseline_target_return)
    b_sl = float(baseline_stop_loss_pct)
    if b_tr <= 0 or b_sl <= 0:
        return LiveBracketCalibrationResult(
            ok=False,
            target_return=None,
            stop_loss_pct=None,
            reason_codes=["LIVE_BRACKET_NOT_VIABLE_WITHIN_GUARDRAILS"],
            meta={**meta, "detail": "missing_positive_baseline_tr_or_sl"},
        )

    tr_cap = min(b_tr * max_tp_mult, b_tr + max_tp_abs_add)
    sl_cap = min(bust_limit, b_sl * max_sl_mult)

    def evaluate(tr: float, sl: float) -> tuple[list[str], list[str]]:
        tp_p, sl_p = _entry_tp_sl_prices(side_u, ep, tr, sl)
        if tp_p is None or sl_p is None:
            return ["LIVE_BRACKET_REQUIRED"], []
        risk = _live_ib_entry_risk_reason_codes(
            side=side_u,
            is_exit=False,
            entry_price=ep,
            target_return=tr,
            stop_loss_pct=sl,
            fee_params=fee_params,
        )
        bracket: list[str] = []
        if _bracket_realism_constraints_active(rcfg):
            bracket = _live_bracket_realism_codes_pure(
                nav_scale=float(nav_scale),
                side=side_u,
                entry_price=ep,
                qty=qv,
                tp_price=float(tp_p),
                sl_price=float(sl_p),
                target_return=tr,
                fee_params=fee_params,
                rcfg=rcfg,
            )
        return risk, bracket

    if not enabled:
        sl0 = min(b_sl, bust_limit)
        risk0, br0 = evaluate(b_tr, sl0)
        codes = sorted(set(risk0 + br0))
        return LiveBracketCalibrationResult(
            ok=(len(codes) == 0),
            target_return=b_tr,
            stop_loss_pct=sl0,
            reason_codes=codes,
            calibrated=False,
            meta=meta,
        )

    min_rr = float(fee_params.get("min_rr") or 1.10)
    min_tp_required = _fee_return_floor(fee_params) + float(fee_params.get("min_net_tp_bps") or 5.0) / 10000.0

    min_tr_bracket, min_sl_bracket = (0.0, 0.0)
    if _bracket_realism_constraints_active(rcfg):
        min_tr_bracket, min_sl_bracket = _bracket_realism_lower_bounds_tr_sl(
            entry_price=ep,
            qty=qv,
            nav_scale=float(nav_scale),
            fee_params=fee_params,
            rcfg=rcfg,
        )

    tr = b_tr
    sl = min(b_sl, bust_limit)

    for _ in range(24):
        risk_c, br_c = evaluate(tr, sl)
        if not risk_c and not br_c:
            calibrated = (tr > b_tr + 1e-12) or (sl > b_sl + 1e-12)
            return LiveBracketCalibrationResult(
                ok=True,
                target_return=tr,
                stop_loss_pct=sl,
                reason_codes=[],
                calibrated=calibrated,
                meta={
                    **meta,
                    "iterations": _,
                    "final_target_return": tr,
                    "final_stop_loss_pct": sl,
                },
            )

        sl_need = max(b_sl, min_sl_bracket)
        sl_target = min(sl_need, sl_cap, bust_limit)
        if sl_target < sl_need - 1e-15:
            return LiveBracketCalibrationResult(
                ok=False,
                target_return=None,
                stop_loss_pct=None,
                reason_codes=["LIVE_BRACKET_CALIBRATION_EXCEEDS_MAX_SL", "LIVE_BRACKET_NOT_VIABLE_WITHIN_GUARDRAILS"],
                meta={**meta, "needed_sl_pct": sl_need, "sl_cap": sl_cap, "last_risk": risk_c, "last_bracket": br_c},
            )
        sl = max(sl, sl_target)

        tr_need = max(
            b_tr,
            min_tp_required,
            min_tr_bracket,
            min_rr * sl,
        )
        tr_target = min(tr_need, tr_cap)
        if tr_target < tr_need - 1e-15:
            return LiveBracketCalibrationResult(
                ok=False,
                target_return=None,
                stop_loss_pct=None,
                reason_codes=["LIVE_BRACKET_CALIBRATION_EXCEEDS_MAX_TP", "LIVE_BRACKET_NOT_VIABLE_WITHIN_GUARDRAILS"],
                meta={**meta, "needed_tr": tr_need, "tr_cap": tr_cap, "last_risk": risk_c, "last_bracket": br_c},
            )
        tr = max(tr, tr_target)

    risk_f, br_f = evaluate(tr, sl)
    return LiveBracketCalibrationResult(
        ok=False,
        target_return=None,
        stop_loss_pct=None,
        reason_codes=["LIVE_BRACKET_NOT_VIABLE_WITHIN_GUARDRAILS"],
        meta={**meta, "last_risk": risk_f, "last_bracket": br_f},
    )
