"""
Trail Activation Monitor

Evaluates filled structural positions for trailing stop activation readiness.
When activation conditions are met, replaces the existing fixed stop with a
trailing stop at IB and updates LIVE_ORDERS state.

Activation types:
  IMMEDIATE             — trail as soon as entry fills
  MFE_RISK_MULTIPLE     — trail when unrealized profit >= N × risk distance
  STRUCTURAL_LEVEL_BREAK — trail when price breaks through next structural level

This module is called from the API endpoint and can be integrated into
periodic reconciliation cycles.
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class TrailCandidate:
    """A filled position with a fixed stop eligible for trail activation."""
    action_id: str
    symbol: str
    direction: str
    portfolio_id: int
    account_id: str

    entry_fill_price: float
    invalidation_level: float
    risk_distance: float
    current_price: float
    mfe: float
    mfe_risk_multiple: float

    trail_style: str
    trail_activation_type: str
    trail_activation_param: float | None

    stop_order_id: str
    stop_broker_order_id: str | None
    stop_price: float | None
    oca_group: str | None

    activated: bool = False
    activation_reason: str | None = None
    trail_amount: float | None = None
    trail_percent: float | None = None


@dataclass
class TrailActivationResult:
    """Result of a full trail activation cycle."""
    evaluated_count: int = 0
    activated_count: int = 0
    skipped_count: int = 0
    error_count: int = 0
    candidates: list[dict] = field(default_factory=list)
    errors: list[dict] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _safe_float(v, default=None) -> float | None:
    if v is None:
        return default
    try:
        return float(v)
    except (ValueError, TypeError):
        return default


def _parse_json_field(v) -> dict:
    if isinstance(v, dict):
        return v
    if isinstance(v, str):
        try:
            parsed = json.loads(v)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


# ---------------------------------------------------------------------------
# Fetch eligible candidates from DB
# ---------------------------------------------------------------------------


def _fetch_trail_candidates(cur, portfolio_id: int | None = None) -> list[dict]:
    """
    Find filled structural positions that have a fixed stop but trail not yet activated.
    Joins LIVE_ACTIONS (structural context) with LIVE_ORDERS (entry + stop legs).
    """
    portfolio_filter = ""
    params: list[Any] = []
    if portfolio_id is not None:
        portfolio_filter = "AND la.PORTFOLIO_ID = %s"
        params.append(portfolio_id)

    sql = f"""
    WITH entry_legs AS (
        SELECT
            lo.ACTION_ID,
            lo.ORDER_ID AS ENTRY_ORDER_ID,
            lo.LIMIT_PRICE AS ENTRY_PRICE,
            lo.QTY_ORDERED,
            lo.STATUS AS ENTRY_STATUS
        FROM MIP.LIVE.LIVE_ORDERS lo
        WHERE lo.ORDER_ROLE = 'ENTRY'
          AND lo.STATUS IN ('FILLED', 'ACKNOWLEDGED', 'PARTIAL_FILL')
    ),
    stop_legs AS (
        SELECT
            lo.ACTION_ID,
            lo.ORDER_ID AS STOP_ORDER_ID,
            lo.BROKER_ORDER_ID AS STOP_BROKER_ORDER_ID,
            lo.STOP_PRICE,
            lo.LIMIT_PRICE AS STOP_LIMIT_PRICE,
            lo.OCA_GROUP,
            lo.TRAIL_ACTIVATED,
            lo.STATUS AS STOP_STATUS,
            lo.ORDER_ROLE,
            lo.PROTECTION_TYPE
        FROM MIP.LIVE.LIVE_ORDERS lo
        WHERE lo.ORDER_ROLE = 'PROTECTIVE_STOP'
          AND lo.PROTECTION_TYPE = 'FIXED_STOP'
          AND COALESCE(lo.TRAIL_ACTIVATED, FALSE) = FALSE
          AND lo.STATUS IN ('SUBMITTED', 'ACKNOWLEDGED', 'PRESUBMITTED')
    )
    SELECT
        la.ACTION_ID,
        la.SYMBOL,
        la.DIRECTION,
        la.PORTFOLIO_ID,
        la.INVALIDATION_LEVEL,
        la.TRAIL_STYLE,
        la.TRAIL_ACTIVATION_TYPE,
        la.TRAIL_ACTIVATION_PARAM,
        la.TRAIL_PARAMS,
        la.ENTRY_ZONE_LOW,
        la.ENTRY_ZONE_HIGH,
        el.ENTRY_PRICE,
        el.ENTRY_ORDER_ID,
        el.ENTRY_STATUS,
        sl.STOP_ORDER_ID,
        sl.STOP_BROKER_ORDER_ID,
        sl.STOP_PRICE,
        sl.STOP_LIMIT_PRICE,
        sl.OCA_GROUP,
        lpc.IBKR_ACCOUNT_ID
    FROM MIP.LIVE.LIVE_ACTIONS la
    JOIN entry_legs el ON el.ACTION_ID = la.ACTION_ID
    JOIN stop_legs sl ON sl.ACTION_ID = la.ACTION_ID
    JOIN MIP.LIVE.LIVE_PORTFOLIO_CONFIG lpc ON lpc.PORTFOLIO_ID = la.PORTFOLIO_ID
    WHERE la.TRAIL_STYLE IS NOT NULL
      AND la.STATUS = 'EXECUTION_REQUESTED'
      {portfolio_filter}
    """
    cur.execute(sql, params)
    cols = [desc[0] for desc in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def _fetch_current_prices(cur, symbols: list[str]) -> dict[str, float]:
    """Get latest prices from broker snapshots (POSITION type has MARKET_VALUE)."""
    if not symbols:
        return {}
    placeholders = ",".join(["%s"] * len(symbols))
    cur.execute(
        f"""
        SELECT UPPER(SYMBOL) AS SYMBOL, MARKET_PRICE
        FROM MIP.LIVE.BROKER_SNAPSHOTS
        WHERE SNAPSHOT_TYPE = 'POSITION'
          AND UPPER(SYMBOL) IN ({placeholders})
        QUALIFY ROW_NUMBER() OVER (PARTITION BY UPPER(SYMBOL) ORDER BY SNAPSHOT_TS DESC) = 1
        """,
        symbols,
    )
    cols = [desc[0] for desc in cur.description]
    rows = [dict(zip(cols, row)) for row in cur.fetchall()]
    return {
        r["SYMBOL"]: float(r["MARKET_PRICE"])
        for r in rows
        if r.get("MARKET_PRICE") is not None
    }


# ---------------------------------------------------------------------------
# Activation evaluation
# ---------------------------------------------------------------------------


def _evaluate_activation(candidate: TrailCandidate) -> TrailCandidate:
    """
    Check if a candidate meets its trail activation condition.
    Mutates candidate.activated and candidate.activation_reason.
    """
    act_type = (candidate.trail_activation_type or "").upper()

    if act_type == "IMMEDIATE":
        candidate.activated = True
        candidate.activation_reason = "IMMEDIATE — trail on fill"
        return candidate

    if act_type == "MFE_RISK_MULTIPLE":
        param = candidate.trail_activation_param or 1.0
        if candidate.risk_distance > 0 and candidate.mfe_risk_multiple >= param:
            candidate.activated = True
            candidate.activation_reason = (
                f"MFE {candidate.mfe_risk_multiple:.2f}x >= {param:.2f}x threshold"
            )
        return candidate

    if act_type == "STRUCTURAL_LEVEL_BREAK":
        direction = candidate.direction.upper()
        if direction == "LONG":
            if candidate.current_price > candidate.entry_fill_price:
                profit_pct = (candidate.current_price - candidate.entry_fill_price) / candidate.entry_fill_price
                if profit_pct >= 0.02:
                    candidate.activated = True
                    candidate.activation_reason = (
                        f"STRUCTURAL_LEVEL_BREAK — price {candidate.current_price:.2f} "
                        f"above entry {candidate.entry_fill_price:.2f} "
                        f"(+{profit_pct*100:.1f}%)"
                    )
        elif direction == "SHORT":
            if candidate.current_price < candidate.entry_fill_price:
                profit_pct = (candidate.entry_fill_price - candidate.current_price) / candidate.entry_fill_price
                if profit_pct >= 0.02:
                    candidate.activated = True
                    candidate.activation_reason = (
                        f"STRUCTURAL_LEVEL_BREAK — price {candidate.current_price:.2f} "
                        f"below entry {candidate.entry_fill_price:.2f} "
                        f"(+{profit_pct*100:.1f}%)"
                    )
        return candidate

    logger.warning("Unknown TRAIL_ACTIVATION_TYPE=%s for action=%s", act_type, candidate.action_id)
    return candidate


def _compute_trail_params(candidate: TrailCandidate) -> TrailCandidate:
    """
    Derive trail_amount or trail_percent for the trailing stop order.
    Uses risk distance as the default trail amount.
    """
    if candidate.risk_distance > 0:
        candidate.trail_amount = round(candidate.risk_distance, 4)
    else:
        candidate.trail_percent = 3.0
    return candidate


# ---------------------------------------------------------------------------
# Core evaluation loop
# ---------------------------------------------------------------------------


def evaluate_trail_candidates(
    cur,
    portfolio_id: int | None = None,
) -> list[TrailCandidate]:
    """
    Fetch and evaluate all eligible trail candidates.
    Returns list of TrailCandidate with activation status set.
    """
    raw_candidates = _fetch_trail_candidates(cur, portfolio_id)
    if not raw_candidates:
        return []

    symbols = list({str(r.get("SYMBOL", "")).upper() for r in raw_candidates if r.get("SYMBOL")})
    prices = _fetch_current_prices(cur, symbols)

    results: list[TrailCandidate] = []
    for row in raw_candidates:
        symbol = str(row.get("SYMBOL", "")).upper()
        direction = str(row.get("DIRECTION", "LONG")).upper()
        entry_price = _safe_float(row.get("ENTRY_PRICE"))
        invalidation = _safe_float(row.get("INVALIDATION_LEVEL"))
        current_price = prices.get(symbol)

        if entry_price is None or invalidation is None or current_price is None:
            logger.warning(
                "Skipping %s/%s: missing price data (entry=%s, inv=%s, current=%s)",
                symbol, row.get("ACTION_ID"), entry_price, invalidation, current_price,
            )
            continue

        risk_distance = abs(entry_price - invalidation)
        if direction == "LONG":
            mfe = max(0.0, current_price - entry_price)
        else:
            mfe = max(0.0, entry_price - current_price)
        mfe_multiple = mfe / risk_distance if risk_distance > 0 else 0.0

        candidate = TrailCandidate(
            action_id=row["ACTION_ID"],
            symbol=symbol,
            direction=direction,
            portfolio_id=int(row["PORTFOLIO_ID"]),
            account_id=str(row.get("IBKR_ACCOUNT_ID") or ""),
            entry_fill_price=entry_price,
            invalidation_level=invalidation,
            risk_distance=risk_distance,
            current_price=current_price,
            mfe=mfe,
            mfe_risk_multiple=mfe_multiple,
            trail_style=str(row.get("TRAIL_STYLE") or ""),
            trail_activation_type=str(row.get("TRAIL_ACTIVATION_TYPE") or ""),
            trail_activation_param=_safe_float(row.get("TRAIL_ACTIVATION_PARAM")),
            stop_order_id=str(row.get("STOP_ORDER_ID") or ""),
            stop_broker_order_id=str(row.get("STOP_BROKER_ORDER_ID") or "") or None,
            stop_price=_safe_float(row.get("STOP_PRICE") or row.get("STOP_LIMIT_PRICE")),
            oca_group=row.get("OCA_GROUP"),
        )

        _evaluate_activation(candidate)
        if candidate.activated:
            _compute_trail_params(candidate)

        results.append(candidate)

    return results


# ---------------------------------------------------------------------------
# Trail replacement: cancel fixed stop -> place trailing stop -> update DB
# ---------------------------------------------------------------------------


def execute_trail_replacement(
    cur,
    candidate: TrailCandidate,
    cancel_fn,
    place_fn,
    dry_run: bool = False,
) -> dict:
    """
    Execute the trail replacement for a single activated candidate.

    cancel_fn: callable(account, symbol, broker_order_id) -> dict
    place_fn:  callable(account, symbol, side, qty, trail_amount, trail_percent, oca_group) -> dict

    Returns status dict with cancel/place results.
    """
    result: dict[str, Any] = {
        "action_id": candidate.action_id,
        "symbol": candidate.symbol,
        "direction": candidate.direction,
        "activation_reason": candidate.activation_reason,
        "trail_amount": candidate.trail_amount,
        "trail_percent": candidate.trail_percent,
        "dry_run": dry_run,
    }

    if dry_run:
        result["status"] = "DRY_RUN"
        logger.info(
            "DRY_RUN: Would activate trail for %s %s — %s (amount=%s, pct=%s)",
            candidate.symbol, candidate.direction, candidate.activation_reason,
            candidate.trail_amount, candidate.trail_percent,
        )
        return result

    exit_side = "SELL" if candidate.direction == "LONG" else "BUY"

    # 1. Cancel the existing fixed stop
    cancel_result = None
    try:
        cancel_result = cancel_fn(
            account=candidate.account_id,
            symbol=candidate.symbol,
            broker_order_id=candidate.stop_broker_order_id,
        )
        result["cancel_result"] = cancel_result
    except Exception as exc:
        result["status"] = "CANCEL_FAILED"
        result["error"] = str(exc)
        logger.error("Trail activation cancel failed for %s: %s", candidate.symbol, exc)
        return result

    # 2. Fetch the stop order's qty
    cur.execute(
        "SELECT QTY_ORDERED FROM MIP.LIVE.LIVE_ORDERS WHERE ORDER_ID = %s",
        (candidate.stop_order_id,),
    )
    qty_row = cur.fetchone()
    qty = float(qty_row[0]) if qty_row else 0.0
    if qty <= 0:
        result["status"] = "NO_QTY"
        result["error"] = "Stop order has no quantity"
        return result

    # 3. Place trailing stop order
    place_result = None
    try:
        place_result = place_fn(
            account=candidate.account_id,
            symbol=candidate.symbol,
            side=exit_side,
            qty=qty,
            trail_amount=candidate.trail_amount,
            trail_percent=candidate.trail_percent,
            oca_group=candidate.oca_group,
        )
        result["place_result"] = place_result
    except Exception as exc:
        result["status"] = "PLACE_FAILED"
        result["error"] = str(exc)
        logger.error("Trail activation place failed for %s: %s", candidate.symbol, exc)
        return result

    # 4. Update existing LIVE_ORDERS stop to reflect trail activation
    now_utc = datetime.now(timezone.utc)
    cur.execute(
        """
        UPDATE MIP.LIVE.LIVE_ORDERS
        SET ORDER_ROLE = 'TRAILING_STOP',
            PROTECTION_TYPE = 'TRAILING_STOP',
            TRAIL_STYLE = %s,
            TRAIL_ACTIVATED = TRUE,
            TRAIL_ACTIVATED_AT = %s,
            TRAIL_AMOUNT = %s,
            TRAIL_PERCENT = %s,
            BROKER_TRAIL_STATE = 'ACTIVE',
            LAST_UPDATED_AT = CURRENT_TIMESTAMP()
        WHERE ORDER_ID = %s
        """,
        (
            candidate.trail_style,
            now_utc,
            candidate.trail_amount,
            candidate.trail_percent,
            candidate.stop_order_id,
        ),
    )

    # 5. Insert the new trail order into LIVE_ORDERS
    trail_orders = (place_result or {}).get("orders") or []
    trail_broker_id = None
    for to in trail_orders:
        if str(to.get("role") or "").upper() == "TRAILING_STOP":
            trail_broker_id = to.get("perm_id") or to.get("order_id")
            break
    if not trail_broker_id and trail_orders:
        to = trail_orders[0]
        trail_broker_id = to.get("perm_id") or to.get("order_id")

    new_order_id = str(uuid.uuid4())
    cur.execute(
        """
        INSERT INTO MIP.LIVE.LIVE_ORDERS (
            ORDER_ID, ACTION_ID, PORTFOLIO_ID, IBKR_ACCOUNT_ID,
            IDEMPOTENCY_KEY, BROKER_ORDER_ID, STATUS,
            SYMBOL, SIDE, ACTION_INTENT, ORDER_TYPE, QTY_ORDERED, LIMIT_PRICE,
            PARENT_ORDER_ID, ORDER_ROLE, PROTECTION_TYPE, OCA_GROUP,
            TRAIL_STYLE, TRAIL_ACTIVATED, TRAIL_ACTIVATED_AT,
            TRAIL_AMOUNT, TRAIL_PERCENT, BROKER_TRAIL_STATE,
            SUBMITTED_AT, ACKNOWLEDGED_AT, LAST_UPDATED_AT, CREATED_AT
        )
        VALUES (
            %s, %s, %s, %s,
            %s, %s, 'SUBMITTED',
            %s, %s, 'ENTRY', 'TRAIL', %s, NULL,
            %s, 'TRAILING_STOP', 'TRAILING_STOP', %s,
            %s, TRUE, %s,
            %s, %s, 'ACTIVE',
            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
        )
        """,
        (
            new_order_id,
            candidate.action_id,
            candidate.portfolio_id,
            candidate.account_id,
            f"{candidate.action_id}:TRAIL:{now_utc.strftime('%Y%m%d%H%M%S')}",
            str(trail_broker_id) if trail_broker_id else None,
            candidate.symbol,
            exit_side,
            qty,
            candidate.stop_order_id,
            candidate.oca_group,
            candidate.trail_style,
            now_utc,
            candidate.trail_amount,
            candidate.trail_percent,
        ),
    )

    # 6. Mark old stop as superseded
    cur.execute(
        """
        UPDATE MIP.LIVE.LIVE_ORDERS
        SET STATUS = 'CANCELLED',
            LAST_UPDATED_AT = CURRENT_TIMESTAMP()
        WHERE ORDER_ID = %s
          AND STATUS NOT IN ('FILLED', 'CANCELLED')
        """,
        (candidate.stop_order_id,),
    )

    result["status"] = "ACTIVATED"
    result["new_trail_order_id"] = new_order_id
    result["old_stop_order_id"] = candidate.stop_order_id
    logger.info(
        "Trail activated for %s %s: %s -> trail order %s",
        candidate.symbol, candidate.direction, candidate.activation_reason, new_order_id,
    )
    return result


# ---------------------------------------------------------------------------
# Full cycle: evaluate + execute
# ---------------------------------------------------------------------------


def run_trail_activation_cycle(
    cur,
    cancel_fn,
    place_fn,
    portfolio_id: int | None = None,
    dry_run: bool = False,
) -> TrailActivationResult:
    """
    Run one full trail activation cycle:
    1. Evaluate all eligible candidates
    2. Execute replacements for activated candidates
    """
    result = TrailActivationResult()
    candidates = evaluate_trail_candidates(cur, portfolio_id)
    result.evaluated_count = len(candidates)

    for candidate in candidates:
        cdict = asdict(candidate)
        if not candidate.activated:
            cdict["status"] = "NOT_READY"
            result.candidates.append(cdict)
            result.skipped_count += 1
            continue

        try:
            exec_result = execute_trail_replacement(
                cur, candidate, cancel_fn, place_fn, dry_run=dry_run,
            )
            cdict.update(exec_result)
            result.candidates.append(cdict)
            if exec_result.get("status") == "ACTIVATED":
                result.activated_count += 1
            elif exec_result.get("status") == "DRY_RUN":
                result.skipped_count += 1
            else:
                result.error_count += 1
                result.errors.append({"action_id": candidate.action_id, "error": exec_result.get("error")})
        except Exception as exc:
            result.error_count += 1
            result.errors.append({"action_id": candidate.action_id, "error": str(exc)})
            logger.error("Trail activation error for %s: %s", candidate.action_id, exc)

    return result
