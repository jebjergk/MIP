from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
import json
from statistics import pstdev
from typing import Any, Iterable

from fastapi import APIRouter, Query

from app.db import fetch_all, get_connection

router = APIRouter(prefix="/symbol-tracker", tags=["symbol-tracker"])

_INTRADAY_INTERVALS = {15, 60}
_VOL_BELOW_RATIO = 0.72
_VOL_ABOVE_RATIO = 1.35
_THESIS_STOP_BUFFER_PCT = 0.015
_THESIS_EDGE_BUFFER_RATIO = 0.10
_THESIS_OUTSIDE_WARN_MULT = 0.75


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _market_type_from_security_type(security_type: str | None) -> str:
    sec = str(security_type or "").upper()
    if sec in {"CASH", "FX", "FOREX"}:
        return "FX"
    if sec in {"ETF"}:
        return "ETF"
    return "STOCK"


def _parse_leg_type(order_type: str | None) -> str:
    ot = str(order_type or "").upper()
    if any(token in ot for token in ("STOP", "STP", "SL")):
        return "SL"
    if any(token in ot for token in ("TP", "TAKE_PROFIT", "LIMIT_TP")):
        return "TP"
    return "PARENT"


def _in_placeholders(values: Iterable[Any]) -> str:
    count = len(values) if hasattr(values, "__len__") else len(list(values))
    return ", ".join(["%s"] * count)


def _safe_progress(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator in (None, 0):
        return None
    return numerator / denominator


def _normalize_headlines(raw: Any) -> list[dict[str, Any]]:
    if raw is None:
        return []
    value = raw
    if isinstance(raw, str):
        try:
            value = json.loads(raw)
        except Exception:
            return []
    if not isinstance(value, list):
        return []
    out = []
    for item in value:
        if not isinstance(item, dict):
            continue
        title = item.get("title") or item.get("TITLE") or item.get("headline") or item.get("HEADLINE")
        if not title:
            continue
        url = item.get("url") or item.get("URL")
        url = str(url).strip() if url is not None else None
        if url and not (url.startswith("http://") or url.startswith("https://")):
            url = None
        out.append({"title": str(title), "url": url})
    return out


def _build_projection_path(
    baseline_price: float,
    avg_return: float,
    upper_return: float,
    lower_return: float,
    horizon_bars: int,
    side: str,
    projection_mode: str = "geometric",
) -> dict[str, Any]:
    if baseline_price <= 0 or horizon_bars <= 0:
        return {
            "center_path": [],
            "upper_path": [],
            "lower_path": [],
        }

    side_sign = 1.0 if side == "LONG" else -1.0
    center_target = baseline_price * (1 + side_sign * avg_return)
    upper_target = baseline_price * (1 + side_sign * upper_return)
    lower_target = baseline_price * (1 + side_sign * lower_return)

    center_path = []
    upper_path = []
    lower_path = []
    if projection_mode == "linear":
        for t in range(1, horizon_bars + 1):
            ratio = t / horizon_bars
            center_path.append({"step": t, "price": baseline_price + (center_target - baseline_price) * ratio})
            upper_path.append({"step": t, "price": baseline_price + (upper_target - baseline_price) * ratio})
            lower_path.append({"step": t, "price": baseline_price + (lower_target - baseline_price) * ratio})
    else:
        center_gross = max(1 + side_sign * avg_return, 0.0001)
        upper_gross = max(1 + side_sign * upper_return, 0.0001)
        lower_gross = max(1 + side_sign * lower_return, 0.0001)

        center_step_factor = center_gross ** (1 / horizon_bars)
        upper_step_factor = upper_gross ** (1 / horizon_bars)
        lower_step_factor = lower_gross ** (1 / horizon_bars)
        for t in range(1, horizon_bars + 1):
            center_path.append({"step": t, "price": baseline_price * (center_step_factor ** t)})
            upper_path.append({"step": t, "price": baseline_price * (upper_step_factor ** t)})
            lower_path.append({"step": t, "price": baseline_price * (lower_step_factor ** t)})

    return {
        "center_path": center_path,
        "upper_path": upper_path,
        "lower_path": lower_path,
    }


def _derive_band_from_row(row: dict[str, Any]) -> tuple[float, float, str]:
    sample_size = int(row.get("sample_size") or 0)
    avg_return = _to_float(row.get("avg_return")) or 0.0
    stddev_return = _to_float(row.get("stddev_return"))
    p10_return = _to_float(row.get("p10_return"))
    p90_return = _to_float(row.get("p90_return"))
    if sample_size >= 30 and p10_return is not None and p90_return is not None:
        return p10_return, p90_return, "PERCENTILE_BAND"
    if stddev_return is not None and stddev_return > 0:
        return avg_return - stddev_return, avg_return + stddev_return, "STDDEV_BAND"
    tolerance = max(abs(avg_return) * 0.35, 0.01)
    return avg_return - tolerance, avg_return + tolerance, "SYMMETRIC_TOLERANCE"


def _interpolate_horizon_metric(horizon_stats: dict[int, dict[str, Any]], target_h: int, metric: str) -> float | None:
    points = []
    for h, row in horizon_stats.items():
        value = _to_float(row.get(metric))
        if value is not None:
            points.append((int(h), float(value)))
    if not points:
        return None
    points.sort(key=lambda x: x[0])
    for h, v in points:
        if h == target_h:
            return v
    lower = [p for p in points if p[0] < target_h]
    upper = [p for p in points if p[0] > target_h]
    if lower and upper:
        h0, v0 = lower[-1]
        h1, v1 = upper[0]
        if h1 == h0:
            return v0
        ratio = (target_h - h0) / (h1 - h0)
        return v0 + (v1 - v0) * ratio
    if lower:
        return lower[-1][1]
    return upper[0][1]


def _pick_horizon_row(horizon_stats: dict[int, dict[str, Any]], target_h: int) -> dict[str, Any] | None:
    if not horizon_stats:
        return None
    if target_h in horizon_stats:
        return horizon_stats[target_h]
    return min(horizon_stats.values(), key=lambda r: abs(int(r.get("horizon_bars") or target_h) - target_h))


def _build_stitched_projection_path(
    baseline_price: float,
    side: str,
    horizon_bars: int,
    horizon_stats: dict[int, dict[str, Any]],
) -> dict[str, Any]:
    if baseline_price <= 0 or horizon_bars <= 0 or not horizon_stats:
        return {"center_path": [], "upper_path": [], "lower_path": []}
    side_sign = 1.0 if side == "LONG" else -1.0

    lower_map: dict[int, float] = {}
    upper_map: dict[int, float] = {}
    for h, row in horizon_stats.items():
        lr, ur, _ = _derive_band_from_row(row)
        lower_map[h] = lr
        upper_map[h] = ur
    lower_row_stats = {h: {"metric": v} for h, v in lower_map.items()}
    upper_row_stats = {h: {"metric": v} for h, v in upper_map.items()}

    center_path = []
    upper_path = []
    lower_path = []
    for t in range(1, horizon_bars + 1):
        avg_r = _interpolate_horizon_metric(horizon_stats, t, "avg_return")
        low_r = _interpolate_horizon_metric(lower_row_stats, t, "metric")
        up_r = _interpolate_horizon_metric(upper_row_stats, t, "metric")
        avg_r = avg_r if avg_r is not None else 0.0
        low_r = low_r if low_r is not None else avg_r
        up_r = up_r if up_r is not None else avg_r

        center_price = baseline_price * max(1 + side_sign * avg_r, 0.0001)
        upper_price = baseline_price * max(1 + side_sign * up_r, 0.0001)
        lower_price = baseline_price * max(1 + side_sign * low_r, 0.0001)
        center_path.append({"step": t, "price": center_price})
        upper_path.append({"step": t, "price": upper_price})
        lower_path.append({"step": t, "price": lower_price})

    return {"center_path": center_path, "upper_path": upper_path, "lower_path": lower_path}


def _compute_live_volatility(closes: list[float]) -> float | None:
    if len(closes) < 3:
        return None
    returns = []
    for i in range(1, len(closes)):
        prev = closes[i - 1]
        curr = closes[i]
        if prev and prev > 0 and curr is not None:
            returns.append((curr / prev) - 1)
    if len(returns) < 2:
        return None
    return pstdev(returns)


def _volatility_label(live_volatility: float | None, trained_volatility: float | None) -> str:
    if live_volatility is None or trained_volatility in (None, 0):
        return "UNKNOWN"
    ratio = live_volatility / trained_volatility
    if ratio < _VOL_BELOW_RATIO:
        return "LIVE_VOL_BELOW_TRAINED_REGIME"
    if ratio > _VOL_ABOVE_RATIO:
        return "LIVE_VOL_ABOVE_TRAINED_REGIME"
    return "LIVE_VOL_ALIGNED"


def _thesis_status(
    side: str,
    entry_price: float | None,
    current_price: float | None,
    sl_price: float | None,
    expectation_center_end: float | None,
    expectation_upper_end: float | None,
    expectation_lower_end: float | None,
) -> dict[str, str]:
    if current_price is None:
        return {"status": "WEAKENING", "reason": "Current price unavailable."}

    if sl_price is not None:
        if side == "LONG" and current_price <= sl_price:
            return {"status": "INVALIDATED", "reason": "Current price crossed stop-loss."}
        if side == "SHORT" and current_price >= sl_price:
            return {"status": "INVALIDATED", "reason": "Current price crossed stop-loss."}
        if current_price > 0:
            distance_to_sl_pct = ((current_price - sl_price) / current_price) if side == "LONG" else ((sl_price - current_price) / current_price)
            if distance_to_sl_pct <= _THESIS_STOP_BUFFER_PCT:
                return {"status": "WEAKENING", "reason": "Live price is close to stop-loss."}

    if (
        expectation_center_end is None
        or expectation_upper_end is None
        or expectation_lower_end is None
    ):
        return {"status": "THESIS_INTACT", "reason": "No expectation band available."}

    low = min(expectation_lower_end, expectation_upper_end)
    high = max(expectation_lower_end, expectation_upper_end)
    if low <= current_price <= high:
        band_width = high - low
        if band_width > 0:
            edge_distance = min(abs(current_price - low), abs(high - current_price))
            if edge_distance <= (band_width * _THESIS_EDGE_BUFFER_RATIO):
                return {"status": "WEAKENING", "reason": "Live price is near the edge of trained expectation range."}
        return {"status": "THESIS_INTACT", "reason": "Live price is inside trained expectation range."}

    band_half_width = abs(high - low) / 2
    if band_half_width == 0:
        return {"status": "WEAKENING", "reason": "Expectation band collapsed; monitor closely."}
    outside = min(abs(current_price - low), abs(current_price - high))
    if outside <= _THESIS_OUTSIDE_WARN_MULT * band_half_width:
        return {"status": "WEAKENING", "reason": "Live price drifted outside the trained expectation range."}

    if entry_price and current_price:
        move_from_entry = abs((current_price / entry_price) - 1) if entry_price > 0 else None
        expected_move = abs((expectation_center_end / entry_price) - 1) if entry_price and expectation_center_end and entry_price > 0 else None
        if move_from_entry is not None and expected_move is not None and expected_move > 0 and move_from_entry >= (2.0 * expected_move):
            return {"status": "INVALIDATED", "reason": "Live move materially exceeded trained move profile."}
    return {"status": "INVALIDATED", "reason": "Live price materially diverged from trained expectation range."}


@router.get("/tiles")
def get_symbol_tracker_tiles(
    mode: str = Query("intraday", pattern="^(intraday|daily)$"),
    chart_style: str = Query("line", pattern="^(line|candles)$"),
    horizon_bars: int = Query(5, ge=1, le=60),
    daily_window_bars: int = Query(120, ge=30, le=300),
    intraday_window_bars: int = Query(120, ge=30, le=400),
    intraday_interval_minutes: int = Query(60, ge=1, le=240),
    projection_mode: str = Query("stitched", pattern="^(stitched|geometric|linear)$"),
):
    interval_minutes = 1440 if mode == "daily" else intraday_interval_minutes
    if mode == "intraday" and interval_minutes not in _INTRADAY_INTERVALS:
        interval_minutes = 60
    window_bars = daily_window_bars if mode == "daily" else intraday_window_bars

    conn = get_connection()
    try:
        cur = conn.cursor()

        cur.execute(
            """
            select PORTFOLIO_ID, IBKR_ACCOUNT_ID
            from MIP.LIVE.LIVE_PORTFOLIO_CONFIG
            where coalesce(IS_ACTIVE, true) = true
            order by PORTFOLIO_ID
            limit 1
            """
        )
        cfg_rows = fetch_all(cur)
        if not cfg_rows:
            return {
                "ok": True,
                "mode": mode,
                "chart_style": chart_style,
                "horizon_bars": horizon_bars,
                "tiles": [],
                "counts": {"tiles": 0},
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "disclaimer": "Training-implied range only. Historical context, not a forecast.",
            }

        cfg = cfg_rows[0]
        portfolio_id = cfg.get("PORTFOLIO_ID")
        account_id = cfg.get("IBKR_ACCOUNT_ID")

        cur.execute(
            """
            with latest_nav as (
              select max(SNAPSHOT_TS) as SNAPSHOT_TS
              from MIP.LIVE.BROKER_SNAPSHOTS
              where SNAPSHOT_TYPE = 'NAV'
                and IBKR_ACCOUNT_ID = %s
            )
            select
              s.SNAPSHOT_TS,
              s.SYMBOL,
              s.SECURITY_TYPE,
              s.POSITION_QTY,
              s.AVG_COST,
              s.MARKET_VALUE,
              s.UNREALIZED_PNL
            from MIP.LIVE.BROKER_SNAPSHOTS s
            join latest_nav ln on s.SNAPSHOT_TS = ln.SNAPSHOT_TS
            where s.SNAPSHOT_TYPE = 'POSITION'
              and s.IBKR_ACCOUNT_ID = %s
              and coalesce(s.POSITION_QTY, 0) <> 0
            order by abs(s.POSITION_QTY) desc, s.SYMBOL
            """,
            (account_id, account_id),
        )
        positions = fetch_all(cur)
        if not positions:
            return {
                "ok": True,
                "mode": mode,
                "chart_style": chart_style,
                "horizon_bars": horizon_bars,
                "tiles": [],
                "counts": {"tiles": 0},
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "disclaimer": "Training-implied range only. Historical context, not a forecast.",
            }

        symbols = [str((p.get("SYMBOL") or "")).upper() for p in positions if p.get("SYMBOL")]
        symbol_params = list(dict.fromkeys(symbols))
        placeholders = _in_placeholders(symbol_params)

        cur.execute(
            f"""
            select
              lo.ACTION_ID,
              upper(lo.SYMBOL) as SYMBOL,
              lo.STATUS,
              lo.SIDE,
              lo.ORDER_TYPE,
              lo.LIMIT_PRICE,
              lo.AVG_FILL_PRICE,
              lo.QTY_FILLED,
              lo.QTY_ORDERED,
              lo.FILLED_AT,
              lo.LAST_UPDATED_AT,
              lo.CREATED_AT
            from MIP.LIVE.LIVE_ORDERS lo
            join MIP.LIVE.LIVE_ACTIONS la
              on la.ACTION_ID = lo.ACTION_ID
            where la.PORTFOLIO_ID = %s
              and upper(lo.SYMBOL) in ({placeholders})
              and coalesce(lo.LAST_UPDATED_AT, lo.CREATED_AT) >= dateadd(day, -120, current_timestamp())
            order by coalesce(lo.LAST_UPDATED_AT, lo.CREATED_AT) desc
            """,
            [portfolio_id, *symbol_params],
        )
        order_rows = fetch_all(cur)

        cur.execute(
            f"""
            select
              upper(la.SYMBOL) as SYMBOL,
              la.ACTION_ID,
              la.STATUS,
              la.COMMITTEE_VERDICT,
              la.CREATED_AT,
              la.UPDATED_AT
            from MIP.LIVE.LIVE_ACTIONS la
            where la.PORTFOLIO_ID = %s
              and upper(la.SYMBOL) in ({placeholders})
              and coalesce(la.UPDATED_AT, la.CREATED_AT) >= dateadd(day, -30, current_timestamp())
            order by coalesce(la.UPDATED_AT, la.CREATED_AT) desc
            """,
            [portfolio_id, *symbol_params],
        )
        action_rows = fetch_all(cur)

        cur.execute(
            f"""
            with bars as (
              select
                SYMBOL, MARKET_TYPE, TS, OPEN, HIGH, LOW, CLOSE, VOLUME,
                row_number() over(partition by SYMBOL, MARKET_TYPE order by TS desc) as RN
              from MIP.MART.MARKET_BARS
              where INTERVAL_MINUTES = %s
                and SYMBOL in ({placeholders})
            )
            select SYMBOL, MARKET_TYPE, TS, OPEN, HIGH, LOW, CLOSE, VOLUME
            from bars
            where RN <= %s
            order by SYMBOL, MARKET_TYPE, TS
            """,
            [interval_minutes, *symbol_params, window_bars],
        )
        bars_rows = fetch_all(cur)

        cur.execute(
            f"""
            select
              r.SYMBOL,
              r.MARKET_TYPE,
              o.HORIZON_BARS,
              count(*) as SAMPLE_SIZE,
              avg(o.REALIZED_RETURN) as AVG_RETURN,
              stddev_samp(o.REALIZED_RETURN) as STDDEV_RETURN,
              percentile_cont(0.10) within group (order by o.REALIZED_RETURN) as P10_RETURN,
              percentile_cont(0.90) within group (order by o.REALIZED_RETURN) as P90_RETURN
            from MIP.APP.RECOMMENDATION_OUTCOMES o
            join MIP.APP.RECOMMENDATION_LOG r
              on r.RECOMMENDATION_ID = o.RECOMMENDATION_ID
            where r.INTERVAL_MINUTES = 1440
              and o.HORIZON_BARS <= %s
              and r.SYMBOL in ({placeholders})
            group by r.SYMBOL, r.MARKET_TYPE, o.HORIZON_BARS
            """,
            [horizon_bars, *symbol_params],
        )
        expectation_rows = fetch_all(cur)

        cur.execute(
            f"""
            select
              n.SYMBOL,
              n.MARKET_TYPE,
              coalesce(n.BADGE, 'NORMAL') as NEWS_CONTEXT_BADGE,
              n.SNAPSHOT_TS,
              n.TOP_CLUSTERS as TOP_HEADLINES
            from MIP.MART.V_NEWS_AGG_LATEST n
            where n.SYMBOL in ({placeholders})
            """,
            [*symbol_params],
        )
        news_rows = fetch_all(cur)

        bars_by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
        market_type_by_symbol: dict[str, str] = {}
        for row in bars_rows:
            symbol = str(row.get("SYMBOL") or "").upper()
            if not symbol:
                continue
            mkt = row.get("MARKET_TYPE")
            if symbol not in market_type_by_symbol and mkt:
                market_type_by_symbol[symbol] = str(mkt)
            bars_by_symbol[symbol].append(
                {
                    "ts": _iso(row.get("TS")),
                    "open": _to_float(row.get("OPEN")),
                    "high": _to_float(row.get("HIGH")),
                    "low": _to_float(row.get("LOW")),
                    "close": _to_float(row.get("CLOSE")),
                    "volume": _to_float(row.get("VOLUME")),
                }
            )

        expectation_by_symbol: dict[str, dict[int, dict[str, Any]]] = defaultdict(dict)
        for row in expectation_rows:
            symbol = str(row.get("SYMBOL") or "").upper()
            if not symbol:
                continue
            h = int(row.get("HORIZON_BARS") or 0)
            expectation_by_symbol[symbol][h] = {
                "horizon_bars": h,
                "sample_size": int(row.get("SAMPLE_SIZE") or 0),
                "avg_return": _to_float(row.get("AVG_RETURN")) or 0.0,
                "stddev_return": _to_float(row.get("STDDEV_RETURN")),
                "p10_return": _to_float(row.get("P10_RETURN")),
                "p90_return": _to_float(row.get("P90_RETURN")),
                "market_type": row.get("MARKET_TYPE"),
            }

        protection_by_symbol: dict[str, dict[str, Any]] = defaultdict(
            lambda: {"tp_price": None, "sl_price": None, "entry_price": None, "opened_at": None, "action_id": None}
        )
        events_by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in order_rows:
            symbol = str(row.get("SYMBOL") or "").upper()
            if not symbol:
                continue
            leg_type = _parse_leg_type(row.get("ORDER_TYPE"))
            limit_price = _to_float(row.get("LIMIT_PRICE"))
            fill_price = _to_float(row.get("AVG_FILL_PRICE")) or _to_float(row.get("LIMIT_PRICE"))
            fill_ts = row.get("FILLED_AT") or row.get("LAST_UPDATED_AT") or row.get("CREATED_AT")
            side = str(row.get("SIDE") or "").upper()

            if leg_type == "TP" and protection_by_symbol[symbol]["tp_price"] is None and limit_price is not None:
                protection_by_symbol[symbol]["tp_price"] = limit_price
                protection_by_symbol[symbol]["action_id"] = row.get("ACTION_ID")
            elif leg_type == "SL" and protection_by_symbol[symbol]["sl_price"] is None and limit_price is not None:
                protection_by_symbol[symbol]["sl_price"] = limit_price
                protection_by_symbol[symbol]["action_id"] = row.get("ACTION_ID")
            elif leg_type == "PARENT":
                if fill_price is not None and protection_by_symbol[symbol]["entry_price"] is None:
                    protection_by_symbol[symbol]["entry_price"] = fill_price
                if fill_ts is not None and protection_by_symbol[symbol]["opened_at"] is None:
                    protection_by_symbol[symbol]["opened_at"] = fill_ts

            if side in {"BUY", "SELL"} and fill_price is not None and protection_by_symbol[symbol]["entry_price"] is None:
                protection_by_symbol[symbol]["entry_price"] = fill_price
                protection_by_symbol[symbol]["opened_at"] = fill_ts

            status = str(row.get("STATUS") or "").upper()
            fill_qty = _to_float(row.get("QTY_FILLED")) or 0.0
            if status in {"PARTIAL_FILL", "FILLED"} or fill_qty > 0:
                event_ts = row.get("FILLED_AT") or row.get("LAST_UPDATED_AT") or row.get("CREATED_AT")
                if event_ts:
                    events_by_symbol[symbol].append(
                        {
                            "type": "FILL",
                            "ts": _iso(event_ts),
                            "label": f"{side or 'ORDER'} fill",
                            "severity": "info",
                            "meta": {
                                "status": status,
                                "qty_filled": fill_qty,
                                "price": fill_price,
                            },
                        }
                    )

        for row in action_rows:
            symbol = str(row.get("SYMBOL") or "").upper()
            if not symbol:
                continue
            event_ts = row.get("UPDATED_AT") or row.get("CREATED_AT")
            if not event_ts:
                continue
            status = str(row.get("STATUS") or "").upper()
            verdict = str(row.get("COMMITTEE_VERDICT") or "").upper()
            severity = "warn" if verdict == "BLOCK" or status == "OPEN_BLOCKED" else "info"
            events_by_symbol[symbol].append(
                {
                    "type": "ACTION",
                    "ts": _iso(event_ts),
                    "label": f"{status or 'ACTION'}{f'/{verdict}' if verdict else ''}",
                    "severity": severity,
                    "meta": {
                        "action_id": row.get("ACTION_ID"),
                        "status": status,
                        "committee_verdict": verdict or None,
                    },
                }
            )

        for row in news_rows:
            symbol = str(row.get("SYMBOL") or "").upper()
            if not symbol:
                continue
            snapshot_ts = row.get("SNAPSHOT_TS")
            headlines = _normalize_headlines(row.get("TOP_HEADLINES"))
            badge = str(row.get("NEWS_CONTEXT_BADGE") or "NORMAL").upper()
            severity = "warn" if badge in {"HOT", "RISK"} else "info"
            for headline in headlines[:2]:
                events_by_symbol[symbol].append(
                    {
                        "type": "NEWS",
                        "ts": _iso(snapshot_ts),
                        "label": headline.get("title"),
                        "severity": severity,
                        "url": headline.get("url"),
                        "meta": {
                            "badge": badge,
                        },
                    }
                )

        tiles = []
        for position in positions:
            symbol = str(position.get("SYMBOL") or "").upper()
            if not symbol:
                continue
            qty = _to_float(position.get("POSITION_QTY")) or 0.0
            side = "LONG" if qty > 0 else "SHORT"
            abs_qty = abs(qty)
            avg_cost = _to_float(position.get("AVG_COST"))
            market_value = _to_float(position.get("MARKET_VALUE"))
            current_price = None
            if market_value is not None and abs_qty > 0:
                current_price = abs(market_value) / abs_qty
            unrealized_pnl = _to_float(position.get("UNREALIZED_PNL"))
            sec_type = position.get("SECURITY_TYPE")
            market_type = market_type_by_symbol.get(symbol) or _market_type_from_security_type(sec_type)

            symbol_bars = bars_by_symbol.get(symbol, [])
            if symbol_bars and current_price is None:
                current_price = symbol_bars[-1].get("close")
            closes = [b.get("close") for b in symbol_bars if b.get("close") is not None]
            live_volatility = _compute_live_volatility(closes[-20:])

            protection = protection_by_symbol[symbol]
            entry_price = protection.get("entry_price") or avg_cost
            opened_at = protection.get("opened_at")
            tp_price = protection.get("tp_price")
            sl_price = protection.get("sl_price")
            tile_events = list(events_by_symbol.get(symbol, []))
            if opened_at is not None:
                tile_events.append(
                    {
                        "type": "ENTRY",
                        "ts": _iso(opened_at),
                        "label": f"{side} entry opened",
                        "severity": "info",
                        "meta": {"entry_price": entry_price},
                    }
                )
            tile_events.sort(key=lambda e: e.get("ts") or "", reverse=True)
            tile_events = tile_events[:10]

            exp = expectation_by_symbol.get(symbol) or {}
            expectation_payload = {
                "is_available": False,
                "method": "NONE",
                "sample_size": 0,
                "avg_return": None,
                "stddev_return": None,
                "horizon_bars": horizon_bars,
                "center_path": [],
                "upper_path": [],
                "lower_path": [],
                "label": f"H{horizon_bars}",
            }
            expectation_center_end = None
            expectation_upper_end = None
            expectation_lower_end = None
            if exp and entry_price:
                selected_row = _pick_horizon_row(exp, horizon_bars)
                if projection_mode == "stitched":
                    projection = _build_stitched_projection_path(
                        baseline_price=entry_price,
                        side=side,
                        horizon_bars=horizon_bars,
                        horizon_stats=exp,
                    )
                    method = "STITCHED_HORIZON"
                else:
                    avg_return = _to_float((selected_row or {}).get("avg_return")) or 0.0
                    lower_return, upper_return, method = _derive_band_from_row(selected_row or {})
                    projection = _build_projection_path(
                        baseline_price=entry_price,
                        avg_return=avg_return,
                        upper_return=upper_return,
                        lower_return=lower_return,
                        horizon_bars=horizon_bars,
                        side=side,
                        projection_mode=projection_mode,
                    )
                center_path = projection["center_path"]
                upper_path = projection["upper_path"]
                lower_path = projection["lower_path"]
                expectation_center_end = center_path[-1]["price"] if center_path else None
                expectation_upper_end = upper_path[-1]["price"] if upper_path else None
                expectation_lower_end = lower_path[-1]["price"] if lower_path else None
                expectation_payload = {
                    "is_available": True,
                    "method": method,
                    "sample_size": int((selected_row or {}).get("sample_size") or 0),
                    "avg_return": _to_float((selected_row or {}).get("avg_return")),
                    "stddev_return": _to_float((selected_row or {}).get("stddev_return")),
                    "horizon_bars": horizon_bars,
                    "center_path": center_path,
                    "upper_path": upper_path,
                    "lower_path": lower_path,
                    "label": f"H{horizon_bars}",
                }

            distance_to_tp_pct = None
            distance_to_sl_pct = None
            progress_to_tp_pct = None
            if current_price and tp_price:
                if side == "LONG":
                    distance_to_tp_pct = (tp_price - current_price) / current_price
                else:
                    distance_to_tp_pct = (current_price - tp_price) / current_price
                progress_to_tp_pct = _safe_progress(
                    (current_price - entry_price) if side == "LONG" else (entry_price - current_price),
                    (tp_price - entry_price) if side == "LONG" else (entry_price - tp_price),
                ) if entry_price is not None else None
            if current_price and sl_price:
                if side == "LONG":
                    distance_to_sl_pct = (current_price - sl_price) / current_price
                else:
                    distance_to_sl_pct = (sl_price - current_price) / current_price

            current_move_pct = None
            expected_move_pct = None
            expected_progress_pct = None
            if entry_price and current_price and entry_price > 0:
                current_move_pct = ((current_price - entry_price) / entry_price) if side == "LONG" else ((entry_price - current_price) / entry_price)
            if entry_price and expectation_center_end and entry_price > 0:
                expected_move_pct = ((expectation_center_end - entry_price) / entry_price) if side == "LONG" else ((entry_price - expectation_center_end) / entry_price)
            if current_move_pct is not None and expected_move_pct not in (None, 0):
                expected_progress_pct = current_move_pct / expected_move_pct

            r_multiple_open = None
            if entry_price is not None and current_price is not None and sl_price is not None:
                risk = (entry_price - sl_price) if side == "LONG" else (sl_price - entry_price)
                reward = (current_price - entry_price) if side == "LONG" else (entry_price - current_price)
                if risk and risk > 0:
                    r_multiple_open = reward / risk

            days_since_entry = None
            if opened_at and hasattr(opened_at, "date"):
                days_since_entry = (datetime.now(timezone.utc).date() - opened_at.date()).days

            bars_since_entry = None
            if opened_at and symbol_bars:
                opened_iso = _iso(opened_at) or ""
                bars_since_entry = sum(1 for b in symbol_bars if (b.get("ts") or "") >= opened_iso)

            trained_volatility = expectation_payload.get("stddev_return")
            vol_label = _volatility_label(live_volatility, trained_volatility)
            thesis = _thesis_status(
                side=side,
                entry_price=entry_price,
                current_price=current_price,
                sl_price=sl_price,
                expectation_center_end=expectation_center_end,
                expectation_upper_end=expectation_upper_end,
                expectation_lower_end=expectation_lower_end,
            )

            status_badges = []
            if tp_price is not None and sl_price is not None:
                status_badges.append("PROTECTED_FULL")
            elif tp_price is not None or sl_price is not None:
                status_badges.append("PROTECTED_PARTIAL")
            else:
                status_badges.append("UNPROTECTED")
            status_badges.append("IN_PROFIT" if (unrealized_pnl or 0) >= 0 else "UNDERWATER")

            tiles.append(
                {
                    "symbol": symbol,
                    "market_type": market_type,
                    "security_type": sec_type,
                    "side": side,
                    "quantity": abs_qty,
                    "entry_price": entry_price,
                    "current_price": current_price,
                    "unrealized_pnl": unrealized_pnl,
                    "opened_at": _iso(opened_at),
                    "position_status_badges": status_badges,
                    "chart": {
                        "interval_minutes": interval_minutes,
                        "bars": symbol_bars,
                    },
                    "events": tile_events,
                    "overlays": {
                        "entry": entry_price,
                        "take_profit": tp_price,
                        "stop_loss": sl_price,
                        "current": current_price,
                    },
                    "expectation": expectation_payload,
                    "thesis": thesis,
                    "progress_metrics": {
                        "distance_to_tp_pct": distance_to_tp_pct,
                        "distance_to_sl_pct": distance_to_sl_pct,
                        "progress_to_tp_pct": progress_to_tp_pct,
                        "current_move_pct": current_move_pct,
                        "expected_move_pct": expected_move_pct,
                        "expected_progress_pct": expected_progress_pct,
                        "r_multiple_open": r_multiple_open,
                    },
                    "holding_context": {
                        "days_since_entry": days_since_entry,
                        "bars_since_entry": bars_since_entry,
                        "best_horizon_reference": f"H{horizon_bars}",
                    },
                    "volatility_context": {
                        "live_volatility": live_volatility,
                        "trained_volatility": trained_volatility,
                        "status": vol_label,
                    },
                }
            )

        tiles.sort(key=lambda t: (t.get("symbol") or ""))

        return {
            "ok": True,
            "mode": mode,
            "chart_style": chart_style,
            "horizon_bars": horizon_bars,
            "projection_mode": projection_mode,
            "window_bars": window_bars,
            "interval_minutes": interval_minutes,
            "tiles": tiles,
            "counts": {"tiles": len(tiles)},
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "disclaimer": "Training-implied range only. Historical context, not a forecast.",
        }
    finally:
        conn.close()
