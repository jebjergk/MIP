"""Brooks INTRADAY Adviser V1.0 Learning View payload (presentation only)."""

from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any

from app.db import get_connection

from .adviser_baseline_v01 import ADVISER_VERSION, LAB_STARTING_CASH, RAG_CORPUS_VERSION
from .adviser_repository import load_adviser_bars
from .adviser_sim_lab_v01 import summarize_sim_trade_rows
from .historical_bar_repository import load_bars_from_store

V1_VALIDATION_SESSIONS: list[dict[str, str]] = [
    {
        "symbol": "JPM",
        "trading_date": "2026-07-14",
        "adviser_attempt_id": "041d2de0-12ec-49e2-b26f-70870921470d",
        "simulation_attempt_id": "810e6cc5-28ad-459f-811d-6efc504ab70f",
        "run_id": "a669144d-0d59-4431-af3a-958f691334d0",
        "label": "JPM · 2026-07-14 · V1.0 validation",
    },
    {
        "symbol": "MCD",
        "trading_date": "2026-07-14",
        "adviser_attempt_id": "81482bb9-0171-481b-9368-a3b0a4b4e50b",
        "simulation_attempt_id": "758f00ab-2d62-46fb-8746-bd46fb00a178",
        "run_id": "a669144d-0d59-4431-af3a-958f691334d0",
        "label": "MCD · 2026-07-14 · V1.0 validation",
    },
]

V1_LEARNING_DEFAULTS = {
    "run_id": "a669144d-0d59-4431-af3a-958f691334d0",
    "symbol": "MCD",
    "trading_date": "2026-07-14",
    "adviser_attempt_id": "81482bb9-0171-481b-9368-a3b0a4b4e50b",
    "simulation_attempt_id": "758f00ab-2d62-46fb-8746-bd46fb00a178",
}

AMZN_V1_STATUS = (
    "AMZN 2026-07-13 is not stored as a BROOKS_INTRADAY_ADVISER_V1_0 adviser attempt "
    "(only legacy/cert paths exist). It is not exposed in the V1.0 Learning View selector."
)


def _v1_session_rank(rec: dict[str, Any]) -> tuple[Any, ...]:
    """Pick one canonical session per symbol/day (more trades, then calls, then newest)."""
    reviewable = 1 if str(rec.get("adviser_status") or rec.get("status") or "").upper() == "COMPLETED" else 0
    calls = int(rec.get("adviser_call_count") or rec.get("call_count") or 0)
    if reviewable and calls < 1:
        reviewable = 0
    created = rec.get("created_at")
    created_key = created.isoformat() if hasattr(created, "isoformat") else str(created or "")
    return (
        reviewable,
        int(rec.get("trade_count") or 0),
        calls,
        created_key,
    )


def dedupe_v1_validation_sessions(sessions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    best: dict[tuple[str, str], dict[str, Any]] = {}
    for s in sessions:
        sym = str(s.get("symbol") or "").upper()
        td = str(s.get("trading_date") or "")[:10]
        if not sym or not td:
            continue
        key = (sym, td)
        cur = best.get(key)
        if cur is None or _v1_session_rank(s) > _v1_session_rank(cur):
            best[key] = s
    return sorted(best.values(), key=lambda x: (str(x.get("trading_date") or ""), str(x.get("symbol") or "")))


def find_v1_session_for_symbol_date(symbol: str, trading_date: date | str) -> dict[str, Any] | None:
    sym = str(symbol or "").upper()
    td = str(trading_date)[:10]
    if not sym or not td:
        return None
    try:
        loaded = dedupe_v1_validation_sessions(_load_reviewable_v1_sessions_from_db())
    except Exception:
        loaded = []
    for s in loaded:
        if s.get("symbol") == sym and str(s.get("trading_date") or "")[:10] == td:
            return s
    return None


def _format_session_selector_label(
    *,
    symbol: str,
    trading_date: str,
    adviser_status: str,
    trade_count: int,
    realized_pnl: float | None,
) -> str:
    st = "COMPLETE" if str(adviser_status or "").upper() == "COMPLETED" else str(adviser_status or "—").upper()
    base = f"{symbol} · {trading_date} · {st} · {int(trade_count)} trade{'s' if trade_count != 1 else ''}"
    if trade_count > 0 and realized_pnl is not None:
        sign = "+" if float(realized_pnl) >= 0 else "-"
        base += f" · {sign}${abs(float(realized_pnl)):.2f}"
    return base


def _load_reviewable_v1_sessions_from_db() -> list[dict[str, Any]]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
              a.ADVISER_ATTEMPT_ID,
              a.RUN_ID,
              a.SYMBOL,
              a.TRADING_DATE,
              a.STATUS,
              a.CREATED_AT,
              s.SIMULATION_ATTEMPT_ID,
              s.TRADE_COUNT,
              s.REALIZED_PNL,
              (
                SELECT COUNT(*)
                FROM MIP.APP.BROOKS_INTRADAY_ADVISER_CALL c
                WHERE c.ADVISER_ATTEMPT_ID = a.ADVISER_ATTEMPT_ID
              ) AS CALL_COUNT
            FROM MIP.APP.BROOKS_INTRADAY_ADVISER_ATTEMPT a
            LEFT JOIN MIP.APP.BROOKS_INTRADAY_SIMULATION_ATTEMPT s
              ON s.CONTEXT_ATTEMPT_ID = a.ADVISER_ATTEMPT_ID
            WHERE a.CONFIG_JSON:adviser_version::STRING = %s
              AND a.STATUS = 'COMPLETED'
            ORDER BY a.TRADING_DATE, a.SYMBOL
            """,
            (ADVISER_VERSION,),
        )
        cols = [d[0].lower() for d in cur.description]
        rows: list[dict[str, Any]] = []
        for raw in cur.fetchall():
            rec = dict(zip(cols, raw))
            call_count = int(rec.get("call_count") or 0)
            if call_count < 1:
                continue
            aid = str(rec["adviser_attempt_id"])
            trade_count = rec.get("trade_count")
            if trade_count is not None:
                trade_count = int(trade_count)
            else:
                cur.execute(
                    """
                    SELECT COUNT(*), COALESCE(SUM(REALIZED_PNL), 0)
                    FROM MIP.APP.BROOKS_INTRADAY_ADVISER_SIM_TRADE
                    WHERE ADVISER_ATTEMPT_ID = %s
                    """,
                    (aid,),
                )
                tc_row = cur.fetchone()
                trade_count = int(tc_row[0] or 0) if tc_row else 0
                if rec.get("realized_pnl") is None and tc_row:
                    rec["realized_pnl"] = float(tc_row[1] or 0)
            pnl = rec.get("realized_pnl")
            if pnl is None and trade_count > 0:
                cur.execute(
                    """
                    SELECT COALESCE(SUM(REALIZED_PNL), 0)
                    FROM MIP.APP.BROOKS_INTRADAY_ADVISER_SIM_TRADE
                    WHERE ADVISER_ATTEMPT_ID = %s
                    """,
                    (aid,),
                )
                pnl_row = cur.fetchone()
                if pnl_row:
                    pnl = float(pnl_row[0] or 0)
            elif pnl is not None:
                pnl = float(pnl)
            sym = str(rec.get("symbol") or "").upper()
            td = str(rec.get("trading_date") or "")[:10]
            rows.append(
                {
                    "symbol": sym,
                    "trading_date": td,
                    "adviser_attempt_id": aid,
                    "simulation_attempt_id": str(rec.get("simulation_attempt_id") or ""),
                    "run_id": str(rec.get("run_id") or ""),
                    "adviser_status": str(rec.get("status") or ""),
                    "created_at": rec.get("created_at"),
                    "trade_count": trade_count,
                    "realized_pnl": pnl,
                    "adviser_call_count": call_count,
                    "reviewable": True,
                    "canonical_v1": True,
                    "label": _format_session_selector_label(
                        symbol=sym,
                        trading_date=td,
                        adviser_status=str(rec.get("status") or ""),
                        trade_count=trade_count,
                        realized_pnl=pnl if trade_count > 0 else None,
                    ),
                    "selector_label": _format_session_selector_label(
                        symbol=sym,
                        trading_date=td,
                        adviser_status=str(rec.get("status") or ""),
                        trade_count=trade_count,
                        realized_pnl=pnl if trade_count > 0 else None,
                    ),
                }
            )
        return dedupe_v1_validation_sessions(rows)
    finally:
        conn.close()


def list_v1_validation_sessions(*, hydrate_from_db: bool = True) -> list[dict[str, Any]]:
    if hydrate_from_db:
        try:
            return _load_reviewable_v1_sessions_from_db()
        except Exception:
            return []
    out: list[dict[str, Any]] = []
    for s in V1_VALIDATION_SESSIONS:
        rec = dict(s)
        rec.setdefault("adviser_status", "COMPLETED")
        rec.setdefault("trade_count", 0)
        rec.setdefault("realized_pnl", None)
        rec.setdefault("reviewable", True)
        rec.setdefault("canonical_v1", True)
        label = _format_session_selector_label(
            symbol=rec["symbol"],
            trading_date=rec["trading_date"],
            adviser_status=rec.get("adviser_status", "COMPLETED"),
            trade_count=int(rec.get("trade_count") or 0),
            realized_pnl=rec.get("realized_pnl"),
        )
        rec["label"] = label
        rec["selector_label"] = label
        out.append(rec)
    return out


def _normalize_completed_trades(raw_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """One row per trade_id (round trip), not per entry/exit event row."""
    by_id: dict[str, dict[str, Any]] = {}
    for r in raw_rows:
        tid = str(r.get("trade_id") or "")
        if not tid:
            continue
        rec = by_id.setdefault(tid, {"trade_id": tid})
        exit_ts = r.get("exit_ts_ny")
        pnl = r.get("realized_pnl")
        if exit_ts is not None and pnl is not None:
            if "entry_ts" not in rec and r.get("entry_ts_ny"):
                rec["entry_ts"] = _ts_key(r.get("entry_ts_ny"))
            rec.update(
                {
                    "exit_ts": _ts_key(exit_ts),
                    "exit_price": float(r.get("exit_price") or 0),
                    "quantity": int(r.get("quantity") or rec.get("quantity") or 0),
                    "exit_reason": r.get("exit_reason"),
                    "realized_pnl": float(pnl),
                }
            )
            if r.get("entry_price") is not None and "entry_price" not in rec:
                rec["entry_price"] = float(r.get("entry_price"))
        elif r.get("entry_ts_ny") and "entry_ts" not in rec:
            rec["entry_ts"] = _ts_key(r.get("entry_ts_ny"))
            rec.setdefault("entry_price", float(r.get("entry_price") or 0))
            rec.setdefault("quantity", int(r.get("quantity") or 0))
            if r.get("stop_price") is not None:
                rec["stop_price"] = float(r["stop_price"])
    out = [v for v in by_id.values() if v.get("exit_ts")]
    out.sort(key=lambda x: str(x.get("entry_ts") or ""))
    for i, t in enumerate(out, 1):
        t["trade_number"] = i
    return out


def _session_trade_metrics(
    raw_rows: list[dict[str, Any]], sim_att: dict[str, Any] | None
) -> tuple[int, float, float, float]:
    summary = summarize_sim_trade_rows(raw_rows)
    completed = _normalize_completed_trades(raw_rows)
    trade_count = int(sim_att.get("trade_count")) if sim_att and sim_att.get("trade_count") is not None else len(completed)
    if trade_count < len(completed):
        trade_count = len(completed)
    realized = summary["realized_pnl"]
    if sim_att and sim_att.get("realized_pnl") is not None:
        realized = float(sim_att["realized_pnl"])
    starting = float(sim_att.get("starting_cash") or LAB_STARTING_CASH) if sim_att else LAB_STARTING_CASH
    ending = float(sim_att.get("ending_cash")) if sim_att and sim_att.get("ending_cash") is not None else starting + realized
    return trade_count, float(realized), starting, ending


def _format_trade_et(ts: Any) -> str:
    if ts is None:
        return "—"
    s = str(ts).replace(" ", "T")
    return s[11:16] if len(s) >= 16 else s


def _enrich_trade_display(trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for t in trades:
        out.append(
            {
                **t,
                "entry_time_et": _format_trade_et(t.get("entry_ts")),
                "exit_time_et": _format_trade_et(t.get("exit_ts")),
            }
        )
    return out


def _parse_json(v: Any) -> Any:
    if v is None or isinstance(v, (dict, list)):
        return v
    if isinstance(v, str):
        try:
            return json.loads(v)
        except json.JSONDecodeError:
            return v
    return v


def _ts_key(ts: Any) -> str:
    if isinstance(ts, datetime):
        return ts.strftime("%Y-%m-%dT%H:%M:%S")
    return str(ts).replace(" ", "T")[:19]


def load_adviser_attempt_meta(adviser_attempt_id: str) -> dict[str, Any] | None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT ADVISER_ATTEMPT_ID, RUN_ID, SYMBOL, TRADING_DATE, STATUS,
                   CORPUS_VERSION, QUERY_TAG, CONFIG_JSON, COST_SUMMARY_JSON
            FROM MIP.APP.BROOKS_INTRADAY_ADVISER_ATTEMPT
            WHERE ADVISER_ATTEMPT_ID = %s
            """,
            (adviser_attempt_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        cols = [d[0].lower() for d in cur.description]
        rec = dict(zip(cols, row))
        rec["config_json"] = _parse_json(rec.get("config_json"))
        rec["cost_summary_json"] = _parse_json(rec.get("cost_summary_json"))
        return rec
    finally:
        conn.close()


def _load_calls_full(adviser_attempt_id: str) -> list[dict[str, Any]]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT CALL_ID, CALL_NUMBER, BAR_TS_NY, BAR_TS_ET, WAKE_REASON, POSITION_STATE,
                   RETRIEVAL_QUERY, RAG_CARD_IDS, RETRIEVED_CONCEPTS, CURRENT_THESIS,
                   BROOKS_MARKET_STATE, ACTION, WATCH_CONDITIONS, CONFIRMATION_CONDITIONS,
                   INVALIDATION_CONDITIONS, WATCH_PREDICATES, INVALIDATION_PREDICATES,
                   CONFIRMATION_PREDICATES, DAILY_INTRADAY_CONTEXT, BROOKS_REASONING_SUMMARY,
                   OBSERVATION_PACKET, RAG_LATENCY_MS, LLM_LATENCY_MS, TOTAL_LATENCY_MS
            FROM MIP.APP.BROOKS_INTRADAY_ADVISER_CALL
            WHERE ADVISER_ATTEMPT_ID = %s
            ORDER BY CALL_NUMBER
            """,
            (adviser_attempt_id,),
        )
        cols = [d[0].lower() for d in cur.description]
        rows = []
        for raw in cur.fetchall():
            rec = dict(zip(cols, raw))
            for k in (
                "rag_card_ids",
                "retrieved_concepts",
                "watch_conditions",
                "confirmation_conditions",
                "invalidation_conditions",
                "watch_predicates",
                "invalidation_predicates",
                "confirmation_predicates",
                "daily_intraday_context",
                "observation_packet",
            ):
                rec[k] = _parse_json(rec.get(k))
            rows.append(rec)
        return rows
    finally:
        conn.close()


def _load_sim_trades(adviser_attempt_id: str) -> list[dict[str, Any]]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT TRADE_ID, ENTRY_TS_NY, EXIT_TS_NY, ENTRY_PRICE, EXIT_PRICE,
                   QUANTITY, STOP_PRICE, EXIT_REASON, REALIZED_PNL
            FROM MIP.APP.BROOKS_INTRADAY_ADVISER_SIM_TRADE
            WHERE ADVISER_ATTEMPT_ID = %s
            ORDER BY ENTRY_TS_NY
            """,
            (adviser_attempt_id,),
        )
        cols = [d[0].lower() for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        conn.close()


def _load_sim_attempt(simulation_attempt_id: str) -> dict[str, Any] | None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT SIMULATION_ATTEMPT_ID, STARTING_CASH, ENDING_CASH, TRADE_COUNT, REALIZED_PNL
            FROM MIP.APP.BROOKS_INTRADAY_SIMULATION_ATTEMPT
            WHERE SIMULATION_ATTEMPT_ID = %s
            """,
            (simulation_attempt_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        cols = [d[0].lower() for d in cur.description]
        return dict(zip(cols, row))
    finally:
        conn.close()


def _card_meta(card_ids: list[str]) -> dict[str, dict[str, Any]]:
    if not card_ids:
        return {}
    conn = get_connection()
    try:
        cur = conn.cursor()
        ph = ",".join(["%s"] * len(card_ids))
        try:
            cur.execute(
                f"""
            SELECT CARD_ID,
                   RAW_CARD:concept_name::STRING,
                   RAW_CARD:execution_relevance::STRING,
                   RAW_CARD:adviser_class::STRING,
                   DISPLAY_TEXT
            FROM MIP.KNOWLEDGE.LITERATURE_RAG_DOCUMENT
            WHERE RAG_SCOPE = 'INTRADAY_ADVISER' AND CARD_ID IN ({ph})
            """,
                card_ids,
            )
        except Exception:
            return {}
        out = {}
        for cid, name, rel, cls, disp in cur.fetchall():
            out[str(cid)] = {
                "card_id": str(cid),
                "concept_name": name,
                "execution_relevance": rel,
                "adviser_class": cls,
                "display_text": disp,
            }
        return out
    finally:
        conn.close()


def _setup_state_label(ctx: dict[str, Any] | None) -> str:
    if not ctx:
        return "—"
    sc = ctx.get("setup_contract") or {}
    if not sc.get("setup_id"):
        return "—"
    armed = sc.get("armed")
    parts = [str(sc.get("setup_id")[:12])]
    if armed:
        parts.append("armed")
    else:
        parts.append("idle")
    return " · ".join(parts)


def _confirmation_state_label(ctx: dict[str, Any] | None) -> str:
    if not ctx:
        return "—"
    sc = ctx.get("setup_contract") or {}
    mand = sc.get("mandatory") or []
    if not mand:
        return "—"
    bits = []
    for m in mand:
        pred = m.get("predicate") or {}
        ptype = pred.get("predicate_type") or pred.get("type") or "?"
        bits.append(f"{ptype}:{m.get('status', '?')}")
    return "; ".join(bits) if bits else "—"


def _invalidation_state_label(inv_preds: list[Any] | None, inv_text: list[Any] | None) -> str:
    preds = inv_preds or []
    if preds:
        bits = []
        for p in preds:
            if isinstance(p, dict):
                bits.append(f"{p.get('type', '?')}@{p.get('level', '')}")
        return ", ".join(bits) if bits else "—"
    if inv_text:
        return str(inv_text[0])[:80]
    return "—"


def _action_markers(action: str | None) -> list[dict[str, str]]:
    a = (action or "").upper()
    mapping = {
        "WATCH_LONG": ("watch_long", "WATCH_LONG"),
        "ARM_LONG": ("arm_long", "ARM_LONG"),
        "CONSIDER_ENTRY": ("consider_entry", "CONSIDER_ENTRY"),
        "HOLD": ("hold", "HOLD"),
        "EXIT": ("exit", "EXIT"),
        "INVALIDATE": ("invalidate", "INVALIDATE"),
    }
    if a in mapping:
        k, lab = mapping[a]
        return [{"kind": k, "label": lab}]
    return []


def _level_near_session(price: float, session_lo: float, session_hi: float, *, pad_pct: float = 0.15) -> bool:
    span = max(float(session_hi) - float(session_lo), 0.01)
    pad = span * pad_pct
    return float(session_lo) - pad <= float(price) <= float(session_hi) + pad


def _levels_from_call(
    call: dict[str, Any] | None,
    *,
    session_lo: float | None = None,
    session_hi: float | None = None,
) -> tuple[list[dict], list[dict]]:
    conf: list[dict] = []
    inv: list[dict] = []
    if not call:
        return conf, inv
    ctx = call.get("daily_intraday_context") or {}
    sc = ctx.get("setup_contract") or {}
    for m in sc.get("mandatory") or []:
        pred = m.get("predicate") or {}
        lvl = pred.get("level")
        if lvl is not None:
            price = float(lvl)
            if session_lo is not None and session_hi is not None and not _level_near_session(
                price, session_lo, session_hi
            ):
                continue
            conf.append(
                {
                    "level": price,
                    "label": f"{pred.get('predicate_type') or pred.get('type')} ({m.get('status')})",
                }
            )
    for p in call.get("invalidation_predicates") or []:
        if isinstance(p, dict) and p.get("level") is not None:
            price = float(p["level"])
            if session_lo is not None and session_hi is not None and not _level_near_session(
                price, session_lo, session_hi
            ):
                continue
            inv.append(
                {
                    "level": price,
                    "label": str(p.get("type") or "invalidation"),
                }
            )
    return conf, inv


def build_adviser_v1_learning_payload(
    *,
    adviser_attempt_id: str,
    simulation_attempt_id: str | None = None,
    symbol: str | None = None,
    trading_date: date | str | None = None,
) -> dict[str, Any]:
    attempt = load_adviser_attempt_meta(adviser_attempt_id)
    if not attempt:
        raise ValueError(f"Adviser attempt not found: {adviser_attempt_id}")
    cfg = attempt.get("config_json") or {}
    if str(cfg.get("adviser_version") or "") != ADVISER_VERSION:
        raise ValueError("Not a BROOKS_INTRADAY_ADVISER_V1_0 attempt")

    sym = str(attempt.get("symbol") or "").upper()
    td_raw = attempt.get("trading_date")
    if isinstance(td_raw, str):
        td = date.fromisoformat(td_raw[:10])
    elif isinstance(td_raw, date):
        td = td_raw
    else:
        td = date.fromisoformat(str(trading_date)[:10]) if trading_date else date.today()
    req_sym = str(symbol or "").upper()
    if req_sym and req_sym != sym:
        # Query params must not override attempt identity (prevents mixed bar/call payloads).
        pass
    sim_id = simulation_attempt_id
    calls = _load_calls_full(adviser_attempt_id)
    calls_by_et = {str(c.get("bar_ts_et")): c for c in calls}
    calls_by_ts = {_ts_key(c.get("bar_ts_ny")): c for c in calls}

    adviser_bars = load_adviser_bars(adviser_attempt_id)
    bar_snap = {_ts_key(b.get("bar_ts_ny")): b for b in adviser_bars}

    rth_bars = [b for b in load_bars_from_store(sym, td) if b.rth]
    session_lo = min(b.low for b in rth_bars) if rth_bars else None
    session_hi = max(b.high for b in rth_bars) if rth_bars else None
    trades_raw = _load_sim_trades(adviser_attempt_id)
    sim_att = _load_sim_attempt(sim_id) if sim_id else None
    completed_trades = _enrich_trade_display(_normalize_completed_trades(trades_raw))
    trade_count, realized_pnl, starting, ending = _session_trade_metrics(trades_raw, sim_att)

    entry_by_ts: dict[str, dict] = {}
    exit_by_ts: dict[str, dict] = {}
    for t in completed_trades:
        ek = _ts_key(t.get("entry_ts"))
        entry_by_ts[ek] = t
        if t.get("exit_ts"):
            exit_by_ts[_ts_key(t.get("exit_ts"))] = t

    all_card_ids: list[str] = []
    for c in calls:
        all_card_ids.extend(str(x) for x in (c.get("rag_card_ids") or []))
    card_lookup = _card_meta(list(dict.fromkeys(all_card_ids)))

    last_call: dict[str, Any] | None = None
    last_regime: dict[str, Any] = {}
    last_context: dict[str, Any] = {}

    grid_rows: list[dict[str, Any]] = []
    chart_bars: list[dict[str, Any]] = []

    for b in rth_bars:
        ts_key = _ts_key(b.ts_ny)
        et = b.ts_ny.strftime("%H:%M") if hasattr(b.ts_ny, "strftime") else str(b.ts_ny)[11:16]
        ab = bar_snap.get(ts_key, {})
        call = calls_by_ts.get(ts_key) or calls_by_et.get(et)
        if call:
            last_call = call
        ctx = (call or last_call or {}).get("daily_intraday_context") if (call or last_call) else {}
        if call:
            last_context = ctx or {}
        elif last_context:
            ctx = last_context
        pkt = (call or {}).get("observation_packet") or {}
        regime = pkt.get("intraday_regime") or (ctx or {}).get("intraday_regime") or {}
        if regime:
            last_regime = regime
        elif last_regime:
            regime = last_regime

        sc = (ctx or {}).get("setup_contract") or {}
        action = (call or {}).get("action") or ab.get("action_snapshot")
        pos_qty = int(ab.get("sim_position_qty") or 0)
        pos_state = "IN_TRADE" if pos_qty > 0 else "FLAT"
        if call and call.get("position_state"):
            pos_state = str(call.get("position_state"))

        conf_lvls, inv_lvls = _levels_from_call(
            call or last_call,
            session_lo=session_lo,
            session_hi=session_hi,
        )

        markers = _action_markers(action if call else None)
        if ts_key in entry_by_ts:
            markers.append({"kind": "sim_entry", "label": "Sim entry"})
        if ts_key in exit_by_ts:
            markers.append({"kind": "sim_exit", "label": "Sim exit"})

        stop_px = None
        if pos_qty > 0:
            for ct in completed_trades:
                ent = _ts_key(ct.get("entry_ts"))
                ext = _ts_key(ct.get("exit_ts")) if ct.get("exit_ts") else None
                if ext and ent <= ts_key <= ext and ct.get("stop_price") is not None:
                    stop_px = float(ct["stop_price"])
                    break
        if (
            stop_px is not None
            and session_lo is not None
            and session_hi is not None
            and not _level_near_session(stop_px, session_lo, session_hi, pad_pct=0.25)
        ):
            stop_px = None

        short_expl = (ab.get("bar_note") or "") if not call else (call.get("wake_reason") or "")
        if call:
            short_expl = f"{call.get('wake_reason')}: {(call.get('brooks_reasoning_summary') or '')[:160]}"

        grid_rows.append(
            {
                "bar_ts": ts_key,
                "bar_ts_ny": str(b.ts_ny),
                "time_et": et,
                "ohlc": {"o": b.open, "h": b.high, "l": b.low, "c": b.close},
                "regime": regime.get("regime"),
                "always_in_bias": regime.get("always_in_bias"),
                "wake_reason": call.get("wake_reason") if call else None,
                "adviser_called": bool(call),
                "adviser_action": action,
                "setup_state": _setup_state_label(ctx),
                "confirmation_state": _confirmation_state_label(ctx),
                "invalidation_state": _invalidation_state_label(
                    (call or last_call or {}).get("invalidation_predicates"),
                    (call or last_call or {}).get("invalidation_conditions"),
                ),
                "position_state": pos_state,
                "short_explanation": short_expl,
                "brooks_market_state": (call or {}).get("brooks_market_state"),
                "current_thesis": (call or {}).get("current_thesis") or ab.get("thesis_snapshot"),
            }
        )

        chart_bars.append(
            {
                "ts_utc": ts_key,
                "ts_ny": str(b.ts_ny),
                "open": b.open,
                "high": b.high,
                "low": b.low,
                "close": b.close,
                "volume": b.volume,
                "active_stop": stop_px,
                "position_quantity": pos_qty,
                "markers": markers,
                "confirmation_levels": conf_lvls,
                "invalidation_levels": inv_lvls,
            }
        )

    cost = attempt.get("cost_summary_json") or {}
    arm = sum(1 for c in calls if c.get("action") == "ARM_LONG")
    consider = sum(1 for c in calls if c.get("action") == "CONSIDER_ENTRY")

    evidence_by_call: list[dict[str, Any]] = []
    for c in calls:
        concepts_raw = c.get("retrieved_concepts") or []
        if isinstance(concepts_raw, str):
            concepts_raw = _parse_json(concepts_raw) or []
        concepts_list = list(concepts_raw) if isinstance(concepts_raw, list) else []
        cards = []
        for i, cid in enumerate(c.get("rag_card_ids") or []):
            meta = dict(card_lookup.get(str(cid), {"card_id": str(cid)}))
            if not meta.get("concept_name") and i < len(concepts_list):
                meta["concept_name"] = concepts_list[i]
            cards.append(
                {
                    **meta,
                }
            )
        ctx = c.get("daily_intraday_context") or {}
        sc = ctx.get("setup_contract") or {}
        pkt = c.get("observation_packet") or {}
        regime = pkt.get("intraday_regime") or ctx.get("intraday_regime") or {}
        evidence_by_call.append(
            {
                "call_number": c.get("call_number"),
                "bar_ts_et": c.get("bar_ts_et"),
                "bar_ts": _ts_key(c.get("bar_ts_ny")),
                "wake_reason": c.get("wake_reason"),
                "action": c.get("action"),
                "retrieval_query": c.get("retrieval_query"),
                "brooks_reasoning_summary": c.get("brooks_reasoning_summary"),
                "brooks_market_state": c.get("brooks_market_state"),
                "current_thesis": c.get("current_thesis"),
                "intraday_regime": regime,
                "setup_id": sc.get("setup_id"),
                "setup_family": sc.get("setup_family"),
                "cards": cards,
            }
        )

    final_qty = int(adviser_bars[-1].get("sim_position_qty") or 0) if adviser_bars else 0

    return {
        "phase": "ADVISER_V1_LEARNING",
        "adviser_version": ADVISER_VERSION,
        "rag_corpus_version": attempt.get("corpus_version") or RAG_CORPUS_VERSION,
        "run_id": attempt.get("run_id"),
        "adviser_attempt_id": adviser_attempt_id,
        "simulation_attempt_id": sim_id,
        "symbol": sym,
        "trading_date": str(td),
        "v1_validation_sessions": list_v1_validation_sessions(),
        "header": {
            "title": f"{sym} · {td.isoformat()} · Adviser V1.0",
            "subtitle": "Canonical validation session — observation only",
        },
        "session_summary": {
            "adviser_calls": len(calls),
            "arm_long_count": arm,
            "consider_entry_count": consider,
            "trades": trade_count,
            "realized_pnl": realized_pnl,
            "rag_calls": cost.get("rag_calls") or len(calls),
            "llm_calls": cost.get("llm_calls") or len(calls),
            "approx_session_credits": cost.get("delta_cloud_credits_approx"),
            "starting_cash": starting,
            "ending_cash": ending,
            "final_position_qty": final_qty,
        },
        "completed_trades": completed_trades,
        "adviser_timeline": [
            {
                "bar_ts": e.get("bar_ts"),
                "bar_ts_et": e.get("bar_ts_et"),
                "wake_reason": e.get("wake_reason"),
                "action": e.get("action"),
                "call_number": e.get("call_number"),
            }
            for e in evidence_by_call
        ],
        "chart": {
            "bars": chart_bars,
            "bar_count": len(chart_bars),
            "expected_rth_bars": 78,
            "overlays": {
                "stop_steps": [
                    {
                        "from_ts": _ts_key(t.get("entry_ts")),
                        "until_ts": _ts_key(t.get("exit_ts")) if t.get("exit_ts") else None,
                        "stop_price": t.get("stop_price"),
                    }
                    for t in completed_trades
                    if t.get("stop_price") is not None
                ],
            },
        },
        "observation_grid": grid_rows,
        "adviser_evidence_by_call": evidence_by_call,
        "adviser_v1_learning": True,
        "technical_details": {
            "adviser_attempt_id": adviser_attempt_id,
            "simulation_attempt_id": sim_id,
            "query_tag": cfg.get("query_tag"),
            "cost_summary": cost,
        },
    }
