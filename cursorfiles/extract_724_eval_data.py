#!/usr/bin/env python3
"""Extract 2026-07-24 fresh-day evaluation data to JSON."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "cursorfiles"))
from query_snowflake import _connect  # noqa: E402

ATTEMPTS = {
    "CRM": "f76c5f27-b3dd-436e-8935-31520a2c1b21",
    "MSFT": "88c67578-d99f-4518-b756-ebe7db3a94ac",
    "NKE": "bcf1aaf0-d700-4cc4-9a1b-93d0eadbe135",
    "NVDA": "8549c812-5f3d-494b-8d3c-7dd3d0d8e24f",
}
NKE_BARS = ["10:30", "10:50", "10:55", "11:10", "11:30", "11:40", "14:40", "14:45"]


def _parse_json(v):
    if v is None:
        return None
    if isinstance(v, (dict, list)):
        return v
    if isinstance(v, str):
        try:
            return json.loads(v)
        except json.JSONDecodeError:
            return v
    return v


def main() -> None:
    conn = _connect()
    out: dict = {"attempts": ATTEMPTS, "symbols": {}}
    try:
        cur = conn.cursor()
        for sym, aid in ATTEMPTS.items():
            cur.execute(
                """
                SELECT a.ADVISER_ATTEMPT_ID, a.SYMBOL, a.TRADING_DATE, a.STATUS,
                       a.CONFIG_JSON, a.COST_SUMMARY_JSON
                FROM MIP.APP.BROOKS_INTRADAY_ADVISER_ATTEMPT a
                WHERE a.ADVISER_ATTEMPT_ID = %s
                """,
                (aid,),
            )
            row = cur.fetchone()
            cols = [d[0] for d in cur.description]
            rec = {c: _parse_json(v) for c, v in zip(cols, row)}
            cost = rec.get("COST_SUMMARY_JSON") or {}
            cur.execute(
                """
                SELECT CALL_NUMBER, BAR_TS_ET, ACTION, WAKE_REASON,
                       BROOKS_MARKET_STATE, CURRENT_THESIS,
                       DAILY_INTRADAY_CONTEXT, OBSERVATION_PACKET,
                       WATCH_PREDICATES, INVALIDATION_PREDICATES,
                       CONFIRMATION_PREDICATES, PROTECTIVE_STOP_MANAGEMENT
                FROM MIP.APP.BROOKS_INTRADAY_ADVISER_CALL
                WHERE ADVISER_ATTEMPT_ID = %s ORDER BY CALL_NUMBER
                """,
                (aid,),
            )
            ccols = [d[0] for d in cur.description]
            calls = []
            for r in cur.fetchall():
                c = {k: _parse_json(v) for k, v in zip(ccols, r)}
                calls.append(c)
            sym_out = {
                "attempt_id": aid,
                "status": rec.get("STATUS"),
                "config": rec.get("CONFIG_JSON"),
                "cost_summary_keys": list(cost.keys()) if isinstance(cost, dict) else [],
                "sim_trades": cost.get("sim_trades") or [],
                "entry_opportunities": cost.get("entry_opportunities") or [],
                "lab_entry_blocked": cost.get("lab_entry_blocked") or [],
                "calls_count": len(calls),
                "calls": calls,
            }
            if sym == "NKE":
                sym_out["nke_timeline"] = [
                    next((c for c in calls if c.get("BAR_TS_ET") == b), {"bar": b, "missing": True})
                    for b in NKE_BARS
                ]
            out["symbols"][sym] = sym_out
    finally:
        conn.close()

    path = ROOT / "cursorfiles" / "brooks_724_fresh_day_raw.json"
    path.write_text(json.dumps(out, indent=2, default=str), encoding="utf-8")
    print(f"Wrote {path}")
    for sym, s in out["symbols"].items():
        trades = s.get("sim_trades") or []
        opps = s.get("entry_opportunities") or []
        print(
            f"{sym}: trades={len(trades)} opps={len(opps)} calls={s['calls_count']}"
        )
        for t in trades:
            print(f"  trade {t}")


if __name__ == "__main__":
    main()
