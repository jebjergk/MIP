#!/usr/bin/env python3
"""
Phase H — Real-world structural contract replay (not synthetic-only).

Use this after deploying the structural committee / submit alignment changes.

Known failing case (pre-fix): ACTION_ID 5b2f948d-3b52-4415-b7ea-4f18db26e9c0 (RIVN) persisted
REASON_CODES including LIVE_TP_REQUIRED_MISSING, LIVE_SL_REQUIRED_MISSING, LIVE_BRACKET_REQUIRED
while SETUP_EVENT_ID was set — i.e. structural row hit the IB gate without a persisted bracket contract.

Replay sequence (same API the UI uses; replace BASE and cookies/auth as you expose locally):

1) Committee run (if you need a fresh COMMITTEE_RUN_ID):
   POST {BASE}/live/trades/actions/{ACTION_ID}/committee/run
   Body: {"actor":"committee_orchestrator","model":"claude-4-sonnet","force_rerun":true,...}

2) Committee apply:
   POST {BASE}/live/trades/actions/{ACTION_ID}/committee/apply
   Body: {"actor":"committee_orchestrator","model":"claude-4-sonnet","verdict":{}}

3) Revalidation SSE (structural path — no legacy LLM):
   GET {BASE}/live/trades/actions/{ACTION_ID}/revalidate/live-prompt

4) Submit / execute:
   POST {BASE}/live/trades/actions/{ACTION_ID}/execute
   Body: {"actor":"execution_operator","attempt_n":1}

Success criteria:
- GET .../activity/overview shows structural.structural_execution_contract_v1 with joint_decision TP/SL
  and structural_contract_complete true in diagnostics after apply.
- Execute either returns 200 / broker submit OK, OR 409 with a real market/policy reason —
  not STRUCT_SUBMIT_CONTRACT_INCOMPLETE or LIVE_TP_REQUIRED_MISSING after self-heal.

Optional: discover latest candidate rows in Snowflake:

  SELECT ACTION_ID, SYMBOL, REASON_CODES, SETUP_EVENT_ID, UPDATED_AT
  FROM MIP.LIVE.LIVE_ACTIONS
  WHERE SETUP_EVENT_ID IS NOT NULL
    AND ARRAY_TO_STRING(REASON_CODES, ',') ILIKE '%LIVE_TP_REQUIRED%'
  ORDER BY UPDATED_AT DESC LIMIT 5;
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.request


def main() -> int:
    p = argparse.ArgumentParser(description="Print replay checklist; optionally GET overview for one action.")
    p.add_argument("--base", default=os.getenv("MIP_API_BASE", "http://127.0.0.1:8000"), help="API base URL")
    p.add_argument("--action-id", default=os.getenv("STRUCTURAL_REPLAY_ACTION_ID", "5b2f948d-3b52-4415-b7ea-4f18db26e9c0"))
    p.add_argument("--fetch-overview", action="store_true", help="GET /live/activity/overview and locate action (read-only)")
    args = p.parse_args()
    aid = args.action_id
    base = args.base.rstrip("/")

    print("Structural contract replay — action_id:", aid)
    print("1. POST", f"{base}/live/trades/actions/{aid}/committee/run")
    print("2. POST", f"{base}/live/trades/actions/{aid}/committee/apply")
    print("3. GET ", f"{base}/live/trades/actions/{aid}/revalidate/live-prompt")
    print("4. POST", f"{base}/live/trades/actions/{aid}/execute")

    if not args.fetch_overview:
        return 0

    url = f"{base}/live/activity/overview?limit=200&order_limit=120"
    try:
        with urllib.request.urlopen(url, timeout=60) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as exc:
        print("overview fetch failed:", exc, file=sys.stderr)
        return 1
    pending = data.get("pending_decisions") or []
    for row in pending:
        if str(row.get("action_id")) == aid:
            st = row.get("structural") or {}
            print("\noverview hit:", json.dumps(st, indent=2, default=str)[:4000])
            return 0
    print("action not in pending_decisions slice; widen limit or check portfolio", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
