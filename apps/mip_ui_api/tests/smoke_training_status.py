"""
Smoke test: run Training Status v1 SQL when Snowflake env vars exist.
No Snowflake env → skip with exit 0. Run from repo root or MIP/apps/mip_ui_api:
  python -m tests.smoke_training_status
  cd MIP/apps/mip_ui_api && python -m tests.smoke_training_status
"""
import os
import sys
from pathlib import Path

# Ensure app is on path
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

REQUIRED_ENV = ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD")


def has_snowflake_env() -> bool:
    return all(os.getenv(k) for k in REQUIRED_ENV)


def main() -> int:
    if not has_snowflake_env():
        print("SKIP: Snowflake env not set (need SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD)")
        return 0
    try:
        from app.db import get_connection, fetch_all
        from app.routers.training import TRAINING_STATUS_SQL, _get_min_signals
        from app.training_status import apply_scoring_to_rows
        from app.db import serialize_rows
    except ImportError as e:
        print(f"Import error: {e}", file=sys.stderr)
        return 1
    try:
        conn = get_connection()
        min_signals = _get_min_signals(conn)
        cur = conn.cursor()
        cur.execute(TRAINING_STATUS_SQL)
        rows = fetch_all(cur)
        conn.close()
        scored = apply_scoring_to_rows(rows, min_signals=min_signals)
        out = serialize_rows(scored)
        print(f"OK: {len(out)} rows")
        for i, r in enumerate(out[:3]):
            print(f"  [{i}] {r.get('market_type')} {r.get('symbol')} pattern_id={r.get('pattern_id')} "
                  f"score={r.get('maturity_score')} stage={r.get('maturity_stage')}")
        if len(out) > 3:
            print(f"  ... and {len(out) - 3} more")
        return 0
    except Exception as e:
        print(f"FAIL: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
