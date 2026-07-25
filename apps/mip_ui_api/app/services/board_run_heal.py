"""Auto-finalize board runs stuck at CHAIR_DONE after proposals were published."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def heal_stale_chair_done_board_runs(conn, *, max_age_minutes: int = 3) -> int:
    """Mark CHAIR_DONE runs COMPLETE when PROPOSED rows exist but finalize never ran.

    Uses SP_HEAL_STALE_CHAIR_DONE_BOARD_RUNS (EXECUTE AS OWNER) so MIP_UI_API_ROLE
    can heal without direct UPDATE on PROPOSAL_BOARD_RUN. Never raises — import must
    continue even when heal is unavailable.
    """
    cur = conn.cursor()
    try:
        cur.execute(
            "CALL MIP.APP.SP_HEAL_STALE_CHAIR_DONE_BOARD_RUNS(%s)",
            (int(max_age_minutes),),
        )
        row = cur.fetchone()
        if row is None:
            return 0
        # Snowflake CALL returns a single-column row (healed count).
        val = row[0] if isinstance(row, (list, tuple)) else row
        return int(val or 0)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "heal_stale_chair_done_board_runs failed (non-fatal): %s",
            exc,
        )
        return 0
    finally:
        try:
            cur.close()
        except Exception:  # noqa: BLE001
            pass
