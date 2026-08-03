"""Insert frozen dossier when absent — never overwrite after run is frozen."""

from __future__ import annotations

import json
import logging
from typing import Any

from app.db import get_connection

logger = logging.getLogger(__name__)


def load_dossiers_for_run(run_id: str) -> list[dict[str, Any]]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT SYMBOL, TRADING_DATE, PAA_ANALYSIS_ID, BOARD_RUN_ID,
                   FROZEN_DOSSIER_JSON, NORMALIZED_STATUS, SOURCE_HASH,
                   DOSSIER_VERSION, COMPILER_VERSION
            FROM MIP.APP.BROOKS_INTRADAY_DOSSIER
            WHERE RUN_ID = %s
            ORDER BY TRADING_DATE, SYMBOL
            """,
            (run_id,),
        )
        cols = [d[0].lower() for d in cur.description]
        rows = []
        for raw in cur.fetchall():
            rec = dict(zip(cols, raw))
            fj = rec.get("frozen_dossier_json")
            if isinstance(fj, str):
                fj = json.loads(fj)
            rec["frozen_dossier_json"] = fj
            rows.append(rec)
        return rows
    finally:
        conn.close()


def get_dossier(run_id: str, symbol: str, trading_date) -> dict[str, Any] | None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT FROZEN_DOSSIER_JSON, PAA_ANALYSIS_ID, NORMALIZED_STATUS,
                   SOURCE_HASH, COMPILER_VERSION, DOSSIER_VERSION, BOARD_RUN_ID
            FROM MIP.APP.BROOKS_INTRADAY_DOSSIER
            WHERE RUN_ID = %s AND SYMBOL = %s AND TRADING_DATE = %s
            """,
            (run_id, symbol.upper(), trading_date),
        )
        row = cur.fetchone()
        if not row:
            return None
        cols = [d[0].lower() for d in cur.description]
        rec = dict(zip(cols, row))
        fj = rec.get("frozen_dossier_json")
        if isinstance(fj, str):
            fj = json.loads(fj)
        rec["frozen_dossier_json"] = fj
        return rec
    finally:
        conn.close()


def insert_dossier_if_absent(
    *,
    run_id: str,
    symbol: str,
    trading_date,
    dossier: dict[str, Any],
    paa_analysis_id: str | None,
    board_run_id: str | None,
    normalized_status: str,
    source_hash: str,
    dossier_version: str,
    compiler_version: str,
    run_frozen: bool,
) -> bool:
    sym = symbol.upper()
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT 1 FROM MIP.APP.BROOKS_INTRADAY_DOSSIER
            WHERE RUN_ID = %s AND SYMBOL = %s AND TRADING_DATE = %s
            """,
            (run_id, sym, trading_date),
        )
        if cur.fetchone():
            return False

        if run_frozen:
            logger.warning(
                "brooks_intraday skip dossier insert — run frozen run_id=%s %s %s",
                run_id,
                sym,
                trading_date,
            )
            return False

        cur.execute(
            """
            INSERT INTO MIP.APP.BROOKS_INTRADAY_DOSSIER (
                RUN_ID, SYMBOL, TRADING_DATE, PAA_ANALYSIS_ID, BOARD_RUN_ID,
                FROZEN_DOSSIER_JSON, SOURCE_HASHES, DOSSIER_VERSION,
                NORMALIZED_STATUS, COMPILER_VERSION, SOURCE_HASH
            )
            SELECT
                %s, %s, %s, %s, %s,
                PARSE_JSON(%s), PARSE_JSON(%s), %s,
                %s, %s, %s
            """,
            (
                run_id,
                sym,
                trading_date,
                paa_analysis_id,
                board_run_id,
                json.dumps(dossier),
                json.dumps({"primary": source_hash}),
                dossier_version,
                normalized_status,
                compiler_version,
                source_hash,
            ),
        )
        conn.commit()
    finally:
        conn.close()
    return True
