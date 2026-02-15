"""
Parallel Worlds endpoints: GET /parallel-worlds/...
Counterfactual simulation results, diffs, regret, narratives.
Read-only; never writes to Snowflake.
"""
import json

from fastapi import APIRouter, Query, HTTPException

from app.db import get_connection, serialize_row, serialize_rows


router = APIRouter(prefix="/parallel-worlds", tags=["parallel-worlds"])


def _fetch_all(cursor):
    """Like db.fetch_all but lowercases column names for consistent key access."""
    columns = [d[0].lower() for d in cursor.description]
    rows = cursor.fetchall()
    return [dict(zip(columns, row)) for row in rows]


def _parse_json(value):
    """Parse JSON value — handles string, dict, or None."""
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return {}
    return {}


def _clean_narrative_json(raw):
    """Strip markdown fences from Cortex output if present."""
    if raw is None:
        return {}
    if isinstance(raw, dict):
        # Check if it's a wrapper with raw_text key
        raw_text = raw.get("raw_text", "")
        if raw_text:
            # Strip markdown fences
            text = raw_text.strip()
            if text.startswith("```"):
                lines = text.split("\n")
                # Remove first and last lines (fences)
                if len(lines) > 2:
                    text = "\n".join(lines[1:-1]).strip()
                    if text.startswith("```"):
                        text = text[text.index("\n") + 1:].strip()
                    if text.endswith("```"):
                        text = text[: text.rfind("```")].strip()
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                return raw
        return raw
    return _parse_json(raw)


# ──────────────────────────────────────────────────────────────
# GET /parallel-worlds/scenarios
# ──────────────────────────────────────────────────────────────
@router.get("/scenarios")
def get_scenarios():
    """List all active parallel-world scenarios."""
    sql = """
    SELECT SCENARIO_ID, NAME, DESCRIPTION, SCENARIO_TYPE, PARAMS_JSON, IS_ACTIVE
    FROM MIP.APP.PARALLEL_WORLD_SCENARIO
    WHERE IS_ACTIVE = true
    ORDER BY SCENARIO_ID
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = _fetch_all(cur)
        return {
            "scenarios": serialize_rows(rows),
            "count": len(rows),
        }
    finally:
        conn.close()


# ──────────────────────────────────────────────────────────────
# GET /parallel-worlds/results
# ──────────────────────────────────────────────────────────────
@router.get("/results")
def get_results(
    portfolio_id: int = Query(..., description="Portfolio ID"),
    as_of_ts: str = Query(None, description="Date filter (YYYY-MM-DD)"),
):
    """
    Latest parallel-worlds results + diffs for a portfolio-day.
    If as_of_ts is not given, returns the most recent day.
    """
    conditions = ["d.PORTFOLIO_ID = %s"]
    params = [portfolio_id]

    if as_of_ts:
        conditions.append("d.AS_OF_TS::date = %s::date")
        params.append(as_of_ts)

    where = " AND ".join(conditions)

    sql = f"""
    SELECT
        d.RUN_ID,
        d.PORTFOLIO_ID,
        d.AS_OF_TS,
        d.SCENARIO_ID,
        d.SCENARIO_NAME,
        d.SCENARIO_DISPLAY_NAME,
        d.SCENARIO_TYPE,
        d.ACTUAL_PNL,
        d.ACTUAL_RETURN_PCT,
        d.ACTUAL_EQUITY,
        d.ACTUAL_TRADES,
        d.ACTUAL_POSITIONS,
        d.CF_PNL,
        d.CF_RETURN_PCT,
        d.CF_EQUITY,
        d.CF_TRADES,
        d.CF_POSITIONS,
        d.PNL_DELTA,
        d.RETURN_PCT_DELTA,
        d.EQUITY_DELTA,
        d.TRADES_DELTA,
        d.OUTPERFORMED,
        d.RISK_STATUS,
        d.ENTRIES_BLOCKED,
        d.CAPACITY_STATUS,
        d.CF_RESULT_JSON
    FROM MIP.MART.V_PARALLEL_WORLD_DIFF d
    WHERE {where}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY d.PORTFOLIO_ID, d.SCENARIO_ID
        ORDER BY d.AS_OF_TS DESC
    ) = 1
    ORDER BY d.PNL_DELTA DESC
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, tuple(params))
        rows = _fetch_all(cur)
        if not rows:
            return {"found": False, "message": "No parallel-worlds results found.", "scenarios": []}

        # Extract actual metrics from first row
        first = serialize_row(rows[0])
        actual = {
            "pnl": first.get("actual_pnl"),
            "return_pct": first.get("actual_return_pct"),
            "equity": first.get("actual_equity"),
            "trades": first.get("actual_trades"),
            "positions": first.get("actual_positions"),
            "risk_status": first.get("risk_status"),
            "entries_blocked": first.get("entries_blocked"),
            "capacity_status": first.get("capacity_status"),
        }

        scenarios = []
        for row in rows:
            r = serialize_row(row)
            cf_json = _parse_json(r.get("cf_result_json"))
            scenarios.append({
                "scenario_id": r.get("scenario_id"),
                "scenario_name": r.get("scenario_name"),
                "display_name": r.get("scenario_display_name") or r.get("scenario_name"),
                "scenario_type": r.get("scenario_type"),
                "cf_pnl": r.get("cf_pnl"),
                "cf_return_pct": r.get("cf_return_pct"),
                "cf_equity": r.get("cf_equity"),
                "cf_trades": r.get("cf_trades"),
                "cf_positions": r.get("cf_positions"),
                "pnl_delta": r.get("pnl_delta"),
                "return_pct_delta": r.get("return_pct_delta"),
                "equity_delta": r.get("equity_delta"),
                "trades_delta": r.get("trades_delta"),
                "outperformed": r.get("outperformed"),
                "decision_trace": cf_json.get("decision_trace", []),
            })

        return {
            "found": True,
            "portfolio_id": portfolio_id,
            "as_of_ts": first.get("as_of_ts"),
            "run_id": first.get("run_id"),
            "actual": actual,
            "scenarios": scenarios,
        }
    finally:
        conn.close()


# ──────────────────────────────────────────────────────────────
# GET /parallel-worlds/regret
# ──────────────────────────────────────────────────────────────
@router.get("/regret")
def get_regret(
    portfolio_id: int = Query(..., description="Portfolio ID"),
    days: int = Query(20, description="Number of days to include"),
):
    """Regret heatmap data: rolling regret per scenario per day."""
    sql = """
    SELECT
        AS_OF_TS,
        SCENARIO_ID,
        SCENARIO_NAME,
        SCENARIO_DISPLAY_NAME,
        SCENARIO_TYPE,
        ROUND(PNL_DELTA, 2) AS PNL_DELTA,
        ROUND(DAILY_REGRET, 2) AS DAILY_REGRET,
        ROUND(ROLLING_REGRET_20D, 2) AS ROLLING_REGRET_20D,
        ROUND(ROLLING_AVG_DELTA_20D, 2) AS ROLLING_AVG_DELTA_20D,
        ROLLING_OUTPERFORM_COUNT_20D,
        ROUND(CUMULATIVE_REGRET, 2) AS CUMULATIVE_REGRET,
        OUTPERFORM_PCT
    FROM MIP.MART.V_PARALLEL_WORLD_REGRET
    WHERE PORTFOLIO_ID = %s
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY SCENARIO_ID
        ORDER BY AS_OF_TS DESC
    ) <= %s
    ORDER BY AS_OF_TS, SCENARIO_ID
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, (portfolio_id, days))
        rows = _fetch_all(cur)
        return {
            "portfolio_id": portfolio_id,
            "days_requested": days,
            "data": serialize_rows(rows),
            "count": len(rows),
        }
    finally:
        conn.close()


# ──────────────────────────────────────────────────────────────
# GET /parallel-worlds/narrative
# ──────────────────────────────────────────────────────────────
@router.get("/narrative")
def get_narrative(
    portfolio_id: int = Query(..., description="Portfolio ID"),
    as_of_ts: str = Query(None, description="Date filter (YYYY-MM-DD)"),
):
    """Latest parallel-worlds narrative for a portfolio."""
    conditions = ["n.PORTFOLIO_ID = %s"]
    params = [portfolio_id]

    if as_of_ts:
        conditions.append("n.AS_OF_TS::date = %s::date")
        params.append(as_of_ts)

    where = " AND ".join(conditions)

    sql = f"""
    SELECT
        n.PORTFOLIO_ID,
        n.AS_OF_TS,
        n.RUN_ID,
        n.AGENT_NAME,
        n.NARRATIVE_TEXT,
        n.NARRATIVE_JSON,
        n.MODEL_INFO,
        n.SOURCE_FACTS_HASH,
        n.CREATED_AT
    FROM MIP.AGENT_OUT.PARALLEL_WORLD_NARRATIVE n
    WHERE {where}
    ORDER BY n.CREATED_AT DESC
    LIMIT 1
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, tuple(params))
        row = cur.fetchone()
        if not row:
            return {"found": False, "message": "No parallel-worlds narrative found."}

        columns = [d[0].lower() for d in cur.description]
        data = serialize_row(dict(zip(columns, row)))

        narrative_json = _clean_narrative_json(_parse_json(data.get("narrative_json")))
        is_ai = data.get("model_info") and data["model_info"] != "DETERMINISTIC_FALLBACK"

        return {
            "found": True,
            "portfolio_id": data.get("portfolio_id"),
            "as_of_ts": data.get("as_of_ts"),
            "run_id": data.get("run_id"),
            "narrative": narrative_json,
            "narrative_text": data.get("narrative_text"),
            "model_info": data.get("model_info"),
            "is_ai_narrative": is_ai,
            "source_facts_hash": data.get("source_facts_hash"),
            "created_at": data.get("created_at"),
        }
    finally:
        conn.close()


# ──────────────────────────────────────────────────────────────
# GET /parallel-worlds/confidence
# ──────────────────────────────────────────────────────────────
@router.get("/confidence")
def get_confidence(
    portfolio_id: int = Query(..., description="Portfolio ID"),
):
    """Confidence classification for each scenario — signal strength + recommendation."""
    sql = """
    SELECT
        PORTFOLIO_ID,
        AS_OF_TS,
        SCENARIO_ID,
        SCENARIO_NAME,
        SCENARIO_DISPLAY_NAME,
        SCENARIO_TYPE,
        TOTAL_DAYS,
        ROUND(OUTPERFORM_PCT, 1) AS OUTPERFORM_PCT,
        ROUND(CUMULATIVE_DELTA, 2) AS CUMULATIVE_DELTA,
        ROUND(CUMULATIVE_REGRET, 2) AS CUMULATIVE_REGRET,
        ROUND(ROLLING_AVG_DELTA_20D, 2) AS ROLLING_AVG_DELTA_20D,
        CONFIDENCE_CLASS,
        CONFIDENCE_REASON,
        RECOMMENDATION_STRENGTH
    FROM MIP.MART.V_PARALLEL_WORLD_CONFIDENCE
    WHERE PORTFOLIO_ID = %s
    ORDER BY
        CASE CONFIDENCE_CLASS
            WHEN 'STRONG' THEN 1
            WHEN 'EMERGING' THEN 2
            WHEN 'WEAK' THEN 3
            ELSE 4
        END,
        CUMULATIVE_DELTA DESC
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, (portfolio_id,))
        rows = _fetch_all(cur)
        return {
            "portfolio_id": portfolio_id,
            "data": serialize_rows(rows),
            "count": len(rows),
        }
    finally:
        conn.close()


# ──────────────────────────────────────────────────────────────
# GET /parallel-worlds/policy-diagnostics
# ──────────────────────────────────────────────────────────────
@router.get("/policy-diagnostics")
def get_policy_diagnostics(
    portfolio_id: int = Query(..., description="Portfolio ID"),
):
    """Policy health summary — combines confidence, attribution, and stability."""
    sql = """
    SELECT
        PORTFOLIO_ID,
        TOTAL_SCENARIOS,
        STRONG_SIGNALS,
        EMERGING_SIGNALS,
        WEAK_SIGNALS,
        NOISE_SIGNALS,
        POLICY_HEALTH,
        POLICY_HEALTH_REASON,
        DOMINANT_DRIVER_TYPE,
        DOMINANT_DRIVER_LABEL,
        ROUND(DOMINANT_DRIVER_REGRET, 2) AS DOMINANT_DRIVER_REGRET,
        DOMINANT_DRIVER_BEST_SCENARIO,
        DOMINANT_DRIVER_BEST_CONFIDENCE,
        TOP_RECOMMENDATION,
        TOP_RECOMMENDATION_TYPE,
        STABILITY_SCORE,
        STABILITY_LABEL
    FROM MIP.MART.V_PARALLEL_WORLD_POLICY_DIAGNOSTICS
    WHERE PORTFOLIO_ID = %s
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, (portfolio_id,))
        rows = _fetch_all(cur)
        if not rows:
            return {"found": False, "message": "No policy diagnostics available."}
        return {
            "found": True,
            "portfolio_id": portfolio_id,
            **serialize_row(rows[0]),
        }
    finally:
        conn.close()


# ──────────────────────────────────────────────────────────────
# GET /parallel-worlds/regret-attribution
# ──────────────────────────────────────────────────────────────
@router.get("/regret-attribution")
def get_regret_attribution(
    portfolio_id: int = Query(..., description="Portfolio ID"),
):
    """Regret attribution by scenario type — identifies the dominant regret driver."""
    sql = """
    SELECT
        PORTFOLIO_ID,
        SCENARIO_TYPE,
        TYPE_LABEL,
        SCENARIO_COUNT,
        AVG_OUTPERFORM_PCT,
        TOTAL_CUMULATIVE_DELTA,
        TOTAL_CUMULATIVE_REGRET,
        AVG_CUMULATIVE_DELTA,
        AVG_ROLLING_AVG_DELTA_20D,
        MAX_CUMULATIVE_DELTA,
        BEST_SCENARIO_NAME,
        BEST_SCENARIO_DISPLAY_NAME,
        BEST_CONFIDENCE_CLASS,
        REGRET_RANK,
        IS_DOMINANT_DRIVER
    FROM MIP.MART.V_PARALLEL_WORLD_REGRET_ATTRIBUTION
    WHERE PORTFOLIO_ID = %s
    ORDER BY REGRET_RANK
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, (portfolio_id,))
        rows = _fetch_all(cur)
        dominant = next((r for r in rows if r.get("is_dominant_driver")), None)
        return {
            "portfolio_id": portfolio_id,
            "data": serialize_rows(rows),
            "dominant_driver": serialize_row(dominant) if dominant else None,
            "count": len(rows),
        }
    finally:
        conn.close()


# ──────────────────────────────────────────────────────────────
# GET /parallel-worlds/equity-curves
# ──────────────────────────────────────────────────────────────
@router.get("/equity-curves")
def get_equity_curves(
    portfolio_id: int = Query(..., description="Portfolio ID"),
    scenario_id: int = Query(None, description="Specific scenario ID (null = all)"),
    days: int = Query(30, description="Number of days of history"),
):
    """Rolling equity curve data: actual + counterfactual overlays."""
    conditions = ["r.PORTFOLIO_ID = %s"]
    params = [portfolio_id]

    if scenario_id is not None:
        conditions.append("(r.SCENARIO_ID = %s OR r.SCENARIO_ID = 0)")
        params.append(scenario_id)

    where = " AND ".join(conditions)

    sql = f"""
    SELECT
        r.AS_OF_TS,
        r.SCENARIO_ID,
        COALESCE(s.DISPLAY_NAME, s.NAME, 'ACTUAL') AS SCENARIO_NAME,
        r.WORLD_KEY,
        ROUND(r.END_EQUITY_SIMULATED, 2) AS EQUITY,
        ROUND(r.PNL_SIMULATED, 2) AS PNL,
        r.TRADES_SIMULATED AS TRADES,
        r.OPEN_POSITIONS_END AS POSITIONS
    FROM MIP.APP.PARALLEL_WORLD_RESULT r
    LEFT JOIN MIP.APP.PARALLEL_WORLD_SCENARIO s ON s.SCENARIO_ID = r.SCENARIO_ID
    WHERE {where}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY r.PORTFOLIO_ID, r.SCENARIO_ID, r.AS_OF_TS::date
        ORDER BY r.CREATED_AT DESC
    ) = 1
    ORDER BY r.AS_OF_TS, r.SCENARIO_ID
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, tuple(params))
        rows = _fetch_all(cur)

        # Group by scenario for chart-friendly format
        by_scenario = {}
        for row in rows:
            r = serialize_row(row)
            key = r.get("scenario_name", "ACTUAL")
            if key not in by_scenario:
                by_scenario[key] = {
                    "scenario_id": r.get("scenario_id"),
                    "scenario_name": key,
                    "world_key": r.get("world_key"),
                    "points": [],
                }
            by_scenario[key]["points"].append({
                "as_of_ts": r.get("as_of_ts"),
                "equity": r.get("equity"),
                "pnl": r.get("pnl"),
                "trades": r.get("trades"),
                "positions": r.get("positions"),
            })

        return {
            "portfolio_id": portfolio_id,
            "curves": list(by_scenario.values()),
            "scenario_count": len(by_scenario),
        }
    finally:
        conn.close()
