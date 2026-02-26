from __future__ import annotations

import threading
import time
from datetime import datetime
from typing import Any, Callable, Optional

from fastapi import APIRouter, HTTPException, Query

from app.db import fetch_all, get_connection, serialize_row, serialize_rows

router = APIRouter(prefix="/intraday", tags=["intraday"])

_CACHE_LOCK = threading.Lock()
_CACHE: dict[tuple[Any, ...], tuple[float, Any]] = {}


def _cached(key: tuple[Any, ...], ttl_seconds: int, loader: Callable[[], Any]) -> Any:
    now = time.monotonic()
    with _CACHE_LOCK:
        cached = _CACHE.get(key)
        if cached and cached[0] > now:
            return cached[1]
    value = loader()
    with _CACHE_LOCK:
        _CACHE[key] = (now + ttl_seconds, value)
    return value


def _as_of_value(as_of_ts: Optional[str]) -> Optional[str]:
    if as_of_ts is None:
        return None
    try:
        datetime.fromisoformat(as_of_ts.replace("Z", "+00:00"))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid as_of_ts, expected ISO-8601 format.") from exc
    return as_of_ts


def _safe_limit(raw_limit: int, default_limit: int, max_limit: int) -> int:
    if raw_limit <= 0:
        return default_limit
    return min(raw_limit, max_limit)


@router.get("/dashboard")
def get_intraday_dashboard(
    as_of_ts: Optional[str] = Query(None, description="ISO timestamp for deterministic trust snapshot selection"),
    days: int = Query(90, ge=7, le=365),
):
    as_of = _as_of_value(as_of_ts)
    cache_key = ("dashboard", as_of, days)

    def _load() -> dict[str, Any]:
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                with as_of_input as (
                    select coalesce(%s::timestamp_ntz, current_timestamp()) as as_of_ts
                ),
                trust_snapshot as (
                    select t.*
                    from MIP.MART.V_INTRADAY_UI_TRUST t
                    join as_of_input a on 1 = 1
                    where t.CALCULATED_AT <= a.as_of_ts
                    qualify row_number() over (
                        partition by t.PATTERN_ID, t.MARKET_TYPE, t.INTERVAL_MINUTES, t.HORIZON_BARS, t.STATE_BUCKET_ID
                        order by t.CALCULATED_AT desc, t.TRAIN_WINDOW_END desc
                    ) = 1
                ),
                signal_rollup as (
                    select
                        count_if(SIGNAL_DATE >= dateadd(day, -7, current_date())) as SIGNALS_7D,
                        count_if(SIGNAL_DATE >= dateadd(day, -30, current_date())) as SIGNALS_30D,
                        count(distinct SYMBOL) as SYMBOLS_TOTAL,
                        count(distinct PATTERN_ID) as PATTERNS_TOTAL
                    from MIP.MART.V_INTRADAY_UI_SIGNALS
                ),
                terrain_latest as (
                    select
                        count(distinct TERRAIN_SCORE) as TERRAIN_DISTINCT_SCORES,
                        stddev(TERRAIN_SCORE) as TERRAIN_STDDEV
                    from MIP.MART.V_INTRADAY_UI_TERRAIN
                    qualify row_number() over (
                        partition by PATTERN_ID, MARKET_TYPE, SYMBOL, HORIZON_BARS, STATE_BUCKET_ID
                        order by CALCULATED_AT desc
                    ) = 1
                )
                select
                    (select as_of_ts from as_of_input) as AS_OF_TS,
                    (select max(CALCULATED_AT) from trust_snapshot) as TRUST_CALCULATED_AT,
                    (select count(*) from trust_snapshot) as TRUST_ROWS,
                    (select count_if(FALLBACK_LEVEL = 'EXACT') from trust_snapshot) as EXACT_ROWS,
                    (select count_if(FALLBACK_LEVEL = 'REGIME_ONLY') from trust_snapshot) as REGIME_ROWS,
                    (select count_if(FALLBACK_LEVEL = 'GLOBAL') from trust_snapshot) as GLOBAL_ROWS,
                    sr.SIGNALS_7D,
                    sr.SIGNALS_30D,
                    sr.SYMBOLS_TOTAL,
                    sr.PATTERNS_TOTAL,
                    tl.TERRAIN_DISTINCT_SCORES,
                    tl.TERRAIN_STDDEV
                from signal_rollup sr
                cross join terrain_latest tl
                """,
                (as_of,),
            )
            kpi_rows = fetch_all(cur)
            kpis = serialize_row(kpi_rows[0]) if kpi_rows else {}

            cur.execute(
                """
                with latest_per_day as (
                    select *
                    from MIP.MART.V_INTRADAY_UI_TRUST
                    where CALCULATED_DATE >= dateadd(day, -%s, current_date())
                    qualify row_number() over (
                        partition by CALCULATED_DATE, PATTERN_ID, MARKET_TYPE, INTERVAL_MINUTES, HORIZON_BARS, STATE_BUCKET_ID
                        order by CALCULATED_AT desc, TRAIN_WINDOW_END desc
                    ) = 1
                )
                select
                    CALCULATED_DATE,
                    count(*) as TOTAL_ROWS,
                    count_if(FALLBACK_LEVEL = 'EXACT') as EXACT_ROWS,
                    count_if(FALLBACK_LEVEL = 'REGIME_ONLY') as REGIME_ROWS,
                    count_if(FALLBACK_LEVEL = 'GLOBAL') as GLOBAL_ROWS
                from latest_per_day
                group by CALCULATED_DATE
                order by CALCULATED_DATE
                """,
                (days,),
            )
            fallback_series = serialize_rows(fetch_all(cur))

            cur.execute(
                """
                select
                    SIGNAL_DATE,
                    count(*) as SIGNALS_TOTAL,
                    count(distinct SYMBOL) as SYMBOLS_COVERED,
                    count(distinct PATTERN_ID) as PATTERNS_ACTIVE
                from MIP.MART.V_INTRADAY_UI_SIGNALS
                where SIGNAL_DATE >= dateadd(day, -%s, current_date())
                group by SIGNAL_DATE
                order by SIGNAL_DATE
                """,
                (days,),
            )
            signals_series = serialize_rows(fetch_all(cur))

            cur.execute(
                """
                select
                    CALCULATED_DATE,
                    count(*) as TERRAIN_ROWS,
                    count(distinct TERRAIN_SCORE) as DISTINCT_SCORE_COUNT,
                    stddev(TERRAIN_SCORE) as TERRAIN_STDDEV,
                    min(TERRAIN_SCORE) as TERRAIN_MIN,
                    max(TERRAIN_SCORE) as TERRAIN_MAX
                from MIP.MART.V_INTRADAY_UI_TERRAIN
                where CALCULATED_DATE >= dateadd(day, -%s, current_date())
                group by CALCULATED_DATE
                order by CALCULATED_DATE
                """,
                (days,),
            )
            terrain_health = serialize_rows(fetch_all(cur))

            cur.execute(
                """
                with as_of_input as (
                    select coalesce(%s::timestamp_ntz, current_timestamp()) as as_of_ts
                ),
                snapshot_rows_a as (
                    select *
                    from MIP.MART.V_INTRADAY_UI_TRUST t
                    join as_of_input a on 1 = 1
                    where t.CALCULATED_AT <= a.as_of_ts
                    qualify row_number() over (
                        partition by t.PATTERN_ID, t.MARKET_TYPE, t.INTERVAL_MINUTES, t.HORIZON_BARS, t.STATE_BUCKET_ID
                        order by t.CALCULATED_AT desc, t.TRAIN_WINDOW_END desc
                    ) = 1
                ),
                snapshot_rows_b as (
                    select *
                    from MIP.MART.V_INTRADAY_UI_TRUST t
                    join as_of_input a on 1 = 1
                    where t.CALCULATED_AT <= a.as_of_ts
                    qualify row_number() over (
                        partition by t.PATTERN_ID, t.MARKET_TYPE, t.INTERVAL_MINUTES, t.HORIZON_BARS, t.STATE_BUCKET_ID
                        order by t.CALCULATED_AT desc, t.TRAIN_WINDOW_END desc
                    ) = 1
                ),
                snapshot_a as (
                    select
                        count(*) as ROWS_A,
                        hash_agg(
                            concat_ws(
                                '|',
                                PATTERN_ID::varchar,
                                MARKET_TYPE,
                                HORIZON_BARS::varchar,
                                coalesce(STATE_BUCKET_ID::varchar, 'NULL'),
                                coalesce(FALLBACK_LEVEL, 'NULL'),
                                coalesce(N_SIGNALS::varchar, 'NULL')
                            )
                        ) as HASH_A
                    from snapshot_rows_a
                ),
                snapshot_b as (
                    select
                        count(*) as ROWS_B,
                        hash_agg(
                            concat_ws(
                                '|',
                                PATTERN_ID::varchar,
                                MARKET_TYPE,
                                HORIZON_BARS::varchar,
                                coalesce(STATE_BUCKET_ID::varchar, 'NULL'),
                                coalesce(FALLBACK_LEVEL, 'NULL'),
                                coalesce(N_SIGNALS::varchar, 'NULL')
                            )
                        ) as HASH_B
                    from snapshot_rows_b
                )
                select
                    a.ROWS_A,
                    b.ROWS_B,
                    a.HASH_A = b.HASH_B as SNAPSHOT_DETERMINISTIC
                from snapshot_a a
                cross join snapshot_b b
                """,
                (as_of,),
            )
            snapshot_validation_rows = fetch_all(cur)
            snapshot_validation = serialize_row(snapshot_validation_rows[0]) if snapshot_validation_rows else {}

            sig_30d_from_series = 0
            cutoff_30 = (datetime.utcnow().date()).toordinal() - 30
            for row in signals_series:
                raw_date = row.get("SIGNAL_DATE")
                if raw_date is None:
                    continue
                row_ord = None
                try:
                    row_ord = datetime.fromisoformat(str(raw_date)).date().toordinal()
                except ValueError:
                    pass
                if row_ord is not None and row_ord >= cutoff_30:
                    sig_30d_from_series += int(row.get("SIGNALS_TOTAL") or 0)

            return {
                "kpis": kpis,
                "fallback_mix_series": fallback_series,
                "signals_per_day": signals_series,
                "terrain_health": terrain_health,
                "validation": {
                    "signals_30d_kpi": int((kpis or {}).get("SIGNALS_30D") or 0),
                    "signals_series_sum": sig_30d_from_series,
                    "signals_reconciled": int((kpis or {}).get("SIGNALS_30D") or 0) == sig_30d_from_series,
                    "snapshot_deterministic": bool((snapshot_validation or {}).get("SNAPSHOT_DETERMINISTIC")),
                    "snapshot_rows_a": int((snapshot_validation or {}).get("ROWS_A") or 0),
                    "snapshot_rows_b": int((snapshot_validation or {}).get("ROWS_B") or 0),
                },
            }
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        finally:
            conn.close()

    return _cached(cache_key, 60, _load)


@router.get("/patterns")
def get_intraday_patterns(
    as_of_ts: Optional[str] = Query(None, description="ISO timestamp for deterministic trust snapshot selection"),
    limit: int = Query(100, ge=1, le=500),
):
    as_of = _as_of_value(as_of_ts)
    capped_limit = _safe_limit(limit, default_limit=100, max_limit=200)
    cache_key = ("patterns", as_of, capped_limit)

    def _load() -> dict[str, Any]:
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                with as_of_input as (
                    select coalesce(%s::timestamp_ntz, current_timestamp()) as as_of_ts
                ),
                trust_snapshot as (
                    select t.*
                    from MIP.MART.V_INTRADAY_UI_TRUST t
                    join as_of_input a on 1 = 1
                    where t.CALCULATED_AT <= a.as_of_ts
                    qualify row_number() over (
                        partition by t.PATTERN_ID, t.MARKET_TYPE, t.INTERVAL_MINUTES, t.HORIZON_BARS, t.STATE_BUCKET_ID
                        order by t.CALCULATED_AT desc, t.TRAIN_WINDOW_END desc
                    ) = 1
                ),
                signal_30d as (
                    select
                        PATTERN_ID,
                        count(*) as SIGNALS_30D,
                        count(distinct SYMBOL) as SYMBOLS_30D
                    from MIP.MART.V_INTRADAY_UI_SIGNALS
                    where SIGNAL_TS >= dateadd(day, -30, current_timestamp())
                    group by PATTERN_ID
                )
                select
                    p.PATTERN_ID,
                    p.PATTERN_NAME,
                    p.PATTERN_TYPE,
                    p.INTRA_IS_ENABLED,
                    count(*) as TRUST_ROWS,
                    avg(t.HIT_RATE) as AVG_HIT_RATE,
                    avg(t.AVG_RETURN_NET) as AVG_RETURN_NET,
                    avg(t.CI_WIDTH) as AVG_CI_WIDTH,
                    avg(t.N_SIGNALS) as AVG_EVIDENCE_N,
                    count_if(t.FALLBACK_LEVEL = 'GLOBAL') as GLOBAL_ROWS,
                    count_if(t.FALLBACK_LEVEL = 'REGIME_ONLY') as REGIME_ROWS,
                    count_if(t.FALLBACK_LEVEL = 'EXACT') as EXACT_ROWS,
                    coalesce(s.SIGNALS_30D, 0) as SIGNALS_30D,
                    coalesce(s.SYMBOLS_30D, 0) as SYMBOLS_30D,
                    max(t.CALCULATED_AT) as CALCULATED_AT
                from MIP.MART.V_INTRADAY_UI_PATTERN_CATALOG p
                left join trust_snapshot t
                  on t.PATTERN_ID = p.PATTERN_ID
                left join signal_30d s
                  on s.PATTERN_ID = p.PATTERN_ID
                group by p.PATTERN_ID, p.PATTERN_NAME, p.PATTERN_TYPE, p.INTRA_IS_ENABLED, s.SIGNALS_30D, s.SYMBOLS_30D
                order by SIGNALS_30D desc, p.PATTERN_ID
                limit %s
                """,
                (as_of, capped_limit),
            )
            return {"rows": serialize_rows(fetch_all(cur)), "limit": capped_limit}
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        finally:
            conn.close()

    return _cached(cache_key, 60, _load)


@router.get("/pattern/{pattern_id}")
def get_intraday_pattern_detail(
    pattern_id: int,
    as_of_ts: Optional[str] = Query(None, description="ISO timestamp for deterministic trust snapshot selection"),
    days: int = Query(90, ge=7, le=365),
):
    as_of = _as_of_value(as_of_ts)
    cache_key = ("pattern_detail", pattern_id, as_of, days)

    def _load() -> dict[str, Any]:
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                with as_of_input as (
                    select coalesce(%s::timestamp_ntz, current_timestamp()) as as_of_ts
                ),
                trust_snapshot as (
                    select t.*
                    from MIP.MART.V_INTRADAY_UI_TRUST t
                    join as_of_input a on 1 = 1
                    where t.CALCULATED_AT <= a.as_of_ts
                      and t.PATTERN_ID = %s
                    qualify row_number() over (
                        partition by t.PATTERN_ID, t.MARKET_TYPE, t.INTERVAL_MINUTES, t.HORIZON_BARS, t.STATE_BUCKET_ID
                        order by t.CALCULATED_AT desc, t.TRAIN_WINDOW_END desc
                    ) = 1
                )
                select
                    p.PATTERN_ID,
                    p.PATTERN_NAME,
                    p.PATTERN_TYPE,
                    p.DESCRIPTION,
                    p.INTRA_IS_ENABLED,
                    count(*) as TRUST_ROWS,
                    avg(t.HIT_RATE) as AVG_HIT_RATE,
                    avg(t.AVG_RETURN_NET) as AVG_RETURN_NET,
                    avg(t.CI_WIDTH) as AVG_CI_WIDTH,
                    avg(t.N_SIGNALS) as AVG_EVIDENCE_N,
                    count_if(t.FALLBACK_LEVEL = 'GLOBAL') as GLOBAL_ROWS,
                    count_if(t.FALLBACK_LEVEL = 'REGIME_ONLY') as REGIME_ROWS,
                    count_if(t.FALLBACK_LEVEL = 'EXACT') as EXACT_ROWS,
                    max(t.CALCULATED_AT) as CALCULATED_AT
                from MIP.MART.V_INTRADAY_UI_PATTERN_CATALOG p
                left join trust_snapshot t
                  on t.PATTERN_ID = p.PATTERN_ID
                where p.PATTERN_ID = %s
                group by p.PATTERN_ID, p.PATTERN_NAME, p.PATTERN_TYPE, p.DESCRIPTION, p.INTRA_IS_ENABLED
                """,
                (as_of, pattern_id, pattern_id),
            )
            summary_rows = fetch_all(cur)
            if not summary_rows:
                raise HTTPException(status_code=404, detail=f"Pattern {pattern_id} not found.")
            summary = serialize_row(summary_rows[0])

            cur.execute(
                """
                select
                    SIGNAL_DATE,
                    count(*) as SIGNALS_TOTAL,
                    count(distinct SYMBOL) as SYMBOLS_COVERED
                from MIP.MART.V_INTRADAY_UI_SIGNALS
                where PATTERN_ID = %s
                  and SIGNAL_DATE >= dateadd(day, -%s, current_date())
                group by SIGNAL_DATE
                order by SIGNAL_DATE
                """,
                (pattern_id, days),
            )
            signals_series = serialize_rows(fetch_all(cur))

            cur.execute(
                """
                with latest_per_day as (
                    select *
                    from MIP.MART.V_INTRADAY_UI_TRUST
                    where PATTERN_ID = %s
                      and CALCULATED_DATE >= dateadd(day, -%s, current_date())
                    qualify row_number() over (
                        partition by CALCULATED_DATE, PATTERN_ID, MARKET_TYPE, INTERVAL_MINUTES, HORIZON_BARS, STATE_BUCKET_ID
                        order by CALCULATED_AT desc, TRAIN_WINDOW_END desc
                    ) = 1
                )
                select
                    CALCULATED_DATE,
                    count(*) as TOTAL_ROWS,
                    count_if(FALLBACK_LEVEL = 'EXACT') as EXACT_ROWS,
                    count_if(FALLBACK_LEVEL = 'REGIME_ONLY') as REGIME_ROWS,
                    count_if(FALLBACK_LEVEL = 'GLOBAL') as GLOBAL_ROWS
                from latest_per_day
                group by CALCULATED_DATE
                order by CALCULATED_DATE
                """,
                (pattern_id, days),
            )
            fallback_series = serialize_rows(fetch_all(cur))

            cur.execute(
                """
                select
                    SYMBOL,
                    count(*) as SIGNALS_TOTAL
                from MIP.MART.V_INTRADAY_UI_SIGNALS
                where PATTERN_ID = %s
                  and SIGNAL_DATE >= dateadd(day, -%s, current_date())
                group by SYMBOL
                order by SIGNALS_TOTAL desc, SYMBOL
                limit 50
                """,
                (pattern_id, days),
            )
            symbol_distribution = serialize_rows(fetch_all(cur))

            return {
                "summary": summary,
                "signals_per_day": signals_series,
                "fallback_mix_series": fallback_series,
                "symbol_distribution": symbol_distribution,
            }
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        finally:
            conn.close()

    return _cached(cache_key, 60, _load)


@router.get("/terrain/top")
def get_intraday_terrain_top(
    as_of_ts: Optional[str] = Query(None, description="ISO timestamp for deterministic terrain snapshot selection"),
    limit: int = Query(25, ge=1, le=500),
):
    as_of = _as_of_value(as_of_ts)
    capped_limit = _safe_limit(limit, default_limit=25, max_limit=100)
    cache_key = ("terrain_top", as_of, capped_limit)

    def _load() -> dict[str, Any]:
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                with as_of_input as (
                    select coalesce(%s::timestamp_ntz, current_timestamp()) as as_of_ts
                ),
                terrain_snapshot as (
                    select t.*
                    from MIP.MART.V_INTRADAY_UI_TERRAIN t
                    join as_of_input a on 1 = 1
                    where t.CALCULATED_AT <= a.as_of_ts
                    qualify row_number() over (
                        partition by t.PATTERN_ID, t.MARKET_TYPE, t.SYMBOL, t.HORIZON_BARS, t.STATE_BUCKET_ID, t.TS
                        order by t.CALCULATED_AT desc
                    ) = 1
                )
                select
                    PATTERN_ID,
                    MARKET_TYPE,
                    SYMBOL,
                    TS,
                    HORIZON_BARS,
                    STATE_BUCKET_ID,
                    TERRAIN_SCORE,
                    EDGE,
                    UNCERTAINTY,
                    SUITABILITY,
                    N_SIGNALS,
                    CANDIDATE_SOURCE,
                    CALCULATED_AT
                from terrain_snapshot
                order by TERRAIN_SCORE desc, TS desc
                limit %s
                """,
                (as_of, capped_limit),
            )
            return {"rows": serialize_rows(fetch_all(cur)), "limit": capped_limit}
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        finally:
            conn.close()

    return _cached(cache_key, 30, _load)


@router.get("/terrain/heatmap")
def get_intraday_terrain_heatmap(
    as_of_ts: Optional[str] = Query(None, description="ISO timestamp for deterministic terrain snapshot selection"),
):
    as_of = _as_of_value(as_of_ts)
    cache_key = ("terrain_heatmap", as_of)

    def _load() -> dict[str, Any]:
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                with as_of_input as (
                    select coalesce(%s::timestamp_ntz, current_timestamp()) as as_of_ts
                ),
                terrain_snapshot as (
                    select t.*
                    from MIP.MART.V_INTRADAY_UI_TERRAIN t
                    join as_of_input a on 1 = 1
                    where t.CALCULATED_AT <= a.as_of_ts
                    qualify row_number() over (
                        partition by t.PATTERN_ID, t.MARKET_TYPE, t.SYMBOL, t.HORIZON_BARS, t.STATE_BUCKET_ID, t.TS
                        order by t.CALCULATED_AT desc
                    ) = 1
                )
                select
                    PATTERN_ID,
                    STATE_BUCKET_ID,
                    count(*) as CELL_COUNT,
                    avg(TERRAIN_SCORE) as TERRAIN_SCORE_AVG,
                    min(TERRAIN_SCORE) as TERRAIN_SCORE_MIN,
                    max(TERRAIN_SCORE) as TERRAIN_SCORE_MAX,
                    stddev(TERRAIN_SCORE) as TERRAIN_SCORE_STDDEV
                from terrain_snapshot
                group by PATTERN_ID, STATE_BUCKET_ID
                order by PATTERN_ID, STATE_BUCKET_ID
                """
                ,
                (as_of,),
            )
            return {"rows": serialize_rows(fetch_all(cur))}
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        finally:
            conn.close()

    return _cached(cache_key, 45, _load)


@router.get("/health")
def get_intraday_health():
    cache_key = ("health",)

    def _load() -> dict[str, Any]:
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute("select * from MIP.MART.V_INTRADAY_PIPELINE_STATUS")
            pipeline_rows = fetch_all(cur)
            pipeline = serialize_row(pipeline_rows[0]) if pipeline_rows else {}

            cur.execute(
                """
                select
                    STATUS,
                    count(*) as RUN_COUNT
                from MIP.MART.V_INTRADAY_UI_BACKFILL_RUNS
                where CREATED_AT >= dateadd(day, -14, current_timestamp())
                group by STATUS
                order by RUN_COUNT desc
                """
            )
            backfill_status_mix = serialize_rows(fetch_all(cur))

            cur.execute(
                """
                select *
                from MIP.MART.V_INTRADAY_UI_BACKFILL_RUNS
                order by CREATED_AT desc
                limit 20
                """
            )
            recent_runs = serialize_rows(fetch_all(cur))

            return {
                "pipeline": pipeline,
                "backfill_status_mix": backfill_status_mix,
                "recent_backfill_runs": recent_runs,
            }
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        finally:
            conn.close()

    return _cached(cache_key, 30, _load)


@router.get("/backfill/runs")
def get_intraday_backfill_runs(
    limit: int = Query(50, ge=1, le=500),
):
    capped_limit = _safe_limit(limit, default_limit=50, max_limit=200)
    cache_key = ("backfill_runs", capped_limit)

    def _load() -> dict[str, Any]:
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                select *
                from MIP.MART.V_INTRADAY_UI_BACKFILL_RUNS
                order by CREATED_AT desc
                limit %s
                """,
                (capped_limit,),
            )
            return {"rows": serialize_rows(fetch_all(cur)), "limit": capped_limit}
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        finally:
            conn.close()

    return _cached(cache_key, 30, _load)


@router.get("/backfill/run/{run_id}")
def get_intraday_backfill_run(run_id: str):
    cache_key = ("backfill_run", run_id)

    def _load() -> dict[str, Any]:
        conn = get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                select *
                from MIP.MART.V_INTRADAY_UI_BACKFILL_RUNS
                where RUN_ID = %s
                order by CREATED_AT desc
                limit 1
                """,
                (run_id,),
            )
            rows = fetch_all(cur)
            if not rows:
                raise HTTPException(status_code=404, detail=f"Backfill run {run_id} not found.")
            return serialize_row(rows[0])
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        finally:
            conn.close()

    return _cached(cache_key, 120, _load)
