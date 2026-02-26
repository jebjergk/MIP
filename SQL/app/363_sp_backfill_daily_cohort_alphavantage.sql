-- 363_sp_backfill_daily_cohort_alphavantage.sql
-- Purpose: Additive, idempotent Alpha Vantage DAILY backfill scoped by symbol cohort.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_BACKFILL_DAILY_ALPHAVANTAGE_COHORT(
    P_RUN_ID string default null,
    P_START_DATE date default '2025-09-01',
    P_END_DATE date default current_date(),
    P_SYMBOL_COHORT string default 'VOL_EXP',
    P_MARKET_TYPE string default null,
    P_WAREHOUSE_OVERRIDE string default null
)
returns variant
language python
runtime_version = '3.12'
packages = ('requests', 'snowflake-snowpark-python')
external_access_integrations = (MIP_ALPHA_EXTERNAL_ACCESS)
handler = 'run'
as
$$
import json
import requests
import time
from datetime import datetime, date
from typing import Dict, List, Tuple, Optional
from snowflake.snowpark import Session

ALPHAVANTAGE_BASE_URL = "https://www.alphavantage.co/query"

def _sql_literal(value) -> str:
    if value is None:
        return "null"
    if isinstance(value, (int, float)):
        return str(value)
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"

def _variant_literal(value) -> str:
    if value is None:
        return "null"
    return f"parse_json({_sql_literal(json.dumps(value))})"

def _safe_float(v):
    try:
        return float(v)
    except Exception:
        return None

def _get_config_map(session: Session) -> Dict[str, str]:
    rows = session.sql("select CONFIG_KEY, CONFIG_VALUE from MIP.APP.APP_CONFIG").collect()
    return {r["CONFIG_KEY"]: r["CONFIG_VALUE"] for r in rows}

def _normalize_fx_pair(pair: str) -> Optional[Tuple[str, str, str]]:
    cleaned = pair.replace(" ", "").replace("-", "/").replace("_", "/").upper()
    if "/" in cleaned:
        a, b = cleaned.split("/", 1)
    elif len(cleaned) == 6:
        a, b = cleaned[:3], cleaned[3:]
    else:
        return None
    if not a or not b:
        return None
    return a, b, f"{a}/{b}"

def _fetch_stock_daily(api_key: str, symbol: str) -> Dict:
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "outputsize": "full",
        "apikey": api_key,
    }
    resp = requests.get(ALPHAVANTAGE_BASE_URL, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()

def _fetch_fx_daily(api_key: str, from_symbol: str, to_symbol: str) -> Dict:
    params = {
        "function": "FX_DAILY",
        "from_symbol": from_symbol,
        "to_symbol": to_symbol,
        "outputsize": "full",
        "apikey": api_key,
    }
    resp = requests.get(ALPHAVANTAGE_BASE_URL, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()

def _extract_api_message(json_data: Dict) -> Optional[str]:
    for key in ("Error Message", "Note", "Information"):
        if key in json_data:
            return str(json_data.get(key))
    return None

def _extract_rows(
    json_data: Dict,
    symbol: str,
    market_type: str,
    start_date: date,
    end_date: date
) -> List[Dict]:
    rows: List[Dict] = []
    ts_key = next((k for k in json_data.keys() if k.startswith("Time Series")), None)
    if not ts_key:
        return rows
    ts_dict = json_data.get(ts_key, {})
    now_utc = datetime.utcnow()
    for ts_str, bar in ts_dict.items():
        try:
            try:
                ts_dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                ts_dt = datetime.strptime(ts_str, "%Y-%m-%d")
            if ts_dt.date() < start_date or ts_dt.date() > end_date:
                continue
            bar_dict = dict(bar)
            rows.append({
                "TS": ts_dt,
                "SYMBOL": symbol,
                "SOURCE": "ALPHAVANTAGE",
                "MARKET_TYPE": market_type,
                "INTERVAL_MINUTES": 1440,
                "OPEN": _safe_float(bar_dict.get("1. open")),
                "HIGH": _safe_float(bar_dict.get("2. high")),
                "LOW": _safe_float(bar_dict.get("3. low")),
                "CLOSE": _safe_float(bar_dict.get("4. close")),
                "VOLUME": _safe_float(bar_dict.get("5. volume")) if market_type in ("STOCK", "ETF") else None,
                "INGESTED_AT": now_utc,
            })
        except Exception:
            continue
    return rows

def _load_targets(session: Session, cohort: str, market_type: Optional[str]) -> List[Dict]:
    where = """
        where coalesce(IS_ENABLED, true)
          and INTERVAL_MINUTES = 1440
          and upper(coalesce(SYMBOL_COHORT, 'CORE')) = upper(?)
    """
    bind = [cohort]
    if market_type:
        where += " and upper(MARKET_TYPE) = upper(?)"
        bind.append(market_type)
    df = session.sql(
        f"""
        select SYMBOL, MARKET_TYPE
          from MIP.APP.INGEST_UNIVERSE
          {where}
         group by SYMBOL, MARKET_TYPE
         order by MARKET_TYPE, SYMBOL
        """,
        params=bind
    )
    return [{"SYMBOL": r["SYMBOL"], "MARKET_TYPE": r["MARKET_TYPE"]} for r in df.collect()]

def _upsert_run_log(session: Session, run_id: str, status: str, details: Dict):
    symbols_processed = int(details.get("symbols_processed") or 0)
    bars_loaded_count = int(details.get("bars_loaded_count") or 0)
    session.sql(
        f"""
        merge into MIP.APP.VOL_EXP_BOOTSTRAP_RUN_LOG t
        using (
            select
                {_sql_literal(run_id)} as RUN_ID,
                'BACKFILL_DAILY' as STEP_NAME
        ) s
        on t.RUN_ID = s.RUN_ID
       and t.STEP_NAME = s.STEP_NAME
        when matched then update set
            t.FINISHED_AT = current_timestamp(),
            t.STATUS = {_sql_literal(status)},
            t.SYMBOLS_PROCESSED = {symbols_processed},
            t.BARS_LOADED_COUNT = {bars_loaded_count},
            t.FAILURES = {_variant_literal(details.get("failures"))},
            t.DETAILS = {_variant_literal(details)}
        when not matched then insert (
            RUN_ID, STEP_NAME, SYMBOL_COHORT, MARKET_TYPE, START_DATE, END_DATE, STARTED_AT, FINISHED_AT,
            STATUS, SYMBOLS_PROCESSED, BARS_LOADED_COUNT, FAILURES, DETAILS
        ) values (
            {_sql_literal(run_id)},
            'BACKFILL_DAILY',
            {_sql_literal(details.get("symbol_cohort"))},
            {_sql_literal(details.get("market_type"))},
            to_date({_sql_literal(details.get("start_date"))}),
            to_date({_sql_literal(details.get("end_date"))}),
            current_timestamp(),
            current_timestamp(),
            {_sql_literal(status)},
            {symbols_processed},
            {bars_loaded_count},
            {_variant_literal(details.get("failures"))},
            {_variant_literal(details)}
        )
        """
    ).collect()

def run(
    session: Session,
    p_run_id=None,
    p_start_date=None,
    p_end_date=None,
    p_symbol_cohort="VOL_EXP",
    p_market_type=None,
    p_warehouse_override=None
):
    run_id = p_run_id or session.sql("select uuid_string() as ID").collect()[0]["ID"]
    start_date = p_start_date if isinstance(p_start_date, date) else date.fromisoformat(str(p_start_date or "2025-09-01"))
    end_date = p_end_date if isinstance(p_end_date, date) else date.fromisoformat(str(p_end_date or date.today()))
    cohort = str(p_symbol_cohort or "VOL_EXP").upper()
    market_type_filter = str(p_market_type).upper() if p_market_type else None

    if start_date > end_date:
        return {"status": "FAIL", "error": "start_date_after_end_date"}

    cfg = _get_config_map(session)
    api_key = cfg.get("ALPHAVANTAGE_API_KEY")
    if not api_key or api_key == "<PLACEHOLDER>":
        return {"status": "FAIL_MISSING_API_KEY", "run_id": run_id}

    targets = _load_targets(session, cohort, market_type_filter)
    if not targets:
        result = {
            "status": "SUCCESS",
            "run_id": run_id,
            "symbol_cohort": cohort,
            "market_type": market_type_filter,
            "start_date": str(start_date),
            "end_date": str(end_date),
            "symbols_processed": 0,
            "bars_loaded_count": 0,
            "failures": [],
            "symbols": [],
        }
        _upsert_run_log(session, run_id, "SUCCESS", result)
        return result

    all_rows: List[Dict] = []
    symbol_results: List[Dict] = []
    failures: List[Dict] = []

    for target in targets:
        symbol = str(target["SYMBOL"]).upper()
        market_type = str(target["MARKET_TYPE"]).upper()
        status = "SUCCESS"
        err = None
        extracted_rows: List[Dict] = []
        normalized_symbol = symbol
        try:
            if market_type in ("STOCK", "ETF"):
                data = _fetch_stock_daily(api_key, symbol)
                msg = _extract_api_message(data)
                if msg and "rate limit" in msg.lower():
                    status = "SKIPPED_RATE_LIMIT"
                    err = msg
                elif msg and "error" in msg.lower():
                    status = "FAIL"
                    err = msg
                else:
                    extracted_rows = _extract_rows(data, symbol, market_type, start_date, end_date)
            elif market_type == "FX":
                parsed = _normalize_fx_pair(symbol)
                if not parsed:
                    status = "FAIL"
                    err = "INVALID_FX_PAIR"
                else:
                    from_sym, to_sym, normalized_symbol = parsed
                    data = _fetch_fx_daily(api_key, from_sym, to_sym)
                    msg = _extract_api_message(data)
                    if msg and "rate limit" in msg.lower():
                        status = "SKIPPED_RATE_LIMIT"
                        err = msg
                    elif msg and "error" in msg.lower():
                        status = "FAIL"
                        err = msg
                    else:
                        extracted_rows = _extract_rows(data, normalized_symbol, market_type, start_date, end_date)
            else:
                status = "SKIPPED"
                err = f"UNSUPPORTED_MARKET_TYPE:{market_type}"
        except Exception as exc:
            status = "FAIL"
            err = str(exc)

        if extracted_rows:
            all_rows.extend(extracted_rows)

        symbol_result = {
            "symbol": normalized_symbol,
            "market_type": market_type,
            "status": status,
            "bars_loaded_count": len(extracted_rows),
            "error": err,
        }
        symbol_results.append(symbol_result)

        session.sql(
            f"""
            insert into MIP.APP.VOL_EXP_BOOTSTRAP_SYMBOL_LOG (
                RUN_ID, STEP_NAME, SYMBOL, MARKET_TYPE, STARTED_AT, FINISHED_AT, STATUS,
                BARS_LOADED_COUNT, ERROR_MESSAGE, DETAILS
            ) values (
                {_sql_literal(run_id)},
                'BACKFILL_DAILY',
                {_sql_literal(normalized_symbol)},
                {_sql_literal(market_type)},
                current_timestamp(),
                current_timestamp(),
                {_sql_literal(status)},
                {len(extracted_rows)},
                {_sql_literal(err)},
                null
            )
            """
        ).collect()

        if status == "FAIL":
            failures.append({"symbol": normalized_symbol, "market_type": market_type, "error": err})

        # Keep API pressure controlled.
        time.sleep(12)

    if all_rows:
        stage_table = "MIP.APP.STG_VOL_EXP_DAILY_BACKFILL"
        session.create_dataframe(all_rows).write.mode("overwrite").save_as_table(stage_table, table_type="transient")
        session.sql(
            f"""
            merge into MIP.MART.MARKET_BARS t
            using {stage_table} s
               on t.MARKET_TYPE = s.MARKET_TYPE
              and t.SYMBOL = s.SYMBOL
              and t.INTERVAL_MINUTES = s.INTERVAL_MINUTES
              and t.TS = s.TS
            when matched and (
                t.SOURCE is distinct from s.SOURCE
                or t.OPEN is distinct from s.OPEN
                or t.HIGH is distinct from s.HIGH
                or t.LOW is distinct from s.LOW
                or t.CLOSE is distinct from s.CLOSE
                or t.VOLUME is distinct from s.VOLUME
                or t.INGESTED_AT is distinct from s.INGESTED_AT
            ) then update set
                SOURCE = s.SOURCE,
                OPEN = s.OPEN,
                HIGH = s.HIGH,
                LOW = s.LOW,
                CLOSE = s.CLOSE,
                VOLUME = s.VOLUME,
                INGESTED_AT = s.INGESTED_AT
            when not matched then insert (
                TS, SYMBOL, SOURCE, MARKET_TYPE, INTERVAL_MINUTES, OPEN, HIGH, LOW, CLOSE, VOLUME, INGESTED_AT
            ) values (
                s.TS, s.SYMBOL, s.SOURCE, s.MARKET_TYPE, s.INTERVAL_MINUTES, s.OPEN, s.HIGH, s.LOW, s.CLOSE, s.VOLUME, s.INGESTED_AT
            )
            """
        ).collect()
        session.sql(f"drop table if exists {stage_table}").collect()

    status = "SUCCESS_WITH_SKIPS" if any(r["status"] != "SUCCESS" for r in symbol_results) else "SUCCESS"
    result = {
        "status": status,
        "run_id": run_id,
        "symbol_cohort": cohort,
        "market_type": market_type_filter,
        "start_date": str(start_date),
        "end_date": str(end_date),
        "symbols_processed": len(symbol_results),
        "bars_loaded_count": len(all_rows),
        "failures": failures,
        "symbols": symbol_results,
    }
    _upsert_run_log(session, run_id, status, result)
    return result
$$;

