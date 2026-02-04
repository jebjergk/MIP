use role ACCOUNTADMIN;  -- or a role with the necessary privileges

---------------------------------
-- 1. Network rule: AlphaVantage
---------------------------------
create or replace network rule MIP_ALPHA_NETWORK_RULE
  type = HOST_PORT
  value_list = ('www.alphavantage.co:443')
  mode = EGRESS;

---------------------------------
-- 2. External access integration
---------------------------------
create or replace external access integration MIP_ALPHA_EXTERNAL_ACCESS
  allowed_network_rules = (MIP_ALPHA_NETWORK_RULE)
  enabled = true;


use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.APP.SP_INGEST_ALPHAVANTAGE_BARS()
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
from datetime import datetime
from typing import List, Dict, Tuple
from snowflake.snowpark import Session

ALPHAVANTAGE_BASE_URL = "https://www.alphavantage.co/query"

def _get_config_map(session: Session) -> Dict[str, str]:
    cfg_df = session.table("MIP.APP.APP_CONFIG")
    rows = cfg_df.collect()
    return {row["CONFIG_KEY"]: row["CONFIG_VALUE"] for row in rows}

def _interval_to_alpha(interval_minutes: int) -> str | None:
    if interval_minutes == 1440:
        return None
    allowed = {1, 5, 15, 30, 60}
    if interval_minutes in allowed:
        return f"{interval_minutes}min"
    return None

def _fetch_stock_bars(api_key: str, symbol: str, interval_minutes: int) -> tuple[Dict, str, str]:
    interval_str = _interval_to_alpha(interval_minutes)
    if interval_str:
        function_name = "TIME_SERIES_INTRADAY"
        expected_key = f"Time Series ({interval_str})"
    else:
        function_name = "TIME_SERIES_DAILY"
        expected_key = "Time Series (Daily)"

    params = {
        "function": function_name,
        "symbol": symbol,
        "outputsize": "compact",
        "apikey": api_key,
    }
    if interval_str:
        params["interval"] = interval_str
    resp = requests.get(ALPHAVANTAGE_BASE_URL, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json(), resp.url, expected_key

def _fetch_fx_bars(api_key: str, from_symbol: str, to_symbol: str, interval_minutes: int) -> tuple[Dict, str, str]:
    interval_str = _interval_to_alpha(interval_minutes)
    if interval_str:
        function_name = "FX_INTRADAY"
        expected_key = f"Time Series FX ({interval_str})"
    else:
        function_name = "FX_DAILY"
        expected_key = "Time Series FX (Daily)"

    params = {
        "function": function_name,
        "from_symbol": from_symbol,
        "to_symbol": to_symbol,
        "outputsize": "compact",
        "apikey": api_key,
    }
    if interval_str:
        params["interval"] = interval_str
    resp = requests.get(ALPHAVANTAGE_BASE_URL, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json(), resp.url, expected_key

def _safe_float(v):
    try:
        return float(v)
    except:
        return None


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


def _log_event(
    session: Session,
    event_type: str,
    event_name: str,
    status: str,
    rows_affected: int | None,
    details: Dict | None,
    error_message: str | None,
    run_id: str,
) -> None:
    d = dict(details) if details else {}
    d.setdefault("scope", "AGG")
    d.setdefault("step_name", "ingestion")
    sql = f"""
        call MIP.APP.SP_LOG_EVENT(
            {_sql_literal(event_type)},
            {_sql_literal(event_name)},
            {_sql_literal(status)},
            {rows_affected if rows_affected is not None else 'null'},
            {_variant_literal(d)},
            {_sql_literal(error_message)},
            {_sql_literal(run_id)},
            null
        )
    """
    session.sql(sql).collect()


def _extract_api_message(json_data: Dict) -> str | None:
    for key in ("Error Message", "Note", "Information"):
        if key in json_data:
            return str(json_data.get(key))
    return None


def _is_rate_limit_message(message: str | None) -> bool:
    if not message:
        return False
    msg = message.lower()
    rate_limit_tokens = (
        "rate limit",
        "standard api rate limit",
        "please subscribe",
        "premium plans",
    )
    return any(token in msg for token in rate_limit_tokens)


def _rate_limit_message(json_data: Dict) -> str | None:
    info = json_data.get("Information")
    if info and _is_rate_limit_message(str(info)):
        return str(info)

    api_message = _extract_api_message(json_data)
    if _is_rate_limit_message(api_message):
        return api_message

    return None


def _require_time_series_key(json_data: Dict, expected_key: str, context: str) -> str | None:
    if expected_key in json_data:
        return None

    rate_limit_msg = _rate_limit_message(json_data)
    if rate_limit_msg:
        return rate_limit_msg

    api_message = _extract_api_message(json_data)
    available_keys = list(json_data.keys())
    extra_msg = f" API message: {api_message}" if api_message else ""
    raise ValueError(
        f"{context} missing expected '{expected_key}' in AlphaVantage response. "
        f"Available keys: {available_keys if available_keys else 'none'}.{extra_msg}"
    )

def _extract_stock_rows(
    json_data: Dict,
    symbol: str,
    interval_minutes: int,
    market_type: str,
) -> List[Dict]:
    rows: List[Dict] = []
    ts_key = next((k for k in json_data.keys() if k.startswith("Time Series")), None)
    if not ts_key:
        return rows

    ts_dict = json_data.get(ts_key, {})
    for ts_str, bar in ts_dict.items():
        try:
            try:
                ts_dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                ts_dt = datetime.strptime(ts_str, "%Y-%m-%d")
            bar_dict = dict(bar)

            rows.append({
                "TS": ts_dt,
                "SYMBOL": symbol,
                "SOURCE": "ALPHAVANTAGE",
                "MARKET_TYPE": market_type,
                "INTERVAL_MINUTES": interval_minutes,
                "OPEN": _safe_float(bar_dict.get("1. open")),
                "HIGH": _safe_float(bar_dict.get("2. high")),
                "LOW": _safe_float(bar_dict.get("3. low")),
                "CLOSE": _safe_float(bar_dict.get("4. close")),
                "VOLUME": _safe_float(bar_dict.get("5. volume")),
                "RAW": {
                    "symbol": symbol,
                    "interval_minutes": interval_minutes,
                    "bar": bar_dict,
                },
                # NEW: match table column INGESTED_AT
                "INGESTED_AT": datetime.utcnow(),
            })
        except:
            continue

    return rows

def _normalize_fx_pair(pair: str) -> tuple[str, str] | None:
    # Accept pairs like "EUR/USD", "EURUSD", "eur-usd", etc.
    cleaned = pair.replace(" ", "").replace("-", "/").replace("_", "/")
    if "/" in cleaned:
        parts = cleaned.split("/", 1)
        from_sym, to_sym = parts[0].upper(), parts[1].upper()
    elif len(cleaned) == 6:
        from_sym, to_sym = cleaned[:3].upper(), cleaned[3:].upper()
    else:
        return None

    if not (from_sym and to_sym):
        return None

    return from_sym, to_sym

def _extract_fx_rows_daily(json_data: Dict, pair: str, interval_minutes: int) -> List[Dict]:
    rows: List[Dict] = []
    ts_key = next((k for k in json_data.keys() if k.startswith("Time Series FX")), None)
    if not ts_key:
        return rows

    ts_dict = json_data.get(ts_key, {})
    for ts_str, bar in ts_dict.items():
        try:
            try:
                ts_dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                ts_dt = datetime.strptime(ts_str, "%Y-%m-%d")
            bar_dict = dict(bar)

            rows.append({
                "TS": ts_dt,
                "SYMBOL": pair,
                "SOURCE": "ALPHAVANTAGE",
                "MARKET_TYPE": "FX",
                "INTERVAL_MINUTES": interval_minutes,
                "OPEN": _safe_float(bar_dict.get("1. open")),
                "HIGH": _safe_float(bar_dict.get("2. high")),
                "LOW": _safe_float(bar_dict.get("3. low")),
                "CLOSE": _safe_float(bar_dict.get("4. close")),
                # FX endpoints do not include volume; keep the column nullable for schema parity
                "VOLUME": None,
                "RAW": {
                    "pair": pair,
                    "interval_minutes": interval_minutes,
                    "bar": bar_dict,
                },
                # NEW: match table column INGESTED_AT
                "INGESTED_AT": datetime.utcnow(),
            })
        except:
            continue

    return rows

def _load_ingest_universe(session: Session) -> Tuple[List[Dict], int, bool]:
    rows = session.sql(
        """
        select
            SYMBOL,
            MARKET_TYPE,
            INTERVAL_MINUTES,
            PRIORITY
        from MIP.APP.INGEST_UNIVERSE
        where coalesce(IS_ENABLED, true)
        order by PRIORITY desc, SYMBOL, MARKET_TYPE, INTERVAL_MINUTES
        """
    ).collect()
    total_rows = len(rows)
    return rows[:25], total_rows, total_rows > 25

def run(session: Session) -> Dict:
    run_id_row = session.sql(
        "select coalesce(nullif(current_query_tag(), ''), uuid_string()) as RUN_ID"
    ).collect()
    run_id = run_id_row[0]["RUN_ID"] if run_id_row else None

    _log_event(
        session,
        "INGESTION",
        "SP_INGEST_ALPHAVANTAGE_BARS",
        "START",
        None,
        None,
        None,
        run_id,
    )

    try:
        cfg = _get_config_map(session)

        api_key = cfg.get("ALPHAVANTAGE_API_KEY")
        if not api_key or api_key == "<PLACEHOLDER>":
            message = "ALPHAVANTAGE_API_KEY missing"
            result = {
                "status": "FAIL_MISSING_API_KEY",
                "rows_inserted": 0,
                "symbols_processed": 0,
                "symbols_skipped": 0,
                "skipped_rate_limit": 0,
                "rate_limit_hit": False,
                "error": message,
            }
            _log_event(
                session,
                "INGESTION",
                "SP_INGEST_ALPHAVANTAGE_BARS",
                "FAIL",
                0,
                {"reason": "missing_api_key", "result": result},
                message,
                run_id,
            )
            return result

        ingest_rows, enabled_total, truncated = _load_ingest_universe(session)
        if not ingest_rows:
            result = {
                "status": "SUCCESS",
                "rows_inserted": 0,
                "symbols_processed": 0,
                "symbols_skipped": 0,
                "skipped_rate_limit": 0,
                "rate_limit_hit": False,
                "symbols_enabled": enabled_total,
                "truncated": truncated,
                "symbols": [],
            }
            _log_event(
                session,
                "INGESTION",
                "SP_INGEST_ALPHAVANTAGE_BARS",
                "SUCCESS",
                0,
                result,
                None,
                run_id,
            )
            return result

        all_rows: List[Dict] = []
        symbol_results: List[Dict] = []

        request_urls: list[str] = []
        diagnostics: list[str] = []

        rate_limit_hit = False
        symbols_processed = 0
        symbols_skipped = 0
        skipped_rate_limit = 0

        for row in ingest_rows:
            market_type = str(row["MARKET_TYPE"]).upper()
            symbol = str(row["SYMBOL"]).upper()
            interval_minutes = int(row["INTERVAL_MINUTES"])

            symbol_result = {
                "symbol": symbol,
                "market_type": market_type,
                "interval_minutes": interval_minutes,
                "status": "PENDING",
                "skip_reason": None,
                "rows_extracted": 0,
            }

            if rate_limit_hit:
                symbol_result.update({
                    "status": "SKIPPED",
                    "skip_reason": "RATE_LIMIT",
                })
                symbol_results.append(symbol_result)
                symbols_skipped += 1
                skipped_rate_limit += 1
                diagnostics.append(f"{symbol}: skipped after rate limit detected")
                continue

            if _interval_to_alpha(interval_minutes) is None and interval_minutes != 1440:
                diagnostics.append(
                    f"{symbol}: unsupported interval_minutes {interval_minutes} (skipping)"
                )
                symbol_result.update({
                    "status": "SKIPPED",
                    "skip_reason": "UNSUPPORTED_INTERVAL",
                })
                symbol_results.append(symbol_result)
                symbols_skipped += 1
                continue

            symbols_processed += 1

            if market_type in ("STOCK", "ETF"):
                data, final_url, expected_key = _fetch_stock_bars(
                    api_key, symbol, interval_minutes
                )
                request_urls.append(
                    f"{market_type.lower()} {symbol} {interval_minutes}m: {final_url}"
                )
                rate_limit_msg = _require_time_series_key(
                    data, expected_key, f"{market_type} {symbol}"
                )
                if rate_limit_msg:
                    rate_limit_hit = True
                    symbol_result.update({
                        "status": "SKIPPED",
                        "skip_reason": "RATE_LIMIT",
                        "api_message": rate_limit_msg,
                    })
                    symbol_results.append(symbol_result)
                    symbols_skipped += 1
                    skipped_rate_limit += 1
                    diagnostics.append(f"{symbol}: rate limit detected")
                    continue
                api_msg = _extract_api_message(data)
                if api_msg:
                    diagnostics.append(f"{symbol}: {api_msg}")
                extracted_rows = _extract_stock_rows(
                    data, symbol, interval_minutes, market_type
                )
            elif market_type == "FX":
                parsed = _normalize_fx_pair(symbol)
                if not parsed:
                    diagnostics.append(f"{symbol}: unable to parse FX pair (skipping)")
                    symbol_result.update({
                        "status": "SKIPPED",
                        "skip_reason": "INVALID_FX_PAIR",
                    })
                    symbol_results.append(symbol_result)
                    symbols_skipped += 1
                    continue
                from_sym, to_sym = parsed
                normalized_pair = f"{from_sym}/{to_sym}"
                symbol_result["symbol"] = normalized_pair
                data, final_url, expected_key = _fetch_fx_bars(
                    api_key, from_sym, to_sym, interval_minutes
                )
                request_urls.append(
                    f"fx {normalized_pair} {interval_minutes}m: {final_url}"
                )
                rate_limit_msg = _require_time_series_key(
                    data, expected_key, f"FX pair {normalized_pair}"
                )
                if rate_limit_msg:
                    rate_limit_hit = True
                    symbol_result.update({
                        "status": "SKIPPED",
                        "skip_reason": "RATE_LIMIT",
                        "api_message": rate_limit_msg,
                    })
                    symbol_results.append(symbol_result)
                    symbols_skipped += 1
                    skipped_rate_limit += 1
                    diagnostics.append(f"{normalized_pair}: rate limit detected")
                    continue
                api_msg = _extract_api_message(data)
                if api_msg:
                    diagnostics.append(f"{normalized_pair}: {api_msg}")
                extracted_rows = _extract_fx_rows_daily(
                    data, normalized_pair, interval_minutes
                )
            else:
                diagnostics.append(
                    f"{symbol}: unknown market_type {market_type} (skipping)"
                )
                symbol_result.update({
                    "status": "SKIPPED",
                    "skip_reason": "UNKNOWN_MARKET_TYPE",
                })
                symbol_results.append(symbol_result)
                symbols_skipped += 1
                continue

            symbol_result.update({
                "status": "SUCCESS",
                "rows_extracted": len(extracted_rows),
            })
            symbol_results.append(symbol_result)
            all_rows.extend(extracted_rows)

            time.sleep(2)

        if rate_limit_hit:
            for pending_result in symbol_results:
                if pending_result["status"] == "PENDING":
                    pending_result.update({
                        "status": "SKIPPED",
                        "skip_reason": "RATE_LIMIT",
                    })

        rows_inserted = len(all_rows)

        if not all_rows:
            status = "SUCCESS_WITH_SKIPS" if symbols_skipped else "SUCCESS"
            result = {
                "status": status,
                "rows_inserted": 0,
                "symbols_processed": symbols_processed,
                "symbols_skipped": symbols_skipped,
                "skipped_rate_limit": skipped_rate_limit,
                "rate_limit_hit": rate_limit_hit,
                "symbols_enabled": enabled_total,
                "truncated": truncated,
                "symbols": symbol_results,
                "request_urls": request_urls,
                "diagnostics": diagnostics,
            }
            _log_event(
                session,
                "INGESTION",
                "SP_INGEST_ALPHAVANTAGE_BARS",
                status,
                0,
                result,
                None,
                run_id,
            )
            return result

        df = session.create_dataframe(all_rows)
        stage_table = "MIP.APP.STG_MARKET_BARS"
        df.write.mode("overwrite").save_as_table(stage_table, table_type="transient")

        merge_sql = f"""
            merge into MIP.MART.MARKET_BARS t
            using {stage_table} s
               on t.MARKET_TYPE = s.MARKET_TYPE
              and t.SYMBOL = s.SYMBOL
              and t.INTERVAL_MINUTES = s.INTERVAL_MINUTES
              and t.TS = s.TS
            when matched and (
                t.SOURCE IS DISTINCT FROM s.SOURCE
                or t.OPEN IS DISTINCT FROM s.OPEN
                or t.HIGH IS DISTINCT FROM s.HIGH
                or t.LOW IS DISTINCT FROM s.LOW
                or t.CLOSE IS DISTINCT FROM s.CLOSE
                or t.VOLUME IS DISTINCT FROM s.VOLUME
                or t.INGESTED_AT IS DISTINCT FROM s.INGESTED_AT
            ) then update set
                t.SOURCE = s.SOURCE,
                t.OPEN = s.OPEN,
                t.HIGH = s.HIGH,
                t.LOW = s.LOW,
                t.CLOSE = s.CLOSE,
                t.VOLUME = s.VOLUME,
                t.INGESTED_AT = s.INGESTED_AT
            when not matched then insert (
                TS,
                SYMBOL,
                SOURCE,
                MARKET_TYPE,
                INTERVAL_MINUTES,
                OPEN,
                HIGH,
                LOW,
                CLOSE,
                VOLUME,
                INGESTED_AT
            ) values (
                s.TS,
                s.SYMBOL,
                s.SOURCE,
                s.MARKET_TYPE,
                s.INTERVAL_MINUTES,
                s.OPEN,
                s.HIGH,
                s.LOW,
                s.CLOSE,
                s.VOLUME,
                s.INGESTED_AT
            )
        """
        session.sql(merge_sql).collect()
        session.sql(f"drop table if exists {stage_table}").collect()

        duplicate_rows = session.sql(
            """
            select
                MARKET_TYPE,
                SYMBOL,
                INTERVAL_MINUTES,
                TS,
                count(*) as CNT
            from MIP.MART.MARKET_BARS
            group by MARKET_TYPE, SYMBOL, INTERVAL_MINUTES, TS
            having count(*) > 1
            order by CNT desc
            limit 5
            """
        ).collect()

        if duplicate_rows:
            sample_keys = "; ".join(
                f"{row['MARKET_TYPE']} {row['SYMBOL']} {row['INTERVAL_MINUTES']} {row['TS']} (cnt={row['CNT']})"
                for row in duplicate_rows
            )
            message = (
                "Ingestion failed guardrail: duplicate keys found in MIP.MART.MARKET_BARS. "
                f"Sample keys: {sample_keys}"
            )
            _log_event(
                session,
                "INGESTION",
                "SP_INGEST_ALPHAVANTAGE_BARS",
                "FAIL",
                rows_inserted,
                {
                    "symbols_processed": symbols_processed,
                    "symbols_skipped": symbols_skipped,
                    "skipped_rate_limit": skipped_rate_limit,
                    "rate_limit_hit": rate_limit_hit,
                },
                message,
                run_id,
            )
            return {
                "status": "FAIL_DUPLICATE_KEYS",
                "rows_inserted": rows_inserted,
                "symbols_processed": symbols_processed,
                "symbols_skipped": symbols_skipped,
                "skipped_rate_limit": skipped_rate_limit,
                "rate_limit_hit": rate_limit_hit,
                "error": message,
                "symbols": symbol_results,
            }

        stock_count = sum(
            1 for row in all_rows if row.get("MARKET_TYPE") in ("STOCK", "ETF")
        )
        fx_count = sum(1 for row in all_rows if row.get("MARKET_TYPE") == "FX")

        status = "SUCCESS_WITH_SKIPS" if symbols_skipped else "SUCCESS"
        result = {
            "status": status,
            "rows_inserted": rows_inserted,
            "symbols_processed": symbols_processed,
            "symbols_skipped": symbols_skipped,
            "skipped_rate_limit": skipped_rate_limit,
            "rate_limit_hit": rate_limit_hit,
            "symbols_enabled": enabled_total,
            "truncated": truncated,
            "stock_rows": stock_count,
            "fx_rows": fx_count,
            "symbols": symbol_results,
            "request_urls": request_urls,
            "diagnostics": diagnostics,
        }

        _log_event(
            session,
            "INGESTION",
            "SP_INGEST_ALPHAVANTAGE_BARS",
            status,
            rows_inserted,
            result,
            None,
            run_id,
        )

        return result
    except Exception as exc:
        _log_event(
            session,
            "INGESTION",
            "SP_INGEST_ALPHAVANTAGE_BARS",
            "FAIL",
            None,
            None,
            str(exc),
            run_id,
        )
        raise
$$;
