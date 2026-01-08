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
returns varchar
language python
runtime_version = '3.12'
packages = ('requests', 'snowflake-snowpark-python')
external_access_integrations = (MIP_ALPHA_EXTERNAL_ACCESS)
handler = 'run'
as
$$
import requests
import time
from datetime import datetime
from typing import List, Dict
from snowflake.snowpark import Session

ALPHAVANTAGE_BASE_URL = "https://www.alphavantage.co/query"

def _get_config_map(session: Session) -> Dict[str, str]:
    cfg_df = session.table("MIP.APP.APP_CONFIG")
    rows = cfg_df.collect()
    return {row["CONFIG_KEY"]: row["CONFIG_VALUE"] for row in rows}

def _parse_symbol_list(raw: str) -> List[str]:
    if not raw:
        return []
    return [s.strip() for s in raw.split(",") if s.strip()]

def _fetch_stock_bars(api_key: str, symbol: str, interval_str: str | None) -> tuple[Dict, str]:
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "outputsize": "compact",
        "apikey": api_key,
    }
    if interval_str:
        params["interval"] = interval_str
    resp = requests.get(ALPHAVANTAGE_BASE_URL, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json(), resp.url

def _fetch_fx_daily(api_key: str, from_symbol: str, to_symbol: str) -> tuple[Dict, str]:
    params = {
        "function": "FX_DAILY",
        "from_symbol": from_symbol,
        "to_symbol": to_symbol,
        "outputsize": "compact",
        "apikey": api_key,
    }
    resp = requests.get(ALPHAVANTAGE_BASE_URL, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json(), resp.url

def _safe_float(v):
    try:
        return float(v)
    except:
        return None


def _extract_api_message(json_data: Dict) -> str | None:
    for key in ("Error Message", "Note", "Information"):
        if key in json_data:
            return str(json_data.get(key))
    return None


def _require_time_series_key(json_data: Dict, expected_key: str, context: str) -> None:
    if expected_key in json_data:
        return

    api_message = _extract_api_message(json_data)
    available_keys = list(json_data.keys())
    extra_msg = f" API message: {api_message}" if api_message else ""
    raise ValueError(
        f"{context} missing expected '{expected_key}' in AlphaVantage response. "
        f"Available keys: {available_keys if available_keys else 'none'}.{extra_msg}"
    )

def _extract_stock_rows(json_data: Dict, symbol: str, interval_minutes: int) -> List[Dict]:
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
                "MARKET_TYPE": "STOCK",
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

def run(session: Session) -> str:
    cfg = _get_config_map(session)

    api_key = cfg.get("ALPHAVANTAGE_API_KEY")
    if not api_key or api_key == "<PLACEHOLDER>":
        return "ALPHAVANTAGE_API_KEY missing"

    stock_symbols = _parse_symbol_list(cfg.get("DEFAULT_STOCK_SYMBOLS"))
    fx_pairs = _parse_symbol_list(cfg.get("DEFAULT_FX_PAIRS"))

    stock_interval_minutes = 1440  # daily
    stock_interval_str = None
    fx_interval_minutes = 1440  # daily

    all_rows: List[Dict] = []

    request_urls: list[str] = []
    diagnostics: list[str] = []

    # STOCKS
    for symbol in stock_symbols:
        data, final_url = _fetch_stock_bars(api_key, symbol, stock_interval_str)
        request_urls.append(f"stock {symbol}: {final_url}")
        _require_time_series_key(data, "Time Series (Daily)", f"Symbol {symbol}")
        api_msg = _extract_api_message(data)
        if api_msg:
            diagnostics.append(f"{symbol}: {api_msg}")
        all_rows.extend(_extract_stock_rows(data, symbol, stock_interval_minutes))
        time.sleep(2)

    # FX DAILY
    for raw_pair in fx_pairs:
        parsed = _normalize_fx_pair(raw_pair)
        if not parsed:
            continue
        from_sym, to_sym = parsed
        normalized_pair = f"{from_sym}/{to_sym}"
        data, final_url = _fetch_fx_daily(api_key, from_sym, to_sym)
        request_urls.append(f"fx {normalized_pair}: {final_url}")
        api_msg = _extract_api_message(data)
        if api_msg:
            diagnostics.append(f"{normalized_pair}: {api_msg}")
        _require_time_series_key(
            data,
            "Time Series FX (Daily)",
            f"FX pair {normalized_pair}",
        )

        all_rows.extend(_extract_fx_rows_daily(data, normalized_pair, fx_interval_minutes))
        time.sleep(2)

    if not all_rows:
        return "Ingestion complete: 0 rows."

    df = session.create_dataframe(all_rows)
    stage_table = "MIP.APP.STG_MARKET_BARS"
    df.write.mode("overwrite").save_as_table(stage_table, table_type="temporary")

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
        return (
            "Ingestion failed guardrail: duplicate keys found in MIP.MART.MARKET_BARS. "
            f"Sample keys: {sample_keys}"
        )

    stock_count = sum(1 for row in all_rows if row.get("MARKET_TYPE") == "STOCK")
    fx_count = sum(1 for row in all_rows if row.get("MARKET_TYPE") == "FX")

    url_info = " | ".join(request_urls) if request_urls else "no requests made"
    diag_info = " | ".join(diagnostics) if diagnostics else "no API warnings"

    return (
        "Ingestion complete: "
        f"merged {len(all_rows)} rows (stocks: {stock_count}, fx: {fx_count}). "
        f"URLs: {url_info}. Diagnostics: {diag_info}"
    )
$$;
