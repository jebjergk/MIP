-- 375_sp_map_news_symbols.sql
-- Purpose: Phase 3 deterministic symbol mapping for news items.
-- Contract:
--   - Deterministic rules only (no ML/LLM sentiment).
--   - Precision-first thresholding from APP_CONFIG.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.NEWS.SP_MAP_NEWS_SYMBOLS(
    P_NEWS_ID_FILTER string default null
)
returns variant
language python
runtime_version = '3.12'
packages = ('snowflake-snowpark-python')
handler = 'run'
as
$$
import json
import re
from datetime import datetime, timezone
import hashlib

def _utc_now():
    return datetime.now(timezone.utc).replace(tzinfo=None)

def _sha(s):
    return hashlib.sha256((s or "").encode("utf-8")).hexdigest()

def _text(v):
    if v is None:
        return ""
    return str(v)

def _upper(v):
    return _text(v).upper()

def _contains_token(text_up, token_up):
    if not token_up:
        return False
    pattern = r"(?<![A-Z0-9])" + re.escape(token_up) + r"(?![A-Z0-9])"
    return re.search(pattern, text_up) is not None

def _contains_phrase(text_up, phrase_up):
    if not phrase_up:
        return False
    phrase_up = re.sub(r"\s+", " ", phrase_up.strip())
    text_norm = re.sub(r"\s+", " ", text_up)
    pattern = r"(?<![A-Z0-9])" + re.escape(phrase_up) + r"(?![A-Z0-9])"
    return re.search(pattern, text_norm) is not None

def run(session, p_news_id_filter=None):
    cfg_rows = session.sql("""
        select CONFIG_KEY, CONFIG_VALUE
        from MIP.APP.APP_CONFIG
        where CONFIG_KEY in ('NEWS_MATCH_CONFIDENCE_MIN', 'NEWS_ENABLED')
    """).collect()
    cfg = {str(r["CONFIG_KEY"]): r["CONFIG_VALUE"] for r in cfg_rows}
    conf_min = float(cfg.get("NEWS_MATCH_CONFIDENCE_MIN", "0.70"))
    news_enabled = str(cfg.get("NEWS_ENABLED", "false")).lower() == "true"

    # Mapping can still run when NEWS_ENABLED=false for QA/testing bootstrap.
    raw_where = ""
    if p_news_id_filter:
        esc = str(p_news_id_filter).replace("'", "''")
        raw_where = f" where NEWS_ID = '{esc}' "

    raw_rows = session.sql(f"""
        select NEWS_ID, TITLE, SUMMARY, URL, upper(SYMBOL_HINT) as SYMBOL_HINT, upper(MARKET_TYPE_HINT) as MARKET_TYPE_HINT
        from MIP.NEWS.NEWS_RAW
        {raw_where}
    """).collect()
    if not raw_rows:
        return {
            "status": "SUCCESS",
            "news_enabled": news_enabled,
            "rows_scanned": 0,
            "mapped_rows_staged": 0,
            "mapped_rows_merged": 0,
        }

    universe_rows = session.sql("""
        select distinct upper(SYMBOL) as SYMBOL, upper(MARKET_TYPE) as MARKET_TYPE
        from MIP.APP.INGEST_UNIVERSE
        where coalesce(IS_ENABLED, true)
          and INTERVAL_MINUTES = 1440
    """).collect()
    symbol_pairs = [(str(r["SYMBOL"]), str(r["MARKET_TYPE"])) for r in universe_rows]

    alias_rows = session.sql("""
        select upper(SYMBOL) as SYMBOL, upper(MARKET_TYPE) as MARKET_TYPE, upper(ALIAS) as ALIAS, upper(ALIAS_TYPE) as ALIAS_TYPE
        from MIP.NEWS.SYMBOL_ALIAS_DICT
        where coalesce(IS_ACTIVE, true)
    """).collect()

    alias_by_symbol = {}
    for a in alias_rows:
        key = (str(a["SYMBOL"]), str(a["MARKET_TYPE"]))
        alias_by_symbol.setdefault(key, []).append((str(a["ALIAS"]), str(a["ALIAS_TYPE"])))

    staged = []
    seen = set()
    now_ts = _utc_now()
    run_id = _sha(now_ts.isoformat())[:32]

    for row in raw_rows:
        news_id = str(row["NEWS_ID"])
        text_up = _upper(row["TITLE"]) + " " + _upper(row["SUMMARY"]) + " " + _upper(row["URL"])
        hinted_symbol = str(row["SYMBOL_HINT"]) if row["SYMBOL_HINT"] is not None else None
        hinted_market = str(row["MARKET_TYPE_HINT"]) if row["MARKET_TYPE_HINT"] is not None else None

        # 0) Fast-path deterministic symbol hint from ticker RSS subscriptions.
        if hinted_symbol and hinted_market:
            key = (news_id, hinted_symbol, hinted_market, "subscription_hint")
            if key not in seen:
                seen.add(key)
                staged.append({
                    "NEWS_ID": news_id,
                    "SYMBOL": hinted_symbol,
                    "MARKET_TYPE": hinted_market,
                    "MATCH_METHOD": "subscription_hint",
                    "MATCH_CONFIDENCE": 0.99,
                    "CREATED_AT": now_ts,
                    "RUN_ID": run_id,
                })

        for symbol, market_type in symbol_pairs:
            # 1) Ticker regex path
            matched = False
            if f"${symbol}" in text_up:
                method = "ticker_regex"
                conf = 0.95
                matched = True
            elif len(symbol) >= 3 and _contains_token(text_up, symbol):
                method = "ticker_regex"
                conf = 0.95
                matched = True

            if matched and conf >= conf_min:
                key = (news_id, symbol, market_type, method)
                if key not in seen:
                    seen.add(key)
                    staged.append({
                        "NEWS_ID": news_id,
                        "SYMBOL": symbol,
                        "MARKET_TYPE": market_type,
                        "MATCH_METHOD": method,
                        "MATCH_CONFIDENCE": conf,
                        "CREATED_AT": now_ts,
                        "RUN_ID": run_id,
                    })

            # 2) Alias dictionary path
            for alias, alias_type in alias_by_symbol.get((symbol, market_type), []):
                if alias == symbol:
                    # already covered by ticker path
                    continue
                if not _contains_phrase(text_up, alias):
                    continue

                if alias_type == "COMPANY_NAME":
                    method = "company_name_match"
                    conf = 0.72
                else:
                    method = "alias_dict"
                    conf = 0.85

                if conf < conf_min:
                    continue

                key = (news_id, symbol, market_type, method)
                if key in seen:
                    continue
                seen.add(key)
                staged.append({
                    "NEWS_ID": news_id,
                    "SYMBOL": symbol,
                    "MARKET_TYPE": market_type,
                    "MATCH_METHOD": method,
                    "MATCH_CONFIDENCE": conf,
                    "CREATED_AT": now_ts,
                    "RUN_ID": run_id,
                })

    if not staged:
        return {
            "status": "SUCCESS",
            "news_enabled": news_enabled,
            "rows_scanned": len(raw_rows),
            "mapped_rows_staged": 0,
            "mapped_rows_merged": 0,
        }

    stage_table = f"MIP.NEWS.STG_NEWS_SYMBOL_MAP_{_sha(now_ts.isoformat())[:10]}"
    session.create_dataframe(staged).write.mode("overwrite").save_as_table(stage_table, table_type="transient")

    before_cnt = session.sql("select count(*) from MIP.NEWS.NEWS_SYMBOL_MAP").collect()[0][0]
    session.sql(f"""
        merge into MIP.NEWS.NEWS_SYMBOL_MAP t
        using {stage_table} s
           on t.NEWS_ID = s.NEWS_ID
          and t.SYMBOL = s.SYMBOL
          and t.MARKET_TYPE = s.MARKET_TYPE
          and t.MATCH_METHOD = s.MATCH_METHOD
        when matched then update set
          t.MATCH_CONFIDENCE = s.MATCH_CONFIDENCE,
          t.CREATED_AT = s.CREATED_AT,
          t.RUN_ID = s.RUN_ID
        when not matched then insert (
            NEWS_ID, SYMBOL, MARKET_TYPE, MATCH_METHOD, MATCH_CONFIDENCE, CREATED_AT, RUN_ID
        ) values (
            s.NEWS_ID, s.SYMBOL, s.MARKET_TYPE, s.MATCH_METHOD, s.MATCH_CONFIDENCE, s.CREATED_AT, s.RUN_ID
        )
    """).collect()
    after_cnt = session.sql("select count(*) from MIP.NEWS.NEWS_SYMBOL_MAP").collect()[0][0]
    session.sql(f"drop table if exists {stage_table}").collect()

    return {
        "status": "SUCCESS",
        "news_enabled": news_enabled,
        "rows_scanned": len(raw_rows),
        "mapped_rows_staged": len(staged),
        "mapped_rows_merged": int(after_cnt - before_cnt),
        "run_id": run_id,
    }
$$;
