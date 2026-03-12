-- 373_sp_ingest_rss_news.sql
-- Purpose: Phase 2 RSS ingestion + deterministic dedup materialization.
-- Architecture contract:
--   - News is decision-time context only.
--   - No signal/training/trust object references MIP.NEWS.
-- v1 content contract:
--   - full_text_optional is always null.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.NEWS.SP_INGEST_RSS_NEWS(
    P_TEST_MODE boolean default false,
    P_SOURCE_LIMIT number default null
)
returns variant
language python
runtime_version = '3.12'
packages = ('requests', 'feedparser', 'snowflake-snowpark-python')
external_access_integrations = (MIP_ALPHA_EXTERNAL_ACCESS)
handler = 'run'
as
$$
import hashlib
import json
import re
import time
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

import feedparser
import requests

TRACKING_PARAMS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "gclid", "fbclid", "mc_cid", "mc_eid"
}

def _utc_now():
    return datetime.now(timezone.utc).replace(tzinfo=None)

def _sha256(s):
    return hashlib.sha256((s or "").encode("utf-8")).hexdigest()

def _clean_text(s):
    if s is None:
        return None
    s = re.sub(r"\s+", " ", str(s)).strip()
    return s if s else None

def _canonicalize_url(url):
    if not url:
        return None
    try:
        p = urlparse(url.strip())
        query_pairs = parse_qsl(p.query, keep_blank_values=True)
        filtered = [(k, v) for (k, v) in query_pairs if k.lower() not in TRACKING_PARAMS]
        filtered.sort(key=lambda kv: (kv[0], kv[1]))
        query = urlencode(filtered, doseq=True)
        path = p.path.rstrip("/") or "/"
        return urlunparse((p.scheme.lower(), p.netloc.lower(), path, "", query, ""))
    except Exception:
        return _clean_text(url)

def _parse_published_ts(entry):
    for key in ("published_parsed", "updated_parsed"):
        v = entry.get(key)
        if v:
            try:
                return datetime(*v[:6], tzinfo=timezone.utc).replace(tzinfo=None)
            except Exception:
                pass
    for key in ("published", "updated"):
        raw = entry.get(key)
        if raw:
            try:
                dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc).replace(tzinfo=None)
            except Exception:
                pass
    return _utc_now()

def _load_config(session):
    rows = session.sql("""
        select CONFIG_KEY, CONFIG_VALUE
        from MIP.APP.APP_CONFIG
        where CONFIG_KEY like 'NEWS_%'
    """).collect()
    out = {}
    for r in rows:
        out[str(r["CONFIG_KEY"])] = r["CONFIG_VALUE"]
    return out

def _safe_int(v, default_value):
    try:
        return int(v)
    except Exception:
        return int(default_value)

def _load_global_sources(session, source_ids, source_limit):
    where = """
        where ALLOWED_FLAG = true
          and IS_ACTIVE = true
          and coalesce(ENABLED_FLAG, true)
          and upper(coalesce(SOURCE_TYPE, 'GLOBAL_RSS')) = 'GLOBAL_RSS'
    """
    if source_ids:
        escaped = ",".join(["'" + str(sid).replace("'", "''") + "'" for sid in source_ids])
        where += f" and SOURCE_ID in ({escaped})"
    limit_clause = f" limit {int(source_limit)}" if source_limit is not None else ""
    return session.sql(f"""
        select
            SOURCE_ID,
            SOURCE_NAME,
            FEED_URL,
            upper(coalesce(SYMBOL_SCOPE, 'ALL')) as SYMBOL_SCOPE,
            coalesce(POLL_MINUTES, INGEST_CADENCE_MINUTES, 30) as POLL_MINUTES,
            upper(coalesce(SOURCE_TYPE, 'GLOBAL_RSS')) as SOURCE_TYPE
        from MIP.NEWS.NEWS_SOURCE_REGISTRY
        {where}
        order by SOURCE_ID
        {limit_clause}
    """).collect()

def _load_ticker_subscriptions(session, source_ids, source_limit):
    where = """
        where r.ALLOWED_FLAG = true
          and r.IS_ACTIVE = true
          and coalesce(r.ENABLED_FLAG, true)
          and upper(coalesce(r.SOURCE_TYPE, 'GLOBAL_RSS')) = 'TICKER_RSS'
          and coalesce(s.ENABLED_FLAG, true)
    """
    if source_ids:
        escaped = ",".join(["'" + str(sid).replace("'", "''") + "'" for sid in source_ids])
        where += f" and r.SOURCE_ID in ({escaped})"
    limit_clause = f" limit {int(source_limit)}" if source_limit is not None else ""
    return session.sql(f"""
        select
            s.SUBSCRIPTION_ID,
            s.SOURCE_ID,
            r.SOURCE_NAME,
            s.SYMBOL,
            s.MARKET_TYPE,
            s.RSS_URL_RESOLVED as FEED_URL,
            upper(coalesce(r.SYMBOL_SCOPE, 'ALL')) as SYMBOL_SCOPE,
            coalesce(r.POLL_MINUTES, r.INGEST_CADENCE_MINUTES, 120) as POLL_MINUTES,
            upper(coalesce(r.SOURCE_TYPE, 'TICKER_RSS')) as SOURCE_TYPE
        from MIP.NEWS.NEWS_SOURCE_SUBSCRIPTIONS s
        join MIP.NEWS.NEWS_SOURCE_REGISTRY r
          on r.SOURCE_ID = s.SOURCE_ID
        {where}
        order by s.SOURCE_ID, s.SYMBOL, s.MARKET_TYPE
        {limit_clause}
    """).collect()

def _load_last_ingested_by_source(session):
    rows = session.sql("""
        select SOURCE_ID, max(INGESTED_AT) as LAST_INGESTED_AT
        from MIP.NEWS.NEWS_RAW
        group by SOURCE_ID
    """).collect()
    out = {}
    for r in rows:
        out[str(r["SOURCE_ID"])] = r["LAST_INGESTED_AT"]
    return out

def _load_last_ingested_by_subscription(session):
    rows = session.sql("""
        select SUBSCRIPTION_ID, max(INGESTED_AT) as LAST_INGESTED_AT
        from MIP.NEWS.NEWS_RAW
        where SUBSCRIPTION_ID is not null
        group by SUBSCRIPTION_ID
    """).collect()
    out = {}
    for r in rows:
        out[str(r["SUBSCRIPTION_ID"])] = r["LAST_INGESTED_AT"]
    return out

def _should_poll(last_ingested_at, poll_minutes, now_ts):
    if last_ingested_at is None:
        return True
    try:
        age_minutes = (now_ts - last_ingested_at).total_seconds() / 60.0
    except Exception:
        return True
    return age_minutes >= max(1, int(poll_minutes))

def _mock_feed_entries(source_id, source_name, feed_url):
    # Deterministic mock rows for idempotency checks.
    day = _utc_now().strftime("%Y-%m-%d")
    if source_id == "FED_RSS":
        base = [
            {
                "title": "Federal Reserve note: USD/JPY volatility rises",
                "summary": "Macro headline mentions USD/JPY and EUR/USD in risk commentary.",
                "url": f"{feed_url.rstrip('/')}/mock-item-a",
                "published": f"{day}T10:00:00Z",
                "language": "en",
            },
            {
                "title": "Fed policy remarks keep USD/CAD and USD/CHF active",
                "summary": "Short-term FX context for USD/CAD and USD/CHF.",
                "url": f"{feed_url.rstrip('/')}/mock-item-b",
                "published": f"{day}T10:05:00Z",
                "language": "en",
            },
        ]
    elif source_id == "GLOBENEWSWIRE_RSS":
        base = [
            {
                "title": "Apple announces update, ticker $AAPL in release",
                "summary": "Apple and Microsoft collaboration update references AAPL and MSFT.",
                "url": f"{feed_url.rstrip('/')}/mock-item-a",
                "published": f"{day}T10:00:00Z",
                "language": "en",
            },
            {
                "title": "NVIDIA and Tesla supplier note",
                "summary": "NVIDIA (NVDA) demand and Tesla (TSLA) production context.",
                "url": f"{feed_url.rstrip('/')}/mock-item-b",
                "published": f"{day}T10:05:00Z",
                "language": "en",
            },
        ]
    else:
        base = [
            {
                "title": f"{source_name} market pulse for SPY and QQQ",
                "summary": "ETF context cites SPY and NASDAQ 100 ETF.",
                "url": f"{feed_url.rstrip('/')}/mock-item-a",
                "published": f"{day}T10:00:00Z",
                "language": "en",
            },
            {
                "title": f"{source_name} broad market mention of AMZN and GOOGL",
                "summary": "Company name aliases Amazon and Alphabet appear in this item.",
                "url": f"{feed_url.rstrip('/')}/mock-item-b",
                "published": f"{day}T10:05:00Z",
                "language": "en",
            },
        ]
    return base

def _fetch_rss_entries(feed_url, max_attempts=2, connect_timeout_sec=5, read_timeout_sec=12):
    headers = {
        "User-Agent": "MIP-News-Ingest/1.0 (+https://mip.local)"
    }
    last_error = None
    max_attempts = max(1, int(max_attempts))
    connect_timeout_sec = max(1, int(connect_timeout_sec))
    read_timeout_sec = max(2, int(read_timeout_sec))
    for attempt in range(1, max_attempts + 1):
        try:
            r = requests.get(feed_url, timeout=(connect_timeout_sec, read_timeout_sec), headers=headers)
            r.raise_for_status()
            parsed = feedparser.parse(r.content)
            entries = parsed.entries or []
            return entries
        except Exception as exc:
            last_error = exc
            if attempt < max_attempts:
                time.sleep(min(3, attempt))
    raise RuntimeError(f"FETCH_FAILED after {max_attempts} attempts: {last_error}")

def _normalize_entry(
    source_id,
    source_name,
    source_type,
    subscription_id,
    symbol_hint,
    market_type_hint,
    entry,
    snapshot_ts
):
    title = _clean_text(entry.get("title"))
    summary = _clean_text(entry.get("summary") or entry.get("description"))
    raw_url = _clean_text(entry.get("link") or entry.get("url"))
    canonical_url = _canonicalize_url(raw_url)
    if not canonical_url:
        return None, "MISSING_URL"

    published_at = _parse_published_ts(entry)
    language = _clean_text(entry.get("language") or "en")
    title_norm = _clean_text(title) or ""
    summary_norm = _clean_text(summary) or ""
    content_fingerprint = f"{title_norm}|{summary_norm}|{canonical_url}"
    content_hash = _sha256(content_fingerprint)
    canonical_url_hash = _sha256(canonical_url)
    id_scope = subscription_id if subscription_id else source_id
    news_id = _sha256(f"{id_scope}|{canonical_url_hash}|{published_at.isoformat()}|{title_norm}")
    dedup_cluster_id = _sha256(f"URL|{canonical_url_hash}")

    row = {
        "NEWS_ID": news_id,
        "SOURCE_ID": source_id,
        "SOURCE_NAME": source_name,
        "PUBLISHED_AT": published_at,
        "INGESTED_AT": _utc_now(),
        "TITLE": title,
        "SUMMARY": summary,
        "FULL_TEXT_OPTIONAL": None,
        "URL": canonical_url,
        "LANGUAGE": language,
        "RAW_PAYLOAD_VARIANT": dict(entry),
        "CONTENT_HASH": content_hash,
        "CANONICAL_URL_HASH": canonical_url_hash,
        "DEDUP_CLUSTER_ID": dedup_cluster_id,
        "PARSE_STATUS": "SUCCESS",
        "ERROR_REASON": None,
        "SNAPSHOT_TS": snapshot_ts,
        "RUN_ID": _sha256(f"{snapshot_ts.isoformat()}|{source_id}")[:32],
        "SUBSCRIPTION_ID": subscription_id,
        "SYMBOL_HINT": symbol_hint,
        "MARKET_TYPE_HINT": market_type_hint,
        "SOURCE_TYPE": source_type,
    }
    return row, None

def run(session, p_test_mode=False, p_source_limit=None):
    cfg = _load_config(session)
    news_enabled = str(cfg.get("NEWS_ENABLED", "false")).lower() == "true"
    max_targets_per_run = _safe_int(cfg.get("NEWS_MAX_TARGETS_PER_RUN", "20"), 20)
    fetch_max_attempts = _safe_int(cfg.get("NEWS_FETCH_MAX_ATTEMPTS", "2"), 2)
    fetch_connect_timeout_sec = _safe_int(cfg.get("NEWS_FETCH_CONNECT_TIMEOUT_SEC", "5"), 5)
    fetch_read_timeout_sec = _safe_int(cfg.get("NEWS_FETCH_READ_TIMEOUT_SEC", "12"), 12)
    fetch_budget_seconds = _safe_int(cfg.get("NEWS_FETCH_BUDGET_SECONDS", "240"), 240)
    max_targets_per_run = max(1, max_targets_per_run)
    fetch_max_attempts = max(1, fetch_max_attempts)
    fetch_connect_timeout_sec = max(1, fetch_connect_timeout_sec)
    fetch_read_timeout_sec = max(2, fetch_read_timeout_sec)
    fetch_budget_seconds = max(30, fetch_budget_seconds)
    source_cfg = cfg.get("NEWS_SOURCES")
    try:
        source_ids = json.loads(source_cfg) if source_cfg else None
        if not isinstance(source_ids, list):
            source_ids = None
    except Exception:
        source_ids = None

    if (not news_enabled) and (not p_test_mode):
        return {
            "status": "SKIPPED_DISABLED",
            "test_mode": bool(p_test_mode),
            "news_enabled": news_enabled,
            "sources_considered": 0,
            "rows_staged": 0,
            "rows_inserted": 0,
        }

    snapshot_ts = _utc_now()
    default_global_poll = _safe_int(cfg.get("NEWS_GLOBAL_POLL_MINUTES", "30"), 30)
    default_ticker_poll = _safe_int(cfg.get("NEWS_TICKER_POLL_MINUTES", "120"), 120)

    global_rows = _load_global_sources(session, source_ids, p_source_limit)
    ticker_rows = _load_ticker_subscriptions(session, source_ids, p_source_limit)

    if not global_rows and not ticker_rows:
        return {
            "status": "SUCCESS",
            "test_mode": bool(p_test_mode),
            "news_enabled": news_enabled,
            "sources_considered": 0,
            "ticker_subscriptions_considered": 0,
            "rows_staged": 0,
            "rows_inserted": 0,
            "dedup_clusters_upserted": 0,
            "errors": [],
        }

    last_by_source = _load_last_ingested_by_source(session)
    last_by_sub = _load_last_ingested_by_subscription(session)

    targets = []
    for s in global_rows:
        source_id = str(s["SOURCE_ID"])
        poll_minutes = _safe_int(s["POLL_MINUTES"], default_global_poll)
        last_ingested_at = last_by_source.get(source_id)
        if (not p_test_mode) and (not _should_poll(last_by_source.get(source_id), poll_minutes, snapshot_ts)):
            continue
        targets.append({
            "target_type": "GLOBAL",
            "source_id": source_id,
            "source_name": str(s["SOURCE_NAME"]),
            "feed_url": str(s["FEED_URL"]),
            "source_type": str(s["SOURCE_TYPE"]),
            "subscription_id": None,
            "symbol_hint": None,
            "market_type_hint": None,
            "poll_minutes": poll_minutes,
            "last_ingested_at": last_ingested_at,
        })

    for s in ticker_rows:
        subscription_id = str(s["SUBSCRIPTION_ID"])
        poll_minutes = _safe_int(s["POLL_MINUTES"], default_ticker_poll)
        last_ingested_at = last_by_sub.get(subscription_id)
        if (not p_test_mode) and (not _should_poll(last_by_sub.get(subscription_id), poll_minutes, snapshot_ts)):
            continue
        targets.append({
            "target_type": "TICKER",
            "source_id": str(s["SOURCE_ID"]),
            "source_name": str(s["SOURCE_NAME"]),
            "feed_url": str(s["FEED_URL"]),
            "source_type": str(s["SOURCE_TYPE"]),
            "subscription_id": subscription_id,
            "symbol_hint": str(s["SYMBOL"]),
            "market_type_hint": str(s["MARKET_TYPE"]),
            "poll_minutes": poll_minutes,
            "last_ingested_at": last_ingested_at,
        })

    def _target_sort_key(t):
        last_ts = t.get("last_ingested_at")
        return (last_ts is not None, last_ts or datetime(1970, 1, 1))

    targets.sort(key=_target_sort_key)
    if (not p_test_mode) and max_targets_per_run and len(targets) > max_targets_per_run:
        targets = targets[:max_targets_per_run]

    if not targets:
        return {
            "status": "SUCCESS_SKIPPED_POLL_WINDOW",
            "test_mode": bool(p_test_mode),
            "news_enabled": news_enabled,
            "sources_considered": len(global_rows),
            "ticker_subscriptions_considered": len(ticker_rows),
            "targets_polled": 0,
            "rows_staged": 0,
            "rows_inserted": 0,
            "dedup_clusters_upserted": 0,
            "errors": [],
        }

    normalized = []
    errors = []
    per_source = []
    skipped_due_to_budget = 0
    fetch_started_at = time.time()

    for t in targets:
        if (not p_test_mode) and ((time.time() - fetch_started_at) >= fetch_budget_seconds):
            skipped_due_to_budget += 1
            continue
        source_id = t["source_id"]
        source_name = t["source_name"]
        feed_url = t["feed_url"]
        source_type = t["source_type"]
        subscription_id = t["subscription_id"]
        symbol_hint = t["symbol_hint"]
        market_type_hint = t["market_type_hint"]
        entry_count = 0
        staged_count = 0
        try:
            if p_test_mode:
                entries = _mock_feed_entries(source_id, source_name, feed_url)
            else:
                entries = _fetch_rss_entries(
                    feed_url,
                    max_attempts=fetch_max_attempts,
                    connect_timeout_sec=fetch_connect_timeout_sec,
                    read_timeout_sec=fetch_read_timeout_sec,
                )
            entry_count = len(entries)

            for e in entries:
                row, err = _normalize_entry(
                    source_id,
                    source_name,
                    source_type,
                    subscription_id,
                    symbol_hint,
                    market_type_hint,
                    e,
                    snapshot_ts
                )
                if row is not None:
                    normalized.append(row)
                    staged_count += 1
                elif err:
                    errors.append({
                        "source_id": source_id,
                        "subscription_id": subscription_id,
                        "symbol_hint": symbol_hint,
                        "error": err
                    })

            per_source.append({
                "source_id": source_id,
                "source_type": source_type,
                "subscription_id": subscription_id,
                "symbol_hint": symbol_hint,
                "entries_seen": entry_count,
                "rows_staged": staged_count,
                "status": "SUCCESS",
            })
        except Exception as exc:
            errors.append({
                "source_id": source_id,
                "source_type": source_type,
                "subscription_id": subscription_id,
                "symbol_hint": symbol_hint,
                "error": str(exc)
            })
            per_source.append({
                "source_id": source_id,
                "source_type": source_type,
                "subscription_id": subscription_id,
                "symbol_hint": symbol_hint,
                "entries_seen": entry_count,
                "rows_staged": staged_count,
                "status": "FAIL",
            })

    if not normalized:
        return {
            "status": "SUCCESS_WITH_NO_ROWS",
            "test_mode": bool(p_test_mode),
            "news_enabled": news_enabled,
            "sources_considered": len(global_rows),
            "ticker_subscriptions_considered": len(ticker_rows),
            "targets_polled": len(targets),
            "targets_skipped_budget": int(skipped_due_to_budget),
            "rows_staged": 0,
            "rows_inserted": 0,
            "dedup_clusters_upserted": 0,
            "sources": per_source,
            "runtime_controls": {
                "max_targets_per_run": int(max_targets_per_run),
                "fetch_budget_seconds": int(fetch_budget_seconds),
                "fetch_max_attempts": int(fetch_max_attempts),
                "fetch_connect_timeout_sec": int(fetch_connect_timeout_sec),
                "fetch_read_timeout_sec": int(fetch_read_timeout_sec),
            },
            "errors": errors,
        }

    stage_suffix = _sha256(snapshot_ts.isoformat())[:10]
    stage_raw = f"MIP.NEWS.STG_NEWS_RAW_LOAD_{stage_suffix}"
    dedup_stage = f"MIP.NEWS.STG_NEWS_DEDUP_AGG_{stage_suffix}"

    session.create_dataframe(normalized).write.mode("overwrite").save_as_table(stage_raw, table_type="transient")

    before_count = session.sql("select count(*) from MIP.NEWS.NEWS_RAW").collect()[0][0]

    merge_sql = f"""
        merge into MIP.NEWS.NEWS_RAW t
        using {stage_raw} s
           on t.NEWS_ID = s.NEWS_ID
        when not matched then
          insert (
            NEWS_ID, SOURCE_ID, SOURCE_NAME, PUBLISHED_AT, INGESTED_AT, TITLE, SUMMARY,
            FULL_TEXT_OPTIONAL, URL, LANGUAGE, RAW_PAYLOAD_VARIANT, CONTENT_HASH, CANONICAL_URL_HASH,
            DEDUP_CLUSTER_ID, PARSE_STATUS, ERROR_REASON, SNAPSHOT_TS, RUN_ID,
            SUBSCRIPTION_ID, SYMBOL_HINT, MARKET_TYPE_HINT, SOURCE_TYPE
          )
          values (
            s.NEWS_ID, s.SOURCE_ID, s.SOURCE_NAME, s.PUBLISHED_AT, s.INGESTED_AT, s.TITLE, s.SUMMARY,
            s.FULL_TEXT_OPTIONAL, s.URL, s.LANGUAGE, s.RAW_PAYLOAD_VARIANT, s.CONTENT_HASH, s.CANONICAL_URL_HASH,
            s.DEDUP_CLUSTER_ID, s.PARSE_STATUS, s.ERROR_REASON, s.SNAPSHOT_TS, s.RUN_ID,
            s.SUBSCRIPTION_ID, s.SYMBOL_HINT, s.MARKET_TYPE_HINT, s.SOURCE_TYPE
          )
    """
    session.sql(merge_sql).collect()

    after_count = session.sql("select count(*) from MIP.NEWS.NEWS_RAW").collect()[0][0]
    rows_inserted = int(after_count - before_count)

    session.sql(f"""
        create or replace transient table {dedup_stage} as
        select
            DEDUP_CLUSTER_ID,
            min(NEWS_ID) as REPRESENTATIVE_NEWS_ID,
            count(*) as CLUSTER_SIZE,
            min(PUBLISHED_AT) as CLUSTER_FIRST_SEEN_AT,
            max(PUBLISHED_AT) as CLUSTER_LAST_SEEN_AT
        from MIP.NEWS.NEWS_RAW
        where DEDUP_CLUSTER_ID is not null
        group by DEDUP_CLUSTER_ID
    """).collect()

    session.sql(f"""
        merge into MIP.NEWS.NEWS_DEDUP t
        using {dedup_stage} s
           on t.DEDUP_CLUSTER_ID = s.DEDUP_CLUSTER_ID
        when matched then update set
            t.REPRESENTATIVE_NEWS_ID = s.REPRESENTATIVE_NEWS_ID,
            t.CLUSTER_SIZE = s.CLUSTER_SIZE,
            t.CLUSTER_FIRST_SEEN_AT = s.CLUSTER_FIRST_SEEN_AT,
            t.CLUSTER_LAST_SEEN_AT = s.CLUSTER_LAST_SEEN_AT,
            t.UPDATED_AT = current_timestamp()
        when not matched then insert (
            DEDUP_CLUSTER_ID, REPRESENTATIVE_NEWS_ID, CLUSTER_SIZE,
            CLUSTER_FIRST_SEEN_AT, CLUSTER_LAST_SEEN_AT, UPDATED_AT
        ) values (
            s.DEDUP_CLUSTER_ID, s.REPRESENTATIVE_NEWS_ID, s.CLUSTER_SIZE,
            s.CLUSTER_FIRST_SEEN_AT, s.CLUSTER_LAST_SEEN_AT, current_timestamp()
        )
    """).collect()

    dedup_clusters = session.sql(f"select count(*) from {dedup_stage}").collect()[0][0]
    session.sql(f"drop table if exists {stage_raw}").collect()
    session.sql(f"drop table if exists {dedup_stage}").collect()

    return {
        "status": "SUCCESS",
        "test_mode": bool(p_test_mode),
        "news_enabled": news_enabled,
        "snapshot_ts": snapshot_ts.isoformat(),
        "sources_considered": len(global_rows),
        "ticker_subscriptions_considered": len(ticker_rows),
        "targets_polled": len(targets),
        "targets_skipped_budget": int(skipped_due_to_budget),
        "rows_staged": len(normalized),
        "rows_inserted": int(rows_inserted),
        "dedup_clusters_upserted": int(dedup_clusters),
        "runtime_controls": {
            "max_targets_per_run": int(max_targets_per_run),
            "fetch_budget_seconds": int(fetch_budget_seconds),
            "fetch_max_attempts": int(fetch_max_attempts),
            "fetch_connect_timeout_sec": int(fetch_connect_timeout_sec),
            "fetch_read_timeout_sec": int(fetch_read_timeout_sec),
        },
        "sources": per_source,
        "errors": errors,
    }
$$;
