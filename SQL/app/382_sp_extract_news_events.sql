-- 382_sp_extract_news_events.sql
-- Purpose: Phase A extraction procedure for structured news events.
-- Notes:
--   - Idempotent upsert into NEWS_EVENT_EXTRACTED.
--   - Chunked processing for bounded runtime.
--   - Attempts Cortex extraction when enabled; falls back to deterministic parsing.

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace procedure MIP.NEWS.SP_EXTRACT_NEWS_EVENTS(
    P_AS_OF_TS timestamp_ntz default null,
    P_LIMIT number default 500,
    P_BATCH_SIZE number default 100,
    P_FORCE_REEXTRACT boolean default false,
    P_PROMPT_VERSION string default 'v1_phase_a',
    P_MODEL string default 'llama3.1-70b',
    P_USE_CORTEX boolean default true
)
returns variant
language python
runtime_version = '3.12'
packages = ('snowflake-snowpark-python')
handler = 'run'
as
$$
import hashlib
import json
import re
from datetime import datetime, timezone


def _utc_now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _text(v):
    if v is None:
        return ""
    return str(v)


def _sha(v):
    return hashlib.sha256(_text(v).encode("utf-8")).hexdigest()


def _safe_json_obj(v):
    if isinstance(v, dict):
        return v
    if isinstance(v, str):
        try:
            p = json.loads(v)
            return p if isinstance(p, dict) else {}
        except Exception:
            return {}
    return {}


def _safe_json_list(v):
    if isinstance(v, list):
        return v
    if isinstance(v, str):
        try:
            p = json.loads(v)
            return p if isinstance(p, list) else []
        except Exception:
            return []
    return []


def _sentence_facts(text, max_items=3):
    parts = [p.strip() for p in re.split(r"[.;\n]+", text) if p and p.strip()]
    return parts[:max_items]


def _heuristic_extract(title, summary):
    text = (_text(title) + " " + _text(summary)).strip()
    up = text.upper()

    event_type = "other"
    if any(k in up for k in ["EARNINGS", "GUIDANCE", "REVENUE", "EPS"]):
        event_type = "earnings"
    elif any(k in up for k in ["FED", "ECB", "RATE", "INFLATION", "CPI", "NFP"]):
        event_type = "macro_policy"
    elif any(k in up for k in ["MERGER", "ACQUISITION", "M&A"]):
        event_type = "mna"
    elif any(k in up for k in ["LAWSUIT", "SEC PROBE", "INVESTIGATION", "FINE"]):
        event_type = "regulatory_legal"
    elif any(k in up for k in ["PRODUCT", "LAUNCH", "PARTNERSHIP"]):
        event_type = "product_business"

    direction = "neutral"
    if any(k in up for k in ["BEAT", "RAISE", "UPGRADE", "GROWTH", "SURGE", "GAIN"]):
        direction = "positive"
    if any(k in up for k in ["MISS", "CUT", "DOWNGRADE", "DECLINE", "DROP", "RISK", "WARN"]):
        direction = "negative"

    impact_horizon = "medium"
    if any(k in up for k in ["TODAY", "THIS WEEK", "IMMEDIATE", "NEAR-TERM"]):
        impact_horizon = "short"
    elif any(k in up for k in ["2027", "2028", "LONG-TERM", "MULTI-YEAR"]):
        impact_horizon = "long"

    relevance_scope = "symbol"
    if event_type == "macro_policy":
        relevance_scope = "macro"
    elif any(k in up for k in ["SECTOR", "INDUSTRY", "PEERS"]):
        relevance_scope = "sector"

    conf = 0.60
    if event_type != "other":
        conf += 0.10
    if direction != "neutral":
        conf += 0.08
    if len(text) > 140:
        conf += 0.05
    conf = min(max(conf, 0.0), 0.95)

    risk_score = 0.20
    if event_type in ("macro_policy", "regulatory_legal"):
        risk_score += 0.30
    if direction == "negative":
        risk_score += 0.20
    if any(k in up for k in ["VOLATILITY", "UNCERTAIN", "RISK"]):
        risk_score += 0.15
    risk_score = min(max(risk_score, 0.0), 1.0)

    theme_tags = []
    for k, tag in [
        ("AI", "ai"),
        ("CLOUD", "cloud"),
        ("RATES", "rates"),
        ("INFLATION", "inflation"),
        ("SUPPLY CHAIN", "supply_chain"),
        ("REGULATION", "regulation"),
        ("FX", "fx"),
    ]:
        if k in up:
            theme_tags.append(tag)

    summary_out = text[:320] if text else None
    key_facts = _sentence_facts(text, max_items=3)

    return {
        "event_type": event_type,
        "direction": direction,
        "confidence": conf,
        "impact_horizon": impact_horizon,
        "relevance_scope": relevance_scope,
        "theme_tags": theme_tags,
        "event_summary": summary_out,
        "key_facts": key_facts,
        "event_risk_score": risk_score,
        "llm_used": False,
        "raw_extract_variant": {
            "mode": "heuristic_fallback",
            "version": "phase_a_v1",
        },
    }


def _try_cortex_extract(session, text, model, prompt_version):
    # Keep schema strict and deterministic; fallback on any failure.
    prompt = f"""
You are an extraction engine. Return only strict JSON object with keys:
event_type, direction, confidence, impact_horizon, relevance_scope, theme_tags, event_summary, key_facts, event_risk_score.
Rules:
- event_type in [earnings, macro_policy, mna, regulatory_legal, product_business, other]
- direction in [positive, negative, neutral]
- confidence numeric 0..1
- impact_horizon in [short, medium, long]
- relevance_scope in [symbol, sector, macro]
- theme_tags JSON array of strings
- key_facts JSON array (max 5)
- event_risk_score numeric 0..1
- no markdown, no prose.
prompt_version={prompt_version}
text={text}
"""
    esc_prompt = prompt.replace("'", "''")
    esc_model = _text(model).replace("'", "''")
    try:
        row = session.sql(
            f"select SNOWFLAKE.CORTEX.COMPLETE('{esc_model}', '{esc_prompt}') as R"
        ).collect()[0]
        raw = _text(row["R"])
        # Try to isolate JSON if model wraps output.
        m = re.search(r"\{.*\}", raw, flags=re.DOTALL)
        payload = _safe_json_obj(m.group(0) if m else raw)
        if not payload:
            return None
        payload["llm_used"] = True
        payload["raw_extract_variant"] = {
            "mode": "cortex",
            "model": model,
            "raw_response": raw[:4000],
        }
        return payload
    except Exception:
        return None


def _norm_direction(v):
    s = _text(v).strip().lower()
    return s if s in ("positive", "negative", "neutral") else "neutral"


def _norm_event_type(v):
    s = _text(v).strip().lower()
    allowed = {
        "earnings",
        "macro_policy",
        "mna",
        "regulatory_legal",
        "product_business",
        "other",
    }
    return s if s in allowed else "other"


def _norm_horizon(v):
    s = _text(v).strip().lower()
    return s if s in ("short", "medium", "long") else "medium"


def _norm_scope(v):
    s = _text(v).strip().lower()
    return s if s in ("symbol", "sector", "macro") else "symbol"


def run(
    session,
    p_as_of_ts=None,
    p_limit=500,
    p_batch_size=100,
    p_force_reextract=False,
    p_prompt_version="v1_phase_a",
    p_model="llama3.1-70b",
    p_use_cortex=True,
):
    now_ts = _utc_now()
    as_of_ts = p_as_of_ts if p_as_of_ts is not None else now_ts

    limit_n = max(int(p_limit or 500), 1)
    batch_n = max(int(p_batch_size or 100), 1)
    force = bool(p_force_reextract)
    prompt_version = _text(p_prompt_version) or "v1_phase_a"
    model = _text(p_model) or "llama3.1-70b"
    use_cortex = bool(p_use_cortex)

    where_reextract = ""
    if not force:
        where_reextract = "and ext.EXTRACT_ID is null"

    candidates = session.sql(
        f"""
        with mapped as (
            select
                m.NEWS_ID,
                m.SYMBOL,
                m.MARKET_TYPE,
                m.MATCH_CONFIDENCE,
                row_number() over (
                    partition by m.NEWS_ID, m.SYMBOL, m.MARKET_TYPE
                    order by m.MATCH_CONFIDENCE desc, m.MATCH_METHOD
                ) as RN
            from MIP.NEWS.NEWS_SYMBOL_MAP m
        ),
        best_map as (
            select * from mapped where RN = 1
        )
        select
            r.NEWS_ID,
            b.SYMBOL,
            b.MARKET_TYPE,
            r.PUBLISHED_AT,
            r.TITLE,
            r.SUMMARY,
            r.URL,
            r.CONTENT_HASH,
            b.MATCH_CONFIDENCE
        from MIP.NEWS.NEWS_RAW r
        join best_map b
          on b.NEWS_ID = r.NEWS_ID
        left join MIP.NEWS.NEWS_EVENT_EXTRACTED ext
          on ext.NEWS_ID = r.NEWS_ID
         and ext.SYMBOL = b.SYMBOL
         and ext.MARKET_TYPE = b.MARKET_TYPE
         and ext.PROMPT_VERSION = '{prompt_version.replace("'", "''")}'
        where r.PUBLISHED_AT <= to_timestamp_ntz('{str(as_of_ts)}')
          {where_reextract}
        order by r.PUBLISHED_AT desc, r.NEWS_ID
        limit {limit_n}
        """
    ).collect()

    if not candidates:
        return {
            "status": "SUCCESS",
            "rows_candidates": 0,
            "rows_merged": 0,
            "llm_attempted": False,
            "llm_used_count": 0,
            "prompt_version": prompt_version,
        }

    staged = []
    llm_used_count = 0
    llm_attempted = False

    for i in range(0, len(candidates), batch_n):
        batch = candidates[i : i + batch_n]
        for r in batch:
            news_id = _text(r["NEWS_ID"])
            symbol = _text(r["SYMBOL"])
            market_type = _text(r["MARKET_TYPE"])
            title = _text(r["TITLE"])
            summary = _text(r["SUMMARY"])
            url = _text(r["URL"])
            published_at = r["PUBLISHED_AT"]
            content_hash = _text(r["CONTENT_HASH"])
            match_conf = float(r["MATCH_CONFIDENCE"] or 0.0)

            input_hash = _sha(f"{news_id}|{symbol}|{market_type}|{title}|{summary}|{url}|{prompt_version}")
            text = (title + " " + summary).strip()

            extracted = None
            if use_cortex and len(text) >= 20:
                llm_attempted = True
                extracted = _try_cortex_extract(session, text, model, prompt_version)

            if not extracted:
                extracted = _heuristic_extract(title, summary)

            event_type = _norm_event_type(extracted.get("event_type"))
            direction = _norm_direction(extracted.get("direction"))
            confidence = float(extracted.get("confidence", 0.60) or 0.60)
            confidence = max(0.0, min(1.0, confidence))
            # Blend mapping confidence to keep extraction anchored to symbol relevance.
            confidence = max(0.0, min(1.0, (confidence * 0.7) + (match_conf * 0.3)))
            impact_horizon = _norm_horizon(extracted.get("impact_horizon"))
            relevance_scope = _norm_scope(extracted.get("relevance_scope"))
            theme_tags = _safe_json_list(extracted.get("theme_tags"))
            key_facts = _safe_json_list(extracted.get("key_facts"))
            event_summary = _text(extracted.get("event_summary"))[:320] or None
            event_risk_score = float(extracted.get("event_risk_score", 0.2) or 0.2)
            event_risk_score = max(0.0, min(1.0, event_risk_score))
            llm_used = bool(extracted.get("llm_used"))
            if llm_used:
                llm_used_count += 1
            raw_extract = extracted.get("raw_extract_variant")
            if not isinstance(raw_extract, (dict, list, str, int, float, bool)) and raw_extract is not None:
                raw_extract = {"value": _text(raw_extract)}

            extract_id = _sha(f"{news_id}|{symbol}|{market_type}|{prompt_version}")
            output_hash = _sha(
                json.dumps(
                    {
                        "event_type": event_type,
                        "direction": direction,
                        "confidence": confidence,
                        "impact_horizon": impact_horizon,
                        "relevance_scope": relevance_scope,
                        "theme_tags": theme_tags,
                        "event_summary": event_summary,
                        "key_facts": key_facts,
                        "event_risk_score": event_risk_score,
                    },
                    sort_keys=True,
                )
            )

            staged.append(
                {
                    "EXTRACT_ID": extract_id,
                    "NEWS_ID": news_id,
                    "SYMBOL": symbol,
                    "MARKET_TYPE": market_type,
                    "EVENT_TS": published_at,
                    "EVENT_TYPE": event_type,
                    "DIRECTION": direction,
                    "CONFIDENCE": confidence,
                    "IMPACT_HORIZON": impact_horizon,
                    "RELEVANCE_SCOPE": relevance_scope,
                    "THEME_TAGS": theme_tags,
                    "EVENT_SUMMARY": event_summary,
                    "KEY_FACTS": key_facts,
                    "EVENT_RISK_SCORE": event_risk_score,
                    "RAW_EXTRACT_VARIANT": raw_extract,
                    "LLM_USED": llm_used,
                    "LLM_MODEL": model if llm_used else None,
                    "PROMPT_VERSION": prompt_version,
                    "INPUT_HASH": input_hash,
                    "OUTPUT_HASH": output_hash,
                    "EXTRACTED_AT": now_ts,
                    "RUN_ID": _sha(f"{now_ts.isoformat()}|{prompt_version}")[:32],
                    "UPDATED_AT": now_ts,
                }
            )

    if not staged:
        return {
            "status": "SUCCESS",
            "rows_candidates": len(candidates),
            "rows_merged": 0,
            "llm_attempted": llm_attempted,
            "llm_used_count": 0,
            "prompt_version": prompt_version,
        }

    stage_table = "MIP.NEWS.STG_NEWS_EVENT_EXTRACTED_" + _sha(now_ts.isoformat())[:12]
    session.create_dataframe(staged).write.mode("overwrite").save_as_table(stage_table, table_type="transient")

    before_count = session.sql("select count(*) from MIP.NEWS.NEWS_EVENT_EXTRACTED").collect()[0][0]
    session.sql(
        f"""
        merge into MIP.NEWS.NEWS_EVENT_EXTRACTED t
        using (
            select *
            from {stage_table}
            qualify row_number() over (
                partition by EXTRACT_ID
                order by EXTRACTED_AT desc, NEWS_ID, SYMBOL
            ) = 1
        ) s
          on t.EXTRACT_ID = s.EXTRACT_ID
        when matched then update set
            t.EVENT_TS = s.EVENT_TS,
            t.EVENT_TYPE = s.EVENT_TYPE,
            t.DIRECTION = s.DIRECTION,
            t.CONFIDENCE = s.CONFIDENCE,
            t.IMPACT_HORIZON = s.IMPACT_HORIZON,
            t.RELEVANCE_SCOPE = s.RELEVANCE_SCOPE,
            t.THEME_TAGS = s.THEME_TAGS,
            t.EVENT_SUMMARY = s.EVENT_SUMMARY,
            t.KEY_FACTS = s.KEY_FACTS,
            t.EVENT_RISK_SCORE = s.EVENT_RISK_SCORE,
            t.RAW_EXTRACT_VARIANT = s.RAW_EXTRACT_VARIANT,
            t.LLM_USED = s.LLM_USED,
            t.LLM_MODEL = s.LLM_MODEL,
            t.INPUT_HASH = s.INPUT_HASH,
            t.OUTPUT_HASH = s.OUTPUT_HASH,
            t.EXTRACTED_AT = s.EXTRACTED_AT,
            t.RUN_ID = s.RUN_ID,
            t.UPDATED_AT = s.UPDATED_AT
        when not matched then insert (
            EXTRACT_ID, NEWS_ID, SYMBOL, MARKET_TYPE, EVENT_TS, EVENT_TYPE, DIRECTION,
            CONFIDENCE, IMPACT_HORIZON, RELEVANCE_SCOPE, THEME_TAGS, EVENT_SUMMARY,
            KEY_FACTS, EVENT_RISK_SCORE, RAW_EXTRACT_VARIANT, LLM_USED, LLM_MODEL,
            PROMPT_VERSION, INPUT_HASH, OUTPUT_HASH, EXTRACTED_AT, RUN_ID, UPDATED_AT
        ) values (
            s.EXTRACT_ID, s.NEWS_ID, s.SYMBOL, s.MARKET_TYPE, s.EVENT_TS, s.EVENT_TYPE, s.DIRECTION,
            s.CONFIDENCE, s.IMPACT_HORIZON, s.RELEVANCE_SCOPE, s.THEME_TAGS, s.EVENT_SUMMARY,
            s.KEY_FACTS, s.EVENT_RISK_SCORE, s.RAW_EXTRACT_VARIANT, s.LLM_USED, s.LLM_MODEL,
            s.PROMPT_VERSION, s.INPUT_HASH, s.OUTPUT_HASH, s.EXTRACTED_AT, s.RUN_ID, s.UPDATED_AT
        )
        """
    ).collect()
    # Snowflake PK constraints are informational; enforce 1 row per EXTRACT_ID explicitly.
    dedup_table = "MIP.NEWS.TMP_NEWS_EVENT_EXTRACTED_DEDUP_" + _sha(now_ts.isoformat())[:10]
    session.sql(
        f"""
        create or replace transient table {dedup_table} as
        select
            EXTRACT_ID, NEWS_ID, SYMBOL, MARKET_TYPE, EVENT_TS, EVENT_TYPE, DIRECTION,
            CONFIDENCE, IMPACT_HORIZON, RELEVANCE_SCOPE, THEME_TAGS, EVENT_SUMMARY,
            KEY_FACTS, EVENT_RISK_SCORE, RAW_EXTRACT_VARIANT, LLM_USED, LLM_MODEL,
            PROMPT_VERSION, INPUT_HASH, OUTPUT_HASH, EXTRACTED_AT, RUN_ID, CREATED_AT, UPDATED_AT
        from MIP.NEWS.NEWS_EVENT_EXTRACTED
        qualify row_number() over (
            partition by EXTRACT_ID
            order by UPDATED_AT desc, EXTRACTED_AT desc, NEWS_ID, SYMBOL
        ) = 1
        """
    ).collect()
    session.sql("truncate table MIP.NEWS.NEWS_EVENT_EXTRACTED").collect()
    session.sql(
        f"""
        insert into MIP.NEWS.NEWS_EVENT_EXTRACTED (
            EXTRACT_ID, NEWS_ID, SYMBOL, MARKET_TYPE, EVENT_TS, EVENT_TYPE, DIRECTION,
            CONFIDENCE, IMPACT_HORIZON, RELEVANCE_SCOPE, THEME_TAGS, EVENT_SUMMARY,
            KEY_FACTS, EVENT_RISK_SCORE, RAW_EXTRACT_VARIANT, LLM_USED, LLM_MODEL,
            PROMPT_VERSION, INPUT_HASH, OUTPUT_HASH, EXTRACTED_AT, RUN_ID, CREATED_AT, UPDATED_AT
        )
        select
            EXTRACT_ID, NEWS_ID, SYMBOL, MARKET_TYPE, EVENT_TS, EVENT_TYPE, DIRECTION,
            CONFIDENCE, IMPACT_HORIZON, RELEVANCE_SCOPE, THEME_TAGS, EVENT_SUMMARY,
            KEY_FACTS, EVENT_RISK_SCORE, RAW_EXTRACT_VARIANT, LLM_USED, LLM_MODEL,
            PROMPT_VERSION, INPUT_HASH, OUTPUT_HASH, EXTRACTED_AT, RUN_ID, CREATED_AT, UPDATED_AT
        from {dedup_table}
        """
    ).collect()
    session.sql(f"drop table if exists {dedup_table}").collect()
    after_count = session.sql("select count(*) from MIP.NEWS.NEWS_EVENT_EXTRACTED").collect()[0][0]
    session.sql(f"drop table if exists {stage_table}").collect()

    return {
        "status": "SUCCESS",
        "rows_candidates": len(candidates),
        "rows_staged": len(staged),
        "rows_merged": int(after_count - before_count),
        "llm_attempted": llm_attempted,
        "llm_used_count": llm_used_count,
        "prompt_version": prompt_version,
        "model": model,
    }
$$;
