import json
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, Query

from app.db import fetch_all, get_connection, serialize_rows

router = APIRouter(prefix="/news", tags=["news"])


def _to_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _to_bool(v: Any) -> Optional[bool]:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.lower() in ("true", "1", "yes")
    try:
        return bool(v)
    except Exception:
        return None


def _safe_iso(ts: Any) -> Optional[str]:
    if ts is None:
        return None
    if hasattr(ts, "isoformat"):
        return ts.isoformat()
    return str(ts)


def _normalize_headlines(raw: Any) -> list[dict[str, Any]]:
    if raw is None:
        return []
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except Exception:
            return []
        raw = parsed
    if not isinstance(raw, list):
        return []
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for h in raw:
        if not isinstance(h, dict):
            continue
        title = h.get("title") or h.get("TITLE") or h.get("headline") or h.get("HEADLINE")
        if not title:
            continue
        url = _normalize_url(h.get("url") or h.get("URL"))
        dedup_key = f"{str(title).strip()}||{url or ''}"
        if dedup_key in seen:
            continue
        seen.add(dedup_key)
        out.append(
            {
                "title": str(title),
                "url": url,
            }
        )
    return out


def _normalize_url(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    lower = s.lower()
    if not (lower.startswith("http://") or lower.startswith("https://")):
        return None
    if "mock-item-" in lower or "/rss/" in lower or lower.endswith(".xml"):
        return None
    return s


def _to_obj(v: Any) -> dict[str, Any]:
    if v is None:
        return {}
    if isinstance(v, dict):
        return v
    if isinstance(v, str):
        try:
            p = json.loads(v)
            return p if isinstance(p, dict) else {}
        except Exception:
            return {}
    return {}


def _to_list(v: Any) -> list[Any]:
    if v is None:
        return []
    if isinstance(v, list):
        return v
    if isinstance(v, str):
        try:
            p = json.loads(v)
            return p if isinstance(p, list) else []
        except Exception:
            return []
    return []


def _headline_signal_profile(
    sentiment: Optional[float],
    uncertainty: Optional[float],
    event_risk: Optional[float],
    is_stale: Optional[bool],
) -> dict[str, Any]:
    """Map numeric news signals to simple UI-friendly tone/effect."""
    s = _to_float(sentiment)
    u = _to_float(uncertainty) or 0.0
    r = _to_float(event_risk) or 0.0
    stale = bool(is_stale) if is_stale is not None else False

    if stale:
        return {
            "icon": "😐",
            "tone": "NO_EFFECT",
            "effect_label": "Low immediate effect (stale context)",
            "committee_note": "Snapshot is stale; treat as watchlist context until refreshed.",
        }

    if s is not None and s <= -0.20 and u < 0.75 and r >= 0.20:
        return {
            "icon": "😠",
            "tone": "NEGATIVE",
            "effect_label": "Potential downside risk",
            "committee_note": "Bias is negative with meaningful risk; require tighter entry discipline.",
        }

    if s is not None and s >= 0.20 and u < 0.75 and r < 0.70:
        return {
            "icon": "😊",
            "tone": "POSITIVE",
            "effect_label": "Potential positive tailwind",
            "committee_note": "Bias is positive; can support conviction if technicals and policy align.",
        }

    return {
        "icon": "😐",
        "tone": "NO_EFFECT",
        "effect_label": "No clear directional edge",
        "committee_note": "Signal is mixed/uncertain; keep as context, not a standalone trigger.",
    }


@router.get("/intelligence")
def get_news_intelligence(
    portfolio_id: Optional[int] = Query(None, description="Optional portfolio scope for exposure overlay"),
    limit_symbols: int = Query(30, ge=5, le=100, description="Max symbol cards"),
):
    """
    Deterministic, evidence-backed news intelligence snapshot.
    No generative synthesis: summaries are computed from stored features only.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()

        cur.execute(
            """
            with cfg as (
                select
                    coalesce(max(try_to_number(case when CONFIG_KEY = 'NEWS_STALENESS_THRESHOLD_MINUTES' then CONFIG_VALUE end)), 180) as STALENESS_MINUTES,
                    coalesce(max(try_to_double(case when CONFIG_KEY = 'NEWS_CONFLICT_HIGH' then CONFIG_VALUE end)), 0.60) as CONFLICT_HIGH
                from MIP.APP.APP_CONFIG
                where CONFIG_KEY in ('NEWS_STALENESS_THRESHOLD_MINUTES', 'NEWS_CONFLICT_HIGH')
            )
            select
                cast(n.AS_OF_TS_BUCKET as date) as AS_OF_DATE,
                n.SYMBOL,
                n.MARKET_TYPE,
                coalesce(n.ITEMS_TOTAL, 0) as NEWS_COUNT,
                coalesce(n.BADGE, 'NORMAL') as NEWS_CONTEXT_BADGE,
                n.NOVELTY as NOVELTY_SCORE,
                n.INFO_PRESSURE as BURST_SCORE,
                iff(coalesce(n.CONFLICT, 0) >= cfg.CONFLICT_HIGH, true, false) as UNCERTAINTY_FLAG,
                n.TOP_CLUSTERS as TOP_HEADLINES,
                n.LAST_PUBLISHED_AT as LAST_NEWS_PUBLISHED_AT,
                n.LAST_INGESTED_AT as LAST_INGESTED_AT,
                n.SNAPSHOT_TS,
                datediff('minute', coalesce(n.LAST_INGESTED_AT, n.SNAPSHOT_TS), current_timestamp()) as NEWS_SNAPSHOT_AGE_MINUTES,
                iff(
                    datediff('minute', coalesce(n.LAST_INGESTED_AT, n.SNAPSHOT_TS), current_timestamp()) > cfg.STALENESS_MINUTES,
                    true,
                    false
                ) as NEWS_IS_STALE,
                cfg.STALENESS_MINUTES as NEWS_STALENESS_THRESHOLD_MINUTES
            from MIP.MART.V_NEWS_AGG_LATEST n
            cross join cfg
            order by
                NEWS_IS_STALE asc,
                BURST_SCORE desc nulls last,
                NEWS_COUNT desc,
                n.SYMBOL
            """
        )
        news_rows = serialize_rows(fetch_all(cur))

        cur.execute(
            """
            with latest_price as (
                select
                    SYMBOL,
                    MARKET_TYPE,
                    CLOSE
                from MIP.MART.MARKET_BARS
                qualify row_number() over (
                    partition by SYMBOL, MARKET_TYPE
                    order by TS desc
                ) = 1
            )
            select
                p.PORTFOLIO_ID,
                p.SYMBOL,
                p.MARKET_TYPE,
                p.QUANTITY,
                p.COST_BASIS,
                coalesce(lp.CLOSE, 0) * coalesce(p.QUANTITY, 0) as MARKET_VALUE
            from MIP.MART.V_PORTFOLIO_OPEN_POSITIONS_CANONICAL p
            left join latest_price lp
              on lp.SYMBOL = p.SYMBOL
             and lp.MARKET_TYPE = p.MARKET_TYPE
            where p.IS_OPEN = true
              and p.INTERVAL_MINUTES = 1440
              and (%s is null or p.PORTFOLIO_ID = %s)
            """,
            (portfolio_id, portfolio_id),
        )
        open_rows = serialize_rows(fetch_all(cur))

        cur.execute(
            """
            select
                p.PROPOSAL_ID,
                p.PORTFOLIO_ID,
                p.SYMBOL,
                p.MARKET_TYPE,
                p.STATUS,
                p.PROPOSED_AT,
                p.SOURCE_SIGNALS,
                p.RATIONALE
            from MIP.AGENT_OUT.ORDER_PROPOSALS p
            where p.STATUS in ('PROPOSED', 'APPROVED', 'EXECUTED')
              and (%s is null or p.PORTFOLIO_ID = %s)
            order by p.PROPOSED_AT desc, p.PROPOSAL_ID desc
            limit 300
            """,
            (portfolio_id, portfolio_id),
        )
        proposal_rows = serialize_rows(fetch_all(cur))

        by_symbol = {(r.get("SYMBOL"), r.get("MARKET_TYPE")): r for r in news_rows}
        total_market_value = 0.0
        risk_market_value = 0.0
        impacted_positions: list[dict[str, Any]] = []

        for pos in open_rows:
            mv = _to_float(pos.get("MARKET_VALUE")) or 0.0
            total_market_value += max(mv, 0.0)
            key = (pos.get("SYMBOL"), pos.get("MARKET_TYPE"))
            news = by_symbol.get(key)
            if not news:
                continue
            # "Exposure at risk" applies only when symbol has actual news context.
            # Symbols with NO_NEWS / zero count are tracked in coverage, not risk%.
            has_news_context = (_to_float(news.get("NEWS_COUNT")) or 0) > 0
            is_risk = has_news_context and (
                _to_bool(news.get("NEWS_IS_STALE"))
                or _to_bool(news.get("UNCERTAINTY_FLAG"))
                or ((news.get("NEWS_CONTEXT_BADGE") or "").upper() == "HOT")
            )
            if is_risk:
                risk_market_value += max(mv, 0.0)
            impacted_positions.append(
                {
                    "portfolio_id": pos.get("PORTFOLIO_ID"),
                    "symbol": pos.get("SYMBOL"),
                    "market_type": pos.get("MARKET_TYPE"),
                    "market_value": mv,
                    "news_count": news.get("NEWS_COUNT"),
                    "news_badge": news.get("NEWS_CONTEXT_BADGE"),
                    "news_is_stale": news.get("NEWS_IS_STALE"),
                    "uncertainty_flag": news.get("UNCERTAINTY_FLAG"),
                    "news_snapshot_age_minutes": news.get("NEWS_SNAPSHOT_AGE_MINUTES"),
                }
            )

        impacted_positions.sort(
            key=lambda x: (-(x.get("market_value") or 0.0), x.get("symbol") or "")
        )

        stale_count = sum(1 for r in news_rows if _to_bool(r.get("NEWS_IS_STALE")))
        hot_count = sum(1 for r in news_rows if (r.get("NEWS_CONTEXT_BADGE") or "").upper() == "HOT")
        with_news = sum(1 for r in news_rows if (_to_float(r.get("NEWS_COUNT")) or 0) > 0)
        avg_age = None
        ages = [_to_float(r.get("NEWS_SNAPSHOT_AGE_MINUTES")) for r in news_rows]
        ages = [a for a in ages if a is not None]
        if ages:
            avg_age = sum(ages) / len(ages)

        top_headlines: list[dict[str, Any]] = []
        for row in news_rows:
            headlines = _normalize_headlines(row.get("TOP_HEADLINES"))
            for h in headlines[:2]:
                top_headlines.append(
                    {
                        "symbol": row.get("SYMBOL"),
                        "market_type": row.get("MARKET_TYPE"),
                        "badge": row.get("NEWS_CONTEXT_BADGE"),
                        "title": h.get("title"),
                        "url": h.get("url"),
                        "snapshot_ts": _safe_iso(row.get("SNAPSHOT_TS")),
                    }
                )
        top_headlines = top_headlines[:8]

        symbol_cards = []
        for row in news_rows[:limit_symbols]:
            symbol_cards.append(
                {
                    "symbol": row.get("SYMBOL"),
                    "market_type": row.get("MARKET_TYPE"),
                    "as_of_date": _safe_iso(row.get("AS_OF_DATE")),
                    "news_count": row.get("NEWS_COUNT"),
                    "news_badge": row.get("NEWS_CONTEXT_BADGE"),
                    "novelty_score": _to_float(row.get("NOVELTY_SCORE")),
                    "burst_score": _to_float(row.get("BURST_SCORE")),
                    "uncertainty_flag": _to_bool(row.get("UNCERTAINTY_FLAG")),
                    "news_snapshot_age_minutes": _to_float(row.get("NEWS_SNAPSHOT_AGE_MINUTES")),
                    "news_is_stale": _to_bool(row.get("NEWS_IS_STALE")),
                    "last_news_published_at": _safe_iso(row.get("LAST_NEWS_PUBLISHED_AT")),
                    "last_ingested_at": _safe_iso(row.get("LAST_INGESTED_AT")),
                    "snapshot_ts": _safe_iso(row.get("SNAPSHOT_TS")),
                    "top_headlines": _normalize_headlines(row.get("TOP_HEADLINES"))[:3],
                    "evidence": {
                        "source_table": "MIP.MART.V_NEWS_AGG_LATEST",
                        "join_contract": "symbol+market_type latest as_of_ts_bucket <= as_of_ts",
                    },
                }
            )

        risk_pct = (risk_market_value / total_market_value * 100.0) if total_market_value > 0 else 0.0
        summary_bullets = [
            f"Coverage: {with_news}/{len(news_rows)} symbols have non-zero news context.",
            f"Freshness: {stale_count} stale snapshots, average age {round(avg_age, 1) if avg_age is not None else 'n/a'} minutes.",
            f"Heat: {hot_count} symbols currently flagged HOT.",
            f"Exposure-at-risk: {round(risk_pct, 1)}% of scoped open market value is in stale/uncertain/HOT context.",
        ]

        decision_rows: list[dict[str, Any]] = []
        with_context = 0
        with_adj = 0
        blocked = 0
        adj_vals: list[float] = []
        for row in proposal_rows:
            src = _to_obj(row.get("SOURCE_SIGNALS"))
            rat = _to_obj(row.get("RATIONALE"))
            news_ctx = _to_obj(src.get("news_context"))
            news_agg = _to_obj(src.get("news_agg"))
            has_context = bool(news_ctx) or bool(news_agg)
            if has_context:
                with_context += 1

            news_adj = _to_float(rat.get("news_score_adj"))
            if news_adj is None:
                news_adj = _to_float(src.get("news_score_adj"))
            if news_adj is not None:
                with_adj += 1
                adj_vals.append(news_adj)

            blocked_entry = _to_bool(rat.get("news_block_new_entry"))
            if blocked_entry is None:
                blocked_entry = _to_bool(src.get("news_block_new_entry"))
            if blocked_entry:
                blocked += 1

            reasons = _to_list(rat.get("news_reasons"))
            if not reasons:
                reasons = _to_list(src.get("news_reasons"))

            # Only surface rows that actually carry news evidence.
            has_news_evidence = (
                has_context
                or news_adj is not None
                or bool(blocked_entry)
                or len(reasons) > 0
            )
            if not has_news_evidence:
                continue

            decision_rows.append(
                {
                    "proposal_id": row.get("PROPOSAL_ID"),
                    "portfolio_id": row.get("PORTFOLIO_ID"),
                    "symbol": row.get("SYMBOL"),
                    "market_type": row.get("MARKET_TYPE"),
                    "status": row.get("STATUS"),
                    "proposed_at": _safe_iso(row.get("PROPOSED_AT")),
                    "news_score_adj": news_adj,
                    "news_block_new_entry": bool(blocked_entry) if blocked_entry is not None else False,
                    "news_snapshot_age_minutes": _to_float(src.get("news_snapshot_age_minutes")),
                    "news_is_stale": _to_bool(src.get("news_is_stale")),
                    "news_badge": news_agg.get("badge") or news_ctx.get("news_context_badge"),
                    "reasons": reasons[:3],
                }
            )

        decision_rows.sort(
            key=lambda x: (
                -(abs(x.get("news_score_adj") or 0.0)),
                x.get("symbol") or "",
            )
        )

        avg_adj = sum(adj_vals) / len(adj_vals) if adj_vals else None

        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "portfolio_scope": portfolio_id,
            "narrative_contract": {
                "mode": "deterministic_v1",
                "hallucination_safe": True,
                "llm_used": False,
                "agentic_ready": True,
            },
            "market_context": {
                "symbols_total": len(news_rows),
                "symbols_with_news": with_news,
                "stale_symbols": stale_count,
                "hot_symbols": hot_count,
                "avg_snapshot_age_minutes": avg_age,
                "top_headlines": top_headlines,
            },
            "portfolio_overlay": {
                "positions_scoped": len(open_rows),
                "total_market_value": total_market_value,
                "risk_market_value": risk_market_value,
                "risk_market_value_pct": risk_pct,
                "top_impacted_positions": impacted_positions[:10],
            },
            "summary_bullets": summary_bullets,
            "symbol_cards": symbol_cards,
            "decision_impact": {
                "proposals_scoped": len(proposal_rows),
                "proposals_with_news_context": with_context,
                "proposals_with_news_score_adj": with_adj,
                "avg_news_score_adj": avg_adj,
                "blocked_new_entry_count": blocked,
                "top_impacts": decision_rows[:12],
                "note": "news_score_adj appears only after proposal scoring influence is enabled.",
            },
            "lineage": {
                "tables": [
                    "MIP.MART.V_NEWS_AGG_LATEST",
                    "MIP.MART.V_PORTFOLIO_OPEN_POSITIONS_CANONICAL",
                    "MIP.MART.MARKET_BARS",
                    "MIP.AGENT_OUT.ORDER_PROPOSALS",
                ],
                "prompt_version": None,
                "model_name": None,
                "input_hash": None,
            },
        }
    finally:
        conn.close()


@router.get("/intelligence/overview")
def get_news_intelligence_overview(
    portfolio_id: Optional[int] = Query(None, description="Optional portfolio scope for exposure overlay"),
    headlines_limit: int = Query(5, ge=3, le=10, description="Max headlines in overview tile"),
):
    """
    Cockpit-focused news overview with concise committee decision guidance.
    """
    payload = get_news_intelligence(portfolio_id=portfolio_id, limit_symbols=30)

    market_context = payload.get("market_context") or {}
    overlay = payload.get("portfolio_overlay") or {}
    decision_impact = payload.get("decision_impact") or {}
    top_headlines = (market_context.get("top_headlines") or [])[:headlines_limit]

    symbols_total = int(market_context.get("symbols_total") or 0)
    symbols_with_news = int(market_context.get("symbols_with_news") or 0)
    stale_symbols = int(market_context.get("stale_symbols") or 0)
    hot_symbols = int(market_context.get("hot_symbols") or 0)
    blocked_new_entry_count = int(decision_impact.get("blocked_new_entry_count") or 0)
    proposals_with_news_context = int(decision_impact.get("proposals_with_news_context") or 0)
    risk_market_value_pct = float(overlay.get("risk_market_value_pct") or 0.0)
    avg_news_score_adj = _to_float(decision_impact.get("avg_news_score_adj"))

    stale_ratio = (stale_symbols / symbols_total) if symbols_total > 0 else 0.0

    if blocked_new_entry_count > 0 or risk_market_value_pct >= 35.0 or stale_ratio >= 0.4:
        tone = "HIGH_RISK"
    elif hot_symbols > 0 or risk_market_value_pct >= 20.0 or proposals_with_news_context >= 3:
        tone = "CAUTION"
    else:
        tone = "BALANCED"

    if tone == "HIGH_RISK":
        committee_hint = (
            "High caution: delay new adds until stale or conflicting news context clears, "
            "and re-check proposal rationale before approval."
        )
    elif tone == "CAUTION":
        committee_hint = (
            "Selective caution: prioritize symbols with fresh context and require explicit "
            "news justification for new entries."
        )
    else:
        committee_hint = (
            "Low immediate news friction: committee can proceed normally while monitoring "
            "headline drift and snapshot freshness."
        )

    summary_bullets = [
        f"{symbols_with_news}/{symbols_total} symbols currently have non-zero news context; {hot_symbols} flagged HOT.",
        f"{stale_symbols} symbols are stale; scoped exposure-at-risk is {round(risk_market_value_pct, 1)}%.",
        f"{proposals_with_news_context} proposals carried news context, with {blocked_new_entry_count} blocked new entries.",
    ]
    if avg_news_score_adj is not None:
        summary_bullets.append(f"Average proposal news adjustment is {avg_news_score_adj:+.3f}.")

    impacted_symbols: list[str] = []
    seen: set[str] = set()
    for row in (decision_impact.get("top_impacts") or []):
        sym = row.get("symbol")
        if not sym or sym in seen:
            continue
        seen.add(sym)
        impacted_symbols.append(sym)
    for row in top_headlines:
        sym = row.get("symbol")
        if not sym or sym in seen:
            continue
        seen.add(sym)
        impacted_symbols.append(sym)

    executive_summary = (
        f"News coverage is active across {symbols_with_news} symbols with {hot_symbols} HOT and "
        f"{stale_symbols} stale contexts. Exposure-at-risk is {round(risk_market_value_pct, 1)}% "
        f"for the current scope; committee tone is {tone.replace('_', ' ').title()}."
    )

    symbol_cards = payload.get("symbol_cards") or []
    card_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for c in symbol_cards:
        key = (str(c.get("symbol") or ""), str(c.get("market_type") or ""))
        card_by_key[key] = c

    # Add feature-level sentiment/risk context for symbols shown in key headlines.
    symbols_for_headlines = [
        (str(h.get("symbol") or ""), str(h.get("market_type") or ""))
        for h in top_headlines
        if h.get("symbol") and h.get("market_type")
    ]
    symbols_for_headlines = list({k for k in symbols_for_headlines if k[0] and k[1]})

    feature_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    if symbols_for_headlines:
        conn = get_connection()
        try:
            cur = conn.cursor()
            values_sql = ", ".join(["(%s, %s)"] * len(symbols_for_headlines))
            params: list[Any] = []
            for sym, mt in symbols_for_headlines:
                params.extend([sym, mt])
            cur.execute(
                f"""
                with targets(symbol, market_type) as (
                    select column1::varchar, column2::varchar
                    from values {values_sql}
                ),
                latest as (
                    select
                        f.SYMBOL,
                        f.MARKET_TYPE,
                        f.NEWS_SENTIMENT,
                        f.UNCERTAINTY_SCORE,
                        f.EVENT_RISK_SCORE,
                        f.NEWS_IS_STALE,
                        row_number() over (
                            partition by f.SYMBOL, f.MARKET_TYPE
                            order by f.AS_OF_TS desc, f.SNAPSHOT_TS desc
                        ) as rn
                    from MIP.MART.V_NEWS_FEATURES_BY_TS f
                    join targets t
                      on t.symbol = f.SYMBOL
                     and t.market_type = f.MARKET_TYPE
                )
                select
                    SYMBOL, MARKET_TYPE,
                    NEWS_SENTIMENT, UNCERTAINTY_SCORE, EVENT_RISK_SCORE, NEWS_IS_STALE
                from latest
                where rn = 1
                """,
                tuple(params),
            )
            rows = serialize_rows(fetch_all(cur))
            for row in rows:
                key = (str(row.get("SYMBOL") or ""), str(row.get("MARKET_TYPE") or ""))
                feature_by_key[key] = {
                    "sentiment": _to_float(row.get("NEWS_SENTIMENT")),
                    "uncertainty_score": _to_float(row.get("UNCERTAINTY_SCORE")),
                    "event_risk_score": _to_float(row.get("EVENT_RISK_SCORE")),
                    "news_is_stale": _to_bool(row.get("NEWS_IS_STALE")),
                }
        finally:
            conn.close()

    enriched_headlines = []
    for h in top_headlines:
        sym = h.get("symbol")
        mt = h.get("market_type")
        key = (str(sym or ""), str(mt or ""))
        features = feature_by_key.get(key, {})
        card = card_by_key.get(key, {})
        profile = _headline_signal_profile(
            sentiment=features.get("sentiment"),
            uncertainty=features.get("uncertainty_score"),
            event_risk=features.get("event_risk_score"),
            is_stale=features.get("news_is_stale") if features else card.get("news_is_stale"),
        )
        enriched_headlines.append(
            {
                "symbol": sym,
                "market_type": mt,
                "badge": h.get("badge") or card.get("news_badge"),
                "title": h.get("title"),
                "url": h.get("url"),
                "icon": profile.get("icon"),
                "tone": profile.get("tone"),
                "effect_label": profile.get("effect_label"),
                "committee_note": profile.get("committee_note"),
                "sentiment": features.get("sentiment"),
                "uncertainty_score": features.get("uncertainty_score"),
                "event_risk_score": features.get("event_risk_score"),
                "news_is_stale": features.get("news_is_stale")
                if features
                else card.get("news_is_stale"),
            }
        )

    return {
        "generated_at": payload.get("generated_at"),
        "portfolio_scope": portfolio_id,
        "found": True,
        "tone": tone,
        "is_ai_generated": True,
        "model_info": "NEWS_INTELLIGENCE_OVERVIEW_HEURISTIC_V1",
        "executive_summary": executive_summary,
        "summary_bullets": summary_bullets,
        "committee_hint": committee_hint,
        "key_headlines": [h for h in enriched_headlines if h.get("title")],
        "impacted_symbols": impacted_symbols[:10],
        "metrics": {
            "symbols_total": symbols_total,
            "symbols_with_news": symbols_with_news,
            "stale_symbols": stale_symbols,
            "hot_symbols": hot_symbols,
            "risk_market_value_pct": round(risk_market_value_pct, 1),
            "proposals_with_news_context": proposals_with_news_context,
            "blocked_new_entry_count": blocked_new_entry_count,
            "avg_news_score_adj": avg_news_score_adj,
        },
        "links": {
            "news_intelligence": "/news-intelligence",
            "decision_console": "/decision-console",
        },
    }


@router.get("/feed-health")
def get_news_feed_health():
    """
    Committee-window feed stability monitor for 07:00-09:00 ET rounds.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            select
                ET_DATE,
                SOURCE_ID,
                SOURCE_NAME,
                SOURCE_TYPE,
                ENABLED_FLAG,
                POLL_MINUTES,
                ENTRIES_TODAY,
                SYMBOLS_COVERED_TODAY,
                LAST_INGESTED_AT_UTC,
                LAST_INGESTED_AT_ET,
                LAST_INGEST_AGE_MINUTES,
                ROUNDS_EXPECTED,
                ROUNDS_WITH_DATA,
                ROUND_SUCCESS_RATE,
                ROUND_0700_OK,
                ROUND_0730_OK,
                ROUND_0800_OK,
                ROUND_0830_OK,
                ROUND_0900_OK,
                MISSING_ROUNDS,
                STALE_THRESHOLD_MINUTES,
                IS_STALE,
                HEALTH_STATUS
            from MIP.MART.V_NEWS_FEED_HEALTH
            order by
                case HEALTH_STATUS
                    when 'CRITICAL' then 1
                    when 'DEGRADED' then 2
                    when 'STALE' then 3
                    when 'WARN' then 4
                    else 5
                end,
                SOURCE_ID
            """
        )
        rows = serialize_rows(fetch_all(cur))

        status_counts = {"HEALTHY": 0, "WARN": 0, "STALE": 0, "DEGRADED": 0, "CRITICAL": 0}
        total_sources = len(rows)
        stale_sources = 0
        round_coverage_total = 0.0
        biggest_gaps: list[dict[str, Any]] = []

        for r in rows:
            st = str(r.get("HEALTH_STATUS") or "WARN").upper()
            if st not in status_counts:
                status_counts[st] = 0
            status_counts[st] += 1

            if _to_bool(r.get("IS_STALE")):
                stale_sources += 1

            rr = _to_float(r.get("ROUND_SUCCESS_RATE")) or 0.0
            round_coverage_total += rr
            missing = _to_list(r.get("MISSING_ROUNDS"))
            if missing:
                biggest_gaps.append(
                    {
                        "source_id": r.get("SOURCE_ID"),
                        "source_name": r.get("SOURCE_NAME"),
                        "health_status": st,
                        "missing_rounds": missing,
                        "last_ingest_age_minutes": _to_float(r.get("LAST_INGEST_AGE_MINUTES")),
                    }
                )

        biggest_gaps.sort(
            key=lambda x: (
                len(x.get("missing_rounds") or []),
                x.get("last_ingest_age_minutes") or 0.0,
            ),
            reverse=True,
        )

        avg_round_coverage_pct = (round_coverage_total / total_sources * 100.0) if total_sources else 0.0

        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "window_contract": {
                "timezone": "America/New_York",
                "committee_rounds": ["07:00", "07:30", "08:00", "08:30", "09:00"],
                "latest_allowed_load": "09:00",
            },
            "summary": {
                "sources_total": total_sources,
                "status_counts": status_counts,
                "stale_sources": stale_sources,
                "avg_round_coverage_pct": avg_round_coverage_pct,
            },
            "largest_gaps": biggest_gaps[:10],
            "sources": [
                {
                    "et_date": _safe_iso(r.get("ET_DATE")),
                    "source_id": r.get("SOURCE_ID"),
                    "source_name": r.get("SOURCE_NAME"),
                    "source_type": r.get("SOURCE_TYPE"),
                    "enabled_flag": _to_bool(r.get("ENABLED_FLAG")),
                    "poll_minutes": _to_float(r.get("POLL_MINUTES")),
                    "entries_today": _to_float(r.get("ENTRIES_TODAY")),
                    "symbols_covered_today": _to_float(r.get("SYMBOLS_COVERED_TODAY")),
                    "last_ingested_at_utc": _safe_iso(r.get("LAST_INGESTED_AT_UTC")),
                    "last_ingested_at_et": _safe_iso(r.get("LAST_INGESTED_AT_ET")),
                    "last_ingest_age_minutes": _to_float(r.get("LAST_INGEST_AGE_MINUTES")),
                    "rounds_expected": _to_float(r.get("ROUNDS_EXPECTED")),
                    "rounds_with_data": _to_float(r.get("ROUNDS_WITH_DATA")),
                    "round_success_rate": _to_float(r.get("ROUND_SUCCESS_RATE")),
                    "round_0700_ok": _to_bool(r.get("ROUND_0700_OK")),
                    "round_0730_ok": _to_bool(r.get("ROUND_0730_OK")),
                    "round_0800_ok": _to_bool(r.get("ROUND_0800_OK")),
                    "round_0830_ok": _to_bool(r.get("ROUND_0830_OK")),
                    "round_0900_ok": _to_bool(r.get("ROUND_0900_OK")),
                    "missing_rounds": _to_list(r.get("MISSING_ROUNDS")),
                    "stale_threshold_minutes": _to_float(r.get("STALE_THRESHOLD_MINUTES")),
                    "is_stale": _to_bool(r.get("IS_STALE")),
                    "health_status": str(r.get("HEALTH_STATUS") or "").upper(),
                }
                for r in rows
            ],
        }
    finally:
        conn.close()
