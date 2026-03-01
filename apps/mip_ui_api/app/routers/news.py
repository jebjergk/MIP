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
        return []
    if not isinstance(raw, list):
        return []
    out: list[dict[str, Any]] = []
    for h in raw:
        if not isinstance(h, dict):
            continue
        title = h.get("title") or h.get("TITLE")
        if not title:
            continue
        out.append(
            {
                "title": str(title),
                "url": h.get("url") or h.get("URL"),
            }
        )
    return out


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
            select
                n.AS_OF_DATE,
                n.SYMBOL,
                n.MARKET_TYPE,
                n.NEWS_COUNT,
                n.NEWS_CONTEXT_BADGE,
                n.NOVELTY_SCORE,
                n.BURST_SCORE,
                n.UNCERTAINTY_FLAG,
                n.TOP_HEADLINES,
                n.LAST_NEWS_PUBLISHED_AT,
                n.LAST_INGESTED_AT,
                n.SNAPSHOT_TS,
                n.NEWS_SNAPSHOT_AGE_MINUTES,
                n.NEWS_IS_STALE,
                n.NEWS_STALENESS_THRESHOLD_MINUTES
            from MIP.MART.V_NEWS_INFO_STATE_LATEST_DAILY n
            order by
                n.NEWS_IS_STALE asc,
                n.NEWS_COUNT desc,
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
            is_risk = _to_bool(news.get("NEWS_IS_STALE")) or _to_bool(news.get("UNCERTAINTY_FLAG")) or (
                (news.get("NEWS_CONTEXT_BADGE") or "").upper() == "HOT"
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
                        "source_table": "MIP.MART.V_NEWS_INFO_STATE_LATEST_DAILY",
                        "join_contract": "symbol+market_type latest snapshot_ts <= as_of_ts",
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
            "lineage": {
                "tables": [
                    "MIP.MART.V_NEWS_INFO_STATE_LATEST_DAILY",
                    "MIP.MART.V_PORTFOLIO_OPEN_POSITIONS_CANONICAL",
                    "MIP.MART.MARKET_BARS",
                ],
                "prompt_version": None,
                "model_name": None,
                "input_hash": None,
            },
        }
    finally:
        conn.close()
