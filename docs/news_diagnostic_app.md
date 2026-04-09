# MIP news stack — app / API diagnostic

## Backend routes ([news.py](../../apps/mip_ui_api/app/routers/news.py))

| Route | Purpose | Primary data |
|-------|---------|----------------|
| `GET /news/intelligence` | Full snapshot + portfolio overlay + proposal news fields | `MIP.MART.V_NEWS_AGG_LATEST`, `ORDER_PROPOSALS`, positions |
| `GET /news/intelligence/overview` | Cockpit tile; **calls `get_news_intelligence()` then post-processes** | Same + `MIP.MART.V_NEWS_FEATURES_BY_TS` for headline tone |
| `GET /news/feed-health` | Committee-window health | `MIP.MART.V_NEWS_FEED_HEALTH` |
| `POST /news/refresh-ibkr` | Triggers IBKR script via agent runtime | Not warehouse-read |

### `/news/intelligence` SQL behavior (faithfulness)

- Reads **`V_NEWS_AGG_LATEST`** as `TOP_HEADLINES` ← actually column **`TOP_CLUSTERS`** from aggregate pipeline, exposed as JSON array of objects with `headline`, `url`, `source_id`, etc. **API renames mentally** — it maps `TOP_CLUSTERS` into `TOP_HEADLINES` in the SELECT list as alias `TOP_HEADLINES` (variant).
- Staleness: `NEWS_IS_STALE` if `ITEMS_TOTAL > 0` and `datediff(minute, coalesce(LAST_INGESTED_AT, SNAPSHOT_TS), current_timestamp()) > NEWS_STALENESS_THRESHOLD_MINUTES` (default **180**).
- **Python-side `_normalize_headlines`**: drops entries without title; dedupes by **`title||url`**; strips non-http URLs and mock/rss patterns. This can **drop** rows relative to raw variant but usually aligns with UX.

### `/news/intelligence/overview` specifics

- Reuses **`get_news_intelligence`** → **identical warehouse snapshot** as main page for underlying metrics.
- **`is_ai_generated`: true** and `model_info: NEWS_INTELLIGENCE_OVERVIEW_HEURISTIC_V1` — narrative is **rule-based**, not an LLM.
- **`_headline_signal_profile`** maps sentiment / uncertainty / event_risk / stale to emoji + committee text.

## Frontend

| Page | Endpoint | Refresh | Notes |
|------|----------|---------|-------|
| [NewsIntelligence.jsx](../../apps/mip_ui_web/src/pages/NewsIntelligence.jsx) | `GET /news/intelligence` | Once on mount (`useEffect`, `[]`) | Persists `mip.newsIntelligence.lastSeenGeneratedAt` |
| [Home.jsx](../../apps/mip_ui_web/src/pages/Home.jsx) | Same | On mount + when `defaultPortfolioId` changes | Uses `market_context.top_headlines` slice `[:3]` |
| [Cockpit.jsx](../../apps/mip_ui_web/src/pages/Cockpit.jsx) | `GET /news/intelligence/overview` | On Cockpit load | Different endpoint but **same underlying intelligence payload** |
| [LearningLedger.jsx](../../apps/mip_ui_web/src/pages/LearningLedger.jsx) | Ledger APIs | n/a | Displays `news_context` strings from **ledger feed**, not live news API |

### Repetition / staleness — backend vs frontend

- **No shared client cache** between Home and News Intelligence: each mount refetches.
- **Cockpit** can show overlapping headline **titles** with Home because both derive from the **same ranking** (`V_NEWS_AGG_LATEST` + API top-headline selection), not because React reuses state.
- **Stale “feeling”** when `LAST_PUBLISHED_AT` is old but `SNAPSHOT_TS` is fresh is primarily **warehouse/ranking** (e.g. no newer mapped items in bucket), not browser cache.

### Dev-only instrumentation

On **News Intelligence**, set `localStorage.setItem('mip_news_diag','1')` and open the page in **dev** build. Console logs `[mip_news_diag]` with client request time, `generated_at`, and headline keys. Remove flag to disable.

## Display proxy vs actual display

- **API response** = what the app *can* show (plus `_normalize_headlines` filtering).
- **What the user saw** is **not logged** anywhere in production (no `DISPLAYED_TS`, no payload hash table). Treat UI as **unknown** without instrumentation or server logs.

## Example trace — AAPL (2026-04-08 Snowflake)

| Stage | Evidence |
|-------|----------|
| Raw | Mapped items exist from RSS sources (not shown in one row here) |
| Aggregate | `NEWS_AGGREGATED_EVENTS`: `BADGE` QUIET, `LAST_PUBLISHED_AT` **2026-04-02**, `SNAPSHOT_TS` **2026-04-08**, `TOP_CLUSTERS` = single **MARKETWATCH_RSS** story |
| API | `/news/intelligence` would surface that cluster in `symbol_cards` / `top_headlines` if symbol ranks high enough |
| Interpretation | **Snapshot fresh, headline content old** — ranking/feed mix, not React cache |

## Example trace — SEC filing row

`NEWS_ID` `8a6c0bc143b36dc0f5c73b7b40cc093fc7d1e0b6fb4119aafd78b52ca33bc3bb`, `SOURCE_ID` `SEC_XBRL_ALL`, title `Holley Inc. (0001822928) (Filer)`, `PUBLISHED_AT` ≈ `INGESTED_AT` within ~1h on 2026-04-08. **Separate URLs** for each filing → **URL dedupe does not collapse** “(Filer)” boilerplate titles.
