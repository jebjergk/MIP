# MIP news diagnostic — final memo

**Evidence date:** Snowflake queries run **2026-04-08** as `CURSOR_AGENT` / `MIP_ADMIN_ROLE`.  
**Companion docs:** [news_diagnostic_architecture.md](news_diagnostic_architecture.md), [news_diagnostic_app.md](news_diagnostic_app.md), SQL pack [../SQL/scripts/news_diagnostic/](../SQL/scripts/news_diagnostic/).

---

## Executive summary

The news stack is **operationally running** (tasks **started**, `SP_REFRESH_NEWS_CONTEXT` **succeeding**, `MIP.APP.MIP_AUDIT_LOG` showing full `NEWS_PIPELINE` steps). **Ingest volume is dominated by `SEC_XBRL_ALL`** (~83% of all-time rows in this account). Several **ticker RSS sources are effectively dead** (last ingest **2026-03-10**). **URL-only dedupe** does not collapse SEC “(Filer)” **title boilerplate** across different URLs. **`V_NEWS_FEED_HEALTH` is not 24h ingestion truth** — it only scores committee-window rounds. **Morning brief JSON does not embed a news section.** Stored **proposal `news_score_adj` is always zero** in sampled history (90d), while **`news_context` / `news_agg` JSON is often present** — so news is **wired for context and guardrails more than measurable score nudges**. **There is no display audit trail**; “what users saw” is **unknowable** without new instrumentation.

**Recommendation:** **Keep but repair specific stages** — (1) refresh or retire stale ticker feeds and align `NEWS_SOURCES` with reality, (2) treat SEC volume separately in UX/diagnostics (filings ≠ headlines), (3) add minimal **display/API audit** if you need trust metrics, (4) only then revisit dedupe/ranking if product still misses the bar.

---

## Mandatory questions — explicit answers

### 1) What sources are active and actually ingesting?

**Actively receiving rows (7d sample):** `SEC_XBRL_ALL`, `GLOBENEWSWIRE_RSS`, `MARKETWATCH_RSS`, `FED_RSS` (minimal).  
**Configured in `NEWS_SOURCES` but absent from `NEWS_RAW` in this account:** e.g. **`SEC_RSS_INDEX`** not in distinct `SOURCE_ID` list.  
**Effectively inactive (0 rows 7d; last ingest March 10):** `YAHOO_TICKER_RSS`, `SEEKING_ALPHA_TICKER_RSS`, `NASDAQ_TICKER_RSS`.  
**ECB:** 0 rows in 7d.  
**IBKR:** `IBKR_NEWS_API` appears on **`V_NEWS_FEED_HEALTH`** with **0** committee-window rows; **no `NEWS_RAW` rows** for that source in this DB.

### 2) How current is news at each stage?

| Layer | Finding |
|-------|---------|
| Publication | `MAX(PUBLISHED_AT)` in `NEWS_RAW` **2026-04-08** (SEC/Globe/MW); ticker sources stale |
| Warehouse ingest | `MAX(INGESTED_AT)` **2026-04-08 13:00** |
| Aggregate `SCORED_TS` | e.g. AAPL row **`SNAPSHOT_TS` 2026-04-08** but **`LAST_PUBLISHED_AT` 2026-04-02** for top cluster |
| API | `generated_at` is **request time**; data from latest `V_NEWS_AGG_LATEST` |
| UI render | **No `DISPLAYED_TS`** |

**Ingest lag:** All sources 30d: **p50 ~735 min** (driven by SEC filing timestamps vs ingest). **Excluding `SEC_XBRL_ALL`:** **p50 ~120 min**, **p90 ~6017 min** (long tail).

### 3) Where do failures happen most often?

- **Scheduling / window:** Outside committee slots, **no automated full refresh** when `NEWS_COMMITTEE_WINDOW_ENFORCED = true`.
- **Source death:** Ticker RSS **silent stop** (no rows, no task failure).
- **Misleading success:** `NEWS_INGEST` **STATUS SUCCESS** with **`rows_inserted: 0`** and **`rows_staged: 235`** — normal **MERGE idempotency**, not failure.
- **Mapping:** **`mapped_rows_merged: 0`** on repeated runs — can be **no new unmapped rows**, not necessarily a bug.

### 4) How much duplication / near-duplication?

- **30d:** **6630** rows, **6484** distinct URLs (**~146** duplicate URL rows vs `NEWS_ID` uniqueness).
- **Titles:** **3933** distinct normalized titles vs **6630** rows → **heavy title repetition**, especially SEC **“(FILER)”** lines (e.g. **57** rows same normalized title in 7d for one bank filer pattern).
- **Dedupe:** **`NEWS_DEDUP`** large clusters = **same canonical URL repeated**, not cross-URL story merge.

### 5) How often is the same story/cluster resurfaced?

- **Same URL cluster** can be **large** (`CLUSTER_SIZE` up to **31** in `NEWS_DEDUP` distribution).
- **Cross-page UI:** Home + Cockpit can show the **same logical story** because **ranking is shared**, not because of client-side cache.

### 6) Is one source dominating ranked/displayed news?

**Raw 30d share:** **`SEC_XBRL_ALL` ~83%** of all-time rows; **~5517/6630** in 30d from SEC in per-source table.  
**Ranked panels:** Symbol **TOP_CLUSTERS** can still be **MarketWatch** when that’s the only/best cluster for that symbol (see AAPL example) — **ingestion dominance ≠ same as headline dominance per symbol**.

### 7) How good is symbol mapping for recent traded names?

- **Methods:** `subscription_hint` (1418), `company_name_match` (1329), `ticker_regex` (853), `alias_dict` (29) — **all-time counts**.
- **Unmapped 7d:** **1315** `NEWS_RAW` rows **without** any `NEWS_SYMBOL_MAP` row.
- **Ambiguity:** Deterministic token rules (e.g. **APA**) need **case-by-case** headline review — not fully answered by SQL alone.

### 8) Company vs sector vs macro vs noise?

- **SEC XBRL titles** are often **issuer metadata**, not tradable “headlines.”
- **GlobeNewswire / MarketWatch** closer to **narrative market news**.
- **Fed/ECB** low volume; **macro underrepresented** in raw row counts.

### 9) Stale/repeated content — backend or frontend?

- **Backend/warehouse** explain **old `LAST_PUBLISHED_AT` with fresh `SNAPSHOT_TS`**.
- **Frontend** refetches on mount; **no** strong evidence of stale React state driving repetition **across** Home vs News Intelligence.

### 10) Is news helping decisions, or decorative / misleading?

- **87 / 112** proposals (30d) have **`news_context` or `news_agg` in `SOURCE_SIGNALS`**.
- **0 / 90d** proposals with **non-zero `news_score_adj`** in `SOURCE_SIGNALS` (numeric influence **not visible** in stored JSON).
- **Morning brief:** **0** briefs in 30d with substring **`news`** in `BRIEF` JSON.
- **Verdict:** **Partially helpful** as **context + policy/caution inputs**; **not proven** as a strong **ranking driver** from persisted proposal fields. **Misleading risk** comes from **volume-heavy SEC filing rows** looking like “news” without editorial framing.

---

## Strongest confirmed findings (examples)

1. **Repo vs environment:** DDL files **`alter task … suspend`**; **deployed tasks were `started`** — always verify with **`show tasks`**.
2. **Silent ingest “failure to add rows”:** Audit log **`NEWS_INGEST` SUCCESS, `rows_inserted`: 0** — expect idempotent merges.
3. **AAPL aggregate row:** `SNAPSHOT_TS` **2026-04-08**, top cluster **MarketWatch** **2026-04-02** — **fresh rollup, aged story**.
4. **SEC sample `NEWS_ID`:** `8a6c0bc143b36dc0f5c73b7b40cc093fc7d1e0b6fb4119aafd78b52ca33bc3bb` — **Holley Inc. (Filer)** URL unique, **title pattern repeats** across other CIKs.
5. **Ticker RSS:** **Last ingest 2026-03-10** for Yahoo / Seeking Alpha — **silent source death**.

---

## Top 10 Snowflake-side issues

1. **Ticker RSS sources stale** (no 7d rows).
2. **`NEWS_SOURCES` vs reality** — e.g. **`SEC_RSS_INDEX`** not ingesting in this account.
3. **IBKR path** not evidenced in **`NEWS_RAW`** here.
4. **SEC row volume** dominates; skews **lag percentiles** and **“news” feel**.
5. **Title near-duplicates** for SEC not deduped (by design — URL key).
6. **1315 unmapped** raw rows (7d) — mapping coverage gap.
7. **`V_NEWS_FEED_HEALTH` CRITICAL** for ECB/FED/IBKR on sameday committee view — **expected** if those feeds are quiet or off-window; easy to **over-interpret**.
8. **Large `NEWS_DEDUP` clusters** — same URL re-seen many times.
9. **Committee window** blocks refresh outside slots — **by design**, affects freshness off-schedule.
10. **No `DISPLAYED_TS`** — observability gap.

---

## 1–5 scorecard

| Dimension | Score | Why |
|-----------|-------|-----|
| Ingestion reliability | **3** | Core RSS + tasks work; several sources stale; IBKR not proven in data |
| Freshness | **3** | Ingest current; **content** can be old per symbol; SEC lag skews metrics |
| Source diversity | **2** | SEC dominance; macro/ticker weak |
| Dedupe quality | **2** | URL-only; good for tracking URLs, weak for “same story” |
| Symbol relevance | **3** | Mixed methods; significant unmapped volume |
| Backend fidelity | **4** | API closely follows `V_NEWS_AGG_LATEST`; light headline normalization |
| Frontend fidelity | **4** | Straightforward fetch; overlap is ranking-driven |
| Auditability | **1** | No display trail |
| Decision usefulness | **3** | Context present; numeric adj absent in samples; brief omits news |
| Maintenance burden | **3** | Many sources + Python ingest + window rules |

---

## Recommendation

**Keep but repair specific stages** (see executive summary). **Do not** rebuild until stale sources, SEC framing, and observability targets are clear.

**Optional debug page:** Deferred — SQL + `/news/intelligence` JSON sufficient for this pass. Revisit if operators need live cluster IDs in UI.

---

## What was changed (this task)

- **Added** [../SQL/scripts/news_diagnostic/](../SQL/scripts/news_diagnostic/) (diagnostic SQL only — **not deployed** as objects).
- **Added** [news_diagnostic_architecture.md](news_diagnostic_architecture.md), [news_diagnostic_app.md](news_diagnostic_app.md), this memo.
- **Added** dev-only console diagnostic in `NewsIntelligence.jsx` when `localStorage mip_news_diag === '1'`.

**Deployed to Snowflake:** **Nothing** (read-only measurement + local scripts/docs).

**Your action required:** None for read-only findings. If you want display audit, approve a small logging/table design next.
