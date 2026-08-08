# Brooks INTRADAY_ADVISER — static RAG deployment plan (approval gate)

**Status:** **Deployed** 2026-08-07 (`v1_static_2026-08-07`). Full JSON log: `cursorfiles/brooks_intraday_adviser_rag_deploy_report.json`.

**Corpus source:** `cursorfiles/brooks_intraday_adviser_corpus.json` (built by `cursorfiles/brooks_intraday_adviser_corpus_build.py`).

**Policy:** `MIP/docs/brooks_adviser_long_only_rag_policy.md`

**SQL sketch:** `MIP/SQL/knowledge/080_literature_intraday_adviser_rag_corpus.sql`

---

## Scope boundaries

| Touch | Action |
|--------|--------|
| `RAG_SCOPE = INTRADAY_ADVISER` | New rows + new Cortex Search service |
| `RAG_SCOPE = REVALIDATION` | **No change** |
| LPA, V0.3/V0.4, Week 3, AMZN replay, Adviser runtime | **No change** |

---

## Target objects

| Object | Role |
|--------|------|
| `MIP.KNOWLEDGE.LITERATURE_INTRADAY_ADVISER_CORPUS_STAGING` | One-time JSON load |
| `MIP.KNOWLEDGE.V_LITERATURE_INTRADAY_ADVISER_RAG_CORPUS` | Search-facing view; drops `EXCLUDED_SHORT_EXECUTION` |
| `MIP.KNOWLEDGE.LITERATURE_RAG_DOCUMENT` | Shared document table; **insert only** `RAG_SCOPE='INTRADAY_ADVISER'` |
| `MIP.KNOWLEDGE.SP_LOAD_INTRADAY_ADVISER_RAG_DOCUMENTS` | Delete Adviser scope rows + insert from view |
| `MIP.KNOWLEDGE.LITERATURE_INTRADAY_ADVISER_SEARCH_SERVICE` | **New** Cortex Search (separate from `LITERATURE_REVALIDATION_SEARCH_SERVICE`) |

---

## Metadata columns (filters)

Stored on each document (columns and/or `RAW_CARD` variant):

| Field | Values / notes |
|--------|----------------|
| `EXECUTION_DIRECTION` | Always `LONG_ONLY` |
| `EXECUTION_RELEVANCE` | `LONG_ENTRY`, `LONG_CONTEXT`, `LONG_MANAGEMENT`, `LONG_FAILURE`, `BEARISH_CONTEXT_ONLY` (never index `EXCLUDED_SHORT_EXECUTION`) |
| `ADVISER_CLASS` | `ADVISER_CORE`, `ADVISER_SUPPORTING`, `LONG_MANAGEMENT`, `BEARISH_CONTEXT_FOR_LONGS` |
| `ADVISER_METADATA` (variant) | `setup_family`, `decision_stage`, `adviser_role`, `intraday_compatibility`, `source_book`, `adviser_execution_tags` |
| Lineage | `CARD_ID`, `SOURCE_BOOK`, `CONCEPT_NAME` (unchanged from literature card) |

**Search column:** `SEARCH_TEXT` (= corpus `retrieval_text` + guardrail footer)

**Display column:** `DISPLAY_TEXT` (structured card for LLM context)

**Cortex attribute filters:** `EXECUTION_DIRECTION`, `EXECUTION_RELEVANCE`, `ADVISER_CLASS`, `CONCEPT_FAMILY`, `SOURCE_BOOK`, `CARD_ID`

---

## Retrieval guardrails (Adviser runtime)

All Adviser RAG calls:

1. `EXECUTION_DIRECTION = 'LONG_ONLY'`
2. Exclude `EXECUTION_RELEVANCE = 'EXCLUDED_SHORT_EXECUTION'` (rows not in view)

**Long entry / watch intents** — allow only:

`LONG_ENTRY`, `LONG_CONTEXT`, `LONG_FAILURE`, `BEARISH_CONTEXT_ONLY`

**Position management intents** — also allow:

`LONG_MANAGEMENT`

Example Cortex filter (entry/watch):

```sql
filter => '@EXECUTION_DIRECTION = ''LONG_ONLY''
  AND @EXECUTION_RELEVANCE IN (''LONG_ENTRY'',''LONG_CONTEXT'',''LONG_FAILURE'',''BEARISH_CONTEXT_ONLY'')'
```

Adviser action schema remains **long-only** (no short action enum).

---

## One-time indexing steps (after approval)

1. Deploy `080_literature_intraday_adviser_rag_corpus.sql` (staging + view + procedure).
2. Load staging from approved JSON (Snowflake `PUT` + `COPY`, or scripted `INSERT` from agent — **one batch**).
3. `CALL MIP.KNOWLEDGE.SP_LOAD_INTRADAY_ADVISER_RAG_DOCUMENTS('v1_static_<date>');`
4. Uncomment and deploy `CREATE CORTEX SEARCH SERVICE … LITERATURE_INTRADAY_ADVISER_SEARCH_SERVICE`.
5. Wait for initial index build (`SHOW CORTEX SEARCH SERVICES` → `indexing_state`).
6. Run validation queries (below).
7. Set `target_lag = '365 days'` (or disable scheduled refresh) — **static corpus**; serving stays up; re-index only on explicit corpus version bump.

---

## Validation queries

```sql
-- Row counts by scope (REVALIDATION unchanged)
SELECT RAG_SCOPE, COUNT(*) FROM MIP.KNOWLEDGE.LITERATURE_RAG_DOCUMENT GROUP BY 1;

-- Adviser execution relevance mix
SELECT EXECUTION_RELEVANCE, COUNT(*)
FROM MIP.KNOWLEDGE.V_LITERATURE_INTRADAY_ADVISER_RAG_CORPUS
GROUP BY 1 ORDER BY 2 DESC;

-- Must be zero
SELECT COUNT(*) FROM MIP.KNOWLEDGE.V_LITERATURE_INTRADAY_ADVISER_RAG_CORPUS
WHERE EXECUTION_RELEVANCE = 'EXCLUDED_SHORT_EXECUTION';

-- Held-back spot check: sample IDs from report held_needs_review must not appear
SELECT CARD_ID FROM MIP.KNOWLEDGE.V_LITERATURE_INTRADAY_ADVISER_RAG_CORPUS
WHERE CARD_ID IN (/* paste held-back sample from report */);

-- Semantic smoke (post-index)
SELECT CARD_ID, CONCEPT_NAME, EXECUTION_RELEVANCE
FROM TABLE(
  SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'MIP.KNOWLEDGE.LITERATURE_INTRADAY_ADVISER_SEARCH_SERVICE',
    'failed bull breakout avoid long',
    5,
    filter => '@EXECUTION_DIRECTION = ''LONG_ONLY'' AND @EXECUTION_RELEVANCE IN (''BEARISH_CONTEXT_ONLY'',''LONG_FAILURE'')'
  )
);
```

---

## Suspend indexing while serving remains available

- Static corpus: set **`target_lag = '365 days'`** after first successful index.
- Do **not** call `SP_LOAD_INTRADAY_ADVISER_RAG_DOCUMENTS` on a schedule.
- Document corpus version in `RAW_CARD:corpus_build_version`.
- Future updates: new approval → reload staging → `CALL SP_LOAD…` → **manual** `ALTER CORTEX SEARCH SERVICE … REFRESH`.

---

## Rollback / removal

1. `DROP CORTEX SEARCH SERVICE IF EXISTS MIP.KNOWLEDGE.LITERATURE_INTRADAY_ADVISER_SEARCH_SERVICE;`
2. `DELETE FROM MIP.KNOWLEDGE.LITERATURE_RAG_DOCUMENT WHERE RAG_SCOPE = 'INTRADAY_ADVISER';`
3. `TRUNCATE TABLE MIP.KNOWLEDGE.LITERATURE_INTRADAY_ADVISER_CORPUS_STAGING;` (optional)
4. `DROP VIEW IF EXISTS MIP.KNOWLEDGE.V_LITERATURE_INTRADAY_ADVISER_RAG_CORPUS;`
5. REVALIDATION service and rows **untouched**.

---

## Retrieval smoke-test plan (post-index)

See `brooks_intraday_adviser_corpus_report.json` → `retrieval_smoke_test_plan`.

Each query validates **Brooks domains / card themes**, not a trading action.

| # | Query | Expected domains / themes |
|---|--------|---------------------------|
| 1 | Second entry long after two-legged pullback in trading range | H1/H2, second entry, two-legged pullback, trading range |
| 2 | Breakout with weak follow-through | failed breakout, follow-through, trap/range |
| 3 | Failed bull breakout — avoid long? | failed bull, BEARISH_CONTEXT_ONLY |
| 4 | Bull trend pullback near support | pullback, trend, support/MA |
| 5 | Double bottom reversal after strong bear move | double bottom, reversal qualifiers |
| 6 | Signal bar quality before long entry | signal bar, entry confirmation |
| 7 | Climax — exit/protect existing long? | climax, EXIT_RISK, management |
| 8 | Bull channel trade management | channel, LONG_MANAGEMENT |

---

## User action

**Approve this plan + final corpus statistics**, then authorize a single deployment task to run the steps above.
