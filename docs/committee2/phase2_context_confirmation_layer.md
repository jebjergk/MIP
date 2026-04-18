# Phase 2 — Context confirmation layer (implementation spec)

## What Phase 2 is (and is not)

**Phase 2** adds a **low-weight context confirmation layer** to Committee 2.0: **U.S. politician trade disclosure context** (STOCK Act–style filings and equivalents, aggregated and normalized for trading use) **mapped to issuer / symbol**, shown as a **compact context card** on LPA inline `inline_hearing` and the **Structural Committee Hearing** page.

- **Phase 2 is not** bounded LLM phrasing, narrative “politician” copy generation, or any model that rewrites committee logic. **Bounded LLM phrasing is Phase 3** and must not ship inside Phase 2.

- **Phase 2 is** **deterministic first** for anything that affects **core** context: stored rows in Snowflake → normalize → join to symbol/issuer → rule-based classification → render. Optional caching and freshness rules only; no stochastic ranking that could be mistaken for an edge signal.

### Domain and source intent (explicit)

| Concept | Meaning |
|--------|---------|
| **Domain** | **Politician trade disclosure context** — who filed, what instrument, buy/sell (or equivalent), trade vs disclosure dates, chamber, provenance. **Not** generic “public disclosure” (earnings, SEC 8-K, news, social sentiment, etc.). |
| **First source concept** | **[Capitol Trades](https://www.capitoltrades.com/)** (or the same underlying disclosures accessed elsewhere) as the **practical** window on U.S. legislative trade reporting — i.e. curated **politician–trade–issuer** facts, not MIP’s invention of a new dataset. |
| **Legal/substance anchor** | Underlying **House/Senate periodic transaction reports** and STOCK Act–style disclosure rules; ingest may be direct from official feeds, via aggregator normalization, or manual curation — **provenance must remain attributable** (`source`, `source_url`). |
| **App-facing store** | A **curated Snowflake table** (e.g. `MIP.APP.PUBLIC_DISCLOSURE_TRANSACTION`) is the **contract** the API reads. That table **means**: *politician trade disclosure rows mapped to symbol/issuer for Committee 2.0 context only* — **not** an arbitrary dump of unrelated public records. |

## Role of the layer (strict)

The card is **supportive / neutral / contradictory context only**:

| Must | Must not |
|------|----------|
| Inform the human operator | Act as a **primary** trading signal |
| Sit beside geometry, path, regime, protection | **Trigger** proposals or entries |
| Reflect **sourced politician trade disclosures** | **Override** structure, geometry, path, or broker-safe gates |
| Degrade gracefully when data is missing | **Increase stance** or “rescue” a weak setup from context alone |

**Hard constraints (Phase 2):**

- **No new primary signal logic** — engine stance, caps, confidence, and LIVE materialization **do not** read this context for decisions.
- **No proposal generation changes.**
- **No stance lift from context alone**; **no rescuing** weak structure / geometry / path.
- **No integration into core committee stance/chair logic** — exhibit is a **sibling** on the payload, never merged into engine inputs.
- **Compact card only** (no heavy charts, no second scroll region inside the card).
- **Deterministic first** for core scoring and copy derived from structured fields.
- **LPA** `inline_hearing` and **full hearing** page may both display the same exhibit shape.
- **Bounded LLM phrasing is Phase 3**, not Phase 2.

## Crawl / ingest scope (priority order)

Work on ingest should follow this order. **Only tier 1 is required** to ship a credible Phase 2 card; tiers 2–3 are optional enrichment.

### 1) Structured transaction data first (required for core card)

Minimum to treat Phase 2 as “real” for a symbol:

- **Trades** and **trade details**: side (buy/sell/exchange-normalized), **trade date**, **disclosure/filed date**, volume or notional **range** (and price **if available** from source).
- **Politician name** (public display).
- **Chamber** (or equivalent role metadata).
- **Issuer / symbol** (after mapping pass).
- **Asset type** if available (equity, option, etc.).

The **context card is built primarily from this tier.** Rule-based fields such as `tone_vs_trade` must be driven only by **tier-1 structured facts** (and explicit deterministic rules), not by narrative.

### 2) Politician profile / activity enrichment second (optional)

If cleanly derivable **deterministically** from stored data (aggregates over ingested rows), optional **light** summary facts may be added, for example:

- Total trades (in scope / in window).
- Total disclosed volume or range (if derivable without guesswork).
- Last traded / last disclosure date for that politician–symbol lens.
- Recent activity frequency (bucketed counts, not hype).
- Broad buy/sell tendency (counts — same conservative rules as tier 1).

These **must not** replace tier 1 as the primary grid; they **must not** drive “rescuing” or stance-like behavior.

### 3) Insights, buzz, press last (optional, non-core)

- **Insights**, **buzz**, **press**, **politician trading habit commentary**, and similar **narrative** sources are **out of scope for core context scoring** in the first implementation.

They may appear **only** as **optional, light enrichment** when **cleanly available** (e.g. a single factual link or one neutral line with provenance). They **must not** drive `tone_vs_trade`, mapping quality, or any numeric that could be read as a committee-adjacent signal.

## Stored layer vs optional live lookup

| Layer | Role |
|-------|------|
| **Snowflake (primary)** | **Deterministic, required path for Phase 2.** The hearing **must remain fully functional** with **only** stored rows. Refresh/rebuild behavior is unchanged if live enrichment is absent or disabled. |
| **Live crawl / lookup (optional)** | **Best-effort, non-blocking bonus.** At hearing time, an implementation **may** consult an external source (e.g. aggregator or API) for **recent, symbol-relevant** politician disclosure context. |

**Live enrichment rules:**

- **Not a hard dependency** — timeouts, errors, empty results, or “nothing relevant” **must not** break or block the hearing.
- **Silent failure** — **no** user-visible error, **no** “failed to fetch” state, **no** degraded banner for this feature.
- **Same UX for “no finding” and “failed lookup”** — the user sees **no extra** live exhibit; **indistinguishable** from simply having nothing to show.
- If live enrichment **succeeds** and returns something **recent and relevant**, it may surface as a **separate, low-weight** optional exhibit (e.g. `exhibit_live_politician_disclosure_context`) or an explicitly marked subsection — **still** not merged into chair/stance/engine inputs, **still** compact, **still** deterministic presentation of fetched facts (no LLM in Phase 2).

Until live enrichment exists, **only** the stored exhibit appears when the flag is on.

## Surfaces

| Surface | Behavior |
|---------|----------|
| **LPA** inline `inline_hearing` | Same compact card (or collapsed row) as hearing page |
| **Structural Committee Hearing** (standalone) | Same card in the evidence area |

Keep copy and layout **one component / one DTO shape** where practical so the two surfaces do not drift.

**UI labeling:** Prefer a **neutral** title such as **“Public disclosure context”** (user-facing) while tooltips or footer may clarify **politician trade disclosures** — avoid hype labels (“alpha”, “smart money”, “conviction signal”).

## Data model (deterministic first)

**Source of truth:** curated rows in Snowflake representing **politician trade disclosures** joinable by:

- **Issuer identity** (CIK / LEI / normalized issuer key) preferred where available, and/or  
- **Symbol + market type** as a practical join key for MIP’s universe.

**Mapping pass (deterministic):**

- Rules-based symbol extraction from free text where possible.  
- Join to reference symbols or MIP symbol dimension; **unmapped** → neutral empty state for that symbol, **never** fabricated links.

**Freshness:**

- Card shows **as-of** ingest timestamp and **dates** from disclosures (not “live political advice”).

### Minimum required fields (logical; align DDL to this contract)

Fields the ingest **should** populate for tier-1 usefulness (names may match existing columns):

| Field | Purpose |
|-------|---------|
| Stable **disclosure / transaction id** | Dedup and lineage |
| **Politician / filer display name** | UI |
| **Chamber** (or equivalent) | UI / filter |
| **Transaction type / side** | Rule-based tone |
| **Trade date** | Ordering, recency |
| **Filed / disclosure date** | Ordering, recency |
| **Mapped symbol** (and issuer key when available) | Join to proposal |
| **Asset class / type** | Context |
| **Volume / notional range** (and **price** if available) | Factual display |
| **Source name** + **source URL** | Provenance; link-out only |
| **Ingested at** | As-of |

## API / bundle shape

**Attachment point:** extend the **assembled hearing payload** and **`inline_hearing`** with a sibling block, e.g. `exhibit_public_disclosure_context`, **never** merged into `chair`, `stance`, or `evidence` fields used by the engine.

**Suggested DTO (versioned):**

```json
{
  "schema_version": "1",
  "symbol": "XYZ",
  "market_type": "STOCK",
  "as_of_utc": "2026-04-20T12:00:00Z",
  "mapping_quality": "MAPPED | PARTIAL | UNMAPPED",
  "tone_vs_trade": "SUPPORTIVE | NEUTRAL | CONTRADICTORY | UNKNOWN",
  "summary_lines": ["Line 1 factual", "Line 2 factual"],
  "recent_transactions": [
    {
      "filed_date": "2026-04-01",
      "transaction_date": "2026-03-15",
      "side": "BUY",
      "filer_display_name": "…",
      "source_url": "https://…"
    }
  ],
  "disclaimer": "Politician trade disclosures only; not a trade signal."
}
```

**Rules for `tone_vs_trade` in Phase 2 (deterministic, conservative):**

- Default **`UNKNOWN`** or **`NEUTRAL`** when ambiguous.  
- **`CONTRADICTORY`** / **`SUPPORTIVE`** only when explicit rules match structured sides vs proposal direction — **no ML**; spec rules in SQL/Python in one place.

**Optional live exhibit (future):** e.g. `exhibit_live_politician_disclosure_context` — only present when a best-effort fetch returns **usable** facts; **omitted** when unavailable (same as failure).

**Orchestrate:** `POST .../committee2/orchestrate` may attach the same stored block into `inline_hearing` from Snowflake (no LLM call).

## UI — compact card only

- **One primary card** per hearing for stored context: neutral title + badges for `mapping_quality` / `tone_vs_trade`.  
- **Max 2–3 summary lines** + **small list** (≤5 rows) of recent mapped transactions with dates and link-out.  
- **Disclaimer** line always visible (footer).  
- **No charts**, no sparklines, no second scroll region inside the card.

## Engineering milestones (suggested order)

1. **Snowflake:** curated **politician trade disclosure** table(s); deterministic map-to-symbol job or view; document provenance and refresh cadence (Capitol Trades or official sources).  
2. **API:** read path from stored rows; unit tests for mapping and tone rules.  
3. **Hearing:** `exhibit_public_disclosure_context` on `_assemble_payload` / refresh path without touching `compute_hearing_bundle`.  
4. **Orchestrate:** include block in `inline_hearing` builder in [`live.py`](../apps/mip_ui_api/app/routers/live.py).  
5. **Web:** shared context card in LPA exhibits + `StructuralCommitteeHearing.jsx`.  
6. **Flag:** `COMMITTEE2_CONTEXT_DISCLOSURE_ENABLED` — see [`04_rollout_and_flags.md`](./04_rollout_and_flags.md); default off until data is trusted.  
7. **Optional:** silent, non-blocking live lookup + separate low-weight exhibit when successful.

## Verification

- With flag **off**: no card, no API errors.  
- With flag **on**, symbol **unmapped**: neutral empty state, no crash.  
- With live enrichment **enabled** (if implemented): failure / empty **indistinguishable** to the user — no extra card, no error.  
- **Regression:** engine tests unchanged; stance distribution unchanged with flag on (context not fed into engine).

## Phase 3 boundary

Phase 3 may add **bounded LLM** to turn the same factual DTO into softer language **after** Phase 2 facts are frozen — still **no** override of deterministic committee outcomes unless explicitly redesigned in a future charter.

---

**Summary:** Phase 2 = **politician trade disclosure context** (Capitol Trades / STOCK Act–style ingest concept) in a **deterministic Snowflake-first** layer, **compact card**, **LPA + hearing**, **optional silent live bonus**, **never** a primary signal, **never** stance/proposal/broker logic impact. LLM and narrative-driven scoring are **out of scope** for Phase 2.
