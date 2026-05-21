-- 010_literature_concept_tables.sql
-- Purpose: Core tables for MIP.KNOWLEDGE literature concept staging layer.
--
-- ADVISORY NOTICE:
--   All data in these tables is derived from offline extraction of trading books.
--   It represents curated conceptual knowledge, NOT market evidence, NOT trading signals,
--   and NOT decision authority. No live MIP agent, stored procedure, or execution path
--   queries these tables. A separate approved task is required before any runtime use.

use role MIP_ADMIN_ROLE;
use database MIP;
use schema MIP.KNOWLEDGE;

-- =============================================================================
-- 1. LITERATURE_LOAD_BATCH
--    Tracks each manual load run of the loader script.
-- =============================================================================

create table if not exists MIP.KNOWLEDGE.LITERATURE_LOAD_BATCH (
    LOAD_BATCH_ID           string        not null,
    SOURCE_PATH             string,
    SOURCE_FILE_COUNT       number,
    RAW_CARD_COUNT          number,
    LOADED_CARD_COUNT       number,
    SKIPPED_LINE_COUNT      number,
    LOAD_STATUS             string,
    ERROR_SUMMARY           string,
    CREATED_AT              timestamp_ntz default CURRENT_TIMESTAMP(),
    COMPLETED_AT            timestamp_ntz,
    constraint PK_LITERATURE_LOAD_BATCH primary key (LOAD_BATCH_ID)
);

comment on table MIP.KNOWLEDGE.LITERATURE_LOAD_BATCH is
    'Audit log of each manual load run from the literature concept extractor. Tracks source files, row counts, and load status.';

-- =============================================================================
-- 2. LITERATURE_CONCEPT_CARD
--    Canonical curated concept-card table. One row per post-processed card.
--    PRIMARY KEY is metadata only — Snowflake does not enforce uniqueness.
--    Use V_LITERATURE_DUPLICATE_CARD_IDS to verify uniqueness after load.
-- =============================================================================

create table if not exists MIP.KNOWLEDGE.LITERATURE_CONCEPT_CARD (
    -- Identity
    CARD_ID                         string        not null,

    -- Classification
    CONCEPT_NAME                    string,
    CONCEPT_TYPE                    string,
    CARD_ROLE                       string,
    CONCEPT_FAMILY                  string,
    EVIDENCE_READINESS_STATUS       string,
    MIP_USAGE_TIER                  string,
    DAILY_BAR_COMPATIBILITY         string,
    CONFIDENCE                      string,

    -- Source metadata
    SOURCE_BOOK                     string,
    SOURCE_FILE                     string,
    PAGE_START                      number,
    PAGE_END                        number,
    CHUNK_ID                        string,
    SOURCE_QUOTE_SHORT              string,

    -- Concept content
    MARKET_CONTEXT                  string,
    SETUP_DEFINITION                string,
    TIMEFRAME_DEPENDENCY            string,
    EXTRACTION_NOTES                string,

    -- Array / semi-structured fields (stored as real VARIANT JSON)
    REQUIRED_EVIDENCE               variant,
    CONFIRMATION_EVIDENCE           variant,
    INVALIDATION_EVIDENCE           variant,
    FAILURE_MODES                   variant,
    TRADE_MANAGEMENT_IMPLICATIONS   variant,
    MIP_SETUP_FAMILY_MAPPING        variant,
    APPLICABLE_AGENTS               variant,
    REQUIRED_INTRADAY_FEATURES      variant,

    -- Deduplication
    DUPLICATE_GROUP_ID              string,
    DUPLICATE_CONFIDENCE            float,
    CANONICAL_CANDIDATE             boolean,
    DUPLICATE_REASON                string,

    -- Full audit: raw card JSON as loaded from JSONL
    RAW_CARD                        variant,

    -- Load tracking
    LOAD_BATCH_ID                   string,
    LOADED_AT                       timestamp_ntz default CURRENT_TIMESTAMP(),

    constraint PK_LITERATURE_CONCEPT_CARD primary key (CARD_ID)
);

comment on table MIP.KNOWLEDGE.LITERATURE_CONCEPT_CARD is
    'Advisory literature-derived concept cards curated from Al Brooks Trading Price Action series. Source excerpts are short references only. NOT market evidence. NOT trading signals. NOT decision authority. AGENT_READY_NOW means safe to inspect - it does NOT activate or authorize agent use. A separate approved task is required before any runtime agent reads from this table.';

comment on column MIP.KNOWLEDGE.LITERATURE_CONCEPT_CARD.CARD_ID is
    'Stable 16-char SHA1 hex ID derived from source_book|chunk_id|concept_name|page_start. NOT enforced unique by Snowflake. Run V_LITERATURE_DUPLICATE_CARD_IDS to verify after load.';

comment on column MIP.KNOWLEDGE.LITERATURE_CONCEPT_CARD.MIP_USAGE_TIER is
    'AGENT_READY_NOW = safe to inspect only. Does NOT activate agent use. FUTURE_INTRADAY = useful but requires intraday summary features not yet built. REFERENCE_LIBRARY = contextual knowledge, not for verdict strength. HUMAN_REVIEW_REQUIRED = needs manual triage before any use.';

comment on column MIP.KNOWLEDGE.LITERATURE_CONCEPT_CARD.RAW_CARD is
    'Full original JSON card as loaded from normalized_concept_cards.jsonl. Preserved for audit.';

comment on column MIP.KNOWLEDGE.LITERATURE_CONCEPT_CARD.SOURCE_QUOTE_SHORT is
    'Short source excerpt (25 words or fewer). Brief reference only, not reproduction of book content.';

-- =============================================================================
-- 3. LITERATURE_POSTPROCESS_SUMMARY
--    Stores postprocess_summary.json for audit per load batch.
-- =============================================================================

create table if not exists MIP.KNOWLEDGE.LITERATURE_POSTPROCESS_SUMMARY (
    LOAD_BATCH_ID                   string,
    SUMMARY_JSON                    variant,
    RAW_CARDS                       number,
    NORMALIZED_CARDS                number,
    SOURCE_BOOKS                    variant,
    BY_EVIDENCE_READINESS_STATUS    variant,
    BY_MIP_USAGE_TIER               variant,
    BY_CONCEPT_FAMILY               variant,
    BY_DAILY_BAR_COMPATIBILITY      variant,
    BY_CARD_ROLE                    variant,
    BY_CONCEPT_TYPE                 variant,
    BY_MIP_SETUP_FAMILY             variant,
    DUPLICATE_GROUPS                number,
    DUPLICATE_CANDIDATES            number,
    RECOMMENDED_FIRST_LOAD_COUNT    number,
    GENERATED_AT                    timestamp_ntz,
    LOADED_AT                       timestamp_ntz default CURRENT_TIMESTAMP()
);

comment on table MIP.KNOWLEDGE.LITERATURE_POSTPROCESS_SUMMARY is
    'Stores the postprocess_summary.json output from the offline post-processing pipeline. One row per load batch.';
