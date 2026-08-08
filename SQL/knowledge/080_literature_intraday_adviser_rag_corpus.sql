-- 080_literature_intraday_adviser_rag_corpus.sql
-- Purpose: PROPOSED static RAG corpus + Cortex Search for BROOKS INTRADAY ADVISER (V0.1).
--
-- STATUS: Deployed 2026-08-07 (one-time static corpus v1_static_2026-08-07).
-- Does NOT modify REVALIDATION scope, LPA, V0.3/V0.4, or existing search services.
--
-- Prerequisites:
--   - Approved local corpus: cursorfiles/brooks_intraday_adviser_corpus.json
--   - One-time load into staging (see deployment plan doc)
--   - MIP_ADMIN_ROLE, CREATE CORTEX SEARCH SERVICE, SNOWFLAKE.CORTEX privileges

use role MIP_ADMIN_ROLE;
use database MIP;
use schema MIP.KNOWLEDGE;

-- =============================================================================
-- Optional column extensions on LITERATURE_RAG_DOCUMENT (if not using VARIANT-only)
-- =============================================================================
-- alter table MIP.KNOWLEDGE.LITERATURE_RAG_DOCUMENT add column if not exists
--     EXECUTION_DIRECTION string comment 'LONG_ONLY for Adviser scope';
-- alter table MIP.KNOWLEDGE.LITERATURE_RAG_DOCUMENT add column if not exists
--     EXECUTION_RELEVANCE string comment 'LONG_ENTRY|LONG_CONTEXT|LONG_MANAGEMENT|LONG_FAILURE|BEARISH_CONTEXT_ONLY';
-- alter table MIP.KNOWLEDGE.LITERATURE_RAG_DOCUMENT add column if not exists
--     ADVISER_CLASS string;
-- alter table MIP.KNOWLEDGE.LITERATURE_RAG_DOCUMENT add column if not exists
--     ADVISER_METADATA variant comment 'Filter facets: setup_family, decision_stage, etc.';

-- =============================================================================
-- Staging table for approved static corpus (one-time load from JSON)
-- =============================================================================
create table if not exists MIP.KNOWLEDGE.LITERATURE_INTRADAY_ADVISER_CORPUS_STAGING (
    CARD_ID                 string        not null,
    RAG_SCOPE               string        not null default 'INTRADAY_ADVISER',
    EXECUTION_DIRECTION     string        not null default 'LONG_ONLY',
    EXECUTION_RELEVANCE     string        not null,
    ADVISER_CLASS           string,
    CONCEPT_NAME            string,
    SOURCE_BOOK             string,
    CARD_ROLE               string,
    CONCEPT_FAMILY          string,
    INTRADAY_COMPATIBILITY  string,
    SEARCH_TEXT             string        not null,
    DISPLAY_TEXT            string,
    ADVISER_METADATA        variant,
    CORPUS_BUILD_VERSION    string,
    LOADED_AT               timestamp_ntz default CURRENT_TIMESTAMP()
);

comment on table MIP.KNOWLEDGE.LITERATURE_INTRADAY_ADVISER_CORPUS_STAGING is
    'One-time approved INTRADAY_ADVISER static corpus staging. Source: brooks_intraday_adviser_corpus.json. Not wired to runtime until Adviser V0.1.';

-- =============================================================================
-- View: searchable corpus (excludes EXCLUDED_SHORT_EXECUTION at SQL layer)
-- =============================================================================
create or replace view MIP.KNOWLEDGE.V_LITERATURE_INTRADAY_ADVISER_RAG_CORPUS
    comment = 'LONG_ONLY Adviser RAG corpus. EXCLUDED_SHORT_EXECUTION rows omitted. Static one-time build.'
as
select
    SHA2('INTRADAY_ADVISER|' || s.CARD_ID, 256) as RAG_DOCUMENT_ID,
    s.CARD_ID,
    s.RAG_SCOPE,
    s.EXECUTION_DIRECTION,
    s.EXECUTION_RELEVANCE,
    s.ADVISER_CLASS,
    s.CONCEPT_NAME,
    s.CONCEPT_FAMILY,
    s.INTRADAY_COMPATIBILITY as DAILY_BAR_COMPATIBILITY,
    s.SOURCE_BOOK,
    s.SEARCH_TEXT,
    s.DISPLAY_TEXT,
    s.ADVISER_METADATA
from MIP.KNOWLEDGE.LITERATURE_INTRADAY_ADVISER_CORPUS_STAGING s
where s.RAG_SCOPE = 'INTRADAY_ADVISER'
  and s.EXECUTION_DIRECTION = 'LONG_ONLY'
  and s.EXECUTION_RELEVANCE <> 'EXCLUDED_SHORT_EXECUTION'
  and not exists (
      select 1
      from MIP.KNOWLEDGE.LITERATURE_CONCEPT_CARD c
      where c.CARD_ID = s.CARD_ID
        and c.MIP_USAGE_TIER = 'HUMAN_REVIEW_REQUIRED'
  );

-- =============================================================================
-- Procedure: one-time merge into LITERATURE_RAG_DOCUMENT (Adviser scope only)
-- =============================================================================
create or replace procedure MIP.KNOWLEDGE.SP_LOAD_INTRADAY_ADVISER_RAG_DOCUMENTS(
    P_CORPUS_BUILD_VERSION string default 'v1_static'
)
returns string
language sql
as
$$
declare
    v_deleted  number default 0;
    v_inserted number default 0;
begin
    delete from MIP.KNOWLEDGE.LITERATURE_RAG_DOCUMENT
    where RAG_SCOPE = 'INTRADAY_ADVISER';
    v_deleted := SQLROWCOUNT;

    insert into MIP.KNOWLEDGE.LITERATURE_RAG_DOCUMENT (
        RAG_DOCUMENT_ID,
        CARD_ID,
        RAG_SCOPE,
        REVIEW_STATUS,
        CONCEPT_NAME,
        CONCEPT_TYPE,
        CONCEPT_FAMILY,
        MIP_USAGE_TIER,
        EVIDENCE_READINESS_STATUS,
        DAILY_BAR_COMPATIBILITY,
        SOURCE_BOOK,
        SEARCH_TEXT,
        DISPLAY_TEXT,
        RAW_CARD
    )
    select
        v.RAG_DOCUMENT_ID,
        v.CARD_ID,
        'INTRADAY_ADVISER',
        'APPROVED_STATIC_CORPUS',
        v.CONCEPT_NAME,
        null,
        v.CONCEPT_FAMILY,
        null,
        null,
        v.DAILY_BAR_COMPATIBILITY,
        v.SOURCE_BOOK,
        v.SEARCH_TEXT,
        v.DISPLAY_TEXT,
        object_construct(
            'execution_direction', v.EXECUTION_DIRECTION,
            'execution_relevance', v.EXECUTION_RELEVANCE,
            'adviser_class', v.ADVISER_CLASS,
            'metadata', v.ADVISER_METADATA,
            'corpus_build_version', :P_CORPUS_BUILD_VERSION
        )
    from MIP.KNOWLEDGE.V_LITERATURE_INTRADAY_ADVISER_RAG_CORPUS v;

    v_inserted := SQLROWCOUNT;
    return 'SP_LOAD_INTRADAY_ADVISER_RAG_DOCUMENTS: deleted=' || v_deleted::string ||
           ' inserted=' || v_inserted::string || ' scope=INTRADAY_ADVISER';
end;
$$;

-- =============================================================================
-- Cortex Search service (separate from LITERATURE_REVALIDATION_SEARCH_SERVICE)
-- =============================================================================
-- create or replace cortex search service MIP.KNOWLEDGE.LITERATURE_INTRADAY_ADVISER_SEARCH_SERVICE
--     on SEARCH_TEXT
--     attributes
--         EXECUTION_DIRECTION,
--         EXECUTION_RELEVANCE,
--         ADVISER_CLASS,
--         CONCEPT_FAMILY,
--         SOURCE_BOOK,
--         CARD_ID
--     warehouse = MIP_WH_XS
--     target_lag = '365 days'
--     embedding_model = 'snowflake-arctic-embed-l-v2.0'
--     comment = 'LONG_ONLY Brooks Intraday Adviser static corpus. Not wired to production Adviser until V0.1 approval.'
--     as (
--         select
--             RAG_DOCUMENT_ID,
--             CARD_ID,
--             EXECUTION_DIRECTION,
--             EXECUTION_RELEVANCE,
--             ADVISER_CLASS,
--             CONCEPT_NAME,
--             CONCEPT_FAMILY,
--             DAILY_BAR_COMPATIBILITY,
--             SOURCE_BOOK,
--             SEARCH_TEXT,
--             DISPLAY_TEXT
--         from MIP.KNOWLEDGE.V_LITERATURE_INTRADAY_ADVISER_RAG_CORPUS
--     );

-- =============================================================================
-- Post-index: suspend automatic refresh (static corpus)
-- =============================================================================
-- alter cortex search service MIP.KNOWLEDGE.LITERATURE_INTRADAY_ADVISER_SEARCH_SERVICE
--     set target_lag = '365 days';
-- Optional: manual refresh only when corpus version changes:
-- alter cortex search service ... refresh;

-- =============================================================================
-- Validation queries (run after load + index)
-- =============================================================================
-- select RAG_SCOPE, count(*) from MIP.KNOWLEDGE.LITERATURE_RAG_DOCUMENT group by 1;
-- select EXECUTION_RELEVANCE, count(*) from MIP.KNOWLEDGE.V_LITERATURE_INTRADAY_ADVISER_RAG_CORPUS group by 1;
-- select count(*) from MIP.KNOWLEDGE.V_LITERATURE_INTRADAY_ADVISER_RAG_CORPUS
--   where EXECUTION_RELEVANCE = 'EXCLUDED_SHORT_EXECUTION';  -- expect 0
-- show cortex search services like 'LITERATURE_INTRADAY_ADVISER%' in schema MIP.KNOWLEDGE;
