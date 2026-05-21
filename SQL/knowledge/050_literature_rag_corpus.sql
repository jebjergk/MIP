-- 050_literature_rag_corpus.sql
-- Purpose: RAG document corpus for approved literature concept cards.
--
-- ADVISORY NOTICE:
--   The RAG corpus prepares approved literature cards as a future evidence-support pack
--   for the agentic revalidation committee. It is NOT wired into:
--     - the deterministic revalidation board
--     - LPA (Live Portfolio Activity)
--     - any agent runtime
--     - proposal generation
--     - Committee 2.0
--     - FastAPI, React, or IBKR execution
--   Literature is advisory only. It is NOT market evidence and NOT a trade signal.
--   A separate future transition from deterministic to agentic revalidation is required
--   before RAG evidence packs become operationally visible.

use role MIP_ADMIN_ROLE;
use database MIP;
use schema MIP.KNOWLEDGE;

-- =============================================================================
-- Table: LITERATURE_RAG_DOCUMENT
--   One searchable RAG document per approved concept card per RAG_SCOPE.
--   Currently only RAG_SCOPE = 'REVALIDATION' is populated.
-- =============================================================================

create table if not exists MIP.KNOWLEDGE.LITERATURE_RAG_DOCUMENT (
    RAG_DOCUMENT_ID                 string        not null,
    CARD_ID                         string        not null,
    RAG_SCOPE                       string,
    REVIEW_STATUS                   string,
    CONCEPT_NAME                    string,
    CONCEPT_TYPE                    string,
    CONCEPT_FAMILY                  string,
    MIP_USAGE_TIER                  string,
    EVIDENCE_READINESS_STATUS       string,
    DAILY_BAR_COMPATIBILITY         string,
    SOURCE_BOOK                     string,
    PAGE_START                      number,
    PAGE_END                        number,
    SEARCH_TEXT                     string,
    DISPLAY_TEXT                    string,
    REQUIRED_EVIDENCE               variant,
    INVALIDATION_EVIDENCE           variant,
    FAILURE_MODES                   variant,
    TRADE_MANAGEMENT_IMPLICATIONS   variant,
    APPLICABLE_AGENTS               variant,
    REQUIRED_INTRADAY_FEATURES      variant,
    RAW_CARD                        variant,
    CREATED_AT                      timestamp_ntz default CURRENT_TIMESTAMP()
);

comment on table MIP.KNOWLEDGE.LITERATURE_RAG_DOCUMENT is
    'ADVISORY ONLY. One RAG document per approved literature concept card. Built for the future agentic revalidation committee. NOT wired into deterministic revalidation, LPA, or any agent runtime. Literature is not market evidence and not a trade signal.';

comment on column MIP.KNOWLEDGE.LITERATURE_RAG_DOCUMENT.RAG_SCOPE is
    'Allowed values: REVALIDATION, POSITION_HEALTH, PROPOSAL_AUDIT_ONLY. Currently only REVALIDATION is populated.';

comment on column MIP.KNOWLEDGE.LITERATURE_RAG_DOCUMENT.SEARCH_TEXT is
    'Concatenated text optimized for embedding-based retrieval. Includes guardrail phrase.';

comment on column MIP.KNOWLEDGE.LITERATURE_RAG_DOCUMENT.DISPLAY_TEXT is
    'Concise structured evidence card optimized for agent/human reading. Includes guardrail footer.';

-- =============================================================================
-- View: V_LITERATURE_REVALIDATION_RAG_CORPUS
--   Exposes the approved revalidation RAG corpus.
-- =============================================================================

create or replace view MIP.KNOWLEDGE.V_LITERATURE_REVALIDATION_RAG_CORPUS
    comment = 'ADVISORY ONLY. Approved literature support for future revalidation RAG retrieval. Built for the agentic revalidation committee, NOT the deterministic revalidation board. NOT wired into LPA or any agent runtime. Literature is not market evidence and not a trade signal.'
as
select
    RAG_DOCUMENT_ID,
    CARD_ID,
    CONCEPT_NAME,
    CONCEPT_FAMILY,
    MIP_USAGE_TIER,
    DAILY_BAR_COMPATIBILITY,
    SOURCE_BOOK,
    PAGE_START,
    PAGE_END,
    SEARCH_TEXT,
    DISPLAY_TEXT
from MIP.KNOWLEDGE.LITERATURE_RAG_DOCUMENT
where RAG_SCOPE = 'REVALIDATION';

-- =============================================================================
-- Procedure: SP_REFRESH_LITERATURE_RAG_DOCUMENTS
--   Deterministic refresh. Deletes existing REVALIDATION rows and rebuilds from
--   V_LITERATURE_APPROVED_REVALIDATION_RAG. No LLM calls, no runtime tables touched.
--   Returns a brief summary string.
-- =============================================================================

create or replace procedure MIP.KNOWLEDGE.SP_REFRESH_LITERATURE_RAG_DOCUMENTS()
returns string
language sql
as
$$
declare
    v_deleted  number default 0;
    v_inserted number default 0;
    v_summary  string;
begin
    delete from MIP.KNOWLEDGE.LITERATURE_RAG_DOCUMENT
    where RAG_SCOPE = 'REVALIDATION';
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
        PAGE_START,
        PAGE_END,
        SEARCH_TEXT,
        DISPLAY_TEXT,
        REQUIRED_EVIDENCE,
        INVALIDATION_EVIDENCE,
        FAILURE_MODES,
        TRADE_MANAGEMENT_IMPLICATIONS,
        APPLICABLE_AGENTS,
        REQUIRED_INTRADAY_FEATURES,
        RAW_CARD
    )
    select
        SHA2('REVALIDATION|' || c.CARD_ID, 256)                       as RAG_DOCUMENT_ID,
        c.CARD_ID,
        'REVALIDATION'                                                  as RAG_SCOPE,
        v.REVIEW_STATUS,
        c.CONCEPT_NAME,
        c.CONCEPT_TYPE,
        c.CONCEPT_FAMILY,
        c.MIP_USAGE_TIER,
        c.EVIDENCE_READINESS_STATUS,
        c.DAILY_BAR_COMPATIBILITY,
        c.SOURCE_BOOK,
        c.PAGE_START,
        c.PAGE_END,

        -- SEARCH_TEXT: concatenated retrieval text + guardrail
        TRIM(
            COALESCE('Concept: '       || c.CONCEPT_NAME           || '\n', '') ||
            COALESCE('Concept type: '  || c.CONCEPT_TYPE           || '\n', '') ||
            COALESCE('Concept family: '|| c.CONCEPT_FAMILY         || '\n', '') ||
            COALESCE('Setup: '         || c.SETUP_DEFINITION       || '\n', '') ||
            COALESCE('Market context: '|| c.MARKET_CONTEXT         || '\n', '') ||
            CASE WHEN c.REQUIRED_EVIDENCE is not null
                 THEN 'Required evidence: ' ||
                      ARRAY_TO_STRING(c.REQUIRED_EVIDENCE::array, '; ') || '\n'
                 ELSE '' END ||
            CASE WHEN c.CONFIRMATION_EVIDENCE is not null
                 THEN 'Confirmation evidence: ' ||
                      ARRAY_TO_STRING(c.CONFIRMATION_EVIDENCE::array, '; ') || '\n'
                 ELSE '' END ||
            CASE WHEN c.INVALIDATION_EVIDENCE is not null
                 THEN 'Invalidation evidence: ' ||
                      ARRAY_TO_STRING(c.INVALIDATION_EVIDENCE::array, '; ') || '\n'
                 ELSE '' END ||
            CASE WHEN c.FAILURE_MODES is not null
                 THEN 'Failure modes: ' ||
                      ARRAY_TO_STRING(c.FAILURE_MODES::array, '; ') || '\n'
                 ELSE '' END ||
            CASE WHEN c.TRADE_MANAGEMENT_IMPLICATIONS is not null
                 THEN 'Trade management: ' ||
                      ARRAY_TO_STRING(c.TRADE_MANAGEMENT_IMPLICATIONS::array, '; ') || '\n'
                 ELSE '' END ||
            CASE WHEN c.APPLICABLE_AGENTS is not null
                 THEN 'Applicable agents: ' ||
                      ARRAY_TO_STRING(c.APPLICABLE_AGENTS::array, ', ') || '\n'
                 ELSE '' END ||
            COALESCE('Source: ' || c.SOURCE_BOOK || ' pp. ' ||
                     COALESCE(c.PAGE_START::string, '?') || '-' ||
                     COALESCE(c.PAGE_END::string, '?')  || '\n', '') ||
            'Advisory literature concept only. Not market evidence. Not a trade signal.'
        )                                                                as SEARCH_TEXT,

        -- DISPLAY_TEXT: readable evidence card with labelled sections + guardrail footer
        TRIM(
            '=== Literature Concept ===' || '\n' ||
            'Concept: '       || COALESCE(c.CONCEPT_NAME, '(unnamed)') || '\n' ||
            'Family: '        || COALESCE(c.CONCEPT_FAMILY, '(none)')  || '\n' ||
            'Daily compat: '  || COALESCE(c.DAILY_BAR_COMPATIBILITY, '(none)') || '\n' ||
            '\n' ||
            'Why relevant:'   || '\n' ||
            '  Setup: '       || COALESCE(c.SETUP_DEFINITION, '(none)') || '\n' ||
            '  Context: '     || COALESCE(c.MARKET_CONTEXT,   '(none)') || '\n' ||
            '\n' ||
            'Required evidence: ' ||
            COALESCE(ARRAY_TO_STRING(c.REQUIRED_EVIDENCE::array, '; '), '(none)') || '\n' ||
            'Invalidation evidence: ' ||
            COALESCE(ARRAY_TO_STRING(c.INVALIDATION_EVIDENCE::array, '; '), '(none)') || '\n' ||
            'Failure modes: ' ||
            COALESCE(ARRAY_TO_STRING(c.FAILURE_MODES::array, '; '), '(none)') || '\n' ||
            'Trade management implications: ' ||
            COALESCE(ARRAY_TO_STRING(c.TRADE_MANAGEMENT_IMPLICATIONS::array, '; '), '(none)') || '\n' ||
            '\n' ||
            'Source: ' || COALESCE(c.SOURCE_BOOK, '(unknown)') || ' pp. ' ||
                COALESCE(c.PAGE_START::string, '?') || '-' ||
                COALESCE(c.PAGE_END::string, '?')   || '\n' ||
            '\n' ||
            'GUARDRAIL: Advisory literature concept only. Not market evidence. Not a trade signal. Use for reasoning support only.'
        )                                                                as DISPLAY_TEXT,

        c.REQUIRED_EVIDENCE,
        c.INVALIDATION_EVIDENCE,
        c.FAILURE_MODES,
        c.TRADE_MANAGEMENT_IMPLICATIONS,
        c.APPLICABLE_AGENTS,
        c.REQUIRED_INTRADAY_FEATURES,
        c.RAW_CARD
    from MIP.KNOWLEDGE.LITERATURE_CONCEPT_CARD c
    join MIP.KNOWLEDGE.V_LITERATURE_APPROVED_REVALIDATION_RAG v
      on v.CARD_ID = c.CARD_ID;

    v_inserted := SQLROWCOUNT;

    v_summary := 'SP_REFRESH_LITERATURE_RAG_DOCUMENTS: deleted=' || v_deleted::string ||
                 ' inserted=' || v_inserted::string ||
                 ' scope=REVALIDATION';
    return v_summary;
end;
$$
;

comment on procedure MIP.KNOWLEDGE.SP_REFRESH_LITERATURE_RAG_DOCUMENTS() is
    'Deterministic refresh of LITERATURE_RAG_DOCUMENT for RAG_SCOPE=REVALIDATION. Deletes existing rows and rebuilds from V_LITERATURE_APPROVED_REVALIDATION_RAG. No LLM calls. ADVISORY ONLY: this procedure does not affect any runtime decision path.';
