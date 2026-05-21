-- 060_literature_cortex_search.sql
-- Purpose: Cortex Search service over the approved revalidation literature RAG corpus.
--
-- ADVISORY NOTICE:
--   This search service is part of a retrieval pilot for the future agentic
--   revalidation committee. It is NOT wired into the deterministic revalidation
--   board, LPA, or any agent runtime. Literature concepts are advisory only.
--
-- Required privileges:
--   - Creating role needs CREATE CORTEX SEARCH SERVICE on the schema and the
--     SNOWFLAKE.CORTEX_USER (or equivalent account-level Cortex Search) privilege.
--   - Query role needs USAGE on the database, schema, and the search service.
--   - Warehouse (MIP_WH_XS) needs USAGE for the search service runtime.
--
-- If creation fails due to privilege, deploy fails cleanly without changing any
-- other object. The fallback path is the keyword search view defined in
-- 070_literature_rag_retrieval.sql (V_LITERATURE_REVALIDATION_RAG_KEYWORD_SEARCH).

use role MIP_ADMIN_ROLE;
use database MIP;
use schema MIP.KNOWLEDGE;

create or replace cortex search service MIP.KNOWLEDGE.LITERATURE_REVALIDATION_SEARCH_SERVICE
    on SEARCH_TEXT
    attributes CONCEPT_FAMILY, MIP_USAGE_TIER, DAILY_BAR_COMPATIBILITY, SOURCE_BOOK, CARD_ID
    warehouse = MIP_WH_XS
    target_lag = '1 day'
    embedding_model = 'snowflake-arctic-embed-l-v2.0'
    comment = 'ADVISORY ONLY. Cortex Search over approved revalidation literature corpus. Built for the future agentic revalidation committee. NOT wired into deterministic revalidation, LPA, or any agent runtime. Literature is not market evidence and not a trade signal.'
    as (
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
        from MIP.KNOWLEDGE.V_LITERATURE_REVALIDATION_RAG_CORPUS
    );
