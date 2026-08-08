-- 081_literature_intraday_adviser_cortex_search.sql
-- One-time Cortex Search service for INTRADAY_ADVISER static corpus.
-- Does NOT modify LITERATURE_REVALIDATION_SEARCH_SERVICE.

use role MIP_ADMIN_ROLE;
use database MIP;
use schema MIP.KNOWLEDGE;

create or replace cortex search service MIP.KNOWLEDGE.LITERATURE_INTRADAY_ADVISER_SEARCH_SERVICE
    on SEARCH_TEXT
    attributes
        EXECUTION_DIRECTION,
        EXECUTION_RELEVANCE,
        ADVISER_CLASS,
        CONCEPT_FAMILY,
        SOURCE_BOOK,
        CARD_ID
    warehouse = MIP_WH_XS
    target_lag = '365 days'
    embedding_model = 'snowflake-arctic-embed-l-v2.0'
    comment = 'LONG_ONLY Brooks Intraday Adviser static corpus. Serving enabled. Indexing suspended after initial build via deploy script.'
    as (
        select
            RAG_DOCUMENT_ID,
            CARD_ID,
            EXECUTION_DIRECTION,
            EXECUTION_RELEVANCE,
            ADVISER_CLASS,
            CONCEPT_NAME,
            CONCEPT_FAMILY,
            DAILY_BAR_COMPATIBILITY,
            SOURCE_BOOK,
            SEARCH_TEXT,
            DISPLAY_TEXT
        from MIP.KNOWLEDGE.V_LITERATURE_INTRADAY_ADVISER_RAG_CORPUS
    );
