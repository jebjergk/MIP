-- 092_literature_rag_smoke_queries.sql
-- Purpose: Smoke queries for the literature RAG retrieval pilot.
--
-- Run with:
--   cursorfiles\.venv\Scripts\python.exe cursorfiles/query_snowflake.py -f MIP/SQL/knowledge/092_literature_rag_smoke_queries.sql
--
-- ADVISORY: All objects below support the retrieval pilot for the future agentic
-- revalidation committee. They are NOT wired into any runtime path.

use role MIP_ADMIN_ROLE;
use database MIP;
use schema MIP.KNOWLEDGE;

-- =============================================================================
-- Smoke 1: Approved revalidation card count
--   Expected (current): 10
-- =============================================================================

select count(*) as APPROVED_REVALIDATION_CARD_COUNT
from MIP.KNOWLEDGE.V_LITERATURE_APPROVED_REVALIDATION_RAG;

-- =============================================================================
-- Smoke 2: RAG document row count (REVALIDATION)
--   Expected (current): 10 after refresh
-- =============================================================================

select count(*) as RAG_DOCUMENT_COUNT
from MIP.KNOWLEDGE.LITERATURE_RAG_DOCUMENT
where RAG_SCOPE = 'REVALIDATION';

-- =============================================================================
-- Smoke 3: RAG corpus view count
--   Expected (current): 10
-- =============================================================================

select count(*) as RAG_CORPUS_VIEW_COUNT
from MIP.KNOWLEDGE.V_LITERATURE_REVALIDATION_RAG_CORPUS;

-- =============================================================================
-- Smoke 4: Sample documents by concept family
-- =============================================================================

select CONCEPT_FAMILY, count(*) as CARD_COUNT,
       array_slice(array_agg(CONCEPT_NAME), 0, 5) as EXAMPLE_CONCEPTS
from MIP.KNOWLEDGE.V_LITERATURE_REVALIDATION_RAG_CORPUS
group by CONCEPT_FAMILY
order by CARD_COUNT desc;

-- =============================================================================
-- Smoke 5: Cortex Search service existence check
--   Expected: 1 row (LITERATURE_REVALIDATION_SEARCH_SERVICE) if creation succeeded.
-- =============================================================================

show cortex search services like 'LITERATURE_REVALIDATION_SEARCH_SERVICE' in schema MIP.KNOWLEDGE;

-- =============================================================================
-- Smoke 6: Keyword fallback - "failed breakout"
-- =============================================================================

select CARD_ID, CONCEPT_NAME, CONCEPT_FAMILY, SOURCE_BOOK, PAGE_START, PAGE_END
from MIP.KNOWLEDGE.V_LITERATURE_REVALIDATION_RAG_KEYWORD_SEARCH
where lower(SEARCH_TEXT) like '%failed breakout%'
order by CONCEPT_FAMILY, CONCEPT_NAME;

-- =============================================================================
-- Smoke 7: Keyword fallback - "trend line break"
-- =============================================================================

select CARD_ID, CONCEPT_NAME, CONCEPT_FAMILY, SOURCE_BOOK, PAGE_START, PAGE_END
from MIP.KNOWLEDGE.V_LITERATURE_REVALIDATION_RAG_KEYWORD_SEARCH
where lower(SEARCH_TEXT) like '%trend line%'
   or lower(SEARCH_TEXT) like '%trend-line%'
   or lower(SEARCH_TEXT) like '%trendline%'
order by CONCEPT_FAMILY, CONCEPT_NAME;

-- =============================================================================
-- Smoke 8: Keyword fallback - "trailing stop"
-- =============================================================================

select CARD_ID, CONCEPT_NAME, CONCEPT_FAMILY, SOURCE_BOOK, PAGE_START, PAGE_END
from MIP.KNOWLEDGE.V_LITERATURE_REVALIDATION_RAG_KEYWORD_SEARCH
where lower(SEARCH_TEXT) like '%trailing stop%'
   or lower(SEARCH_TEXT) like '%protective stop%'
order by CONCEPT_FAMILY, CONCEPT_NAME;

-- =============================================================================
-- Smoke 9: Retrieval audit row count
-- =============================================================================

select count(*) as RETRIEVAL_AUDIT_ROW_COUNT,
       max(RETRIEVED_AT) as LATEST_RETRIEVAL_AT
from MIP.KNOWLEDGE.LITERATURE_RAG_RETRIEVAL_AUDIT;

-- =============================================================================
-- Smoke 10: Integrity check - every RAG document maps to an approved card
--   Expected: 0 rows.
-- =============================================================================

select d.RAG_DOCUMENT_ID, d.CARD_ID, d.CONCEPT_NAME
from MIP.KNOWLEDGE.LITERATURE_RAG_DOCUMENT d
left join MIP.KNOWLEDGE.V_LITERATURE_APPROVED_REVALIDATION_RAG a
       on a.CARD_ID = d.CARD_ID
where d.RAG_SCOPE = 'REVALIDATION'
  and a.CARD_ID is null;
