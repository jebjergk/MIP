-- 061_literature_cortex_search_cost_controls.sql
-- Purpose: Cost-control maintenance for LITERATURE_REVALIDATION_SEARCH_SERVICE.
--
-- =============================================================================
-- WHY THIS FILE EXISTS
-- =============================================================================
-- The Brooks revalidation RAG corpus is STATIC by default. New approved cards
-- only enter the corpus when:
--   a) a row is inserted/updated in MIP.KNOWLEDGE.LITERATURE_CARD_REVIEW with
--      REVIEW_STATUS = 'APPROVED_FOR_REVALIDATION_RAG'
--   b) CALL MIP.KNOWLEDGE.SP_REFRESH_LITERATURE_RAG_DOCUMENTS() rebuilds
--      LITERATURE_RAG_DOCUMENT
--   c) Cortex Search indexing is resumed (this file)
--   d) the service refreshes or naturally catches up to the new rows
--   e) indexing is suspended again to stop billing for re-index checks
--
-- Snowflake Cortex Search requires a TARGET_LAG when the service is created.
-- We set TARGET_LAG = '1 day' as a baseline, but with INDEXING SUSPENDED the
-- service does not run continuous re-index checks and does not bill for them.
--
-- SERVING may remain running for interactive retrieval tests. If retrieval is
-- not needed for an extended period, SERVING can also be suspended to avoid
-- serving-time charges. The keyword fallback view
-- (V_LITERATURE_REVALIDATION_RAG_KEYWORD_SEARCH) is always available regardless
-- of serving state.
--
-- ADVISORY: This service is part of the future agentic revalidation committee
-- retrieval pilot. It is NOT wired into deterministic revalidation, LPA, any
-- agent runtime, proposal generation, FastAPI, React, or IBKR.

use role MIP_ADMIN_ROLE;
use database MIP;
use schema MIP.KNOWLEDGE;

-- =============================================================================
-- ACTIVE STEP: Suspend indexing on the existing service.
--   Run once after the initial indexing has completed (indexing_state = ACTIVE
--   and source_data_num_rows = expected count). After this, the service stops
--   running periodic re-index checks. Serving continues unchanged.
-- =============================================================================

alter cortex search service MIP.KNOWLEDGE.LITERATURE_REVALIDATION_SEARCH_SERVICE
    suspend indexing;

-- =============================================================================
-- OPTIONAL MAINTENANCE COMMANDS (commented out)
--
-- Run these manually when adding new approved cards or controlling cost.
-- Always pair RESUME + manual refresh + SUSPEND so the service is only
-- indexing when there is actually new data to absorb.
-- =============================================================================

-- Step 1: Resume indexing before refreshing the search index:
-- alter cortex search service MIP.KNOWLEDGE.LITERATURE_REVALIDATION_SEARCH_SERVICE
--     resume indexing;

-- Step 2: (Operator) approve new cards in LITERATURE_CARD_REVIEW, then refresh:
-- call MIP.KNOWLEDGE.SP_REFRESH_LITERATURE_RAG_DOCUMENTS();

-- Step 3: Wait for the service to catch up. Check status:
-- show cortex search services like 'LITERATURE_REVALIDATION_SEARCH_SERVICE' in schema MIP.KNOWLEDGE;
-- Look for: indexing_state=ACTIVE, source_data_num_rows matches LITERATURE_RAG_DOCUMENT count.

-- Step 4: Suspend indexing again after refresh:
-- alter cortex search service MIP.KNOWLEDGE.LITERATURE_REVALIDATION_SEARCH_SERVICE
--     suspend indexing;

-- Optional: suspend serving when retrieval is not being tested:
-- alter cortex search service MIP.KNOWLEDGE.LITERATURE_REVALIDATION_SEARCH_SERVICE
--     suspend serving;

-- Optional: resume serving before retrieval tests:
-- alter cortex search service MIP.KNOWLEDGE.LITERATURE_REVALIDATION_SEARCH_SERVICE
--     resume serving;
