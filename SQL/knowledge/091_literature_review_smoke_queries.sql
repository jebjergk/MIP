-- 091_literature_review_smoke_queries.sql
-- Purpose: Post-deploy smoke queries for the MIP.KNOWLEDGE review/approval layer.
--
-- Run with:
--   cursorfiles\.venv\Scripts\python.exe cursorfiles/query_snowflake.py -f MIP/SQL/knowledge/091_literature_review_smoke_queries.sql
--
-- These queries are advisory only. The review layer does NOT activate RAG or agent runtime use.

use role MIP_ADMIN_ROLE;
use database MIP;
use schema MIP.KNOWLEDGE;

-- =============================================================================
-- Smoke 1: Total reviewed cards (distinct CARD_IDs with at least one review row)
-- =============================================================================

select count(distinct CARD_ID) as REVIEWED_CARDS
from MIP.KNOWLEDGE.LITERATURE_CARD_REVIEW;

-- =============================================================================
-- Smoke 2: Approved-for-revalidation RAG count
-- =============================================================================

select count(*) as APPROVED_REVALIDATION_RAG_COUNT
from MIP.KNOWLEDGE.V_LITERATURE_APPROVED_REVALIDATION_RAG;

-- =============================================================================
-- Smoke 3: Approved-for-position-health RAG count
-- =============================================================================

select count(*) as APPROVED_POSITION_HEALTH_RAG_COUNT
from MIP.KNOWLEDGE.V_LITERATURE_APPROVED_POSITION_HEALTH_RAG;

-- =============================================================================
-- Smoke 4: Excluded-from-retrieval count
--   Uses the latest review row per CARD_ID.
-- =============================================================================

with latest_review as (
    select
        CARD_ID,
        REVIEW_STATUS,
        row_number() over (partition by CARD_ID order by REVIEWED_AT desc) as rn
    from MIP.KNOWLEDGE.LITERATURE_CARD_REVIEW
)
select count(*) as EXCLUDED_FROM_RETRIEVAL_COUNT
from latest_review
where rn = 1
  and REVIEW_STATUS = 'EXCLUDE_FROM_RETRIEVAL';

-- =============================================================================
-- Smoke 5: Unreviewed agent-ready-now cards
-- =============================================================================

select count(*) as UNREVIEWED_AGENT_READY_NOW_COUNT
from MIP.KNOWLEDGE.V_LITERATURE_UNREVIEWED_AGENT_READY_NOW;

-- =============================================================================
-- Smoke 6: Suspicious review queue
-- =============================================================================

select count(*) as SUSPICIOUS_REVIEW_QUEUE_COUNT
from MIP.KNOWLEDGE.V_LITERATURE_SUSPICIOUS_REVIEW_QUEUE;

-- =============================================================================
-- Smoke 7: Breakdown of latest review status across reviewed cards
-- =============================================================================

with latest_review as (
    select
        CARD_ID,
        REVIEW_STATUS,
        row_number() over (partition by CARD_ID order by REVIEWED_AT desc) as rn
    from MIP.KNOWLEDGE.LITERATURE_CARD_REVIEW
)
select REVIEW_STATUS, count(*) as CARD_COUNT
from latest_review
where rn = 1
group by REVIEW_STATUS
order by CARD_COUNT desc;
