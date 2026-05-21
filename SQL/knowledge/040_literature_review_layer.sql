-- 040_literature_review_layer.sql
-- Purpose: Manual review/approval layer for MIP.KNOWLEDGE literature concept cards.
--
-- ADVISORY NOTICE:
--   This approval layer does NOT activate RAG usage or agent runtime use.
--   It only prepares a controlled, human-reviewed subset for a future
--   separate retrieval pilot. No agent, FastAPI, React, IBKR, revalidation,
--   or proposal path reads from these views.
--
-- Allowed REVIEW_STATUS values (convention, not enforced):
--   APPROVED_FOR_REVALIDATION_RAG
--   APPROVED_FOR_POSITION_HEALTH_RAG
--   REFERENCE_ONLY
--   EXCLUDE_FROM_RETRIEVAL
--   NEEDS_MANUAL_FIX

use role MIP_ADMIN_ROLE;
use database MIP;
use schema MIP.KNOWLEDGE;

-- =============================================================================
-- Table: LITERATURE_CARD_REVIEW
--   One row per manual review decision. CARD_ID is logically a foreign key into
--   LITERATURE_CONCEPT_CARD.CARD_ID, but Snowflake does not enforce it.
--   Multiple historical review rows per CARD_ID are allowed; use the latest
--   REVIEWED_AT when joining.
-- =============================================================================

create table if not exists MIP.KNOWLEDGE.LITERATURE_CARD_REVIEW (
    CARD_ID         string        not null,
    REVIEW_STATUS   string,
    REVIEW_REASON   string,
    REVIEWED_BY     string,
    REVIEWED_AT     timestamp_ntz default CURRENT_TIMESTAMP(),
    NOTES           string
);

comment on table MIP.KNOWLEDGE.LITERATURE_CARD_REVIEW is
    'Manual review and approval decisions for literature concept cards. ADVISORY ONLY. This table does NOT activate RAG usage or agent runtime use. It prepares a controlled, human-reviewed subset for a future separate retrieval pilot.';

comment on column MIP.KNOWLEDGE.LITERATURE_CARD_REVIEW.REVIEW_STATUS is
    'Convention-based status. Allowed values: APPROVED_FOR_REVALIDATION_RAG, APPROVED_FOR_POSITION_HEALTH_RAG, REFERENCE_ONLY, EXCLUDE_FROM_RETRIEVAL, NEEDS_MANUAL_FIX. Not enforced at the DB level.';

-- =============================================================================
-- View 1: V_LITERATURE_APPROVED_REVALIDATION_RAG
--   Cards approved for future revalidation RAG. NOT wired into revalidation runtime.
--   Uses latest review row per CARD_ID.
-- =============================================================================

create or replace view MIP.KNOWLEDGE.V_LITERATURE_APPROVED_REVALIDATION_RAG
    comment = 'ADVISORY ONLY. Cards manually approved for future revalidation RAG. This view does NOT activate RAG. NOT wired into revalidation runtime. A separate approved task is required before any retrieval pilot uses these rows.'
as
with latest_review as (
    select
        CARD_ID,
        REVIEW_STATUS,
        REVIEW_REASON,
        REVIEWED_BY,
        REVIEWED_AT,
        NOTES,
        row_number() over (partition by CARD_ID order by REVIEWED_AT desc) as rn
    from MIP.KNOWLEDGE.LITERATURE_CARD_REVIEW
)
select
    c.CARD_ID,
    c.CONCEPT_NAME,
    c.CONCEPT_FAMILY,
    c.MIP_USAGE_TIER,
    c.EVIDENCE_READINESS_STATUS,
    c.DAILY_BAR_COMPATIBILITY,
    c.CONFIDENCE,
    c.SOURCE_BOOK,
    c.PAGE_START,
    c.PAGE_END,
    c.SOURCE_QUOTE_SHORT,
    c.MARKET_CONTEXT,
    c.SETUP_DEFINITION,
    c.REQUIRED_EVIDENCE,
    c.CONFIRMATION_EVIDENCE,
    c.INVALIDATION_EVIDENCE,
    c.FAILURE_MODES,
    c.TRADE_MANAGEMENT_IMPLICATIONS,
    c.MIP_SETUP_FAMILY_MAPPING,
    c.APPLICABLE_AGENTS,
    c.REQUIRED_INTRADAY_FEATURES,
    r.REVIEW_STATUS,
    r.REVIEW_REASON,
    r.REVIEWED_BY,
    r.REVIEWED_AT,
    r.NOTES
from MIP.KNOWLEDGE.LITERATURE_CONCEPT_CARD c
join latest_review r
  on r.CARD_ID = c.CARD_ID
where r.rn = 1
  and r.REVIEW_STATUS = 'APPROVED_FOR_REVALIDATION_RAG'
  and (c.DUPLICATE_GROUP_ID IS NULL OR c.CANONICAL_CANDIDATE = TRUE);

-- =============================================================================
-- View 2: V_LITERATURE_APPROVED_POSITION_HEALTH_RAG
--   Cards approved for future position health RAG. NOT wired into runtime.
-- =============================================================================

create or replace view MIP.KNOWLEDGE.V_LITERATURE_APPROVED_POSITION_HEALTH_RAG
    comment = 'ADVISORY ONLY. Cards manually approved for future position health RAG. This view does NOT activate RAG. NOT wired into position health runtime. A separate approved task is required before any retrieval pilot uses these rows.'
as
with latest_review as (
    select
        CARD_ID,
        REVIEW_STATUS,
        REVIEW_REASON,
        REVIEWED_BY,
        REVIEWED_AT,
        NOTES,
        row_number() over (partition by CARD_ID order by REVIEWED_AT desc) as rn
    from MIP.KNOWLEDGE.LITERATURE_CARD_REVIEW
)
select
    c.CARD_ID,
    c.CONCEPT_NAME,
    c.CONCEPT_FAMILY,
    c.MIP_USAGE_TIER,
    c.EVIDENCE_READINESS_STATUS,
    c.DAILY_BAR_COMPATIBILITY,
    c.CONFIDENCE,
    c.SOURCE_BOOK,
    c.PAGE_START,
    c.PAGE_END,
    c.SOURCE_QUOTE_SHORT,
    c.MARKET_CONTEXT,
    c.SETUP_DEFINITION,
    c.REQUIRED_EVIDENCE,
    c.CONFIRMATION_EVIDENCE,
    c.INVALIDATION_EVIDENCE,
    c.FAILURE_MODES,
    c.TRADE_MANAGEMENT_IMPLICATIONS,
    c.MIP_SETUP_FAMILY_MAPPING,
    c.APPLICABLE_AGENTS,
    c.REQUIRED_INTRADAY_FEATURES,
    r.REVIEW_STATUS,
    r.REVIEW_REASON,
    r.REVIEWED_BY,
    r.REVIEWED_AT,
    r.NOTES
from MIP.KNOWLEDGE.LITERATURE_CONCEPT_CARD c
join latest_review r
  on r.CARD_ID = c.CARD_ID
where r.rn = 1
  and r.REVIEW_STATUS = 'APPROVED_FOR_POSITION_HEALTH_RAG'
  and (c.DUPLICATE_GROUP_ID IS NULL OR c.CANONICAL_CANDIDATE = TRUE);

-- =============================================================================
-- View 3: V_LITERATURE_UNREVIEWED_AGENT_READY_NOW
--   Agent-ready-now cards that do not yet have any review row.
--   Drives the manual review backlog.
-- =============================================================================

create or replace view MIP.KNOWLEDGE.V_LITERATURE_UNREVIEWED_AGENT_READY_NOW
    comment = 'Cards in V_LITERATURE_AGENT_READY_NOW that have no row in LITERATURE_CARD_REVIEW yet. Drives the manual review backlog.'
as
select
    a.CARD_ID,
    a.CONCEPT_NAME,
    a.CONCEPT_FAMILY,
    a.MIP_USAGE_TIER,
    a.EVIDENCE_READINESS_STATUS,
    a.DAILY_BAR_COMPATIBILITY,
    a.CONFIDENCE,
    a.SOURCE_BOOK,
    a.PAGE_START,
    a.PAGE_END,
    a.SOURCE_QUOTE_SHORT,
    a.SETUP_DEFINITION,
    a.MARKET_CONTEXT,
    a.REQUIRED_EVIDENCE,
    a.INVALIDATION_EVIDENCE,
    a.APPLICABLE_AGENTS,
    a.MIP_SETUP_FAMILY_MAPPING
from MIP.KNOWLEDGE.V_LITERATURE_AGENT_READY_NOW a
where not exists (
    select 1
    from MIP.KNOWLEDGE.LITERATURE_CARD_REVIEW r
    where r.CARD_ID = a.CARD_ID
);

-- =============================================================================
-- View 4: V_LITERATURE_SUSPICIOUS_REVIEW_QUEUE
--   Combined queue of:
--     - cards from V_LITERATURE_SUSPICIOUS_READY_CARDS (agent-ready with quality flags)
--     - HUMAN_REVIEW_REQUIRED cards (NEEDS_REVIEW status)
--   Shows current review status if one exists.
-- =============================================================================

create or replace view MIP.KNOWLEDGE.V_LITERATURE_SUSPICIOUS_REVIEW_QUEUE
    comment = 'Combined manual review queue: suspicious agent-ready cards plus HUMAN_REVIEW_REQUIRED cards. Shows latest review status if one exists.'
as
with latest_review as (
    select
        CARD_ID,
        REVIEW_STATUS,
        REVIEW_REASON,
        REVIEWED_BY,
        REVIEWED_AT,
        NOTES,
        row_number() over (partition by CARD_ID order by REVIEWED_AT desc) as rn
    from MIP.KNOWLEDGE.LITERATURE_CARD_REVIEW
),
queue_cards as (
    select
        s.CARD_ID,
        s.CONCEPT_NAME,
        s.MIP_USAGE_TIER,
        s.EVIDENCE_READINESS_STATUS,
        s.DAILY_BAR_COMPATIBILITY,
        s.CONCEPT_TYPE,
        s.SOURCE_BOOK,
        s.PAGE_START,
        s.PAGE_END,
        s.APPLICABLE_AGENTS,
        s.REQUIRED_EVIDENCE,
        s.INVALIDATION_EVIDENCE,
        s.SUSPICION_REASON as QUEUE_REASON
    from MIP.KNOWLEDGE.V_LITERATURE_SUSPICIOUS_READY_CARDS s
    union all
    select
        c.CARD_ID,
        c.CONCEPT_NAME,
        c.MIP_USAGE_TIER,
        c.EVIDENCE_READINESS_STATUS,
        c.DAILY_BAR_COMPATIBILITY,
        c.CONCEPT_TYPE,
        c.SOURCE_BOOK,
        c.PAGE_START,
        c.PAGE_END,
        c.APPLICABLE_AGENTS,
        c.REQUIRED_EVIDENCE,
        c.INVALIDATION_EVIDENCE,
        'HUMAN_REVIEW_REQUIRED' as QUEUE_REASON
    from MIP.KNOWLEDGE.LITERATURE_CONCEPT_CARD c
    where c.MIP_USAGE_TIER = 'HUMAN_REVIEW_REQUIRED'
)
select
    q.CARD_ID,
    q.CONCEPT_NAME,
    q.MIP_USAGE_TIER,
    q.EVIDENCE_READINESS_STATUS,
    q.DAILY_BAR_COMPATIBILITY,
    q.CONCEPT_TYPE,
    q.SOURCE_BOOK,
    q.PAGE_START,
    q.PAGE_END,
    q.APPLICABLE_AGENTS,
    q.REQUIRED_EVIDENCE,
    q.INVALIDATION_EVIDENCE,
    array_agg(distinct q.QUEUE_REASON) as QUEUE_REASONS,
    max(r.REVIEW_STATUS) as REVIEW_STATUS,
    max(r.REVIEWED_BY)   as REVIEWED_BY,
    max(r.REVIEWED_AT)   as REVIEWED_AT,
    max(r.NOTES)         as REVIEW_NOTES
from queue_cards q
left join latest_review r
  on r.CARD_ID = q.CARD_ID
 and r.rn = 1
group by
    q.CARD_ID, q.CONCEPT_NAME, q.MIP_USAGE_TIER, q.EVIDENCE_READINESS_STATUS,
    q.DAILY_BAR_COMPATIBILITY, q.CONCEPT_TYPE, q.SOURCE_BOOK,
    q.PAGE_START, q.PAGE_END, q.APPLICABLE_AGENTS,
    q.REQUIRED_EVIDENCE, q.INVALIDATION_EVIDENCE;
