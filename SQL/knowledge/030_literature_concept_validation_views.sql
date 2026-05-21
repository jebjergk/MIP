-- 030_literature_concept_validation_views.sql
-- Purpose: Validation and audit views for the MIP.KNOWLEDGE literature staging layer.
--
-- These views are for post-load quality inspection only.
-- They are NOT wired into any live agent or execution path.

use role MIP_ADMIN_ROLE;
use database MIP;
use schema MIP.KNOWLEDGE;

-- =============================================================================
-- 1. V_LITERATURE_LOAD_COUNTS
--    Multi-dimensional count breakdown for post-load verification.
-- =============================================================================

create or replace view MIP.KNOWLEDGE.V_LITERATURE_LOAD_COUNTS
    comment = 'Count breakdown by key dimensions for post-load verification.'
as
select
    SOURCE_BOOK,
    EVIDENCE_READINESS_STATUS,
    MIP_USAGE_TIER,
    CONCEPT_FAMILY,
    DAILY_BAR_COMPATIBILITY,
    CARD_ROLE,
    CONCEPT_TYPE,
    count(*) as CARD_COUNT
from MIP.KNOWLEDGE.LITERATURE_CONCEPT_CARD
group by
    SOURCE_BOOK,
    EVIDENCE_READINESS_STATUS,
    MIP_USAGE_TIER,
    CONCEPT_FAMILY,
    DAILY_BAR_COMPATIBILITY,
    CARD_ROLE,
    CONCEPT_TYPE
order by SOURCE_BOOK, CARD_COUNT desc;

-- =============================================================================
-- 2. V_LITERATURE_AGENT_READY_COUNTS
--    One row per MIP_USAGE_TIER with total card count.
-- =============================================================================

create or replace view MIP.KNOWLEDGE.V_LITERATURE_AGENT_READY_COUNTS
    comment = 'Total card counts grouped by MIP_USAGE_TIER. Compare against postprocess_summary.json to verify load integrity.'
as
select
    MIP_USAGE_TIER,
    count(*) as CARD_COUNT
from MIP.KNOWLEDGE.LITERATURE_CONCEPT_CARD
group by MIP_USAGE_TIER
order by CARD_COUNT desc;

-- =============================================================================
-- 3. V_LITERATURE_SUSPICIOUS_READY_CARDS
--    Agent-ready cards that may need attention:
--    - Missing required_evidence or invalidation_evidence
--    - Wrong daily_bar_compatibility for AGENT_READY_NOW
--    - Contains option/derivatives keywords (not appropriate for stock-only daily flow)
--    - Empty applicable_agents
-- =============================================================================

create or replace view MIP.KNOWLEDGE.V_LITERATURE_SUSPICIOUS_READY_CARDS
    comment = 'Agent-ready cards with potential quality issues: missing evidence, wrong compatibility, options/derivatives keywords, or empty agents. Review before any potential use.'
as
select
    CARD_ID,
    CONCEPT_NAME,
    MIP_USAGE_TIER,
    EVIDENCE_READINESS_STATUS,
    DAILY_BAR_COMPATIBILITY,
    CONCEPT_TYPE,
    SOURCE_BOOK,
    PAGE_START,
    PAGE_END,
    APPLICABLE_AGENTS,
    REQUIRED_EVIDENCE,
    INVALIDATION_EVIDENCE,
    case
        when MIP_USAGE_TIER = 'AGENT_READY_NOW'
             and DAILY_BAR_COMPATIBILITY != 'DAILY_COMPATIBLE'
            then 'AGENT_READY_NOW_BUT_NOT_DAILY_COMPATIBLE'
        when MIP_USAGE_TIER in ('AGENT_READY_NOW', 'AGENT_READY_WITH_CAUTION')
             and (REQUIRED_EVIDENCE is null or array_size(REQUIRED_EVIDENCE) = 0)
            then 'AGENT_READY_EMPTY_REQUIRED_EVIDENCE'
        when MIP_USAGE_TIER in ('AGENT_READY_NOW', 'AGENT_READY_WITH_CAUTION')
             and (INVALIDATION_EVIDENCE is null or array_size(INVALIDATION_EVIDENCE) = 0)
            then 'AGENT_READY_EMPTY_INVALIDATION_EVIDENCE'
        when MIP_USAGE_TIER in ('AGENT_READY_NOW', 'AGENT_READY_WITH_CAUTION')
             and (APPLICABLE_AGENTS is null or array_size(APPLICABLE_AGENTS) = 0)
            then 'AGENT_READY_EMPTY_APPLICABLE_AGENTS'
        when MIP_USAGE_TIER in ('AGENT_READY_NOW', 'AGENT_READY_WITH_CAUTION')
             and (
                 lower(CONCEPT_NAME) like '%option%'
                 or lower(CONCEPT_NAME) like '%options%'
                 or lower(CONCEPT_NAME) like '% call%'
                 or lower(CONCEPT_NAME) like '% calls%'
                 or lower(CONCEPT_NAME) like '% put%'
                 or lower(CONCEPT_NAME) like '% puts%'
                 or lower(CONCEPT_NAME) like '%spread%'
                 or lower(CONCEPT_NAME) like '%spreads%'
                 or lower(SETUP_DEFINITION) like '%option%'
                 or lower(SETUP_DEFINITION) like '% call %'
                 or lower(SETUP_DEFINITION) like '% put %'
                 or lower(SETUP_DEFINITION) like '%spread%'
                 or lower(MARKET_CONTEXT) like '%option%'
                 or lower(MARKET_CONTEXT) like '% put %'
             )
            then 'AGENT_READY_CONTAINS_OPTION_DERIVATIVES_KEYWORDS'
        else 'OTHER_SUSPICIOUS'
    end as SUSPICION_REASON
from MIP.KNOWLEDGE.LITERATURE_CONCEPT_CARD
where MIP_USAGE_TIER in ('AGENT_READY_NOW', 'AGENT_READY_WITH_CAUTION')
  and (
      (MIP_USAGE_TIER = 'AGENT_READY_NOW' and DAILY_BAR_COMPATIBILITY != 'DAILY_COMPATIBLE')
      or (REQUIRED_EVIDENCE is null or array_size(REQUIRED_EVIDENCE) = 0)
      or (INVALIDATION_EVIDENCE is null or array_size(INVALIDATION_EVIDENCE) = 0)
      or (APPLICABLE_AGENTS is null or array_size(APPLICABLE_AGENTS) = 0)
      or lower(CONCEPT_NAME) like '%option%'
      or lower(CONCEPT_NAME) like '%options%'
      or lower(CONCEPT_NAME) like '% call%'
      or lower(CONCEPT_NAME) like '% calls%'
      or lower(CONCEPT_NAME) like '% put%'
      or lower(CONCEPT_NAME) like '% puts%'
      or lower(CONCEPT_NAME) like '%spread%'
      or lower(CONCEPT_NAME) like '%spreads%'
      or lower(SETUP_DEFINITION) like '%option%'
      or lower(SETUP_DEFINITION) like '% call %'
      or lower(SETUP_DEFINITION) like '% put %'
      or lower(SETUP_DEFINITION) like '%spread%'
      or lower(MARKET_CONTEXT) like '%option%'
      or lower(MARKET_CONTEXT) like '% put %'
  );

-- =============================================================================
-- 4. V_LITERATURE_DUPLICATE_GROUP_SUMMARY
--    One row per duplicate_group_id showing all members and canonical card.
-- =============================================================================

create or replace view MIP.KNOWLEDGE.V_LITERATURE_DUPLICATE_GROUP_SUMMARY
    comment = 'One row per duplicate group. Shows canonical card, all member names, source books, families, and tiers.'
as
select
    DUPLICATE_GROUP_ID,
    count(*)                                                        as CARD_COUNT,
    max(case when CANONICAL_CANDIDATE = true then CARD_ID end)     as CANONICAL_CARD_ID,
    max(case when CANONICAL_CANDIDATE = true then CONCEPT_NAME end) as CANONICAL_CONCEPT_NAME,
    array_agg(distinct CONCEPT_NAME)                               as ALL_CONCEPT_NAMES,
    array_agg(distinct SOURCE_BOOK)                                as SOURCE_BOOKS,
    array_agg(distinct CONCEPT_FAMILY)                             as CONCEPT_FAMILIES,
    array_agg(distinct MIP_USAGE_TIER)                             as MIP_USAGE_TIERS,
    array_agg(distinct DUPLICATE_REASON)                           as DUPLICATE_REASONS
from MIP.KNOWLEDGE.LITERATURE_CONCEPT_CARD
where DUPLICATE_GROUP_ID is not null
group by DUPLICATE_GROUP_ID
order by CARD_COUNT desc;

-- =============================================================================
-- 5. V_LITERATURE_MIP_SETUP_FAMILY_MAP
--    Flatten MIP_SETUP_FAMILY_MAPPING to show card counts per structural family.
-- =============================================================================

create or replace view MIP.KNOWLEDGE.V_LITERATURE_MIP_SETUP_FAMILY_MAP
    comment = 'Flattens MIP_SETUP_FAMILY_MAPPING to show card counts and agent-readiness per structural MIP setup family.'
as
select
    f.value::string                                                     as MIP_SETUP_FAMILY,
    count(*)                                                            as CARD_COUNT,
    sum(case when MIP_USAGE_TIER = 'AGENT_READY_NOW' then 1 else 0 end)           as AGENT_READY_NOW_COUNT,
    sum(case when MIP_USAGE_TIER = 'AGENT_READY_WITH_CAUTION' then 1 else 0 end)  as AGENT_READY_WITH_CAUTION_COUNT,
    sum(case when MIP_USAGE_TIER = 'FUTURE_INTRADAY' then 1 else 0 end)           as FUTURE_INTRADAY_COUNT,
    array_slice(array_agg(c.CONCEPT_NAME), 0, 5)                      as EXAMPLE_CARDS
from MIP.KNOWLEDGE.LITERATURE_CONCEPT_CARD c,
    lateral flatten(input => c.MIP_SETUP_FAMILY_MAPPING) f
group by f.value::string
order by CARD_COUNT desc;

-- =============================================================================
-- 6. V_LITERATURE_SOURCE_BOOK_COVERAGE
--    Card counts per source book broken down by MIP_USAGE_TIER.
-- =============================================================================

create or replace view MIP.KNOWLEDGE.V_LITERATURE_SOURCE_BOOK_COVERAGE
    comment = 'Card counts per source book broken down by tier, with page coverage.'
as
select
    SOURCE_BOOK,
    count(*)                                                                        as TOTAL_CARDS,
    sum(case when MIP_USAGE_TIER = 'AGENT_READY_NOW' then 1 else 0 end)           as AGENT_READY_NOW,
    sum(case when MIP_USAGE_TIER = 'AGENT_READY_WITH_CAUTION' then 1 else 0 end)  as AGENT_READY_WITH_CAUTION,
    sum(case when MIP_USAGE_TIER = 'FUTURE_INTRADAY' then 1 else 0 end)           as FUTURE_INTRADAY,
    sum(case when MIP_USAGE_TIER = 'REFERENCE_LIBRARY' then 1 else 0 end)         as REFERENCE_LIBRARY,
    sum(case when MIP_USAGE_TIER = 'HUMAN_REVIEW_REQUIRED' then 1 else 0 end)     as NEEDS_REVIEW,
    sum(case when MIP_USAGE_TIER = 'DUPLICATE_REVIEW' then 1 else 0 end)          as DUPLICATES,
    min(PAGE_START)                                                                 as MIN_PAGE,
    max(PAGE_END)                                                                   as MAX_PAGE
from MIP.KNOWLEDGE.LITERATURE_CONCEPT_CARD
group by SOURCE_BOOK
order by TOTAL_CARDS desc;

-- =============================================================================
-- 7. V_LITERATURE_DUPLICATE_CARD_IDS
--    Find CARD_IDs loaded more than once. Expected result: 0 rows.
--    NOTE: Snowflake does not enforce PRIMARY KEY uniqueness — use this view
--    to verify uniqueness after each load.
-- =============================================================================

create or replace view MIP.KNOWLEDGE.V_LITERATURE_DUPLICATE_CARD_IDS
    comment = 'Lists CARD_IDs that appear more than once. Expected: 0 rows after a clean load. Snowflake does not enforce PRIMARY KEY — use this view to verify uniqueness.'
as
select
    CARD_ID,
    count(*) as ROW_COUNT,
    array_agg(CONCEPT_NAME) as CONCEPT_NAMES,
    array_agg(LOAD_BATCH_ID) as LOAD_BATCH_IDS
from MIP.KNOWLEDGE.LITERATURE_CONCEPT_CARD
group by CARD_ID
having count(*) > 1
order by ROW_COUNT desc;
