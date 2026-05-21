-- 020_literature_concept_views.sql
-- Purpose: Usage and research views for MIP.KNOWLEDGE literature concepts.
--
-- ADVISORY NOTICE:
--   All views in this file expose advisory literature knowledge only.
--   These views are for inspection, audit, and future design research.
--   They are NOT wired into any live agent, proposal, revalidation, or execution path.
--   A separate approved task is required before any runtime component queries these views.

use role MIP_ADMIN_ROLE;
use database MIP;
use schema MIP.KNOWLEDGE;

-- Canonical duplicate filter used across all views:
--   (DUPLICATE_GROUP_ID IS NULL OR CANONICAL_CANDIDATE = TRUE)

-- =============================================================================
-- 1. V_LITERATURE_AGENT_READY_NOW
--    Cards safe to inspect now: DAILY_COMPATIBLE, full evidence, not duplicates.
-- =============================================================================

create or replace view MIP.KNOWLEDGE.V_LITERATURE_AGENT_READY_NOW
    comment = 'ADVISORY ONLY. Cards classified AGENT_READY_NOW. AGENT_READY_NOW means safe to inspect — it does NOT activate or authorize agent use. NOT market evidence. NOT trading authority.'
as
select
    CARD_ID,
    CONCEPT_NAME,
    CONCEPT_TYPE,
    CARD_ROLE,
    CONCEPT_FAMILY,
    EVIDENCE_READINESS_STATUS,
    MIP_USAGE_TIER,
    DAILY_BAR_COMPATIBILITY,
    CONFIDENCE,
    SOURCE_BOOK,
    SOURCE_FILE,
    PAGE_START,
    PAGE_END,
    CHUNK_ID,
    SOURCE_QUOTE_SHORT,
    MARKET_CONTEXT,
    SETUP_DEFINITION,
    TIMEFRAME_DEPENDENCY,
    EXTRACTION_NOTES,
    REQUIRED_EVIDENCE,
    CONFIRMATION_EVIDENCE,
    INVALIDATION_EVIDENCE,
    FAILURE_MODES,
    TRADE_MANAGEMENT_IMPLICATIONS,
    MIP_SETUP_FAMILY_MAPPING,
    APPLICABLE_AGENTS,
    REQUIRED_INTRADAY_FEATURES,
    DUPLICATE_GROUP_ID,
    CANONICAL_CANDIDATE,
    LOAD_BATCH_ID,
    LOADED_AT
from MIP.KNOWLEDGE.LITERATURE_CONCEPT_CARD
where MIP_USAGE_TIER = 'AGENT_READY_NOW'
  and EVIDENCE_READINESS_STATUS = 'READY_FOR_DAILY_AGENT_USE'
  and DAILY_BAR_COMPATIBILITY = 'DAILY_COMPATIBLE'
  and (DUPLICATE_GROUP_ID IS NULL OR CANONICAL_CANDIDATE = TRUE);

-- =============================================================================
-- 2. V_LITERATURE_AGENT_READY_WITH_CAUTION
--    Daily-compatible-with-caution cards. Require manual review before use.
-- =============================================================================

create or replace view MIP.KNOWLEDGE.V_LITERATURE_AGENT_READY_WITH_CAUTION
    comment = 'ADVISORY ONLY. Cards classified AGENT_READY_WITH_CAUTION (DAILY_COMPATIBLE_WITH_CAUTION). Require manual review before use. NOT market evidence. NOT trading authority.'
as
select
    CARD_ID,
    CONCEPT_NAME,
    CONCEPT_TYPE,
    CARD_ROLE,
    CONCEPT_FAMILY,
    EVIDENCE_READINESS_STATUS,
    MIP_USAGE_TIER,
    DAILY_BAR_COMPATIBILITY,
    CONFIDENCE,
    SOURCE_BOOK,
    SOURCE_FILE,
    PAGE_START,
    PAGE_END,
    CHUNK_ID,
    SOURCE_QUOTE_SHORT,
    MARKET_CONTEXT,
    SETUP_DEFINITION,
    TIMEFRAME_DEPENDENCY,
    EXTRACTION_NOTES,
    REQUIRED_EVIDENCE,
    CONFIRMATION_EVIDENCE,
    INVALIDATION_EVIDENCE,
    FAILURE_MODES,
    TRADE_MANAGEMENT_IMPLICATIONS,
    MIP_SETUP_FAMILY_MAPPING,
    APPLICABLE_AGENTS,
    REQUIRED_INTRADAY_FEATURES,
    DUPLICATE_GROUP_ID,
    CANONICAL_CANDIDATE,
    LOAD_BATCH_ID,
    LOADED_AT
from MIP.KNOWLEDGE.LITERATURE_CONCEPT_CARD
where MIP_USAGE_TIER = 'AGENT_READY_WITH_CAUTION'
  and EVIDENCE_READINESS_STATUS = 'READY_FOR_DAILY_AGENT_USE'
  and DAILY_BAR_COMPATIBILITY = 'DAILY_COMPATIBLE_WITH_CAUTION'
  and (DUPLICATE_GROUP_ID IS NULL OR CANONICAL_CANDIDATE = TRUE);

-- =============================================================================
-- 3. V_LITERATURE_FUTURE_INTRADAY
--    Intraday-native concepts. Useful future knowledge requiring intraday features.
-- =============================================================================

create or replace view MIP.KNOWLEDGE.V_LITERATURE_FUTURE_INTRADAY
    comment = 'ADVISORY ONLY. Intraday-native concepts. These are NOT discarded — they are useful future knowledge requiring intraday summary features not yet built in MIP.'
as
select
    CARD_ID,
    CONCEPT_NAME,
    CONCEPT_TYPE,
    CARD_ROLE,
    CONCEPT_FAMILY,
    EVIDENCE_READINESS_STATUS,
    MIP_USAGE_TIER,
    DAILY_BAR_COMPATIBILITY,
    CONFIDENCE,
    SOURCE_BOOK,
    PAGE_START,
    PAGE_END,
    REQUIRED_INTRADAY_FEATURES,
    APPLICABLE_AGENTS,
    REQUIRED_EVIDENCE,
    INVALIDATION_EVIDENCE,
    MARKET_CONTEXT,
    SETUP_DEFINITION,
    DUPLICATE_GROUP_ID,
    CANONICAL_CANDIDATE,
    LOAD_BATCH_ID
from MIP.KNOWLEDGE.LITERATURE_CONCEPT_CARD
where MIP_USAGE_TIER = 'FUTURE_INTRADAY'
  and EVIDENCE_READINESS_STATUS = 'INTRADAY_REQUIRES_FEATURES';

-- =============================================================================
-- 4. V_LITERATURE_REFERENCE_LIBRARY
--    Reference and context cards. For explanation/regime language only.
-- =============================================================================

create or replace view MIP.KNOWLEDGE.V_LITERATURE_REFERENCE_LIBRARY
    comment = 'ADVISORY ONLY. Reference and market context cards. Useful for explanation language but must NOT drive verdict strength.'
as
select
    CARD_ID,
    CONCEPT_NAME,
    CONCEPT_TYPE,
    CARD_ROLE,
    CONCEPT_FAMILY,
    EVIDENCE_READINESS_STATUS,
    MIP_USAGE_TIER,
    DAILY_BAR_COMPATIBILITY,
    CONFIDENCE,
    SOURCE_BOOK,
    PAGE_START,
    PAGE_END,
    MARKET_CONTEXT,
    SETUP_DEFINITION,
    APPLICABLE_AGENTS,
    MIP_SETUP_FAMILY_MAPPING,
    LOAD_BATCH_ID
from MIP.KNOWLEDGE.LITERATURE_CONCEPT_CARD
where MIP_USAGE_TIER = 'REFERENCE_LIBRARY'
  and EVIDENCE_READINESS_STATUS = 'REFERENCE_ONLY';

-- =============================================================================
-- 5. V_LITERATURE_NEEDS_REVIEW
--    Cards requiring human triage before any use.
-- =============================================================================

create or replace view MIP.KNOWLEDGE.V_LITERATURE_NEEDS_REVIEW
    comment = 'ADVISORY ONLY. Cards requiring human review before any potential use. Missing evidence, agents, or other quality flags.'
as
select
    CARD_ID,
    CONCEPT_NAME,
    CONCEPT_TYPE,
    CARD_ROLE,
    CONCEPT_FAMILY,
    EVIDENCE_READINESS_STATUS,
    MIP_USAGE_TIER,
    DAILY_BAR_COMPATIBILITY,
    CONFIDENCE,
    SOURCE_BOOK,
    PAGE_START,
    PAGE_END,
    EXTRACTION_NOTES,
    APPLICABLE_AGENTS,
    REQUIRED_EVIDENCE,
    INVALIDATION_EVIDENCE,
    LOAD_BATCH_ID
from MIP.KNOWLEDGE.LITERATURE_CONCEPT_CARD
where MIP_USAGE_TIER = 'HUMAN_REVIEW_REQUIRED'
  and EVIDENCE_READINESS_STATUS = 'NEEDS_REVIEW';

-- =============================================================================
-- 6. V_LITERATURE_DUPLICATE_REVIEW
--    Non-canonical duplicate candidates for review.
-- =============================================================================

create or replace view MIP.KNOWLEDGE.V_LITERATURE_DUPLICATE_REVIEW
    comment = 'Non-canonical duplicate candidates. Originals are preserved — these are marked for review, not deleted.'
as
select
    CARD_ID,
    CONCEPT_NAME,
    CONCEPT_TYPE,
    CARD_ROLE,
    CONCEPT_FAMILY,
    EVIDENCE_READINESS_STATUS,
    MIP_USAGE_TIER,
    SOURCE_BOOK,
    PAGE_START,
    PAGE_END,
    DUPLICATE_GROUP_ID,
    DUPLICATE_CONFIDENCE,
    CANONICAL_CANDIDATE,
    DUPLICATE_REASON,
    LOAD_BATCH_ID
from MIP.KNOWLEDGE.LITERATURE_CONCEPT_CARD
where MIP_USAGE_TIER = 'DUPLICATE_REVIEW'
  and EVIDENCE_READINESS_STATUS = 'DUPLICATE_CANDIDATE';

-- =============================================================================
-- 7. V_LITERATURE_REVALIDATION_CANDIDATES
--    Cards most relevant for future revalidation agent research.
--    NOT wired into revalidation runtime.
-- =============================================================================

create or replace view MIP.KNOWLEDGE.V_LITERATURE_REVALIDATION_CANDIDATES
    comment = 'ADVISORY ONLY. Cards relevant for future revalidation agent research. NOT wired into revalidation runtime. NOT market evidence. A separate approved task is required before runtime use.'
as
select
    CARD_ID,
    CONCEPT_NAME,
    CONCEPT_FAMILY,
    MIP_USAGE_TIER,
    EVIDENCE_READINESS_STATUS,
    DAILY_BAR_COMPATIBILITY,
    CONFIDENCE,
    REQUIRED_INTRADAY_FEATURES,
    REQUIRED_EVIDENCE,
    INVALIDATION_EVIDENCE,
    FAILURE_MODES,
    TRADE_MANAGEMENT_IMPLICATIONS,
    MIP_SETUP_FAMILY_MAPPING,
    APPLICABLE_AGENTS,
    SOURCE_BOOK,
    PAGE_START,
    PAGE_END,
    LOAD_BATCH_ID
from MIP.KNOWLEDGE.LITERATURE_CONCEPT_CARD
where MIP_USAGE_TIER != 'HUMAN_REVIEW_REQUIRED'
  and (DUPLICATE_GROUP_ID IS NULL OR CANONICAL_CANDIDATE = TRUE)
  and (
      ARRAY_CONTAINS('Revalidation Agent'::variant, APPLICABLE_AGENTS)
      or CONCEPT_FAMILY in (
          'FAILED_BREAKOUT',
          'TREND_PULLBACK',
          'BREAKOUT_PULLBACK',
          'TWO_BAR_THREE_BAR_REVERSAL',
          'WEDGE_FINAL_FLAG',
          'CLIMAX_EXHAUSTION',
          'TREND_CHANNEL',
          'TRAILING_STOP_POSITION_MANAGEMENT'
      )
  );

-- =============================================================================
-- 8. V_LITERATURE_POSITION_HEALTH_CANDIDATES
--    Cards most relevant for future position health research.
--    NOT wired into position health runtime.
-- =============================================================================

create or replace view MIP.KNOWLEDGE.V_LITERATURE_POSITION_HEALTH_CANDIDATES
    comment = 'ADVISORY ONLY. Cards relevant for future position health and trade management research. NOT wired into position health runtime. A separate approved task is required before runtime use.'
as
select
    CARD_ID,
    CONCEPT_NAME,
    CONCEPT_FAMILY,
    MIP_USAGE_TIER,
    EVIDENCE_READINESS_STATUS,
    DAILY_BAR_COMPATIBILITY,
    CONFIDENCE,
    REQUIRED_INTRADAY_FEATURES,
    REQUIRED_EVIDENCE,
    INVALIDATION_EVIDENCE,
    FAILURE_MODES,
    TRADE_MANAGEMENT_IMPLICATIONS,
    MIP_SETUP_FAMILY_MAPPING,
    APPLICABLE_AGENTS,
    SOURCE_BOOK,
    PAGE_START,
    PAGE_END,
    LOAD_BATCH_ID
from MIP.KNOWLEDGE.LITERATURE_CONCEPT_CARD
where MIP_USAGE_TIER != 'HUMAN_REVIEW_REQUIRED'
  and (DUPLICATE_GROUP_ID IS NULL OR CANONICAL_CANDIDATE = TRUE)
  and (
      ARRAY_CONTAINS('Position Health Agent'::variant, APPLICABLE_AGENTS)
      or CONCEPT_FAMILY in (
          'TRAILING_STOP_POSITION_MANAGEMENT',
          'RISK_POSITION_SIZING',
          'CLIMAX_EXHAUSTION',
          'TREND_CHANNEL',
          'TRADING_RANGE',
          'FAILED_BREAKOUT'
      )
  );

-- =============================================================================
-- 9. V_LITERATURE_PROPOSAL_CANDIDATES_AUDIT_ONLY
--    Audit-only view for proposal board research. NOT for runtime use.
-- =============================================================================

create or replace view MIP.KNOWLEDGE.V_LITERATURE_PROPOSAL_CANDIDATES_AUDIT_ONLY
    comment = 'AUDIT/RESEARCH ONLY. Cards tagged for Proposal Chair. This view MUST NOT be wired into proposal generation without a separate approved task. NOT market evidence. NOT trading authority.'
as
select
    CARD_ID,
    CONCEPT_NAME,
    CONCEPT_FAMILY,
    MIP_USAGE_TIER,
    EVIDENCE_READINESS_STATUS,
    DAILY_BAR_COMPATIBILITY,
    CONFIDENCE,
    REQUIRED_EVIDENCE,
    INVALIDATION_EVIDENCE,
    FAILURE_MODES,
    MIP_SETUP_FAMILY_MAPPING,
    APPLICABLE_AGENTS,
    SOURCE_BOOK,
    PAGE_START,
    PAGE_END,
    LOAD_BATCH_ID
from MIP.KNOWLEDGE.LITERATURE_CONCEPT_CARD
where MIP_USAGE_TIER != 'HUMAN_REVIEW_REQUIRED'
  and (DUPLICATE_GROUP_ID IS NULL OR CANONICAL_CANDIDATE = TRUE)
  and ARRAY_CONTAINS('Proposal Chair'::variant, APPLICABLE_AGENTS);

-- =============================================================================
-- 10. V_LITERATURE_INTRADAY_FEATURE_ROADMAP
--     Flatten REQUIRED_INTRADAY_FEATURES to show which future features would
--     unlock the most Brooks-derived knowledge.
-- =============================================================================

create or replace view MIP.KNOWLEDGE.V_LITERATURE_INTRADAY_FEATURE_ROADMAP
    comment = 'Flattens REQUIRED_INTRADAY_FEATURES to show which intraday summary features would unlock the most future knowledge cards.'
as
select
    f.value::string                                                     as REQUIRED_INTRADAY_FEATURE,
    count(*)                                                            as CARD_COUNT,
    array_agg(distinct c.CONCEPT_FAMILY)                               as CONCEPT_FAMILIES,
    array_agg(distinct c.SOURCE_BOOK)                                  as SOURCE_BOOKS,
    array_slice(array_agg(c.CONCEPT_NAME), 0, 5)                      as EXAMPLE_CARDS
from MIP.KNOWLEDGE.LITERATURE_CONCEPT_CARD c,
    lateral flatten(input => c.REQUIRED_INTRADAY_FEATURES) f
where c.EVIDENCE_READINESS_STATUS = 'INTRADAY_REQUIRES_FEATURES'
group by f.value::string
order by CARD_COUNT desc;
