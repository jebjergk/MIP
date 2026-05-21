-- 090_literature_concept_smoke_queries.sql
-- Purpose: Post-load validation queries for MIP.KNOWLEDGE literature staging.
--
-- Run with:
--   cursorfiles\.venv\Scripts\python.exe cursorfiles/query_snowflake.py -f MIP/SQL/knowledge/090_literature_concept_smoke_queries.sql
--
-- Expected counts are from the BROOKS_2026_05_21 postprocess run.
-- These are reference targets, not hard assertions.

use role MIP_ADMIN_ROLE;
use database MIP;
use schema MIP.KNOWLEDGE;

-- =============================================================================
-- Smoke 1: Total cards loaded
-- Expected: ~1482
-- =============================================================================

select count(*) as TOTAL_CARDS
from MIP.KNOWLEDGE.LITERATURE_CONCEPT_CARD;

-- =============================================================================
-- Smoke 2: Cards per source book
-- Expected: 3 books
--   Al Brooks   Trading Price Action Reversals:       ~514
--   Al Brooks   Trading Price Action Trading Ranges:  ~608
--   Al Brooks   Trading Price Action Trends:          ~360
-- =============================================================================

select SOURCE_BOOK, count(*) as CARD_COUNT
from MIP.KNOWLEDGE.LITERATURE_CONCEPT_CARD
group by SOURCE_BOOK
order by CARD_COUNT desc;

-- =============================================================================
-- Smoke 3: Counts by MIP_USAGE_TIER
-- Expected:
--   FUTURE_INTRADAY:        ~682
--   AGENT_READY_WITH_CAUTION: ~354
--   AGENT_READY_NOW:          ~60
--   REFERENCE_LIBRARY:        ~167
--   HUMAN_REVIEW_REQUIRED:    ~107
--   DUPLICATE_REVIEW:         ~112
-- =============================================================================

select MIP_USAGE_TIER, count(*) as CARD_COUNT
from MIP.KNOWLEDGE.LITERATURE_CONCEPT_CARD
group by MIP_USAGE_TIER
order by CARD_COUNT desc;

-- =============================================================================
-- Smoke 4: Counts by EVIDENCE_READINESS_STATUS
-- Expected:
--   INTRADAY_REQUIRES_FEATURES: ~682
--   READY_FOR_DAILY_AGENT_USE:  ~414
--   REFERENCE_ONLY:             ~167
--   NEEDS_REVIEW:               ~107
--   DUPLICATE_CANDIDATE:        ~112
-- =============================================================================

select EVIDENCE_READINESS_STATUS, count(*) as CARD_COUNT
from MIP.KNOWLEDGE.LITERATURE_CONCEPT_CARD
group by EVIDENCE_READINESS_STATUS
order by CARD_COUNT desc;

-- =============================================================================
-- Smoke 5: AGENT_READY_NOW card count
-- Expected: ~60
-- =============================================================================

select count(*) as AGENT_READY_NOW_COUNT
from MIP.KNOWLEDGE.V_LITERATURE_AGENT_READY_NOW;

-- =============================================================================
-- Smoke 6: FUTURE_INTRADAY card count
-- Expected: ~682
-- =============================================================================

select count(*) as FUTURE_INTRADAY_COUNT
from MIP.KNOWLEDGE.V_LITERATURE_FUTURE_INTRADAY;

-- =============================================================================
-- Smoke 7: Suspicious agent-ready cards count
-- Expected: small number (put-protection and options concepts from Trends)
-- =============================================================================

select SUSPICION_REASON, count(*) as CARD_COUNT
from MIP.KNOWLEDGE.V_LITERATURE_SUSPICIOUS_READY_CARDS
group by SUSPICION_REASON
order by CARD_COUNT desc;

-- =============================================================================
-- Smoke 8: Duplicate groups count
-- Expected: ~66 groups
-- =============================================================================

select count(distinct DUPLICATE_GROUP_ID) as DUPLICATE_GROUP_COUNT
from MIP.KNOWLEDGE.LITERATURE_CONCEPT_CARD
where DUPLICATE_GROUP_ID is not null;

-- =============================================================================
-- Smoke 9: Top concept families
-- =============================================================================

select CONCEPT_FAMILY, count(*) as CARD_COUNT
from MIP.KNOWLEDGE.LITERATURE_CONCEPT_CARD
group by CONCEPT_FAMILY
order by CARD_COUNT desc
limit 10;

-- =============================================================================
-- Smoke 10: Top required intraday features
-- (from V_LITERATURE_INTRADAY_FEATURE_ROADMAP)
-- =============================================================================

select REQUIRED_INTRADAY_FEATURE, CARD_COUNT
from MIP.KNOWLEDGE.V_LITERATURE_INTRADAY_FEATURE_ROADMAP
order by CARD_COUNT desc
limit 10;

-- =============================================================================
-- Smoke 11: CARD_ID uniqueness check
-- Expected: 0 rows (no duplicate CARD_IDs)
-- NOTE: Snowflake PRIMARY KEY is metadata only — this query enforces uniqueness.
-- =============================================================================

select CARD_ID, count(*) as ROW_COUNT
from MIP.KNOWLEDGE.LITERATURE_CONCEPT_CARD
group by CARD_ID
having count(*) > 1
order by ROW_COUNT desc;

-- =============================================================================
-- Smoke 12: Load batch record
-- =============================================================================

select LOAD_BATCH_ID, SOURCE_FILE_COUNT, RAW_CARD_COUNT, LOADED_CARD_COUNT,
       SKIPPED_LINE_COUNT, LOAD_STATUS, CREATED_AT, COMPLETED_AT
from MIP.KNOWLEDGE.LITERATURE_LOAD_BATCH
order by CREATED_AT desc
limit 5;

-- =============================================================================
-- Smoke 13: Postprocess summary matches loaded counts
-- =============================================================================

select
    s.NORMALIZED_CARDS                                                          as SUMMARY_NORMALIZED,
    (select count(*) from MIP.KNOWLEDGE.LITERATURE_CONCEPT_CARD)               as TABLE_TOTAL,
    s.NORMALIZED_CARDS - (select count(*) from MIP.KNOWLEDGE.LITERATURE_CONCEPT_CARD) as DELTA,
    s.LOAD_BATCH_ID
from MIP.KNOWLEDGE.LITERATURE_POSTPROCESS_SUMMARY s
order by s.LOADED_AT desc
limit 1;

-- =============================================================================
-- Smoke 14: Revalidation candidates count
-- =============================================================================

select count(*) as REVALIDATION_CANDIDATE_COUNT
from MIP.KNOWLEDGE.V_LITERATURE_REVALIDATION_CANDIDATES;

-- =============================================================================
-- Smoke 15: Position health candidates count
-- =============================================================================

select count(*) as POSITION_HEALTH_CANDIDATE_COUNT
from MIP.KNOWLEDGE.V_LITERATURE_POSITION_HEALTH_CANDIDATES;
