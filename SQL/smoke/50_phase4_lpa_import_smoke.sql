-- =============================================================================
-- 50_phase4_lpa_import_smoke.sql
-- Phase 2A LPA delivery plumbing — post-deploy validation (read-only + repair)
--
-- T50-A  Backfill missing geometry on executable proposals (chair null zones)
-- T50-B  Cross-direction PG visible in timeline RESEARCH_PROPOSALS count
-- T50-C  Executable authoritative proposals must be import-ready (geometry present)
-- T50-D  No open executable PROPOSED rows with NULL entry zones
-- =============================================================================

USE ROLE MIP_ADMIN_ROLE;
USE WAREHOUSE MIP_WH_XS;
USE DATABASE MIP;
USE SCHEMA APP;

-- T50-A: Repair proposals where chair omitted entry zones but setup event has them.
UPDATE MIP.APP.STRUCTURAL_TRADE_PROPOSALS sp
   SET ENTRY_ZONE_LOW = COALESCE(sp.ENTRY_ZONE_LOW, ev.ENTRY_ZONE_LOW),
       ENTRY_ZONE_HIGH = COALESCE(sp.ENTRY_ZONE_HIGH, ev.ENTRY_ZONE_HIGH),
       PRICE_INVALIDATION_LEVEL = COALESCE(sp.PRICE_INVALIDATION_LEVEL, ev.PRICE_INVALIDATION_LEVEL),
       INVALIDATION_RULE = COALESCE(sp.INVALIDATION_RULE, ev.INVALIDATION_RULE, 'AGENTIC_INVALIDATION')
  FROM MIP.APP.STRUCTURAL_SETUP_EVENTS ev
 WHERE sp.STATUS = 'PROPOSED'
   AND sp.EXECUTION_POLICY_STATUS = 'EXECUTABLE'
   AND NOT COALESCE(sp.IS_RESEARCH_ONLY, FALSE)
   AND sp.SETUP_EVENT_ID = ev.SETUP_EVENT_ID
   AND (sp.ENTRY_ZONE_LOW IS NULL OR sp.ENTRY_ZONE_HIGH IS NULL OR sp.PRICE_INVALIDATION_LEVEL IS NULL);

-- T50-B: PG cross-direction row counts as research in timeline summary.
SELECT 'T50B_PG_RESEARCH_VISIBLE' AS CHECK_NAME,
       COALESCE(MAX(v.RESEARCH_PROPOSALS), 0) AS pg_research_count
  FROM MIP.MART.V_STRUCTURAL_TIMELINE_SUMMARY v
 WHERE v.SYMBOL = 'PG';
-- Expect >= 1 when proposal 4301 is still PROPOSED.

-- T50-C: Import-ready contract — executable authoritative rows have full geometry.
SELECT 'T50C_IMPORT_READY_GEOMETRY' AS CHECK_NAME,
       COUNT(*) AS bad_rows
  FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS sp
  JOIN MIP.MART.V_LATEST_AUTHORITATIVE_BOARD_RUN auth ON auth.RUN_ID = sp.BOARD_RUN_ID
 WHERE sp.STATUS = 'PROPOSED'
   AND sp.EXECUTION_POLICY_STATUS = 'EXECUTABLE'
   AND NOT COALESCE(sp.IS_RESEARCH_ONLY, FALSE)
   AND (sp.ENTRY_ZONE_LOW IS NULL OR sp.ENTRY_ZONE_HIGH IS NULL OR sp.PRICE_INVALIDATION_LEVEL IS NULL);
-- Expect 0.

-- T50-D: List any remaining geometry gaps for operator visibility.
SELECT 'T50D_GEOMETRY_GAPS' AS CHECK_NAME,
       sp.PROPOSAL_ID,
       sp.SYMBOL,
       sp.EXECUTION_POLICY_STATUS,
       sp.ENTRY_ZONE_LOW,
       sp.ENTRY_ZONE_HIGH,
       sp.PRICE_INVALIDATION_LEVEL
  FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS sp
 WHERE sp.STATUS = 'PROPOSED'
   AND sp.BOARD_RUN_ID IS NOT NULL
   AND (sp.ENTRY_ZONE_LOW IS NULL OR sp.ENTRY_ZONE_HIGH IS NULL OR sp.PRICE_INVALIDATION_LEVEL IS NULL)
 ORDER BY sp.PROPOSAL_ID DESC
 LIMIT 10;
