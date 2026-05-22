-- 094_shadow_pack_version_diagnostics.sql
-- Purpose: Diagnostic queries for shadow board evidence pack v2.0.0 upgrade.
--          Verifies Phase 4 slice availability, pack version distribution,
--          and null-safe behaviour for pre-Phase-4 proposals.
--
-- DIAGNOSTIC ONLY. No writes. No runtime impact.
--
-- Run with:
--   cursorfiles\.venv\Scripts\python.exe cursorfiles/query_snowflake.py -f MIP/SQL/knowledge/094_shadow_pack_version_diagnostics.sql

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;

-- =============================================================================
-- 1. Pack version distribution in SHADOW_BOARD_SESSION
-- =============================================================================
SELECT
    PACK_VERSION,
    STATUS,
    COUNT(*)            AS SESSION_COUNT,
    MAX(CREATED_AT)     AS LATEST_SESSION_AT,
    MIN(CREATED_AT)     AS EARLIEST_SESSION_AT
FROM MIP.APP.SHADOW_BOARD_SESSION
GROUP BY PACK_VERSION, STATUS
ORDER BY PACK_VERSION DESC, SESSION_COUNT DESC;

-- =============================================================================
-- 2. Find a recent Phase 4-native hearing (BOARD_DOSSIER_ID not null)
--    that can be used for a positive smoke test.
-- =============================================================================
SELECT
    h.HEARING_ID,
    p.PROPOSAL_ID,
    p.SYMBOL,
    p.DIRECTION,
    p.SETUP_FAMILY,
    p.BOARD_RUN_ID,
    p.BOARD_DOSSIER_ID,
    CASE WHEN tv.DOSSIER_ID IS NOT NULL THEN 'YES' ELSE 'NO' END AS HAS_THESIS_VERDICT,
    CASE WHEN ds.DOSSIER_ID IS NOT NULL THEN 'YES' ELSE 'NO' END AS HAS_DOSSIER_SNAPSHOT,
    tv.CHAIR_OUTPUT_JSON:thesis_health::STRING AS THESIS_HEALTH,
    ds.DOSSIER_PAYLOAD_JSON:actionability_context:continuation_quality::STRING AS CONTINUATION_QUALITY,
    h.HEARING_TS AS HEARING_TS
FROM MIP.APP.COMMITTEE_HEARING h
JOIN MIP.APP.STRUCTURAL_TRADE_PROPOSALS p ON p.PROPOSAL_ID = h.PROPOSAL_ID
LEFT JOIN MIP.APP.PROPOSAL_BOARD_THESIS_VERDICT tv
       ON tv.RUN_ID = p.BOARD_RUN_ID AND tv.DOSSIER_ID = p.BOARD_DOSSIER_ID
LEFT JOIN MIP.APP.PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT ds
       ON ds.RUN_ID = p.BOARD_RUN_ID AND ds.DOSSIER_ID = p.BOARD_DOSSIER_ID
WHERE p.BOARD_DOSSIER_ID IS NOT NULL
  AND p.BOARD_RUN_ID IS NOT NULL
ORDER BY h.HEARING_TS DESC
LIMIT 5;

-- =============================================================================
-- 3. Find a pre-Phase-4 hearing (BOARD_DOSSIER_ID is null)
--    that can be used for the null-safe smoke test.
-- =============================================================================
SELECT
    h.HEARING_ID,
    p.PROPOSAL_ID,
    p.SYMBOL,
    p.DIRECTION,
    p.SETUP_FAMILY,
    p.BOARD_RUN_ID,
    p.BOARD_DOSSIER_ID,
    h.HEARING_TS AS HEARING_TS
FROM MIP.APP.COMMITTEE_HEARING h
JOIN MIP.APP.STRUCTURAL_TRADE_PROPOSALS p ON p.PROPOSAL_ID = h.PROPOSAL_ID
WHERE (p.BOARD_DOSSIER_ID IS NULL OR p.BOARD_RUN_ID IS NULL)
ORDER BY h.HEARING_TS DESC
LIMIT 5;

-- =============================================================================
-- 4. Smoke test A: Phase 4-aware hearing — verify GET_SHADOW_EVIDENCE_SLICE
--    returns phase4_thesis_verdict and phase4_dossier_context slices.
--    Replace <PHASE4_HEARING_ID> with a HEARING_ID from query 2 above.
--
--    Expected: slice data returned with phase4_available=true
-- =============================================================================
-- CALL MIP.APP.GET_SHADOW_EVIDENCE_SLICE('<PHASE4_HEARING_ID>', 'STRUCTURAL_THESIS', 'phase4_thesis_verdict');
-- CALL MIP.APP.GET_SHADOW_EVIDENCE_SLICE('<PHASE4_HEARING_ID>', 'STRUCTURAL_THESIS', 'phase4_dossier_context');
-- CALL MIP.APP.GET_SHADOW_EVIDENCE_SLICE('<PHASE4_HEARING_ID>', 'SHADOW_CHAIR', 'phase4_thesis_verdict');
-- CALL MIP.APP.GET_SHADOW_EVIDENCE_SLICE('<PHASE4_HEARING_ID>', 'SHADOW_CHAIR', 'phase4_dossier_context');

-- =============================================================================
-- 5. Smoke test B: Pre-Phase-4 hearing — verify GET_SHADOW_EVIDENCE_SLICE
--    returns phase4_available=false without error.
--    Replace <PRE_PHASE4_HEARING_ID> with a HEARING_ID from query 3 above.
--
--    Expected: slice returned with phase4_available=false, all fields null
-- =============================================================================
-- CALL MIP.APP.GET_SHADOW_EVIDENCE_SLICE('<PRE_PHASE4_HEARING_ID>', 'STRUCTURAL_THESIS', 'phase4_thesis_verdict');
-- CALL MIP.APP.GET_SHADOW_EVIDENCE_SLICE('<PRE_PHASE4_HEARING_ID>', 'SHADOW_CHAIR', 'phase4_dossier_context');

-- =============================================================================
-- 6. Verify GET_SHADOW_EVIDENCE_SLICE rejects invalid slice names
--    (allowlist enforcement — should return SLICE_NOT_IN_CATALOG error)
-- =============================================================================
-- CALL MIP.APP.GET_SHADOW_EVIDENCE_SLICE('any-id', 'STRUCTURAL_THESIS', 'real_board_verdict');
-- CALL MIP.APP.GET_SHADOW_EVIDENCE_SLICE('any-id', 'STRUCTURAL_THESIS', 'literature_support');

-- =============================================================================
-- 7. Verify role restrictions are enforced
--    (ENTRY_GEOMETRY should not access phase4_thesis_verdict)
--    Expected: SLICE_NOT_ALLOWED_FOR_ROLE
-- =============================================================================
-- CALL MIP.APP.GET_SHADOW_EVIDENCE_SLICE('any-id', 'ENTRY_GEOMETRY', 'phase4_thesis_verdict');

-- =============================================================================
-- 8. Inspect a cached evidence pack for a recent session — verify pack_version
--    and presence of Phase 4 slices in PACK_JSON.
-- =============================================================================
SELECT
    s.SESSION_ID,
    s.PACK_VERSION,
    s.STATUS,
    s.CREATED_AT,
    c.PACK_JSON:pack_version::STRING                                        AS PACK_JSON_VERSION,
    c.PACK_JSON:slices:phase4_thesis_verdict:phase4_available::BOOLEAN      AS P4T_AVAILABLE,
    c.PACK_JSON:slices:phase4_thesis_verdict:thesis_health::STRING          AS THESIS_HEALTH,
    c.PACK_JSON:slices:phase4_thesis_verdict:final_action::STRING           AS FINAL_ACTION,
    c.PACK_JSON:slices:phase4_dossier_context:phase4_available::BOOLEAN     AS P4D_AVAILABLE,
    c.PACK_JSON:slices:phase4_dossier_context:continuation_quality::STRING  AS CONTINUATION_QUALITY,
    c.PACK_JSON:slices:phase4_dossier_context:resistance_overhead_risk::STRING AS RESISTANCE_RISK
FROM MIP.APP.SHADOW_BOARD_SESSION s
JOIN MIP.APP.SHADOW_EVIDENCE_PACK_CACHE c ON c.SESSION_ID = s.SESSION_ID
WHERE s.PACK_VERSION = '2.0.0'
ORDER BY s.CREATED_AT DESC
LIMIT 5;

-- =============================================================================
-- 9. Summary: Phase 4 availability across ALL cached packs
--    (covers both v1 and v2 sessions for comparison)
-- =============================================================================
SELECT
    c.PACK_JSON:pack_version::STRING                                   AS PACK_VERSION,
    COUNT(*)                                                           AS TOTAL_SESSIONS,
    COUNT(CASE WHEN c.PACK_JSON:slices:phase4_thesis_verdict IS NOT NULL THEN 1 END)
                                                                       AS HAS_P4T_SLICE,
    COUNT(CASE WHEN c.PACK_JSON:slices:phase4_thesis_verdict:phase4_available::BOOLEAN = TRUE THEN 1 END)
                                                                       AS P4T_AVAILABLE_TRUE,
    COUNT(CASE WHEN c.PACK_JSON:slices:phase4_dossier_context:phase4_available::BOOLEAN = TRUE THEN 1 END)
                                                                       AS P4D_AVAILABLE_TRUE
FROM MIP.APP.SHADOW_EVIDENCE_PACK_CACHE c
GROUP BY c.PACK_JSON:pack_version::STRING
ORDER BY PACK_VERSION DESC;
