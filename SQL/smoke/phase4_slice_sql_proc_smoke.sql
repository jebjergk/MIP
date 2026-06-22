-- ================================================================
-- phase4_slice_sql_proc_smoke.sql
-- Smoke test for the pure-SQL GET_PHASE4_DOSSIER_SLICE rewrite.
-- Verifies: allowlist/error paths, slice->payload-key aliasing,
-- null-payload preservation, and successful pack reads.
-- Self-cleaning (uses a synthetic SMOKE_SLICE_TEST cache row).
-- ================================================================
USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;
USE SCHEMA APP;

-- Seed a synthetic dossier pack (idempotent).
DELETE FROM MIP.APP.PROPOSAL_BOARD_DOSSIER_PACK_CACHE WHERE RUN_ID = 'SMOKE_SLICE_TEST';

INSERT INTO MIP.APP.PROPOSAL_BOARD_DOSSIER_PACK_CACHE
    (RUN_ID, DOSSIER_ID, SYMBOL, MARKET_TYPE, AS_OF_DATE, PACK_JSON, PACK_HASH, EXPIRES_AT)
SELECT 'SMOKE_SLICE_TEST', 999999, 'SMOKE', 'STOCK', CURRENT_DATE(),
       OBJECT_CONSTRUCT(
           'identity', OBJECT_CONSTRUCT('symbol', 'SMOKE', 'market_type', 'STOCK'),
           'levels', OBJECT_CONSTRUCT('nearest_support', 100, 'nearest_resistance', 110),
           'recent_price_action_summary', 'orderly pullback into support',
           'policy', OBJECT_CONSTRUCT('short_live_enabled', FALSE, 'fx_live_enabled', FALSE),
           'market_structure_map', OBJECT_CONSTRUCT('primary_structure', 'UPTREND')
       ),
       'smokehash', DATEADD('hour', 1, CURRENT_TIMESTAMP());

-- 1. Success: CHAIR/identity -> payload identity object.
CALL MIP.APP.GET_PHASE4_DOSSIER_SLICE('SMOKE_SLICE_TEST', 999999, 'CHAIR', 'identity');

-- 2. Alias: LEVEL_PRICE_ACTION/zone_context -> payload from 'levels'.
CALL MIP.APP.GET_PHASE4_DOSSIER_SLICE('SMOKE_SLICE_TEST', 999999, 'LEVEL_PRICE_ACTION', 'zone_context');

-- 3. Alias: THESIS/recent_price_action -> payload from 'recent_price_action_summary'.
CALL MIP.APP.GET_PHASE4_DOSSIER_SLICE('SMOKE_SLICE_TEST', 999999, 'THESIS', 'recent_price_action');

-- 4. Alias: RISK_EXECUTION/policy_flags -> payload from 'policy'.
CALL MIP.APP.GET_PHASE4_DOSSIER_SLICE('SMOKE_SLICE_TEST', 999999, 'RISK_EXECUTION', 'policy_flags');

-- 5. Null payload preserved: CHAIR/structural_timeline_bars (key absent in pack).
CALL MIP.APP.GET_PHASE4_DOSSIER_SLICE('SMOKE_SLICE_TEST', 999999, 'CHAIR', 'structural_timeline_bars');

-- 6. Error: ROLE_NOT_ALLOWED.
CALL MIP.APP.GET_PHASE4_DOSSIER_SLICE('SMOKE_SLICE_TEST', 999999, 'NOT_A_ROLE', 'identity');

-- 7. Error: SLICE_NOT_IN_CATALOG.
CALL MIP.APP.GET_PHASE4_DOSSIER_SLICE('SMOKE_SLICE_TEST', 999999, 'CHAIR', 'banana');

-- 8. Error: SLICE_NOT_ALLOWED_FOR_ROLE (HISTORICAL_EVIDENCE cannot read 'price').
CALL MIP.APP.GET_PHASE4_DOSSIER_SLICE('SMOKE_SLICE_TEST', 999999, 'HISTORICAL_EVIDENCE', 'price');

-- 9. Error: PACK_NOT_FOUND_OR_EXPIRED.
CALL MIP.APP.GET_PHASE4_DOSSIER_SLICE('NO_SUCH_RUN', 1, 'CHAIR', 'identity');

-- Cleanup.
DELETE FROM MIP.APP.PROPOSAL_BOARD_DOSSIER_PACK_CACHE WHERE RUN_ID = 'SMOKE_SLICE_TEST';
