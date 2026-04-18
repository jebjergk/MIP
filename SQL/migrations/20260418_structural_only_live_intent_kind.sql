-- 20260418_structural_only_live_intent_kind.sql
-- LIVE_INTENT_KIND on LIVE_ACTIONS + deployment flags for structural-only live trading.

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;

ALTER TABLE MIP.LIVE.LIVE_ACTIONS
    ADD COLUMN IF NOT EXISTS LIVE_INTENT_KIND VARCHAR(30);

COMMENT ON COLUMN MIP.LIVE.LIVE_ACTIONS.LIVE_INTENT_KIND IS
    'STRUCTURAL | LEGACY_PATTERN | OPERATOR_EXIT | UNKNOWN — canonical live intent classification.';

------------------------------------------------------------------------
-- APP_CONFIG: structural-only mode + disable legacy overview import
------------------------------------------------------------------------
MERGE INTO MIP.APP.APP_CONFIG t
USING (
    SELECT 'LIVE_STRUCTURAL_ONLY' AS CONFIG_KEY,
           'true' AS CONFIG_VALUE,
           'When true, legacy ORDER_PROPOSALS import and legacy multi-agent committee are forbidden for live trading; only structural + operator exits are operational.' AS DESCRIPTION
    UNION ALL
    SELECT 'LIVE_AUTO_IMPORT_PROPOSALS_ON_OVERVIEW',
           'false',
           'When true, overview may auto-import from ORDER_PROPOSALS (deprecated). Keep false under LIVE_STRUCTURAL_ONLY.'
) s
ON t.CONFIG_KEY = s.CONFIG_KEY
WHEN NOT MATCHED THEN INSERT (CONFIG_KEY, CONFIG_VALUE, DESCRIPTION, UPDATED_AT)
VALUES (s.CONFIG_KEY, s.CONFIG_VALUE, s.DESCRIPTION, CURRENT_TIMESTAMP());

------------------------------------------------------------------------
-- Backfill LIVE_INTENT_KIND (order: operator exit, legacy import, structural, default legacy)
------------------------------------------------------------------------
UPDATE MIP.LIVE.LIVE_ACTIONS
SET LIVE_INTENT_KIND = 'OPERATOR_EXIT',
    UPDATED_AT = CURRENT_TIMESTAMP()
WHERE LIVE_INTENT_KIND IS NULL
  AND TRY_PARSE_JSON(PARAM_SNAPSHOT):source::STRING = 'BROKER_POSITION_EXIT';

UPDATE MIP.LIVE.LIVE_ACTIONS
SET LIVE_INTENT_KIND = 'LEGACY_PATTERN',
    UPDATED_AT = CURRENT_TIMESTAMP()
WHERE LIVE_INTENT_KIND IS NULL
  AND TRY_PARSE_JSON(PARAM_SNAPSHOT):source::STRING = 'ORDER_PROPOSALS';

UPDATE MIP.LIVE.LIVE_ACTIONS
SET LIVE_INTENT_KIND = 'STRUCTURAL',
    UPDATED_AT = CURRENT_TIMESTAMP()
WHERE LIVE_INTENT_KIND IS NULL
  AND (
      SETUP_EVENT_ID IS NOT NULL
      OR (SETUP_FAMILY IS NOT NULL AND TRIM(SETUP_FAMILY) <> '')
      OR TRY_PARSE_JSON(PARAM_SNAPSHOT):structural_source::BOOLEAN = TRUE
  );

UPDATE MIP.LIVE.LIVE_ACTIONS
SET LIVE_INTENT_KIND = 'LEGACY_PATTERN',
    UPDATED_AT = CURRENT_TIMESTAMP()
WHERE LIVE_INTENT_KIND IS NULL;

------------------------------------------------------------------------
-- Verification
------------------------------------------------------------------------
SELECT LIVE_INTENT_KIND, COUNT(*) AS CNT
FROM MIP.LIVE.LIVE_ACTIONS
GROUP BY LIVE_INTENT_KIND
ORDER BY 1;

SELECT CONFIG_KEY, CONFIG_VALUE
FROM MIP.APP.APP_CONFIG
WHERE CONFIG_KEY IN ('LIVE_STRUCTURAL_ONLY', 'LIVE_AUTO_IMPORT_PROPOSALS_ON_OVERVIEW');
