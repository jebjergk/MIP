-- Phase 2 align: politician trade disclosure semantics, TRADE_DATE, optional live flag.
-- If TRADE_DATE already exists, comment out the ALTER below and re-run the rest.

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;

-- Trade date vs filed/disclosure date (tier-1 structured fields).
ALTER TABLE MIP.APP.PUBLIC_DISCLOSURE_TRANSACTION ADD COLUMN TRADE_DATE DATE;

COMMENT ON TABLE MIP.APP.PUBLIC_DISCLOSURE_TRANSACTION IS
  'Curated U.S. politician trade disclosure rows (STOCK Act-style / Capitol Trades-class ingest) mapped to ASSET_SYMBOL for Committee 2.0 context only — not generic public disclosure.';

MERGE INTO MIP.APP.APP_CONFIG t
USING (
    SELECT 'COMMITTEE2_LIVE_POLITICIAN_DISCLOSURE_ENABLED' AS CONFIG_KEY,
           'false' AS CONFIG_VALUE,
           'When true and MIP_POLITICIAN_DISCLOSURE_LIVE_URL_TEMPLATE is set, hearing may attach exhibit_live_politician_disclosure_context (silent failure).' AS DESCRIPTION
) s
ON t.CONFIG_KEY = s.CONFIG_KEY
WHEN NOT MATCHED THEN INSERT (CONFIG_KEY, CONFIG_VALUE, DESCRIPTION, UPDATED_AT)
VALUES (s.CONFIG_KEY, s.CONFIG_VALUE, s.DESCRIPTION, CURRENT_TIMESTAMP());

SELECT CONFIG_KEY, CONFIG_VALUE FROM MIP.APP.APP_CONFIG
 WHERE CONFIG_KEY IN ('COMMITTEE2_CONTEXT_DISCLOSURE_ENABLED', 'COMMITTEE2_LIVE_POLITICIAN_DISCLOSURE_ENABLED')
 ORDER BY CONFIG_KEY;
