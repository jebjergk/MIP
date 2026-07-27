-- PAA proposal-panel pre-screen configuration.
-- Disabled on insert; enable explicitly after smoke test passes.
USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;
USE SCHEMA APP;

MERGE INTO MIP.APP.APP_CONFIG t
USING (
    SELECT column1 AS CONFIG_KEY, column2 AS CONFIG_VALUE, column3 AS DESCRIPTION
    FROM VALUES
      ('PROPOSAL_BOARD_PAA_PRESCREEN_ENABLED', 'false',
       'Use Price Action Analyser ranking for proposal-panel candidate selection. Enable after smoke.'),
      ('PROPOSAL_BOARD_PAA_TOP_N', '30',
       'Top-N PAA-ranked STOCK symbols sent to the proposal panel.'),
      ('PROPOSAL_BOARD_PAA_MAX_SYMBOLS', '80',
       'Hard cap on STOCK symbols scanned by PAA per board run.'),
      ('PROPOSAL_BOARD_PAA_MAX_LLM_CALLS', '80',
       'Max projected PAA methodologist LLM calls per board run (one per symbol).'),
      ('PROPOSAL_BOARD_PAA_MAX_USD_ESTIMATE', '3.00',
       'Max projected PAA USD estimate per board run before agent fan-out.'),
      ('PROPOSAL_BOARD_PAA_LOOKBACK_BARS', '120',
       'Daily bar lookback for PAA pre-screen scans.')
) s
ON t.CONFIG_KEY = s.CONFIG_KEY
WHEN MATCHED THEN UPDATE SET
    t.DESCRIPTION = s.DESCRIPTION
WHEN NOT MATCHED THEN INSERT (
    CONFIG_KEY, CONFIG_VALUE, DESCRIPTION, UPDATED_AT
) VALUES (
    s.CONFIG_KEY, s.CONFIG_VALUE, s.DESCRIPTION, CURRENT_TIMESTAMP()
);

SELECT CONFIG_KEY, CONFIG_VALUE
FROM MIP.APP.APP_CONFIG
WHERE CONFIG_KEY LIKE 'PROPOSAL_BOARD_PAA_%'
ORDER BY CONFIG_KEY;
