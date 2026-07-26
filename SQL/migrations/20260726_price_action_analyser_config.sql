-- Standalone Price Action Analyser configuration.
-- Existing values are preserved. New RAG/LLM switches are inserted disabled so
-- the first deployment cannot enable model-backed behavior as a side effect.
USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;
USE SCHEMA APP;

MERGE INTO MIP.APP.APP_CONFIG t
USING (
    SELECT column1 AS CONFIG_KEY, column2 AS CONFIG_VALUE, column3 AS DESCRIPTION
    FROM VALUES
      ('PRICE_ACTION_ANALYSER_ENABLED', 'true',
       'Enable the standalone manual Price Action Analyser feature.'),
      ('PRICE_ACTION_ANALYSER_PIVOT_LEFT', '2',
       'Daily bars required to the left of a deterministic swing pivot.'),
      ('PRICE_ACTION_ANALYSER_PIVOT_RIGHT', '2',
       'Daily bars required to the right of a deterministic swing pivot.'),
      ('PRICE_ACTION_ANALYSER_MAX_LOOKBACK_BARS', '250',
       'Maximum daily bars accepted by a standalone analysis request.'),
      ('PRICE_ACTION_ANALYSER_RAG_ENABLED', 'false',
       'Enable approved-card retrieval. New deployments start disabled.'),
      ('PRICE_ACTION_ANALYSER_RAG_MAX_CALLS', '3',
       'Maximum grouped knowledge retrieval calls per analysis.'),
      ('PRICE_ACTION_ANALYSER_RAG_MAX_RETRIES', '0',
       'Knowledge retrieval retry count is zero by design.'),
      ('PRICE_ACTION_ANALYSER_RAG_MAX_TOTAL_CARDS', '8',
       'Maximum unique knowledge cards across one analysis.'),
      ('PRICE_ACTION_ANALYSER_RAG_MAX_CARDS_PER_TOPIC', '2',
       'Maximum cards returned by each grouped topic retrieval.'),
      ('PRICE_ACTION_ANALYSER_RAG_MAX_CARD_CHARS', '900',
       'Maximum compact methodologist-view characters per card.'),
      ('PRICE_ACTION_ANALYSER_RAG_MAX_TOTAL_CHARS', '8000',
       'Maximum compact knowledge characters per analysis.'),
      ('PRICE_ACTION_ANALYSER_RAG_TIMEOUT_SECONDS', '10',
       'Knowledge retrieval timeout in seconds.'),
      ('PRICE_ACTION_ANALYSER_LLM_ENABLED', 'false',
       'Enable the single bounded methodologist LLM call. New deployments start disabled.'),
      ('PRICE_ACTION_ANALYSER_AUDIT_ENABLED', 'true',
       'Write standalone analysis results to PRICE_ACTION_ANALYSER_AUDIT.')
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
WHERE CONFIG_KEY LIKE 'PRICE_ACTION_ANALYSER_%'
ORDER BY CONFIG_KEY;
