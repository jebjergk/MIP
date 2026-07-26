-- Cost-safe, advisory-only LPA price-action RAG configuration.
-- Master switch is deliberately disabled. Runtime code also enforces stricter
-- hard caps and treats any APP_CONFIG read failure as DISABLED.
USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;
USE SCHEMA APP;

MERGE INTO MIP.APP.APP_CONFIG t
USING (
    SELECT column1 AS CONFIG_KEY, column2 AS CONFIG_VALUE, column3 AS DESCRIPTION
    FROM VALUES
      ('LPA_PRICE_ACTION_RAG_ENABLED', 'false', 'Master switch for advisory long-only LPA literature support. Default disabled.'),
      ('LPA_PRICE_ACTION_RAG_LONG_ONLY', 'true', 'Restrict LPA literature support to LONG structural-entry revalidation.'),
      ('LPA_PRICE_ACTION_RAG_MAX_CARDS', '3', 'Requested maximum approved cards per LPA session. Runtime hard cap is 5.'),
      ('LPA_PRICE_ACTION_RAG_MAX_QUERY_RETRIES', '0', 'Retrieval retries. Runtime hard cap is zero.'),
      ('LPA_PRICE_ACTION_RAG_MAX_SNIPPET_CHARS_PER_CARD', '900', 'Maximum approved summary characters per card. Runtime hard cap is 900.'),
      ('LPA_PRICE_ACTION_RAG_MAX_TOTAL_CHARS', '3000', 'Maximum total literature-support characters. Runtime hard cap is 3000.'),
      ('LPA_PRICE_ACTION_RAG_FAIL_OPEN', 'true', 'On retrieval failure continue the committee without literature support.'),
      ('LPA_PRICE_ACTION_RAG_AUDIT_ENABLED', 'true', 'Write bounded LPA retrieval metadata and compact approved summaries to the dedicated audit table.')
) s
ON t.CONFIG_KEY = s.CONFIG_KEY
WHEN MATCHED THEN UPDATE SET
    t.DESCRIPTION = s.DESCRIPTION,
    t.UPDATED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (CONFIG_KEY, CONFIG_VALUE, DESCRIPTION, UPDATED_AT)
VALUES (s.CONFIG_KEY, s.CONFIG_VALUE, s.DESCRIPTION, CURRENT_TIMESTAMP());

-- Never enable as a side effect of deployment.
UPDATE MIP.APP.APP_CONFIG
   SET CONFIG_VALUE = 'false',
       UPDATED_AT = CURRENT_TIMESTAMP()
 WHERE CONFIG_KEY = 'LPA_PRICE_ACTION_RAG_ENABLED';

SELECT CONFIG_KEY, CONFIG_VALUE
  FROM MIP.APP.APP_CONFIG
 WHERE CONFIG_KEY LIKE 'LPA_PRICE_ACTION_RAG_%'
 ORDER BY CONFIG_KEY;
