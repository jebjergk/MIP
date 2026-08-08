-- V1.0 methodology: correct intraday adviser corpus metadata (bull-continuation mis-tagged BEARISH_CONTEXT_ONLY).
-- Does not change retrieval_text / Brooks prose.

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;
USE SCHEMA KNOWLEDGE;

-- Card metadata corrections (execution_relevance + adviser_class only).
-- Audit: 15 candidates; 9 updated here (content clearly bull continuation / long setup).
-- Not updated (legitimate bearish-context or exit-risk framing): 028fc133, 2b31d50, fe09d009, 69417e26.

CREATE OR REPLACE TEMPORARY TABLE _ADVISER_METADATA_FIX AS
SELECT * FROM VALUES
  ('539254f0fba1443b', 'LONG_ENTRY', 'ADVISER_CORE'),
  ('d72d63a18ef3a5c9', 'LONG_CONTEXT', 'ADVISER_CORE'),
  ('ad6137edad7dc491', 'LONG_CONTEXT', 'ADVISER_CORE'),
  ('8ab32794b8540a70', 'LONG_CONTEXT', 'ADVISER_CORE'),
  ('987d655730d7e269', 'LONG_ENTRY', 'ADVISER_CORE'),
  ('8615bf2e18c61977', 'LONG_ENTRY', 'ADVISER_CORE'),
  ('97519cd665557c73', 'LONG_CONTEXT', 'ADVISER_CORE'),
  ('1dedfb1beb969513', 'LONG_CONTEXT', 'ADVISER_CORE'),
  ('d81c9a400ad113ee', 'LONG_CONTEXT', 'ADVISER_CORE')
AS t(CARD_ID, EXECUTION_RELEVANCE, ADVISER_CLASS);

UPDATE MIP.KNOWLEDGE.LITERATURE_RAG_DOCUMENT d
SET RAW_CARD = OBJECT_INSERT(
    OBJECT_INSERT(
        OBJECT_INSERT(
            d.RAW_CARD,
            'execution_relevance',
            f.EXECUTION_RELEVANCE,
            TRUE
        ),
        'adviser_class',
        f.ADVISER_CLASS,
        TRUE
    ),
    'metadata',
    OBJECT_INSERT(
        OBJECT_INSERT(
            COALESCE(d.RAW_CARD:metadata, OBJECT_CONSTRUCT()),
            'execution_relevance',
            f.EXECUTION_RELEVANCE,
            TRUE
        ),
        'adviser_class',
        f.ADVISER_CLASS,
        TRUE
    ),
    TRUE
)
FROM _ADVISER_METADATA_FIX f
WHERE d.RAG_SCOPE = 'INTRADAY_ADVISER'
  AND d.CARD_ID = f.CARD_ID;

SELECT d.CARD_ID,
       d.RAW_CARD:concept_name::STRING AS CONCEPT,
       d.RAW_CARD:execution_relevance::STRING AS EXECUTION_RELEVANCE,
       d.RAW_CARD:adviser_class::STRING AS ADVISER_CLASS
FROM MIP.KNOWLEDGE.LITERATURE_RAG_DOCUMENT d
JOIN _ADVISER_METADATA_FIX f ON f.CARD_ID = d.CARD_ID
ORDER BY 1;
