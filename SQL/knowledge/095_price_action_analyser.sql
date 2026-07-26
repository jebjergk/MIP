-- Standalone Price Action Analyser persistence and bounded knowledge retrieval.
-- Advisory/manual only: no LPA, proposal, authority, decision, materialization,
-- LIVE_ACTIONS, LIVE_ORDERS, or broker integration.
USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;
USE SCHEMA KNOWLEDGE;

CREATE TABLE IF NOT EXISTS MIP.KNOWLEDGE.PRICE_ACTION_ANALYSER_AUDIT (
    ANALYSIS_ID                 VARCHAR(36) NOT NULL,
    CREATED_AT                  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    SYMBOL                      VARCHAR(32) NOT NULL,
    SIDE                        VARCHAR(16) NOT NULL,
    TIMEFRAME                   VARCHAR(16) NOT NULL,
    LOOKBACK_BARS               NUMBER,
    AS_OF_DATE                  DATE,
    RAG_ENABLED                 BOOLEAN,
    RAG_STATUS                  VARCHAR(40),
    RAG_CALL_COUNT              NUMBER,
    CARD_COUNT                  NUMBER,
    CARD_IDS                    VARIANT,
    TOTAL_RAG_CHARS             NUMBER,
    GEOMETRY_SUMMARY_JSON       VARIANT,
    SITUATION_MODEL_JSON        VARIANT,
    METHODOLOGIST_OUTPUT_JSON   VARIANT,
    VERDICT                     VARCHAR(40),
    CONFIDENCE                  FLOAT,
    ERROR_CLASS                 VARCHAR(200),
    ERROR_MESSAGE_TRUNC         VARCHAR(500),
    LATENCY_MS                  NUMBER,
    PRIMARY KEY (ANALYSIS_ID)
);

-- Keeps deployment safe if an earlier pre-release table was created with the
-- misspelled draft column. Runtime code must use only the correctly spelled name.
ALTER TABLE MIP.KNOWLEDGE.PRICE_ACTION_ANALYSER_AUDIT
    ADD COLUMN IF NOT EXISTS METHODOLOGIST_OUTPUT_JSON VARIANT;

COMMENT ON TABLE MIP.KNOWLEDGE.PRICE_ACTION_ANALYSER_AUDIT IS
    'Standalone manual Price Action Analyser audit. Advisory LONG analysis only and has no trading authority or execution effect.';

CREATE OR REPLACE PROCEDURE MIP.KNOWLEDGE.SP_SEARCH_PRICE_ACTION_KNOWLEDGE(
    QUERY_TEXT STRING,
    TOPIC      STRING,
    TOP_K      NUMBER,
    SIDE       STRING
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    v_topic          STRING;
    v_top_k          NUMBER;
    v_query          STRING;
    v_request        STRING;
    v_raw            VARIANT;
    v_hits           VARIANT;
    v_cards          VARIANT;
    v_count          NUMBER DEFAULT 0;
    v_total_chars    NUMBER DEFAULT 0;
    v_error          STRING;
BEGIN
    IF (UPPER(COALESCE(:SIDE, '')) <> 'LONG') THEN
        RETURN OBJECT_CONSTRUCT(
            'status', 'UNSUPPORTED_SIDE',
            'topic', UPPER(COALESCE(:TOPIC, '')),
            'result_count', 0,
            'total_chars', 0,
            'results', ARRAY_CONSTRUCT()
        );
    END IF;

    v_topic := UPPER(COALESCE(:TOPIC, ''));
    IF (v_topic NOT IN ('MARKET_STRUCTURE', 'PATTERN_RISK', 'LONG_DECISION')) THEN
        RETURN OBJECT_CONSTRUCT(
            'status', 'UNSUPPORTED_TOPIC',
            'topic', v_topic,
            'result_count', 0,
            'total_chars', 0,
            'results', ARRAY_CONSTRUCT()
        );
    END IF;

    -- Per-topic cap is deliberately two. The caller may make at most the three
    -- grouped topic calls above and applies the global eight-card/8,000-char cap.
    v_top_k := LEAST(GREATEST(COALESCE(:TOP_K, 2), 1), 2);
    v_query := LEFT(COALESCE(:QUERY_TEXT, ''), 2000);
    v_request := OBJECT_CONSTRUCT(
        'query', v_query,
        'columns', ARRAY_CONSTRUCT('CARD_ID'),
        'limit', 12
    )::STRING;

    v_raw := (
        SELECT PARSE_JSON(
            SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
                'MIP.KNOWLEDGE.LITERATURE_REVALIDATION_SEARCH_SERVICE',
                :v_request
            )
        )
    );
    v_hits := COALESCE(v_raw:results, ARRAY_CONSTRUCT());

    SELECT
        COALESCE(
            ARRAY_AGG(
                OBJECT_CONSTRUCT(
                    'card_id', x.CARD_ID,
                    'topic', :v_topic,
                    'concept', LEFT(COALESCE(x.CONCEPT_NAME, 'Price-action concept'), 160),
                    'methodologist_view', LEFT(
                        TRIM(
                            COALESCE(x.SETUP_DEFINITION, '') || ' ' ||
                            COALESCE(x.MARKET_CONTEXT, '')
                        ),
                        900
                    ),
                    'relevance_reason',
                        'Approved daily-compatible concept matched the supplied ' ||
                        LOWER(:v_topic) || ' chart situation.',
                    'applies_to', 'LONG_EVALUATION'
                )
            ) WITHIN GROUP (ORDER BY x.HIT_INDEX),
            ARRAY_CONSTRUCT()
        ),
        COUNT(*),
        COALESCE(SUM(LENGTH(LEFT(
            TRIM(COALESCE(x.SETUP_DEFINITION, '') || ' ' || COALESCE(x.MARKET_CONTEXT, '')),
            900
        ))), 0)
      INTO :v_cards, :v_count, :v_total_chars
      FROM (
          SELECT
              f.INDEX AS HIT_INDEX,
              c.CARD_ID,
              c.CONCEPT_NAME,
              c.SETUP_DEFINITION,
              c.MARKET_CONTEXT
          FROM TABLE(FLATTEN(INPUT => :v_hits)) f
          JOIN MIP.KNOWLEDGE.LITERATURE_RAG_DOCUMENT d
            ON d.CARD_ID = COALESCE(f.VALUE:CARD_ID::STRING, f.VALUE:card_id::STRING)
          JOIN MIP.KNOWLEDGE.LITERATURE_CONCEPT_CARD c
            ON c.CARD_ID = d.CARD_ID
          WHERE d.RAG_SCOPE = 'REVALIDATION'
            AND d.REVIEW_STATUS = 'APPROVED_FOR_REVALIDATION_RAG'
            AND d.EVIDENCE_READINESS_STATUS = 'READY_FOR_DAILY_AGENT_USE'
            AND d.MIP_USAGE_TIER IN ('AGENT_READY_NOW', 'AGENT_READY_WITH_CAUTION')
            AND d.DAILY_BAR_COMPATIBILITY IN (
                'DAILY_COMPATIBLE',
                'DAILY_COMPATIBLE_WITH_CAUTION'
            )
            AND COALESCE(c.CANONICAL_CANDIDATE, TRUE)
            AND UPPER(COALESCE(c.CARD_ROLE, '')) NOT IN (
                'SHORT_SETUP', 'SHORT_ENTRY', 'SHORT_MANAGEMENT', 'INTRADAY_SCALP'
            )
            AND NOT REGEXP_LIKE(
                LOWER(
                    COALESCE(c.CONCEPT_NAME, '') || ' ' ||
                    COALESCE(c.CONCEPT_TYPE, '') || ' ' ||
                    COALESCE(c.CARD_ROLE, '') || ' ' ||
                    COALESCE(c.SETUP_DEFINITION, '') || ' ' ||
                    COALESCE(c.MARKET_CONTEXT, '')
                ),
                '(^|[^a-z])(scalp|scalping|short entry|enter short|sell short|short-only)([^a-z]|$)'
            )
          QUALIFY ROW_NUMBER() OVER (ORDER BY f.INDEX) <= :v_top_k
      ) x;

    RETURN OBJECT_CONSTRUCT(
        'status', IFF(v_count > 0, 'USED', 'NO_RELEVANT_CARDS'),
        'topic', v_topic,
        'result_count', v_count,
        'total_chars', v_total_chars,
        'results', v_cards
    );
EXCEPTION
    WHEN OTHER THEN
        v_error := SQLERRM;
        RETURN OBJECT_CONSTRUCT(
            'status', 'UNAVAILABLE',
            'topic', v_topic,
            'result_count', 0,
            'total_chars', 0,
            'error', LEFT(v_error, 500),
            'results', ARRAY_CONSTRUCT()
        );
END;
$$;

COMMENT ON PROCEDURE MIP.KNOWLEDGE.SP_SEARCH_PRICE_ACTION_KNOWLEDGE(
    STRING, STRING, NUMBER, STRING
) IS
    'Bounded approved daily-compatible knowledge retrieval for the standalone LONG-only Price Action Analyser. Returns compact content without source metadata or raw quotes.';

-- Grants are intentionally limited to this feature's audit and retrieval API.
GRANT USAGE ON SCHEMA MIP.KNOWLEDGE TO ROLE MIP_UI_API_ROLE;
GRANT USAGE ON PROCEDURE MIP.KNOWLEDGE.SP_SEARCH_PRICE_ACTION_KNOWLEDGE(
    STRING, STRING, NUMBER, STRING
) TO ROLE MIP_UI_API_ROLE;
GRANT SELECT, INSERT ON TABLE MIP.KNOWLEDGE.PRICE_ACTION_ANALYSER_AUDIT
    TO ROLE MIP_UI_API_ROLE;
