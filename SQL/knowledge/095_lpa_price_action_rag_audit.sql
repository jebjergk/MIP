-- Advisory-only, long-only LPA literature retrieval and audit objects.
-- Reuses the existing static Cortex Search service. This file does not refresh
-- the corpus, resume indexing, rebuild embeddings, or touch trading authority.
USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;
USE SCHEMA KNOWLEDGE;

CREATE TABLE IF NOT EXISTS MIP.KNOWLEDGE.LPA_PRICE_ACTION_RAG_AUDIT (
    AUDIT_ID                    VARCHAR(36)   NOT NULL,
    CREATED_AT                  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    COMPLETED_AT                TIMESTAMP_NTZ,
    ACTION_ID                   VARCHAR(64),
    SYMBOL                      VARCHAR(32),
    SIDE                        VARCHAR(16),
    PROPOSAL_ID                 NUMBER,
    HEARING_ID                  VARCHAR(64),
    SHADOW_SESSION_ID           VARCHAR(64)   NOT NULL,
    EVIDENCE_PACK_HASH          VARCHAR(64),
    RAG_ENABLED                 BOOLEAN,
    RAG_STATUS                  VARCHAR(40),
    QUERY_TEXT                  VARCHAR(4000),
    TOP_K_REQUESTED             NUMBER,
    CARD_IDS_RETURNED           VARIANT,
    CARD_COUNT                  NUMBER,
    TOTAL_SNIPPET_CHARS         NUMBER,
    APPROX_PROMPT_CHARS_ADDED   NUMBER,
    RETRIEVAL_LATENCY_MS        NUMBER,
    RETRY_COUNT                 NUMBER        DEFAULT 0,
    ERROR_CLASS                 VARCHAR(200),
    ERROR_MESSAGE_TRUNC         VARCHAR(500),
    METHODOLOGIST_EFFECT        VARCHAR(40)   DEFAULT 'NO_MATERIAL_EFFECT',
    CHAIR_FINAL_VERDICT         VARCHAR(40),
    CHAIR_CONFIDENCE            FLOAT,
    RETRIEVAL_ID                VARCHAR(64),
    LITERATURE_SUPPORT_JSON     VARIANT,
    PRIMARY KEY (SHADOW_SESSION_ID)
);

COMMENT ON TABLE MIP.KNOWLEDGE.LPA_PRICE_ACTION_RAG_AUDIT IS
    'Cost and reasoning trace for advisory-only LONG LPA literature support. One compact row per shadow session. Never grants authority or creates trades.';

-- Three-argument overload dedicated to the LPA long-only path. The existing
-- two-argument pilot procedure remains unchanged for offline tooling.
CREATE OR REPLACE PROCEDURE MIP.KNOWLEDGE.SP_SEARCH_REVALIDATION_LITERATURE(
    QUERY_TEXT STRING,
    TOP_K      NUMBER,
    DIRECTION  STRING
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    v_top_k          NUMBER;
    v_retrieval_id   STRING DEFAULT UUID_STRING();
    v_request        STRING;
    v_raw            VARIANT;
    v_hits           VARIANT;
    v_cards          VARIANT;
    v_card_ids       VARIANT;
    v_count          NUMBER DEFAULT 0;
    v_error          STRING;
BEGIN
    v_top_k := LEAST(GREATEST(COALESCE(:TOP_K, 3), 1), 5);

    IF (UPPER(COALESCE(:DIRECTION, '')) <> 'LONG') THEN
        RETURN OBJECT_CONSTRUCT(
            'retrieval_id', v_retrieval_id,
            'status', 'SKIPPED_SIDE_NOT_LONG',
            'result_count', 0,
            'results', ARRAY_CONSTRUCT()
        );
    END IF;

    v_request := OBJECT_CONSTRUCT(
        'query', :QUERY_TEXT,
        'columns', ARRAY_CONSTRUCT('CARD_ID'),
        'limit', v_top_k
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
                    'card_id', d.CARD_ID,
                    'title', d.CONCEPT_NAME,
                    'methodologist_view',
                        LEFT(
                            SPLIT_PART(d.DISPLAY_TEXT, '\nSource:', 1),
                            900
                        ),
                    'relevance_reason',
                        'Approved long-compatible ' || COALESCE(d.CONCEPT_FAMILY, 'price-action') ||
                        ' concept retrieved for this LONG revalidation; use only when observed MIP evidence matches.',
                    'supports', 'MIXED',
                    'snippet',
                        LEFT(
                            SPLIT_PART(d.DISPLAY_TEXT, '\nSource:', 1),
                            900
                        )
                )
            ) WITHIN GROUP (ORDER BY f.INDEX),
            ARRAY_CONSTRUCT()
        ),
        COALESCE(ARRAY_AGG(d.CARD_ID) WITHIN GROUP (ORDER BY f.INDEX), ARRAY_CONSTRUCT()),
        COUNT(*)
      INTO :v_cards, :v_card_ids, :v_count
      FROM TABLE(FLATTEN(INPUT => :v_hits)) f
      JOIN MIP.KNOWLEDGE.LITERATURE_RAG_DOCUMENT d
        ON d.CARD_ID = COALESCE(f.VALUE:CARD_ID::STRING, f.VALUE:card_id::STRING)
      JOIN MIP.KNOWLEDGE.LITERATURE_CONCEPT_CARD c
        ON c.CARD_ID = d.CARD_ID
     WHERE d.RAG_SCOPE = 'REVALIDATION'
       AND d.REVIEW_STATUS = 'APPROVED_FOR_REVALIDATION_RAG'
       AND d.EVIDENCE_READINESS_STATUS = 'READY_FOR_DAILY_AGENT_USE'
       AND d.MIP_USAGE_TIER IN ('AGENT_READY_NOW', 'AGENT_READY_WITH_CAUTION')
       AND d.DAILY_BAR_COMPATIBILITY IN ('DAILY_COMPATIBLE', 'DAILY_COMPATIBLE_WITH_CAUTION')
       AND UPPER(COALESCE(c.CARD_ROLE, '')) NOT IN (
           'SHORT_SETUP', 'SHORT_ENTRY', 'SHORT_MANAGEMENT', 'INTRADAY_SCALP'
       )
       AND NOT REGEXP_LIKE(
           LOWER(
               COALESCE(d.CONCEPT_NAME, '') || ' ' ||
               COALESCE(d.DISPLAY_TEXT, '') || ' ' ||
               COALESCE(d.TRADE_MANAGEMENT_IMPLICATIONS::STRING, '')
           ),
           'failed breakout short|shorting below|use failed breakout short|short entry|enter short|sell short'
       );

    INSERT INTO MIP.KNOWLEDGE.LITERATURE_RAG_RETRIEVAL_AUDIT
        (RETRIEVAL_ID, RAG_SCOPE, QUERY_TEXT, FILTER_JSON, TOP_K, RESULT_COUNT, RESULTS_JSON, NOTES)
    SELECT
        :v_retrieval_id,
        'LPA_REVALIDATION',
        :QUERY_TEXT,
        OBJECT_CONSTRUCT(
            'direction_scope', 'LONG_ONLY',
            'approval_status', 'APPROVED_FOR_REVALIDATION_RAG',
            'readiness', 'READY_FOR_DAILY_AGENT_USE',
            'fallback', 'NONE'
        ),
        :v_top_k,
        :v_count,
        :v_card_ids,
        'cortex_search_lpa_long_sanitized';

    RETURN OBJECT_CONSTRUCT(
        'retrieval_id', v_retrieval_id,
        'status', IFF(v_count > 0, 'USED', 'NO_RELEVANT_LONG_CARDS'),
        'top_k', v_top_k,
        'result_count', v_count,
        'results', v_cards
    );

EXCEPTION
    WHEN OTHER THEN
        v_error := SQLERRM;
        INSERT INTO MIP.KNOWLEDGE.LITERATURE_RAG_RETRIEVAL_AUDIT
            (RETRIEVAL_ID, RAG_SCOPE, QUERY_TEXT, FILTER_JSON, TOP_K, RESULT_COUNT, RESULTS_JSON, NOTES)
        SELECT
            :v_retrieval_id,
            'LPA_REVALIDATION',
            :QUERY_TEXT,
            OBJECT_CONSTRUCT('direction_scope', 'LONG_ONLY', 'fallback', 'NONE'),
            :v_top_k,
            0,
            ARRAY_CONSTRUCT(),
            'cortex_search_lpa_error: ' || LEFT(:v_error, 300);
        RETURN OBJECT_CONSTRUCT(
            'retrieval_id', v_retrieval_id,
            'status', 'UNAVAILABLE',
            'error', LEFT(v_error, 500),
            'result_count', 0,
            'results', ARRAY_CONSTRUCT()
        );
END;
$$;

COMMENT ON PROCEDURE MIP.KNOWLEDGE.SP_SEARCH_REVALIDATION_LITERATURE(STRING, NUMBER, STRING) IS
    'One-call, no-fallback, sanitized approved-card retrieval for LONG LPA revalidation. Advisory only.';

GRANT USAGE ON SCHEMA MIP.KNOWLEDGE TO ROLE MIP_UI_API_ROLE;
GRANT USAGE ON PROCEDURE MIP.KNOWLEDGE.SP_SEARCH_REVALIDATION_LITERATURE(STRING, NUMBER, STRING)
    TO ROLE MIP_UI_API_ROLE;
GRANT SELECT, INSERT, UPDATE ON TABLE MIP.KNOWLEDGE.LPA_PRICE_ACTION_RAG_AUDIT
    TO ROLE MIP_UI_API_ROLE;
GRANT SELECT, INSERT, UPDATE ON TABLE MIP.KNOWLEDGE.LPA_PRICE_ACTION_RAG_AUDIT
    TO ROLE MIP_ADMIN_ROLE;
