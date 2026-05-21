-- 070_literature_rag_retrieval.sql
-- Purpose: Retrieval audit table, retrieval helper procedure, and keyword fallback view.
--
-- ADVISORY NOTICE:
--   These objects support a retrieval pilot for the future agentic revalidation
--   committee. They are NOT wired into the deterministic revalidation board,
--   LPA, any agent runtime, proposal generation, FastAPI, React, or IBKR.
--   The retrieval helper procedure is callable later by the agentic revalidation
--   committee, but is NOT called from any current runtime path.
--   Literature is advisory only and cannot become a trade signal.

use role MIP_ADMIN_ROLE;
use database MIP;
use schema MIP.KNOWLEDGE;

-- =============================================================================
-- Table: LITERATURE_RAG_RETRIEVAL_AUDIT
--   Records every retrieval call (manual tests, future agent retrieval packs).
--   Does not affect any decision.
-- =============================================================================

create table if not exists MIP.KNOWLEDGE.LITERATURE_RAG_RETRIEVAL_AUDIT (
    RETRIEVAL_ID    string        not null,
    RETRIEVED_AT    timestamp_ntz default CURRENT_TIMESTAMP(),
    RAG_SCOPE       string,
    QUERY_TEXT      string,
    FILTER_JSON     variant,
    TOP_K           number,
    RESULT_COUNT    number,
    RESULTS_JSON    variant,
    NOTES           string
);

comment on table MIP.KNOWLEDGE.LITERATURE_RAG_RETRIEVAL_AUDIT is
    'Audit log of literature RAG retrieval calls. Each row records one search and its returned card_ids/scores. ADVISORY ONLY. Does not affect any agent decision or runtime path.';

-- =============================================================================
-- View: V_LITERATURE_REVALIDATION_RAG_KEYWORD_SEARCH
--   Parameterless fallback view used when Cortex Search is unavailable.
--   Caller adds WHERE LOWER(SEARCH_TEXT) LIKE '%...%'.
-- =============================================================================

create or replace view MIP.KNOWLEDGE.V_LITERATURE_REVALIDATION_RAG_KEYWORD_SEARCH
    comment = 'ADVISORY ONLY. Deterministic keyword fallback over approved revalidation RAG corpus. Used when Cortex Search privileges are unavailable. Caller adds WHERE LOWER(SEARCH_TEXT) LIKE pattern.'
as
select
    RAG_DOCUMENT_ID,
    CARD_ID,
    CONCEPT_NAME,
    CONCEPT_FAMILY,
    MIP_USAGE_TIER,
    DAILY_BAR_COMPATIBILITY,
    SOURCE_BOOK,
    PAGE_START,
    PAGE_END,
    SEARCH_TEXT,
    DISPLAY_TEXT
from MIP.KNOWLEDGE.V_LITERATURE_REVALIDATION_RAG_CORPUS
order by CONCEPT_FAMILY, CONCEPT_NAME;

-- =============================================================================
-- Procedure: SP_SEARCH_REVALIDATION_LITERATURE
--   Calls Cortex Search service, returns top matches, writes one audit row.
--   Returns VARIANT array of result objects.
--   Clamps TOP_K to [1, 10]. Defaults to 5 if NULL or <=0.
--
-- The procedure invokes the Cortex Search service via SNOWFLAKE.CORTEX.SEARCH_PREVIEW
-- using the service-qualified identifier. The returned JSON is unpacked into result
-- objects {card_id, concept_name, concept_family, source_book, page_start, page_end,
-- display_text, score} and inserted into LITERATURE_RAG_RETRIEVAL_AUDIT.
-- =============================================================================

create or replace procedure MIP.KNOWLEDGE.SP_SEARCH_REVALIDATION_LITERATURE(
    QUERY_TEXT string,
    TOP_K      number
)
returns variant
language sql
as
$$
declare
    v_top_k         number;
    v_retrieval_id  string;
    v_request       string;
    v_raw           variant;
    v_results       variant;
    v_count         number;
begin
    v_top_k := COALESCE(:TOP_K, 5);
    if (v_top_k <= 0) then v_top_k := 5; end if;
    if (v_top_k > 10) then v_top_k := 10; end if;

    v_retrieval_id := UUID_STRING();

    v_request := OBJECT_CONSTRUCT(
        'query',   :QUERY_TEXT,
        'columns', ARRAY_CONSTRUCT(
                       'CARD_ID', 'CONCEPT_NAME', 'CONCEPT_FAMILY',
                       'SOURCE_BOOK', 'PAGE_START', 'PAGE_END',
                       'DISPLAY_TEXT'
                   ),
        'limit',   v_top_k
    )::string;

    v_raw := (
        select PARSE_JSON(
            SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
                'MIP.KNOWLEDGE.LITERATURE_REVALIDATION_SEARCH_SERVICE',
                :v_request
            )
        )
    );

    v_results := v_raw:results;
    v_count   := COALESCE(ARRAY_SIZE(v_results), 0);

    insert into MIP.KNOWLEDGE.LITERATURE_RAG_RETRIEVAL_AUDIT
        (RETRIEVAL_ID, RAG_SCOPE, QUERY_TEXT, FILTER_JSON, TOP_K, RESULT_COUNT, RESULTS_JSON, NOTES)
    select
        :v_retrieval_id,
        'REVALIDATION',
        :QUERY_TEXT,
        null,
        :v_top_k,
        :v_count,
        :v_results,
        'cortex_search';

    return OBJECT_CONSTRUCT(
        'retrieval_id', v_retrieval_id,
        'query_text',   :QUERY_TEXT,
        'top_k',        v_top_k,
        'result_count', v_count,
        'results',      v_results
    );

exception
    when other then
        insert into MIP.KNOWLEDGE.LITERATURE_RAG_RETRIEVAL_AUDIT
            (RETRIEVAL_ID, RAG_SCOPE, QUERY_TEXT, FILTER_JSON, TOP_K, RESULT_COUNT, RESULTS_JSON, NOTES)
        select
            :v_retrieval_id,
            'REVALIDATION',
            :QUERY_TEXT,
            null,
            :v_top_k,
            0,
            null,
            'cortex_search_error: ' || SQLERRM;
        return OBJECT_CONSTRUCT(
            'retrieval_id', v_retrieval_id,
            'error',        SQLERRM,
            'query_text',   :QUERY_TEXT,
            'top_k',        v_top_k,
            'result_count', 0
        );
end;
$$
;

comment on procedure MIP.KNOWLEDGE.SP_SEARCH_REVALIDATION_LITERATURE(string, number) is
    'ADVISORY ONLY. Retrieval helper for the future agentic revalidation committee. Calls Cortex Search over approved revalidation literature. Writes one row to LITERATURE_RAG_RETRIEVAL_AUDIT. NOT called from any current runtime path.';
