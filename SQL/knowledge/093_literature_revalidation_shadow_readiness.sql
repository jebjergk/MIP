-- 093_literature_revalidation_shadow_readiness.sql
-- Purpose: Diagnostic read-only view showing approved revalidation RAG cards
--          alongside shadow board session availability and Phase 4 dossier availability
--          for any proposal that matches the card's concept family.
--
-- This is an AUDIT/DIAGNOSTIC view only. It does NOT affect any runtime path.
-- Run with:
--   cursorfiles\.venv\Scripts\python.exe cursorfiles/query_snowflake.py -f MIP/SQL/knowledge/093_literature_revalidation_shadow_readiness.sql
--
-- Answers the readiness question:
--   - Which approved revalidation RAG cards exist?
--   - Are there active COMMITTEE_HEARING rows with a matching concept family?
--   - Do those hearings have a shadow board session?
--   - Do the linked proposals have a Phase 4 dossier snapshot?
--   - Is a PROPOSAL_BOARD_THESIS_VERDICT available?

use role MIP_ADMIN_ROLE;
use database MIP;

-- =============================================================================
-- View: KNOWLEDGE.V_LITERATURE_REVALIDATION_SHADOW_READINESS
--   Cross-reference between approved RAG cards and active committee hearings.
--   Concept-family matching is approximate (LIKE-based); exact lineage
--   requires BOARD_DOSSIER_ID linkage added in the Phase 4 evidence pack upgrade.
-- =============================================================================

create or replace view MIP.KNOWLEDGE.V_LITERATURE_REVALIDATION_SHADOW_READINESS
    comment = 'DIAGNOSTIC ONLY. Cross-reference approved revalidation RAG cards vs active shadow board sessions and Phase 4 dossier availability. Advisory only. Does not affect runtime.'
as
with approved_rag as (
    select
        r.CARD_ID,
        r.CONCEPT_NAME,
        r.CONCEPT_FAMILY,
        r.SOURCE_BOOK,
        r.PAGE_START,
        r.PAGE_END,
        r.MIP_USAGE_TIER,
        r.REVIEW_STATUS,
        r.REVIEWED_AT
    from MIP.KNOWLEDGE.V_LITERATURE_APPROVED_REVALIDATION_RAG r
),
shadow_sessions as (
    select
        s.SESSION_ID,
        s.HEARING_ID,
        s.SHADOW_STANCE,
        s.SHADOW_CONFIDENCE,
        s.STATUS              as SHADOW_STATUS,
        s.STAGE_REACHED,
        s.CREATED_AT          as SHADOW_CREATED_AT,
        h.PROPOSAL_ID,
        p.SYMBOL              as PROPOSAL_SYMBOL,
        p.DIRECTION           as PROPOSAL_DIRECTION,
        p.SETUP_FAMILY        as PROPOSAL_SETUP_FAMILY,
        p.BOARD_DOSSIER_ID,
        p.BOARD_RUN_ID
    from MIP.APP.SHADOW_BOARD_SESSION s
    join MIP.APP.COMMITTEE_HEARING h on h.HEARING_ID = s.HEARING_ID
    join MIP.APP.STRUCTURAL_TRADE_PROPOSALS p on p.PROPOSAL_ID = h.PROPOSAL_ID
),
dossier_availability as (
    select
        ds.DOSSIER_ID,
        ds.AS_OF_DATE,
        ds.DOSSIER_PAYLOAD_JSON:actionability_context:continuation_quality::string   as CONTINUATION_QUALITY,
        ds.DOSSIER_PAYLOAD_JSON:actionability_context:resistance_overhead_risk::string as RESISTANCE_OVERHEAD_RISK,
        ds.DOSSIER_PAYLOAD_JSON:candle_psychology:recent_cluster::string              as RECENT_CLUSTER,
        tv.FINAL_ACTION,
        tv.CHAIR_OUTPUT_JSON:thesis_health::string                                    as THESIS_HEALTH,
        tv.CHAIR_OUTPUT_JSON:prior_thesis_reference::variant                          as PRIOR_THESIS_REFERENCE
    from MIP.APP.PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT ds
    left join MIP.APP.PROPOSAL_BOARD_THESIS_VERDICT tv
           on tv.DOSSIER_ID = ds.DOSSIER_ID
)
select
    r.CARD_ID,
    r.CONCEPT_NAME,
    r.CONCEPT_FAMILY,
    r.SOURCE_BOOK,
    r.PAGE_START,
    r.PAGE_END,
    r.REVIEW_STATUS,
    r.REVIEWED_AT,

    -- Shadow session (concept-family matched, approximate)
    count(distinct s.SESSION_ID)                                            as MATCHING_SHADOW_SESSIONS,
    sum(case when s.SHADOW_STATUS = 'COMPLETE' then 1 else 0 end)          as COMPLETE_SHADOW_SESSIONS,
    max(s.SHADOW_CREATED_AT)                                                as LATEST_SHADOW_SESSION_AT,
    max(s.SHADOW_STANCE)                                                    as LATEST_SHADOW_STANCE,
    max(s.SHADOW_CONFIDENCE)                                                as LATEST_SHADOW_CONFIDENCE,

    -- Phase 4 dossier availability for matching proposals
    count(distinct d.DOSSIER_ID)                                            as MATCHING_DOSSIER_SNAPSHOTS,
    count(case when d.THESIS_HEALTH is not null then 1 end)                 as DOSSIERS_WITH_THESIS_HEALTH,
    count(case when d.CONTINUATION_QUALITY is not null then 1 end)          as DOSSIERS_WITH_CONTINUATION_QUALITY,

    -- Readiness flags
    case when count(distinct s.SESSION_ID) > 0 then 'YES' else 'NO' end    as HAS_SHADOW_SESSION,
    case when count(distinct d.DOSSIER_ID) > 0 then 'YES' else 'NO' end    as HAS_PHASE4_DOSSIER,
    case when
        count(distinct s.SESSION_ID) > 0
        and count(distinct d.DOSSIER_ID) > 0
    then 'READY_FOR_PHASE1_EVIDENCE_PACK_UPGRADE' else 'NOT_YET_MATCHED' end as READINESS_STATUS

from approved_rag r
left join shadow_sessions s
       on UPPER(s.PROPOSAL_SETUP_FAMILY) like '%' || UPPER(r.CONCEPT_FAMILY) || '%'
       or UPPER(r.CONCEPT_FAMILY) like '%' || UPPER(s.PROPOSAL_SETUP_FAMILY) || '%'
left join dossier_availability d
       on s.BOARD_DOSSIER_ID = d.DOSSIER_ID

group by
    r.CARD_ID, r.CONCEPT_NAME, r.CONCEPT_FAMILY, r.SOURCE_BOOK,
    r.PAGE_START, r.PAGE_END, r.REVIEW_STATUS, r.REVIEWED_AT

order by
    case when count(distinct s.SESSION_ID) > 0 then 0 else 1 end,
    r.CONCEPT_FAMILY,
    r.CONCEPT_NAME;

-- =============================================================================
-- Diagnostic queries
-- =============================================================================

-- 1. Full readiness matrix
select * from MIP.KNOWLEDGE.V_LITERATURE_REVALIDATION_SHADOW_READINESS;

-- 2. Summary: how many approved cards have matching shadow sessions vs not
select
    READINESS_STATUS,
    count(*) as CARD_COUNT,
    array_agg(CONCEPT_NAME) as CARD_NAMES
from MIP.KNOWLEDGE.V_LITERATURE_REVALIDATION_SHADOW_READINESS
group by READINESS_STATUS;

-- 3. Phase 4 dossier coverage check (independent of RAG cards)
select
    count(*) as TOTAL_DOSSIER_SNAPSHOTS,
    count(case when THESIS_HEALTH is not null then 1 end) as WITH_THESIS_HEALTH,
    count(case when CONTINUATION_QUALITY is not null then 1 end) as WITH_CONTINUATION_QUALITY,
    count(case when RECENT_CLUSTER is not null then 1 end) as WITH_RECENT_CLUSTER
from (
    select
        ds.DOSSIER_ID,
        tv.CHAIR_OUTPUT_JSON:thesis_health::string as THESIS_HEALTH,
        ds.DOSSIER_PAYLOAD_JSON:actionability_context:continuation_quality::string as CONTINUATION_QUALITY,
        ds.DOSSIER_PAYLOAD_JSON:candle_psychology:recent_cluster::string as RECENT_CLUSTER
    from MIP.APP.PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT ds
    left join MIP.APP.PROPOSAL_BOARD_THESIS_VERDICT tv on tv.DOSSIER_ID = ds.DOSSIER_ID
);

-- 4. Shadow board session count by status
select STATUS, count(*) as SESSION_COUNT, max(CREATED_AT) as LATEST_AT
from MIP.APP.SHADOW_BOARD_SESSION
group by STATUS
order by SESSION_COUNT desc;

-- 5. Check SHADOW_BOARD_ENABLED flag
select CONFIG_KEY, CONFIG_VALUE
from MIP.APP.APP_CONFIG
where CONFIG_KEY in ('SHADOW_BOARD_ENABLED', 'SHADOW_BOARD_TIMEOUT_SEC');
