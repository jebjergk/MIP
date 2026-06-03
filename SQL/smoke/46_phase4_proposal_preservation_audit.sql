-- =============================================================================
-- 46_phase4_proposal_preservation_audit.sql
-- Phase 4 — READ-ONLY proposal preservation audit.
--
-- Goal: account for EVERY chair PROPOSE_LONG / PROPOSE_SHORT from the last
-- 5 completed board runs, tracing each through:
--   thesis_verdict -> final_slate -> structural_proposal -> live_action -> LPA
--
-- Any proposal intent that disappears WITHOUT an explicit reason is
-- classified UNKNOWN_LOSS and treated as a bug.
--
-- Read-only.  No DML.  Does NOT run the Cortex agent panel.
-- =============================================================================


-- A1: DETAILED TRACE — one row per chair-proposed candidate
-- -----------------------------------------------------------------------------
WITH target_runs AS (
    SELECT
        RUN_ID,
        AS_OF_DATE,
        TRY_TO_NUMBER(MODEL_CONFIG_JSON:max_proposals::STRING) AS cfg_max_proposals
    FROM MIP.APP.PROPOSAL_BOARD_RUN
    WHERE RUN_STATUS IN ('COMPLETE','PARTIAL_FAILURE')
    ORDER BY CREATED_AT DESC
    LIMIT 5
),
chair_proposed AS (
    SELECT
        tv.RUN_ID, tv.DOSSIER_ID, tv.SYMBOL, tv.MARKET_TYPE,
        tv.FINAL_ACTION, tv.FINAL_DIRECTION, tv.PRIMARY_REASON_CODE
    FROM MIP.APP.PROPOSAL_BOARD_THESIS_VERDICT tv
    WHERE tv.RUN_ID IN (SELECT RUN_ID FROM target_runs)
      AND tv.FINAL_ACTION IN ('PROPOSE_LONG','PROPOSE_SHORT')
),
-- Pick one structural proposal per board dossier (latest by PROPOSAL_ID).
stp_pick AS (
    SELECT *
    FROM (
        SELECT
            stp.*,
            ROW_NUMBER() OVER (
                PARTITION BY stp.BOARD_RUN_ID, stp.BOARD_DOSSIER_ID
                ORDER BY stp.PROPOSAL_ID DESC
            ) AS rn
        FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS stp
        WHERE stp.BOARD_RUN_ID IN (SELECT RUN_ID FROM target_runs)
    )
    WHERE rn = 1
),
-- Pick one live action per proposal (latest by CREATED_AT).
la_pick AS (
    SELECT *
    FROM (
        SELECT
            la.PROPOSAL_ID, la.ACTION_ID, la.PORTFOLIO_ID, la.SYMBOL AS la_symbol,
            la.STATUS AS la_status, la.BROKER_NAME, la.BROKER_UNIVERSE_TYPE,
            la.IBKR_ACCOUNT_ID, la.SETUP_STILL_VALID, la.PRICE_MOVED_TOO_FAR,
            la.FRESHNESS_ASSESSMENT, la.LIVE_INTENT_KIND, la.ACTION_INTENT,
            ROW_NUMBER() OVER (
                PARTITION BY la.PROPOSAL_ID ORDER BY la.CREATED_AT DESC
            ) AS rn
        FROM MIP.LIVE.LIVE_ACTIONS la
        WHERE la.PROPOSAL_ID IN (SELECT PROPOSAL_ID FROM stp_pick)
    )
    WHERE rn = 1
)
SELECT
    r.AS_OF_DATE,
    cp.SYMBOL,
    cp.FINAL_ACTION                                   AS chair_action,
    -- Stage 1: final slate
    fs.RANK                                           AS slate_rank,
    fs.PUBLICATION_STATUS                             AS slate_pub_status,
    fs.PUBLICATION_ERROR_JSON:reason::STRING          AS slate_pub_reason,
    fs.PUBLISHED_PROPOSAL_ID                          AS slate_pub_proposal_id,
    r.cfg_max_proposals                               AS cfg_max_proposals,
    -- Stage 3-4: structural proposal
    sp.PROPOSAL_ID                                    AS stp_proposal_id,
    sp.STATUS                                         AS stp_status,
    sp.DIRECTION                                      AS stp_direction,
    sp.EXECUTION_POLICY_STATUS                        AS exec_status,
    sp.EXECUTION_POLICY_REASON                        AS exec_reason,
    sp.IS_RESEARCH_ONLY                               AS is_research_only,
    sp.PORTFOLIO_ID                                   AS stp_portfolio_id,
    -- Stage 5-6: live action / LPA
    la.ACTION_ID                                      AS la_action_id,
    la.la_status                                      AS la_status,
    la.PORTFOLIO_ID                                   AS la_portfolio_id,
    la.BROKER_NAME                                    AS la_broker,
    la.BROKER_UNIVERSE_TYPE                           AS la_universe,
    la.SETUP_STILL_VALID                              AS la_setup_valid,
    la.PRICE_MOVED_TOO_FAR                            AS la_price_far,
    la.FRESHNESS_ASSESSMENT                           AS la_freshness,
    -- ---------------------------------------------------------------------
    -- TERMINAL REASON (priority-ordered classification)
    -- ---------------------------------------------------------------------
    CASE
        -- 1. No final slate row at all
        WHEN fs.RUN_ID IS NULL
            THEN 'FINAL_SLATE_MISSING'
        -- 2. Rank cap — ONLY when cfg max_proposals is not null and rank exceeds it
        WHEN r.cfg_max_proposals IS NOT NULL
              AND fs.RANK IS NOT NULL
              AND fs.RANK > r.cfg_max_proposals
            THEN 'FINAL_SLATE_RANK_CAP:rank=' || fs.RANK::STRING
              || '_cap=' || r.cfg_max_proposals::STRING
        -- Explicit non-published terminal states on the slate
        WHEN fs.PUBLICATION_STATUS = 'SKIPPED_GUARDRAIL'
            THEN 'SLATE_GUARDRAIL:' || COALESCE(fs.PUBLICATION_ERROR_JSON:reason::STRING,'?')
        WHEN fs.PUBLICATION_STATUS = 'SHORT_RESEARCH_ONLY'
            THEN 'SLATE_SHORT_RESEARCH_ONLY'
        WHEN fs.PUBLICATION_STATUS = 'NOT_PUBLISHABLE'
            THEN 'SLATE_NOT_PUBLISHABLE:' || COALESCE(fs.PUBLICATION_ERROR_JSON:reason::STRING,'?')
        WHEN fs.PUBLICATION_STATUS = 'PENDING'
            THEN 'SLATE_PENDING_NOT_YET_PUBLISHED'
        -- 3. Published but no structural proposal row -> integrity failure
        WHEN fs.PUBLICATION_STATUS = 'PUBLISHED' AND sp.PROPOSAL_ID IS NULL
            THEN 'PUBLICATION_INTEGRITY_FAILURE'
        -- 4. Structural exists but blocked / research-only
        WHEN sp.PROPOSAL_ID IS NOT NULL
              AND sp.EXECUTION_POLICY_STATUS IS NOT NULL
              AND sp.EXECUTION_POLICY_STATUS != 'EXECUTABLE'
            THEN 'EXECUTION_POLICY_BLOCKED:' || sp.EXECUTION_POLICY_STATUS
              || '/' || COALESCE(sp.EXECUTION_POLICY_REASON,'?')
        WHEN sp.PROPOSAL_ID IS NOT NULL AND sp.IS_RESEARCH_ONLY = TRUE
            THEN 'RESEARCH_ONLY'
        -- 5. Executable, not research-only, but not imported to LPA
        WHEN sp.PROPOSAL_ID IS NOT NULL
              AND sp.EXECUTION_POLICY_STATUS = 'EXECUTABLE'
              AND sp.IS_RESEARCH_ONLY = FALSE
              AND la.ACTION_ID IS NULL
            THEN 'NOT_IMPORTED_TO_LPA'
        -- 6. Imported — check LPA visibility heuristics
        WHEN la.ACTION_ID IS NOT NULL
              AND la.la_status IN ('CANCELLED','REJECTED','EXPIRED','SUPERSEDED')
            THEN 'LPA_VISIBILITY_FILTERED:status=' || la.la_status
        WHEN la.ACTION_ID IS NOT NULL
              AND (la.SETUP_STILL_VALID = FALSE OR la.PRICE_MOVED_TOO_FAR = TRUE)
            THEN 'LPA_VISIBILITY_FILTERED:freshness='
              || COALESCE(la.FRESHNESS_ASSESSMENT,'?')
        WHEN la.ACTION_ID IS NOT NULL
            THEN 'IMPORTED_OK:la_status=' || COALESCE(la.la_status,'?')
        -- Anything else is an unexplained disappearance
        ELSE 'UNKNOWN_LOSS'
    END                                               AS terminal_reason,
    cp.RUN_ID
FROM chair_proposed cp
JOIN target_runs r
    ON r.RUN_ID = cp.RUN_ID
LEFT JOIN MIP.APP.PROPOSAL_BOARD_FINAL_SLATE_V2 fs
    ON fs.RUN_ID = cp.RUN_ID AND fs.DOSSIER_ID = cp.DOSSIER_ID
LEFT JOIN stp_pick sp
    ON sp.BOARD_RUN_ID = cp.RUN_ID AND sp.BOARD_DOSSIER_ID = cp.DOSSIER_ID
LEFT JOIN la_pick la
    ON la.PROPOSAL_ID = sp.PROPOSAL_ID
ORDER BY r.AS_OF_DATE DESC, cp.SYMBOL;


-- A2: SUMMARY grouped by terminal_reason
-- -----------------------------------------------------------------------------
WITH target_runs AS (
    SELECT RUN_ID,
           TRY_TO_NUMBER(MODEL_CONFIG_JSON:max_proposals::STRING) AS cfg_max_proposals
    FROM MIP.APP.PROPOSAL_BOARD_RUN
    WHERE RUN_STATUS IN ('COMPLETE','PARTIAL_FAILURE')
    ORDER BY CREATED_AT DESC
    LIMIT 5
),
chair_proposed AS (
    SELECT tv.RUN_ID, tv.DOSSIER_ID, tv.SYMBOL, tv.FINAL_ACTION
    FROM MIP.APP.PROPOSAL_BOARD_THESIS_VERDICT tv
    WHERE tv.RUN_ID IN (SELECT RUN_ID FROM target_runs)
      AND tv.FINAL_ACTION IN ('PROPOSE_LONG','PROPOSE_SHORT')
),
stp_pick AS (
    SELECT * FROM (
        SELECT stp.PROPOSAL_ID, stp.BOARD_RUN_ID, stp.BOARD_DOSSIER_ID,
               stp.EXECUTION_POLICY_STATUS, stp.EXECUTION_POLICY_REASON,
               stp.IS_RESEARCH_ONLY,
               ROW_NUMBER() OVER (PARTITION BY stp.BOARD_RUN_ID, stp.BOARD_DOSSIER_ID
                                  ORDER BY stp.PROPOSAL_ID DESC) AS rn
        FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS stp
        WHERE stp.BOARD_RUN_ID IN (SELECT RUN_ID FROM target_runs)
    ) WHERE rn = 1
),
la_pick AS (
    SELECT * FROM (
        SELECT la.PROPOSAL_ID, la.ACTION_ID, la.STATUS AS la_status,
               la.SETUP_STILL_VALID, la.PRICE_MOVED_TOO_FAR, la.FRESHNESS_ASSESSMENT,
               ROW_NUMBER() OVER (PARTITION BY la.PROPOSAL_ID ORDER BY la.CREATED_AT DESC) AS rn
        FROM MIP.LIVE.LIVE_ACTIONS la
        WHERE la.PROPOSAL_ID IN (SELECT PROPOSAL_ID FROM stp_pick)
    ) WHERE rn = 1
),
classified AS (
    SELECT
        CASE
            WHEN fs.RUN_ID IS NULL THEN 'FINAL_SLATE_MISSING'
            WHEN r.cfg_max_proposals IS NOT NULL AND fs.RANK IS NOT NULL
                  AND fs.RANK > r.cfg_max_proposals THEN 'FINAL_SLATE_RANK_CAP'
            WHEN fs.PUBLICATION_STATUS = 'SKIPPED_GUARDRAIL'
                THEN 'SLATE_GUARDRAIL:' || COALESCE(fs.PUBLICATION_ERROR_JSON:reason::STRING,'?')
            WHEN fs.PUBLICATION_STATUS = 'SHORT_RESEARCH_ONLY' THEN 'SLATE_SHORT_RESEARCH_ONLY'
            WHEN fs.PUBLICATION_STATUS = 'NOT_PUBLISHABLE'
                THEN 'SLATE_NOT_PUBLISHABLE:' || COALESCE(fs.PUBLICATION_ERROR_JSON:reason::STRING,'?')
            WHEN fs.PUBLICATION_STATUS = 'PENDING' THEN 'SLATE_PENDING_NOT_YET_PUBLISHED'
            WHEN fs.PUBLICATION_STATUS = 'PUBLISHED' AND sp.PROPOSAL_ID IS NULL
                THEN 'PUBLICATION_INTEGRITY_FAILURE'
            WHEN sp.PROPOSAL_ID IS NOT NULL AND sp.EXECUTION_POLICY_STATUS IS NOT NULL
                  AND sp.EXECUTION_POLICY_STATUS != 'EXECUTABLE'
                THEN 'EXECUTION_POLICY_BLOCKED:' || sp.EXECUTION_POLICY_STATUS
                  || '/' || COALESCE(sp.EXECUTION_POLICY_REASON,'?')
            WHEN sp.PROPOSAL_ID IS NOT NULL AND sp.IS_RESEARCH_ONLY = TRUE THEN 'RESEARCH_ONLY'
            WHEN sp.PROPOSAL_ID IS NOT NULL AND sp.EXECUTION_POLICY_STATUS = 'EXECUTABLE'
                  AND sp.IS_RESEARCH_ONLY = FALSE AND la.ACTION_ID IS NULL
                THEN 'NOT_IMPORTED_TO_LPA'
            WHEN la.ACTION_ID IS NOT NULL
                  AND la.la_status IN ('CANCELLED','REJECTED','EXPIRED','SUPERSEDED')
                THEN 'LPA_VISIBILITY_FILTERED:status=' || la.la_status
            WHEN la.ACTION_ID IS NOT NULL
                  AND (la.SETUP_STILL_VALID = FALSE OR la.PRICE_MOVED_TOO_FAR = TRUE)
                THEN 'LPA_VISIBILITY_FILTERED:freshness'
            WHEN la.ACTION_ID IS NOT NULL THEN 'IMPORTED_OK'
            ELSE 'UNKNOWN_LOSS'
        END AS terminal_reason
    FROM chair_proposed cp
    JOIN target_runs r ON r.RUN_ID = cp.RUN_ID
    LEFT JOIN MIP.APP.PROPOSAL_BOARD_FINAL_SLATE_V2 fs
        ON fs.RUN_ID = cp.RUN_ID AND fs.DOSSIER_ID = cp.DOSSIER_ID
    LEFT JOIN stp_pick sp ON sp.BOARD_RUN_ID = cp.RUN_ID AND sp.BOARD_DOSSIER_ID = cp.DOSSIER_ID
    LEFT JOIN la_pick la ON la.PROPOSAL_ID = sp.PROPOSAL_ID
)
SELECT terminal_reason, COUNT(*) AS candidates
FROM classified
GROUP BY 1
ORDER BY 2 DESC;
