-- =============================================================================
-- 45_phase4_attrition_diagnostic.sql
-- Phase 4 agentic board — read-only attrition diagnostic (Stage 1)
--
-- Run this against recent COMPLETE/PARTIAL_FAILURE board runs to answer:
--   Q1: Per-run funnel (one row per run, no row multiplication)
--   Q2: First terminal reason per symbol per run
--   Q3: Zombie run check (RUNNING status > 30 min)
--   Q4: Eligibility skip-reason distribution (cost-cap separated)
--   Q5: Chair verdict distribution
--   Q6: Proposals by execution policy + import state
--
-- Classification rule:
--   NOT_SENT_TO_AGENT_PANEL_COST_CAP = Stage 2 cost-cap — NOT a quality rejection.
--   Never mix with genuine eligibility blocks in any count.
--
-- Read-only.  Safe to run at any time.  No DML.
-- =============================================================================


-- Q1: Per-run funnel (CTE-based — no row multiplication)
--
--   Each pipeline stage is aggregated independently in its own CTE before the
--   final SELECT joins back to PROPOSAL_BOARD_RUN.  One-to-many joins cannot
--   inflate counts.  cost_capped is a separate column from elig_blocked_genuine.
-- -----------------------------------------------------------------------------
WITH target_runs AS (
    SELECT
        RUN_ID,
        AS_OF_DATE,
        RUN_STATUS,
        CANDIDATE_COUNT,
        FINAL_PROPOSAL_COUNT,
        STARTED_AT,
        TRY_TO_NUMBER(MODEL_CONFIG_JSON:max_candidates::STRING) AS cfg_max_candidates
    FROM MIP.APP.PROPOSAL_BOARD_RUN
    WHERE CREATED_AT >= DATEADD('day', -7, CURRENT_DATE())
    ORDER BY CREATED_AT DESC
    LIMIT 10
),
elig AS (
    SELECT
        RUN_ID,
        SUM(IFF(ELIGIBLE, 1, 0))
            AS elig_passed,
        SUM(IFF(NOT ELIGIBLE
                AND PRIMARY_REASON_CODE != 'NOT_SENT_TO_AGENT_PANEL_COST_CAP', 1, 0))
            AS elig_blocked_genuine,
        SUM(IFF(PRIMARY_REASON_CODE = 'NOT_SENT_TO_AGENT_PANEL_COST_CAP', 1, 0))
            AS cost_capped
    FROM MIP.APP.PROPOSAL_BOARD_REVIEW_ELIGIBILITY
    WHERE RUN_ID IN (SELECT RUN_ID FROM target_runs)
    GROUP BY RUN_ID
),
agent_outs AS (
    SELECT
        RUN_ID,
        COUNT(DISTINCT DOSSIER_ID)
            AS dossiers_with_outcomes,
        COUNT(DISTINCT IFF(
            VERDICT = 'INVALID' OR PRIMARY_REASON_CODE = 'AGENT_OUTPUT_INVALID',
            DOSSIER_ID, NULL))
            AS dossiers_with_agent_fail
    FROM MIP.APP.PROPOSAL_BOARD_AGENT_OUTCOME_V2
    WHERE RUN_ID IN (SELECT RUN_ID FROM target_runs)
    GROUP BY RUN_ID
),
chairs AS (
    SELECT
        RUN_ID,
        SUM(IFF(FINAL_ACTION IN ('PROPOSE_LONG','PROPOSE_SHORT'), 1, 0)) AS chair_propose,
        SUM(IFF(FINAL_ACTION NOT IN ('PROPOSE_LONG','PROPOSE_SHORT'), 1, 0)) AS chair_no_trade
    FROM MIP.APP.PROPOSAL_BOARD_THESIS_VERDICT
    WHERE RUN_ID IN (SELECT RUN_ID FROM target_runs)
    GROUP BY RUN_ID
),
slate AS (
    SELECT
        RUN_ID,
        SUM(IFF(PUBLICATION_STATUS = 'PUBLISHED',         1, 0)) AS slate_published,
        SUM(IFF(PUBLICATION_STATUS = 'SKIPPED_GUARDRAIL', 1, 0)) AS slate_guardrail
    FROM MIP.APP.PROPOSAL_BOARD_FINAL_SLATE_V2
    WHERE RUN_ID IN (SELECT RUN_ID FROM target_runs)
    GROUP BY RUN_ID
),
props AS (
    SELECT
        BOARD_RUN_ID AS RUN_ID,
        SUM(IFF(EXECUTION_POLICY_STATUS = 'EXECUTABLE', 1, 0))  AS props_executable,
        SUM(IFF(EXECUTION_POLICY_STATUS != 'EXECUTABLE', 1, 0)) AS props_blocked
    FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS
    WHERE BOARD_RUN_ID IN (SELECT RUN_ID FROM target_runs)
    GROUP BY BOARD_RUN_ID
),
imports AS (
    SELECT
        stp.BOARD_RUN_ID AS RUN_ID,
        COUNT(DISTINCT la.ACTION_ID) AS imported_to_lpa
    FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS stp
    JOIN MIP.LIVE.LIVE_ACTIONS la ON la.PROPOSAL_ID = stp.PROPOSAL_ID
    WHERE stp.BOARD_RUN_ID IN (SELECT RUN_ID FROM target_runs)
    GROUP BY stp.BOARD_RUN_ID
)
SELECT
    r.AS_OF_DATE,
    r.RUN_STATUS,
    r.cfg_max_candidates                             AS cfg_max_candidates,
    r.CANDIDATE_COUNT                                AS snapshotted,
    COALESCE(e.elig_passed,          0)              AS elig_passed,
    COALESCE(e.elig_blocked_genuine, 0)              AS elig_blocked_genuine,
    COALESCE(e.cost_capped,          0)              AS cost_capped,
    COALESCE(a.dossiers_with_outcomes,    0)         AS agents_ran,
    COALESCE(a.dossiers_with_agent_fail,  0)         AS agent_failures,
    COALESCE(c.chair_propose,    0)                  AS chair_propose,
    COALESCE(c.chair_no_trade,   0)                  AS chair_no_trade,
    COALESCE(s.slate_published,  0)                  AS slate_published,
    COALESCE(s.slate_guardrail,  0)                  AS slate_guardrail,
    COALESCE(p.props_executable, 0)                  AS props_executable,
    COALESCE(p.props_blocked,    0)                  AS props_blocked,
    COALESCE(i.imported_to_lpa,  0)                  AS imported_to_lpa,
    r.FINAL_PROPOSAL_COUNT                           AS run_recorded_proposals,
    r.RUN_ID
FROM target_runs r
LEFT JOIN elig       e ON e.RUN_ID = r.RUN_ID
LEFT JOIN agent_outs a ON a.RUN_ID = r.RUN_ID
LEFT JOIN chairs     c ON c.RUN_ID = r.RUN_ID
LEFT JOIN slate      s ON s.RUN_ID = r.RUN_ID
LEFT JOIN props      p ON p.RUN_ID = r.RUN_ID
LEFT JOIN imports    i ON i.RUN_ID = r.RUN_ID
ORDER BY r.STARTED_AT DESC;


-- Q2: First terminal reason per symbol per run
--
--   RANK_CAPPED reads the actual run config's max_proposals / max_candidates,
--   never hardcoded.  cost_capped symbols surface first (before eligibility
--   blocks) so they cannot be miscounted as quality failures.
-- -----------------------------------------------------------------------------
WITH target_runs AS (
    SELECT
        RUN_ID,
        AS_OF_DATE,
        RUN_STATUS,
        TRY_TO_NUMBER(MODEL_CONFIG_JSON:max_proposals::STRING)   AS run_max_proposals,
        TRY_TO_NUMBER(MODEL_CONFIG_JSON:max_candidates::STRING)  AS run_max_candidates
    FROM MIP.APP.PROPOSAL_BOARD_RUN
    WHERE CREATED_AT >= DATEADD('day', -7, CURRENT_DATE())
      AND RUN_STATUS IN ('COMPLETE','PARTIAL_FAILURE')
),
agent_fails AS (
    SELECT RUN_ID, DOSSIER_ID, MIN(AGENT_NAME) AS failed_agent
    FROM MIP.APP.PROPOSAL_BOARD_AGENT_OUTCOME_V2
    WHERE VERDICT = 'INVALID' OR PRIMARY_REASON_CODE = 'AGENT_OUTPUT_INVALID'
    GROUP BY RUN_ID, DOSSIER_ID
)
SELECT
    r.AS_OF_DATE,
    s.SYMBOL,
    COALESCE(
        -- 1. Cost-cap (Stage 2 cheap mode): not a real eligibility reject
        CASE WHEN e.PRIMARY_REASON_CODE = 'NOT_SENT_TO_AGENT_PANEL_COST_CAP'
             THEN 'COST_CAP:not_sent_to_agents' END,
        -- 2. Genuine eligibility block
        CASE WHEN e.ELIGIBLE = FALSE
              AND e.PRIMARY_REASON_CODE != 'NOT_SENT_TO_AGENT_PANEL_COST_CAP'
             THEN 'ELIG_BLOCK:' || e.PRIMARY_REASON_CODE END,
        -- 3. Agent execution failure
        CASE WHEN af.failed_agent IS NOT NULL
             THEN 'AGENT_FAIL:' || af.failed_agent END,
        -- 4. Chair declined
        CASE WHEN tv.FINAL_ACTION NOT IN ('PROPOSE_LONG','PROPOSE_SHORT')
              AND tv.FINAL_ACTION IS NOT NULL
             THEN 'CHAIR:' || tv.FINAL_ACTION
               || COALESCE(':' || tv.PRIMARY_REASON_CODE, '') END,
        -- 5. No chair verdict reached (agent ran but chair step missing)
        CASE WHEN tv.FINAL_ACTION IS NULL AND e.ELIGIBLE = TRUE
             THEN 'NO_CHAIR_VERDICT' END,
        -- 6. Rank cap — uses actual run config, not hardcoded constant
        CASE WHEN fs.RANK IS NOT NULL
              AND r.run_max_proposals IS NOT NULL
              AND fs.RANK > r.run_max_proposals
             THEN 'RANK_CAPPED:rank=' || fs.RANK::STRING
               || '_cap=' || r.run_max_proposals::STRING END,
        -- 7. Guardrail skip at publication
        CASE WHEN fs.PUBLICATION_STATUS = 'SKIPPED_GUARDRAIL'
             THEN 'GUARDRAIL:' || COALESCE(fs.PUBLICATION_ERROR_JSON:reason::STRING, '?') END,
        -- 8. Execution policy blocked post-insert
        CASE WHEN stp.EXECUTION_POLICY_STATUS IS NOT NULL
              AND stp.EXECUTION_POLICY_STATUS != 'EXECUTABLE'
             THEN 'EXEC_POLICY:' || stp.EXECUTION_POLICY_STATUS
               || '/' || COALESCE(stp.EXECUTION_POLICY_REASON, '?') END,
        -- 9. Inserted + executable but never imported to LPA
        CASE WHEN stp.PROPOSAL_ID IS NOT NULL
              AND stp.EXECUTION_POLICY_STATUS = 'EXECUTABLE'
              AND NOT EXISTS (
                  SELECT 1 FROM MIP.LIVE.LIVE_ACTIONS la
                  WHERE la.PROPOSAL_ID = stp.PROPOSAL_ID)
             THEN 'NOT_IMPORTED_TO_LPA' END,
        -- 10. Made it all the way
        CASE WHEN stp.PROPOSAL_ID IS NOT NULL
              AND EXISTS (
                  SELECT 1 FROM MIP.LIVE.LIVE_ACTIONS la
                  WHERE la.PROPOSAL_ID = stp.PROPOSAL_ID)
             THEN 'IMPORTED_OK' END,
        'UNKNOWN'
    ) AS terminal_reason,
    r.RUN_ID
FROM target_runs r
JOIN  MIP.APP.PROPOSAL_BOARD_REVIEW_ELIGIBILITY     e  ON e.RUN_ID   = r.RUN_ID
JOIN  MIP.APP.PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT s  ON s.RUN_ID  = r.RUN_ID
                                                       AND s.DOSSIER_ID = e.DOSSIER_ID
LEFT JOIN agent_fails                               af  ON af.RUN_ID  = r.RUN_ID AND af.DOSSIER_ID = e.DOSSIER_ID
LEFT JOIN MIP.APP.PROPOSAL_BOARD_THESIS_VERDICT     tv  ON tv.RUN_ID  = r.RUN_ID AND tv.DOSSIER_ID = e.DOSSIER_ID
LEFT JOIN MIP.APP.PROPOSAL_BOARD_FINAL_SLATE_V2     fs  ON fs.RUN_ID  = r.RUN_ID AND fs.DOSSIER_ID = e.DOSSIER_ID
LEFT JOIN MIP.APP.STRUCTURAL_TRADE_PROPOSALS        stp ON stp.BOARD_RUN_ID  = r.RUN_ID
                                                       AND stp.BOARD_DOSSIER_ID = e.DOSSIER_ID
ORDER BY r.AS_OF_DATE DESC, s.SYMBOL;


-- Q3: Zombie run check (RUNNING > 30 minutes)
-- -----------------------------------------------------------------------------
SELECT
    RUN_ID,
    AS_OF_DATE,
    STARTED_AT,
    DATEDIFF('minute', STARTED_AT, CURRENT_TIMESTAMP()) AS age_minutes
FROM MIP.APP.PROPOSAL_BOARD_RUN
WHERE RUN_STATUS = 'RUNNING'
ORDER BY STARTED_AT;


-- Q4: Eligibility skip-reason distribution
--   ** COST_CAP ** labelled separately so it cannot be confused with
--   genuine quality or market-condition blocks.
-- -----------------------------------------------------------------------------
SELECT
    r.AS_OF_DATE,
    CASE WHEN e.PRIMARY_REASON_CODE = 'NOT_SENT_TO_AGENT_PANEL_COST_CAP'
         THEN '** COST_CAP ** (not a quality rejection)'
         ELSE e.PRIMARY_REASON_CODE
    END  AS reason_code,
    COUNT(*) AS cnt
FROM MIP.APP.PROPOSAL_BOARD_RUN r
JOIN MIP.APP.PROPOSAL_BOARD_REVIEW_ELIGIBILITY e ON e.RUN_ID = r.RUN_ID
WHERE r.CREATED_AT >= DATEADD('day', -7, CURRENT_DATE())
  AND NOT e.ELIGIBLE
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC;


-- Q5: Chair verdict distribution
-- -----------------------------------------------------------------------------
SELECT
    r.AS_OF_DATE,
    tv.FINAL_ACTION,
    tv.PRIMARY_REASON_CODE,
    COUNT(*) AS cnt
FROM MIP.APP.PROPOSAL_BOARD_RUN r
JOIN MIP.APP.PROPOSAL_BOARD_THESIS_VERDICT tv ON tv.RUN_ID = r.RUN_ID
WHERE r.CREATED_AT >= DATEADD('day', -7, CURRENT_DATE())
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 4 DESC;


-- Q6: Proposals by execution policy status + import state
-- -----------------------------------------------------------------------------
SELECT
    stp.EXECUTION_POLICY_STATUS,
    stp.EXECUTION_POLICY_REASON,
    stp.IS_RESEARCH_ONLY,
    stp.STATUS,
    COUNT(*)                  AS proposal_count,
    COUNT(la.ACTION_ID)       AS in_live_actions
FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS stp
LEFT JOIN MIP.LIVE.LIVE_ACTIONS la ON la.PROPOSAL_ID = stp.PROPOSAL_ID
WHERE stp.BOARD_RUN_ID IS NOT NULL
  AND stp.CREATED_AT >= DATEADD('day', -7, CURRENT_DATE())
GROUP BY 1, 2, 3, 4
ORDER BY 5 DESC;
