/* ================================================================
   phase4_retire_candidate_review_board_proposals.sql
   Retire active candidate-review board proposals for Phase 4 cutover.

   This preserves historical rows and does not mutate executed trades,
   IBKR/live truth, reconciliation rows, or broker-submitted actions.
   Pre-broker structural live actions linked to retired proposals are
   superseded so they stop surfacing as actionable.
   ================================================================ */

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;
USE SCHEMA APP;

CREATE OR REPLACE TEMPORARY TABLE TMP_PHASE4_RETIRED_CANDIDATE_REVIEW_PROPOSALS AS
SELECT
    p.PROPOSAL_ID,
    p.SYMBOL,
    p.BOARD_RUN_ID,
    p.BOARD_CANDIDATE_ID
FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
LEFT JOIN MIP.APP.PROPOSAL_BOARD_RUN r
  ON r.RUN_ID = p.BOARD_RUN_ID
WHERE p.STATUS = 'PROPOSED'
  AND COALESCE(r.MODEL_CONFIG_JSON:mode::STRING, '') <> 'symbol_dossier_agentic_thesis_board';

SELECT
    'BEFORE_RETIRE' AS STEP,
    COUNT(*) AS PROPOSALS_TO_EXPIRE,
    ARRAY_AGG(PROPOSAL_ID) WITHIN GROUP (ORDER BY PROPOSAL_ID) AS PROPOSAL_IDS
FROM TMP_PHASE4_RETIRED_CANDIDATE_REVIEW_PROPOSALS;

UPDATE MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
   SET STATUS = 'EXPIRED',
       COMMITTEE_PAYLOAD = OBJECT_INSERT(
           OBJECT_INSERT(
               COALESCE(p.COMMITTEE_PAYLOAD, OBJECT_CONSTRUCT()),
               'phase4_candidate_review_retired',
               OBJECT_CONSTRUCT(
                   'reason_code', 'RETIRED_CANDIDATE_REVIEW_BOARD_CUTOVER',
                   'retired_at', CURRENT_TIMESTAMP(),
                   'preserved_board_run_id', p.BOARD_RUN_ID,
                   'preserved_board_candidate_id', p.BOARD_CANDIDATE_ID,
                   'historical_row_preserved', TRUE
               ),
               TRUE
           ),
           'status_before_phase4_cutover',
           'PROPOSED',
           TRUE
       ),
       BOARD_PAYLOAD_JSON = OBJECT_INSERT(
           COALESCE(p.BOARD_PAYLOAD_JSON, OBJECT_CONSTRUCT()),
           'phase4_candidate_review_retired',
           OBJECT_CONSTRUCT(
               'reason_code', 'RETIRED_CANDIDATE_REVIEW_BOARD_CUTOVER',
               'retired_at', CURRENT_TIMESTAMP(),
               'historical_row_preserved', TRUE
           ),
           TRUE
       )
 WHERE p.PROPOSAL_ID IN (
     SELECT PROPOSAL_ID FROM TMP_PHASE4_RETIRED_CANDIDATE_REVIEW_PROPOSALS
 );

UPDATE MIP.LIVE.LIVE_ACTIONS la
   SET STATUS = 'SUPERSEDED',
       REASON_CODES = ARRAY_APPEND(
           COALESCE(la.REASON_CODES, ARRAY_CONSTRUCT()),
           'RETIRED_CANDIDATE_REVIEW_BOARD_CUTOVER'
       ),
       UPDATED_AT = CURRENT_TIMESTAMP()
 WHERE la.LIVE_INTENT_KIND = 'STRUCTURAL'
   AND la.PROPOSAL_ID IN (
       SELECT PROPOSAL_ID FROM TMP_PHASE4_RETIRED_CANDIDATE_REVIEW_PROPOSALS
   )
   AND la.STATUS IN (
       'PROPOSED',
       'INTENT_APPROVED',
       'PENDING_OPEN_VALIDATION',
       'OPEN_BLOCKED'
   );

SELECT
    'AFTER_RETIRE' AS STEP,
    (SELECT COUNT(*) FROM TMP_PHASE4_RETIRED_CANDIDATE_REVIEW_PROPOSALS) AS PROPOSALS_EXPIRED,
    (SELECT COUNT(*)
       FROM MIP.LIVE.LIVE_ACTIONS la
      WHERE la.PROPOSAL_ID IN (SELECT PROPOSAL_ID FROM TMP_PHASE4_RETIRED_CANDIDATE_REVIEW_PROPOSALS)
        AND la.STATUS = 'SUPERSEDED'
        AND ARRAY_CONTAINS('RETIRED_CANDIDATE_REVIEW_BOARD_CUTOVER'::VARIANT, la.REASON_CODES)
    ) AS PRE_BROKER_ACTIONS_SUPERSEDED,
    (SELECT COUNT(*)
       FROM MIP.LIVE.LIVE_ACTIONS la
      WHERE la.PROPOSAL_ID IN (SELECT PROPOSAL_ID FROM TMP_PHASE4_RETIRED_CANDIDATE_REVIEW_PROPOSALS)
        AND la.STATUS = 'EXECUTION_REQUESTED'
    ) AS BROKER_SUBMITTED_ACTIONS_LEFT_UNCHANGED;

DROP TABLE IF EXISTS TMP_PHASE4_RETIRED_CANDIDATE_REVIEW_PROPOSALS;
