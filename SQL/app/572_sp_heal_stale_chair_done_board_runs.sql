/* ================================================================
   572_sp_heal_stale_chair_done_board_runs.sql
   Finalize PROPOSAL_BOARD_RUN rows stuck at CHAIR_DONE after publish.

   The board subprocess can die after STRUCTURAL_TRADE_PROPOSALS inserts
   but before RUN_STATUS='COMPLETE'. LPA import and authoritative-run views
   then ignore the published proposals. Callable from MIP_UI_API_ROLE.
   ================================================================ */

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;
USE SCHEMA APP;

CREATE OR REPLACE PROCEDURE MIP.APP.SP_HEAL_STALE_CHAIR_DONE_BOARD_RUNS(
    MAX_AGE_MINUTES INT DEFAULT 3
)
RETURNS INT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    healed INT DEFAULT 0;
BEGIN
    UPDATE MIP.APP.PROPOSAL_BOARD_RUN r
       SET RUN_STATUS = 'COMPLETE',
           FINISHED_AT = CURRENT_TIMESTAMP(),
           FINAL_PROPOSAL_COUNT = pub.PUB_COUNT,
           CANDIDATE_COUNT = GREATEST(COALESCE(r.CANDIDATE_COUNT, 0), pub.PUB_COUNT),
           ERROR_JSON = OBJECT_CONSTRUCT(
               'reason_code', 'AUTO_FINALIZED_STALE_CHAIR_DONE',
               'message', 'Run stalled at CHAIR_DONE after publish; auto-finalized via SP.',
               'max_age_minutes', :MAX_AGE_MINUTES,
               'final_proposal_count_source', 'STRUCTURAL_TRADE_PROPOSALS row count'
           )
      FROM (
          SELECT BOARD_RUN_ID AS RUN_ID, COUNT(*) AS PUB_COUNT
          FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS
          WHERE STATUS IN ('PROPOSED', 'EXPIRED')
          GROUP BY BOARD_RUN_ID
      ) pub
     WHERE pub.RUN_ID = r.RUN_ID
       AND r.RUN_STATUS = 'CHAIR_DONE'
       AND r.FINISHED_AT IS NULL
       AND r.STARTED_AT < DATEADD('minute', -:MAX_AGE_MINUTES, CURRENT_TIMESTAMP())
       AND pub.PUB_COUNT > 0;

    healed := SQLROWCOUNT;
    RETURN healed;
END;
$$;

GRANT USAGE ON PROCEDURE MIP.APP.SP_HEAL_STALE_CHAIR_DONE_BOARD_RUNS(INT) TO ROLE MIP_UI_API_ROLE;
GRANT USAGE ON PROCEDURE MIP.APP.SP_HEAL_STALE_CHAIR_DONE_BOARD_RUNS(INT) TO ROLE MIP_APP_ROLE;
