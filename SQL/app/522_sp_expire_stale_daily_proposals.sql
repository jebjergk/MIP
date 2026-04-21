/*  ================================================================
    522_sp_expire_stale_daily_proposals.sql
    MIP Structural Strategy Framework — Daily Proposal Expiry

    Enforces the "max one daily-bar cycle" lifetime rule for
    STRUCTURAL_TRADE_PROPOSALS:

      * Any STRUCTURAL_TRADE_PROPOSALS row with STATUS='PROPOSED'
        whose CREATED_AT::DATE is BEFORE the current as-of date
        is marked STATUS='EXPIRED' (no longer surfaced as actionable
        in V_STRUCTURAL_PROPOSALS_FOR_COMMITTEE).

      * Cascades to LIVE_ACTIONS where LIVE_INTENT_KIND='STRUCTURAL'
        and the action is still in an open / pre-broker state
        (PROPOSED, INTENT_APPROVED, PENDING_OPEN_VALIDATION,
        OPEN_BLOCKED). Those are marked STATUS='SUPERSEDED' with
        REASON_CODES appended 'OLD_DAILY_PROPOSAL_EXPIRED'.

      * EXECUTION_REQUESTED actions tied to expired proposals are
        NOT modified (they're live with the broker — race risk).
        They are reported in the result for visibility / manual review.

    Called automatically at the start of SP_RUN_STRUCTURAL_DAILY_PIPELINE
    (right before SP_PROPOSE_STRUCTURAL_TRADES). Safe to invoke ad-hoc.
    ================================================================ */

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;
USE SCHEMA APP;

CREATE OR REPLACE PROCEDURE MIP.APP.SP_EXPIRE_STALE_DAILY_PROPOSALS(
    P_AS_OF_DATE DATE DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_as_of                    DATE := COALESCE(P_AS_OF_DATE, CURRENT_DATE());
    v_run_start                TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
    v_proposals_expired        NUMBER := 0;
    v_actions_superseded       NUMBER := 0;
    v_actions_at_broker_stale  NUMBER := 0;
    v_expired_ids              ARRAY := ARRAY_CONSTRUCT();
    v_superseded_action_ids    ARRAY := ARRAY_CONSTRUCT();
    v_at_broker_action_ids     ARRAY := ARRAY_CONSTRUCT();
BEGIN

    -- ============================================================
    -- STEP 1: Identify stale PROPOSED rows (created on a prior day)
    -- ============================================================
    CREATE OR REPLACE TEMPORARY TABLE TMP_EXPIRED_PROPOSALS AS
    SELECT PROPOSAL_ID
      FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS
     WHERE STATUS = 'PROPOSED'
       AND CREATED_AT::DATE < :v_as_of;

    SELECT COUNT(*), ARRAY_AGG(PROPOSAL_ID) WITHIN GROUP (ORDER BY PROPOSAL_ID)
      INTO :v_proposals_expired, :v_expired_ids
      FROM TMP_EXPIRED_PROPOSALS;

    v_expired_ids := COALESCE(:v_expired_ids, ARRAY_CONSTRUCT());

    -- ============================================================
    -- STEP 2: Expire those proposals
    -- ============================================================
    IF (v_proposals_expired > 0) THEN
        UPDATE MIP.APP.STRUCTURAL_TRADE_PROPOSALS
           SET STATUS = 'EXPIRED'
         WHERE PROPOSAL_ID IN (SELECT PROPOSAL_ID FROM TMP_EXPIRED_PROPOSALS);
    END IF;

    -- ============================================================
    -- STEP 3: Cascade to LIVE_ACTIONS — pre-broker open states only.
    --   EXECUTION_REQUESTED is intentionally NOT auto-superseded
    --   (broker race). It's surfaced in the result so the user can
    --   review and cancel manually.
    -- ============================================================
    IF (v_proposals_expired > 0) THEN
        SELECT ARRAY_AGG(ACTION_ID) WITHIN GROUP (ORDER BY ACTION_ID)
          INTO :v_superseded_action_ids
          FROM MIP.LIVE.LIVE_ACTIONS
         WHERE LIVE_INTENT_KIND = 'STRUCTURAL'
           AND PROPOSAL_ID IN (SELECT PROPOSAL_ID FROM TMP_EXPIRED_PROPOSALS)
           AND STATUS IN (
               'PROPOSED',
               'INTENT_APPROVED',
               'PENDING_OPEN_VALIDATION',
               'OPEN_BLOCKED'
           );

        v_superseded_action_ids := COALESCE(:v_superseded_action_ids, ARRAY_CONSTRUCT());

        UPDATE MIP.LIVE.LIVE_ACTIONS
           SET STATUS       = 'SUPERSEDED',
               REASON_CODES = ARRAY_APPEND(
                                 COALESCE(REASON_CODES, ARRAY_CONSTRUCT()),
                                 'OLD_DAILY_PROPOSAL_EXPIRED'
                              ),
               UPDATED_AT   = CURRENT_TIMESTAMP()
         WHERE LIVE_INTENT_KIND = 'STRUCTURAL'
           AND PROPOSAL_ID IN (SELECT PROPOSAL_ID FROM TMP_EXPIRED_PROPOSALS)
           AND STATUS IN (
               'PROPOSED',
               'INTENT_APPROVED',
               'PENDING_OPEN_VALIDATION',
               'OPEN_BLOCKED'
           );
        v_actions_superseded := SQLROWCOUNT;

        -- ============================================================
        -- STEP 4: Identify (don't mutate) EXECUTION_REQUESTED actions
        --         tied to expired proposals — these are live with broker.
        -- ============================================================
        SELECT COUNT(*), ARRAY_AGG(ACTION_ID) WITHIN GROUP (ORDER BY ACTION_ID)
          INTO :v_actions_at_broker_stale, :v_at_broker_action_ids
          FROM MIP.LIVE.LIVE_ACTIONS
         WHERE LIVE_INTENT_KIND = 'STRUCTURAL'
           AND PROPOSAL_ID IN (SELECT PROPOSAL_ID FROM TMP_EXPIRED_PROPOSALS)
           AND STATUS = 'EXECUTION_REQUESTED';

        v_at_broker_action_ids := COALESCE(:v_at_broker_action_ids, ARRAY_CONSTRUCT());
    END IF;

    DROP TABLE IF EXISTS TMP_EXPIRED_PROPOSALS;

    -- ============================================================
    -- STEP 5: Audit log
    -- ============================================================
    BEGIN
        CALL MIP.APP.SP_LOG_EVENT(
            'STRUCTURAL',
            'SP_EXPIRE_STALE_DAILY_PROPOSALS',
            'SUCCESS',
            :v_proposals_expired,
            OBJECT_CONSTRUCT(
                'as_of_date',                  :v_as_of,
                'proposals_expired',           :v_proposals_expired,
                'expired_proposal_ids',        :v_expired_ids,
                'actions_superseded',          :v_actions_superseded,
                'superseded_action_ids',       :v_superseded_action_ids,
                'actions_at_broker_stale',     :v_actions_at_broker_stale,
                'at_broker_action_ids_for_review', :v_at_broker_action_ids,
                'elapsed_sec',                 DATEDIFF('second', :v_run_start, CURRENT_TIMESTAMP())
            ),
            NULL,
            NULL,
            NULL
        );
    EXCEPTION WHEN OTHER THEN
        -- Audit log failure must not block the sweep itself.
        NULL;
    END;

    RETURN OBJECT_CONSTRUCT(
        'status',                          'SUCCESS',
        'as_of_date',                      :v_as_of,
        'proposals_expired',               :v_proposals_expired,
        'expired_proposal_ids',            :v_expired_ids,
        'actions_superseded',              :v_actions_superseded,
        'superseded_action_ids',           :v_superseded_action_ids,
        'actions_at_broker_stale',         :v_actions_at_broker_stale,
        'at_broker_action_ids_for_review', :v_at_broker_action_ids,
        'elapsed_sec',                     DATEDIFF('second', :v_run_start, CURRENT_TIMESTAMP())
    );
END;
$$;
