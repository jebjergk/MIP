/*  ================================================================
    522_sp_expire_stale_daily_proposals.sql
    MIP Structural Strategy Framework — Daily Proposal Expiry

    Enforces two lifetime rules for STRUCTURAL_TRADE_PROPOSALS:

      RULE 1 — Daily-bar staleness (Phase 4a):
        Any STRUCTURAL_TRADE_PROPOSALS row with STATUS='PROPOSED'
        whose CREATED_AT::DATE is BEFORE the current as-of date
        is marked STATUS='EXPIRED' (no longer surfaced as actionable
        in V_STRUCTURAL_PROPOSALS_FOR_COMMITTEE).

      RULE 2 — Setup-lifecycle sync (Phase 7 PQI fix Fix 2):
        Any STRUCTURAL_TRADE_PROPOSALS row with STATUS='PROPOSED'
        whose underlying STRUCTURAL_SETUP_EVENTS row is now
        STALE / INVALIDATED / EXPIRED / WAITING, OR no longer
        exists at all (orphan), is also marked STATUS='EXPIRED'.
        This prevents dead/dangling proposals lingering after the
        daily detection re-runs cycle out the underlying event.
        Reason code on cascade: 'UNDERLYING_SETUP_NOT_ELIGIBLE'.

      * Cascades to LIVE_ACTIONS where LIVE_INTENT_KIND='STRUCTURAL'
        and the action is still in an open / pre-broker state
        (PROPOSED, INTENT_APPROVED, PENDING_OPEN_VALIDATION,
        OPEN_BLOCKED). Those are marked STATUS='SUPERSEDED' with
        REASON_CODES appended 'OLD_DAILY_PROPOSAL_EXPIRED' (rule 1)
        or 'UNDERLYING_SETUP_NOT_ELIGIBLE' (rule 2).

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
    v_as_of                          DATE := COALESCE(P_AS_OF_DATE, CURRENT_DATE());
    v_run_start                      TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
    v_proposals_expired              NUMBER := 0;
    v_proposals_expired_lifecycle    NUMBER := 0;
    v_actions_superseded             NUMBER := 0;
    v_actions_superseded_lifecycle   NUMBER := 0;
    v_actions_at_broker_stale        NUMBER := 0;
    v_expired_ids                    ARRAY := ARRAY_CONSTRUCT();
    v_expired_ids_lifecycle          ARRAY := ARRAY_CONSTRUCT();
    v_superseded_action_ids          ARRAY := ARRAY_CONSTRUCT();
    v_superseded_action_ids_lifecycle ARRAY := ARRAY_CONSTRUCT();
    v_at_broker_action_ids           ARRAY := ARRAY_CONSTRUCT();
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

    -- ============================================================
    -- STEP 5: Phase 7 PQI Fix 2 — Setup-lifecycle sync expiry.
    --   Even on the same calendar day, a PROPOSED proposal whose
    --   underlying STRUCTURAL_SETUP_EVENTS row has aged out
    --   (STALE / INVALIDATED / EXPIRED / WAITING) or no longer
    --   exists at all (orphaned by the daily detector re-run that
    --   DELETEs and re-INSERTs setups for SETUP_DATE = AS_OF) should
    --   no longer be surfaced as actionable. This is the structural
    --   guarantee that proposal health tracks setup health.
    -- ============================================================
    CREATE OR REPLACE TEMPORARY TABLE TMP_EXPIRED_LIFECYCLE AS
    SELECT p.PROPOSAL_ID
      FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
      LEFT JOIN MIP.APP.STRUCTURAL_SETUP_EVENTS s
        ON s.SETUP_EVENT_ID = p.SETUP_EVENT_ID
     WHERE p.STATUS = 'PROPOSED'
       AND (
           -- orphaned: underlying setup event no longer exists
           s.SETUP_EVENT_ID IS NULL
           -- or underlying setup is no longer eligible
           OR s.SETUP_STATUS IN ('STALE', 'INVALIDATED', 'EXPIRED', 'WAITING')
       );

    SELECT COUNT(*), ARRAY_AGG(PROPOSAL_ID) WITHIN GROUP (ORDER BY PROPOSAL_ID)
      INTO :v_proposals_expired_lifecycle, :v_expired_ids_lifecycle
      FROM TMP_EXPIRED_LIFECYCLE;

    v_expired_ids_lifecycle := COALESCE(:v_expired_ids_lifecycle, ARRAY_CONSTRUCT());

    IF (v_proposals_expired_lifecycle > 0) THEN
        UPDATE MIP.APP.STRUCTURAL_TRADE_PROPOSALS
           SET STATUS = 'EXPIRED'
         WHERE PROPOSAL_ID IN (SELECT PROPOSAL_ID FROM TMP_EXPIRED_LIFECYCLE);

        -- Cascade to LIVE_ACTIONS pre-broker open states only.
        SELECT ARRAY_AGG(ACTION_ID) WITHIN GROUP (ORDER BY ACTION_ID)
          INTO :v_superseded_action_ids_lifecycle
          FROM MIP.LIVE.LIVE_ACTIONS
         WHERE LIVE_INTENT_KIND = 'STRUCTURAL'
           AND PROPOSAL_ID IN (SELECT PROPOSAL_ID FROM TMP_EXPIRED_LIFECYCLE)
           AND STATUS IN (
               'PROPOSED',
               'INTENT_APPROVED',
               'PENDING_OPEN_VALIDATION',
               'OPEN_BLOCKED'
           );

        v_superseded_action_ids_lifecycle := COALESCE(:v_superseded_action_ids_lifecycle, ARRAY_CONSTRUCT());

        UPDATE MIP.LIVE.LIVE_ACTIONS
           SET STATUS       = 'SUPERSEDED',
               REASON_CODES = ARRAY_APPEND(
                                 COALESCE(REASON_CODES, ARRAY_CONSTRUCT()),
                                 'UNDERLYING_SETUP_NOT_ELIGIBLE'
                              ),
               UPDATED_AT   = CURRENT_TIMESTAMP()
         WHERE LIVE_INTENT_KIND = 'STRUCTURAL'
           AND PROPOSAL_ID IN (SELECT PROPOSAL_ID FROM TMP_EXPIRED_LIFECYCLE)
           AND STATUS IN (
               'PROPOSED',
               'INTENT_APPROVED',
               'PENDING_OPEN_VALIDATION',
               'OPEN_BLOCKED'
           );
        v_actions_superseded_lifecycle := SQLROWCOUNT;
    END IF;

    DROP TABLE IF EXISTS TMP_EXPIRED_PROPOSALS;
    DROP TABLE IF EXISTS TMP_EXPIRED_LIFECYCLE;

    -- ============================================================
    -- STEP 6: Audit log
    -- ============================================================
    BEGIN
        CALL MIP.APP.SP_LOG_EVENT(
            'STRUCTURAL',
            'SP_EXPIRE_STALE_DAILY_PROPOSALS',
            'SUCCESS',
            :v_proposals_expired + :v_proposals_expired_lifecycle,
            OBJECT_CONSTRUCT(
                'as_of_date',                       :v_as_of,
                'proposals_expired',                :v_proposals_expired,
                'expired_proposal_ids',             :v_expired_ids,
                'proposals_expired_lifecycle',      :v_proposals_expired_lifecycle,
                'expired_proposal_ids_lifecycle',   :v_expired_ids_lifecycle,
                'actions_superseded',               :v_actions_superseded,
                'superseded_action_ids',            :v_superseded_action_ids,
                'actions_superseded_lifecycle',     :v_actions_superseded_lifecycle,
                'superseded_action_ids_lifecycle',  :v_superseded_action_ids_lifecycle,
                'actions_at_broker_stale',          :v_actions_at_broker_stale,
                'at_broker_action_ids_for_review',  :v_at_broker_action_ids,
                'elapsed_sec',                      DATEDIFF('second', :v_run_start, CURRENT_TIMESTAMP())
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
        'proposals_expired_lifecycle',     :v_proposals_expired_lifecycle,
        'expired_proposal_ids_lifecycle',  :v_expired_ids_lifecycle,
        'actions_superseded',              :v_actions_superseded,
        'superseded_action_ids',           :v_superseded_action_ids,
        'actions_superseded_lifecycle',    :v_actions_superseded_lifecycle,
        'superseded_action_ids_lifecycle', :v_superseded_action_ids_lifecycle,
        'actions_at_broker_stale',         :v_actions_at_broker_stale,
        'at_broker_action_ids_for_review', :v_at_broker_action_ids,
        'elapsed_sec',                     DATEDIFF('second', :v_run_start, CURRENT_TIMESTAMP())
    );
END;
$$;
