/*  ================================================================
    522_sp_expire_stale_daily_proposals.sql
    MIP Structural Strategy Framework — Daily Proposal Expiry

    Enforces three lifetime rules for STRUCTURAL_TRADE_PROPOSALS:

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

      RULE 3 — Same-day board run supersession (Phase 2 Sprint 4 / 3a):
        Any STRUCTURAL_TRADE_PROPOSALS row with STATUS='PROPOSED'
        AND BOARD_RUN_ID IS NOT NULL AND BOARD_RUN_ID is *not* the
        latest authoritative PROPOSAL_BOARD_RUN.RUN_ID for the same
        AS_OF_DATE is also marked STATUS='EXPIRED'. This closes the
        gap when multiple board runs occur on the same calendar day
        (smoke + production, or a manual re-run). Without it, Rule 1
        doesn't fire (same calendar day) and Rule 2 only fires if
        the underlying setup itself moved — leaving overlapping
        PROPOSED rows from prior same-day runs.
        Reason code on cascade: 'SUPERSEDED_BY_NEWER_BOARD_RUN'.

        AUTHORITATIVE-RUN GUARD (Phase 2 Sprint 4 / 3a safety):
        A run is treated as "latest authoritative" only when ALL of:
          (a) RUN_STATUS = 'COMPLETE'
              (excludes RUNNING and FAILED — both validation
              failures and SQL exceptions land in FAILED).
          (b) CANDIDATE_COUNT > 0
              (distinguishes the legitimate chair verdict
              "NO_GOOD_IDEAS_TODAY" — board evaluated N>0
              candidates and rejected all — from the upstream
              empty-evidence early-exit path that uses the SAME
              reason_code but had ZERO candidates to evaluate.
              See SP_RUN_PROPOSAL_BOARD: when v_candidate_count = 0
              the run is marked COMPLETE with the same reason_code,
              so CANDIDATE_COUNT is the only field that separates
              the two cases).
          (c) No rows in PROPOSAL_BOARD_OUTPUT_ERROR for the run
              (defensive belt-and-suspenders — SP_RUN_PROPOSAL_BOARD
              already marks runs as FAILED when validation populates
              this table, but this guard protects Rule 3 against any
              future SP change that allows partial publication on top
              of validation errors).

        Net effect: a broken or empty-evidence board run can never
        wipe a healthier predecessor's PROPOSED rows on the same
        AS_OF_DATE.

      RULE 0 — Orphan-action cleanup pass (added post-Phase-3 for
        operator safety after the manual transition refresh):
        ANY open-state STRUCTURAL LIVE_ACTION whose linked proposal
        is ALREADY in STATUS='EXPIRED' is marked STATUS='SUPERSEDED'
        with REASON_CODES appended 'PROPOSAL_EXPIRED_AT_PARENT'.
        This catches actions that were created against a proposal
        that has since been expired by ANY path (manual update,
        admin maintenance, future SP, this SP's later rules), not
        just by Rules 1-3 within the current call.

        Without this pass, a one-off manual UPDATE of
        STRUCTURAL_TRADE_PROPOSALS.STATUS to 'EXPIRED' would leave
        in-flight live actions in PENDING_OPEN_VALIDATION pointing
        at a dead proposal — a real operator-safety hole, since the
        cockpit would still surface them as actionable. Running this
        SP now closes the gap on every invocation.

      * Cascades to LIVE_ACTIONS where LIVE_INTENT_KIND='STRUCTURAL'
        and the action is still in an open / pre-broker state
        (PROPOSED, INTENT_APPROVED, PENDING_OPEN_VALIDATION,
        OPEN_BLOCKED). Those are marked STATUS='SUPERSEDED' with
        REASON_CODES appended 'PROPOSAL_EXPIRED_AT_PARENT' (rule 0),
        'OLD_DAILY_PROPOSAL_EXPIRED' (rule 1),
        'UNDERLYING_SETUP_NOT_ELIGIBLE' (rule 2), or
        'SUPERSEDED_BY_NEWER_BOARD_RUN' (rule 3).

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
    v_proposals_expired_board        NUMBER := 0;
    v_actions_superseded             NUMBER := 0;
    v_actions_superseded_lifecycle   NUMBER := 0;
    v_actions_superseded_board       NUMBER := 0;
    v_actions_superseded_orphan      NUMBER := 0;
    v_actions_at_broker_stale        NUMBER := 0;
    v_expired_ids                    ARRAY := ARRAY_CONSTRUCT();
    v_expired_ids_lifecycle          ARRAY := ARRAY_CONSTRUCT();
    v_expired_ids_board              ARRAY := ARRAY_CONSTRUCT();
    v_superseded_action_ids          ARRAY := ARRAY_CONSTRUCT();
    v_superseded_action_ids_lifecycle ARRAY := ARRAY_CONSTRUCT();
    v_superseded_action_ids_board    ARRAY := ARRAY_CONSTRUCT();
    v_superseded_action_ids_orphan   ARRAY := ARRAY_CONSTRUCT();
    v_at_broker_action_ids           ARRAY := ARRAY_CONSTRUCT();
BEGIN

    -- ============================================================
    -- STEP 0 (Rule 0): Orphan-action cleanup.
    --
    -- Catch open-state LIVE_ACTIONs whose linked proposal is
    -- ALREADY in STATUS='EXPIRED' (regardless of whether THIS SP
    -- expired it). Without this pass a one-off manual UPDATE
    -- of STRUCTURAL_TRADE_PROPOSALS.STATUS would leave dangling
    -- pending live actions pointing at dead proposals — a real
    -- operator-safety hole.
    --
    -- We deliberately scope to STRUCTURAL kind and pre-broker
    -- statuses, matching the cascade behaviour of Rules 1-3.
    -- EXECUTION_REQUESTED is excluded (broker race), as elsewhere.
    -- ============================================================
    SELECT ARRAY_AGG(la.ACTION_ID) WITHIN GROUP (ORDER BY la.ACTION_ID)
      INTO :v_superseded_action_ids_orphan
      FROM MIP.LIVE.LIVE_ACTIONS la
      JOIN MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
        ON p.PROPOSAL_ID = la.PROPOSAL_ID
     WHERE la.LIVE_INTENT_KIND = 'STRUCTURAL'
       AND la.STATUS IN (
           'PROPOSED',
           'INTENT_APPROVED',
           'PENDING_OPEN_VALIDATION',
           'OPEN_BLOCKED'
       )
       AND p.STATUS = 'EXPIRED';

    v_superseded_action_ids_orphan := COALESCE(:v_superseded_action_ids_orphan, ARRAY_CONSTRUCT());

    UPDATE MIP.LIVE.LIVE_ACTIONS la
       SET STATUS       = 'SUPERSEDED',
           REASON_CODES = ARRAY_APPEND(
                             COALESCE(la.REASON_CODES, ARRAY_CONSTRUCT()),
                             'PROPOSAL_EXPIRED_AT_PARENT'
                          ),
           UPDATED_AT   = CURRENT_TIMESTAMP()
      FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
     WHERE p.PROPOSAL_ID = la.PROPOSAL_ID
       AND la.LIVE_INTENT_KIND = 'STRUCTURAL'
       AND la.STATUS IN (
           'PROPOSED',
           'INTENT_APPROVED',
           'PENDING_OPEN_VALIDATION',
           'OPEN_BLOCKED'
       )
       AND p.STATUS = 'EXPIRED';
    v_actions_superseded_orphan := SQLROWCOUNT;

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

    -- ============================================================
    -- STEP 6 (Phase 2 Sprint 4 / option 3a):
    -- Same-day board run supersession.
    --
    --   When multiple PROPOSAL_BOARD_RUN rows complete on the same
    --   AS_OF_DATE (smoke + production, manual re-run, etc.), the
    --   prior runs' published proposals are no longer the freshest
    --   board verdict for that day. Without this rule those rows
    --   sit alongside the new rows because Rule 1 only fires across
    --   calendar boundaries, and Rule 2 only fires when the
    --   underlying setup itself moves.
    --
    --   We define "latest" as the latest *authoritative* board run
    --   for the same AS_OF_DATE — see the docstring on Rule 3 for
    --   the full guard. STARTED_AT is the tiebreaker (FINISHED_AT
    --   would also work; STARTED_AT is unambiguous and the procedure
    --   populates STARTED_AT before any row publication).
    --   PORTFOLIO_ID is intentionally not part of the partitioning:
    --   the board can publish across portfolios but each AS_OF_DATE
    --   has one canonical "latest" board.
    --
    --   AUTHORITATIVE-RUN GUARD:
    --     RUN_STATUS = 'COMPLETE'
    --       — excludes RUNNING and FAILED.
    --     COALESCE(CANDIDATE_COUNT, 0) > 0
    --       — separates the legitimate chair verdict
    --         NO_GOOD_IDEAS_TODAY (CANDIDATE_COUNT >= 1, board
    --         actually evaluated and rejected all) from the upstream
    --         empty-evidence early-exit path that emits the SAME
    --         reason_code with CANDIDATE_COUNT = 0.
    --     NOT EXISTS PROPOSAL_BOARD_OUTPUT_ERROR
    --       — defensive: SP_RUN_PROPOSAL_BOARD already flips runs
    --         to FAILED when validation captures rows here, but we
    --         double-check so a future SP change can't quietly let
    --         a half-broken run wipe healthy predecessors.
    -- ============================================================
    CREATE OR REPLACE TEMPORARY TABLE TMP_LATEST_BOARD_RUN_BY_DATE AS
    SELECT
        AS_OF_DATE,
        RUN_ID AS LATEST_RUN_ID
    FROM (
        SELECT
            r.AS_OF_DATE,
            r.RUN_ID,
            ROW_NUMBER() OVER (
                PARTITION BY r.AS_OF_DATE
                ORDER BY r.STARTED_AT DESC, r.RUN_ID DESC
            ) AS RN
        FROM MIP.APP.PROPOSAL_BOARD_RUN r
        WHERE r.RUN_STATUS = 'COMPLETE'
          AND COALESCE(r.CANDIDATE_COUNT, 0) > 0
          AND NOT EXISTS (
                SELECT 1
                  FROM MIP.APP.PROPOSAL_BOARD_OUTPUT_ERROR e
                 WHERE e.RUN_ID = r.RUN_ID
              )
    )
    WHERE RN = 1;

    CREATE OR REPLACE TEMPORARY TABLE TMP_EXPIRED_BOARD AS
    SELECT p.PROPOSAL_ID
      FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
      JOIN MIP.APP.PROPOSAL_BOARD_RUN r
        ON r.RUN_ID = p.BOARD_RUN_ID
      LEFT JOIN TMP_LATEST_BOARD_RUN_BY_DATE l
        ON l.AS_OF_DATE = r.AS_OF_DATE
     WHERE p.STATUS = 'PROPOSED'
       AND p.BOARD_RUN_ID IS NOT NULL
       AND l.LATEST_RUN_ID IS NOT NULL
       AND p.BOARD_RUN_ID <> l.LATEST_RUN_ID
       -- Defensive: don't double-fire on rows already caught by
       -- Rule 1 or Rule 2 (those temp tables were dropped above? -
       -- actually we now drop them after this step).
       AND p.PROPOSAL_ID NOT IN (SELECT PROPOSAL_ID FROM TMP_EXPIRED_PROPOSALS)
       AND p.PROPOSAL_ID NOT IN (SELECT PROPOSAL_ID FROM TMP_EXPIRED_LIFECYCLE);

    SELECT COUNT(*), ARRAY_AGG(PROPOSAL_ID) WITHIN GROUP (ORDER BY PROPOSAL_ID)
      INTO :v_proposals_expired_board, :v_expired_ids_board
      FROM TMP_EXPIRED_BOARD;

    v_expired_ids_board := COALESCE(:v_expired_ids_board, ARRAY_CONSTRUCT());

    IF (v_proposals_expired_board > 0) THEN
        UPDATE MIP.APP.STRUCTURAL_TRADE_PROPOSALS
           SET STATUS = 'EXPIRED'
         WHERE PROPOSAL_ID IN (SELECT PROPOSAL_ID FROM TMP_EXPIRED_BOARD);

        -- Cascade to LIVE_ACTIONS pre-broker open states only.
        SELECT ARRAY_AGG(ACTION_ID) WITHIN GROUP (ORDER BY ACTION_ID)
          INTO :v_superseded_action_ids_board
          FROM MIP.LIVE.LIVE_ACTIONS
         WHERE LIVE_INTENT_KIND = 'STRUCTURAL'
           AND PROPOSAL_ID IN (SELECT PROPOSAL_ID FROM TMP_EXPIRED_BOARD)
           AND STATUS IN (
               'PROPOSED',
               'INTENT_APPROVED',
               'PENDING_OPEN_VALIDATION',
               'OPEN_BLOCKED'
           );

        v_superseded_action_ids_board := COALESCE(:v_superseded_action_ids_board, ARRAY_CONSTRUCT());

        UPDATE MIP.LIVE.LIVE_ACTIONS
           SET STATUS       = 'SUPERSEDED',
               REASON_CODES = ARRAY_APPEND(
                                 COALESCE(REASON_CODES, ARRAY_CONSTRUCT()),
                                 'SUPERSEDED_BY_NEWER_BOARD_RUN'
                              ),
               UPDATED_AT   = CURRENT_TIMESTAMP()
         WHERE LIVE_INTENT_KIND = 'STRUCTURAL'
           AND PROPOSAL_ID IN (SELECT PROPOSAL_ID FROM TMP_EXPIRED_BOARD)
           AND STATUS IN (
               'PROPOSED',
               'INTENT_APPROVED',
               'PENDING_OPEN_VALIDATION',
               'OPEN_BLOCKED'
           );
        v_actions_superseded_board := SQLROWCOUNT;
    END IF;

    DROP TABLE IF EXISTS TMP_EXPIRED_PROPOSALS;
    DROP TABLE IF EXISTS TMP_EXPIRED_LIFECYCLE;
    DROP TABLE IF EXISTS TMP_EXPIRED_BOARD;
    DROP TABLE IF EXISTS TMP_LATEST_BOARD_RUN_BY_DATE;

    -- ============================================================
    -- STEP 7: Audit log
    -- ============================================================
    BEGIN
        CALL MIP.APP.SP_LOG_EVENT(
            'STRUCTURAL',
            'SP_EXPIRE_STALE_DAILY_PROPOSALS',
            'SUCCESS',
            :v_proposals_expired + :v_proposals_expired_lifecycle + :v_proposals_expired_board,
            OBJECT_CONSTRUCT(
                'as_of_date',                       :v_as_of,
                'proposals_expired',                :v_proposals_expired,
                'expired_proposal_ids',             :v_expired_ids,
                'proposals_expired_lifecycle',      :v_proposals_expired_lifecycle,
                'expired_proposal_ids_lifecycle',   :v_expired_ids_lifecycle,
                'proposals_expired_board',          :v_proposals_expired_board,
                'expired_proposal_ids_board',       :v_expired_ids_board,
                'actions_superseded_orphan',        :v_actions_superseded_orphan,
                'superseded_action_ids_orphan',     :v_superseded_action_ids_orphan,
                'actions_superseded',               :v_actions_superseded,
                'superseded_action_ids',            :v_superseded_action_ids,
                'actions_superseded_lifecycle',     :v_actions_superseded_lifecycle,
                'superseded_action_ids_lifecycle',  :v_superseded_action_ids_lifecycle,
                'actions_superseded_board',         :v_actions_superseded_board,
                'superseded_action_ids_board',      :v_superseded_action_ids_board,
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
        'proposals_expired_board',         :v_proposals_expired_board,
        'expired_proposal_ids_board',      :v_expired_ids_board,
        'actions_superseded_orphan',       :v_actions_superseded_orphan,
        'superseded_action_ids_orphan',    :v_superseded_action_ids_orphan,
        'actions_superseded',              :v_actions_superseded,
        'superseded_action_ids',           :v_superseded_action_ids,
        'actions_superseded_lifecycle',    :v_actions_superseded_lifecycle,
        'superseded_action_ids_lifecycle', :v_superseded_action_ids_lifecycle,
        'actions_superseded_board',        :v_actions_superseded_board,
        'superseded_action_ids_board',     :v_superseded_action_ids_board,
        'actions_at_broker_stale',         :v_actions_at_broker_stale,
        'at_broker_action_ids_for_review', :v_at_broker_action_ids,
        'elapsed_sec',                     DATEDIFF('second', :v_run_start, CURRENT_TIMESTAMP())
    );
END;
$$;
