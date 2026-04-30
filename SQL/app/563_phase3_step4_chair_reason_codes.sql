/*  ================================================================
    563_phase3_step4_chair_reason_codes.sql
    MIP Agentic Proposal Board -- Phase 3 Calibration, Step 4 of 4.

    Additive-only migration: extends MIP.APP.PROPOSAL_BOARD_REASON_CODE
    with the four extra chair codes the Step 4 chair matrix emits in
    addition to the Step 1 catalog (file 562).

    Why these four are needed (each is a path the Step 4 matrix needs
    to be able to emit so the chair reason-code distribution is
    actually diverse):

      WATCH_OPPORTUNITY_NOT_RIPE
        Opportunity verdict is "watch" or "weak" for reasons other
        than the explicit OPPORTUNITY_WAIT_FOR_ENTRY case (e.g.
        TREND_STALE, OPPORTUNITY_WEAK).  Distinct from the legacy
        catch-all WATCHLIST_ONLY, which now only fires for the rare
        STRUCT=reject / HIST=reject paths.

      APPROVED_REDUCED_OPPOSING_SETUP
        OPPOSING_SETUP warning flag fires but specialists are NOT
        weak/ambiguous (per amendment 1).  Demote sizing rather than
        force WATCH.

      APPROVED_REDUCED_REPEATED_REPITCH
        REPEATED_REPITCH warning fires but the compound REJECT_REPEATED_REPITCH
        rule (HIST not OK + OPP not actionable + STRUCT weak/reject)
        does not.  Demote sizing instead of letting the warning have
        zero policy effect.

      APPROVED_REDUCED_NOISY_CHOPPY_PRICE_ACTION
        NOISY_CHOPPY_PRICE_ACTION fires AND the opportunity agent did
        not override it with a verdict of "attractive" (per
        amendment 3).  Caps sizing to acknowledge the noisy regime.

    This file is purely catalog. It does NOT change behaviour on its
    own; the new codes sit unreferenced until the SP_RUN_PROPOSAL_BOARD
    matrix patch in the same Step 4 deployment lands.

    The same MERGE pattern as 560_proposal_board_tables.sql /
    562_phase3_calibration_reason_codes.sql is used so re-runs are
    idempotent and safely upsert IS_ACTIVE = TRUE.

    The new codes are ALSO mirrored into 560_proposal_board_tables.sql
    so a fresh bootstrap of the catalog seeds them too.  The two files
    must stay in lockstep.
    ================================================================ */

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;
USE SCHEMA APP;

MERGE INTO MIP.APP.PROPOSAL_BOARD_REASON_CODE tgt
USING (
    SELECT * FROM VALUES
        ('WATCH_OPPORTUNITY_NOT_RIPE',                    'CHAIR', 'WARN', 'Held as watchlist evidence: opportunity agent verdict is watch or weak for reasons other than waiting-for-entry (e.g. TREND_STALE, OPPORTUNITY_WEAK). Distinct from the catch-all WATCHLIST_ONLY.'),
        ('APPROVED_REDUCED_OPPOSING_SETUP',               'CHAIR', 'INFO', 'Approved at reduced size because the same symbol carries an opposing-direction setup, but specialists do NOT converge on weak / ambiguous, so the chair demotes rather than forcing WATCH.'),
        ('APPROVED_REDUCED_REPEATED_REPITCH',             'CHAIR', 'INFO', 'Approved at reduced size because REPEATED_REPITCH fires but the compound REJECT_REPEATED_REPITCH rule (history not OK and opportunity not actionable and structure weak/reject) does not. Demotion gives the warning real policy effect.'),
        ('APPROVED_REDUCED_NOISY_CHOPPY_PRICE_ACTION',    'CHAIR', 'INFO', 'Approved at reduced size because NOISY_CHOPPY_PRICE_ACTION fires and the opportunity agent did NOT override it with an attractive verdict. Caps sizing to acknowledge the noisy regime per amendment 3.')
    AS v(REASON_CODE, REASON_CATEGORY, SEVERITY, DESCRIPTION)
) src
ON tgt.REASON_CODE = src.REASON_CODE
WHEN MATCHED THEN UPDATE SET
    tgt.REASON_CATEGORY = src.REASON_CATEGORY,
    tgt.SEVERITY        = src.SEVERITY,
    tgt.DESCRIPTION     = src.DESCRIPTION,
    tgt.IS_ACTIVE       = TRUE
WHEN NOT MATCHED THEN INSERT (
    REASON_CODE, REASON_CATEGORY, SEVERITY, DESCRIPTION, IS_ACTIVE
) VALUES (
    src.REASON_CODE, src.REASON_CATEGORY, src.SEVERITY, src.DESCRIPTION, TRUE
);
