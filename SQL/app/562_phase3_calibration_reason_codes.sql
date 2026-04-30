/*  ================================================================
    562_phase3_calibration_reason_codes.sql
    MIP Agentic Proposal Board — Phase 3 Calibration, Step 1 of 4.

    Additive-only migration: extends the canonical
    MIP.APP.PROPOSAL_BOARD_REASON_CODE catalog with the new chair,
    risk, structure, history, and opportunity codes that Phase 3
    Steps 2-4 will start emitting.

    This file is purely catalog. It does NOT change behaviour:
      - SP_RUN_PROPOSAL_BOARD continues to emit only the legacy
        codes that already exist.
      - The chair decision matrix is untouched.
      - The specialist prompts are untouched.

    Deploying this file alone is safe and reversible. The new codes
    sit unreferenced until Steps 2-4 begin populating them.

    The same MERGE pattern as 560_proposal_board_tables.sql is used
    so re-runs are idempotent and safely upsert active=TRUE.

    The new codes are ALSO mirrored into 560_proposal_board_tables.sql
    so a fresh bootstrap of the catalog seeds them too. The two files
    must stay in lockstep.
    ================================================================ */

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;
USE SCHEMA APP;

MERGE INTO MIP.APP.PROPOSAL_BOARD_REASON_CODE tgt
USING (
    SELECT * FROM VALUES
        -- ============================================================
        -- CHAIR primary reason codes (Phase 3 expansion).
        -- The chair will start emitting these in Step 4 once the
        -- decision matrix lands. Until then they sit unused.
        -- ============================================================
        ('APPROVED_CLEAN_LONG_STRUCTURE',          'CHAIR', 'INFO',  'Approved at full size: long with clean structure, attractive opportunity, executable risk, and supportive history. No material warnings.'),
        ('APPROVED_CLEAN_SHORT_STRUCTURE',         'CHAIR', 'INFO',  'Approved at full size: short with clean structure, attractive opportunity, executable risk. Only fires when SHORT_LIVE_ENABLED is true.'),
        ('APPROVED_REDUCED_RISK_CONSTRAINED',      'CHAIR', 'INFO',  'Approved at reduced size because the risk/execution agent flagged the candidate as constrained (e.g. SIZE_REDUCE_REQUIRED, GAP_RISK_HIGH).'),
        ('APPROVED_REDUCED_HISTORY_MIXED',         'CHAIR', 'INFO',  'Approved at reduced size because historical evidence is mixed but structure, opportunity, and risk are otherwise clean.'),
        ('WATCH_SHORT_RESEARCH_ONLY',              'CHAIR', 'WARN',  'Held as research-only watchlist evidence: short candidate surfaced but SHORT_LIVE_ENABLED is false.'),
        ('WATCH_DIRECTION_NOT_EXECUTABLE',         'CHAIR', 'WARN',  'Held as watchlist evidence: candidate direction is not currently live-enabled (non-short cases such as FX without FX_LIVE_ENABLED).'),
        ('WATCH_OPPOSING_SETUP',                   'CHAIR', 'WARN',  'Held as watchlist evidence: same symbol carries an opposing-direction setup AND specialist signals do not converge on approve.'),
        ('WATCH_WAITING_FOR_ENTRY',                'CHAIR', 'WARN',  'Held as watchlist evidence: structure and opportunity look adequate but price is outside the entry zone right now.'),
        ('WATCH_RISK_REWARD_UNATTRACTIVE',         'CHAIR', 'WARN',  'Held as watchlist evidence: tradeable but risk/reward is below threshold per the risk agent.'),
        ('REJECT_REPEATED_REPITCH',                'CHAIR', 'WARN',  'Rejected: same setup has been repeatedly proposed without improvement and opportunity quality is weak.'),
        ('REJECT_STALE_WEAK_STRUCTURE',            'CHAIR', 'WARN',  'Rejected: stale candidate (STALE_BUT_NOT_EXPIRED) combined with weak structure and prior re-pitches.'),
        ('REJECT_RECENT_FAILURE_NO_IMPROVEMENT',   'CHAIR', 'WARN',  'Rejected: recent terminal trade outcome on this symbol AND no measurable improvement in evidence.'),
        ('REJECT_WEAK_OPPORTUNITY',                'CHAIR', 'WARN',  'Rejected: opportunity quality agent emitted a hard-reject verdict (e.g. TOO_EXTENDED, REVERSAL_TOO_EARLY).'),
        ('REJECT_EXECUTION_HARD_BLOCK',            'CHAIR', 'ERROR', 'Rejected: risk/execution agent emitted hard_block (instrument disabled, market halted, or otherwise impossible to execute).'),

        -- ============================================================
        -- RISK_EXECUTION primary reason codes (Phase 3 expansion).
        -- Step 2 will refine the RISK agent vocabulary; these new
        -- codes are the corresponding primary reason codes.
        -- ============================================================
        ('EXECUTION_RESEARCH_ONLY',                'RISK_EXECUTION', 'WARN', 'Risk agent verdict research_only: direction is research-visible but not live-enabled today (the policy case, distinct from execution impossibility).'),
        ('RISK_REWARD_UNATTRACTIVE',               'RISK_EXECUTION', 'WARN', 'Risk agent verdict risk_unattractive: candidate is tradeable but the risk/reward ratio is below threshold (e.g. invalidation too near vs target).'),

        -- ============================================================
        -- STRUCTURE primary reason codes (Phase 3 expansion).
        -- Step 3 will add an "acceptable" verdict between approve
        -- and weak. STRUCTURE_APPROVED keeps its existing meaning;
        -- STRUCTURE_ACCEPTABLE is the new middle band.
        -- ============================================================
        ('STRUCTURE_ACCEPTABLE',                   'STRUCTURE', 'INFO', 'Structure is recognizable and tradeable but not pristine. Confidence and level significance are in the middle band.'),

        -- ============================================================
        -- HISTORY primary reason codes (Phase 3 expansion).
        -- Step 3 will add a "sparse_but_acceptable" verdict so the
        -- agent stops using "mixed" as a non-answer.
        -- ============================================================
        ('EVIDENCE_SPARSE_BUT_ACCEPTABLE',         'HISTORY', 'INFO', 'Historical sample size is sparse (SAMPLE_SIZE_LOW) but the available statistics lean positive, so this is not a non-answer.'),

        -- ============================================================
        -- OPPORTUNITY primary reason codes (Phase 3 expansion).
        -- Step 3 will add "acceptable", "weak", and a wait-for-entry
        -- code so the OPP agent has the granularity to distinguish
        -- "not great" from "not now".
        -- ============================================================
        ('OPPORTUNITY_ACCEPTABLE',                 'OPPORTUNITY', 'INFO', 'Opportunity quality is adequate but not standout. Drives APPROVE_REDUCED rather than APPROVE.'),
        ('OPPORTUNITY_WEAK',                       'OPPORTUNITY', 'WARN', 'Opportunity quality is weak (poor risk/reward, confluence light) but not a hard reject.'),
        ('OPPORTUNITY_WAIT_FOR_ENTRY',             'OPPORTUNITY', 'WARN', 'Opportunity is valid but price is outside or far from the entry zone right now.')

    AS v(REASON_CODE, REASON_CATEGORY, SEVERITY, DESCRIPTION)
) src
ON tgt.REASON_CODE = src.REASON_CODE
WHEN MATCHED THEN UPDATE SET
    tgt.REASON_CATEGORY = src.REASON_CATEGORY,
    tgt.SEVERITY = src.SEVERITY,
    tgt.DESCRIPTION = src.DESCRIPTION,
    tgt.IS_ACTIVE = TRUE
WHEN NOT MATCHED THEN INSERT (
    REASON_CODE, REASON_CATEGORY, SEVERITY, DESCRIPTION, IS_ACTIVE
) VALUES (
    src.REASON_CODE, src.REASON_CATEGORY, src.SEVERITY, src.DESCRIPTION, TRUE
);
