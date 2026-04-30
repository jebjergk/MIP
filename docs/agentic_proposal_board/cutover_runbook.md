# Agentic Proposal Board — Cutover Runbook

This runbook describes the operational steps required to deploy the
Agentic Proposal Board as the **sole** production selector for
structural trade proposals. There is no fallback to the old
deterministic selector and no UI toggle.

It must be followed in order. Step 5 is the **mandatory cleanup** step
that resolves any active legacy proposal rows left behind by the old
selector at the moment of cutover. Step 5b is a follow-up safety
sweep that handles a subtle edge case the legacy cleanup can expose
(duplicate board proposals against setup events whose downstream live
actions are still in flight).

---

## 0) Pre-flight

- Confirm the live structural pipeline is paused (no `SP_PROPOSE_STRUCTURAL_TRADES`
  call is currently in flight).
- Confirm working directory points at the `MIP/` repo root and the
  `cursorfiles/.venv` Python is available for running deploy/smoke
  scripts.
- Snapshot row counts for sanity:
  - `MIP.APP.STRUCTURAL_TRADE_PROPOSALS` total and `STATUS='PROPOSED'`.
  - `MIP.APP.PROPOSAL_BOARD_RUN`, `*_CANDIDATE_SNAPSHOT`,
    `*_AGENT_OUTCOME`, `*_INTERACTION`, `*_ORCHESTRATOR_VERDICT`,
    `*_FINAL_SLATE`, `*_OUTPUT_ERROR`, `*_REASON_CODE` counts.

## 1) Deploy evidence contract

Apply `MIP/SQL/views/mart/v_proposal_board_candidate_evidence.sql`.
This is the single deterministic source of truth for board candidate
snapshots; downstream board logic must not bypass it.

## 2) Deploy board persistence schema

Apply `MIP/SQL/app/560_proposal_board_tables.sql`. This script:
- Creates the seven board persistence tables and the
  `PROPOSAL_BOARD_REASON_CODE` and `PROPOSAL_BOARD_OUTPUT_ERROR` tables.
- Adds the `BOARD_*` lineage columns to
  `MIP.APP.STRUCTURAL_TRADE_PROPOSALS`.
- Seeds the active reason code catalog.

## 3) Deploy the board procedure

Apply `MIP/SQL/app/561_sp_run_proposal_board.sql`. This installs
`MIP.APP.SP_RUN_PROPOSAL_BOARD` — the sole selector that writes board
runs, candidate snapshots, specialist outcomes, orchestrator verdicts,
final slate, and the published rows in
`MIP.APP.STRUCTURAL_TRADE_PROPOSALS`.

## 4) Replace the legacy entrypoint

Apply `MIP/SQL/app/520_sp_propose_structural_trades.sql` — this is now
a compatibility shim that delegates to `SP_RUN_PROPOSAL_BOARD`. **Do
not** revert this stored procedure to its pre-board body. There is no
shadow path or bake-off.

## 5) **Mandatory** legacy proposal cleanup

After the board is deployed but before the first production run, retire
any remaining active rows in `STRUCTURAL_TRADE_PROPOSALS` that were
created by the old selector. Active in this context means
`STATUS='PROPOSED'` AND `BOARD_RUN_ID IS NULL`.

Reason this is mandatory:
- Any such row violates the board contract that every active proposal
  carries full board lineage.
- The board's snapshot writer dedupes against active
  `STRUCTURAL_TRADE_PROPOSALS` rows by `SETUP_EVENT_ID`. A leftover
  legacy row will block its underlying setup event from being
  re-snapshotted by the board, masquerading as a "missing" candidate.
- The cockpit/UI orders by `BOARD_FINAL_RANK NULLS LAST`, so legacy
  rows render below board-published rows without a board rationale,
  visibly polluting the active slate.

Run:

```
cursorfiles\.venv\Scripts\python.exe cursorfiles/query_snowflake.py \
    -f MIP/SQL/scripts/600_retire_pre_board_cutover_proposals.sql --json
```

What the script does (see file header for full contract):
- Pre-check report listing `PROPOSAL_ID`s and symbols that match the
  retire predicate.
- Single Snowflake Scripting block that:
  - moves matching rows from `STATUS='PROPOSED'` to `STATUS='EXPIRED'`;
  - sets `BOARD_RATIONALE` to a human-readable explanation prefixed
    with reason code `RETIRED_PRE_BOARD_CUTOVER_NO_BOARD_LINEAGE`;
  - augments `COMMITTEE_PAYLOAD` additively via `OBJECT_INSERT` with a
    `cutover_retired` key (existing payload keys are preserved
    verbatim — no overwrite of `setup_family`, `composite_score`,
    `gap_risk`, etc.);
  - writes a single `MIP.APP.MIP_AUDIT_LOG` row with
    `EVENT_NAME='RETIRE_PRE_BOARD_CUTOVER_PROPOSALS'`.
- Post-check 1: `STATUS='PROPOSED'` lineage check. All `MISSING_*`
  counters must be `0`.
- Post-check 2: list of remaining active rows. Only board-published
  proposals must remain.
- Post-check 3: detail of retired rows confirming
  `STATUS='EXPIRED'`, populated `BOARD_RATIONALE`, and that
  pre-existing payload keys still resolve.

What the script does **not** do:
- Does not delete any historical rows.
- Does not modify `PROPOSAL_ID`, `SETUP_EVENT_ID`, `SYMBOL`,
  `DIRECTION`, `SETUP_FAMILY`, `ENTRY_*`, `INVALIDATION_*`,
  `RATIONALE_TEXT`, or `CREATED_AT`.
- Does not touch `MIP.LIVE.LIVE_ACTIONS`. In-flight live actions
  remain in their current state and the board's independent
  `LIVE_ACTIONS` dedup guard continues to protect against re-pitching.

The script is idempotent. Re-running it once all stale rows are already
`EXPIRED` is a no-op (zero rows match the predicate; an audit row with
`ROWS_AFFECTED=0` is still written, which is intentional).

## 5b) **Mandatory** duplicate-inflight follow-up sweep

The dedup guard inside `SP_RUN_PROPOSAL_BOARD` historically used
`COALESCE(la.PORTFOLIO_ID, -1) = COALESCE(:P_PORTFOLIO_ID, -1)` to match
in-flight live actions to the board run's portfolio scope. That
expression fails open when the procedure is called with
`P_PORTFOLIO_ID = NULL` ("all portfolios" mode) but the live actions
carry a concrete portfolio id (which is the production case). Before
script 600 ran, the broken live-actions guard was masked by the
active-proposal guard catching the same setups; once script 600 moved
those proposals to `EXPIRED`, the next board run could publish
duplicate proposals against setup events whose live actions were
already executing.

The procedure has been patched (see step 3 — `SP_RUN_PROPOSAL_BOARD`
now uses NULL-aware `( :P_PORTFOLIO_ID IS NULL OR ... )` semantics for
both the active-proposal guard and the live-actions guard). On any
historical environment that ran an older version of the procedure
between steps 5 and 6, the residual duplicates must be retired before
the next operator action can spawn a second live action on the same
setup.

Run:

```
cursorfiles\.venv\Scripts\python.exe cursorfiles/query_snowflake.py \
    -f MIP/SQL/scripts/601_retire_duplicate_inflight_board_proposals.sql --json
```

What the script does (see file header for full contract):
- Pre-check report listing `PROPOSAL_ID`s and the older
  `BLOCKING_PROPOSAL_ID` whose live action exposed the duplicate.
- Single Snowflake Scripting block that:
  - moves matching board rows from `STATUS='PROPOSED'` to
    `STATUS='EXPIRED'`;
  - **appends** to `BOARD_RATIONALE` (does not overwrite the chair's
    original rationale) with reason code
    `RETIRED_DUPLICATE_OF_INFLIGHT_LIVE_ACTION`;
  - augments `COMMITTEE_PAYLOAD` additively via `OBJECT_INSERT` with a
    `duplicate_inflight_retired` key (existing payload keys
    untouched);
  - writes a single `MIP.APP.MIP_AUDIT_LOG` row with
    `EVENT_NAME='RETIRE_DUPLICATE_INFLIGHT_BOARD_PROPOSALS'`.
- Post-check 1: `STATUS='PROPOSED'` lineage check. All `MISSING_*`
  counters must be `0`.
- Post-check 2: residual duplicate count must be `0`.
- Post-check 3: list of remaining active rows.
- Post-check 4: detail of retired duplicate rows confirming preserved
  board lineage (`BOARD_RUN_ID`, `BOARD_FINAL_RANK`,
  `BOARD_FINAL_VERDICT`, `BOARD_PRIMARY_REASON_CODE`).

What the script does **not** do:
- Does not modify the board lineage columns. The retired row keeps a
  full audit trail of the chair's original verdict.
- Does not touch `MIP.LIVE.LIVE_ACTIONS` or the older proposal that
  owns the in-flight execution.

The script is idempotent. On a clean environment (i.e. one whose first
production board run already used the patched procedure) it will find
zero matching rows and exit as a no-op.

## 6) First production board run

Run the board with the **production** proposal cap, not the smoke cap:

```
cursorfiles\.venv\Scripts\python.exe cursorfiles/query_snowflake.py \
    -q "CALL MIP.APP.SP_RUN_PROPOSAL_BOARD(NULL, 8, NULL, 7)"
```

(Pass the production cap your environment uses; `8` is the current
default. Confirm the scheduler invocation passes the same value.)

## 7) Post-run validation

Run, in order:

A. Active proposal lineage:
```
SELECT COUNT(*) AS ACTIVE_PROPOSED,
       COUNT_IF(BOARD_RUN_ID IS NULL) AS MISSING_RUN_ID,
       COUNT_IF(BOARD_CANDIDATE_ID IS NULL) AS MISSING_CANDIDATE_ID,
       COUNT_IF(BOARD_FINAL_RANK IS NULL) AS MISSING_RANK,
       COUNT_IF(BOARD_FINAL_VERDICT IS NULL) AS MISSING_VERDICT,
       COUNT_IF(BOARD_PRIMARY_REASON_CODE IS NULL) AS MISSING_REASON
FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS
WHERE STATUS = 'PROPOSED';
-- Expected: MISSING_* all = 0
```

B. Remaining active proposals:
```
SELECT PROPOSAL_ID, SYMBOL, STATUS, BOARD_RUN_ID, BOARD_FINAL_RANK, BOARD_FINAL_VERDICT
FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS
WHERE STATUS = 'PROPOSED'
ORDER BY BOARD_FINAL_RANK, CREATED_AT;
-- Expected: only board-published proposals
```

C. Output errors must be empty for the new run:
```
SELECT COUNT(*) FROM MIP.APP.PROPOSAL_BOARD_OUTPUT_ERROR
WHERE RUN_ID = '<new run id>';
-- Expected: 0
```

D. Previously blocked setup events:
- For every `SETUP_EVENT_ID` that was retired in step 5, confirm it is
  now either:
  1. Re-snapshotted in
     `MIP.APP.PROPOSAL_BOARD_CANDIDATE_SNAPSHOT` for the new run, or
  2. Correctly skipped because it still has an in-flight
     `MIP.LIVE.LIVE_ACTIONS` row (`PROPOSED |
     INTENT_APPROVED | PENDING_OPEN_VALIDATION | OPEN_BLOCKED |
     REVALIDATED_PASS | EXECUTION_REQUESTED`).
- A retired setup event must not appear in
  `STRUCTURAL_TRADE_PROPOSALS` with `STATUS='PROPOSED'` again unless
  it carries fresh board lineage from the new run.

E. Duplicate-vs-in-flight invariant (must be 0 after step 5b and on
every subsequent run):
```
SELECT COUNT(*) AS RESIDUAL_DUPLICATES
FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
WHERE p.STATUS = 'PROPOSED'
  AND EXISTS (
      SELECT 1
      FROM MIP.LIVE.LIVE_ACTIONS la
      JOIN MIP.APP.STRUCTURAL_TRADE_PROPOSALS op
        ON op.PROPOSAL_ID = la.PROPOSAL_ID
      WHERE la.LIVE_INTENT_KIND = 'STRUCTURAL'
        AND op.SETUP_EVENT_ID = p.SETUP_EVENT_ID
        AND op.PROPOSAL_ID <> p.PROPOSAL_ID
        AND la.STATUS IN ('PROPOSED','INTENT_APPROVED','PENDING_OPEN_VALIDATION',
                          'OPEN_BLOCKED','REVALIDATED_PASS','EXECUTION_REQUESTED')
  );
-- Expected: 0
```
A non-zero result means the patched dedup did not catch a setup that
was already being acted on. Investigate before promoting the run.

## 8) Rollback procedure

There is no runtime fallback to the old selector. Rollback means:
- Re-deploy the previous body of `SP_PROPOSE_STRUCTURAL_TRADES` from
  Git history.
- The retired rows from step 5 remain `EXPIRED` with their cutover
  marker intact; nothing about the cleanup is reversed automatically.
- If the operator needs the retired rows back as `PROPOSED` (not
  recommended), this must be done explicitly by hand and audited
  separately. The cleanup script does **not** provide an "undo".

## 9) Forbidden post-cutover actions

- Do not re-introduce any deterministic composite recomputation
  (`structural_priority.py`, `rank_proposals`, `composite_score`
  recompute) anywhere in the API or SQL.
- Do not bypass `SP_RUN_PROPOSAL_BOARD` to write directly into
  `MIP.APP.STRUCTURAL_TRADE_PROPOSALS` with `STATUS='PROPOSED'`.
- Do not add a UI toggle, feature flag, or environment switch that
  selects between "old selector" and "board".
- Do not delete board persistence rows during normal operation. They
  are the audit substrate for every published proposal.
