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

Apply, in order:

1. `MIP/SQL/app/562_phase3_calibration_reason_codes.sql` — Phase 3 Step 1
   chair / RISK / STRUCTURE / HISTORY / OPPORTUNITY reason-code expansion
   (idempotent MERGE, no behaviour change on its own).
2. `MIP/SQL/app/563_phase3_step4_chair_reason_codes.sql` — Phase 3 Step 4
   additive chair codes (`WATCH_OPPORTUNITY_NOT_RIPE`,
   `APPROVED_REDUCED_OPPOSING_SETUP`, `APPROVED_REDUCED_REPEATED_REPITCH`,
   `APPROVED_REDUCED_NOISY_CHOPPY_PRICE_ACTION`).  Also idempotent;
   ordering matters because the Step 4 chair matrix below references
   these codes.
3. `MIP/SQL/app/561_sp_run_proposal_board.sql` — installs
   `MIP.APP.SP_RUN_PROPOSAL_BOARD` (Phase 3 Step 4 chair matrix +
   re-pitch policy) — the sole selector that writes board runs,
   candidate snapshots, specialist outcomes, orchestrator verdicts,
   final slate, and the published rows in
   `MIP.APP.STRUCTURAL_TRADE_PROPOSALS`.

Note: `560_proposal_board_tables.sql` already mirrors every code from
562 / 563, so a fresh bootstrap of the catalog seeds them too. Keep
560 / 562 / 563 in lockstep when adding chair codes in the future.

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

## 10) Phase 2 Sprint 1 — Cortex-backed specialists

`SP_RUN_PROPOSAL_BOARD` now runs the four specialist agents through
`snowflake.cortex.complete('mistral-large2', prompt)` per candidate.
The deterministic v1 logic remains in the same procedure as a
per-row fallback, used only when the Cortex output is missing or
fails the per-agent verdict / reason-code contract. The chair
(orchestrator), final-slate selection, and publication path are
unchanged from Phase 1 (still deterministic; chair upgrades happen
in Sprint 2).

### Run-mode markers

`MIP.APP.PROPOSAL_BOARD_RUN.MODEL_CONFIG_JSON` for any post-Sprint-1
run carries:
- `mode = 'cortex_specialists_with_deterministic_fallback'`
- `specialists = [STRUCTURE_AGENT, OPPORTUNITY_QUALITY_AGENT, HISTORICAL_EVIDENCE_AGENT, RISK_EXECUTION_FEASIBILITY_AGENT]`
- `specialist_model = 'mistral-large2'`
- `chair_mode = 'deterministic_local_chair'`
- `fallback_policy = 'per_row_deterministic_when_cortex_invalid'`

`PROMPT_VERSION = 'proposal_board_v2_phase2_sprint1'` and
`POLICY_VERSION = 'proposal_board_policy_v2_phase2_sprint1'`.

### Per-row Cortex audit

Every row in `PROPOSAL_BOARD_AGENT_OUTCOME` now carries
`STRUCTURED_OUTPUT_JSON` with these keys:
- `verdict_schema = 'proposal_board_agent_v2_cortex'`
- `mode` either `'cortex'` or `'deterministic_fallback'`
- `model` either `'mistral-large2'` or `'DETERMINISTIC_FALLBACK'`
- `cortex_parse_succeeded` boolean
- `cortex_validation_passed` boolean
- `cortex_raw_text` raw model response
- `cortex_cleaned_text` after fence-stripping
- `cortex_parsed_json` parsed JSON or null
- `concern_flags` includes literal `'MODEL_FALLBACK_USED'` whenever
  the deterministic fallback was used for that row
- `prompt_text` the exact prompt that was sent to the model

This is the audit substrate Sprint 4 calibration views consume.

### Per-agent verdict + reason-code allow-lists

Hard-validated by `SP_RUN_PROPOSAL_BOARD` on every Cortex response.
A response is rejected and the deterministic fallback is used when
either `verdict` or `primary_reason_code` is missing or outside the
allow-list. `secondary_reason_code` is soft-discarded (nulled) when
invalid; `confidence` is soft-defaulted to 0.5 when missing or out
of range.

| Agent | verdict (one of) | primary_reason_code (one of) |
|---|---|---|
| STRUCTURE_AGENT | approve, weak, reject | STRUCTURE_APPROVED, STRUCTURE_NOT_FRESH, STRUCTURE_WEAK, FAMILY_INTERPRETATION_WEAK, CONFLICTING_STRUCTURE |
| OPPORTUNITY_QUALITY_AGENT | attractive, watch, weak | OPPORTUNITY_ATTRACTIVE, OPPORTUNITY_WATCH, PULLBACK_TOO_WEAK, REVERSAL_TOO_EARLY, TREND_STALE, TOO_EXTENDED, NOISY_CHOPPY_PRICE_ACTION |
| HISTORICAL_EVIDENCE_AGENT | evidence_supported, mixed, weak, reject | EVIDENCE_SUPPORTED, EVIDENCE_MIXED, EVIDENCE_WEAK, RECENT_FAILED_SYMBOL, REPEATED_REPITCH, PATH_SURVIVAL_WEAK, SAMPLE_SIZE_LOW |
| RISK_EXECUTION_FEASIBILITY_AGENT | executable, constrained, reject | EXECUTABLE, EXECUTION_CONSTRAINED, EXECUTION_IMPRACTICAL, INVALIDATION_TOO_NEAR, INVALIDATION_TOO_FAR, GAP_RISK_HIGH, SIZE_REDUCE_REQUIRED, SHORT_HISTORY_NOT_OPERATIONAL, DIRECTION_NOT_EXECUTABLE |

### Sprint 1 smoke validation

Run after every board run that is meant to exercise the Cortex
path. All three queries must hold for the run to be considered
clean.

F. Cortex acceptance + run mode:
```
WITH r AS (SELECT '<new run id>' AS RUN_ID)
SELECT
    o.AGENT_NAME,
    o.STRUCTURED_OUTPUT_JSON:mode::STRING AS MODE,
    COUNT(*) AS N
FROM MIP.APP.PROPOSAL_BOARD_AGENT_OUTCOME o, r
WHERE o.RUN_ID = r.RUN_ID
GROUP BY 1, 2
ORDER BY 1, 2;
-- Expected: each agent has MODE in ('cortex', 'deterministic_fallback')
-- and the fallback share is small. A 100% fallback share for a
-- specific agent indicates the prompt or model has drifted.
```

G. Schema drift guard:
```
SELECT COUNT(DISTINCT STRUCTURED_OUTPUT_JSON:verdict_schema::STRING)
FROM MIP.APP.PROPOSAL_BOARD_AGENT_OUTCOME
WHERE RUN_ID = '<new run id>';
-- Expected: 1
```

H. Reason-code validity reuses the existing reason-code guard from
section 7 step C — `PROPOSAL_BOARD_OUTPUT_ERROR` must be empty for
the run regardless of which path produced each row.

### Cost note

Each candidate snapshotted incurs four `snowflake.cortex.complete`
calls per board run. With ~50 candidates/day this is ~200 calls per
run; plan capacity accordingly when scheduling multiple intra-day
board runs (e.g. smoke + production on the same day).

## 11) Phase 2 Sprint 2 — Templated chair rationale

Sprint 2 replaces the Sprint-1 canned chair string with deterministic
templated synthesis. The chair logic for `FINAL_VERDICT` and
`PRIMARY_REASON_CODE` is unchanged; only the audit text fields are
candidate-specific. No model call is made on the chair path —
templated synthesis is cheap and fully audited from the four
specialist outputs already in `PROPOSAL_BOARD_AGENT_OUTCOME`.

### Run-mode markers

`MIP.APP.PROPOSAL_BOARD_RUN.MODEL_CONFIG_JSON.chair_mode` for any
post-Sprint-2 run is `'deterministic_templated_chair'` and
`chair_template_version = 'phase2_sprint2_deterministic_templated_v1'`.

`PROMPT_VERSION = 'proposal_board_v2_phase2_sprint2'` and
`POLICY_VERSION = 'proposal_board_policy_v2_phase2_sprint2'`.

### Per-candidate chair fields

`PROPOSAL_BOARD_ORCHESTRATOR_VERDICT.FINAL_RATIONALE` is a
single-sentence chair synthesis interpolating: symbol, direction,
family, setup_date, status, all four specialist verdicts and
primary reason codes, average confidence,
support/concern/reject counts, the chair's verdict and primary
(plus secondary) reason code, and the warning-flag list. Two
different candidates always produce different rationale strings
(modulo the unlikely case of identical inputs).

`PROPOSAL_BOARD_ORCHESTRATOR_VERDICT.WHY_SELECTED_OR_REJECTED` is
verdict-class differentiated:
- `APPROVE`: states the support/concern/reject counts and that the
  threshold was met (no reject, fewer than two concerns, no
  operational block), and lists the specialist line.
- `APPROVE_REDUCED`: lists which specialists raised concerns with
  their primary reason codes, and notes when sizing is constrained
  because `risk_class IN ('HIGH','GAP_AWARE')`.
- `WATCH`: distinguishes `DIRECTION_NOT_EXECUTABLE` cases (operational
  block) from concern-driven holds.
- `REJECT`: lists which specialists voted reject with their primary
  reason codes, plus the full specialist line.

`PROPOSAL_BOARD_ORCHESTRATOR_VERDICT.COMPARATIVE_REASONING_JSON` now
also carries:
- `specialist_breakdown`: per-agent verdict, primary/secondary reason
  code, confidence, and `mode` (`cortex` or `deterministic_fallback`).
- `chair_template_version`.
- `mode_line`: compact text of the four agent modes for the
  candidate, useful for fallback-tracking dashboards.

`MIP.APP.STRUCTURAL_TRADE_PROPOSALS.BOARD_RATIONALE` continues to be
formed as `'Board rank N | <verdict> | <primary_reason_code> | ' ||
FINAL_RATIONALE`, so the cockpit benefits from per-candidate text
without any UI change.

### Sprint 2 smoke validation

I. Per-candidate uniqueness:
```
SELECT COUNT(DISTINCT FINAL_RATIONALE) AS UNIQUE_RATIONALES,
       COUNT(*) AS TOTAL_VERDICTS
FROM MIP.APP.PROPOSAL_BOARD_ORCHESTRATOR_VERDICT
WHERE RUN_ID = '<new run id>';
-- Expected: UNIQUE_RATIONALES = TOTAL_VERDICTS (every candidate
-- gets its own chair text). A non-equal result indicates the chair
-- has reverted to a templated string that does not interpolate
-- candidate-specific data.
```

J. No canned rationale leaked into a published row:
```
SELECT COUNT(*) AS LEAKED
FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS
WHERE STATUS = 'PROPOSED'
  AND BOARD_RATIONALE LIKE '%Chair synthesized four specialist reviews%';
-- Expected: 0 (only legacy expired rows from Sprint-1 runs may
-- contain the canned text; active rows must use the templated chair.)
```

## 12) Phase 2 Sprint 3 — Cockpit board explanation

Sprint 3 surfaces the per-proposal board audit trail to the cockpit
and LPA Committee 2 Exhibits page. No DB schema change, no new write
paths — purely a read surface over the persistence tables already
populated by Sprint 1 (specialist verdicts) and Sprint 2 (chair).

### New API endpoint

`GET /committee/proposal/{proposal_id}/board-explanation`

Joins `STRUCTURAL_TRADE_PROPOSALS.BOARD_*` lineage to the four board
persistence tables and returns one consolidated payload:
- `run` block: `MODEL_CONFIG_JSON` (mode, specialist_model, chair_mode,
  chair_template_version), `prompt_version`, `policy_version`,
  candidate/final counts, run timestamps.
- `specialists`: per-agent verdict, primary/secondary reason code,
  confidence, rationale, `mode` (`cortex` vs
  `deterministic_fallback`), `concern_flags`, plus the Cortex audit
  fields (`cortex_parse_succeeded`, `cortex_validation_passed`).
- `chair`: `final_verdict`, `primary_reason_code`, `final_rationale`,
  `why_selected_or_rejected`, full `comparative_reasoning_json` (so
  the panel matches what the chair persisted).
- `disagreement`: support / concern / reject counts, avg confidence,
  warning flags, plus any cross-agent records from
  `PROPOSAL_BOARD_INTERACTION`.

Fail-soft contract: HTTP 200 with `{ "available": false, "note": ... }`
when the proposal is missing, has no board lineage (legacy
pre-cutover row), or any of the underlying queries fail.

Backed by [explanation.py](MIP/apps/mip_ui_api/app/services/board/explanation.py),
wired through [committee.py](MIP/apps/mip_ui_api/app/routers/committee.py).

### Cockpit UI component

[BoardExplanationPanel.jsx](MIP/apps/mip_ui_web/src/components/board/BoardExplanationPanel.jsx)
renders inside [LpaCommittee2Exhibits.jsx](MIP/apps/mip_ui_web/src/pages/LpaCommittee2Exhibits.jsx)
between the masthead and the exhibit columns. Disclosure is
**collapsed by default** — the API call is lazy and fires only when
the operator opens the panel, so we don't pay the join cost on
every page load.

The panel is strictly read-only: it shows the four specialist
verdicts (with `cortex` vs `fallback` badges), the disagreement
summary computed by the chair, the chair's templated rationale, and
the run lineage chips (run id, mode, specialist model, chair mode,
prompt/policy version). No operator-facing override of board
verdicts in this sprint.

### Sprint 3 smoke validation

K. End-to-end service smoke (run from project root):
```
python cursorfiles/smoke_board_explanation.py <proposal_id>
```
Expected on a recently-published row: `available=true`, four
`specialists` entries, all four `cortex_validation_passed=true` (or
`mode=deterministic_fallback` for any specialists that failed
validation), chair fields populated.

L. Fail-soft check on a legacy non-board proposal:
```
python cursorfiles/smoke_board_explanation.py <legacy_proposal_id>
-- Expected: available=false, note describes the missing lineage.
```

## 13) Phase 2 Sprint 4 — Lifecycle (option 3a) and same-symbol (option 4a)

Sprint 4 closes two operational gaps surfaced by Phase 1 / Sprint 3:
multiple board runs on the same calendar day used to leave
overlapping `PROPOSED` rows, and the cockpit had no visible signal
that the active slate carried more than one row on the same
symbol.

### Sprint 4a — Same-day board run supersession (option 3a)

`MIP.APP.SP_EXPIRE_STALE_DAILY_PROPOSALS` now enforces a third rule:

> **Rule 3** — Any `STRUCTURAL_TRADE_PROPOSALS` row with
> `STATUS='PROPOSED'` and `BOARD_RUN_ID IS NOT NULL` whose
> `BOARD_RUN_ID` is **not** the latest *authoritative*
> `PROPOSAL_BOARD_RUN.RUN_ID` for the same `AS_OF_DATE` is marked
> `STATUS='EXPIRED'`. Cascades to LIVE_ACTIONS pre-broker open
> states with reason code `'SUPERSEDED_BY_NEWER_BOARD_RUN'`.

"Latest" is the latest *authoritative* board run for the
`AS_OF_DATE`; ties on `STARTED_AT` are resolved by `RUN_ID`
(lexicographic descending) for determinism. `PORTFOLIO_ID` is
intentionally NOT part of the partitioning key — the board
publishes across portfolios but the AS_OF_DATE has one canonical
"latest" board.

#### Authoritative-run guard

A run only qualifies as the "latest authoritative" run when ALL of:

1. `RUN_STATUS = 'COMPLETE'`
   — excludes `RUNNING` and `FAILED`. Both validation failures
   (per `PROPOSAL_BOARD_OUTPUT_ERROR`) and SQL exceptions land in
   `FAILED`.
2. `COALESCE(CANDIDATE_COUNT, 0) > 0`
   — separates the legitimate chair verdict
   `NO_GOOD_IDEAS_TODAY` (board evaluated `N >= 1` candidates and
   rejected all) from the upstream empty-evidence early-exit path
   that emits the **same** reason code with `CANDIDATE_COUNT = 0`.
   `SP_RUN_PROPOSAL_BOARD` writes `NO_GOOD_IDEAS_TODAY` for both
   paths, so `CANDIDATE_COUNT` is the only field that distinguishes
   them.
3. `NOT EXISTS PROPOSAL_BOARD_OUTPUT_ERROR(RUN_ID)`
   — defensive belt-and-suspenders. `SP_RUN_PROPOSAL_BOARD` already
   marks runs as `FAILED` when validation captures rows here, but
   this guard protects Rule 3 against any future SP change that
   would allow partial publication on top of validation errors.

**Net effect:** a broken or empty-evidence board run can never
wipe a healthier predecessor's `PROPOSED` rows on the same
`AS_OF_DATE`. If today's only run is broken, Rule 3 falls through
(no row in `TMP_LATEST_BOARD_RUN_BY_DATE`) and any earlier
authoritative same-day run remains the survivor.

Rule 3 fires *after* Rule 2 (lifecycle) so that rows whose
underlying setup has already moved are correctly attributed to
`expired_proposal_ids_lifecycle` rather than
`expired_proposal_ids_board`. The new SP return / audit log
fields:

```
proposals_expired_board       NUMBER
expired_proposal_ids_board    ARRAY
actions_superseded_board      NUMBER
superseded_action_ids_board   ARRAY
```

Sprint 4a smoke validation:

M. Confirm Rule 3 fires correctly when multiple same-day runs exist:
```
-- 1. Pick a date with >1 COMPLETE board runs
SELECT AS_OF_DATE, COUNT(*) AS RUN_COUNT
FROM MIP.APP.PROPOSAL_BOARD_RUN
WHERE RUN_STATUS = 'COMPLETE'
GROUP BY AS_OF_DATE
HAVING COUNT(*) > 1
ORDER BY AS_OF_DATE DESC;

-- 2. CALL the SP and inspect the new fields
CALL MIP.APP.SP_EXPIRE_STALE_DAILY_PROPOSALS();
-- Expected: proposals_expired_board > 0 covering rows whose
-- BOARD_RUN_ID is not the latest run for their AS_OF_DATE.
```

N. Verify the latest-authoritative-run rows survive (guarded picker):
```
WITH latest AS (
    SELECT AS_OF_DATE, RUN_ID
    FROM (SELECT r.AS_OF_DATE, r.RUN_ID,
                 ROW_NUMBER() OVER (PARTITION BY r.AS_OF_DATE
                                    ORDER BY r.STARTED_AT DESC, r.RUN_ID DESC) AS RN
          FROM MIP.APP.PROPOSAL_BOARD_RUN r
          WHERE r.RUN_STATUS='COMPLETE'
            AND COALESCE(r.CANDIDATE_COUNT, 0) > 0
            AND NOT EXISTS (SELECT 1 FROM MIP.APP.PROPOSAL_BOARD_OUTPUT_ERROR e
                            WHERE e.RUN_ID = r.RUN_ID))
    WHERE RN=1
)
SELECT COUNT(*)
FROM MIP.APP.STRUCTURAL_TRADE_PROPOSALS p
JOIN MIP.APP.PROPOSAL_BOARD_RUN r ON r.RUN_ID = p.BOARD_RUN_ID
LEFT JOIN latest l ON l.AS_OF_DATE = r.AS_OF_DATE
WHERE p.STATUS = 'PROPOSED'
  AND p.BOARD_RUN_ID IS NOT NULL
  AND l.RUN_ID IS NOT NULL
  AND p.BOARD_RUN_ID <> l.RUN_ID;
-- Expected: 0 (any non-latest authoritative board-run rows must be EXPIRED).
```

N2. Confirm the authoritative-run guard hasn't accidentally
    excluded a healthy run (guarded vs unguarded diff):
```
WITH guarded AS (
    SELECT AS_OF_DATE, RUN_ID AS LATEST
    FROM (SELECT r.AS_OF_DATE, r.RUN_ID,
                 ROW_NUMBER() OVER (PARTITION BY r.AS_OF_DATE
                                    ORDER BY r.STARTED_AT DESC, r.RUN_ID DESC) AS RN
          FROM MIP.APP.PROPOSAL_BOARD_RUN r
          WHERE r.RUN_STATUS='COMPLETE'
            AND COALESCE(r.CANDIDATE_COUNT, 0) > 0
            AND NOT EXISTS (SELECT 1 FROM MIP.APP.PROPOSAL_BOARD_OUTPUT_ERROR e
                            WHERE e.RUN_ID = r.RUN_ID))
    WHERE RN=1
),
unguarded AS (
    SELECT AS_OF_DATE, RUN_ID AS LATEST
    FROM (SELECT AS_OF_DATE, RUN_ID,
                 ROW_NUMBER() OVER (PARTITION BY AS_OF_DATE
                                    ORDER BY STARTED_AT DESC, RUN_ID DESC) AS RN
          FROM MIP.APP.PROPOSAL_BOARD_RUN WHERE RUN_STATUS='COMPLETE')
    WHERE RN=1
)
SELECT COALESCE(g.AS_OF_DATE, u.AS_OF_DATE) AS AS_OF_DATE,
       g.LATEST AS GUARDED_LATEST,
       u.LATEST AS UNGUARDED_LATEST,
       CASE WHEN g.LATEST = u.LATEST              THEN 'SAME'
            WHEN g.LATEST IS NULL                  THEN 'GUARD_DROPPED_TO_NONE'
            ELSE                                        'GUARD_PICKED_DIFFERENT'
       END AS DELTA
FROM guarded g
FULL OUTER JOIN unguarded u ON u.AS_OF_DATE = g.AS_OF_DATE
ORDER BY AS_OF_DATE DESC;
-- Expected: every row DELTA='SAME' under healthy ops.
-- DELTA='GUARD_DROPPED_TO_NONE' is *correct* and *expected* on a
-- date whose only COMPLETE run had CANDIDATE_COUNT=0 or output
-- errors; in that case Rule 3 must NOT fire and proposals from
-- earlier same-day runs (if any) must survive.
-- DELTA='GUARD_PICKED_DIFFERENT' is the desirable behaviour when
-- a broken run was the most recent COMPLETE run and an earlier
-- authoritative same-day run is the rightful "latest".
```

### Sprint 4b — Same-symbol policy (option 4a)

The board itself is unchanged — same-symbol candidates remain
allowed (Phase 1 surfaced TGT and CRWD doing this legitimately).
Sprint 4b just makes the multiplicity visible to the operator via
a non-blocking UI badge.

API additions on each cockpit proposal payload (and on
`/committee/proposal/{id}/priority-context`):

```
same_symbol_other_active        BOOLEAN
same_symbol_other_count         NUMBER
same_symbol_other_directions    ARRAY[STRING]   -- de-duped, e.g. ['LONG'] or ['LONG','SHORT']
same_symbol_other_proposal_ids  ARRAY[NUMBER]
```

UI surfaces:
- Cockpit `TradeProposalsPanel`: amber pill `+N on <SYMBOL>` next
  to the stance.
- LPA Committee 2 Exhibits masthead: matching amber pill next to
  the priority pill.

Sprint 4b smoke validation:

O. Helper unit smoke (no DB required):
```
python cursorfiles/smoke_same_symbol.py
-- Expected: PASS — same-symbol map helper produced expected output.
```

P. Verify the API surfaces the flag when same-symbol candidates exist:
```
curl http://localhost:8000/cockpit?portfolio_id=1 | jq '.trade_proposals.proposals[] | select(.same_symbol_other_active==true)'
-- Expected: zero or more rows; each row carries
-- same_symbol_other_count >= 1, same_symbol_other_directions
-- non-empty, same_symbol_other_proposal_ids non-empty.
```

## 14) Phase 2 / item 5 — Calibration analytics

Read-only views that accumulate evidence in the background so
later threshold/prompt revisions can be data-driven (with bumped
`POLICY_VERSION`).

### Views deployed

- `MIP.MART.V_BOARD_VERDICT_OUTCOMES` — base view: one row per
  (board candidate × eval window) joining
  `PROPOSAL_BOARD_FINAL_SLATE` → snapshot → orchestrator verdict
  → `STRUCTURAL_SETUP_OUTCOMES`. Restricted to
  `RUN_STATUS='COMPLETE'` so partial runs cannot pollute calibration.
- `MIP.MART.V_BOARD_CALIBRATION_BY_VERDICT` — aggregates by
  `BOARD_FINAL_VERDICT` × `EVAL_WINDOW` × `BOARD_MODE`. Use to
  detect Cortex regression vs deterministic baseline once enough
  rows accumulate.
- `MIP.MART.V_BOARD_CALIBRATION_BY_PRIMARY_REASON` — aggregates by
  chair `PRIMARY_REASON_CODE` × `BOARD_FINAL_VERDICT` ×
  `EVAL_WINDOW`. Use to detect reason-code drift.
- `MIP.MART.V_BOARD_CORTEX_HEALTH` — per-run, per-agent Cortex
  acceptance (parse %, validation %, fallback %). Sustained
  validation drops are the regression alarm; spot blips are noise.

### Smoke

```
cursorfiles\.venv\Scripts\python.exe cursorfiles/query_snowflake.py -f MIP/SQL/checks/board_verdict_calibration.sql
```

Calibration views expect outcomes; they will return empty rows for
the verdict / reason calibration tables until
`STRUCTURAL_SETUP_OUTCOMES` is populated for the published
candidates. The Cortex health view returns rows immediately
because it reads `PROPOSAL_BOARD_AGENT_OUTCOME` directly.
