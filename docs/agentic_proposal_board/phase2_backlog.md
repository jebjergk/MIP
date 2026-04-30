# Agentic Proposal Board — Phase 2 Backlog

Phase 1 is **accepted and frozen**. The board is the sole production
selector, lineage is enforced on every active proposal, in-flight
duplicates are blocked at snapshot time, and the cutover runbook
captures the legacy + duplicate-inflight cleanups (`600_*` and
`601_*`).

This file lists the deferred items to take up in Phase 2. They are
intentionally captured as scope, not yet as designs. Do not start any
of them outside of an explicit Phase 2 kickoff.

Phase 1 constraints that remain in force during all Phase 2 work:
- No fallback to the old deterministic selector.
- No UI toggle between "old" and "board".
- No restoration of `structural_priority.py` or downstream
  composite-score recomputation.
- `STRUCTURAL_TRADE_PROPOSALS` remains the single publication target.
  Any new artifact must extend, not bypass, the board persistence
  tables.

---

## 1. Candidate-specific chair rationale

**Status today.** `PROPOSAL_BOARD_ORCHESTRATOR_VERDICT.FINAL_RATIONALE`
is currently a single canned string ("Chair synthesized four specialist
reviews and selected a final board verdict.") for every candidate.
Phase 1 validation (PANW, PYPL, the post-cleanup runs) confirmed the
mechanism works end-to-end but the human-readable rationale is
non-differentiating.

**Phase 2 scope.**
- Replace the canned rationale with a per-candidate synthesis that
  references the actual specialist verdicts, the warning flags that
  were present, and why the chair went `APPROVE` vs `APPROVE_REDUCED`
  vs `WATCH`.
- Decide whether the synthesis is rendered locally in the procedure
  (still deterministic) or by a Cortex chair agent (see item 6).
- Reason codes already cover the structured slot; this item only
  improves the prose slot.

**Touch points.** `MIP/SQL/app/561_sp_run_proposal_board.sql`
(`PROPOSAL_BOARD_ORCHESTRATOR_VERDICT` insert; the
`'Board rank ... | ... | ... | ...'` line for `RATIONALE_TEXT`).

---

## 2. Better board explanation UX in cockpit

**Status today.** Cockpit/API ordering already uses
`BOARD_FINAL_RANK NULLS LAST, CREATED_AT` and surfaces
`BOARD_FINAL_VERDICT`, `BOARD_PRIMARY_REASON_CODE`,
`BOARD_REASON_CODES`, `BOARD_RATIONALE`. There is no UI affordance for:
- expanding into the four specialist verdicts behind a row,
- visualising disagreements (`PROPOSAL_BOARD_INTERACTION` rows),
- showing the chair's structured `COMPARATIVE_REASONING_JSON`
  (support / concern / reject counts, avg confidence, warning flags).

**Phase 2 scope.**
- Design a board-explanation panel in the cockpit that, for any active
  proposal, shows the four specialist verdicts, the disagreement
  summary, the chair's structured comparative reasoning, and links to
  the underlying `BOARD_RUN_ID`.
- Reuse Phase 1 lineage columns plus a thin read-only API on top of
  `PROPOSAL_BOARD_AGENT_OUTCOME`, `_INTERACTION`,
  `_ORCHESTRATOR_VERDICT`, `_FINAL_SLATE`.

**Touch points.** `MIP/apps/mip_ui_api/app/services/cockpit/trade_proposals.py`,
`MIP/apps/mip_ui_api/app/routers/cockpit.py`,
`MIP/apps/mip_ui_api/app/routers/committee.py`, plus a new
`MIP/apps/mip_ui_web` view.

---

## 3. Active proposal lifecycle policy across board runs

**Status today.** Each board run currently dedupes only by
"is there an active `STATUS='PROPOSED'` row for this `SETUP_EVENT_ID`
already, or is there an in-flight live action against it." Stale
`PROPOSED` rows from prior board runs remain `PROPOSED` indefinitely
unless an upstream pipeline expires them or an operator acts on them.
Phase 1 validation surfaced this as an operational shape (e.g. TGT 2004
from board run 2 still active alongside TGT 2015 from board run 3 on a
different setup event).

**Phase 2 scope.**
- Define an explicit lifecycle for `STRUCTURAL_TRADE_PROPOSALS` rows
  written by the board: how long a `PROPOSED` row is allowed to remain
  active before automatic expiry, and under which signals it should be
  marked stale (e.g. setup `EXPIRED`/`INVALIDATED`, age in days, new
  conflicting board verdict on the same setup, etc.).
- Encode the policy inside the board procedure so an operator does not
  have to manually expire rows.
- Audit-log every automatic expiry with a structured reason code, in
  the same shape as the cutover scripts (`600_*`, `601_*`).

**Touch points.** `MIP/SQL/app/561_sp_run_proposal_board.sql`,
possibly a new `MIP/SQL/app/562_sp_age_out_proposals.sql`,
`PROPOSAL_BOARD_REASON_CODE` (new lifecycle codes).

---

## 4. Same-symbol multiple proposal policy or warning

**Status today.** Same-setup-event dedup is enforced. Same-symbol
across-setup-events is **not** dedup'd (intentional in Phase 1, see
post-cleanup validation note on TGT and CRWD). The cockpit currently
shows them as two separate rows with no visual hint that they share a
symbol.

**Phase 2 scope.**
- Decide the policy: allow but visually warn, or actively reduce to
  one slot per symbol at chair time, or allow when setups disagree on
  direction and warn otherwise.
- If "allow + warn", add a board-side warning flag (or a UI-side
  badge) that does not block publication.
- If "reduce to one slot", encode that into the chair's verdict
  selection logic and add a reason code such as
  `SAME_SYMBOL_DEDUP_PREFERRED`.

**Touch points.** Either
`MIP/SQL/views/mart/v_proposal_board_candidate_evidence.sql` (warning
flag) or `561_sp_run_proposal_board.sql` (chair-side dedup) plus
cockpit rendering.

---

## 5. Review APPROVE / APPROVE_REDUCED / WATCH quality against real examples

**Status today.** Verdict distribution from the first three production
board runs was sane in shape (mix of all three classes, no `REJECT`
emissions, no output-validation errors). Whether the boundary between
classes is well-calibrated against actual subsequent outcomes has not
been tested.

**Phase 2 scope.**
- Once enough board runs have accumulated, join
  `PROPOSAL_BOARD_FINAL_SLATE` to subsequent
  `STRUCTURAL_SETUP_OUTCOMES` and live-execution outcomes.
- Compare realised quality across the three verdict classes and
  across primary reason codes.
- Adjust the chair's verdict thresholds and/or specialist verdict
  rules based on the evidence (not on intuition). Any threshold
  change must be reason-coded so the change is auditable in
  `PROPOSAL_BOARD_RUN.POLICY_VERSION`.

**Touch points.** Read-only analytic queries first (likely a new
`MIP/SQL/checks/board_verdict_calibration.sql`), then targeted edits
to `561_sp_run_proposal_board.sql` with a bumped `POLICY_VERSION`.

---

## 6. Cortex-backed specialist upgrade

**Status today.** Specialist outcomes are produced by deterministic
local SQL inside `561_sp_run_proposal_board.sql` (the v1
`mode = deterministic_local_specialists` recorded in
`PROPOSAL_BOARD_RUN.MODEL_CONFIG_JSON`). The board *contract* —
separate roles, structured outputs, validated reason codes,
disagreement logging, chair synthesis — already exists and is enforced.

**Phase 2 scope.**
- Replace the four specialist `INSERT INTO PROPOSAL_BOARD_AGENT_OUTCOME`
  blocks with calls into Snowflake Cortex Agents (or whichever
  provider is operationally stable at that point).
- Keep the schema validation and reason-code validation
  (`PROPOSAL_BOARD_OUTPUT_ERROR`) so a Cortex regression cannot
  silently publish junk.
- Update `PROPOSAL_BOARD_RUN.MODEL_CONFIG_JSON.mode` to
  `cortex_agent_specialists` and bump `PROMPT_VERSION` and
  `POLICY_VERSION`.
- Optionally migrate the chair to Cortex too (related to item 1's
  candidate-specific rationale).

**Touch points.** `MIP/SQL/app/561_sp_run_proposal_board.sql`,
prompt artifacts (new), and possibly an external orchestrator if
Cortex Agents cannot be invoked from inside the procedure body.
