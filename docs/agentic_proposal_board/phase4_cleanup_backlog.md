# Phase 4 Agentic Proposal Board — Cleanup Backlog

These items were surfaced while making short publication safe. They are
**not blockers** for the Phase 4 daily run, but they encode legacy
behavior or terminology that we should retire. Each item is captured as
scope only — do not start without an explicit kickoff.

The "today" snapshot below reflects the state after the
`20260501_ibkr_account_mode.sql` migration and the
`PROPOSAL_BOARD_REVIEW_ELIGIBILITY` rollout.

---

## 1. ADAPTER_MODE / LIVE_EXECUTION_MODE terminology

**Status today.**
- `MIP.LIVE.LIVE_PORTFOLIO_CONFIG.ADAPTER_MODE='LIVE'` means
  *"submit through the IBKR broker submit path"*. It does **not** mean
  "real-money account".
- `ADAPTER_MODE='PAPER'` means *"use the internal MIP placeholder leg"*
  (synthetic / simulated execution). It does **not** mean
  "IBKR paper account".
- The IBKR paper-vs-real distinction now lives on the new
  `IBKR_ACCOUNT_MODE` column added by `20260501_ibkr_account_mode.sql`.
- Phase 4 short publication uses `IBKR_ACCOUNT_MODE = 'PAPER'`, never
  `ADAPTER_MODE`.

**Cleanup scope.**
- Rename / re-document `ADAPTER_MODE` so the values describe the
  *broker submit path* rather than the account mode:
  `ADAPTER_MODE = 'LIVE'` -> `BROKER_SUBMIT_IBKR`,
  `ADAPTER_MODE = 'PAPER'` -> `INTERNAL_PLACEHOLDER`.
- Update `MIP/SQL/app/392_live_execution_foundation.sql`,
  `MIP/SQL/views/app/v_live_actions_with_intent.sql`, the live router
  (`MIP/apps/mip_ui_api/app/routers/live.py`), and the operator UI to
  use the new names. Add `BROKER_SUBMIT_PATH` and `IBKR_ACCOUNT_MODE`
  in user-facing copy.
- Audit references in
  `MIP/docs/MULTI_PORTFOLIO_READINESS.md`,
  `MIP/docs/05_DEVELOPER_ONBOARDING.md`, and the cutover runbooks.
- Owner: TBD.

---

## 2. Idempotent replay returning `PAPER_PLACEHOLDER`

**Status today.**
- The live router has an idempotent-replay branch that, for actions
  originally submitted via IBKR, can return `mode='PAPER_PLACEHOLDER'`
  on replay. This is misleading — the original submission path was
  IBKR, not the placeholder.
- See `MIP/apps/mip_ui_api/app/routers/live.py` and the
  `BROKER_SNAPSHOTS` reconciliation. The replay metadata is what
  surfaces in the UI / audit rows.

**Cleanup scope.**
- Replace the `PAPER_PLACEHOLDER` replay label with a more honest
  status (e.g. `IBKR_REPLAY` when the original action was submitted
  via IBKR, `INTERNAL_PLACEHOLDER_REPLAY` only for actions that
  truly used the placeholder leg).
- Backfill the audit log so replay rows produced before the fix have
  a clear flag indicating this is a *display* artifact, not a real
  placeholder execution.
- Owner: TBD.

---

## 3. Audit `LIVE_EXECUTION_MODE=IBKR` env override

**Status today.**
- The live router supports a `LIVE_EXECUTION_MODE=IBKR` environment
  override. When set, it routes orders to IBKR even if Snowflake's
  `ADAPTER_MODE` says `'PAPER'`.
- That made sense when the live path was being bootstrapped, but it
  is dangerous now: an operator could enable real-broker submission
  by setting an env var on the API host without changing any DB row.

**Cleanup scope.**
- Decide whether the env override is still needed. If yes, gate it
  on `IBKR_ACCOUNT_MODE = 'PAPER'` *and* a startup log line that
  records the override and the resolved IBKR account ID.
- If no, remove the override and require all live submissions to be
  driven by Snowflake config only.
- Add a startup self-check that loudly fails if `IBKR_ACCOUNT_MODE`
  for the configured portfolio is `REAL` or `UNKNOWN` while broker
  submit is enabled.
- Owner: TBD.

---

## 4. Visible UI / account banner

**Status today.**
- The UI does not display the resolved broker submit path or the IBKR
  account mode. Operators have to infer it from logs.

**Cleanup scope.**
- Add a banner / status pill to the live UI that shows:
  - `Broker submit path: IBKR | Internal placeholder`
  - `IBKR account mode: PAPER | REAL | UNKNOWN`
  - `IBKR account id: <masked>`
  - `Internal placeholder mode: true / false`
- Source the values from a single API endpoint that joins
  `MIP.LIVE.LIVE_PORTFOLIO_CONFIG` and the live execution router state.
- Owner: TBD.

---

## 5. Tighten eligibility thresholds based on telemetry

**Status today.**
- The new `PROPOSAL_BOARD_REVIEW_ELIGIBILITY` filter ships with
  conservative thresholds (10-day setup window, 14-day state-change
  window, 3% near-level distance). Initial estimates show only a
  small fraction of universes get skipped.
- We deliberately err on the side of including symbols so we don't
  hide regressions early in the rollout.
- 2026-05-01 run `55c576c8-...` confirmed 86/86 dossiers eligible
  (skipped=0). On the 117-min wall-clock cost of that run, this is
  the single biggest runtime lever still on the table.

**Cleanup scope.**
- After a few weeks of data, review the
  `PROPOSAL_BOARD_REVIEW_ELIGIBILITY` log alongside actual chair
  outcomes for skipped symbols (sampled run with
  `--symbols ELIG_OVERRIDE_SYMBOL_FILTER` for spot checks).
- Tighten thresholds and/or add new include reasons (e.g.
  proximity-to-prior-trade exit) once we have evidence.
- Owner: TBD.

---

## 6. Orchestrator does not record CANDIDATE_COUNT on PROPOSAL_BOARD_RUN

**Status today.**
- `MIP.scripts.proposal_board_phase4.orchestrator` writes
  `FINAL_PROPOSAL_COUNT` correctly on the `PROPOSAL_BOARD_RUN` row
  but leaves `CANDIDATE_COUNT = 0` (or NULL) at finalization. Confirmed
  on run `55c576c8-ab83-45a7-9884-b00a84016c7c` (2026-05-01): 86
  dossiers evaluated, `CANDIDATE_COUNT = 0` until manually backfilled.
- This silently disables `MIP.MART.V_LATEST_AUTHORITATIVE_BOARD_RUN`
  for the run, because the view's authoritative-gate clause
  `COALESCE(r.CANDIDATE_COUNT, 0) > 0` is meant to distinguish the
  legitimate `NO_GOOD_IDEAS_TODAY` outcome from an empty-evidence
  early exit.
- Effect: a successful run can produce `STATUS_TRADE_PROPOSALS` rows
  that never reach Cockpit / LPA because the run does not become
  authoritative.

**Cleanup scope.**
- In `orchestrator.py`, set `CANDIDATE_COUNT` at the same place the
  dossier list is finalized (post-eligibility, pre-specialist). The
  natural value is the number of eligible dossiers passed to the
  multi-agent loop (matches `eligible_count` printed by
  `run_board.py`).
- Add a final `UPDATE PROPOSAL_BOARD_RUN SET CANDIDATE_COUNT = ...`
  in the run-finalize block as defensive belt-and-suspenders.
- Add a dedicated assertion to
  `MIP/SQL/smoke/phase4_eligibility_short_gate_smoke.sql`: any run
  in the last 7 days with `RUN_STATUS = 'COMPLETE'` and
  `FINAL_PROPOSAL_COUNT > 0` must have `CANDIDATE_COUNT > 0`.
- Owner: TBD.

---

## 7. PARTIAL_FAILURE handling for specialist JSON-parse errors

**Status today.**
- When a specialist Cortex Agent returns malformed JSON, the
  orchestrator marks the dossier `INVALID` and inserts a row into
  `MIP.APP.PROPOSAL_BOARD_OUTPUT_ERROR`. The presence of any error
  row makes `V_LATEST_AUTHORITATIVE_BOARD_RUN` reject the run, even
  if dozens of OTHER dossiers were validated and published cleanly.
- The 2026-05-01 run had 17 such error rows on 16 invalid dossiers
  while still publishing 4 valid `PROPOSE_LONG` proposals. Resulted
  in `RUN_STATUS = 'PARTIAL_FAILURE'` and required a manual
  `MIP/SQL/scripts/promote_partial_failure_run_55c576c8.sql`
  promotion to expose the proposals to the cockpit.

**Cleanup scope.**
- Decide on the right semantics:
  - Tolerant mode: treat `PARTIAL_FAILURE` as authoritative when at
    least one chair verdict produced a `PUBLISHED` row, and move
    error rows to a separate `PROPOSAL_BOARD_OUTPUT_ERROR_QUARANTINE`
    table that does NOT block the authoritative view.
  - Strict mode (current): keep PARTIAL_FAILURE blocking but make
    operators rerun on the failed subset to recover.
- Make specialist JSON parsing more forgiving: a single agent's
  malformed output should mark only that role as invalid for the
  dossier, not invalidate the entire dossier.
- Add automatic retry for `MISSING_OR_NON_OBJECT_JSON` (small N
  attempts, then fall through).
- Owner: TBD.
