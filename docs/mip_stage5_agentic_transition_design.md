# Stage 5 — Full Agentic Transition Design

**Status:** Phase 5A completed. **Phase 5B implemented and smoke-verified.** Phase 5C / Phase 6 design notes below; not yet implemented.

**Goal:** Fully retire deterministic Committee 2.0 from the operator / LPA / Submit
path. Deterministic hearing data may remain queryable for historical inspection,
but it must not be (re-)computed on every revalidation, and must not be a runtime
dependency for any operator-path code.

This builds on Stage 4d (submit gate), Stage 4e (materializer decoupling), and
Stage 4f (no fallback). Those stages already prove deterministic C2 cannot
write to `LIVE_ACTIONS` from the LPA / operator path. Stage 5 goes further:
**stop *running* deterministic C2 on revalidation, and rename the agentic board
to reflect that it is the only authoritative review surface.**

---

## Phase 5A — Discovery + UI rename (COMPLETED)

### 5A.1 — Downstream reader audit

Comprehensive read-only audit of every reader of:

- `MIP.APP.COMMITTEE_FINAL_DECISION` (CFD)
- `MIP.LIVE.COMMITTEE_VERDICT` (CV)
- `MIP.APP.COMMITTEE_HEARING` (CH)

across `MIP/apps/mip_ui_api/`, `MIP/SQL/`, `MIP/apps/mip_ui_web/src/`, and
`cursorfiles/`. Result: **~22 distinct OPERATOR_PATH_LIVE readers** still
depend on these tables.

**Top-5 highest-risk readers (must be migrated before stopping orchestrate writes):**

1. **`commit_operator_authority_for_session`** — `MIP/apps/mip_ui_api/app/committee/agentic_authority.py:1069–1100`. The agentic operator commit reads `COMMITTEE_FINAL_DECISION.HEARING_ID` and `COMMITTEE_HEARING.EVIDENCE_PACK_HASH`. Without fresh orchestrate writes, the operator's own "Apply Agentic Review" returns `SKIP_ACTION_HEARING_MISSING` / `SKIP_EVIDENCE_PACK_STALE`. **This is the critical Stage 5 choke point** — the agentic system literally depends on the deterministic tables for its own anchor.
2. **`_materialize_structural_entry_agentic_apply`** — `live.py:10475`. Reads CFD `DECISION_JSON.joint_decision` for TP/SL / bracket sizing baselines, even in the agentic path.
3. **`orchestrate_committee2_structural_entry`** — `live.py:11271–11344` + `committee.py:554–850`. The "Run Intelligence Review" button path. Refreshes CH, commits CFD, kicks off shadow board.
4. **LPA cockpit pending-decisions builder** — `live.py:7678–7838`. Joins CV for sizing preview / bracket display.
5. **`_load_executable_entry_bracket_for_action` / `execute_live_action`** — `live.py:2002–2014`, `:13305`. At IBKR submit, reads CV `VERDICT_JSON:verdict:joint_decision` for live TP/SL.

The full classified table lives in this design doc's working notes and in the
explore-subagent audit returned during Stage 5A (see chat transcript).

**Disposition counts:**

| Classification | Count | Stage-5 action |
|---|---|---|
| `OPERATOR_PATH_LIVE` | ~22 | RETIRE or MIGRATE_TO_AGENTIC (Phase 5B) |
| `INTERNAL_HELPER` | 6 | RETIRE from operator chain; may remain for audit |
| `HISTORICAL_AUDIT` | 11 | KEEP_AUDIT_ONLY |
| `DOWNSTREAM_NIGHTLY` | 2 | KEEP_AUDIT_ONLY (accept stale baseline) |
| `TRAINING_DATA` | 4 | KEEP_AUDIT_ONLY |
| `TESTING` | 9 | update fixtures as needed |

### 5A.2 — UI rename + relabel (COMPLETED)

User-facing strings migrated from "Shadow Board / Shadow Chair / Deterministic
Baseline" → "Agentic Committee / Evidence Snapshot". Touched files:

- `MIP/apps/mip_ui_web/src/pages/LpaCommittee2Exhibits.jsx`
- `MIP/apps/mip_ui_web/src/pages/LivePortfolioActivity.jsx`
- `MIP/apps/mip_ui_web/src/components/committee/ShadowBoardPanel.jsx`
- `MIP/apps/mip_ui_web/src/pages/StructuralCommitteeHearing.jsx`
- `MIP/apps/mip_ui_web/src/pages/CommitteePerformance.jsx` (reframed as historical analytics)
- `MIP/apps/mip_ui_api/app/routers/live.py` (one operator-path status hint)

Notes:

- **Internal variable names left as-is** for now (`shadowBoardByAction`,
  `ShadowBoardPanel`, `/shadow-board` route, `shadow_session_id`, etc.).
  Renaming these is mechanically simple but high-blast-radius; it can be
  done in a dedicated cleanup pass after Phase 5B / 5C land.
- **CSS class names left as-is** (`lpa-c2-dual-shadow`, `sbp-*`). Same reason.
- **`/structural-committee/{hearingId}` route + page title "Hearing Replay —
  Deterministic Baseline"** intentionally kept — that page is the historical
  inspection surface, and the label correctly describes the historical data
  it shows.
- **`CommitteePerformance` bake-off page** kept but reframed at the top as
  "historical analytics from the pre-Stage-4 era". The `REAL_*` / `SHADOW_*`
  column labels remain because they map to Snowflake columns.

---

## Phase 5B — Decouple agentic anchors from deterministic tables (IMPLEMENTED)

**Verdict: deterministic C2 is no longer a runtime dependency of the agentic
operator path when `AGENTIC_PRIMARY_MATERIALIZATION_ENABLED = true`.**

### As-built design summary

The discovery phase revealed two key facts that simplified the implementation:

1. The "deterministic chair" is not an LLM call — `compute_hearing_bundle` is
   ~470 lines of pure deterministic Python (geometry, regime, path metrics →
   rule-based stance + confidence). Removing it is therefore a code-architecture
   decision (don't write its verdict to DB / don't depend on it), not an LLM-cost
   decision.
2. The Agentic Committee's `build_shadow_evidence_pack` already **explicitly
   excludes** chair verdict fields (`STANCE`, `CONFIDENCE`, `CHAIR_OUTPUT_JSON`,
   role `stance_badge` / `one_liner` / `influence`) per its own docstring. It
   only reads `EVIDENCE_JSON`, `DELTAS_JSON.categories` (drift buckets, not
   verdicts), artifact `KIND`, and role `evidence_refs`. So Phase 5B can write
   a CH row with chair fields NULL'd and the agentic board still has every
   input it needs.

This made the schema migration unnecessary: `AGENTIC_REVALIDATION_AUTHORITY`
already carries `HEARING_ID`, `SHADOW_SESSION_ID`, `PACK_VERSION`, and
`AUTHORITY_PAYLOAD_JSON`, while `SHADOW_BOARD_SESSION` already carries
`EVIDENCE_PACK_HASH`, `SNAPSHOT_ID`, `PACK_VERSION`. The Phase 5B path
**reuses `COMMITTEE_HEARING` as a "container of last resort"** for evidence
(`EVIDENCE_JSON` / `DELTAS_JSON` / `EVIDENCE_PACK_HASH` / role+artifact rows)
but writes `STANCE`, `CONFIDENCE`, and `CHAIR_OUTPUT_JSON` as NULL — this is
exactly what the user authorized when they said *"COMMITTEE_HEARING may
remain historical/container-only if needed"*.

### What landed in 5B

#### 5B.1 — Anchor migrations in `MIP/apps/mip_ui_api/app/committee/agentic_authority.py`

- **`_fetch_current_hearing_id_for_action_sync`** (operator commit anchor):
  resolution order now is
  1. `AGENTIC_REVALIDATION_AUTHORITY` by `ACTION_ID` + `IS_LATEST=TRUE`
  2. `COMMITTEE_HEARING` joined via `LIVE_ACTIONS.PROPOSAL_ID` (evidence
     container; still written by the Phase 5B orchestrate path)
  3. `COMMITTEE_FINAL_DECISION` (legacy fallback, only fires for actions
     migrated mid-flight from the pre-Phase-5B era)
- **`_resolve_action_id_from_hearing_sync`** (AUTO_AUDIT hearing→action
  reverse mapping): same agentic-native → CH-joined → CFD-legacy chain.
- **`_fetch_current_pack_hash_for_hearing_sync`** (staleness anchor): now
  1. `COMMITTEE_HEARING.EVIDENCE_PACK_HASH` (primary container; still written)
  2. `SHADOW_BOARD_SESSION.EVIDENCE_PACK_HASH` (agentic-native fallback when
     CH is missing or has NULL hash)
- **`_fetch_c2_final_for_action_sync`** kept but expected to return `None`
  for Phase-5B-orchestrated actions. `build_authority_row` already handles
  `c2_final_decision=None` cleanly: `DETERMINISTIC_BASELINE_STANCE` and
  `DISAGREES_WITH_BASELINE` simply become `NULL` on the authority row.

#### 5B.2 — New evidence-only dossier in `MIP/apps/mip_ui_api/app/committee/engine.py`

- **`compute_evidence_only_dossier(snapshot, live, ...)`** — produces the
  same top-level dict shape as `compute_hearing_bundle` so existing
  persistence helpers can write it. Crucial differences:
  - `stance` / `confidence` / `posture` = `None`
  - `chair` = `{}` (empty — no deterministic chair verdict)
  - 6 role entries: `evidence_refs` only; `stance_badge` / `one_liner` /
    `influence` / `output` are all `None`
  - The `execution_implication` synthetic delta (which named a stance) is
    dropped; only factual drift buckets remain
  - `operational.stance` / `operational.confidence` / `operational.posture`
    are `None`
- New constant **`EVIDENCE_ONLY_PACK_VERSION = "2.0.0"`** — bumped from
  `"1.0.0"` to mark the Phase-5B era. Shadow sessions tagged with this
  pack version are guaranteed to have run against an evidence-only hearing.

#### 5B.3 — New evidence-only persistence in `MIP/apps/mip_ui_api/app/routers/committee.py`

- **`_persist_evidence_only_hearing(conn, hearing_id, proposal_id, snapshot_id, bundle, version)`**
  — sibling of `_persist_hearing_atomic`. Mirrors it exactly EXCEPT:
  - `STANCE`, `CONFIDENCE`, `CHAIR_OUTPUT_JSON` written as `NULL`
  - Role rows carry only `evidence_refs`; verdict fields all `NULL`
- **`_run_evidence_only_refresh(conn, hearing_id, proposal_id, snapshot, proposal)`**
  — sibling of `_run_refresh`. Builds the evidence-only dossier and persists
  it. Still appends the intraday substantiation artifact (pure evidence
  visualization, not a verdict). Returns the `_assemble_payload`-shaped dict
  with `stance` / `confidence` = `None`.

#### 5B.4 — Orchestrate split in `MIP/apps/mip_ui_api/app/routers/live.py`

`orchestrate_committee2_structural_entry` and `_intelligence_only_shadow_kickoff`
now branch on `AGENTIC_PRIMARY_MATERIALIZATION_ENABLED`:

- **When `true`** (current production setting):
  - `_run_evidence_only_refresh` writes the evidence container (no chair).
  - `committee_final_decision_commit_for_action` is **skipped**: no CFD row
    is written.
  - `_materialize_structural_entry_committee_apply` is **skipped** (Stage 4f
    guard).
  - `evidence_pack_hash` is computed via `compute_evidence_pack_hash` and
    persisted onto `COMMITTEE_HEARING.EVIDENCE_PACK_HASH` so the shadow
    session can cache-bind to the same snapshot identity.
  - `kickoff_shadow_board_for_snapshot` fires (unchanged).
- **When `false`** (rollback / pre-Stage-4 only): old `_run_refresh` +
  `committee_final_decision_commit_for_action` + materialize chain runs
  as before.

#### 5B.5 — Materializer cleanup in `_materialize_structural_entry_agentic_apply`

The CFD `DECISION_JSON.joint_decision` read at `live.py:10478` was discovered
to be a **dead no-op**: `COMMITTEE_FINAL_DECISION` has no `DECISION_JSON`
column, so `fd_apply.get("DECISION_JSON")` always returned `None` and
`jd_raw` was always `None`. The materializer's `jd` dict was effectively
always `{"should_enter": ..., "agentic_source": True,
"agentic_authority_status": ...}`. Phase 5B makes this explicit: the
materializer now seeds `jd` from scratch with those 3 fields, no CFD read.

TP/SL / bracket sizing flows downstream through
`_load_executable_entry_bracket_for_action`, which already has its own
priority chain on `LIVE_ACTIONS.PARAM_SNAPSHOT.structural_execution_contract_v1`
(Priority 1) → `PARAM_SNAPSHOT.executable_bracket` (Priority 3) →
`COMMITTEE_VERDICT.VERDICT_JSON` (Priority 4, legacy). The IBKR submit path
remains identical to today's behavior.

### What did NOT change (and why)

- **No SQL schema migration.** Authority + shadow session tables already
  carry every anchor the operator path needs.
- **`_load_executable_entry_bracket_for_action`** untouched. Its `COMMITTEE_VERDICT`
  fallback (Priority 4) still works because the agentic materializer writes
  to `COMMITTEE_VERDICT` itself (tagged `MODEL_NAME='AGENTIC_AUTHORITY_v1'`,
  not deterministic). User constraint: *"No IBKR/order changes."*
- **`/structural-committee/{hearingId}` historical replay** — still reads
  CH/CFD as before. Phase-5B-era CH rows show `STANCE`/`CONFIDENCE` as `—`
  (NULL); pre-5B rows display historical chair verdicts. User constraint:
  *"Keep historical Hearing Replay inspectable."*
- **Daily position verdict / training views / nightly jobs** — still read
  CFD/CV historical rows. They will see stale baseline stance for actions
  revalidated under Phase 5B (no new CFD rows). This was explicitly
  accepted in the Phase 5A audit ("KEEP_AUDIT_ONLY — accept stale baseline").
- **Stage 4f route guards** (`apply_live_trade_committee` /
  `run_live_trade_committee` raising 409 on structural ENTRY when
  agentic-primary is on) — still in place as belt-and-suspenders.

### Remaining deterministic dependencies (audit)

After Phase 5B, the **operator path** (orchestrate → shadow board → operator
commit → materialize → Submit → IBKR) has these residual deterministic-table
touch points, all of which are either non-blocking, audit-only, or have
explicit non-CFD primary sources:

| # | Site | Type | Risk | Notes |
|---|---|---|---|---|
| 1 | `_fetch_c2_final_for_action_sync` | LEGACY_AUDIT | Low | Returns `None` for Phase-5B actions; `build_authority_row` handles `None` cleanly. |
| 2 | `_fetch_current_hearing_id_for_action_sync` step 3 | LEGACY_FALLBACK | Low | Only fires when both agentic-native and CH lookups fail. Pure migration safety net. |
| 3 | `_resolve_action_id_from_hearing_sync` step 3 | LEGACY_FALLBACK | Low | Same as above. |
| 4 | `_load_executable_entry_bracket_for_action` Priority 4 (CV read) | AGENTIC_WRITER | None | CV row at submit time was written by the **agentic** materializer (`MODEL_NAME='AGENTIC_AUTHORITY_v1'`), not deterministic C2. |
| 5 | `/structural-committee/{hearingId}` page reads CH/CFD | HISTORICAL_REPLAY | None | Read-only, user-facing inspection surface. |
| 6 | LPA pending-decisions builder `LEFT JOIN COMMITTEE_VERDICT cv` | AGENTIC_WRITER | None | Same as #4 — Phase-5B CV rows are agentic-tagged. |
| 7 | `trade_proposals.py:251–277` "latest stance" display | DISPLAY_ONLY | Low | Cockpit display; does not affect Submit. To be cleaned up in Phase 5C or a follow-up display pass. |
| 8 | `V_AGENTIC_AUTHORITY_LATEST` view | DISPLAY_ONLY | Low | Joins `AGENTIC_REVALIDATION_AUTHORITY` to CFD/CH for legacy display columns. Authority columns are the truth. |
| 9 | Daily position verdict / training views | NIGHTLY_AUDIT | None | Accept stale CFD per Phase 5A audit. |

**Verdict: no operator-path runtime dependency on deterministic C2 remains.**
All `OPERATOR_PATH_LIVE` readers identified in Phase 5A are either fully
migrated to agentic-native anchors (1, 2, 3) or read agentic-written rows
that happen to live in legacy tables (4, 6).

### 5B.6 — Smoke proof

`cursorfiles/smoke_stage5b_agentic_only.py` — six check blocks, all pass:

1. `compute_evidence_only_dossier` produces NULL `stance` / NULL `confidence`
   / empty `chair` / NULL `posture`; 6 roles with NULL verdict fields; no
   `execution_implication` synthetic delta.
2. `_persist_evidence_only_hearing` writes CH with `STANCE=NULL`,
   `CONFIDENCE=NULL`, `CHAIR_OUTPUT_JSON=NULL`, `EVIDENCE_JSON` populated,
   `DELTAS_JSON` populated, `EVIDENCE_PACK_VERSION='2.0.0'`, 6 role rows
   with no verdict fields.
3. `_fetch_current_hearing_id_for_action_sync` resolves the live action's
   hearing via `AGENTIC_REVALIDATION_AUTHORITY` (verified against an
   action with both an authority row and a legacy CFD row — authority
   wins).
4. `_resolve_action_id_from_hearing_sync` resolves the hearing's action
   via `AGENTIC_REVALIDATION_AUTHORITY` (same proof, opposite direction).
5. `_fetch_current_pack_hash_for_hearing_sync` returns CH primary value.
6. End-to-end: `_run_evidence_only_refresh` against a real proposal
   produces a CH row with NULL chair/stance/confidence and **zero**
   `COMMITTEE_FINAL_DECISION` rows for the hearing.

Regression: `cursorfiles/smoke_stage4f_no_fallback.py` still passes —
Stage 4f hard guards remain in place as belt-and-suspenders defenses.

---

## Phase 5C — Remove the rollback flag + dead code (NOT YET IMPLEMENTED)

User chose `no_rollback`. Once Phase 5B is stable in production for ~1 week:

- Delete `AGENTIC_PRIMARY_MATERIALIZATION_ENABLED` from `APP_CONFIG`.
- Remove `should_block_deterministic_materialization` and its callers.
- Remove `_read_agentic_primary_flag_via_cursor` and its callers.
- Remove the Stage 4f route guards in `apply_live_trade_committee` /
  `run_live_trade_committee` (the entire structural-entry branches go away).
- Remove `_materialize_structural_entry_committee_apply` (now unreachable).
- Remove `is_agentic_primary_materialization_enabled` from `agentic_authority.py`.
- Remove the rollback branches inside `orchestrate_committee2_structural_entry`.
- Drop `committee2/orchestrate`'s deterministic exception handling.

Historical artifacts that REMAIN:

- `COMMITTEE_HEARING` / `COMMITTEE_FINAL_DECISION` / `COMMITTEE_VERDICT` /
  `COMMITTEE_RUN` tables — keep for historical hearings, training, and the
  bake-off analytics.
- `GET /committee/hearing/{id}` and adjacent read-only inspection routes.
- `MIP.MART.V_TRADE_INTELLIGENCE` and `MIP.LIVE.V_ENTRY_INTEL_LIFECYCLE_RECONSTRUCTION`
  (training / lifecycle reconstruction).
- `SP_RUN_DAILY_POSITION_VERDICT` (nightly position-health baseline).
- The bake-off page + `SP_REFRESH_COMMITTEE_BAKEOFF`.

---

## Phase 6 — Agentic EXIT verdict (NOT YET DESIGNED)

User asked to also migrate **structural EXIT** actions off the deterministic
execution-only verdict path. Today `build_structural_exit_execution_only_verdict`
produces broker-bracket execution decisions for closing positions; it does not
go through any committee.

Stage 6 needs its own design pass. Open questions:

- Does an EXIT need a full multi-specialist agentic committee, or just a
  single agentic chair?
- What evidence pack does it run against? (Position state + current bars +
  exit thesis from the entry?)
- How does it interact with bracket-already-placed-with-IBKR positions?
- Does EXIT have authority gating analogous to the entry gate?

**Defer until Phase 5B + 5C are stable.**

---

## Decision log

| Decision | User answer | Date |
|---|---|---|
| Rename "Shadow Board" → ? | "Agentic Committee" | 2026-05-26 |
| Downstream readers | "Audit first" | 2026-05-26 |
| Rollback flag | "No rollback — remove flag + legacy branches" | 2026-05-26 |
| EXIT migration | "Yes, design + build agentic EXIT verdict" | 2026-05-26 |
