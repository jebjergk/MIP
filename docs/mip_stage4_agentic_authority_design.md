# MIP Stage 4 Agentic Authority Design

**Status:** Planning document — no runtime code, SQL, UI, or submission-gating changes included.  
**Depends on:** Stages 1–3 complete (UI labeling, shadow prominence, baseline demotion).  
**Prerequisite satisfied:** Shadow evidence pack is Phase 4-aware at `pack_version = "2.0.0"`. Stage 4 trusts only pack versions on the explicit `SUPPORTED_PACK_VERSIONS` allow-list (initially `{"2.0.0"}`).

---

## Summary

Stages 1–3 changed operator vocabulary and visual prominence without changing any execution
authority. As of Stage 3:

- The shadow/agentic Chair Verdict is the **primary intelligence readout** in LPA.
- The deterministic Committee 2.0 baseline is collapsed/diagnostic.
- Submit is still gated by `LIVE_ACTIONS.STATUS = REVALIDATED_PASS` derived from the
  **deterministic** orchestrate path.
- Shadow board disagreement does not block Submit.

Stage 4 is the authority transition: the agentic/shadow board becomes the decision gate for
approve/wait/reject/reduced-approval, and deterministic Committee 2.0 is demoted to
diagnostics. The price/bar/news guard remains as an independent final safety utility.

Stage 4 is implemented in sub-stages (4a–4f). The earliest sub-stages are **additive and
non-breaking** (new table, new endpoint, audit-only). Submit gating is not changed until
Stage 4d, after the agentic authority path is validated in production.

---

## Current Materialization Chain

### Full chain: "Run Intelligence Review" → EXECUTION_REQUESTED

```
UI: runCommittee2Orchestrate (LivePortfolioActivity.jsx)
  POST /live/trades/actions/{action_id}/committee2/orchestrate
    → orchestrate_committee2_structural_entry  (live.py)
        ├── _run_opening_sanity_gate            may write LIVE_ACTIONS STATUS=OPEN_BLOCKED
        ├── _run_refresh (committee.py)
        │     └── compute_hearing_bundle        deterministic stance/confidence from snapshot+tape
        │         _persist_hearing_atomic       WRITES: COMMITTEE_HEARING, COMMITTEE_ROLE_OUTPUT,
        │                                               COMMITTEE_EVIDENCE_ARTIFACT
        ├── committee_final_decision_commit_for_action (committee.py)
        │     └── INSERT / UPDATE               WRITES: COMMITTEE_FINAL_DECISION (bound to action_id)
        ├── _materialize_structural_entry_committee_apply (live.py)
        │     └── structural_entry_verdict_from_committee2_final (committee2_live_bridge.py)
        │         UPDATE LIVE_ACTIONS           WRITES: STATUS, COMMITTEE_STATUS, COMMITTEE_RUN_ID,
        │                                               COMMITTEE_VERDICT, PROPOSED_QTY, REASON_CODES
        │         INSERT COMMITTEE_RUN          WRITES: MIP.LIVE.COMMITTEE_RUN
        │         INSERT COMMITTEE_VERDICT      WRITES: MIP.LIVE.COMMITTEE_VERDICT
        └── kickoff_shadow_board_for_snapshot   ASYNC — advisory, no LIVE_ACTIONS writes
              INSERT SHADOW_BOARD_SESSION (RUNNING)
              asyncio.create_task → orchestrate_shadow_board

  POST /live/decisions/{action_id}/approve-flow
    → approve_live_decision_flow
        pm_accept_live_action          STATUS → PM_ACCEPTED
        compliance_decide_live_action  STATUS → COMPLIANCE_APPROVED
        submit_live_trade_intent       STATUS → INTENT_SUBMITTED
        approve_live_trade_intent      STATUS → INTENT_APPROVED

  POST /live/trades/actions/{action_id}/revalidate  { force_refresh_1m: true }
    → revalidate_live_action
        price guard ≤2%  → STATUS = REVALIDATED_PASS, REVALIDATION_OUTCOME = PASS
        price guard 2–4% → STATUS = REVALIDATED_PASS, REVALIDATION_OUTCOME = PASS_WITH_REDUCED_SIZE
        price guard >4%  → STATUS = REVALIDATED_FAIL, REVALIDATION_OUTCOME = FAIL

  UI Submit button enabled:
    canSubmit = (statusUpper === 'REVALIDATED_PASS') && Boolean(d.submission_allowed)
    submission_allowed requires: not blocked, not committee_blocks_entry,
                                  trade_surface_ok, not superseded_blocked

  POST /live/decisions/{action_id}/submit-only
    → submit_live_decision_only → execute_live_action
        STATUS → EXECUTION_REQUESTED
        WRITES: LIVE_ORDERS, BROKER_EVENT_LEDGER
```

**Important finding:** There are no Snowflake stored procedures on the LPA hot path.
All committee orchestration, final-decision commit, and live-action materialization is
Python code in `live.py` and `committee.py` with direct SQL. This means Stage 4 changes
require Python edits, not SP replacements.

**Important finding:** `REVALIDATED_PASS` is set by the price/bar/news guard in
`revalidate_live_action`, not by deterministic Committee 2.0. The guard is a separate
safety utility that runs after the committee. Stage 4 must not remove this guard.

---

## Deterministic Committee 2.0 Authority Points

Every location where deterministic C2 currently has authority, classified by type:

| Location | File | Classification |
|---|---|---|
| `compute_hearing_bundle` | `committee/engine.py` | Produces verdict (stance, confidence) |
| `_persist_hearing_atomic` | `committee.py` | Creates/refreshes hearing — COMMITTEE_HEARING |
| `committee_final_decision_commit_for_action` | `committee.py` | Writes frozen decision — COMMITTEE_FINAL_DECISION |
| `structural_entry_verdict_from_committee2_final` | `committee2_live_bridge.py` | Maps C2 stance → PROCEED/BLOCK/PROCEED_REDUCED |
| `_materialize_structural_entry_committee_apply` | `live.py` | **Writes action materialization** — LIVE_ACTIONS STATUS, COMMITTEE_* |
| `_run_opening_sanity_gate` | `live.py` | Pre-orchestrate gate — can write OPEN_BLOCKED before C2 |
| `orchestrate_committee2_structural_entry` | `live.py` | Orchestrates the full C2 path |
| `submission_allowed` / `committee_blocks_entry` | `live.py` pending-decisions builder | Gates Submit via `d.submission_allowed` |
| `execute_live_action` | `live.py` | Reads COMMITTEE_VERDICT joint decision as additional execute gate |
| `revalidate_live_action` | `live.py` | Sets REVALIDATED_PASS — **not** C2 logic; independent safety guard |
| `SP_RUN_DAILY_POSITION_VERDICT` | `551_sp_run_daily_position_verdict.sql` | Reads COMMITTEE_FINAL_DECISION for position health |
| `SP_REFRESH_COMMITTEE_BAKEOFF` | `601_sp_refresh_committee_bakeoff.sql` | Reads COMMITTEE_FINAL_DECISION for bakeoff |
| Shadow board kickoff | `shadow_board.py` | Triggers shadow — audit/advisory only, no authority |

**C2 does NOT set REVALIDATED_PASS.** That is the price/bar/news guard, which runs
independently after the committee and must be preserved in Stage 4.

**C2 does NOT trigger shadow board.** The shadow board is kicked off by `orchestrate_committee2_structural_entry` after the C2 transaction, so it depends on C2 running first. This dependency must be redesigned in Stage 4: the shadow board will need to be triggerable independently or concurrently.

---

## Current Shadow / Agentic Output Shape

### Tables

**`MIP.APP.SHADOW_BOARD_SESSION`**

| Column | Type | Notes |
|---|---|---|
| SESSION_ID | VARCHAR(36) | PK |
| HEARING_ID | VARCHAR(36) | Links to COMMITTEE_HEARING |
| PROPOSAL_ID | NUMBER | |
| SNAPSHOT_ID | NUMBER | |
| EVIDENCE_PACK_HASH | VARCHAR(64) | Detects stale evidence |
| SHADOW_STANCE | VARCHAR(20) | APPROVE / APPROVE_REDUCED / WAIT_RECLAIM / DEFER / DENY |
| SHADOW_CONFIDENCE | FLOAT | 0.0–1.0 |
| STAGE_REACHED | NUMBER | 0=none, 1=specialists, 2=conflicts, 3=challenge, 4=revisions, 5=chair |
| STATUS | VARCHAR(20) | RUNNING / COMPLETE / DEGRADED / FAILED |
| DEGRADED | BOOLEAN | |
| DEGRADED_REASON | VARCHAR(500) | |
| AGENT_MODEL | VARCHAR(80) | DEFAULT 'claude-sonnet-4-6' |
| PACK_VERSION | VARCHAR(32) | Runtime writes '2.0.0' |
| RUN_MS | NUMBER | Wall-clock run time |
| CREATED_AT | TIMESTAMP_NTZ | |
| COMPLETED_AT | TIMESTAMP_NTZ | |

**`MIP.APP.SHADOW_CHAIR_RULING`**

| Column | Type | Notes |
|---|---|---|
| RULING_ID | NUMBER AUTOINCREMENT | PK |
| SESSION_ID | VARCHAR(36) | UNIQUE — one ruling per session |
| HEARING_ID | VARCHAR(36) | |
| SHADOW_STANCE | VARCHAR(20) | **Primary authority signal** |
| SHADOW_CONFIDENCE | FLOAT | **Primary confidence signal** |
| PLURALITY_BASIS | VARCHAR(500) | How specialist votes drove the stance |
| CONFLICT_RESOLUTION | VARCHAR(2000) | Narrative resolution of conflicts |
| SHADOW_TRADE_JSON | VARIANT | Symbolic: entry_zone, size_posture, trail_posture, key_condition |
| TOP_SUPPORTS | VARIANT | JSON array of evidence bullets |
| TOP_TENSIONS | VARIANT | JSON array of risk/concern bullets |
| PARSE_OK | BOOLEAN | False when chair output could not be parsed |
| DEGRADED | BOOLEAN | |
| DEGRADED_REASON | VARCHAR(500) | |
| AGENT_ELAPSED_MS | NUMBER | |
| CREATED_AT | TIMESTAMP_NTZ | |

**`MIP.APP.SHADOW_SPECIALIST_POSITION`** — ROLE_NAME, STANCE, CONFIDENCE, RATIONALE,
EVIDENCE_USED, PARSE_OK, DEGRADED per role per session.

**`MIP.APP.SHADOW_CONFLICT_MAP`** — ROLE_A, ROLE_B, SEVERITY (MINOR/MAJOR/CRITICAL)
per conflict per session.

**`MIP.APP.SHADOW_CHALLENGE_TURN`**, **`MIP.APP.SHADOW_REVISION_TURN`** — debate record.

### Python output shape (ShadowBoardResult)

| Field | Authority-capable? |
|---|---|
| `session_id` | Identity key |
| `status` (COMPLETE/DEGRADED/FAILED) | **Yes — required for authority gating** |
| `shadow_stance` | **Yes — primary verdict** |
| `shadow_confidence` | **Yes — confidence threshold** |
| `degraded` | **Yes — fail-closed trigger** |
| `chair.plurality_basis` | Evidence narrative |
| `chair.conflict_resolution` | Conflict narrative |
| `chair.top_supports` | Evidence bullets for approval |
| `chair.top_tensions` | Risk bullets for concern |
| `chair.shadow_trade.size_posture` | Sizing guidance (FULL/REDUCED/MINIMAL) |
| `pack_version` | Staleness / evidence-quality check |
| `stage_reached` | Degradation depth indicator |
| `positions` | Specialist vote distribution (6 roles) |
| `conflicts` | Conflict count and severity |

### Phase 4 evidence (pack_version 2.0.0)

The chair receives `phase4_thesis_verdict` and `phase4_dossier_context` slices, which include:
`final_action`, `final_direction`, `thesis_health`, `prior_thesis_reference`,
`actionability_summary`, `primary_reason_code`, `why_not_opposite`, `continuation_quality`,
`resistance_overhead_risk`, `candle_psychology`, `structural_timeline_summary`, `levels`.

These inform the chair's rationale and stances but are not independently surfaced as
authority fields — the authority output remains `SHADOW_STANCE` and `SHADOW_CONFIDENCE`.

---

## Proposed Agentic Authority Taxonomy

### Normalized authority statuses

| Status | Maps from shadow stance | Meaning |
|---|---|---|
| `AGENTIC_APPROVE` | `APPROVE` (COMPLETE, not degraded) | Full approval; action may proceed at proposed size |
| `AGENTIC_APPROVE_REDUCED` | `APPROVE_REDUCED` (COMPLETE, not degraded) | Approval at reduced size; size_posture = REDUCED |
| `AGENTIC_WAIT_RECLAIM` | `WAIT_RECLAIM` (COMPLETE, not degraded) | Action blocked; wait for reclaim condition |
| `AGENTIC_DEFER` | `DEFER` (COMPLETE, not degraded) | Action blocked; defer pending re-review |
| `AGENTIC_REJECT` | `DENY` (COMPLETE, not degraded) | Action rejected; do not submit |
| `AGENTIC_DEGRADED_NO_AUTHORITY` | COMPLETE but `degraded=true`, or STAGE_REACHED < 5 | Session ran but produced unreliable output |
| `AGENTIC_FAILED_NO_AUTHORITY` | FAILED, RUNNING, missing, stale, PACK_VERSION mismatch | No usable authority result |

### Mapping logic (deterministic)

Pack-version validation uses an **explicit supported-list**, not string less-than. This
prevents accidental ordering bugs (e.g. `"10.0.0" < "2.0.0"` lexicographically) and forces
us to enumerate which pack versions Stage 4 trusts.

```python
# Module-level constant. Update only via design review.
SUPPORTED_PACK_VERSIONS = frozenset({"2.0.0"})


def is_pack_version_supported(pack_version: str | None) -> bool:
    """Explicit allow-list. No tuple/string comparison."""
    if not pack_version:
        return False
    return pack_version in SUPPORTED_PACK_VERSIONS


def map_shadow_to_authority_status(session: dict) -> str:
    status = session.get("status")          # COMPLETE / DEGRADED / FAILED / RUNNING
    degraded = session.get("degraded", False)
    stance = session.get("shadow_stance")   # APPROVE / APPROVE_REDUCED / WAIT_RECLAIM / DEFER / DENY
    pack_version = session.get("pack_version")
    stage_reached = session.get("stage_reached", 0)

    # Fail-closed conditions
    if status != "COMPLETE":
        return "AGENTIC_FAILED_NO_AUTHORITY"
    if degraded:
        return "AGENTIC_DEGRADED_NO_AUTHORITY"
    if stage_reached < 5:                   # chair stage not reached
        return "AGENTIC_DEGRADED_NO_AUTHORITY"
    if not is_pack_version_supported(pack_version):
        return "AGENTIC_DEGRADED_NO_AUTHORITY"

    # Positive authority
    _MAP = {
        "APPROVE":         "AGENTIC_APPROVE",
        "APPROVE_REDUCED": "AGENTIC_APPROVE_REDUCED",
        "WAIT_RECLAIM":    "AGENTIC_WAIT_RECLAIM",
        "DEFER":           "AGENTIC_DEFER",
        "DENY":            "AGENTIC_REJECT",
    }
    return _MAP.get(stance, "AGENTIC_FAILED_NO_AUTHORITY")
```

When a new evidence pack version is introduced (e.g. `2.1.0` for a future slice addition),
the supported-list is extended explicitly in a follow-up code change rather than being
implicitly accepted by a comparator. The `PACK_VERSION_OK` column in the authority row
captures this check result at commit time.

### Per-status effects (target state, Stage 4d+)

| Authority Status | LIVE_ACTIONS effect | Submit eligibility | Operator override |
|---|---|---|---|
| `AGENTIC_APPROVE` | Allow approve-flow | Enabled (after safety revalidation) | N/A |
| `AGENTIC_APPROVE_REDUCED` | Allow approve-flow, apply size reduction | Enabled at reduced size | N/A |
| `AGENTIC_WAIT_RECLAIM` | Set OPEN_BLOCKED equivalent | Disabled | Disabled in Stage 4d |
| `AGENTIC_DEFER` | Set OPEN_BLOCKED equivalent | Disabled | Disabled in Stage 4d |
| `AGENTIC_REJECT` | Set OPEN_BLOCKED equivalent | Disabled | Disabled in Stage 4d |
| `AGENTIC_DEGRADED_NO_AUTHORITY` | No positive authority; action paused | Disabled | Initially disabled; later: constrained |
| `AGENTIC_FAILED_NO_AUTHORITY` | No positive authority; action paused | Disabled | Initially disabled; later: constrained |

---

## Submit Gating Options

### Option A — Overload LIVE_ACTIONS.STATUS with new agentic values

Write `AGENTIC_REVALIDATED_PASS` / `AGENTIC_REVALIDATED_BLOCK` into `LIVE_ACTIONS.STATUS`.

**Problems:** Breaks the existing `_ALLOWED_TRANSITIONS` FSM; every consumer of STATUS must be
updated; historical rows become ambiguous; one field carries two different authority signals.

**Verdict: Rejected.**

### Option B — Set REVALIDATED_PASS only when agentic authority permits

Reuse existing STATUS values but make `REVALIDATED_PASS` contingent on agentic verdict.

**Problems:** `REVALIDATED_PASS` currently means price/bar guard passed — adding committee
semantics silently overloads it. Makes safety guard inseparable from committee authority,
which is architecturally backwards.

**Verdict: Rejected.**

### Option C — New explicit authority columns or table (Recommended)

Add a dedicated append-only `MIP.APP.AGENTIC_REVALIDATION_AUTHORITY` table. Submit gating in
Stage 4d adds a second requirement, and explicitly trusts only operator-committed authority:

```
STATUS = REVALIDATED_PASS
AND EXISTS (
  SELECT 1 FROM MIP.APP.AGENTIC_REVALIDATION_AUTHORITY ara
   WHERE ara.ACTION_ID = LIVE_ACTIONS.ACTION_ID
     AND ara.IS_LATEST = TRUE
     AND ara.AUTHORITY_MODE = 'OPERATOR_COMMITTED'
     AND ara.IS_STALE = FALSE
     AND ara.AUTHORITY_STATUS IN ('AGENTIC_APPROVE', 'AGENTIC_APPROVE_REDUCED')
)
```

Background `AUTO_AUDIT` rows are visible in `V_AGENTIC_AUTHORITY_LATEST` for observation
but never gate Submit.

**Advantages:**
- LIVE_ACTIONS schema untouched until Stage 4e.
- Authority row is missing by default → fail-closed automatically.
- Full audit trail per action per session.
- Deterministic C2 path continues without changes until Stage 4e.
- Safety revalidation and agentic authority remain independent, composable gates.

**Verdict: Recommended.**

---

## Recommended Authority Persistence Model

### Table: `MIP.APP.AGENTIC_REVALIDATION_AUTHORITY` (append-only history)

The table is **append-only**. Every authority commit (auto-audit from background, or
operator-committed via the LPA button) writes a new row. The latest non-superseded row per
`ACTION_ID` is selected via the `IS_LATEST` flag and the `V_AGENTIC_AUTHORITY_LATEST` view.
Prior authority decisions are never overwritten — they are marked superseded.

```sql
CREATE TABLE MIP.APP.AGENTIC_REVALIDATION_AUTHORITY (
    AUTHORITY_ID             VARCHAR(36)     NOT NULL,  -- uuid4 — unique per row
    ACTION_ID                VARCHAR(36)     NOT NULL,  -- non-unique; many rows per action over time
    PROPOSAL_ID              NUMBER          NOT NULL,
    HEARING_ID               VARCHAR(36)     NOT NULL,
    SHADOW_SESSION_ID        VARCHAR(36)     NOT NULL,
    PACK_VERSION             VARCHAR(32)     NOT NULL,
    PACK_VERSION_OK          BOOLEAN         NOT NULL,  -- PACK_VERSION in SUPPORTED_PACK_VERSIONS

    -- Authority provenance
    AUTHORITY_MODE           VARCHAR(20)     NOT NULL,  -- 'AUTO_AUDIT' | 'OPERATOR_COMMITTED'
    COMMITTED_BY             VARCHAR(100),              -- 'system_shadow_audit' or operator user id
    IS_LATEST                BOOLEAN         NOT NULL DEFAULT TRUE,
    SUPERSEDED_AT            TIMESTAMP_NTZ,             -- set when a newer row supersedes this one
    SUPERSEDED_BY            VARCHAR(36),               -- AUTHORITY_ID of the row that superseded this one

    -- Mapped authority verdict
    AUTHORITY_STATUS         VARCHAR(40)     NOT NULL,
    AUTHORITY_REASON_CODE    VARCHAR(100),              -- e.g. SHADOW_COMPLETE_APPROVE
    AUTHORITY_CONFIDENCE     FLOAT,                     -- from SHADOW_CONFIDENCE

    -- Raw shadow signals
    SHADOW_STANCE_RAW        VARCHAR(20),               -- APPROVE / DENY / etc.
    SHADOW_STATUS_RAW        VARCHAR(20),               -- COMPLETE / DEGRADED / FAILED
    SHADOW_STAGE_REACHED     NUMBER,
    SHADOW_DEGRADED          BOOLEAN,
    SHADOW_DEGRADED_REASON   VARCHAR(500),
    SHADOW_PLURALITY_BASIS   VARCHAR(500),
    SHADOW_SIZE_POSTURE      VARCHAR(20),               -- from shadow_trade.size_posture

    -- Comparison
    DETERMINISTIC_BASELINE_STANCE    VARCHAR(20),       -- from COMMITTEE_FINAL_DECISION.STANCE
    DISAGREES_WITH_BASELINE          BOOLEAN,

    -- Staleness / validity
    IS_STALE                 BOOLEAN         NOT NULL DEFAULT FALSE,
    STALE_REASON             VARCHAR(200),
    SESSION_AGE_MINUTES      NUMBER,                    -- age at commit time

    -- Override (Stage 4 initially disabled)
    IS_OPERATOR_OVERRIDDEN   BOOLEAN         NOT NULL DEFAULT FALSE,
    OVERRIDE_BY              VARCHAR(100),
    OVERRIDE_AT              TIMESTAMP_NTZ,
    OVERRIDE_REASON          VARCHAR(500),
    OVERRIDE_ORIGINAL_STATUS VARCHAR(40),               -- what would have been set without override

    -- Full payload
    AUTHORITY_PAYLOAD_JSON   VARIANT,                   -- full shadow session snapshot at commit time

    CREATED_AT               TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT               TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_AGENTIC_AUTHORITY PRIMARY KEY (AUTHORITY_ID)
    -- NO UNIQUE constraint on ACTION_ID — append-only history
);
```

### Supersession semantics

When a new authority row is inserted for an `ACTION_ID`:

1. The insert is wrapped in a transaction.
2. All existing rows for that `ACTION_ID` where `IS_LATEST = TRUE` are updated:
   `IS_LATEST = FALSE, SUPERSEDED_AT = current_timestamp(), SUPERSEDED_BY = <new AUTHORITY_ID>`.
3. The new row is inserted with `IS_LATEST = TRUE`.

This applies regardless of whether the new row is `AUTO_AUDIT` or `OPERATOR_COMMITTED`.
Prior decisions are preserved for audit; downstream readers always filter on `IS_LATEST = TRUE`.

### Authority mode rules

| `AUTHORITY_MODE` | Created by | Used for Stage 4d Submit gating? |
|---|---|---|
| `AUTO_AUDIT` | Background hook after shadow board completes | **No.** Observation only. |
| `OPERATOR_COMMITTED` | LPA "Apply Agentic Review" button (POST `.../agentic-authority/commit`) | **Yes.** Only OPERATOR_COMMITTED authorities gate Submit. |

**Rationale.** An automatic audit row captures the shadow verdict at the moment shadow
completes, but the operator may not yet have seen it, the snapshot may have moved, or the
operator may choose to re-run intelligence review. Treating auto-audit as authoritative would
also create races where a stale background row blocks a fresh proposal. Submit gating in
Stage 4d looks only at `OPERATOR_COMMITTED` rows: the operator must explicitly bind the
agentic verdict to the action.

### Staleness rules (applied at commit time)

| Condition | IS_STALE | AUTHORITY_STATUS forced to |
|---|---|---|
| Shadow session CREATED_AT < action's last proposal refresh | True | AGENTIC_FAILED_NO_AUTHORITY |
| Shadow session EVIDENCE_PACK_HASH != current COMMITTEE_HEARING.EVIDENCE_PACK_HASH | True | AGENTIC_FAILED_NO_AUTHORITY |
| Session age > 4 hours (configurable) | True | AGENTIC_FAILED_NO_AUTHORITY |
| `PACK_VERSION` not in `SUPPORTED_PACK_VERSIONS` allow-list | False (PACK_VERSION_OK=false) | AGENTIC_DEGRADED_NO_AUTHORITY |
| Session STATUS != COMPLETE | N/A | AGENTIC_FAILED_NO_AUTHORITY |
| Session DEGRADED = true | N/A | AGENTIC_DEGRADED_NO_AUTHORITY |
| Session STAGE_REACHED < 5 | N/A | AGENTIC_DEGRADED_NO_AUTHORITY |

### Supporting diagnostic views

```sql
-- Latest non-superseded authority row per action (any mode)
CREATE OR REPLACE VIEW MIP.APP.V_AGENTIC_AUTHORITY_LATEST AS
SELECT
    ara.*,
    la.STATUS                   AS LIVE_ACTION_STATUS,
    la.PROPOSED_QTY             AS ACTION_QTY,
    la.SYMBOL,
    cfd.STANCE                  AS C2_STANCE,
    cfd.CONFIDENCE              AS C2_CONFIDENCE
FROM MIP.APP.AGENTIC_REVALIDATION_AUTHORITY ara
JOIN MIP.LIVE.LIVE_ACTIONS la         ON la.ACTION_ID = ara.ACTION_ID
LEFT JOIN MIP.APP.COMMITTEE_FINAL_DECISION cfd ON cfd.ACTION_ID = ara.ACTION_ID
WHERE ara.IS_LATEST = TRUE;

-- Latest OPERATOR_COMMITTED row per action — drives Stage 4d Submit gating
CREATE OR REPLACE VIEW MIP.APP.V_AGENTIC_AUTHORITY_LATEST_OPERATOR AS
SELECT
    ara.*,
    la.STATUS                   AS LIVE_ACTION_STATUS,
    la.SYMBOL
FROM MIP.APP.AGENTIC_REVALIDATION_AUTHORITY ara
JOIN MIP.LIVE.LIVE_ACTIONS la         ON la.ACTION_ID = ara.ACTION_ID
WHERE ara.AUTHORITY_MODE = 'OPERATOR_COMMITTED'
  AND ara.AUTHORITY_ID = (
        SELECT a2.AUTHORITY_ID
        FROM MIP.APP.AGENTIC_REVALIDATION_AUTHORITY a2
        WHERE a2.ACTION_ID = ara.ACTION_ID
          AND a2.AUTHORITY_MODE = 'OPERATOR_COMMITTED'
        ORDER BY a2.CREATED_AT DESC
        LIMIT 1
  );

-- Full audit trail per action (any mode, including superseded)
CREATE OR REPLACE VIEW MIP.APP.V_AGENTIC_AUTHORITY_HISTORY AS
SELECT
    ara.ACTION_ID,
    ara.AUTHORITY_ID,
    ara.AUTHORITY_MODE,
    ara.AUTHORITY_STATUS,
    ara.AUTHORITY_CONFIDENCE,
    ara.SHADOW_STANCE_RAW,
    ara.SHADOW_SESSION_ID,
    ara.PACK_VERSION,
    ara.PACK_VERSION_OK,
    ara.IS_STALE,
    ara.IS_LATEST,
    ara.SUPERSEDED_AT,
    ara.SUPERSEDED_BY,
    ara.IS_OPERATOR_OVERRIDDEN,
    ara.COMMITTED_BY,
    ara.CREATED_AT
FROM MIP.APP.AGENTIC_REVALIDATION_AUTHORITY ara
ORDER BY ara.ACTION_ID, ara.CREATED_AT DESC;
```

---

## Recommended API / Backend Design

### Should Stage 4 reuse `committee2/orchestrate` or create a new endpoint?

**Recommendation: new dedicated endpoint.** Reasons:

1. `committee2/orchestrate` runs deterministic C2, commits `COMMITTEE_FINAL_DECISION`, and
   materializes `LIVE_ACTIONS` in a single transaction. Grafting agentic authority commit
   into that transaction couples the two authorities incorrectly.
2. Shadow board completes asynchronously, potentially long after orchestrate returns. Authority
   commit is a separate event.
3. A dedicated endpoint provides a clean audit boundary and can be disabled independently.

### New endpoint: `POST /live/trades/actions/{action_id}/agentic-authority/commit`

**Purpose:** Operator explicitly triggers "Apply Agentic Review" after shadow board completes.
This is not automatic — operator must see the shadow verdict and intentionally commit it.

**Request body:**
```json
{
  "shadow_session_id": "<uuid>",    // required — explicit session binding
  "hearing_id": "<uuid>",          // required — cross-check against action
  "override_reason": null           // null = no override; string = override with reason (initially rejected)
}
```

**Server-side logic:**

```
POST /live/trades/actions/{action_id}/agentic-authority/commit
  → commit_agentic_authority(action_id, body, current_user)

  1. Fetch live action — verify exists, not already EXECUTION_REQUESTED or EXECUTED
  2. Fetch shadow session by session_id — verify hearing_id matches action's proposal hearing
  3. Fetch COMMITTEE_FINAL_DECISION for action — get deterministic baseline stance
  4. Staleness checks:
       session.EVIDENCE_PACK_HASH == current COMMITTEE_HEARING.EVIDENCE_PACK_HASH ?
       session.CREATED_AT age < AGENTIC_MAX_SESSION_AGE_MINUTES ?
  5. Map shadow → AUTHORITY_STATUS using mapping logic (see taxonomy section)
  6. Disagrees_with_baseline = (shadow_stance != c2_stance)
  7. Override validation:
       Stage 4a/4b/4c: override not supported → 400 OVERRIDE_NOT_ENABLED if override_reason provided
       Stage 4d+: override allowed only for DEGRADED/FAILED statuses, not REJECT/BLOCK
  8. BEGIN TRANSACTION
       UPDATE AGENTIC_REVALIDATION_AUTHORITY
          SET IS_LATEST = FALSE,
              SUPERSEDED_AT = current_timestamp(),
              SUPERSEDED_BY = :new_authority_id
        WHERE ACTION_ID = :action_id AND IS_LATEST = TRUE;
       INSERT INTO AGENTIC_REVALIDATION_AUTHORITY (
         AUTHORITY_ID, ACTION_ID, ..., AUTHORITY_MODE, COMMITTED_BY, IS_LATEST, ...
       ) VALUES (
         :new_authority_id, :action_id, ..., 'OPERATOR_COMMITTED', :current_user, TRUE, ...
       );
     COMMIT
  9. Return:
       { authority_id, action_id, authority_mode: 'OPERATOR_COMMITTED',
         authority_status, authority_confidence, shadow_stance_raw,
         disagrees_with_baseline, is_stale, created_at, superseded_authority_id }
```

The supersession update + insert run in a single transaction so that the latest-flag
invariant (at most one `IS_LATEST = TRUE` row per `ACTION_ID`) cannot be violated by a
concurrent commit. The endpoint always writes `AUTHORITY_MODE = 'OPERATOR_COMMITTED'`;
`AUTO_AUDIT` rows are only created by the background hook (Stage 4b).

**Error responses:**

| Condition | HTTP | Error code |
|---|---|---|
| session_id not found | 404 | SESSION_NOT_FOUND |
| hearing_id mismatch | 409 | HEARING_MISMATCH |
| action already EXECUTED | 409 | ACTION_ALREADY_EXECUTED |
| evidence pack stale | 422 | EVIDENCE_PACK_STALE |
| override not yet enabled | 400 | OVERRIDE_NOT_ENABLED |

### Should shadow board completion auto-commit authority?

**Recommendation: No.** Auto-commit creates the risk of a stale or degraded session
automatically blocking a valid trade. The operator has already seen the shadow verdict in
the LPA headline (Stage 2/3). The explicit "Apply Agentic Review" click is a deliberate
operator action that binds the agentic verdict to the action at a specific moment.

### Preventing stale session/action mismatch

The `EVIDENCE_PACK_HASH` field on `SHADOW_BOARD_SESSION` is computed from the same hearing
snapshot used to build the evidence pack. If the operator re-runs "Run Intelligence Review"
on a modified snapshot, a new `EVIDENCE_PACK_HASH` is computed and the prior shadow session
will fail the staleness check at commit time.

---

## LPA Operator UX Design

### Stage 4a/4b — UI unchanged

Shadow Chair Verdict displays as today (Stage 3). No "Apply Agentic Review" button.

In Stage 4b, every completed shadow session causes a background hook to insert an
`AUTHORITY_MODE = 'AUTO_AUDIT'` row into `AGENTIC_REVALIDATION_AUTHORITY` (see Stage 4b
tasks). These rows are observation-only and never gate Submit.

### Stage 4c — Display mode (read authority, no gating)

A new "Apply Agentic Review" button appears on the shadow verdict card after the shadow board
reaches STATUS = COMPLETE. The operator can click it to write an
`AUTHORITY_MODE = 'OPERATOR_COMMITTED'` row.

LPA fetches the latest authority row from `V_AGENTIC_AUTHORITY_LATEST` and displays:

- If no `OPERATOR_COMMITTED` row exists yet: show the auto-audit status as a preview chip
  with a "Preview only — Apply review to commit" sub-label, and show the "Apply Agentic
  Review" button.
- If an `OPERATOR_COMMITTED` row exists and `IS_STALE = FALSE`: show the authority chip as
  the committed verdict (see table below). The "Apply Agentic Review" button is hidden.
- If an `OPERATOR_COMMITTED` row exists but `IS_STALE = TRUE` (e.g. evidence pack hash drift
  after a fresh orchestrate): show a "Re-commit (stale)" button.

LPA displays the committed authority status (label and behavior identical for AUTO_AUDIT
preview and OPERATOR_COMMITTED; the difference is whether Stage 4d gating trusts it):

| Authority status | Headline chip | Sub-label |
|---|---|---|
| AGENTIC_APPROVE | "Agentic: Approved" (green) | "Submit enabled after safety revalidation" |
| AGENTIC_APPROVE_REDUCED | "Agentic: Approve (reduced size)" (amber) | "Submit enabled at reduced size" |
| AGENTIC_WAIT_RECLAIM | "Agentic: Wait / Reclaim" (orange) | "Submit will be blocked in Stage 4d" |
| AGENTIC_DEFER | "Agentic: Defer" (orange) | "Submit will be blocked in Stage 4d" |
| AGENTIC_REJECT | "Agentic: Reject" (red) | "Submit will be blocked in Stage 4d" |
| AGENTIC_DEGRADED_NO_AUTHORITY | "Agentic: Degraded — no authority" (grey) | "Review degraded; run new review or override in Stage 4d+" |
| AGENTIC_FAILED_NO_AUTHORITY | "Agentic: Not available" (grey) | "No agentic authority committed; run review" |

**Submit gating in Stage 4c: unchanged.** Submit is still `REVALIDATED_PASS + submission_allowed`.
The authority status chip is purely informational — it shows what would happen in Stage 4d.

### Stage 4d — Submit gating switch

Submit now requires both conditions:

```
LIVE_ACTIONS.STATUS = REVALIDATED_PASS
AND there exists a row in AGENTIC_REVALIDATION_AUTHORITY for this ACTION_ID with:
    IS_LATEST         = TRUE
    AUTHORITY_MODE    = 'OPERATOR_COMMITTED'
    IS_STALE          = FALSE
    AUTHORITY_STATUS IN ('AGENTIC_APPROVE', 'AGENTIC_APPROVE_REDUCED')
```

Background `AUTO_AUDIT` rows are explicitly **not** trusted for Submit gating; the operator
must click "Apply Agentic Review" to bind the verdict.

If `AGENTIC_APPROVE_REDUCED`, the `shadow_trade.size_posture = REDUCED` feeds into a new
sizing instruction that replaces the deterministic C2 `POSTURE_JSON.size_posture` as the
source of truth for submission quantity.

Submit button disabled states:

| Condition | Button label / tooltip |
|---|---|
| No `OPERATOR_COMMITTED` row | "Agentic review not committed — apply review first" |
| Only `AUTO_AUDIT` row exists | "Agentic review not committed — apply review first" |
| AGENTIC_WAIT_RECLAIM | "Agentic board: Wait / Reclaim — submit blocked" |
| AGENTIC_DEFER | "Agentic board: Defer — submit blocked" |
| AGENTIC_REJECT | "Agentic board: Reject — submit blocked" |
| AGENTIC_DEGRADED_NO_AUTHORITY | "Agentic review degraded — run new review" |
| AGENTIC_FAILED_NO_AUTHORITY | "Agentic review unavailable — run new review" |
| IS_STALE = true | "Agentic authority stale — re-commit after new review" |

---

## Deterministic Fallback / Fail-Closed Policy

### Core principle

**There must be no silent fallback to deterministic C2 approval in the operator path.**
If agentic authority is absent, stale, degraded, or failed, the action must be blocked
or paused — not secretly allowed by the old deterministic board.

### Fail-closed matrix

| Agentic authority state | Stage 4d behavior |
|---|---|
| No row at all for this ACTION_ID | Submit blocked; action paused |
| Only AUTO_AUDIT row, no OPERATOR_COMMITTED row | Submit blocked; "Apply Agentic Review" required |
| Latest OPERATOR_COMMITTED row IS_STALE = TRUE | Submit blocked; offer "re-commit after new review" |
| Latest OPERATOR_COMMITTED row is RUNNING (impossible — operator only commits after COMPLETE) | N/A |
| Latest OPERATOR_COMMITTED row is DEGRADED_NO_AUTHORITY | Submit blocked; offer "run new review" |
| Latest OPERATOR_COMMITTED row is FAILED_NO_AUTHORITY | Submit blocked; offer "run new review" |
| `PACK_VERSION` not in SUPPORTED_PACK_VERSIONS | DEGRADED_NO_AUTHORITY path (Phase 4 evidence missing or unsupported version) |
| Confidence < threshold (configurable, e.g. 0.40) | DEGRADED_NO_AUTHORITY path |
| Session age > AGENTIC_MAX_SESSION_AGE_MINUTES | IS_STALE = true path |
| DISAGREES_WITH_BASELINE = true | Does not block; shown as informational Δ chip |

### Safety revalidation guard (retained independently)

`revalidate_live_action` (price/bar/news) is **not** replaced in Stage 4. It remains a
separate final safety gate that must pass regardless of agentic authority:

- Agentic APPROVE + price deviation > 4% → REVALIDATED_FAIL → Submit blocked.
- Safety revalidation is the last line of execution protection.
- It is not Committee 2.0 logic. It must not be labeled or treated as C2.

### Confidence threshold (recommended)

Introduce `AGENTIC_MIN_CONFIDENCE_THRESHOLD` in `APP_CONFIG` (default: `0.40`).

Sessions where `SHADOW_CONFIDENCE < threshold` map to `AGENTIC_DEGRADED_NO_AUTHORITY`
regardless of stance. This prevents a low-confidence APPROVE from granting full authority.

---

## Operator Override Policy

### Stage 4d initial policy: override disabled

Override is **not enabled** in Stage 4d. Rationale: the first operator-facing authority
gate should be strict to establish baseline behavior. Override complexity can be added
once the authority path is validated in production.

### Future Stage 4e+ override design (when enabled)

**Eligible for override:**
- `AGENTIC_DEGRADED_NO_AUTHORITY` only.
- `AGENTIC_FAILED_NO_AUTHORITY` only when caused by timeout (not parse failures or stale evidence).

**Not eligible for override:**
- `AGENTIC_WAIT_RECLAIM`, `AGENTIC_DEFER`, `AGENTIC_REJECT` — these are intentional
  verdicts from the chair, not failures. Overriding a deliberate "do not trade" is a
  different policy decision requiring separate approval.
- IS_STALE = true — the right response is to run a fresh review, not override.

**Override confirmation dialog:**

> "The agentic intelligence review could not produce a reliable result (degraded).
> You are requesting to proceed without agentic authority.
> This override will be audited.
> Reason (required): [text field]
> [ Cancel ] [ Override — Proceed at My Risk ]"

**Override fields written to `AGENTIC_REVALIDATION_AUTHORITY`:**

```
IS_OPERATOR_OVERRIDDEN = true
OVERRIDE_BY            = <current_user>
OVERRIDE_AT            = current_timestamp()
OVERRIDE_REASON        = <entered reason>
OVERRIDE_ORIGINAL_STATUS = <status before override>
AUTHORITY_STATUS updated to: AGENTIC_APPROVE (or AGENTIC_APPROVE_REDUCED per shadow_trade.size_posture)
```

**Override cannot force a submit that fails safety revalidation.** Price/bar/news guard
always runs independently and cannot be overridden.

---

## RAG Positioning

RAG is not wired in Stage 4 and is explicitly excluded from the authority pathway.

### Future Stage 4.5 / Stage 5 integration point

The only correct insertion point for literature support is **inside the shadow board evidence
pack**, as an additional advisory slice — not as an independent authority signal.

Specifically: after `_stage0_build_evidence_pack` in `orchestrate_shadow_board`
(`shadow_board.py`) and before the specialists receive their slices, add a
`literature_support` slice built by calling:

```python
CALL MIP.KNOWLEDGE.SP_SEARCH_REVALIDATION_LITERATURE(:query_text, :top_k)
```

or the equivalent Python helper `search_literature_rag.py`.

**Design constraints for RAG slice:**

- The `literature_support` slice is **advisory only**.
- It must include an explicit guardrail string:
  `"Advisory literature concept. Not market evidence. Not a trade signal."`
- It must include: `retrieved_card_ids`, `concept_names`, `display_texts`, `source_books`,
  `retrieval_score`.
- It must include a `match_statement`: which MIP-observed facts (from `proposal_meta`,
  `structural_state`, etc.) match or do not match the retrieved concept.
- Retrieved CARD_IDs must be persisted in the authority table's `AUTHORITY_PAYLOAD_JSON`
  for audit.
- The authority decision is still based on observed MIP evidence, not on literature.
  Literature is context that the chair may reference in rationale, not a vote.

**RAG must not be enabled until:**
1. Stage 4 authority path is validated in production (Stage 4d+).
2. `LITERATURE_REVALIDATION_SEARCH_SERVICE` is confirmed active (indexing resumed).
3. At least one end-to-end test confirms literature slice arrives at chair and is cited
   in `PLURALITY_BASIS` or `CONFLICT_RESOLUTION` without becoming a decision driver.

---

## Rollout Plan

### Stage 4a — Authority table and diagnostic tooling only

**No submission gating changes. No UI changes.**

Deliverables:
- Create `MIP.APP.AGENTIC_REVALIDATION_AUTHORITY` table (DDL).
- Create `MIP.APP.V_AGENTIC_AUTHORITY_LATEST` diagnostic view.
- Create diagnostic script or smoke query to populate/inspect authority rows manually.
- Validate: can read latest shadow session for an action, map to AUTHORITY_STATUS, insert row.
- Validate: staleness detection (hash mismatch, age, pack_version) works correctly.

**No operator-visible changes.**

### Stage 4b — Backend authority population (audit mode)

**Populates authority table automatically after shadow board completes. Audit-only.**

Deliverables:
- Add post-shadow-completion hook: when `orchestrate_shadow_board` finalizes a COMPLETE
  session, auto-call `_commit_agentic_authority_audit(action_id, session_id)`.
- This is an internal audit function, not an operator-facing commit. No endpoint yet.
- Monitor: after each shadow board run, verify authority row is populated correctly.
- Monitor: watch for unexpected DEGRADED/FAILED patterns.
- Validate: authority table accurately reflects shadow verdicts over a meaningful sample.

**No operator-visible changes. No Submit gating changes.**

### Stage 4c — LPA authority display (read-only)

**Adds "Apply Agentic Review" button and authority status chips. Submit gating unchanged.**

Deliverables:
- New endpoint: `POST /live/trades/actions/{action_id}/agentic-authority/commit` (see API section).
- `GET /live/trades/actions/{action_id}/agentic-authority` — returns latest authority row or 404.
- LPA: show authority status chip on shadow verdict card.
- LPA: show "Apply Agentic Review" button when shadow STATUS = COMPLETE and no committed row.
- LPA: show "Re-commit (stale)" when IS_STALE = true.
- LPA: show disagrees chip when DISAGREES_WITH_BASELINE = true.
- Submit gating: unchanged (`REVALIDATED_PASS + submission_allowed` only).

**Operator can see what the Stage 4d gate would do, without it blocking yet.**

### Stage 4d — Submit gating switch

**Submit now requires agentic authority. Deterministic C2 materialization still runs but
does not gate Submit.**

Deliverables:
- Update `submission_allowed` logic in `live.py` pending-decisions builder:
  add `agentic_authority_ok` condition.
- Update `execute_live_action` to check `AGENTIC_REVALIDATION_AUTHORITY.AUTHORITY_STATUS`.
- Update Submit button in LPA to show authority-blocked states (see UX section).
- Deploy `AGENTIC_MIN_CONFIDENCE_THRESHOLD` to `APP_CONFIG`.
- Confirm: deterministic C2 still runs and materializes COMMITTEE_FINAL_DECISION (unchanged).
- Confirm: safety revalidation still runs independently (unchanged).

**Smoke test gate:** run at least 5 complete shadow board sessions across different actions,
verify all map correctly to authority status, verify Submit reflects the agentic gate.

### Stage 4e — Deterministic C2 materialization demoted

**C2 still runs but no longer sets LIVE_ACTIONS.STATUS or writes COMMITTEE_RUN/VERDICT.**

Deliverables:
- Decouple `_materialize_structural_entry_committee_apply` from the orchestrate transaction.
- C2 still writes `COMMITTEE_HEARING` and `COMMITTEE_FINAL_DECISION` (for history/position health).
- LIVE_ACTIONS `STATUS` after orchestrate is set by a new agentic-authority-aware materializer
  (or remains at initial status until agentic authority is committed).
- Shadow board kickoff becomes independent: no longer requires C2 transaction to complete first.
  Shadow board can be triggered directly from the orchestrate endpoint regardless of C2 outcome.
- `COMMITTEE_VERDICT` and `COMMITTEE_RUN` writes become diagnostic/history only.

### Stage 4f — Quarantine and archive candidates

**No deletion yet — only moving code to diagnostic/archive scope.**

Candidates (after Stage 4e validates):
- `structural_entry_verdict_from_committee2_final` in `committee2_live_bridge.py`.
- `_materialize_structural_entry_committee_apply` (legacy path only).
- `committee_blocks_entry` flag in `submission_allowed` (replaced by agentic authority gate).
- `COMMITTEE_VERDICT` and `COMMITTEE_RUN` as operational tables (moved to diagnostic reads).
- References to `COMMITTEE_VERDICT` in `execute_live_action`.

**Not candidates for deletion in Stage 4:**
- `COMMITTEE_HEARING` — still written by C2 and used as evidence anchor for shadow.
- `COMMITTEE_FINAL_DECISION` — read by `SP_RUN_DAILY_POSITION_VERDICT` and position health.
- `committee_final_decision_commit_for_action` — still needed for position health lineage.
- `revalidate_live_action` — retained permanently as safety guard.
- `_run_opening_sanity_gate` — retained as pre-orchestrate sanity check.

---

## Risks and Mitigations

| Risk | Severity | Mitigation |
|---|---|---|
| **Stale shadow session** — operator applies authority from a session built on old evidence | High | EVIDENCE_PACK_HASH comparison at commit time; max-age check; IS_STALE flag blocks submit |
| **DEGRADED session grants authority** — degraded chair output used as APPROVE | High | DEGRADED flag and STAGE_REACHED < 5 both force AGENTIC_DEGRADED_NO_AUTHORITY; no positive authority without COMPLETE + not degraded + stage 5 |
| **Model timeout** — shadow board times out at 240s; action paused indefinitely | Medium | AGENTIC_FAILED_NO_AUTHORITY; offer "run new review"; no auto-retry that could loop; operator can re-run orchestrate to get fresh session |
| **Contradictory deterministic/agentic** — C2 says APPROVE, shadow says REJECT | Medium | DISAGREES_WITH_BASELINE chip shown; agentic verdict wins in Stage 4d; no silent C2 fallback; both records preserved in audit trail |
| **LIVE_ACTIONS compatibility** — existing downstream readers of COMMITTEE_VERDICT expect deterministic data | Medium | Stage 4d: COMMITTEE_VERDICT still written by C2; Stage 4e: new authority table is the gate, COMMITTEE_VERDICT becomes diagnostic; downstream readers (position health SPs) continue reading COMMITTEE_FINAL_DECISION until explicitly migrated |
| **Operator confusion** — two authority signals visible simultaneously | Low–Medium | Stages 4a–4c use clear visual hierarchy; deterministic baseline is collapsed/diagnostic (Stage 3); authority chip on shadow card is explicit; disagrees chip is informational |
| **Overblocking good trades** — agentic WAIT_RECLAIM on a genuinely valid setup | Medium | Stage 4c is display-only; Stage 4d is gating; operator has time in 4c to observe false positive rate; confidence threshold provides a second quality filter |
| **Silent deterministic approval reinstatement** — C2 path accidentally re-enabled after Stage 4d | High | `submission_allowed` logic explicitly requires agentic authority; `execute_live_action` explicitly checks AGENTIC_REVALIDATION_AUTHORITY; no fallback to C2 committee_blocks_entry alone |
| **Cost / performance** — shadow board runs 100–180s; 240s timeout; frequent degradation | Medium | Shadow board already runs in production; Stage 4a–4c is observation-only; DEGRADED sessions fail-closed; model and timeout already tuned to `claude-sonnet-4-6` / 240s |
| **Auditability** — unclear which authority source produced a given submit decision | Low | AGENTIC_REVALIDATION_AUTHORITY table captures full session snapshot, pack_version, deterministic baseline stance, disagrees flag, and override record; immutable per action |
| **Pack version regression** — shadow session built with an unsupported pack_version (e.g. legacy `1.0.0` or a future unreviewed `2.1.0`) | Medium | Explicit `SUPPORTED_PACK_VERSIONS` allow-list; PACK_VERSION_OK = false → AGENTIC_DEGRADED_NO_AUTHORITY; new pack versions require an explicit allow-list update reviewed against the evidence-pack contract |
| **Auto-audit row mistaken for authority** — background AUTO_AUDIT row exists, operator assumes Submit is gated by it | Medium | Stage 4d gate explicitly requires `AUTHORITY_MODE = 'OPERATOR_COMMITTED'`; LPA "Preview" badge on AUTO_AUDIT chip; "Apply Agentic Review" button visible until operator commits |
| **Concurrent commit race** — two operators or two auto-audit hooks fire simultaneously, both insert with IS_LATEST=TRUE | Low | Supersession + insert run in a single transaction; if both transactions commit, the later one's update sets the earlier's IS_LATEST = FALSE; idempotent for downstream readers (filter on IS_LATEST) |
| **Lost authority history on re-review** — operator re-runs Intelligence Review, prior authority decisions are overwritten and lost | Mitigated | Append-only model: every commit creates a new row; prior rows marked `IS_LATEST = FALSE, SUPERSEDED_AT, SUPERSEDED_BY` are retained in `V_AGENTIC_AUTHORITY_HISTORY` |
| **Shadow board not triggered** — SHADOW_BOARD_ENABLED = false in APP_CONFIG | Low | AGENTIC_FAILED_NO_AUTHORITY; operator cannot submit until review is run; circuit-breaker exists if shadow board is down for maintenance |

---

## Exact Implementation Tasks for Stage 4a

Stage 4a is additive only. No runtime behavior changes. No UI changes. No Submit gating changes.

### 4a-1: Create AGENTIC_REVALIDATION_AUTHORITY table (append-only)

File: `MIP/SQL/app/545_agentic_authority_tables.sql`

- DDL for `MIP.APP.AGENTIC_REVALIDATION_AUTHORITY` (see schema above) including:
  - `AUTHORITY_MODE`, `IS_LATEST`, `SUPERSEDED_AT`, `SUPERSEDED_BY`, `COMMITTED_BY` columns
  - **No** UNIQUE constraint on ACTION_ID — append-only history
- DDL for diagnostic views:
  - `V_AGENTIC_AUTHORITY_LATEST` — latest non-superseded row per action (any mode)
  - `V_AGENTIC_AUTHORITY_LATEST_OPERATOR` — latest OPERATOR_COMMITTED row per action
  - `V_AGENTIC_AUTHORITY_HISTORY` — full audit trail
- `COMMENT ON TABLE` documenting that authority is advisory until Stage 4d, that AUTO_AUDIT
  rows never gate Submit, and that this table is append-only.
- `GRANT SELECT, INSERT, UPDATE ON MIP.APP.AGENTIC_REVALIDATION_AUTHORITY TO ROLE MIP_UI_API_ROLE`.
- Deploy to Snowflake. Smoke:
  - `SELECT COUNT(*) FROM MIP.APP.AGENTIC_REVALIDATION_AUTHORITY` → 0 rows, no error.
  - `SELECT * FROM MIP.APP.V_AGENTIC_AUTHORITY_LATEST LIMIT 1` → compiles.
  - `SELECT * FROM MIP.APP.V_AGENTIC_AUTHORITY_LATEST_OPERATOR LIMIT 1` → compiles.

### 4a-2: Create authority mapping module

File: `MIP/apps/mip_ui_api/app/committee/agentic_authority.py`

Module-level constant:
```python
SUPPORTED_PACK_VERSIONS = frozenset({"2.0.0"})
```

Functions:
- `is_pack_version_supported(pack_version: str | None) -> bool` — explicit allow-list lookup.
- `map_shadow_to_authority_status(session: dict, c2_stance: str | None, config: dict) -> str`
  Uses the mapping logic from the taxonomy section. Reads `AGENTIC_MIN_CONFIDENCE_THRESHOLD`
  from config. Uses `is_pack_version_supported`, not string comparison.
- `build_staleness_check(session: dict, current_pack_hash: str, max_age_minutes: int) -> dict`
  Returns `{is_stale: bool, stale_reason: str | None, session_age_minutes: int}`.
- `build_authority_row(action_id, session, c2_final_decision, config, *, authority_mode, committed_by) -> dict`
  Assembles the full row. `authority_mode` must be `'AUTO_AUDIT'` or `'OPERATOR_COMMITTED'`.
  `committed_by` is the user id for operator commits, or `'system_shadow_audit'` for auto-audit.
- `insert_authority_row_with_supersession(conn, row: dict) -> str`
  Runs the transactional supersede-then-insert pattern. Returns the new `AUTHORITY_ID`.
  Marks all prior `IS_LATEST = TRUE` rows for the same `ACTION_ID` as superseded before
  inserting the new row.

### 4a-3: Create diagnostic population script

File: `cursorfiles/populate_agentic_authority_diagnostic.py`

CLI: `python populate_agentic_authority_diagnostic.py --action-id <id> --mode AUTO_AUDIT`

Behavior:
- Fetches latest shadow session for the action's hearing_id.
- Fetches COMMITTEE_FINAL_DECISION for the action.
- Calls `build_authority_row` with `authority_mode='AUTO_AUDIT'` and `committed_by='cursor_diagnostic'`.
- Calls `insert_authority_row_with_supersession`.
- Prints the new AUTHORITY_ID, AUTHORITY_STATUS, and any prior row that was superseded.
- Does not change any LIVE_ACTIONS fields.

### 4a-4: Create smoke queries

File: `MIP/SQL/smoke/06_agentic_authority_smoke.sql`

Smoke checks:
1. Table exists and is empty: `SELECT COUNT(*) FROM MIP.APP.AGENTIC_REVALIDATION_AUTHORITY`
2. All three views compile.
3. For a known COMPLETE shadow session: run diagnostic script, verify row inserted with
   `AUTHORITY_MODE = 'AUTO_AUDIT'`, `IS_LATEST = TRUE`.
4. Run diagnostic script twice on the same action: verify the first row is superseded
   (`IS_LATEST = FALSE`, `SUPERSEDED_AT` set, `SUPERSEDED_BY` populated) and only the
   second row has `IS_LATEST = TRUE`.
5. For a known DEGRADED session: verify AUTHORITY_STATUS = AGENTIC_DEGRADED_NO_AUTHORITY.
6. For a known pre-Phase-4 session (PACK_VERSION not in SUPPORTED_PACK_VERSIONS): verify
   PACK_VERSION_OK = false and AUTHORITY_STATUS = AGENTIC_DEGRADED_NO_AUTHORITY.
7. Staleness: verify a session with mismatched EVIDENCE_PACK_HASH gets IS_STALE = true.
8. Latest-operator view: with only AUTO_AUDIT rows present,
   `V_AGENTIC_AUTHORITY_LATEST_OPERATOR` returns zero rows for that ACTION_ID.

### 4a-5: APP_CONFIG additions

Add to `APP_CONFIG`:
- `AGENTIC_AUTHORITY_ENABLED` = `false` (circuit breaker for Stage 4d gating)
- `AGENTIC_MIN_CONFIDENCE_THRESHOLD` = `0.40`
- `AGENTIC_MAX_SESSION_AGE_MINUTES` = `240`
- `AGENTIC_AUTO_AUDIT_ENABLED` = `false` (controls whether Stage 4b background hook writes
  AUTO_AUDIT rows; left off until Stage 4b is explicitly deployed)

Smoke: `SELECT CONFIG_KEY, CONFIG_VALUE FROM MIP.APP.APP_CONFIG WHERE CONFIG_KEY LIKE 'AGENTIC_%'` → 4 rows.

---

## Exact Implementation Tasks for Stage 4b

Stage 4b adds background authority population after shadow board completes. Still audit-only.

### 4b-1: Auto-populate AUTO_AUDIT authority after shadow board completion

File: `MIP/apps/mip_ui_api/app/committee/shadow_board.py`

In `_finalize_session_sync` (called after chair completes), add a guarded background hook
that writes an `AUTHORITY_MODE = 'AUTO_AUDIT'` row:

```python
if action_id and _is_truthy(config.get("AGENTIC_AUTO_AUDIT_ENABLED")):
    try:
        await _audit_agentic_authority_for_action(
            action_id=action_id,
            session=fetched_session,
            conn=conn,
            config=config,
            committed_by="system_shadow_audit",
        )
    except Exception as exc:
        logger.warning(
            "Agentic authority AUTO_AUDIT commit failed",
            extra={"action_id": action_id, "session_id": fetched_session["session_id"], "error": str(exc)},
        )
        # Never raise — shadow board result must not be blocked by audit-row failure.
```

`_audit_agentic_authority_for_action` builds the row via `build_authority_row(..., authority_mode='AUTO_AUDIT')`
and writes it via `insert_authority_row_with_supersession`. If `ACTION_ID` is missing (e.g.
session triggered from a diagnostic endpoint without action context), skip silently and log.

Important: this background hook **never gates Submit**. It only populates observation rows.
Stage 4d gating reads `AUTHORITY_MODE = 'OPERATOR_COMMITTED'` only.

### 4b-2: Add ACTION_ID to shadow board session context

Currently `kickoff_shadow_board_for_snapshot` receives `hearing_id`, `proposal_id`,
`snapshot_id`, `evidence_pack_hash`. Add `action_id` as an optional parameter so the
background audit commit can reference it without a separate lookup. When the function is
invoked from a non-action context (manual diagnostic), `action_id` remains `None` and the
audit hook is skipped.

### 4b-3: Append-only supersession semantics for AUTO_AUDIT

If a shadow session completes for an action that already has previous authority rows (e.g.
from prior Intelligence Review runs), `insert_authority_row_with_supersession` marks all
existing `IS_LATEST = TRUE` rows as superseded — including any prior `OPERATOR_COMMITTED`
rows. This is the correct behavior: a fresh shadow session for the same action invalidates
older verdicts. The operator must re-commit (or the auto-audit will be the only current row,
which does not gate Submit).

LPA must detect this case and prompt "Re-commit (stale)" when the latest authority row is
`AUTHORITY_MODE = 'AUTO_AUDIT'` after a previous `OPERATOR_COMMITTED` row was superseded.

### 4b-4: Monitor and validate

After deploying 4b (with `AGENTIC_AUTO_AUDIT_ENABLED = true`), observe:
- AUTO_AUDIT rows appear after each completed shadow board session that has an action_id.
- `V_AGENTIC_AUTHORITY_LATEST` shows the latest auto-audit row for every recent action.
- `V_AGENTIC_AUTHORITY_LATEST_OPERATOR` remains empty until Stage 4c is deployed.
- AUTHORITY_STATUS distribution matches expected shadow stances.
- DEGRADED sessions correctly map to AGENTIC_DEGRADED_NO_AUTHORITY.
- DISAGREES_WITH_BASELINE correctly detects mismatches.
- No authority write failures block shadow board completion.

Minimum validation sample before proceeding to Stage 4c: 10 complete shadow sessions
across at least 3 different actions, covering at least 2 different AUTHORITY_STATUS values.

---

## Exact Implementation Tasks for Stage 4c

Stage 4c adds the operator-facing "Apply Agentic Review" button and authority display chips
in LPA. **Submit gating remains unchanged.**

### 4c-1: New API endpoints

File: `MIP/apps/mip_ui_api/app/routers/live.py` (or new `agentic_authority.py` router)

`POST /live/trades/actions/{action_id}/agentic-authority/commit`
- Always writes `AUTHORITY_MODE = 'OPERATOR_COMMITTED'`.
- Runs the supersede-then-insert transaction.
- See API design section for full spec.

`GET /live/trades/actions/{action_id}/agentic-authority`
- Returns latest authority row from `V_AGENTIC_AUTHORITY_LATEST` (any mode).
- Includes a derived field `gate_eligible: bool` set to true only when the row has
  `AUTHORITY_MODE = 'OPERATOR_COMMITTED'`, `IS_STALE = FALSE`, and
  `AUTHORITY_STATUS IN ('AGENTIC_APPROVE', 'AGENTIC_APPROVE_REDUCED')`.
- Used by LPA to decide which chip and button to render.

`GET /live/trades/actions/{action_id}/agentic-authority/history`
- Returns ordered history from `V_AGENTIC_AUTHORITY_HISTORY` for diagnostic use.

### 4c-2: LPA — authority status chip

File: `MIP/apps/mip_ui_web/src/pages/LivePortfolioActivity.jsx`

- Add `agenticAuthorityByAction` state (keyed by action_id).
- Fetch authority row when shadow verdict card is displayed.
- Render authority chip on shadow verdict card (see UX table above).
- When the latest row is `AUTHORITY_MODE = 'AUTO_AUDIT'`, show the chip with a small
  "Preview" badge so the operator knows this is observation, not a committed authority.
- When the latest row is `AUTHORITY_MODE = 'OPERATOR_COMMITTED'`, show the chip with a
  "Committed" badge.
- Render disagrees chip when DISAGREES_WITH_BASELINE = true.

### 4c-3: LPA — "Apply Agentic Review" button

File: `MIP/apps/mip_ui_web/src/pages/LivePortfolioActivity.jsx`

- Show button when: shadow STATUS = COMPLETE AND latest authority row is missing or has
  `AUTHORITY_MODE = 'AUTO_AUDIT'`.
- Show "Re-commit (stale)" when: latest OPERATOR_COMMITTED row exists but `IS_STALE = true`,
  or when a newer AUTO_AUDIT row has superseded a prior OPERATOR_COMMITTED row.
- Hide the button when: latest OPERATOR_COMMITTED row exists and is not stale.
- On click: call `POST .../agentic-authority/commit` with session_id and hearing_id from the
  current shadow result.
- On success: update `agenticAuthorityByAction` state with the new row; show updated chip
  with "Committed" badge.

### 4c-4: LPA — Stage 4c informational submit message

When authority status is WAIT_RECLAIM / DEFER / REJECT / DEGRADED / FAILED:
show a greyed informational chip: "Would block submit in Stage 4d".
Do not disable Submit yet.

### 4c-5: CSS

File: `MIP/apps/mip_ui_web/src/pages/LivePortfolioActivity.css`

Add styles for authority chips:
- `.lpa-authority-approve` — green chip.
- `.lpa-authority-approve-reduced` — amber chip.
- `.lpa-authority-block` — orange chip.
- `.lpa-authority-reject` — red chip.
- `.lpa-authority-degraded` — grey chip.
- `.lpa-authority-disagrees` — small Δ chip.

---

## Deterministic C2 Paths That Remain Temporarily

The following deterministic C2 paths **must not be removed until Stage 4e or later**:

| Path | Why it must remain | Safe to remove in |
|---|---|---|
| `orchestrate_committee2_structural_entry` (full function) | Runs C2 hearing, kicks off shadow, remains the trigger point | Stage 4e (after shadow trigger is decoupled) |
| `compute_hearing_bundle` + `_persist_hearing_atomic` | COMMITTEE_HEARING is still the evidence anchor for shadow | Stage 4f (after shadow anchor is redesigned) |
| `committee_final_decision_commit_for_action` | Read by SP_RUN_DAILY_POSITION_VERDICT and position health | After position health is migrated to shadow authority |
| `_materialize_structural_entry_committee_apply` | Sets LIVE_ACTIONS STATUS until Stage 4e decouples it | Stage 4e |
| `structural_entry_verdict_from_committee2_final` (`committee2_live_bridge.py`) | Maps C2 stance for materialize; needed until Stage 4e | Stage 4e |
| `COMMITTEE_VERDICT` write in `execute_live_action` | Additional execute gate; replaced by agentic authority in Stage 4d | Stage 4e (after authority gate is confirmed) |
| `committee_blocks_entry` in `submission_allowed` | Part of submission gate; replaced in Stage 4d but needs coexistence | Stage 4e |
| `SP_RUN_DAILY_POSITION_VERDICT` reading `COMMITTEE_FINAL_DECISION` | Position health still uses deterministic final decision | Future position health migration |
| Legacy SSE `/committee/live-prompt` + `committee/apply` | Used by non-structural-entry rows and sync/exit flows | Separate retirement track (not Stage 4) |

---

## Open Questions

1. **Shadow board trigger independence.** Currently the shadow board is kicked off inside
   `orchestrate_committee2_structural_entry` after the C2 transaction. Stage 4e requires
   decoupling this. Design question: should shadow board be triggerable directly from the
   orchestrate endpoint before C2 runs, or should there be a separate `POST .../shadow-board/trigger`
   endpoint? The answer affects whether C2 can be bypassed in Stage 4e without
   breaking the shadow kickoff path.

2. **COMMITTEE_HEARING as shadow evidence anchor.** The shadow board uses `COMMITTEE_HEARING`
   as the primary evidence source (via `_fetch_hearing_data`). In Stage 4e, if C2 no longer
   runs the hearing computation, what creates or updates `COMMITTEE_HEARING`?
   Option: keep C2 running hearing refresh only (not materialization); option: rebuild
   shadow evidence fetch from STRUCTURAL_TRADE_PROPOSALS directly.

3. **Position health migration.** `SP_RUN_DAILY_POSITION_VERDICT` reads `COMMITTEE_FINAL_DECISION`
   for daily position health. After Stage 4e, if deterministic C2 is demoted, position health
   needs an equivalent agentic source. This is a separate migration track that must be designed
   before Stage 4f can proceed.

4. **Action_id availability in shadow session.** Currently `kickoff_shadow_board_for_snapshot`
   does not take `action_id`. Stage 4b requires adding it. Risk: in some code paths (e.g.
   manual shadow board run from the diagnostics endpoint), there may be no `action_id`
   associated with the session. The `_commit_agentic_authority_audit` function must handle
   the missing `action_id` gracefully.

5. **Multi-run authority handling.** Resolved by the append-only model:
   `AGENTIC_REVALIDATION_AUTHORITY` has no UNIQUE constraint on `ACTION_ID`. Every commit
   inserts a new row; prior rows are marked `IS_LATEST = FALSE, SUPERSEDED_AT, SUPERSEDED_BY`
   inside the same transaction. Full history is retained in `V_AGENTIC_AUTHORITY_HISTORY`.
   Open follow-up: define a retention policy for very old authority rows (probably none for
   the first year so we can study override behavior, then optional archival).

6. **Confidence threshold calibration.** The recommended default of `0.40` for
   `AGENTIC_MIN_CONFIDENCE_THRESHOLD` is a starting point based on shadow board output
   observation. This threshold should be reviewed after Stage 4b data accumulates.
   A threshold that is too high will cause excessive DEGRADED_NO_AUTHORITY; too low will
   allow uncertain verdicts to grant full authority.

7. **Non-structural-entry rows.** The legacy SSE committee path (`/committee/live-prompt`
   + `/committee/apply`) is used for non-structural-entry actions (exits, syncs, legacy
   patterns). Stage 4's authority design focuses on structural-entry. These other paths
   have their own retirement track and are not addressed here.

8. **Bake-off reads of COMMITTEE_FINAL_DECISION.** `SP_REFRESH_COMMITTEE_BAKEOFF` reads
   `COMMITTEE_FINAL_DECISION`. If Stage 4e reduces the writing of COMMITTEE_FINAL_DECISION,
   bakeoff data will become sparser. This is acceptable (bake-off is diagnostic only), but
   should be acknowledged in the Stage 4e task.

9. **Should AUTO_AUDIT ever gate Submit?** Default policy: no — only OPERATOR_COMMITTED
   gates Submit. There may later be a narrow opt-in policy (e.g. fully automated trading
   mode) where a high-confidence AUTO_AUDIT row that exactly matches the deterministic
   baseline could gate Submit without explicit operator click. This is **not** part of
   Stage 4 and would require a separate design review with explicit policy approval.
   Documenting here so the AUTHORITY_MODE column makes such a future policy possible
   without schema change.

10. **Stage 4e shadow trigger decoupling design.** If Stage 4e decouples the shadow board
    from `orchestrate_committee2_structural_entry`, what triggers the shadow board? Options:
    (a) a separate `POST .../shadow-board/trigger` endpoint called by LPA before orchestrate;
    (b) move shadow kickoff into a new "agentic intelligence review" endpoint that replaces
    the user-facing orchestrate. Choice affects whether deterministic C2 remains the
    upstream trigger or is fully bypassed.
