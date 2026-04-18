# Committee 2.0 cutover — legacy structural committee removal

## 1) Entry points that used legacy structural committee

### Backend (`mip_ui_api`)

| Location | Endpoint / behavior | Pre-cutover |
|----------|---------------------|------------|
| [`live.py`](../apps/mip_ui_api/app/routers/live.py) | `POST /live/trades/actions/{id}/committee/run` | Structural branch called `run_structural_committee(action)` then wrote `COMMITTEE_RUN` / `COMMITTEE_VERDICT` / `LIVE_ACTIONS`. |
| [`live.py`](../apps/mip_ui_api/app/routers/live.py) | `POST /live/trades/actions/{id}/committee/apply` | Structural branch **re-ran** `run_structural_committee` then inserted completed run + verdict. |
| [`live.py`](../apps/mip_ui_api/app/routers/live.py) | `GET /live/trades/actions/{id}/committee/live-prompt` (SSE) | Structural branch streamed fake “Chair” then `run_structural_committee` role summaries. |
| [`live.py`](../apps/mip_ui_api/app/routers/live.py) | `GET /live/trades/actions/{id}/revalidate/live-prompt` (SSE) | Structural branch streamed `run_structural_committee` role outputs. |
| [`live.py`](../apps/mip_ui_api/app/routers/live.py) | `execute_live_action` (structural entry self-heal) | Called `run_structural_committee` only to recover `joint_decision` TP/SL when missing. |
| [`structural_committee.py`](../apps/mip_ui_api/app/services/live_intelligence/structural_committee.py) | `run_structural_committee` | Deterministic **entry** evaluator (parallel to Committee 2.0). |

**Structural EXIT:** No committee or entry-style validation — `build_structural_exit_execution_only_verdict` (broker position qty only); labels `STRUCTURAL_EXIT_EXECUTION_ONLY`. See [`structural_exit_committee_removal_spec.md`](./structural_exit_committee_removal_spec.md).

### Frontend (`mip_ui_web`)

| Location | Behavior |
|----------|----------|
| [`LivePortfolioActivity.jsx`](../apps/mip_ui_web/src/pages/LivePortfolioActivity.jsx) | “Committee revalidation” → `committee/apply` + SSE `committee/live-prompt`. Copy still referred to “committee” generically. |
| [`LiveIntelligenceCockpit.jsx`](../apps/mip_ui_web/src/pages/LiveIntelligenceCockpit.jsx) | Copy mentions “AI committee” for events (legacy mental model). |
| [`SymbolTracker.jsx`](../apps/mip_ui_web/src/pages/SymbolTracker.jsx) | Local `evaluateCommittee` — **not** `MIP.LIVE` committee; **unchanged** (living chart heuristic). |

### Already Committee 2.0

- [`committee.py`](../apps/mip_ui_api/app/routers/committee.py) — hearing open/get/refresh/commit.
- [`StructuralCommitteeHearing.jsx`](../apps/mip_ui_web/src/pages/StructuralCommitteeHearing.jsx), [`StlHearingLaunch.jsx`](../apps/mip_ui_web/src/components/structural-timeline/StlHearingLaunch.jsx).

---

## 2) Committee 2.0 → execution translation contract

**Source of truth for structural ENTRY:** `MIP.APP.COMMITTEE_FINAL_DECISION` row where `ACTION_ID` matches the live action (set when user **commits** the hearing: `POST /committee/hearing/{hearing_id}/commit` with `action_id`).

**Validation:** If `LIVE_ACTIONS.PROPOSAL_ID` and `COMMITTEE_FINAL_DECISION.PROPOSAL_ID` are both non-null, they **must match** or the sync is rejected (`409`).

### Stance → live verdict (`recommendation`, `blocked`, `should_enter`)

| Committee 2.0 `STANCE` | `blocked` | `recommendation` | `joint_decision.should_enter` |
|------------------------|-----------|-------------------|--------------------------------|
| `DENY` | `true` | `BLOCK` | `false` |
| `DEFER` | `true` | `BLOCK` | `false` |
| `WAIT_RECLAIM` | `true` | `BLOCK` | `false` |
| `APPROVE_REDUCED` | `false` | `PROCEED_REDUCED` | `true` |
| `APPROVE` | `false` | `PROCEED` | `true` |

### Size posture → `size_factor` / `position_size_factor`

| `POSTURE_JSON.size_posture` (if present) | `size_factor` |
|------------------------------------------|---------------|
| `ZERO` | `0.0` (treated as block; should not occur if stance is approving) |
| `REDUCED` | `0.5` |
| `FULL` | `1.0` |

If missing, derive from stance: `APPROVE_REDUCED` → `0.5`, `APPROVE` → `1.0`, non-approving → `0.0`.

### Trail / entry posture (advisory in `joint_decision`)

- `POSTURE_JSON.trail_posture` copied into `joint_decision.trail.note` prefix (`DEFENSIVE` / `STANDARD`) without replacing broker bracket gates.
- Entry zone / invalidation / TP/SL **numeric** contract: built from **`LIVE_ACTIONS`** + existing structural TP/SL helpers (`build_structural_entry_joint_decision`) so IB submit gates stay consistent — Committee 2.0 does **not** invent new TP/SL; it gates **whether** to proceed and **size_factor**.

### Reason codes

- Always include `COMMITTEE2_SYNCED`.
- If blocked: `COMMITTEE2_STANCE_BLOCKS_ENTRY` + stance string.
- Preserve `COMMITTEE2_HEARING_ID`, `COMMITTEE2_FINAL_DECISION_ID` inside `VERDICT_JSON` / envelope where existing Phase-3 helpers expect metadata.

---

## 3) Legacy code: removed vs disabled vs bypassed

| Item | Treatment |
|------|-----------|
| `run_structural_committee` | **Removed** (ENTRY was already on Committee 2.0; EXIT replaced by execution-only pass-through). |
| TP/SL shell for structural entry | **Extracted** to `build_structural_entry_joint_decision(action)` — used by C2 bridge + execute self-heal (no full entry committee). |
| `committee/run` + `committee/apply` structural **ENTRY** | **Bypass** old evaluator; **load** `COMMITTEE_FINAL_DECISION` by `action_id`. |
| `committee/run` + `committee/apply` structural **EXIT** | Execution-only verdict (position gate); no `COMMITTEE_FINAL_DECISION`. |
| SSE `live-prompt` / `revalidate/live-prompt` structural **ENTRY** | Replay Committee 2.0 committed summaries or error if missing final decision. |
| SSE structural **EXIT** | Single execution summary + `committee_model` `STRUCTURAL_EXIT_EXECUTION_ONLY`. |
| `MIP.LIVE.COMMITTEE_*` tables | **Retained** as execution **materialization** layer (single write path from C2 sync) so submit/ledger code stays stable. |

---

## 4) End-to-end flow (structural ENTRY)

1. Operator opens **Committee 2.0** for `PROPOSAL_ID`, refreshes hearing as needed.
2. Operator commits: `POST /committee/hearing/{hearing_id}/commit` with `{ "action_id": "<LIVE_ACTION_ID>" }`.
3. `COMMITTEE_FINAL_DECISION` row stores frozen packet + `ACTION_ID`.
4. In **Live Portfolio Activity**, operator runs **“Sync Committee 2.0”** (same endpoint as before: `POST .../committee/run` or `.../committee/apply` — both load final decision for structural entry).
5. API maps final decision → legacy verdict shape → writes `COMMITTEE_RUN` / `COMMITTEE_VERDICT`, updates `LIVE_ACTIONS` like today.
6. Operator continues **revalidate / approve / submit**; broker-safe gates unchanged.

---

## 5) Validation / smoke

- **API:** Structural `committee/run` with no final decision → `409` + `COMMITTEE2_FINAL_DECISION_REQUIRED`.
- **API:** Mismatched `PROPOSAL_ID` → `409` + `COMMITTEE2_PROPOSAL_MISMATCH`.
- **SQL:** [`33_committee2_hearing_smoke.sql`](../SQL/smoke/33_committee2_hearing_smoke.sql) + optional row checks on `COMMITTEE_FINAL_DECISION.ACTION_ID`.
- **Manual:** Commit hearing with `action_id` → sync from LPA → `COMMITTEE_VERDICT.SIZE_FACTOR` matches stance mapping.

---

## Implementation note

Single structural **entry** authority: **Committee 2.0 commit → LIVE materialization**. Structural **exit**: **execution-only** pass-through (`STRUCTURAL_EXIT_EXECUTION_ONLY`), not Committee 2.0.

---

## 6) LPA inline hearing (Phase 1)

- **Default UX:** [LivePortfolioActivity.jsx](../apps/mip_ui_web/src/pages/LivePortfolioActivity.jsx) uses **`POST /live/trades/actions/{action_id}/committee2/orchestrate`** as the primary structural **entry** action (**Run Committee 2.0** / **Refresh decision**). The response includes **`inline_hearing`**: exhibit-oriented fields (geometry hero, path quality, regime continuity, protection, symbol fingerprint, what-changed strip, chair board). See [lpa_inline_hearing_phase1.md](./lpa_inline_hearing_phase1.md).
- **Engine:** [engine.py](../apps/mip_ui_api/app/committee/engine.py) produces differentiated confidence, evidence-linked chair lines, and non-placeholder fingerprint/symbol behavior when metrics exist; [committee.py](../apps/mip_ui_api/app/routers/committee.py) `_live_context` supplies **`recent_bar_trace`** (daily closes, oldest→newest) for the hero sparkline.
- **Standalone:** [StructuralCommitteeHearing.jsx](../apps/mip_ui_web/src/pages/StructuralCommitteeHearing.jsx) — when `action_id` is present in the query string, commit binds to it by default; manual override lives under **Advanced**.

---

## 7) Phase 2 — Context confirmation layer (roadmap)

- **Spec:** [phase2_context_confirmation_layer.md](./phase2_context_confirmation_layer.md) — **politician trade disclosure** context (U.S. STOCK Act–style / [Capitol Trades](https://www.capitoltrades.com/)–class ingest concept) in Snowflake **first**, **compact card** on LPA inline + hearing page; optional **silent** live lookup only as non-blocking enrichment; **not** a primary signal and **not** LLM phrasing (Phase 3).
