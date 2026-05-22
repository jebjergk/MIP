# MIP Deterministic Committee 2.0 Retirement Audit

**Date:** 2026-05-22  
**Status:** Audit and retirement planning only. No code deleted. No runtime, API, UI, or Snowflake DDL changes made in this task.  
**Scope:** All deterministic Committee 2.0 code, tables, views, endpoints, UI surfaces, and config flags that currently feed the LPA / operator intelligence path, plus the dependencies that must move to the agentic / shadow path before retirement is safe.

---

## Summary

Deterministic Committee 2.0 ("C2") today is the **authoritative intelligence layer** that LPA uses to convert a structural trade proposal into a committed verdict and a materialized `LIVE_ACTIONS` row that the operator can submit to IBKR. Its writers are entirely in **Python** (`MIP/apps/mip_ui_api/app/committee/engine.py` plus persistence helpers in `routers/committee.py`), not in Snowflake stored procedures. The five core APP tables (`STRUCTURAL_PROPOSAL_SNAPSHOT`, `COMMITTEE_HEARING`, `COMMITTEE_ROLE_OUTPUT`, `COMMITTEE_EVIDENCE_ARTIFACT`, `COMMITTEE_FINAL_DECISION`) are defined in `MIP/SQL/app/540_committee2_tables.sql` and written by that Python engine through three FastAPI write endpoints under `/committee/hearing/*` and one orchestration endpoint at `/live/trades/actions/{id}/committee2/orchestrate`.

The **price/bar/news guard** at `POST /live/trades/actions/{id}/revalidate` is **not Committee 2.0**. It is a lightweight safety check on bar freshness, price deviation, and news flags, and it does not call the engine, persist a hearing, write a final decision, or materialize a verdict. It must be preserved through retirement as a safety utility.

The **agentic / shadow committee** (`541_shadow_board_tables.sql`, `542_shadow_board_agents.sql`, `MIP/apps/mip_ui_api/app/committee/shadow_board.py`) currently runs **after** C2 commits, in an asyncio background task, with the Phase 4-aware `pack_version = 2.0.0` evidence pack. It produces a full chair ruling and specialist positions, but it **does not yet write `COMMITTEE_FINAL_DECISION`**, it **does not materialize `LIVE_ACTIONS`**, and it is presented in the UI as a non-authoritative **Shadow Board** panel. Until that gap is closed, C2 cannot be retired.

This audit identifies:

- 11 Python files, 8 SQL files (540, 541, 542, 543, 561, 565, 566, 600, 601), 1 router (`committee.py`), key sections of `live.py`, ~9 React components, and 4 critical `APP_CONFIG` keys.
- The exact materialization chain that turns a C2 stance into a submittable order: `compute_hearing_bundle` → `_persist_hearing_atomic` → `committee_final_decision_commit_for_action` → `_materialize_structural_entry_committee_apply` → `MIP.LIVE.COMMITTEE_RUN` / `COMMITTEE_VERDICT` / `LIVE_ACTIONS`.
- A clean separation between **C2 intelligence logic** (retirement target) and **the price/bar/news safety guard** (keep).
- A 6-stage retirement plan that never deletes code before the agentic path can materialize `LIVE_ACTIONS` and never retires `COMMITTEE_FINAL_DECISION` before its replacement is wired.

**Hard finding:** the agentic path is not yet authoritative anywhere — not in `LIVE_ACTIONS`, not in `MIP.LIVE.COMMITTEE_VERDICT`, not in the operator UI. Retirement Stages 1–2 (label/visibility) are safe today; Stages 3–6 require the new agentic materialization to exist first.

---

## Deterministic Committee 2.0 Inventory

### Python files

| File | Role | Class |
|---|---|---|
| `MIP/apps/mip_ui_api/app/committee/__init__.py` | Package marker — describes "Committee 2.0 — structural hearing room (deterministic engine)" | Doc only |
| `MIP/apps/mip_ui_api/app/committee/engine.py` | **Core deterministic engine.** Defines `compute_hearing_bundle(snapshot, live_context)` and `bundle_to_db_json()`. Implements stance computation (`APPROVE / APPROVE_REDUCED / WAIT_RECLAIM / DEFER / DENY`), six role templates, six artifact templates, chair output, deltas, continuous confidence scoring, and `BLOCK_REASON_TAXONOMY`. **No LLM, no Cortex calls.** | C2 core |
| `MIP/apps/mip_ui_api/app/committee/committee2_live_bridge.py` | Maps `COMMITTEE_FINAL_DECISION` to the legacy LIVE verdict shape (`blocked`, `recommendation`, `size_factor`, `joint_decision`, role outputs). Used by every C2 live entry point. | C2 core |
| `MIP/apps/mip_ui_api/app/committee/intraday_substantiation.py` | Optional 15m IBKR-bars evidence artifact. **Evidence-only**, never alters stance. | C2 exhibit |
| `MIP/apps/mip_ui_api/app/committee/public_disclosure_context.py` | Phase 2 politician-disclosure exhibit (read-only). | C2 exhibit |
| `MIP/apps/mip_ui_api/app/committee/live_politician_disclosure.py` | Optional live disclosure enrichment. | C2 exhibit |
| `MIP/apps/mip_ui_api/app/committee/capitol_trades_poc_scrape.py` | POC HTML scraper for Capitol Trades link-out. | C2 exhibit |
| `MIP/apps/mip_ui_api/app/committee/shadow_types.py` | Pydantic types and `build_shadow_evidence_pack` (now pack_version 2.0.0). | Shadow (agentic) |
| `MIP/apps/mip_ui_api/app/committee/shadow_board.py` | Cortex-driven shadow orchestration. Reads C2 hearing rows; never writes `COMMITTEE_FINAL_DECISION`. | Shadow (agentic) |
| `MIP/apps/mip_ui_api/app/committee/shadow_cortex_client.py` | Cortex Agents REST client for shadow. | Shadow (agentic) |
| `MIP/apps/mip_ui_api/app/committee/shadow_linkage.py` | Links real IBKR fills to shadow sessions via `EVIDENCE_PACK_HASH`. | Shadow observability |

### SQL files

| File | Objects | Class |
|---|---|---|
| `MIP/SQL/app/540_committee2_tables.sql` | `STRUCTURAL_PROPOSAL_SNAPSHOT`, `COMMITTEE_HEARING`, `COMMITTEE_ROLE_OUTPUT`, `COMMITTEE_EVIDENCE_ARTIFACT`, `COMMITTEE_FINAL_DECISION` | **C2 core tables** |
| `MIP/SQL/app/541_shadow_board_tables.sql` | `SHADOW_EVIDENCE_PACK_CACHE`, `SHADOW_BOARD_SESSION`, `SHADOW_SPECIALIST_POSITION`, `SHADOW_CONFLICT_MAP`, `SHADOW_CHALLENGE_TURN`, `SHADOW_REVISION_TURN`, `SHADOW_CHAIR_RULING` | Shadow (agentic) |
| `MIP/SQL/app/542_shadow_board_agents.sql` | `GET_SHADOW_EVIDENCE_SLICE` proc; 7 Cortex agents; seeds `SHADOW_BOARD_ENABLED`, `SHADOW_BOARD_MODEL`, `SHADOW_BOARD_TIMEOUT_SEC` in `APP_CONFIG` | Shadow (agentic) |
| `MIP/SQL/app/543_shadow_trade_linkage.sql` | `SHADOW_TRADE_LINKAGE` table; `SHADOW_TRADE_LINKAGE_DEDUPE` proc | Shadow observability |
| `MIP/SQL/app/561_sp_run_proposal_board.sql` | `SP_RUN_PROPOSAL_BOARD` (deterministic) — writes `STRUCTURAL_PROPOSAL_SNAPSHOT` | C2 input source |
| `MIP/SQL/app/565_sp_run_proposal_board_phase4_symbol_dossier.sql` | Phase 4 variant of `SP_RUN_PROPOSAL_BOARD` (writes snapshot) | C2 input source |
| `MIP/SQL/app/566_sp_run_proposal_board_phase4_symbol_dossier_combined.sql` | Combined Phase 4 board + snapshot writer | C2 input source |
| `MIP/SQL/app/600_committee_bakeoff_tables.sql` | `COMMITTEE_BAKEOFF_LATCH`, `COMMITTEE_BAKEOFF_OUTCOME`, `COMMITTEE_BAKEOFF_CONFIG` | Diagnostic only |
| `MIP/SQL/app/601_sp_refresh_committee_bakeoff.sql` | `SP_REFRESH_COMMITTEE_BAKEOFF` | Diagnostic only |
| `MIP/SQL/migrations/20260418_committee2_config_grants.sql` | `COMMITTEE2_ENABLED` config + UI grants | C2 master flag |
| `MIP/SQL/migrations/20260418_public_disclosure_context.sql` | `COMMITTEE2_CONTEXT_DISCLOSURE_ENABLED` | C2 exhibit toggle |
| `MIP/SQL/migrations/20260418_public_disclosure_phase2_align.sql` | `COMMITTEE2_LIVE_POLITICIAN_DISCLOSURE_ENABLED` | C2 exhibit toggle |
| `MIP/SQL/migrations/20260418_structural_only_live_intent_kind.sql` | `LIVE_STRUCTURAL_ONLY`, `LIVE_AUTO_IMPORT_PROPOSALS_ON_OVERVIEW` | Legacy committee gate |
| `MIP/SQL/migrations/add_live_committee_foundation.sql` | `MIP.LIVE.COMMITTEE_RUN`, `MIP.LIVE.COMMITTEE_VERDICT`, `MIP.LIVE.COMMITTEE_ROLE_OUTPUT`; `LIVE_ACTIONS` committee columns | C2 materialization target |
| `MIP/SQL/smoke/33_committee2_hearing_smoke.sql` | Smoke: hearing tables and `COMMITTEE2_ENABLED` exist | Audit |
| `MIP/SQL/smoke/16_live_committee_foundation_smoke.sql` | Smoke: live committee schema | Audit |

### Snowflake tables and views (C2-related)

| Object | Schema | Purpose | Class |
|---|---|---|---|
| `STRUCTURAL_PROPOSAL_SNAPSHOT` | `MIP.APP` | Frozen proposal-time inputs | C2 core |
| `COMMITTEE_HEARING` | `MIP.APP` | Current hearing state (stance, confidence, evidence/deltas/chair JSON, evidence_pack_hash) | C2 core |
| `COMMITTEE_ROLE_OUTPUT` | `MIP.APP` | Per-specialist deterministic output | C2 core |
| `COMMITTEE_EVIDENCE_ARTIFACT` | `MIP.APP` | Structure map, geometry meter, intraday substantiation, disclosure | C2 core |
| `COMMITTEE_FINAL_DECISION` | `MIP.APP` | Committed packet bound to `ACTION_ID` | C2 core (materialization source) |
| `COMMITTEE_BAKEOFF_LATCH` / `_OUTCOME` / `_CONFIG` | `MIP.APP` | Real-vs-shadow comparison sidecar | Diagnostic |
| `COMMITTEE_RUN` | `MIP.LIVE` | Live execution committee-run shell | Materialization target |
| `COMMITTEE_VERDICT` | `MIP.LIVE` | Live execution verdict shape | Materialization target |
| `COMMITTEE_ROLE_OUTPUT` | `MIP.LIVE` | Legacy live role outputs (multi-agent path) | Legacy materialization |
| `LIVE_ACTIONS` | `MIP.LIVE` | The submission record; carries `COMMITTEE_STATUS`, `COMMITTEE_RUN_ID`, `COMMITTEE_VERDICT`, `COMMITTEE_COMPLETED_TS` | Materialization target |
| `V_COMMITTEE_BAKEOFF_OPPORTUNITIES`, `_SCORECARD`, `_LABEL_DIST`, `_DISAGREEMENTS`, `_MTD` | `MIP.MART` | Diagnostic bakeoff views | Diagnostic |
| `V_STRUCTURAL_PROPOSALS_FOR_COMMITTEE` | `MIP.MART` | Proposal feed for committee | C2 input |
| `V_TRADE_INTELLIGENCE` | `MIP.MART` | Reads `MIP.LIVE.COMMITTEE_VERDICT` | Audit |
| `V_ENTRY_INTEL_LIFECYCLE_RECONSTRUCTION` | `MIP.LIVE` | Reads `MIP.LIVE.COMMITTEE_VERDICT` + `LIVE_ACTIONS` | Audit |
| `V_SHADOW_POSITION_LIFECYCLE_STATUS` | `MIP.MART` | Position-health shadow lifecycle | Shadow audit |

**No views in `MIP/SQL/views/` directly read `COMMITTEE_HEARING` or `COMMITTEE_ROLE_OUTPUT`.** The only direct readers of `MIP.APP.COMMITTEE_FINAL_DECISION` are the Python application (cockpit, position-health builder, committee_final_decision endpoint), the bakeoff refresh proc, and the daily position verdict proc.

### FastAPI endpoints

Router prefix `/committee` (`MIP/apps/mip_ui_api/app/routers/committee.py`):

| Method + path | Function | Class |
|---|---|---|
| `POST /committee/hearing/open` | open new or refresh existing hearing | **C2 write** |
| `GET  /committee/hearing/{hearing_id}` | read assembled hearing | **C2 read** |
| `POST /committee/hearing/{hearing_id}/refresh` | re-run engine + persist | **C2 write** |
| `POST /committee/hearing/{hearing_id}/commit` | bind `COMMITTEE_FINAL_DECISION` to `ACTION_ID` | **C2 write** (FD) |
| `GET  /committee/proposal/{proposal_id}/final-decision` | read latest FD for proposal | **C2 read** |
| `GET  /committee/proposal/{proposal_id}/priority-context` | cockpit rank | Auxiliary read |
| `GET  /committee/proposal/{proposal_id}/board-explanation` | Phase 4 agentic board read | Agentic read (not C2) |
| `POST /committee/hearing/{hearing_id}/shadow-board/run` | manual shadow replay | Shadow write |
| `GET  /committee/hearing/{hearing_id}/shadow-board` | read shadow session | Shadow read |

Router prefix `/live` (`MIP/apps/mip_ui_api/app/routers/live.py`):

| Method + path | Function | Class |
|---|---|---|
| `POST /live/trades/actions/{id}/revalidate` | `revalidate_live_action` | **Safety guard** — bar/price/news. Not C2. |
| `POST /live/trades/actions/{id}/committee2/orchestrate` | `orchestrate_committee2_structural_entry` | **C2 primary live entry point** — refresh + commit + materialize + shadow kickoff |
| `POST /live/trades/actions/{id}/committee/apply` | `apply_live_trade_committee` | C2 materialize (structural ENTRY) / execution verdict (EXIT) / legacy (other) |
| `GET  /live/trades/actions/{id}/committee/live-prompt` (SSE) | `stream_live_trade_committee_prompt` | C2 replay (structural ENTRY) / legacy multi-agent (gated by `LIVE_STRUCTURAL_ONLY`) |

### React components

| File (`MIP/apps/mip_ui_web/src/`) | Component | Role |
|---|---|---|
| `pages/LivePortfolioActivity.jsx` | `LivePortfolioActivity` | **Operator primary** — "Run Committee 2.0", "Refresh decision", "Sync Committee 2.0", "Replay execution verdict"; renders `committee_verdict`, `committee_decision`, `sizing.committee_size_factor` |
| `pages/LpaCommittee2Exhibits.jsx` | `LpaCommittee2Exhibits` | Inline C2 exhibits + dual REAL/SHADOW columns; calls `priority-context` and `shadow-board` |
| `pages/StructuralCommitteeHearing.jsx` | `StructuralCommitteeHearing` | **Replay/diagnostics** hearing inspector — calls `/committee/hearing/*` open/refresh/commit; banner explicitly says live hearings run in LPA |
| `components/structural-timeline/StlHearingLaunch.jsx` | `StlHearingLaunch` | Opens hearing from structural timeline |
| `components/structural-timeline/StlAgenticBoardRead.jsx` | `StlAgenticBoardRead` | Phase 4 agentic board read |
| `components/board/BoardExplanationPanel.jsx` | `BoardExplanationPanel` | Phase 4 agentic + Phase 3 audit trail |
| `components/board/Phase4ChairSection.jsx` | `Phase4ChairSection` | Phase 4 chair UI |
| `components/committee/ShadowBoardPanel.jsx` | `ShadowBoardPanel` | Shadow boardroom UI; "zero trade authority" label |
| `pages/CommitteePerformance.jsx` | `CommitteePerformance` | Bake-off diagnostic page |
| `pages/cockpit/TradeProposalsPanel.jsx` | `TradeProposalsPanel` | Displays `committee_stance` (no direct C2 call) |
| `components/AppLayout.jsx` | sidebar | Routes: **Committee 2.0** → `/structural-committee`; **Committee Bake-off** → `/committee-performance` |

### `APP_CONFIG` keys (C2-relevant)

| Key | Source file | Role |
|---|---|---|
| `COMMITTEE2_ENABLED` | `20260418_committee2_config_grants.sql` | Master flag — every C2 endpoint short-circuits if false |
| `COMMITTEE2_CONTEXT_DISCLOSURE_ENABLED` | `20260418_public_disclosure_context.sql` | Disclosure exhibit toggle |
| `COMMITTEE2_LIVE_POLITICIAN_DISCLOSURE_ENABLED` | `20260418_public_disclosure_phase2_align.sql` | Live politician exhibit toggle |
| `LIVE_STRUCTURAL_ONLY` | `20260418_structural_only_live_intent_kind.sql` | Forbids legacy ORDER_PROPOSALS / `_run_multiagent_dialogue` on structural rows |
| `LIVE_AUTO_IMPORT_PROPOSALS_ON_OVERVIEW` | same migration | Legacy import (must stay false under structural-only) |
| `SHADOW_BOARD_ENABLED` | `542_shadow_board_agents.sql` | Shadow board kickoff |
| `SHADOW_BOARD_MODEL` | `542_shadow_board_agents.sql` | Documented Cortex model |
| `SHADOW_BOARD_TIMEOUT_SEC` | `542_shadow_board_agents.sql` | Per-agent timeout |

---

## Current LPA / Operator Dependencies

### What LPA actually calls (today)

Operator workflow for a structural-entry pending decision:

1. Operator clicks **"Run Committee 2.0"** → `POST /live/trades/actions/{id}/committee2/orchestrate` (in `live.py`, function `orchestrate_committee2_structural_entry`).
2. That endpoint runs `_run_refresh` → `compute_hearing_bundle` (deterministic engine).
3. `_persist_hearing_atomic` writes `COMMITTEE_HEARING`, `COMMITTEE_ROLE_OUTPUT`, `COMMITTEE_EVIDENCE_ARTIFACT`.
4. `committee_final_decision_commit_for_action` writes `COMMITTEE_FINAL_DECISION` and binds to `ACTION_ID`.
5. `_materialize_structural_entry_committee_apply` translates the FD via `committee2_live_bridge` and writes `MIP.LIVE.COMMITTEE_RUN`, `MIP.LIVE.COMMITTEE_VERDICT`, and updates `MIP.LIVE.LIVE_ACTIONS` (`COMMITTEE_STATUS`, `COMMITTEE_RUN_ID`, `COMMITTEE_VERDICT`, `COMMITTEE_COMPLETED_TS`, `STATUS`, `PROPOSED_PRICE/QTY`, `REASON_CODES`).
6. If `SHADOW_BOARD_ENABLED=true`, `kickoff_shadow_board_for_snapshot` fires an asyncio background task that runs the agentic shadow board.
7. Operator submission is then gated by `revalidate_live_action` (safety guard), and a successful `REVALIDATED_PASS` allows the order to be sent to IBKR.

### Operator-visible UI dependencies on C2 output

| UI surface | Field / API consumed | Required for operator action? |
|---|---|---|
| LPA "Committee 2.0 (last run)" panel | `stance`, `confidence`, `recommendation`, `blocked`, `reason_codes`, `committee_verdict`, `committee_run_id` from orchestrate response and overview row | **Yes** — operator reads this before submit |
| LPA stale-row detection | `COMMITTEE_STATUS`, `COMMITTEE_COMPLETED_TS` on `LIVE_ACTIONS` | **Yes** — controls "Refresh decision" / "Sync Committee 2.0" prompts |
| LPA Submit button | `STATUS` transitions to `READY_FOR_APPROVAL_FLOW` after C2 materialization | **Yes** — submission blocked until C2 ran |
| LPA inline exhibits | `inline_hearing` payload from orchestrate (`hearing_evidence`, role outputs, artifacts, chair) | Advisory display |
| Cockpit `TradeProposalsPanel` | `committee_stance`, `committee_stance_source` from overview | Display only |
| `StructuralCommitteeHearing` (replay) | Full hearing payload | Diagnostics |
| `BoardExplanationPanel` (Phase 4) | Independent of C2 — uses `PROPOSAL_BOARD_THESIS_VERDICT` | Read-only intelligence |
| `ShadowBoardPanel` | Shadow session payload | Advisory, non-authoritative |

### LPA endpoints that **must** keep returning C2-compatible payloads through retirement

- `POST /live/trades/actions/{id}/committee2/orchestrate` — primary trigger
- `GET  /live/activity/overview` — overview row carries committee fields
- `POST /live/trades/actions/{id}/committee/apply` — secondary materialization path
- `GET  /live/trades/actions/{id}/committee/live-prompt` (SSE) — replay stream

Any retirement step that removes or changes the **shape** of these responses must ship the agentic-source equivalents at the same time.

---

## Current Materialization Dependencies

This section answers the user's special-focus questions explicitly.

### What writes `COMMITTEE_HEARING`

- **Only writer:** Python `_persist_hearing_atomic` in `MIP/apps/mip_ui_api/app/routers/committee.py`. Called from `_run_refresh` (used by `POST /committee/hearing/open`, `POST /committee/hearing/{id}/refresh`, and `POST /live/.../committee2/orchestrate`). UPDATE or INSERT; never via a Snowflake stored procedure.
- One additional inline update: `orchestrate_committee2_structural_entry` sets `EVIDENCE_PACK_HASH` on the row.

### What writes `COMMITTEE_FINAL_DECISION`

- **Only writer:** Python `committee_final_decision_commit_for_action` in `routers/committee.py`. Called from `POST /committee/hearing/{id}/commit` and inside `orchestrate_committee2_structural_entry`. Handles INSERT, idempotent rebind, stale rebind, stance-drift UPDATE.

### What writes `COMMITTEE_ROLE_OUTPUT`

- **APP schema (`MIP.APP.COMMITTEE_ROLE_OUTPUT`):** Python `_persist_hearing_atomic` only (DELETE + INSERT per role, transactional with hearing row).
- **LIVE schema (`MIP.LIVE.COMMITTEE_ROLE_OUTPUT`):** Python `_run_multiagent_dialogue` (legacy Cortex multi-agent path) in `live.py`. **Forbidden for structural rows** at function entry and forbidden globally when `LIVE_STRUCTURAL_ONLY=true`.

### What writes `COMMITTEE_RUN` / `COMMITTEE_VERDICT` (`MIP.LIVE`)

- `_materialize_structural_entry_committee_apply` in `live.py` for structural ENTRY (translated from `COMMITTEE_FINAL_DECISION` via `committee2_live_bridge`).
- `apply_live_trade_committee` for structural EXIT (execution-only verdict, no C2 hearing) and for non-structural rows.
- `_run_multiagent_dialogue` for legacy non-structural rows (gated off by `LIVE_STRUCTURAL_ONLY`).

### What updates `LIVE_ACTIONS` with committee state

Same three paths above, all writing the columns: `COMMITTEE_STATUS`, `COMMITTEE_RUN_ID`, `COMMITTEE_COMPLETED_TS`, `COMMITTEE_VERDICT`, `STATUS` transitions, and `REASON_CODES`. There is no Snowflake stored procedure that updates `LIVE_ACTIONS` based on C2 — it is entirely in Python.

Other non-C2 writers of `LIVE_ACTIONS`:

- `SP_EXPIRE_STALE_DAILY_PROPOSALS` (`522_…`) — only flips proposals to expired; not committee.
- Phase 4 board tables (`564_…`) — alters `LIVE_ACTIONS` schema (DDL), not C2.

### What triggers the shadow board

- `kickoff_shadow_board_for_snapshot` in `shadow_board.py`, called by `orchestrate_committee2_structural_entry` **after** C2 commits, in an `asyncio.create_task` background coroutine.
- Manual replay: `POST /committee/hearing/{id}/shadow-board/run` (used by `StructuralCommitteeHearing` page when `?diagnostics=1`).
- **Implication:** any retirement that removes the C2 orchestration path must rewire the shadow kickoff (or replace it with a primary agentic call) before stopping C2 invocation. The shadow board does not self-trigger from a `LIVE_ACTIONS` event.

### What LPA calls directly

| LPA action | Direct dependency |
|---|---|
| Refresh / Run Committee 2.0 | `/live/.../committee2/orchestrate` (C2 + shadow) |
| Replay / Sync Committee 2.0 | `/live/.../committee/live-prompt` (SSE) + `/live/.../committee/apply` |
| Revalidate (auto, post-apply) | `/live/.../revalidate` (safety guard, not C2) |
| Submit | reads `STATUS` from `LIVE_ACTIONS` set by C2 materialization |
| Show exhibits | orchestrate response `inline_hearing` |
| Show shadow board | `/committee/hearing/{id}/shadow-board` |
| Show board-explanation | `/committee/proposal/{id}/board-explanation` (Phase 4 agentic; independent of C2) |

### What endpoints the UI requires

Removing or hiding (but not deleting) the **diagnostic** hearing room (`/structural-committee/{id}`) is safe. Removing any of the four LPA-facing endpoints (`orchestrate`, `committee/apply`, `committee/live-prompt`, `revalidate`) is unsafe until an agentic equivalent is wired.

---

## Shadow / Agentic Replacement Requirements

Before deterministic C2 can be retired, the agentic / shadow path must produce all of the following, at parity with what `_materialize_structural_entry_committee_apply` consumes today.

### Operator-facing verdict shape (parity contract)

The agentic chair must output, per hearing:

| Field | Current C2 source | Required agentic output |
|---|---|---|
| `stance` | `compute_hearing_bundle().stance` | `SHADOW_CHAIR_RULING.stance` (already produced) |
| `confidence` | `compute_hearing_bundle().confidence` | `SHADOW_CHAIR_RULING.confidence` (already produced) |
| `recommendation` (`BLOCK` / `PROCEED` / `PROCEED_REDUCED`) | `committee2_live_bridge.structural_entry_verdict_from_committee2_final` | Needs an agentic equivalent — a new "agentic live bridge" must map shadow chair stance → live recommendation |
| `size_factor` | derived from stance in bridge | derived from agentic stance in new bridge |
| `is_blocked` | derived from stance | derived from agentic stance |
| `reason_codes` | C2 chair primary/secondary | shadow chair primary/secondary + dissent codes |
| `joint_decision` | bridge synthesizes from FD | agentic equivalent built from `SHADOW_CHAIR_RULING` + `SHADOW_SPECIALIST_POSITION` |
| `role_outputs` (display) | `ROLE_OUTPUTS_JSON` | per-specialist position summaries from `SHADOW_SPECIALIST_POSITION` |

### Action materialization decision

A new path must update `MIP.LIVE.LIVE_ACTIONS` with `COMMITTEE_STATUS`, `COMMITTEE_RUN_ID`, `COMMITTEE_VERDICT`, `COMMITTEE_COMPLETED_TS`, and `STATUS` transitions from an agentic source. This is a new function (e.g. `_materialize_structural_entry_agentic_apply`) or a replacement of the existing one that consumes `SHADOW_CHAIR_RULING` instead of `COMMITTEE_FINAL_DECISION`. **This does not exist today.**

### Explanation payload (operator UX)

Required for UI display:

| UX element | Today | Future |
|---|---|---|
| "Why this stance" | C2 chair `supports`/`tensions` | Shadow chair ruling JSON + Phase 4 thesis context |
| "Why not opposite" | derived in C2 from `BLOCK_REASON_TAXONOMY` | Agentic chair must produce this (Phase 4 already does — Phase 4 `WHY_NOT_OPPOSITE` is in `PROPOSAL_BOARD_THESIS_VERDICT`) |
| "Thesis health" | C2 chair includes structural deltas | Phase 4 `thesis_health` (already in evidence pack 2.0.0) |
| "Confidence / degraded flag" | C2 deterministic scoring | Agentic chair confidence + `DEGRADED` flag on `SHADOW_BOARD_SESSION` |
| "Source evidence" | C2 `COMMITTEE_EVIDENCE_ARTIFACT` rows | Shadow evidence pack snapshot + Phase 4 dossier + (future) RAG cards |

### Rejection / approval reason

Today `committee_final_decision_commit_for_action` writes `STANCE` + `CHAIR_OUTPUT_JSON` + role-level rationales. The agentic equivalent already writes `SHADOW_CHAIR_RULING.CHAIR_OUTPUT_JSON` with stance, primary reason, and dissent. Gap: no standardized `block_primary_reason` taxonomy code on the shadow chair output. A reason-code taxonomy must be defined for the agentic path so the UI can render the same red-flag chips it renders today.

### Audit trail

Today: `COMMITTEE_HEARING` + `COMMITTEE_ROLE_OUTPUT` + `COMMITTEE_EVIDENCE_ARTIFACT` + `COMMITTEE_FINAL_DECISION` form a self-contained audit row per hearing. The agentic equivalent: `SHADOW_BOARD_SESSION` + `SHADOW_SPECIALIST_POSITION` + `SHADOW_CONFLICT_MAP` + `SHADOW_CHALLENGE_TURN` + `SHADOW_REVISION_TURN` + `SHADOW_CHAIR_RULING`. This already exists. The gap is **a single read view** that joins these into a hearing-equivalent payload that the LPA and hearing-page UIs can consume without code changes.

### `LIVE_ACTIONS` compatibility

`LIVE_ACTIONS.COMMITTEE_VERDICT` is a `STRING` (e.g. `BLOCK / PROCEED / PROCEED_REDUCED`). The agentic chair must be mappable to this set. The `committee2_live_bridge` does this mapping today; an analogous `agentic_live_bridge` must exist before the source switch.

### Required new artifacts (summary)

| New artifact | Reason |
|---|---|
| `MIP.APP.AGENTIC_LIVE_BRIDGE_VIEW` (or Python equivalent) | Map `SHADOW_CHAIR_RULING` → `recommendation / size_factor / is_blocked / reason_codes` |
| `MIP.LIVE.AGENTIC_LIVE_FINAL_DECISION` (or repurposed `COMMITTEE_FINAL_DECISION` shape) | The "FD-equivalent" the materializer reads |
| `_materialize_structural_entry_agentic_apply` (new function) | Write `LIVE_ACTIONS` + `LIVE.COMMITTEE_RUN` + `LIVE.COMMITTEE_VERDICT` from agentic source |
| `V_AGENTIC_HEARING_FOR_UI` (or per-endpoint payload builder) | Single view that LPA can consume in place of `_run_refresh` output |
| Agentic reason-code taxonomy | UX parity with C2 `BLOCK_REASON_TAXONOMY` |

---

## Safety Guard vs Committee Logic Distinction

This is the single most important boundary in this audit. The retirement plan must **not** touch the safety guard.

### Safety guard (KEEP)

- **Endpoint:** `POST /live/trades/actions/{id}/revalidate`
- **Handler:** `revalidate_live_action` in `live.py`
- **What it does:** 1m IBKR bar refresh, bar-freshness check (`QUOTE_FRESHNESS_THRESHOLD_SEC`, `MAX_BAR_END_LAG_SEC`), price-deviation guard vs `PROPOSED_PRICE`, news-event guard (`NEWS_REVALIDATION_CAUTION`, `NEWS_EVENT_SHOCK_BLOCK`), extended-hours window, market-order exit bypass, learning-ledger append.
- **What it does NOT do:** call the engine, write a hearing, write a final decision, write `LIVE.COMMITTEE_*`, run any LLM or Cortex agent.
- **Why keep:** it is the last-mile safety check before order submission. It must continue to gate `LIVE_ACTIONS.STATUS` transitions through `REVALIDATED_PASS` regardless of whether intelligence is deterministic or agentic.

### Deterministic Committee 2.0 intelligence (RETIREMENT TARGET)

- **Engine:** `compute_hearing_bundle` in `committee/engine.py`
- **Persistence:** `_persist_hearing_atomic`, `committee_final_decision_commit_for_action`
- **Materialization:** `_materialize_structural_entry_committee_apply`, `committee2_live_bridge`
- **Why retire:** rule-based; cannot reason about Phase 4 thesis health, structural levels, or candle psychology beyond hard-coded thresholds; produces template-generated role/chair JSON that the operator reads as "intelligence" but is actually a deterministic scoring function.

### Operator UI labels that must change wording (not behaviour) early

- "Committee 2.0" sidebar label → "Hearing Replay (Diagnostics)" or similar
- LPA "Run Committee 2.0" button → "Run Intelligence Review" (when behind the same orchestrate endpoint that will be source-switched later)
- LPA "Committee revalidation" wording when it actually means the safety guard → "Run safety revalidation" (already partially done; finish it)

These rewordings are part of Stage 1 (label/visibility) and require **no backend changes**.

---

## Safe Cleanup Candidates

The following items can be safely renamed, hidden, or moved to a `diagnostics/` namespace without breaking runtime, **provided no code is deleted**.

### Now-safe (no runtime caller would break)

| Item | Action | Rationale |
|---|---|---|
| Sidebar link `Committee 2.0` → `/structural-committee` in `AppLayout.jsx` | Rename label to "Hearing Replay (Diagnostics)" and/or move under "Diagnostics" sub-section | Page itself is already labelled "Replay / Diagnostics view"; only the sidebar still suggests it is the primary path |
| `LpaCommittee2Exhibits.jsx` panel header | Rename to "Intelligence Review Evidence" | Decouples operator vocabulary from "Committee 2.0" while leaving fetch logic intact |
| LPA "Run Committee 2.0" button label | Rename to "Run Intelligence Review" (keep same endpoint) | Endpoint stays, label aligns with future agentic source |
| LPA "Sync Committee 2.0" button label | Rename to "Sync Intelligence Review" | Same |
| `StructuralCommitteeHearing` `?diagnostics=1` shadow re-run button | Already gated; tighten visibility default to require admin role | Reduces accidental operator interaction |
| `CommitteePerformance` page | Confirm label "Committee Bake-off" and explicitly mark as "diagnostic only — does not change live execution" (already present) | Confirmed; no change required |
| `BoardExplanationPanel` Phase 4 section | Promote in LPA layout above Committee 2.0 panel | Phase 4 is already the richer intelligence source |
| `intraday_substantiation.py`, `public_disclosure_context.py`, `live_politician_disclosure.py`, `capitol_trades_poc_scrape.py` | Mark as "evidence exhibit only — independent of committee logic" in docstrings | They will outlive C2 retirement and should be portable to the agentic path |
| `MIP/SQL/smoke/33_committee2_hearing_smoke.sql` | Keep; will continue to be useful for replay/diagnostics layer | No action |
| Comments / docstrings that still call C2 "the intelligence board" | Replace with "deterministic baseline; agentic shadow is the live intelligence path" | Doc-only |

### Soon-safe once a single read view exists

| Item | Action | Prerequisite |
|---|---|---|
| LPA "Committee 2.0 (last run)" panel field source | Switch from `committee_decision` to a new `intelligence_decision` field on the overview row | Backend overview must populate `intelligence_decision` from agentic source |
| Cockpit `TradeProposalsPanel` `committee_stance` field | Switch source to `intelligence_stance` | Same |
| `StructuralCommitteeHearing` page | Keep but rebrand as "Deterministic baseline hearing inspector" | No behaviour change |

**Important constraint:** none of the safe cleanups touch `COMMITTEE_FINAL_DECISION` materialization, `LIVE_ACTIONS` writes, the safety guard, IBKR, or order placement.

---

## Dangerous Deletion Candidates

The following items look retire-able at a glance but are still **load-bearing for runtime** today. They must not be deleted until the agentic path provides equivalent output and `LIVE_ACTIONS` materialization is source-switched.

| Item | Why dangerous to delete now |
|---|---|
| `MIP.APP.COMMITTEE_FINAL_DECISION` table | Only source consumed by `_materialize_structural_entry_committee_apply`. Deleting it before the agentic materialization exists breaks LPA submit flow |
| `committee/engine.py` (`compute_hearing_bundle`) | Called from every `orchestrate` / `refresh` / `commit` request. Removing it breaks LPA "Run Committee 2.0" |
| `committee/committee2_live_bridge.py` | Only translator from `COMMITTEE_FINAL_DECISION` to live verdict shape. Removing it breaks `_materialize_structural_entry_committee_apply` |
| `_materialize_structural_entry_committee_apply` in `live.py` | Sole writer of structural-entry `LIVE_ACTIONS` committee state. Removing it leaves no path to `READY_FOR_APPROVAL_FLOW` |
| `routers/committee.py` endpoints `open / refresh / commit` | Called from `StructuralCommitteeHearing` (still in nav) and indirectly via orchestrate. Removing them breaks the diagnostics/replay surface and the orchestrate inline flow |
| `MIP.APP.COMMITTEE_HEARING`, `_ROLE_OUTPUT`, `_EVIDENCE_ARTIFACT` | Read by `shadow_board.orchestrate_shadow_board` to build the evidence pack. Removing them breaks the shadow board kickoff |
| `MIP.LIVE.COMMITTEE_RUN` / `COMMITTEE_VERDICT` | Read by `V_TRADE_INTELLIGENCE`, `V_ENTRY_INTEL_LIFECYCLE_RECONSTRUCTION`, and by every cockpit / position-health / lifecycle reconstruction caller |
| `LIVE_ACTIONS` committee columns | Read by LPA overview, cockpit, position-health payload builder, every audit/history caller |
| `SHADOW_TRADE_LINKAGE` (`543_…`) | Written on every real IBKR execute path to link the real action to its shadow session. Required for ongoing observability of agentic alignment |
| `COMMITTEE_BAKEOFF_*` tables and `SP_REFRESH_COMMITTEE_BAKEOFF` | The mart views (`V_COMMITTEE_BAKEOFF_*`) and the bakeoff page consume them. They are **diagnostic only and explicitly not a promotion baseline**, but they should remain available for retrospective comparison after agentic source switch |
| `SP_RUN_DAILY_POSITION_VERDICT` (`551_…`) | Reads `COMMITTEE_FINAL_DECISION` as baseline context for position-health verdicts. Removing it before the position-health builder is source-switched breaks daily position health |
| `LIVE_STRUCTURAL_ONLY` config flag | Acts as the kill switch for the legacy multi-agent committee path. Must stay `true` and must not be deleted before the legacy path is excised |
| `_run_multiagent_dialogue` (legacy in `live.py`) | Dormant under `LIVE_STRUCTURAL_ONLY=true` but still referenced from `stream_live_trade_committee_prompt`. Deleting it before that endpoint is rewritten breaks the SSE replay surface |
| `committee/intraday_substantiation.py`, `public_disclosure_context.py`, etc. | Evidence-only modules used inside `_run_refresh`. They will be useful inputs to the agentic path as well; do not delete |
| `revalidate_live_action` and the `/revalidate` endpoint | **Safety guard, not C2.** Not a deletion candidate at all. Keep. |

---

## Recommended Retirement Stages

This is the staged plan the user requested. **No stage is implemented in this task.** Each stage lists its preconditions and its hard "do-not-touch" boundaries.

### Stage 1 — Rename / hide deterministic C2 in operator-facing LPA

**Goal:** Stop presenting deterministic C2 as the intelligence board. Keep all backend behaviour identical.

Actions (UI only):

- Rename sidebar "Committee 2.0" → "Hearing Replay (Diagnostics)" or move under "Diagnostics" section.
- Rename LPA buttons: "Run Committee 2.0" → "Run Intelligence Review"; "Sync Committee 2.0" → "Sync Intelligence Review"; "Committee revalidation" → "Run safety revalidation" wherever it labels the safety guard.
- Reword "Committee 2.0 (last run)" panel header to "Intelligence Review (last run)".
- Promote `BoardExplanationPanel` (Phase 4) above the C2 panel in the LPA layout.
- Add an explicit "Deterministic baseline" badge on the `StructuralCommitteeHearing` page header.

Preconditions: none beyond this audit. **Safe today.**

Boundaries: do not change `orchestrate` endpoint, do not change `apply`, do not change `revalidate`, do not change `LIVE_ACTIONS` writes.

### Stage 2 — Make the agentic / shadow board the primary visible intelligence path

**Goal:** Surface the shadow board as the primary operator-facing review while keeping C2 running and writing `LIVE_ACTIONS` underneath.

Actions:

- In LPA, swap visual prominence so the shadow chair ruling (with Phase 4 thesis health, `why_not_opposite`, `primary_reason_code`) is the headline; C2 stance becomes a secondary "deterministic baseline" line.
- Expose shadow chair JSON to LPA via the existing `/committee/hearing/{id}/shadow-board` endpoint (no new endpoint needed).
- Surface "agentic stance vs deterministic stance" delta when they disagree.
- Keep submission gating on `LIVE_ACTIONS.STATUS` (still set by C2 materialization).

Preconditions: Phase 4 evidence pack `2.0.0` is wired (done as of 2026-05-21).

Boundaries: do not write to `LIVE_ACTIONS` from the shadow path yet; do not change submission flow; do not remove C2 backend.

### Stage 3 — Keep deterministic C2 only as diagnostic / fallback

**Goal:** Reduce the number of operator-visible entry points to deterministic C2 to one: a clearly labelled "Run deterministic baseline" diagnostic button.

Actions:

- Remove all primary-flow C2 labels from LPA; keep one diagnostic "Run deterministic baseline" button (still hitting `orchestrate` until Stage 4).
- Mark `StructuralCommitteeHearing` page as admin-only or behind feature flag.
- Add a single LPA toggle "Show deterministic baseline" defaulting to off.

Preconditions: Stage 2 complete and the agentic stance is shown by default.

Boundaries: backend still runs C2 inside `orchestrate`; `LIVE_ACTIONS` still gated by C2 materialization.

### Stage 4 — Replace materialization dependencies with agentic-compatible outputs

**Goal:** Switch the source of `MIP.LIVE.COMMITTEE_RUN`, `COMMITTEE_VERDICT`, and `LIVE_ACTIONS` committee columns from `COMMITTEE_FINAL_DECISION` to a new agentic-source equivalent.

Actions (new code, not deletions):

- Implement an "agentic live bridge" (Python module mirroring `committee2_live_bridge.py`) that maps `SHADOW_CHAIR_RULING` → `recommendation / size_factor / is_blocked / reason_codes / joint_decision / role_outputs`.
- Implement `_materialize_structural_entry_agentic_apply` in `live.py` that consumes the agentic bridge and writes the same `LIVE_ACTIONS` / `LIVE.COMMITTEE_RUN` / `LIVE.COMMITTEE_VERDICT` columns. Same column shape, new source.
- Add a feature flag `LIVE_MATERIALIZE_SOURCE` in `APP_CONFIG` with values `DETERMINISTIC_C2` (default) and `AGENTIC_SHADOW`.
- Run both paths in parallel for a defined burn-in period; compare outputs; do not promote until confidence is established.
- Switch `LIVE_MATERIALIZE_SOURCE=AGENTIC_SHADOW`.

Preconditions:

- Stage 3 complete.
- Agentic reason-code taxonomy defined.
- Shadow chair output has been validated end-to-end on N representative hearings (Phase 4-native and pre-Phase-4).
- The `revalidate` safety guard still gates submission and is untouched.
- **Bake-off data is not used as the promotion baseline.** Promotion criteria must be independently defined (e.g. agentic chair output passes a defined parity test against C2 on a hand-picked set of past actions, plus exceeds C2 on Phase 4-specific cases where C2 has no signal).

Boundaries: do not delete `COMMITTEE_FINAL_DECISION`; keep it as a parallel write target for one cycle so audit history is continuous; do not change `revalidate` or order placement.

### Stage 5 — Quarantine / archive deterministic C2 code

**Goal:** Move deterministic C2 modules to a clearly named `legacy/` or `archive/` namespace and stop calling them from the primary path.

Actions:

- Move `committee/engine.py`, `committee2_live_bridge.py`, `routers/committee.py` write endpoints (`open / refresh / commit`) into a `MIP/apps/mip_ui_api/app/committee/legacy/` subpackage.
- Remove the diagnostic "Run deterministic baseline" button.
- Keep `StructuralCommitteeHearing` page reachable only via direct URL or admin role.
- Keep `COMMITTEE_FINAL_DECISION` table for historical reads; mark new writes as halted.
- Keep `COMMITTEE_BAKEOFF_*` for retrospective comparison.

Preconditions:

- Stage 4 has been stable on `AGENTIC_SHADOW` for a defined period.
- No new `COMMITTEE_FINAL_DECISION` rows have been read by any operator-facing path for that period.
- `SP_RUN_DAILY_POSITION_VERDICT` has been source-switched to read the agentic FD-equivalent.

Boundaries: still no deletions; still keep `revalidate`; still keep IBKR untouched.

### Stage 6 — Delete deterministic C2 code once no runtime references remain

**Goal:** Remove the quarantined code permanently.

Actions:

- Drop `MIP.APP.COMMITTEE_FINAL_DECISION` only after a defined retention policy has been honoured and an export has been archived.
- Drop the legacy `engine.py`, `committee2_live_bridge.py`, `committee.py` write endpoints, and `_materialize_structural_entry_committee_apply`.
- Drop `LIVE_STRUCTURAL_ONLY` (no longer needed once legacy multi-agent paths are also dropped).
- Drop bakeoff tables only if explicitly approved.

Preconditions:

- Stage 5 has been stable for a defined burn-in.
- All audit/history views have been source-switched or archived.
- Explicit user approval.

Boundaries: still keep `revalidate`. Still keep IBKR / order placement. Still keep Phase 4 proposal-board artifacts (independent of C2).

---

## Exact Next Implementation Task

The next concrete, low-risk implementation step is **Stage 1: UI label retirement only.**

Specifically:

1. In `MIP/apps/mip_ui_web/src/components/AppLayout.jsx`, rename the sidebar entry "Committee 2.0" → "Hearing Replay (Diagnostics)" and move it under a "Diagnostics" group (which already contains "Committee Bake-off").
2. In `MIP/apps/mip_ui_web/src/pages/LivePortfolioActivity.jsx`, rename the operator buttons "Run Committee 2.0" → "Run Intelligence Review", "Sync Committee 2.0" → "Sync Intelligence Review", and any "Committee revalidation" wording that refers to the safety guard → "Run safety revalidation". Rename the panel header "Committee 2.0 (last run)" → "Intelligence Review (last run)". Add a "Deterministic baseline" sub-label on the panel body.
3. In `MIP/apps/mip_ui_web/src/pages/LpaCommittee2Exhibits.jsx`, rename the panel header to "Intelligence Review Evidence".
4. In `MIP/apps/mip_ui_web/src/App.jsx`, change the browser title for `/structural-committee` to "Hearing Replay — Deterministic Baseline".
5. No backend endpoint changes. No SQL changes. No deletions. No changes to `revalidate`, `orchestrate`, `apply`, `live-prompt`, or `LIVE_ACTIONS`.
6. After deployment, verify in LPA that all submit flows still work end-to-end (same endpoints, just new labels) and that the safety guard still gates submission.

This stage is reversible by editing the same strings back. It separates operator vocabulary from backend identity and unblocks Stage 2 (visual promotion of the agentic / shadow board) as the immediate follow-up.

---

## Open Risks and Questions

### Risks

1. **`SP_RUN_DAILY_POSITION_VERDICT` reads `COMMITTEE_FINAL_DECISION` as baseline context.** If C2 is retired before this proc is source-switched, daily position health loses its committee baseline. This is a Stage 4–5 dependency.
2. **`SHADOW_TRADE_LINKAGE` is written on every real IBKR execute and uses `EVIDENCE_PACK_HASH` from `COMMITTEE_HEARING`.** Removing `COMMITTEE_HEARING` writes before the linkage logic is source-switched breaks IBKR-side observability.
3. **`MIP.LIVE.COMMITTEE_RUN` and `COMMITTEE_VERDICT` are read by `V_TRADE_INTELLIGENCE` and `V_ENTRY_INTEL_LIFECYCLE_RECONSTRUCTION`.** These views are downstream of the materialization shape; the agentic materialization must preserve the exact column shape or both views need to be updated in lockstep.
4. **The Cortex shadow agents were just recreated (542 deploy).** Stage 4 requires the agentic chair output to be stable and deterministic enough to drive `LIVE_ACTIONS`. Any planned changes to the chair prompt or pack should be frozen during the parallel-write validation window.
5. **Bake-off data must not be used as a promotion gate.** It is sidecar comparison only; the user has flagged it explicitly as never fully implemented. A separate promotion-readiness gate must be defined for Stage 4.
6. **`_run_multiagent_dialogue` is still present in `live.py` for non-structural rows.** Even though `LIVE_STRUCTURAL_ONLY=true` blocks it for structural rows, the codepath itself is alive. Stage 6 deletion must remove it as part of the legacy excision.
7. **`StructuralCommitteeHearing` still exposes manual refresh/commit when reached directly.** Stage 1 reduces visibility but does not block direct URL access. Stage 5 should add server-side admin-role gating.

### Questions for the user before any stage is implemented

1. Should the deterministic baseline be retained indefinitely as an audit-only mirror (parallel-write Stage 4) or fully retired at Stage 5?
2. Should `COMMITTEE_BAKEOFF_*` survive C2 retirement as a retrospective view, or be archived at Stage 6?
3. What is the acceptable parallel-write burn-in period between Stage 4 source switch and Stage 5 quarantine?
4. Should the diagnostic "Run deterministic baseline" button at Stage 3 require an admin role or just a feature flag?
5. What is the promotion-readiness gate for Stage 4 source switch, given that bake-off cannot be used? Suggested: hand-picked validation set of N past actions with documented expected stance, plus N Phase 4-only cases where C2 has no signal but agentic must produce one.
6. Should the renaming in Stage 1 also retire the term "Committee" entirely from operator vocabulary in favour of "Intelligence Review", or keep "Committee" only as the agentic chair label?
7. Does the position-health pipeline (`SP_RUN_DAILY_POSITION_VERDICT`) need an agentic baseline replacement before Stage 5, or can it gracefully degrade to "no committee baseline" while agentic position-health review continues to run?

---

## Appendix: Files Inspected

### Python
- `MIP/apps/mip_ui_api/app/committee/__init__.py`
- `MIP/apps/mip_ui_api/app/committee/engine.py`
- `MIP/apps/mip_ui_api/app/committee/committee2_live_bridge.py`
- `MIP/apps/mip_ui_api/app/committee/intraday_substantiation.py`
- `MIP/apps/mip_ui_api/app/committee/public_disclosure_context.py`
- `MIP/apps/mip_ui_api/app/committee/live_politician_disclosure.py`
- `MIP/apps/mip_ui_api/app/committee/capitol_trades_poc_scrape.py`
- `MIP/apps/mip_ui_api/app/committee/shadow_types.py`
- `MIP/apps/mip_ui_api/app/committee/shadow_board.py`
- `MIP/apps/mip_ui_api/app/committee/shadow_cortex_client.py`
- `MIP/apps/mip_ui_api/app/committee/shadow_linkage.py`
- `MIP/apps/mip_ui_api/app/routers/committee.py`
- `MIP/apps/mip_ui_api/app/routers/live.py`
- `MIP/apps/mip_ui_api/app/routers/position_health.py`
- `MIP/apps/mip_ui_api/app/services/position_health/payload_builder.py`
- `MIP/apps/mip_ui_api/app/services/cockpit/trade_proposals.py`
- `MIP/apps/mip_ui_api/app/services/board/explanation.py`

### SQL
- `MIP/SQL/app/540_committee2_tables.sql`
- `MIP/SQL/app/541_shadow_board_tables.sql`
- `MIP/SQL/app/542_shadow_board_agents.sql`
- `MIP/SQL/app/543_shadow_trade_linkage.sql`
- `MIP/SQL/app/521_structural_pipeline_and_views.sql`
- `MIP/SQL/app/522_sp_expire_stale_daily_proposals.sql`
- `MIP/SQL/app/551_sp_run_daily_position_verdict.sql`
- `MIP/SQL/app/561_sp_run_proposal_board.sql`
- `MIP/SQL/app/565_sp_run_proposal_board_phase4_symbol_dossier.sql`
- `MIP/SQL/app/566_sp_run_proposal_board_phase4_symbol_dossier_combined.sql`
- `MIP/SQL/app/600_committee_bakeoff_tables.sql`
- `MIP/SQL/app/601_sp_refresh_committee_bakeoff.sql`
- `MIP/SQL/app/392_live_execution_foundation.sql`
- `MIP/SQL/migrations/20260418_committee2_config_grants.sql`
- `MIP/SQL/migrations/20260418_public_disclosure_context.sql`
- `MIP/SQL/migrations/20260418_public_disclosure_phase2_align.sql`
- `MIP/SQL/migrations/20260418_structural_only_live_intent_kind.sql`
- `MIP/SQL/migrations/add_live_committee_foundation.sql`
- `MIP/SQL/views/mart/v_committee_bakeoff.sql`
- `MIP/SQL/views/mart/v_trade_intelligence.sql`
- `MIP/SQL/views/live/v_entry_intel_lifecycle_reconstruction.sql`
- `MIP/SQL/views/mart/v_position_health.sql`
- `MIP/SQL/smoke/33_committee2_hearing_smoke.sql`
- `MIP/SQL/smoke/16_live_committee_foundation_smoke.sql`

### React
- `MIP/apps/mip_ui_web/src/pages/LivePortfolioActivity.jsx`
- `MIP/apps/mip_ui_web/src/pages/LpaCommittee2Exhibits.jsx`
- `MIP/apps/mip_ui_web/src/pages/StructuralCommitteeHearing.jsx`
- `MIP/apps/mip_ui_web/src/pages/CommitteePerformance.jsx`
- `MIP/apps/mip_ui_web/src/components/AppLayout.jsx`
- `MIP/apps/mip_ui_web/src/components/board/BoardExplanationPanel.jsx`
- `MIP/apps/mip_ui_web/src/components/board/Phase4ChairSection.jsx`
- `MIP/apps/mip_ui_web/src/components/committee/ShadowBoardPanel.jsx`
- `MIP/apps/mip_ui_web/src/components/structural-timeline/StlHearingLaunch.jsx`
- `MIP/apps/mip_ui_web/src/components/structural-timeline/StlAgenticBoardRead.jsx`
- `MIP/apps/mip_ui_web/src/pages/cockpit/TradeProposalsPanel.jsx`
- `MIP/apps/mip_ui_web/src/App.jsx`

---

**End of audit. No runtime changes were made. No code was deleted. No RAG was wired. The agentic shadow board was not promoted. The price/bar/news safety guard is preserved across every retirement stage.**
