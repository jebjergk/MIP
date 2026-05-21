# MIP LPA Agentic Revalidation Transition Audit

**Date:** 2026-05-21  
**Status:** Discovery and planning only. No runtime, API, UI, or Snowflake app schema changes made.  
**Scope:** LPA revalidation path, shadow/agentic committee, Phase 4 dossier compatibility, and RAG insertion planning.

---

## Summary

MIP currently has two separate "revalidation" concepts that are easy to conflate but must be distinguished for transition planning:

1. **The price/bar/news guard** (`POST /live/trades/actions/{id}/revalidate`): a lightweight deterministic gate that checks bar freshness, price deviation from the revalidation reference, and news flags. This is not a committee or intelligence layer; it is a safety guardrail before submission. It was historically labeled "revalidation" because it gates whether a pending decision is still actionable.

2. **The intelligence board** (`POST /live/trades/actions/{id}/committee2/orchestrate`): Committee 2.0 — a deterministic proposal board that consults role outputs and commits a `COMMITTEE_FINAL_DECISION`. When `SHADOW_BOARD_ENABLED=true`, it also kicks off a fully agentic parallel shadow board (6 Cortex specialists + chair) in a background asyncio task.

The future target is to **make the agentic shadow board the primary operator-facing intelligence path in LPA** — not just an audit artefact. The old price/bar/news guard may remain as a safety gate but should not present itself as the intelligence or revalidation board. LPA's committee orchestration and shadow board execution already exist and produce rich structured verdicts; the gap is in surfacing them as the primary operational review, in upgrading the shadow evidence pack to Phase 4 awareness, and eventually in adding a RAG literature support slice.

**RAG may not be wired into the shadow committee until the evidence pack is Phase 4-aware.** This ordering is firm.

---

## Current LPA Deterministic Revalidation Flow

### UI surface

| Component / file | Path | Role |
|---|---|---|
| `LivePortfolioActivity.jsx` | `MIP/apps/mip_ui_web/src/pages/` | Main LPA page; pending decisions, "Revalidate" button, "Run Committee 2.0" button, stale-row detection, SSE stream panel |
| `LpaCommittee2Exhibits.jsx` | `MIP/apps/mip_ui_web/src/pages/` | Inline exhibits after orchestrate: geometry, path quality, regime continuity, shadow board panel |
| `BoardExplanationPanel.jsx` | `MIP/apps/mip_ui_web/src/components/board/` | Board audit trail: Phase 3 specialists, Phase 4 chair, `thesis_health`, `why_not_opposite` |
| `Phase4ChairSection.jsx` | `MIP/apps/mip_ui_web/src/components/board/` | Phase 4 thesis health, zones, candle psychology, structural timeline, `prior_thesis_reference` |
| `ShadowBoardPanel.jsx` | `MIP/apps/mip_ui_web/src/components/committee/` | Shadow boardroom: stance/confidence, specialists, conflicts, challenges, revisions, chair finale |

**Operator-visible actions on LPA:**

- "Revalidate" button → `POST /api/live/trades/actions/{id}/revalidate` (price/bar/news guard)
- "Run Committee 2.0" button → `POST /api/live/trades/actions/{id}/committee2/orchestrate`
- "Replay / Sync / Committee revalidation" → `EventSource .../committee/live-prompt` (replays C2 role summaries)
- Submission gated on `REVALIDATED_PASS` + `submission_allowed`

**Stale detection labels currently shown:**

- "Revalidation expired — run Committee 2.0 / replay execution verdict / sync Committee 2.0 / run committee revalidation before submit."
- Row class `lpa-row-stale` when `isStaleRevalidationState` (reason codes: `EXECUTION_CLICK_REVALIDATION_STALE`, `MISSING_REVALIDATION`, etc.)

### API endpoints

| Method | URL | Backend handler | Type |
|---|---|---|---|
| `POST` | `/live/trades/actions/{id}/revalidate` | `revalidate_live_action` | Deterministic price/bar guard |
| `POST` | `/live/trades/actions/{id}/committee2/orchestrate` | `orchestrate_committee2_structural_entry` | Deterministic real board + optional agentic shadow |
| `POST` | `/live/trades/actions/{id}/committee/apply` | `apply_live_trade_committee` | Materialise verdict to `LIVE_ACTIONS` |
| `GET` (SSE) | `/live/trades/actions/{id}/committee/live-prompt` | `stream_live_trade_committee_prompt` | C2 role summary replay or legacy multi-agent |
| `GET` | `/committee/proposal/{id}/board-explanation` | `committee_proposal_board_explanation` | Phase 4 board audit (read-only) |
| `GET` | `/committee/hearing/{id}/shadow-board` | `committee_shadow_board_get` | Shadow board session read |

### Backend functions and Snowflake objects

**`revalidate_live_action`** (`live.py`):

- Reads: `MIP.LIVE.LIVE_ACTIONS`, `MIP.LIVE.LIVE_PORTFOLIO_CONFIG`, `MIP.MART.MARKET_BARS` (1m IBKR bars)
- Writes: `MIP.LIVE.LIVE_ACTIONS` (revalidation columns, status: `REVALIDATED_PASS` / `REVALIDATED_FAIL`)
- Side: `_append_learning_ledger_event`, `_fetch_latest_symbol_news_context`
- **No committee call. No LLM. No Snowflake SP.**
- If `OPEN_BLOCKED`: delegates to `run_live_trade_committee` (a separate committee path)

**`orchestrate_committee2_structural_entry`** (`live.py`):

- Reads: `MIP.APP.COMMITTEE_HEARING`, `MIP.APP.APP_CONFIG` (`SHADOW_BOARD_ENABLED`, `SHADOW_BOARD_TIMEOUT_SEC`)
- Calls `_run_refresh` → `compute_hearing_bundle` (deterministic, `committee/engine.py`)
- Calls `committee_final_decision_commit_for_action` → writes `MIP.APP.COMMITTEE_FINAL_DECISION`
- Calls `_materialize_structural_entry_committee_apply` → updates `MIP.LIVE.LIVE_ACTIONS`
- If enabled: `compute_evidence_pack_hash`, `UPDATE MIP.APP.COMMITTEE_HEARING (EVIDENCE_PACK_HASH)`, then `kickoff_shadow_board_for_snapshot` (background asyncio)

**`orchestrate_shadow_board`** (`shadow_board.py`):

- Reads from Snowflake to build evidence pack: `MIP.APP.COMMITTEE_HEARING`, `MIP.APP.STRUCTURAL_TRADE_PROPOSALS`, `MIP.APP.STRUCTURAL_PROPOSAL_SNAPSHOT`, `MIP.APP.COMMITTEE_ROLE_OUTPUT`, `MIP.APP.COMMITTEE_EVIDENCE_ARTIFACT`
- Stages pack: INSERT `MIP.APP.SHADOW_EVIDENCE_PACK_CACHE`
- Runs 6 specialists via Cortex agent tool `GET_SHADOW_EVIDENCE_SLICE` → INSERT `MIP.APP.SHADOW_SPECIALIST_POSITION`
- Conflict detection → INSERT `MIP.APP.SHADOW_CONFLICT_MAP`
- Challenge/revision rounds → `SHADOW_CHALLENGE_TURN`, `SHADOW_REVISION_TURN`
- Chair agent → INSERT `MIP.APP.SHADOW_CHAIR_RULING`
- Lifecycle update: `MIP.APP.SHADOW_BOARD_SESSION`

**`load_board_explanation`** (`board/explanation.py`):

- Reads: `MIP.APP.STRUCTURAL_TRADE_PROPOSALS`, `MIP.APP.PROPOSAL_BOARD_THESIS_VERDICT`, `MIP.APP.PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT`
- Returns Phase 4 chair fields: `thesis_health`, `prior_thesis_reference`, `actionability_summary`, `continuation_quality`, `resistance_overhead_risk`, `candle_psychology`, `recent_cluster`, `broken_resistance_as_support`, `current_range_position_pct`, `why_not_opposite`, `why_not_no_trade`, `risk_treatment`
- This endpoint is already called from LPA (via `BoardExplanationPanel`) but is read-only audit; it is not part of the shadow board evidence pack

### Payload fields returned to LPA UI (pending decision row)

From `GET /live/activity/overview`:

- `status`, `compliance_status`, `required_next_step`, `submission_allowed`, `submission_gate_hints`, `execution_hard_blocked`
- `REVALIDATED_PASS` / `REVALIDATED_FAIL` gating; `reason_codes[]`
- `price_guard.price_deviation_pct`, `revalidation_price`
- `committee_verdict`, `committee_run_id`, `committee_decision.{should_enter, risk_notes, realistic_target_return, stop_loss_pct}`
- Committee 2.0 last run: `stance`, `confidence`, `recommendation`, `blocked`, `reason_codes`, `hearing_id`, `inline_hearing`
- `inline_hearing.chair_board.{stance, confidence, top_supports, top_tensions, execution_shaping}`
- Shadow seeds: `shadow_session_id`, `shadow_status`, `evidence_pack_hash`
- Structural: `setup_family`, `direction`, `entry_zone`, `invalidation`, `proposal_id`, `board_dossier_id`

---

## Current Agentic / Shadow Revalidation Flow

### What it is

The shadow board is a **full agentic parallel committee** that runs in the background after Committee 2.0 real-board orchestration. It is currently used for **proposal entry review**, not open-position revalidation (which is a separate `POSITION_HEALTH_REVIEW_AGENT` path). It produces structured verdicts including specialist stances, conflict maps, challenge/revision turns, and a chair ruling — all stored in Snowflake and already exposed in LPA via `ShadowBoardPanel`.

### Trigger path

```
POST /live/trades/actions/{id}/committee2/orchestrate
  └─ orchestrate_committee2_structural_entry (live.py)
       └─ [real board] _run_refresh → compute_hearing_bundle → committee_final_decision_commit_for_action
       └─ [shadow board] kickoff_shadow_board_for_snapshot → asyncio.create_task(orchestrate_shadow_board)
```

Controlled by `MIP.APP.APP_CONFIG.SHADOW_BOARD_ENABLED` (default: false). When true, the shadow run is idempotent on `(hearing_id, evidence_pack_hash)`.

### Files

| File | Role |
|---|---|
| `MIP/apps/mip_ui_api/app/committee/shadow_board.py` | Full orchestration: evidence pack build, 5 pipeline stages, session lifecycle |
| `MIP/apps/mip_ui_api/app/committee/shadow_types.py` | Types: `ShadowEvidencePack`, `ROLE_SLICE_MAP`, `SpecialistPosition`, `ShadowChairRuling`, `build_shadow_evidence_pack` |
| `MIP/SQL/app/541_shadow_board_tables.sql` | Tables: `SHADOW_EVIDENCE_PACK_CACHE`, `SHADOW_BOARD_SESSION`, `SHADOW_SPECIALIST_POSITION`, `SHADOW_CONFLICT_MAP`, `SHADOW_CHALLENGE_TURN`, `SHADOW_REVISION_TURN`, `SHADOW_CHAIR_RULING` |
| `MIP/SQL/app/542_shadow_board_agents.sql` | `GET_SHADOW_EVIDENCE_SLICE` tool + 6 specialist agents + `SHADOW_CHAIR_AGENT` |
| `MIP/SQL/app/543_shadow_trade_linkage.sql` | `SHADOW_TRADE_LINKAGE` (real vs shadow linkage after execute) |
| `MIP/SQL/app/600_committee_bakeoff_tables.sql` | `COMMITTEE_BAKEOFF_*` analytics |
| `MIP/SQL/app/601_sp_refresh_committee_bakeoff.sql` | `SP_REFRESH_COMMITTEE_BAKEOFF` |
| `MIP/SQL/views/mart/v_committee_bakeoff.sql` | `V_COMMITTEE_BAKEOFF_*` read views |

### Specialist roles and evidence slices

`ROLE_SLICE_MAP` in `shadow_types.py`:

| Role | Evidence slices available |
|---|---|
| `STRUCTURAL_THESIS` | `proposal_meta`, `structural_state`, `thesis_summary` |
| `ENTRY_GEOMETRY` | `proposal_meta`, `entry_zone`, `live_price` |
| `REGIME` | `proposal_meta`, `regime_state`, `live_bars` |
| `PATH_TRADEABILITY` | `proposal_meta`, `path_metrics`, `mfe_mae` |
| `PROTECTION_EXIT` | `proposal_meta`, `invalidation`, `live_price` |
| `SYMBOL_BEHAVIOR` | `proposal_meta`, `trust_label`, `path_metrics`, `live_bars` |
| `SHADOW_CHAIR` | All 13 slices |

### Persisted outputs

| Table | Contents |
|---|---|
| `SHADOW_BOARD_SESSION` | `SHADOW_STANCE`, `SHADOW_CONFIDENCE`, `STATUS`, `STAGE_REACHED`, `EVIDENCE_PACK_HASH` |
| `SHADOW_SPECIALIST_POSITION` | Per-role `STANCE`, `CONFIDENCE`, `RATIONALE`, `EVIDENCE_USED` |
| `SHADOW_CONFLICT_MAP` | Pairwise disagreements, `SEVERITY`, challenger/target roles |
| `SHADOW_CHALLENGE_TURN` | Single challenge round |
| `SHADOW_REVISION_TURN` | Post-challenge specialist revision |
| `SHADOW_CHAIR_RULING` | `SHADOW_STANCE`, `SHADOW_CONFIDENCE`, `PLURALITY_BASIS`, `CONFLICT_RESOLUTION`, `TOP_SUPPORTS`, `TOP_TENSIONS`, `SHADOW_TRADE_JSON` |

### Current limitations

1. **Evidence pack is not Phase 4-aware.** The pack (`pack_version 1.0.0`) reads from `STRUCTURAL_PROPOSAL_SNAPSHOT`, `COMMITTEE_HEARING`, and `COMMITTEE_EVIDENCE_ARTIFACT`. It does not read from `PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT` or `PROPOSAL_BOARD_THESIS_VERDICT`. All Phase 4 dossier context is absent.
2. **No position/open-trade revalidation.** The shadow board is scoped to proposal entry hearings. Ongoing position revalidation uses a separate `POSITION_HEALTH_REVIEW_AGENT` (single-agent, not panel).
3. **Gated off by default.** `SHADOW_BOARD_ENABLED=false` in production config unless explicitly set.
4. **Not the primary operator-facing flow.** It runs in background; LPA shows `ShadowBoardPanel` when data is available but the primary decision gate is still the real Committee 2.0 verdict + price guard.
5. **No RAG integration.** No literature evidence retrieved or presented to any agent or operator.

### Whether it has been running

The shadow board infrastructure is production-ready code. Whether it has been accumulating session data depends on whether `SHADOW_BOARD_ENABLED=true` has been set in `MIP.APP.APP_CONFIG` and whether Committee 2.0 orchestrate calls have been made for structural entries during that period. The bake-off views (`V_COMMITTEE_BAKEOFF_*`) and `COMMITTEE_BAKEOFF_*` tables can be used to audit real-vs-shadow comparison history.

### Separate system: position-health shadow

`POST /position-health/run-shadow` → `POSITION_HEALTH_REVIEW_AGENT` runs over `DAILY_POSITION_VERDICT` rows for open positions. This is a distinct system from the shadow board. It does not use `GET_SHADOW_EVIDENCE_SLICE` and is not the target of the RAG integration described here.

---

## Phase 4 Proposal Artifact Compatibility

### Artifacts currently included in shadow evidence pack

Evidence pack `pack_version 1.0.0` slices:

| Slice | Source | Present? |
|---|---|---|
| `proposal_meta` | `STRUCTURAL_TRADE_PROPOSALS`, `STRUCTURAL_PROPOSAL_SNAPSHOT` | Yes |
| `structural_state` | `STRUCTURAL_PROPOSAL_SNAPSHOT` | Yes |
| `thesis_summary` | `STRUCTURAL_PROPOSAL_SNAPSHOT.PROPOSAL_SUMMARY_JSON` | Yes (pre-Phase 4 summary) |
| `entry_zone` | `STRUCTURAL_PROPOSAL_SNAPSHOT.ENTRY_ZONE_JSON` | Yes |
| `live_price` | `COMMITTEE_HEARING.EVIDENCE_JSON` | Yes |
| `regime_state` | `COMMITTEE_HEARING.EVIDENCE_JSON` | Yes |
| `live_bars` | `COMMITTEE_HEARING.EVIDENCE_JSON` | Yes |
| `path_metrics` | `STRUCTURAL_PROPOSAL_SNAPSHOT.PATH_METRICS_JSON` | Yes |
| `mfe_mae` | `STRUCTURAL_PROPOSAL_SNAPSHOT.MFE_MAE_JSON` | Yes |
| `invalidation` | `STRUCTURAL_PROPOSAL_SNAPSHOT.INVALIDATION_JSON` + live cushion | Yes |
| `trust_label` | `STRUCTURAL_PROPOSAL_SNAPSHOT.TRUST_LABEL` | Yes |
| `deltas_summary` | `COMMITTEE_HEARING.DELTAS_JSON` | Yes |
| `artifacts_summary` | `COMMITTEE_EVIDENCE_ARTIFACT` (kinds only, no payloads) | Partial |

### Phase 4 artifacts missing from the shadow evidence pack

All of the following exist in `PROPOSAL_BOARD_THESIS_VERDICT` and `PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT` but are **absent** from `shadow_types.py::build_shadow_evidence_pack` and from `GET_SHADOW_EVIDENCE_SLICE`:

| Field | Source table | Column / JSON path | Missing from pack |
|---|---|---|---|
| `thesis_health` | `PROPOSAL_BOARD_THESIS_VERDICT` | `CHAIR_OUTPUT_JSON:thesis_health` | Yes |
| `prior_thesis_reference` | `PROPOSAL_BOARD_THESIS_VERDICT` | `CHAIR_OUTPUT_JSON:prior_thesis_reference` | Yes |
| `actionability_summary` | `PROPOSAL_BOARD_THESIS_VERDICT` | `CHAIR_OUTPUT_JSON:actionability_summary` | Yes |
| `final_action` / `final_direction` | `PROPOSAL_BOARD_THESIS_VERDICT` | `FINAL_ACTION`, `FINAL_DIRECTION` | Yes |
| `primary_reason_code` (Phase 4) | `PROPOSAL_BOARD_THESIS_VERDICT` | `PRIMARY_REASON_CODE` | Yes |
| `why_not_opposite` | `PROPOSAL_BOARD_THESIS_VERDICT` | `WHY_NOT_OPPOSITE` | Yes |
| `why_not_no_trade` | `PROPOSAL_BOARD_THESIS_VERDICT` | `WHY_NOT_NO_TRADE` | Yes |
| `board_dossier_id` | `STRUCTURAL_TRADE_PROPOSALS` | `BOARD_DOSSIER_ID` | Yes (in `proposal_meta` only as a pass-through column) |
| `board_run_id` | `STRUCTURAL_TRADE_PROPOSALS` | `BOARD_RUN_ID` | Yes |
| `continuation_quality` | `PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT` | `DOSSIER_PAYLOAD_JSON:actionability_context:continuation_quality` | Yes |
| `resistance_overhead_risk` | `PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT` | `DOSSIER_PAYLOAD_JSON:actionability_context:resistance_overhead_risk` | Yes |
| `candle_psychology` / `recent_cluster` | `PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT` | `DOSSIER_PAYLOAD_JSON:candle_psychology:recent_cluster` | Yes |
| `broken_resistance_as_support` | `PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT` | `DOSSIER_PAYLOAD_JSON:levels:broken_resistance_as_support` | Yes |
| `broken_resistance_support_confidence` | `PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT` | `DOSSIER_PAYLOAD_JSON:actionability_context:broken_resistance_support_confidence` | Yes |
| `current_range_position_pct` | `PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT` | `DOSSIER_PAYLOAD_JSON:structural_timeline_summary:current_range_position_pct` | Yes |
| `structural_timeline_summary` | `PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT` | `DOSSIER_PAYLOAD_JSON:structural_timeline_summary` | Yes |

### Note on availability

These Phase 4 fields are already surfaced in LPA via `GET /committee/proposal/{id}/board-explanation` → `BoardExplanationPanel` → `Phase4ChairSection`. They are read from `PROPOSAL_BOARD_THESIS_VERDICT` and `PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT` in `board/explanation.py`. The gap is that they are **not fed into the shadow committee evidence pack** that the specialist agents and chair receive as their working context.

### Recommended evidence-pack additions for Phase 4 upgrade

New slices to add to `ROLE_SLICE_MAP` and `build_shadow_evidence_pack`:

| Proposed slice key | Source fields | Suggested roles |
|---|---|---|
| `phase4_thesis_verdict` | `thesis_health`, `prior_thesis_reference`, `actionability_summary`, `final_action`, `primary_reason_code`, `why_not_opposite`, `why_not_no_trade`, `board_dossier_id`, `board_run_id` | `STRUCTURAL_THESIS`, `SHADOW_CHAIR` |
| `phase4_dossier_context` | `continuation_quality`, `resistance_overhead_risk`, `recent_cluster`, `broken_resistance_as_support`, `broken_resistance_support_confidence`, `current_range_position_pct`, `structural_timeline_summary` | `STRUCTURAL_THESIS`, `ENTRY_GEOMETRY`, `REGIME`, `PATH_TRADEABILITY`, `SHADOW_CHAIR` |

Pack version should be bumped from `1.0.0` to `2.0.0` when Phase 4 slices are added, and `SHADOW_BOARD_SESSION` should carry the pack version for audit.

---

## Deterministic Revalidation Retirement Plan

The goal is not to delete the price/bar/news guard but to stop presenting it as the primary intelligence review for the operator. The following describes the recommended lifecycle of each component.

### Keep as permanent safety gate

| Component | Status | Notes |
|---|---|---|
| `revalidate_live_action` (backend) | Keep | Price/bar freshness gate is correct safety logic |
| `REVALIDATED_PASS` / `REVALIDATED_FAIL` status | Keep | Required for submission gating |
| `price_guard` fields in overview payload | Keep | Useful diagnostic |
| `MAX_BAR_END_LAG_SEC`, `QUOTE_FRESHNESS_THRESHOLD_SEC` | Keep | Operational safety parameters |

### Move to diagnostics / de-emphasise in LPA UI

| Component | Recommendation | Risk |
|---|---|---|
| "Revalidate" button as primary UI action | De-emphasise; make it a "confirm prices" utility rather than the intelligence review | Low: the button still works; it just becomes secondary |
| Stale-row labels referencing "revalidation expired" | Rename to "prices not confirmed" or "guard check required" | Low: wording only |
| `isStaleRevalidationState` label that says "run Committee 2.0 to revalidate" | Reframe: "run Committee review" rather than "revalidate" | Low: wording only |
| `REVALIDATION_PREVIEW` SSE replay | Keep for diagnostics; remove from primary operator flow | Low |
| Legacy `_run_multiagent_dialogue` in `live.py` | Keep gated by `LIVE_STRUCTURAL_ONLY`; no removal until shadow board is the default | Medium |

### Keep temporarily as fallback

| Component | Condition for retirement |
|---|---|
| Deterministic `SP_RUN_DAILY_POSITION_VERDICT` | Keep until position-health shadow is validated and accepted as primary |
| `COMMITTEE_FINAL_DECISION` as primary gate | Keep until shadow board is the operational committee; becomes comparison/fallback |
| `committee2_live_bridge.py` | Keep; remove only after the new agentic verdict materialisation path is stable |

### Eventual deletion candidates (deferred, not now)

- `_run_multiagent_dialogue` / `COMMITTEE_ROLES` legacy path in `live.py` (gated by `LIVE_STRUCTURAL_ONLY=false`)
- Pre-Phase 4 `PROPOSAL_BOARD_AGENT_OUTCOME` / `PROPOSAL_BOARD_ORCHESTRATOR_VERDICT` (Phase 3 board, already superseded by Phase 4)
- `SP_RUN_PROPOSAL_BOARD` stub `567` (fail-closed; already inactive)

### Risk notes

- **Submission gate**: `REVALIDATED_PASS` is a hard submission gate in `live.py`. Any UI renaming must not remove this check.
- **Idempotency**: `orchestrate_committee2_structural_entry` is designed idempotent on the same `hearing_id` + `evidence_pack_hash`. Shadow board kick-off is also idempotent. No change required here.
- **Shadow board default-off**: `SHADOW_BOARD_ENABLED=false` is the current default. Before making shadow board operator-facing, this flag must be set, validated, and monitored.
- **Bake-off baseline**: `V_COMMITTEE_BAKEOFF_*` tracks real vs shadow outcomes. This comparison baseline should be reviewed before the shadow board is promoted to primary.

---

## Proposed Agentic Revalidation LPA Flow

### Target operator experience

1. Operator opens LPA. Pending structural entries show committee intelligence (shadow board verdict, specialists, chair) as the primary review panel — not just the "last Committee 2.0 run".
2. If shadow board has a fresh result for the current `evidence_pack_hash`, LPA surfaces it prominently: shadow stance, shadow confidence, top supports, top tensions, chair ruling.
3. If no shadow result exists (stale hash, new price snapshot), operator clicks "Run Committee Review" → triggers `POST .../committee2/orchestrate` which refreshes the real board and kicks off a new shadow run. The shadow result streams in and updates the panel.
4. The legacy "Revalidate" button (price guard) remains as a small "Confirm prices" action, not as the primary intelligence button.
5. Submission gate logic (`REVALIDATED_PASS`) remains unchanged.

### API shape (no new endpoints required)

| Use | Existing endpoint | Change needed |
|---|---|---|
| Trigger committee review + shadow kick-off | `POST .../committee2/orchestrate` | None |
| Read shadow result | `GET .../shadow-board` | None |
| Read Phase 4 board explanation | `GET .../board-explanation` | None |
| Price/bar guard | `POST .../revalidate` | None |
| Submit | `POST .../submit-only` | None |

No new FastAPI endpoints need to be created. The operator flow change is purely in how LPA's UI presents existing data.

### Snowflake source of truth for agentic revalidation output

| Data | Table / View |
|---|---|
| Shadow verdict (stance, confidence) | `SHADOW_BOARD_SESSION` |
| Specialist positions | `SHADOW_SPECIALIST_POSITION` |
| Conflicts / challenges / revisions | `SHADOW_CONFLICT_MAP`, `SHADOW_CHALLENGE_TURN`, `SHADOW_REVISION_TURN` |
| Chair ruling | `SHADOW_CHAIR_RULING` |
| Phase 4 board thesis | `PROPOSAL_BOARD_THESIS_VERDICT` |
| Phase 4 dossier context | `PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT` |
| Real committee decision | `COMMITTEE_FINAL_DECISION` (retained as comparison) |

### Operator actions preserved

- View shadow specialist stances and chair ruling
- See real vs shadow comparison via `V_COMMITTEE_BAKEOFF_*`
- Trigger fresh committee review when hash is stale
- Confirm price guard before submission
- Submit when `REVALIDATED_PASS` + `submission_allowed`

### Fallback behavior

If `SHADOW_BOARD_ENABLED=false` or shadow run fails: LPA falls back to real Committee 2.0 final decision as today. The shadow panel shows a "no shadow result available" state (already handled by `ShadowBoardPanel` `PENDING`/`RUNNING` states).

---

## Future RAG Insertion Point

> **Prerequisite: Phase 4 evidence-pack upgrade must be completed before RAG is wired.**  
> The shadow committee agents must be operating with dossier-aware context before literature evidence is added. Adding RAG to a Phase 1.0.0 evidence pack would mean specialists receive rich literature support but are missing critical Phase 4 field context, creating a misleading intelligence mix.

### Exact insertion point

**File:** `MIP/apps/mip_ui_api/app/committee/shadow_board.py`  
**Function:** `orchestrate_shadow_board`  
**Stage:** After Stage 0 (evidence pack staged to `SHADOW_EVIDENCE_PACK_CACHE`) and **before** Stage 1 (specialist agents call `GET_SHADOW_EVIDENCE_SLICE`)

```python
# Future: after _stage_evidence_pack(pack, session_id) succeeds

# RAG literature support (Phase 4-aware evidence pack required first)
lit_pack = await _fetch_literature_rag_pack(
    query_text=_build_rag_query_from_pack(pack),
    top_k=5,
)
if lit_pack:
    _extend_evidence_pack_with_literature(pack, session_id, lit_pack)
    # Re-stage updated pack to SHADOW_EVIDENCE_PACK_CACHE
```

### Query construction

The RAG query should be synthesised from Phase 4-aware pack fields:

- `thesis_summary.proposal_summary` (setup family + direction)
- `phase4_thesis_verdict.thesis_health` + `prior_thesis_reference` (from Phase 4 upgrade)
- `structural_state.structural_state_now`
- `invalidation.invalidation_json` (invalidation context)
- Setup family from `proposal_meta.setup_family`

Example constructed query for `FAILED_BREAKOUT + TREND_PULLBACK` thesis:

```
"failed breakout reversal no follow-through {setup_family} {structural_state_now} 
 {thesis_health} invalidation risk trailing stop"
```

Call: `CALL MIP.KNOWLEDGE.SP_SEARCH_REVALIDATION_LITERATURE(:query_text, 5)` — or the Python equivalent `search_literature_rag.py` logic inlined.

### New evidence slice: `literature_support`

Add to `ROLE_SLICE_MAP` for `SHADOW_CHAIR` and all specialist roles that benefit (at minimum `STRUCTURAL_THESIS`, `PROTECTION_EXIT`, `PATH_TRADEABILITY`):

```python
"literature_support": {
    "retrieved_cards": [
        {
            "card_id": "...",
            "concept_name": "...",
            "concept_family": "...",
            "source_book": "...",
            "page_start": ...,
            "page_end": ...,
            "display_text": "..."
        },
        ...
    ],
    "retrieval_id": "...",          # links to LITERATURE_RAG_RETRIEVAL_AUDIT
    "query_text": "...",
    "guardrail": "Advisory literature concepts only. Not market evidence. Not a trade signal."
}
```

The guardrail string must be present in every `literature_support` slice and must appear in the agent system prompt for `SHADOW_STRUCTURAL_THESIS_AGENT`, `SHADOW_PROTECTION_EXIT_AGENT`, and `SHADOW_CHAIR_AGENT`.

### Suggested persistence

| What | Where |
|---|---|
| Retrieved CARD_IDs + scores per session | New column `LITERATURE_RETRIEVAL_ID` on `SHADOW_BOARD_SESSION`, FK to `MIP.KNOWLEDGE.LITERATURE_RAG_RETRIEVAL_AUDIT.RETRIEVAL_ID` |
| Full literature pack per session | New column `LITERATURE_SUPPORT_JSON VARIANT` on `SHADOW_BOARD_SESSION` |
| Audit trail | Existing `MIP.KNOWLEDGE.LITERATURE_RAG_RETRIEVAL_AUDIT` (one row per shadow board session that triggers retrieval) |

No new tables required: `LITERATURE_RAG_RETRIEVAL_AUDIT` already exists for this purpose.

### Guardrails

- Retrieved cards must carry explicit "not market evidence, not a trade signal" language in every `display_text`
- The `literature_support` slice is labelled advisory in the slice metadata
- Agents must not cite literature as evidence of price action; only as conceptual framing
- Chair ruling `TOP_SUPPORTS` / `TOP_TENSIONS` must not be sourced solely from literature cards
- No literature card may override `invalidation_evidence` or `REVALIDATED_PASS` / `REVALIDATED_FAIL`

---

## Recommended Implementation Phases

### Phase 1 — Phase 4 evidence-pack upgrade for shadow/agentic committee

**Goal:** Shadow specialist agents and chair receive full Phase 4 dossier context.  
**Files to change:**

| File | Change |
|---|---|
| `shadow_board.py::_fetch_hearing_data` | Add JOIN to `STRUCTURAL_TRADE_PROPOSALS.BOARD_DOSSIER_ID` → `PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT` and `PROPOSAL_BOARD_THESIS_VERDICT` |
| `shadow_types.py::build_shadow_evidence_pack` | Add `phase4_thesis_verdict` and `phase4_dossier_context` slices from fetched data |
| `shadow_types.py::ROLE_SLICE_MAP` | Add new slice keys to appropriate roles |
| `542_shadow_board_agents.sql::GET_SHADOW_EVIDENCE_SLICE` | Add new slice keys to `_ALLOWED_SLICES` |
| `shadow_board.py` | Bump `pack_version` to `"2.0.0"` |
| `541_shadow_board_tables.sql` | No schema change; `SHADOW_BOARD_SESSION` can carry `PACK_VERSION` via existing metadata or add column |

**Acceptance criteria:** Shadow specialists receive `thesis_health`, `continuation_quality`, `candle_psychology`, `broken_resistance_as_support`, and Phase 4 board lineage identifiers in their working slices.

### Phase 2 — LPA operator-flow transition

**Goal:** Shadow board verdict is the primary operator-facing intelligence panel. Price/bar guard is demoted to a utility action.  
**Files to change:**

| File | Change |
|---|---|
| `LivePortfolioActivity.jsx` | Promote `ShadowBoardPanel` stance/confidence/chair to primary verdict display; demote "Revalidate" button to "Confirm prices" utility |
| `LivePortfolioActivity.jsx` | Rename stale-row label from "Revalidation expired" to "Committee review required" or "Prices not confirmed" |
| `LivePortfolioActivity.jsx` | Primary action for structural entries: "Run Committee Review" (triggers orchestrate, shadow in background) |
| `ShadowBoardPanel.jsx` | Potentially elevate from collapsible panel to primary verdict section |
| LPA overview payload | No backend change; frontend re-prioritises existing shadow fields |

**No new API endpoints required.** No changes to submission gating or price guard logic.

**Acceptance criteria:** An operator on LPA sees shadow board stance/confidence/chair as the primary decision support display; price guard is a small secondary action.

### Phase 3 — RAG insertion (only after Phase 1 is complete)

**Goal:** Shadow evidence pack includes a `literature_support` slice with approved revalidation literature cards retrieved via `SP_SEARCH_REVALIDATION_LITERATURE`.  
**Files to change:**

| File | Change |
|---|---|
| `shadow_board.py` | Add `_fetch_literature_rag_pack`, `_build_rag_query_from_pack`, `_extend_evidence_pack_with_literature` |
| `shadow_types.py` | Add `literature_support` to `ROLE_SLICE_MAP` (at minimum `STRUCTURAL_THESIS`, `PROTECTION_EXIT`, `PATH_TRADEABILITY`, `SHADOW_CHAIR`) |
| `542_shadow_board_agents.sql::GET_SHADOW_EVIDENCE_SLICE` | Add `literature_support` to `_ALLOWED_SLICES` |
| `541_shadow_board_tables.sql` | Add `LITERATURE_RETRIEVAL_ID STRING`, `LITERATURE_SUPPORT_JSON VARIANT` to `SHADOW_BOARD_SESSION` |
| Phase 4 specialist agent system prompts (`570`) | Add guardrail clause for literature slices |

**No changes to** `MIP.KNOWLEDGE` tables (read-only from the shadow board path). No changes to `REVALIDATED_PASS` / submission gate.

**Acceptance criteria:** Shadow board session row carries a `LITERATURE_RETRIEVAL_ID` linking to `LITERATURE_RAG_RETRIEVAL_AUDIT`; specialists and chair receive `literature_support` slice; all literature is labelled advisory in agent output.

### Phase 4 — Retire deterministic price-guard label from primary LPA flow; move to diagnostics

**Goal:** LPA no longer presents the price/bar guard as a revalidation board. All remaining deterministic outputs (real Committee 2.0 final decision) become comparison/fallback.  
**Files to change:**

| File | Change |
|---|---|
| `LivePortfolioActivity.jsx` | Move legacy committee fields to a collapsible "Real board (deterministic)" diagnostic section |
| `SSE live-prompt` | Retain as replay diagnostic; remove from primary decision flow |

**Do not delete any backend functions.** Price guard logic must remain for submission safety.

### Phase 5 — Proposal-chair RAG challenge / future consideration

**Deferred.** After Phase 3 is validated, evaluate whether the Phase 4 proposal chair (`PHASE4_CHAIR_PORTFOLIO_PM_AGENT`) should also receive a `literature_support` slice as a challenge/validation layer at proposal generation time — not only at entry revalidation time.

---

## Exact Files To Change (by phase)

| Phase | File | Change type |
|---|---|---|
| 1 | `MIP/apps/mip_ui_api/app/committee/shadow_board.py` | Evidence pack build — add Phase 4 queries |
| 1 | `MIP/apps/mip_ui_api/app/committee/shadow_types.py` | `ROLE_SLICE_MAP` + `build_shadow_evidence_pack` — add Phase 4 slices |
| 1 | `MIP/SQL/app/542_shadow_board_agents.sql` | `GET_SHADOW_EVIDENCE_SLICE` — add allowed slice keys |
| 1 | `MIP/SQL/app/541_shadow_board_tables.sql` | Optional: add `PACK_VERSION` column to `SHADOW_BOARD_SESSION` |
| 2 | `MIP/apps/mip_ui_web/src/pages/LivePortfolioActivity.jsx` | UI flow + labels |
| 2 | `MIP/apps/mip_ui_web/src/components/committee/ShadowBoardPanel.jsx` | Elevation to primary verdict |
| 3 | `MIP/apps/mip_ui_api/app/committee/shadow_board.py` | RAG fetch + slice injection |
| 3 | `MIP/apps/mip_ui_api/app/committee/shadow_types.py` | `literature_support` slice |
| 3 | `MIP/SQL/app/542_shadow_board_agents.sql` | `literature_support` in allowed slices |
| 3 | `MIP/SQL/app/541_shadow_board_tables.sql` | `LITERATURE_RETRIEVAL_ID`, `LITERATURE_SUPPORT_JSON` |
| 3 | `MIP/SQL/app/570_phase4_agentic_board_agents.sql` | Guardrail in specialist prompts |
| 4 | `MIP/apps/mip_ui_web/src/pages/LivePortfolioActivity.jsx` | Deterministic fields → diagnostics section |

---

## Open Questions / Risks

| Question | Risk level | Notes |
|---|---|---|
| Is `SHADOW_BOARD_ENABLED` currently true in production? | Medium | Determines whether shadow sessions have been accumulating. Check `MIP.APP.APP_CONFIG` before Phase 2. |
| How complete is the bake-off data? | Medium | `V_COMMITTEE_BAKEOFF_*` should be reviewed before shadow board is promoted to primary. |
| Does `PROPOSAL_BOARD_SYMBOL_DOSSIER_SNAPSHOT` have a row for every active proposal tied to a live action? | High | Required for Phase 1. Some older proposals pre-Phase 4 may not have a dossier snapshot; pack builder must handle NULL gracefully. |
| `SHADOW_BOARD_TIMEOUT_SEC` under load | Medium | If shadow board takes >120s (default timeout), session may be marked DEGRADED. Review before Phase 2. |
| Phase 4 stub `567_sp_run_proposal_board_phase4_disabled_stub.sql` is the active SP | Low | Real Phase 4 runs via Python orchestrator (`scripts/proposal_board_phase4/`). Confirm this is the current production path before Phase 1. |
| Literature RAG corpus has only 10 approved cards (BROOKS_2026_05_21 batch) | Low now, Medium later | 10 cards may produce narrow retrieval results. Approve more cards before Phase 3. |
| Single-agent vs panel for position health revalidation | Low | `POSITION_HEALTH_REVIEW_AGENT` is separate from shadow board. This audit covers proposal-entry revalidation only; position health transition is a separate planning task. |
| Proposal-chair RAG (Phase 5) interacts with Phase 4 disabled stub | Low | Deferred; note for future. |

---

## Files Inspected in This Audit

**React UI:**
- `MIP/apps/mip_ui_web/src/pages/LivePortfolioActivity.jsx`
- `MIP/apps/mip_ui_web/src/pages/LpaCommittee2Exhibits.jsx`
- `MIP/apps/mip_ui_web/src/components/board/BoardExplanationPanel.jsx`
- `MIP/apps/mip_ui_web/src/components/board/Phase4ChairSection.jsx`
- `MIP/apps/mip_ui_web/src/components/committee/ShadowBoardPanel.jsx`
- `MIP/apps/mip_ui_web/src/config/apiBase.js`

**FastAPI backend:**
- `MIP/apps/mip_ui_api/app/routers/live.py`
- `MIP/apps/mip_ui_api/app/routers/committee.py`
- `MIP/apps/mip_ui_api/app/routers/position_health.py`
- `MIP/apps/mip_ui_api/app/routers/cockpit.py`
- `MIP/apps/mip_ui_api/app/committee/shadow_board.py`
- `MIP/apps/mip_ui_api/app/committee/shadow_types.py`
- `MIP/apps/mip_ui_api/app/committee/committee2_live_bridge.py`
- `MIP/apps/mip_ui_api/app/services/board/explanation.py`
- `MIP/apps/mip_ui_api/app/services/position_health/payload_builder.py`

**Snowflake SQL:**
- `MIP/SQL/app/540_committee2_tables.sql`
- `MIP/SQL/app/541_shadow_board_tables.sql`
- `MIP/SQL/app/542_shadow_board_agents.sql`
- `MIP/SQL/app/543_shadow_trade_linkage.sql`
- `MIP/SQL/app/550_daily_position_verdict_tables.sql`
- `MIP/SQL/app/551_sp_run_daily_position_verdict.sql`
- `MIP/SQL/app/552_sp_update_shadow_position_lifecycle.sql`
- `MIP/SQL/app/553_position_health_review_agent.sql`
- `MIP/SQL/app/560_proposal_board_tables.sql`
- `MIP/SQL/app/561_sp_run_proposal_board.sql`
- `MIP/SQL/app/564_phase4_symbol_dossier_board_tables.sql`
- `MIP/SQL/app/565_sp_run_proposal_board_phase4_symbol_dossier.sql`
- `MIP/SQL/app/567_sp_run_proposal_board_phase4_disabled_stub.sql`
- `MIP/SQL/app/568_phase4_dossier_pack_cache.sql`
- `MIP/SQL/app/569_phase4_get_dossier_slice.sql`
- `MIP/SQL/app/570_phase4_agentic_board_agents.sql`
- `MIP/SQL/app/600_committee_bakeoff_tables.sql`
- `MIP/SQL/app/601_sp_refresh_committee_bakeoff.sql`
- `MIP/SQL/views/mart/v_position_health.sql`
- `MIP/SQL/views/mart/v_proposal_board_symbol_dossier.sql`
- `MIP/SQL/views/mart/v_committee_bakeoff.sql`

---

*No runtime, API, UI, or Snowflake app-schema code was modified in this audit.*
