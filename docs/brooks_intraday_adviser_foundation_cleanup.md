# Brooks Intraday Lab — Adviser foundation cleanup (architecture record)

**Date:** 2026-08-07  
**Scope:** Compute deactivation + policy guards. No DROP/DELETE/TRUNCATE, no Adviser build, no RAG re-index.

## Retained architecture (foundation)

| Capability | Primary modules / objects |
|------------|---------------------------|
| 5m historical bars | `BROOKS_INTRADAY_HISTORICAL_BAR`, acquisition |
| RTH calendar / replay schedule | `calendar`, `replay_engine` |
| Bar-at-a-time replay (objective ruleset) | `process_next_bar`, `BROOKS_INTRADAY_REPLAY_STATE` |
| Learning View | `learning_view`, `learning_bar_narrative`, `learning_service` |
| Daily frozen PAA | `BROOKS_INTRADAY_DOSSIER` |
| Sim $1k, long-only, one position | `constants`, `simulation_runner`, PM/re-entry V0.1 |
| Trade ledger (read + future Adviser sim) | `BROOKS_INTRADAY_SIM_TRADE`, `BLOCKED_SIGNAL` |
| RAG / literature | `MIP.KNOWLEDGE.*`, Cortex Search (read) |

**New runs** default to `lab_execution_mode=ADVISER_FOUNDATION` and `lab_pipeline_profile=BROOKS_INTRADAY_ADVISER_V0_1` in `CONFIG_JSON`.

## Retired from future execution path (`DIAGNOSTIC_LEGACY`)

Blocked unless `?diagnostic_legacy=true`, run mode `DIAGNOSTIC_LEGACY`, or `BROOKS_LAB_LEGACY_PIPELINE_ENABLED=1`:

- Phase 4 **objective bulk**
- Phase 5 **pattern bulk**, pattern next-bar, pattern reset
- Phase 6 / 6B **context bulk** V0.1–V0.4 (incl. V0.4 paths A–E, shadow FSM, daily-verdict FSM)
- Phase 7 **simulation bulk** driven by legacy context
- Phase 9 pipeline stages that invoke the above (when env not set)

Code markers: `lab_execution_policy.py`, `lab_pipeline_registry.py`, guards in `store.py`, `replay_engine.py`, `context_replay.py`, `simulation_replay.py`.

## Preserved historical evidence (read-only review)

| Label | ID |
|-------|-----|
| Week 1 V0.3 context | `3defa3de-d699-424a-8ceb-78020b453284` |
| Baseline V0.1 simulation | `8de2e63f-99f8-46ed-a2ec-f5b8c321c651` |
| Canonical PM certification | `125eb282-3dc7-41a7-8fbf-602f75ad6b51` |
| V0.4 W1/W2 disposable | context `e35b6713…`, `e912b33d…`; sim `03eaf144…`, `622321da…` |

Learning View: `resolve_attempt_chain`, `REVIEW_CHAIN_SUPPLEMENTS`, Snowflake-loaded alternates — **unchanged** for review.

## Target Adviser data flow (design)

```
BROOKS_INTRADAY_HISTORICAL_BAR (+ dossier)
        ↓
lightweight observation / event builder (future)
        ↓
BROOKS_INTRADAY_ADVISER_AUDIT (table not deployed yet)
        ↓
thesis + watch conditions (local monitor; wake → RAG)
        ↓
simulator / PM / re-entry V0.1
        ↓
Learning View
```

Avoid: bars → objective bulk → pattern bulk → context bulk → certification sim chain for **new** work.

## Snowflake object inventory

Full machine-readable list: `GET /api/research/brooks-intraday/meta` (`snowflake_inventory_count`, `pipeline_stages`, `compute_hotspots`) and `lab_pipeline_registry.snowflake_object_inventory()`.

Summary:

| Object | LV read | Replay write | Adviser | Stop write (foundation) |
|--------|---------|--------------|---------|-------------------------|
| HISTORICAL_BAR | indirect | prepare | yes | no |
| DOSSIER | yes | prepare | yes | no |
| BAR_OBSERVATION | yes | objective bulk/step | reuse candidate | stop bulk |
| PATTERN_INSTANCE / BAR_PATTERN_LINK | yes | pattern bulk/step | no | **yes** |
| CONTEXT_ATTEMPT / CONTEXT_OBSERVATION | yes | context bulk | no | **yes** |
| SIM_* | yes | sim bulk | yes | stop legacy bulk |
| BROOKS_EXPERIMENT_* | no | phase 9 | no | **yes** |
| KNOWLEDGE RAG | no | manual refresh only | yes | no |

## Compute hotspots (full-week legacy chain, ~1,560 steps)

1. **Context bulk V0.2–V0.4** — largest WH + insert volume (~6k+ context observation rows/week).
2. **Pattern bulk V0.3** — instance + link writes every step.
3. **Objective bulk** — bar observations.
4. **Simulation bulk** — CPU-heavy scan, fewer writes.
5. **Bar/dossier prepare** — amortized once per week.

**Estimated savings:** Deactivating the legacy chain on new runs avoids **~100%** of repeat context/pattern bulk WH for accidental replays; versus event-driven Adviser design (~90% fewer RAG wakes vs per-bar context — separate from SF inserts). Foundation guards prevent the dominant **context + pattern** insert path by default.

## Future writes stopped (effective immediately on new runs)

- `CONTEXT_OBSERVATION`, new disposable `CONTEXT_ATTEMPT`
- `PATTERN_INSTANCE`, `BAR_PATTERN_LINK`
- Legacy-linked `SIMULATION_ATTEMPT` / trades from bulk replay
- Phase 9 stage progression that calls legacy bulk (without env)

**Still allowed:** bar-at-a-time objective observations, prepare/dossier, Learning View reads, manual diagnostic with opt-in.

## Components requiring no change

Learning View UI, chart/grid/navigation, review chain resolution, PM V0.1 / re-entry V0.1 **read paths**, pinned attempts, Freeze V1, LPA, RAG corpus.

## Components removable later (after Adviser cutover + approval)

- Pattern tables (optional archive)
- Bulk-only experiment orchestration tables
- Context ruleset V0.1–V0.2 code paths (keep V0.3/V0.4 read-only for history)

## Proposed cleanup requiring explicit approval (not executed)

1. **Snowflake:** No DROP in this stage. Later proposal: drop or rename unused tasks/streams if any exist; archive `BROOKS_EXPERIMENT_*` after Phase 9 retirement.
2. **API:** Remove `/context/bulk` and `/simulation/bulk` after diagnostic window (optional).
3. **Data:** TTL or storage policy on non-preserved `CONTEXT_ATTEMPT` rows (exclude preserved IDs list in `lab_execution_policy.py`).
4. **Deploy:** Create `BROOKS_INTRADAY_ADVISER_AUDIT` DDL when Adviser build starts.

## Long-only RAG policy

See `MIP/docs/brooks_adviser_long_only_rag_policy.md`.

## Verification

- Unit: `tests/test_brooks_lab_execution_policy.py`
- UI API restart required for guards to apply.
- Smoke: confirm preserved context/sim attempts exist in Snowflake (read-only COUNT).
