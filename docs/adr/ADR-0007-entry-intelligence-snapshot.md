# ADR-0007: Entry Intelligence Snapshot (EIS) and lifecycle naming

## Status

Accepted — Phase 1 backbone (immutable snapshot + linkage + closeout).

## Context

MIP previously used the word “worlds” for several different mechanisms (portfolio/day Policy Parallel Worlds, committee portfolio evidence, LIC analog bands, LIC runtime heuristics). Operators and engineers conflated them. We need **precise internal names**, a **single immutable pre-trade record** per proposal lineage, and **auditable linkage** from proposal → execution → closeout.

## Decision

### 1. Policy Parallel Worlds (PPW)

- **What:** Existing Snowflake portfolio/day counterfactual engine (`SP_RUN_PARALLEL_WORLDS`, `PARALLEL_WORLD_*`, mart views, `/parallel-worlds` UI).
- **Phase 1:** **Unchanged** — no engine or mart logic changes.
- **Role:** Slow-moving **policy / regret** context; optional **input** to EIS summaries in later phases.

### 2. Entry Intelligence Snapshot (EIS)

- **What:** New **immutable** row in `MIP.LIVE.ENTRY_INTEL_SNAPSHOT` created when a proposal enters the lifecycle (or is ensured before live promotion).
- **Grain:** **One proposal** (natural key `PROPOSAL_ID`); supersession = **new row** with higher `EIS_VERSION`, never `UPDATE` of `WORLDS_SPEC` / `ALPHA_SPEC`.
- **Role:** **Bridge** between training/HOD inputs and the **trade lifecycle** (committee, execution, closeout). This is the canonical “what we knew pre-trade” for audit.

### 3. Historical Outcome Distribution (HOD)

- **What:** Distribution built from **analog** historical outcomes (e.g. `RECOMMENDATION_OUTCOMES` / LIC analog matching — today’s `LicWorldsScenarios` / bootstrap analog packs).
- **Role:** **Source data** for `WORLDS_SPEC` inside EIS (Phase 1 may stub; Phase 2+ fills).
- **Not interchangeable with:** PPW (portfolio/day) or RSM (session heuristic).

### 4. Runtime Scenario Mix (RSM)

- **What:** **Live-session** deterministic scenario list from `build_scenario_worlds` (tape/vol/stop proximity heuristics).
- **Role:** **Post-entry UX** and intraday reasoning; **not** persisted as pre-trade truth.
- **Not interchangeable with:** EIS, HOD, or PPW.

## User-facing wording (UI)

| Internal | Suggested UI label |
|----------|-------------------|
| PPW | “Parallel Worlds” (keep product name) + subtitle *Portfolio policy (slow-moving)* |
| EIS | “Entry analysis” / “Pre-trade snapshot” |
| HOD | “Similar setups (history)” / “Historical outcomes” |
| RSM | “Live scenarios” / “How this session could play out” |

**HOD** and **RSM** are **internal abbreviations** by default; expose in UI only as the friendly labels above (optional dev tooltip).

## Consequences

- All lifecycle audit questions resolve to: **which `SNAPSHOT_ID`** was bound at committee/execution/closeout.
- Application roles that must not mutate history use **append-only** grants (see `411_entry_intel_grants.sql`).
- Committee and APIs must pass **`entry_intel_snapshot_id`** explicitly in payloads and verdict envelopes.

## References

- Implementation plan: `.cursor/plans/parallel_worlds_implementation_roadmap.plan.md`
- DDL: `MIP/SQL/app/410_entry_intel_lifecycle.sql`
