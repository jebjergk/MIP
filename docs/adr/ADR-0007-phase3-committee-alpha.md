# ADR-0007 Phase 3 addendum: Committee vs Entry Intelligence (alpha baseline)

## Status

Accepted — implemented in `mip_ui_api` committee run + apply paths (`live.py`).

## Goals

- Make **EIS / ALPHA_SPEC** structurally part of committee decisioning, not background JSON.
- Persist **explicit override classes** and **justification audit** on `COMMITTEE_VERDICT.VERDICT_JSON`.
- Stay compatible with **missing EIS**, **EIS_SCHEMA_V1 stubs**, and **EIS_SCHEMA_V2** rows.

## Effective baseline

An **actionable alpha baseline** exists only when `ALPHA_SPEC` has a real `recommended_action` in `ENTER|REDUCE|SKIP` and is **not** a Phase 1 stub (`stub` + `phase` 1). Otherwise `alpha_override_class` = `NO_ALPHA_BASELINE`.

## Override classes (`alpha_override_class`)

| Class | Meaning (ENTRY) |
|-------|------------------|
| `ACCEPT_ALPHA` | Joint recommendation matches deterministic alpha posture (e.g. ENTER+PROCEED, SKIP+BLOCK). |
| `REDUCE_VS_ALPHA` | Committee more cautious than alpha ENTER (PROCEED_REDUCED or implied size down vs full PROCEED). |
| `INCREASE_VS_ALPHA` | Committee more aggressive than alpha (e.g. PROCEED vs REDUCE baseline, or PROCEED/PROCEED_REDUCED vs SKIP). |
| `BLOCK_DESPITE_ALPHA` | Alpha would enter (ENTER) but committee BLOCK. |
| `NO_ALPHA_BASELINE` | No snapshot, stub, or unknown alpha action. |
| `UNKNOWN_OVERRIDE` | Unmapped pair — manual review. |

**EXIT** intent: justification status `NOT_APPLICABLE_EXIT`; override class still computed for audit where baseline exists.

## Reason codes (deterministic)

Every completed verdict appends:

- `ALPHA_OVERRIDE_<alpha_override_class>`
- `ALPHA_DEVIATION_JUSTIFICATION_<status>` where status is:
  - `NOT_REQUIRED` — ACCEPT / no baseline / unknown mapping
  - `PRESENT` — role outputs contain baseline alignment/deviation tokens (heuristic scan)
  - `MISSING` — override requires explanation but no token matched
  - `NOT_EVALUATED_MANUAL_APPLY` — `committee/apply` stream path (no per-role outputs)
  - `NOT_APPLICABLE_EXIT` — exit committee

## Prompt contract (multi-agent run)

When an actionable baseline exists for **ENTRY**, each role must include in `reasons` either:

- `ALPHA_BASELINE_ALIGNED`, or  
- a token starting with `ALPHA_BASELINE_DEVIATION:` plus a short explanation.

This supports the `PRESENT` / `MISSING` heuristic.

## `entry_intel_audit_v1` (VERDICT_JSON)

Nested object with:

- `comparison_rule_version` = `ALPHA_COMMITTEE_V3`
- Snapshot id, `eis_source_version`, `eis_version`, schema versions, HOD sample size
- Baseline action, size band, EV net
- Committee recommendation, override class, legacy alignment, notes
- Justification status, consensus note, manual apply flag, role count

Flat legacy keys (`alpha_baseline_action`, `alpha_committee_alignment`, `alpha_override_class`, …) remain on `VERDICT_JSON` for existing readers.

## Consensus policy (v1)

- **BLOCK_DESPITE_ALPHA** — uses existing committee BLOCK supermajority; high-impact, explicit rationale expected in role reasons.
- **INCREASE_VS_ALPHA** — no extra gate in Phase 3; flagged for audit; Phase 4+ may add gates.

## Deferred (not Phase 3)

- Rich closeout alignment scoring, LIC UI, PPW redesign, automated block on `JUSTIFICATION_MISSING`.

## Open operational tracking (unchanged)

- `411` append-only role deployment (ACCOUNTADMIN).
- Real `ENTRY_ACTION_ID` / `TRADE_CLOSEOUT` / `GET /live/entry-intel/summary/by-action/{id}` validation when linked actions exist.
