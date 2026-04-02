# ADR-0007 Phase 4: TRADE_CLOSEOUT broker truth + ALIGN_RULE_V1

## Status

Accepted — implemented in `entry_intel_hooks.py`, `closeout_alignment_v1.py`, `TRADE_CLOSEOUT` DDL, and `V_ENTRY_INTEL_LIFECYCLE_RECONSTRUCTION`.

## Goals

- Persist a **durable, auditable** closeout row: frozen **pre-trade EIS / alpha** expectations, **broker-grounded** exit metrics, and **deterministic** alignment vs realized outcome.
- Respect **Phase 3** `alpha_override_class` when `COMMITTEE_VERDICT` exists for the entry action’s `COMMITTEE_RUN_ID`.
- Stay safe with **missing EIS**, **stub ALPHA_SPEC**, **EIS_SCHEMA_V2**, and **missing committee** metadata.

## Closeout truth model (broker wins)

**Finalization trigger:** an **EXIT** `LIVE_ACTIONS` row’s `LIVE_ORDERS` order reaches **`FILLED`** (full fill for that order). The existing `update_live_order_status` path calls `maybe_write_trade_closeout_on_exit_filled`.

**Authoritative fields for economics:**

| Field | Source |
|--------|--------|
| Entry price / qty | `LIVE_ORDERS` for `ENTRY_ACTION_ID` with `QTY_FILLED > 0` and `AVG_FILL_PRICE` not null; **weighted average** by qty; deterministic `ORDER_ID` ordering |
| Exit price / qty | Same for `EXIT_ACTION_ID` |
| `ENTRY_TS` | Minimum `FILLED_AT` among contributing entry orders (nullable if missing) |
| `EXIT_TS` | Maximum `FILLED_AT` among exit orders, else `current_timestamp()` at insert |
| `REALIZED_RETURN_PCT` | Position return: long `(exit−entry)/entry`; short `(entry−exit)/entry` using entry `LIVE_ACTIONS.SIDE` (`SELL` ⇒ short) |
| `REALIZED_SIZE` | `min(entry_qty, exit_qty)` |
| `REALIZED_PNL` | `(exit−entry)*size` long; `(entry−exit)*size` short (notional in price units; no contract multiplier in v1) |

**Internal vs broker:** marks on the action row do **not** override `LIVE_ORDERS` fills. If fills are missing, `REALIZED_*` stay null and reason codes include `MISSING_ENTRY_FILL` / `MISSING_EXIT_FILL`.

**Flat / closed position:** v1 assumes the **exit FILLED** event that triggers the hook corresponds to closing the position for lifecycle purposes; there is no separate broker ledger join in Phase 4.

## Frozen entry expectation (`FROZEN_ENTRY_EXPECTATION` VARIANT)

Written at closeout from `ENTRY_INTEL_SNAPSHOT` (via link `SNAPSHOT_ID`) plus committee:

- `expected_value_net_at_entry`, `confidence_band_at_entry`, `downside_risk_band_at_entry`
- `recommended_action_at_entry`, `recommended_size_band_at_entry`
- `eis_source_version`, `eis_version`
- `alpha_schema_version_at_entry`
- `alpha_override_class_at_entry` from `COMMITTEE_VERDICT.VERDICT_JSON.alpha_override_class` when `COMMITTEE_RUN_ID` resolves

No recompute of live alpha at read time for alignment; this object is the **audit baseline**.

## ALIGN_RULE_V1 (`ALIGNMENT_JSON`)

| Key | Description |
|-----|-------------|
| `comparison_rule_version` | `ALIGN_RULE_V1` |
| `alignment_class` | `ALIGNED` \| `NEUTRAL` \| `ADVERSE` |
| `realized_outcome_class` | `FAVORABLE` \| `FLAT` \| `UNFAVORABLE` \| null |
| `alignment_reason_codes` | Ordered, deduplicated string codes |
| `summary` | Short deterministic sentence |
| `epsilon_flat_pct` | Band for FLAT (default **0.0005** fractional = 5 bps) |

### `realized_outcome_class`

Uses **signed position return** (positive = good for the position):

- `FLAT` if `abs(return) ≤ epsilon_flat_pct`
- `FAVORABLE` if above the band in the profitable direction
- `UNFAVORABLE` if below

If return cannot be computed, class is null and codes include broker-data gaps.

### `alignment_class` (vs alpha baseline)

Baseline action from the same rules as Phase 3 **actionable** alpha (`recommended_action_from_alpha_spec`).

| Baseline | Realized outcome | `alignment_class` |
|----------|------------------|-------------------|
| `ENTER` or `REDUCE` | FAVORABLE | `ALIGNED` |
| `ENTER` or `REDUCE` | FLAT | `NEUTRAL` |
| `ENTER` or `REDUCE` | UNFAVORABLE | `ADVERSE` |
| `SKIP` | UNFAVORABLE | `ADVERSE` (proceeded vs skip and lost) |
| `SKIP` | FAVORABLE or FLAT | `NEUTRAL` |
| No actionable baseline | any | `NEUTRAL` |
| Missing entry/exit fill data | any | `NEUTRAL` (`ALIGN_INSUFFICIENT_BROKER_DATA`) |

### Override-related reason codes (non-exhaustive)

- `ENTRY_ALPHA_SKIP_OVERRIDDEN` — `INCREASE_VS_ALPHA` with SKIP baseline
- `ENTRY_ALPHA_REDUCE_OVERRIDDEN` — `INCREASE_VS_ALPHA` with REDUCE baseline
- `ENTRY_COMMITTEE_REDUCED_VS_ENTER`, `ENTRY_COMMITTEE_ACCEPT_ALPHA`, `NO_ALPHA_BASELINE`, `UNKNOWN_OVERRIDE_CLASS`, `ENTRY_ALPHA_BLOCK_DESPITE_ENTER`
- Exit tagging: `EXIT_STOP_LOSS`, `EXIT_TAKE_PROFIT`, `EXIT_REVALIDATION`, `EXIT_MANUAL`, `EXIT_EARLY`, `EXIT_OTHER`

## EXIT_TYPE mapping

`LIVE_ACTIONS.EXIT_TYPE` (and similar hints) map to: `SL`, `TP`, `EARLY`, `MANUAL`, **`REVALIDATION`** (substring `REVAL` without collapsing to `EARLY`), `OTHER`.

## Partial-fill / multi-action policy (v1)

- **One row** per `ENTRY_ACTION_ID` (unique constraint). **First** successful closeout insert wins; later exit fills are ignored for that entry.
- **Resolver:** latest **EXECUTED** **ENTRY** for same `PORTFOLIO_ID` + `SYMBOL` with `UPDATED_AT` ≤ exit action’s `UPDATED_AT` (existing behavior).
- **Partial entry:** economics use **filled** qty/avg only.
- **Staged exit / multiple exit actions:** whichever exit first produces a **FILLED** order and passes the resolver **wins**; no history table in Phase 4.
- **Deferred:** multi-closeout audit, full cost basis / multi-leg, contract multipliers, broker ledger reconciliation.

## API

- `GET /live/entry-intel/summary/by-action/{action_id}` includes **`closeout_summary`** when a closeout exists (same connection round-trip as EIS summary).
- `GET /live/entry-intel/closeout/by-entry-action/{id}` returns **`closeout`** (raw row) and **`closeout_intel`** (parsed VARIANTs + nested summary).

No new Snowflake polling pattern for LIC; page-open fetches only.

## Reconstruction

Query [`MIP.LIVE.V_ENTRY_INTEL_LIFECYCLE_RECONSTRUCTION`](../../SQL/views/live/v_entry_intel_lifecycle_reconstruction.sql) or the equivalent join documented here: `TRADE_CLOSEOUT` → entry/exit `LIVE_ACTIONS` → `ENTRY_INTEL_SNAPSHOT` → `COMMITTEE_VERDICT`.

## Intentionally deferred

- Rich scenario-path taxonomy, LLM scoring, SQL Parallel Worlds redesign
- Full PnL currency / fee attribution
- Multiple closeout rows per entry
- Continuous Snowflake polling

---

_See also: `ADR-0007-phase3-committee-alpha.md`, `phase2_entry_intel_validation.md` (Phase 4 section)._
