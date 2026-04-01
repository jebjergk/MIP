# ADR-0007 Phase 2 addendum: WORLDS_SPEC and ALPHA_SPEC (deterministic v1)

## Status

Accepted — Phase 2 (implemented in `MIP.APP.F_BUILD_ENTRY_INTEL_FOR_PROPOSAL`, consumed by `SP_ENSURE_ENTRY_INTEL_*`).

## Scope

This addendum defines **how** pre-trade `WORLDS_SPEC` and `ALPHA_SPEC` are built. It does **not** change Policy Parallel Worlds (PPW) or LIC runtime scenario mix (RSM).

## WORLDS_SPEC v1 (`schema_version: WORLDS_SPEC_V1`)

### Data source (HOD)

- Join `MIP.APP.RECOMMENDATION_LOG` `r` to `MIP.APP.RECOMMENDATION_OUTCOMES` `o`.
- Match to the proposal row in `MIP.AGENT_OUT.ORDER_PROPOSALS`:
  - `r.SYMBOL = op.SYMBOL`
  - `r.MARKET_TYPE = op.MARKET_TYPE`
  - `r.INTERVAL_MINUTES = coalesce(op.INTERVAL_MINUTES, op.SIGNAL_INTERVAL_MINUTES, 1440)`
  - If `op.SIGNAL_PATTERN_ID` is null: **no** pattern filter (symbol + interval only). If non-null: `r.PATTERN_ID = op.SIGNAL_PATTERN_ID`.
- Keep rows with `o.EVAL_STATUS = 'SUCCESS'` and `o.REALIZED_RETURN` not null.

### Horizon selection

- Group by `HORIZON_BARS`, count rows.
- Choose the horizon with **maximum** count; tie-break: **larger** `HORIZON_BARS`.

### Return buckets (fixed thresholds)

- **Upside:** `REALIZED_RETURN > 0.005` (+0.5%).
- **Base:** `-0.005 <= REALIZED_RETURN <= 0.005`.
- **Downside:** `REALIZED_RETURN < -0.005`.

### Aggregates

- Per bucket: count → `upside_probability`, `base_probability`, `downside_probability` (= count / total `n` for that horizon slice).
- Per bucket: `avg(REALIZED_RETURN)` → `upside_avg_return`, `base_avg_return`, `downside_avg_return`.
- `sample_size` = `n` (total outcomes in chosen horizon).

### Insufficient sample

- If `n < 12`: `supporting.insufficient_sample = true`. Probabilities may be null; alpha rules treat this as **SKIP**-biased (see below).

### Optional / supporting

- `supporting.hod_source` documents the join path.
- No dependency on same-run PPW completion.

## ALPHA_SPEC v1 (`alpha_schema_version: ALPHA_SPEC_V1`)

### Cost floor

- `estimated_cost_floor = 0.002` (0.2% round-trip drag placeholder for v1 — not broker-specific).

### Expected value

- If `n >= 12`:
  - `expected_value_gross` = Σ (bucket_probability × bucket_avg_return) using **0** for null bucket averages.
  - `expected_value_net` = `expected_value_gross - estimated_cost_floor`.
- If `n < 12`: both EV fields are **null**; action is **SKIP**.

### confidence_band (from HOD depth only)

- `HIGH` if `n >= 60`
- `MEDIUM` if `n >= 25`
- else `LOW`

### downside_risk_band (from downside bucket)

- `HIGH` if `p_down >= 0.35` or `avg_down < -0.015` (when `n >= 12`)
- else `MEDIUM` if `p_down >= 0.22` or `avg_down < -0.008`
- else `LOW`
- If `n < 12`: **HIGH**

### recommended_action (deterministic priority)

1. If `n < 12` → **SKIP**
2. Else if `expected_value_net < 0` → **SKIP**
3. Else if `expected_value_net < 0.005` OR `confidence_band = LOW` OR `downside_risk_band = HIGH` → **REDUCE**
4. Else → **ENTER**

### recommended_size_band (coarse buckets)

- **SKIP** → `XS`
- **REDUCE** → `XS` if `confidence_band = LOW` else `S`
- **ENTER** → `M` if `confidence_band = HIGH` and `downside_risk_band = LOW`; else `S`

### Narrative

- `alpha_summary_text`: short deterministic string (truncated) with `n`, horizon, EV_net, action, size band.
- `alpha_reason_codes`: includes `INSUFFICIENT_HOD_SAMPLE` or `HOD_SAMPLE_OK`, plus fixed tags `EV_RULE_V1`, `SIZE_MAP_V1`.

## Committee vs alpha (Phase 2)

- Stored in `COMMITTEE_VERDICT.VERDICT_JSON`: `alpha_baseline_action`, `committee_recommendation_summary`, `alpha_committee_alignment` (`ALIGNED` | `DIVERGENT` | `UNKNOWN`), `committee_vs_alpha_notes`.
- **Deferred to Phase 3:** automated execution gating from alignment, rich override taxonomy, LLM-derived alpha, continuous Snowflake reads for LIC.

## LIC page-open contract (no polling)

- **One** HTTP GET per page open, e.g. `GET /live/entry-intel/summary/by-action/{entry_action_id}` — returns `snapshot_id`, compact `worlds_summary`, `alpha_summary`.
- **Bootstrap fields:** frozen `WORLDS_SPEC` / `ALPHA_SPEC` from EIS (above summaries).
- **Not** loaded on a timer from Snowflake: intraday **RSM** and live prices remain client/session or separate explicit refresh — not background EIS polling.

## Legacy snapshots

- Rows with `SOURCE_VERSION = EIS_SCHEMA_V1` remain **immutable** stubs. New inserts use `EIS_SCHEMA_V2` with real specs. Optional **backfill** (new `EIS_VERSION` row) is out of scope for Phase 2 unless explicitly requested.
