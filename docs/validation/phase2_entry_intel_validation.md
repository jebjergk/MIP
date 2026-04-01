# Phase 2 Entry Intelligence — validation note

This note records Phase 1 retro completion, deployment dependencies, and smoke results for real `WORLDS_SPEC` / `ALPHA_SPEC`.

## 1. Immutability (append-only role)

**Script:** [`MIP/SQL/deploy/411_entry_intel_append_only_role.sql`](../../SQL/deploy/411_entry_intel_append_only_role.sql)

**Dependency:** `CREATE ROLE` requires an account-level privileged role (typically `ACCOUNTADMIN`). `CURSOR_AGENT` / `MIP_ADMIN_ROLE` may not have this privilege.

**Steps (when privileged):**

1. Run the full `411` script as `ACCOUNTADMIN` (or equivalent).
2. As `MIP_EIS_APPEND_ONLY`, run:

```sql
USE ROLE MIP_EIS_APPEND_ONLY;
USE DATABASE MIP;
USE SCHEMA LIVE;
UPDATE MIP.LIVE.ENTRY_INTEL_SNAPSHOT SET EIS_NOTE = 'immutability_test' WHERE 1=0;
```

**Expected:** Authorization or compilation error indicating `UPDATE` is not allowed for this role.

**Evidence (2026-04-01 session):**

- Deploy as `MIP_ADMIN_ROLE`: `CREATE ROLE MIP_EIS_APPEND_ONLY` failed with **Insufficient privileges to operate on account** (`003001`). Grants skipped because role missing.
- **Next step:** run [`411`](../../SQL/deploy/411_entry_intel_append_only_role.sql) as `ACCOUNTADMIN`, then run Test 1 UPDATE and paste error snippet here.

_If not run:_ immutability remains **design + grants script only** until `411` is applied.

## 2. End-to-end lifecycle chain (one real example)

Capture one row chain after a real or smoke import + exit:

| Step | ID | Notes |
|------|-----|------|
| PROPOSAL_ID | | |
| SNAPSHOT_ID | | From `MIP.LIVE.ENTRY_INTEL_SNAPSHOT` |
| ENTRY_ACTION_ID | | From `MIP.LIVE.ENTRY_INTEL_ACTION_LINK` |
| CLOSEOUT | | From `MIP.LIVE.TRADE_CLOSEOUT` (if exit `FILLED`) |

**API checks (fill after run):**

- `GET /live/entry-intel/snapshot/by-proposal/{proposal_id}`
- `GET /live/entry-intel/snapshot/by-action/{entry_action_id}`
- `GET /live/entry-intel/closeout/by-entry-action/{entry_action_id}`

**Chain OK?** (Y/N + gaps):

- _2026-04-01:_ No `ENTRY_INTEL_ACTION_LINK` rows in the connected account; full chain **not** captured. After import + exit `FILLED`, use `GET /live/entry-intel/summary/by-action/{entry_action_id}` to verify.

## 3. EIS ensure failure visibility (Phase 2)

- **`188` hook:** On failure, a row is inserted into `MIP.APP.EIS_ENSURE_FAILURE_LOG` (run + portfolio + error message).
- **Python:** `ensure_entry_intel_for_proposal` logs warnings on failure (see `entry_intel_hooks.py`).

**Phase 3+:** Consider **fail-closed** proposes once EIS is mandatory for execution gating — document when product approves.

## 4. Partial-fill and closeout (v1 policy)

Documented in code: [`entry_intel_hooks.py`](../../apps/mip_ui_api/app/entry_intel_hooks.py) (`maybe_write_trade_closeout_on_exit_filled`).

**v1 rules:**

- **Closeout row** is created only when an **EXIT** live action’s order reaches **`FILLED`** via `update_live_order_status` (full fill: `QTY_FILLED` set to `QTY_ORDERED` for that order).
- **Entry** `PARTIAL_FILL` / `EXECUTION_PARTIAL` does **not** create a closeout (closeout is exit-scoped).
- **Exit** partial fills do **not** create a closeout until the exit order is marked **`FILLED`** (full close of that exit order in v1).
- **At most one** `TRADE_CLOSEOUT` per `ENTRY_ACTION_ID` (enforced by unique constraint).

## 5. Smoke execution log (Phase 2)

Run after deploying SQL:

| Smoke file | Result | Notes |
|------------|--------|-------|
| `MIP/SQL/smoke/18_entry_intel_lifecycle_smoke.sql` | Not re-run end-to-end this session | Immutability Test 1 still manual / role-dependent |
| `MIP/SQL/smoke/19_entry_intel_phase2_smoke.sql` | **Pass** | `DETERMINISM_F_BUILD` true; `WORLDS_SPEC_V1` / `ALPHA_SPEC_V1` keys present; `PROB_SUM` 1.0 for sample proposal; re-run after V2 insert to expect `EIS_V2_ROWS` ≥ 1 |

**Determinism check:** `19` compares two calls to `F_BUILD_ENTRY_INTEL_FOR_PROPOSAL` via `TO_JSON` equality (excluding volatile fields if any — see smoke comments).

## 6. Post–Phase 2 deploy validation (real data, 2026-04-01)

### 6.1 Critical fix: `SP_ENSURE_*` + SQL UDF

Snowflake returned **`Unsupported subquery type cannot be evaluated inside Function object: F_BUILD_ENTRY_INTEL_FOR_PROPOSAL`** when `F_BUILD` was referenced inside an `INSERT … SELECT` that also contained `NOT EXISTS` (and similarly for a derived table around the UDF).

**Fix (deployed in `410_entry_intel_lifecycle.sql`):**

- `SP_ENSURE_ENTRY_INTEL_FOR_PROPOSAL`: `SELECT F_BUILD(…) INTO :v_payload`, then `INSERT` using `get_path(:v_payload, 'WORLDS_SPEC'|'ALPHA_SPEC')` (no UDF call inside the mutating `SELECT`).
- `SP_ENSURE_ENTRY_INTEL_FOR_RUN`: cursor over proposals for the run; same `INTO :v_payload` + `INSERT` per missing `PROPOSAL_ID`.

Until this fix is deployed, **new EIS rows from procs may fail** while standalone `F_BUILD` still works.

### 6.2 EIS_SCHEMA_V2 row (evidence)

| Field | Value |
|--------|--------|
| **PROPOSAL_ID** | `6301` |
| **SNAPSHOT_ID** | `2ee30be4-7489-4e5a-a5ca-6ee886428603` |
| **SOURCE_VERSION** | `EIS_SCHEMA_V2` |
| **SYMBOL** | `AUD/USD` |
| **Created** | Via `CALL MIP.APP.SP_ENSURE_ENTRY_INTEL_FOR_PROPOSAL(6301)` after fix (proposal had **no** prior EIS row). |

**WORLDS_SPEC summary:** `schema_version` = `WORLDS_SPEC_V1`; `historical_distribution` with `sample_size` = 66, horizon `1`, probabilities summing to 1, FX `interval_minutes` 1440, `pattern_id` 901; `supporting.insufficient_sample` = false.

**ALPHA_SPEC summary:** `ALPHA_SPEC_V1`; `expected_value_gross` ≈ 0.00053; `estimated_cost_floor` 0.002; `expected_value_net` ≈ -0.00147 → **`recommended_action` = SKIP**, **`recommended_size_band` = XS**; `confidence_band` HIGH, `downside_risk_band` MEDIUM; `alpha_summary_text` matches deterministic template.

**Business sanity:** With a small positive gross edge but a fixed 20 bps round-trip cost floor, **net EV negative → SKIP** is consistent with the v1 rules.

### 6.3 Committee context (`entry_intel_baseline`)

**Not exercised via HTTP in this pass** (no local API run). Code path: [`_build_action_decision_context`](../../apps/mip_ui_api/app/routers/live.py) loads `entry_intel_snapshot_id` from `PROPOSAL_ID`, then `_fetch_entry_intel_baseline(cur, snapshot_id)` returns `{ snapshot_id, worlds_spec, alpha_spec }` on the committee context. For a live `LIVE_ACTIONS` row with `PROPOSAL_ID = 6301` and latest snapshot `2ee30be4-…`, the baseline should mirror the JSON above.

### 6.4 Full chain PROPOSAL → SNAPSHOT → ENTRY_ACTION → CLOSEOUT

**Not available in this account:** `ENTRY_INTEL_ACTION_LINK` had **0** rows before and after; no `TRADE_CLOSEOUT` linked. **Next:** import proposal `6301` (or any new proposal after a pipeline run), then call `GET /live/entry-intel/summary/by-action/{ENTRY_ACTION_ID}`.

### 6.5 `GET /live/entry-intel/summary/by-action/{action_id}`

**Not called** — requires a real `ENTRY_ACTION_ID` present in `ENTRY_INTEL_ACTION_LINK`. Expected shape when linked: `ok`, `action_id`, `summary.snapshot_id`, `summary.proposal_id`, `worlds_summary`, `alpha_summary` (see `live.py`).

### 6.6 Open blockers (unchanged + new)

| Blocker | Status |
|---------|--------|
| **411** append-only role | Still requires ACCOUNTADMIN; immutability smoke not executed |
| **E2E chain** | Still needs real import + exit `FILLED` + link row |
| **SP_ENSURE UDF+INSERT** | **Resolved** in repo + redeployed to Snowflake for this validation |

### 6.7 Phase 3 readiness

**Proceed with Phase 3 planning**, with operational caveats: ensure **410 procedural fix** is deployed everywhere Phase 2 ran; complete **411** + one **E2E** chain when an import exists; optionally re-smoke `19` for `EIS_V2_ROWS`.

---

_Last updated: post–Phase 2 data validation + SP_ENSURE fix._
