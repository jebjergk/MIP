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
| `MIP/SQL/smoke/19_entry_intel_phase2_smoke.sql` | **Pass** | `DETERMINISM_F_BUILD` true; `WORLDS_SPEC_V1` / `ALPHA_SPEC_V1` keys present; `PROB_SUM` 1.0 for sample proposal; `EIS_V2_ROWS` 0 (no new inserts after deploy in this env) |

**Determinism check:** `19` compares two calls to `F_BUILD_ENTRY_INTEL_FOR_PROPOSAL` via `TO_JSON` equality (excluding volatile fields if any — see smoke comments).

---

_Last updated: Phase 2 implementation (fill evidence rows after deployment)._
