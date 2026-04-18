# Structural EXIT — legacy exit “committee” removal (spec + implementation)

**Decision:** Committee 2.0 is **entry-only**. Structural **EXIT** has **no** committee layer and **no** entry-style gates (freshness/trust/regime). Exits use **`build_structural_exit_execution_only_verdict`** — block **only** on broker-truth position quantity (flat → `EXIT_POSITION_MISSING`). Neutral labels: reason code **`STRUCTURAL_EXIT_EXECUTION_ONLY`**, verdict field / SSE **`committee_model`:** **`STRUCTURAL_EXIT_EXECUTION_ONLY`**, `COMMITTEE_RUN` **`committee_model`** for exit runs: same string.

**Non-goals:** No IB/redesign; no exit hearing type.

---

## 1) Removed (historical)

| Area | Was |
|------|-----|
| `structural_committee.py` | `_run_structural_exit_committee`, `run_structural_committee` |
| `live.py` | Structural EXIT called `run_structural_committee` on run/apply/SSE |
| SSE | `STRUCTURAL_EXIT_V1`, multi-role “exit committee” stream |
| Reason codes | `STRUCTURAL_EXIT_COMMITTEE_REVIEWED`, stance-driven exit blocks |

---

## 2) Current behavior

1. **Helper:** `build_structural_exit_execution_only_verdict(action, exit_position_qty=...)`.
2. **Materialization:** Same `COMMITTEE_RUN` / `COMMITTEE_VERDICT` / `LIVE_ACTIONS` flow where needed; structural EXIT does **not** insert synthetic `COMMITTEE_ROLE_OUTPUT` rows from entry-style evaluations (no role loop for EXIT on `committee/run`).
3. **SSE:** One short `agent_turn` / `role_summary` from role **`Execution`**, then `final` with `committee_model`: **`STRUCTURAL_EXIT_EXECUTION_ONLY`**.
4. **ENTRY:** Unchanged — Committee 2.0 + `COMMITTEE_FINAL_DECISION` only.

---

## 3) Optional follow-ups

- **`live_intent_policy.py`:** Docstring clarifying structural exit “sync” is workflow-only, not an authority.
- **SymbolTracker** “committee” naming — out of scope for live trading.

---

## 4) Validation / smoke

- Structural EXIT: non-blocked when position qty ≠ 0; blocked when flat (`EXIT_POSITION_MISSING` + `STRUCTURAL_EXIT_EXECUTION_ONLY`).
- Structural ENTRY: unchanged 409 / happy paths.

---

## 5) Rollout

Deploy **`mip_ui_api`**. No Snowflake DDL.
