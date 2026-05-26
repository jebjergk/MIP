# MIP Stage 4f — No-C2-Fallback Audit Report

**Status:** Implemented, tested, and deployed in code. Smoke-verified end-to-end.
**Flag involved:** `AGENTIC_PRIMARY_MATERIALIZATION_ENABLED` (in `MIP.APP.APP_CONFIG`).
**Active flag value at time of audit:** `'true'`.
**Scope:** structural ENTRY actions on the LPA / operator authority path.
**Out of scope:** IBKR / order placement, RAG, short-proposal validation, structural EXIT,
safety revalidation, price-bar-news guard, audit / history tables.

---

## 1. Final verdict

> **No remaining fallback to deterministic C2 materialization exists from the
> operator authority path when `AGENTIC_PRIMARY_MATERIALIZATION_ENABLED='true'`.**

* `_materialize_structural_entry_committee_apply` refuses to run (HTTP 409 with
  reason code `DETERMINISTIC_MATERIALIZATION_DISABLED_AGENTIC_PRIMARY`) when the
  flag is on or the flag-row cannot be read. This guard is **at the function
  itself**, so every present and future caller is gated.
* `POST /live/trades/actions/{id}/committee/apply` and
  `POST /live/trades/actions/{id}/committee/run` reject structural ENTRY calls
  with HTTP 409 + Stage-4f reason codes before any C2 materialization SQL runs.
* `POST /live/trades/actions/{id}/committee2/orchestrate` continues to refresh
  `COMMITTEE_HEARING` and write `COMMITTEE_FINAL_DECISION` as diagnostic /
  baseline only — it does not touch `LIVE_ACTIONS.STATUS` (Stage 4e).
* `evaluate_authority_gate` fails CLOSED on DB error when the agentic-primary
  flag is on or unreadable, returning the new `AGENTIC_AUTHORITY_EVAL_ERROR`
  reason code. No silent fallthrough is possible.
* The LPA `submission_allowed` derivation drops `committee_blocks_entry`
  contribution for structural ENTRY rows when agentic-primary is on, so the
  deterministic `joint_decision.should_enter` cannot participate in the
  Submit decision.
* The agentic-primary materializer overrides `joint_decision.should_enter` to
  match its own outcome, so the `COMMITTEE_VERDICT.VERDICT_JSON:joint_decision`
  view column always reflects the active authority.

The legacy rollback path (`AGENTIC_PRIMARY_MATERIALIZATION_ENABLED='false'`)
remains available as a single point of control. Setting the flag to `'false'`
deterministically restores the pre-Stage-4e behavior; the guards are inert in
that mode (verified by smoke `[6]`).

---

## 2. Deterministic C2 materialization call-site inventory

### 2.1 Function: `_materialize_structural_entry_committee_apply`
`MIP/apps/mip_ui_api/app/routers/live.py` ~ line 10123.

| Caller | File / Line | Classification | Stage-4f treatment |
|---|---|---|---|
| `orchestrate_committee2_structural_entry` (structural ENTRY branch) | `live.py` ~ 11250 | Forbidden materialization | Already skipped by Stage-4e flag check inside orchestrate **and** now hard-guarded by the new in-function check. |
| `apply_live_trade_committee` (structural ENTRY branch) | `live.py` ~ 11460 | Forbidden materialization | New route-level guard returns 409 + `DETERMINISTIC_APPLY_DISABLED_AGENTIC_PRIMARY` before the call site. Function-level guard backs it up. |

### 2.2 Function: `committee_final_decision_commit_for_action`
`MIP/apps/mip_ui_api/app/routers/committee.py` ~ line 554.

| Caller | File / Line | Classification | Stage-4f treatment |
|---|---|---|---|
| `orchestrate_committee2_structural_entry` | `live.py` ~ 11231 | **Diagnostic baseline** — writes `COMMITTEE_FINAL_DECISION` only, never `LIVE_ACTIONS` | Allowed. CFD is the canonical hearing-replay / baseline-of-record artifact. |
| `committee_hearing_commit` (internal idempotent recovery path) | `committee.py` ~ 847 | Idempotent retry handler — never materializes `LIVE_ACTIONS` | Allowed. |
| `committee_hearing_commit` (route handler) | `committee.py` ~ 954 | Direct hearing-commit endpoint, writes CFD only | Allowed (diagnostic). |

### 2.3 SQL writes: `MIP.LIVE.COMMITTEE_RUN`, `MIP.LIVE.COMMITTEE_VERDICT`, `MIP.APP.COMMITTEE_FINAL_DECISION`

| File / Line | Function | Classification | Stage-4f treatment |
|---|---|---|---|
| `live.py` 9618 (RUN), 9919 (VERDICT) | `run_live_trade_committee` | **Forbidden materialization** for structural ENTRY (this route also updates `LIVE_ACTIONS.STATUS`) | New guard at the structural-ENTRY branch returns 409 + `DETERMINISTIC_RUN_DISABLED_AGENTIC_PRIMARY` before the RUN/VERDICT inserts. Smoke `[5b]` shows the route refuses to materialize for an INTENT_APPROVED action via the pre-existing status check (defense-in-depth: even without the Stage-4f guard, status gating already blocks). |
| `live.py` 10260 (RUN), 10278 (VERDICT) | Inside `_materialize_structural_entry_committee_apply` | Forbidden materialization | Hard-gated by the new in-function guard. |
| `live.py` 10571 (RUN), 10593 (VERDICT) | `_materialize_structural_entry_agentic_apply` (Stage 4e) | **Permitted** — agentic-authority materializer | `MODEL_NAME='AGENTIC_AUTHORITY_v1'`, the only path that may write LIVE_ACTIONS for structural ENTRY when 4e is on. |
| `live.py` 11750 (RUN), 11766 (VERDICT) | `apply_live_trade_committee` (non-structural / structural-EXIT branch) | Out of scope: structural EXIT and legacy multi-agent (non-structural) | EXIT actions never run shadow board; `assert_live_committee_policy` already enforces structural-only mode for production. No Stage-4f guard needed because no agentic-primary path exists for these. |
| `committee.py` 811 (CFD INSERT) | `committee_final_decision_commit_for_action` | Diagnostic baseline | Allowed. Used by 2.0 hearing replay surface. |

### 2.4 Routes audited

| Route | Verdict |
|---|---|
| `POST /live/trades/actions/{id}/committee2/orchestrate` | **OK** — Stage 4e gates materializer; response carries `agentic_primary_enabled` + `recommendation: AGENTIC_PENDING_COMMIT`. |
| `POST /live/trades/actions/{id}/committee/apply` | **GATED** — Stage 4f route-level guard rejects structural ENTRY with 409 + Stage-4f reason code. |
| `POST /live/trades/actions/{id}/committee/run` | **GATED** — Stage 4f guard rejects structural ENTRY with 409 before any C2 write. |
| `GET /live/trades/actions/{id}/committee/live-prompt` | **Pure SSE stream**, no DB writes. Materialization only happens via the `final` event triggering `/committee/apply`, which is gated. |
| `POST /committee/hearing/{hearing_id}/commit` | **Allowed** — writes CFD diagnostic record only. |
| `POST /live/trades/actions/{id}/agentic-authority/commit` | **Stage 4e primary path** — operator commits agentic verdict; helper `_maybe_run_agentic_materializer` advances `LIVE_ACTIONS.STATUS`. |
| `POST /live/decisions/{id}/approve-and-submit` | Operator-driven status transition, not C2-derived. Out of scope. |
| `POST /live/decisions/{id}/approve-flow` | Operator-driven. Out of scope. |
| `POST /live/decisions/{id}/submit-only` | Operator-driven. Out of scope. |
| `POST /live/trades/actions/{id}/execute` | **Submit endpoint.** Reads `LIVE_ACTIONS.STATUS` (which only the agentic materializer can move) and applies the Stage-4d agentic gate. No deterministic verdict is read. |

---

## 3. Hardening guards added

### 3.1 Function-level (`_materialize_structural_entry_committee_apply`)

`live.py` — top of function:

```python
# Stage 4f circuit breaker. Fail-CLOSED on config read errors.
_block = _read_agentic_primary_flag_via_cursor(cur)
if _block:
    _log.error("STAGE_4F_GUARD_BLOCKED_DETERMINISTIC_MATERIALIZE ...")
    raise HTTPException(status_code=409, detail={
        "message": "Deterministic committee materialization is disabled while ...",
        "reason_codes": [
            "DETERMINISTIC_MATERIALIZATION_DISABLED_AGENTIC_PRIMARY",
            "STAGE_4F_NO_C2_FALLBACK",
        ],
        "action_id": action_id,
        "apply_detail_source": apply_detail_source,
    })
```

The helper `_read_agentic_primary_flag_via_cursor`:
* Returns `True` when the row is missing.
* Returns `True` when the cursor raises.
* Returns `True` only for explicit truthy values; otherwise returns the parsed boolean.

This means **failing to read the flag is treated as "the flag is on"**, which
is the fail-closed direction for materialization.

### 3.2 Route-level guards

`POST /live/trades/actions/{id}/committee/apply` — `live.py` ~ 11460:

```python
if is_structural and not is_exit and _read_agentic_primary_flag_via_cursor(cur):
    _log.error("STAGE_4F_GUARD apply_live_trade_committee blocked ...")
    raise HTTPException(status_code=409, detail={
        "reason_codes": [
            "DETERMINISTIC_APPLY_DISABLED_AGENTIC_PRIMARY",
            "STAGE_4F_NO_C2_FALLBACK",
        ],
        ...
    })
```

`POST /live/trades/actions/{id}/committee/run` — `live.py` ~ 9692:

```python
if _read_agentic_primary_flag_via_cursor(cur):
    _log.error("STAGE_4F_GUARD run_live_trade_committee blocked ...")
    raise HTTPException(status_code=409, detail={
        "reason_codes": [
            "DETERMINISTIC_RUN_DISABLED_AGENTIC_PRIMARY",
            "STAGE_4F_NO_C2_FALLBACK",
        ],
        ...
    })
```

### 3.3 Submit gate fail-closed (`evaluate_authority_gate`)

`MIP/apps/mip_ui_api/app/committee/agentic_authority.py` ~ 1469:

When the gate evaluator hits an exception:

* `should_block_deterministic_materialization(conn)` is consulted.
* If True (flag is on **or unreadable**), gate returns
  `gate_enabled=True, gate_ok=False, reason_code=AGENTIC_AUTHORITY_EVAL_ERROR`.
* If False (flag is explicitly off and reachable), the historical Stage-4d
  fail-open behavior is preserved.

Stage 4f added one new constant: `GATE_REASON_EVAL_ERROR = "AGENTIC_AUTHORITY_EVAL_ERROR"`.

### 3.4 LPA Submit-allowed derivation

`live.py` ~ 7944:

```python
try:
    _agentic_primary_on = is_agentic_primary_materialization_enabled(conn)
except Exception:
    _agentic_primary_on = False
...
# Stage 4f — when agentic-primary is active, the deterministic JD is diagnostic
# only. Do NOT let `should_enter=False` from a stale C2 verdict participate in
# the Submit gate; the agentic gate (below) is the single source of truth.
if _agentic_primary_on and is_structural_row_for_4f and not is_exit:
    committee_blocks_entry = False
```

This eliminates the last channel through which a stale deterministic verdict
could influence Submit eligibility.

### 3.5 Agentic materializer JD override

`live.py` ~ 10501 inside `_materialize_structural_entry_agentic_apply`:

```python
jd = dict(jd_raw) if isinstance(jd_raw, dict) else {}
jd["should_enter"] = (not blocked)
jd["agentic_source"] = True
jd["agentic_authority_status"] = outcome["status_code"]
```

So the `COMMITTEE_JOINT_DECISION` view column ends up reflecting the agentic
outcome, not a stale C2 JD. This protects the LPA UI / Submit derivation
from a misleading `should_enter=True` left over from a C2 PROCEED that was
later overridden by an agentic BLOCK.

### 3.6 UI/API semantics

* `LpaCommittee2Exhibits.jsx` — "REAL BOARD" chip renamed to
  "DETERMINISTIC BASELINE". Subtitle now reads
  "Diagnostic baseline · hearing replay (no longer materializes — agentic primary)".
* `LivePortfolioActivity.jsx` — collapsible panel subtitle reads
  "Deterministic baseline · diagnostic only (no longer materializes — agentic primary)".
* No code path now describes the deterministic committee as "authority",
  "real board", "executes", or "materializes" once 4f is in effect.

---

## 4. What remains diagnostic only

* `MIP.APP.COMMITTEE_FINAL_DECISION` — still written by 2.0 orchestrate / hearing
  commit. Used for hearing replay, position-health context, training, and
  diagnostic comparison vs the agentic verdict. **Never read for Submit
  eligibility, never updates `LIVE_ACTIONS`.**
* `MIP.LIVE.COMMITTEE_RUN` / `MIP.LIVE.COMMITTEE_VERDICT` rows with
  `MODEL_NAME='STRUCTURAL_V1'` — historical legacy. Already historical-only
  under Stage 4e; Stage 4f makes the materialization site that produced them
  unreachable from operator paths.
* The Inline Hearing exhibit panel in LPA (the "REAL BOARD" → now
  "DETERMINISTIC BASELINE" section) — renders for human inspection only.

---

## 5. Authority failure-mode behavior matrix

Verified by smoke `[2]`, `[4a]`, `[4b]`, `[4c]` plus the existing Stage 4d unit
smoke (12 cases) and Stage 4e unit smoke (13 cases).

| Failure mode | Authority row | Gate result with 4e=on | Materialization possible? |
|---|---|---|---|
| No `OPERATOR_COMMITTED` row | None / AUTO_AUDIT only | `NOT_COMMITTED` (block) | No |
| Stale `OPERATOR_COMMITTED` row | `IS_STALE=True` | `STALE` (block) | No |
| `AGENTIC_WAIT_RECLAIM` | OPERATOR_COMMITTED | `BLOCKED_WAIT_RECLAIM` (block) | No |
| `AGENTIC_DEFER` | OPERATOR_COMMITTED | `BLOCKED_DEFER` (block) | No |
| `AGENTIC_REJECT` | OPERATOR_COMMITTED | `BLOCKED_REJECT` (block) | No |
| `AGENTIC_DEGRADED_NO_AUTHORITY` | OPERATOR_COMMITTED | `DEGRADED` (block) | No |
| `AGENTIC_FAILED_NO_AUTHORITY` | OPERATOR_COMMITTED | `FAILED` (block) | No |
| Unsupported `PACK_VERSION` | Reported by `build_authority_row` as `AGENTIC_DEGRADED_NO_AUTHORITY` | `DEGRADED` (block) | No |
| Hearing/action mismatch | `commit_operator_authority_for_session` returns `SKIP_HEARING_MISMATCH`; no new row written. Gate sees prior row state, which already fails closed for stale / non-positive cases | block | No |
| Authority DB lookup error | New behavior: `evaluate_authority_gate` returns `EVAL_ERROR` fail-closed when flag is on or unreadable | block | No |
| Shadow session missing | `commit_operator_authority_for_session` returns `SKIP_SESSION_NOT_FOUND`; no new authority row | gate sees prior row → block | No |
| Shadow session stale | `commit_operator_authority_for_session` writes `AGENTIC_FAILED_NO_AUTHORITY` row with `IS_STALE=true` | `STALE` (block) | No |
| Shadow session degraded | `commit_operator_authority_for_session` writes `AGENTIC_DEGRADED_NO_AUTHORITY` | `DEGRADED` (block) | No |
| Deterministic baseline APPROVE but agentic non-positive | `AUTHORITY_STATUS` reflects agentic only | block per status mapping | No (CFD is diagnostic only; Stage 4f LPA submit derivation drops `committee_blocks_entry` so deterministic PROCEED also cannot help) |

---

## 6. Smoke results

Run on 2026-05-25 against the production Snowflake instance (CURSOR_AGENT /
MIP_ADMIN_ROLE) with `AGENTIC_PRIMARY_MATERIALIZATION_ENABLED='true'`.

### `cursorfiles/smoke_stage4f_no_fallback.py` — Stage 4f-specific (NEW)

```
[OK] 1a flag=false  -> _read_agentic_primary_flag_via_cursor=False
[OK] 1b flag=true   -> _read_agentic_primary_flag_via_cursor=True
[OK] 2  materializer raises 409 + DETERMINISTIC_MATERIALIZATION_DISABLED_AGENTIC_PRIMARY
[OK] 3a should_block(flag=true)  -> True
[OK] 3b should_block(flag=false) -> False
[OK] 4a missing-row + flag=on            -> NOT_COMMITTED (block)
[OK] 4b broken-conn + flag fallback      -> EVAL_ERROR fail-closed
[OK] 4c broken-conn + flag=false (cannot read flag) -> EVAL_ERROR fail-closed
[OK] 5a POST /committee/apply  -> 409 + Stage 4f reason
[OK] 5b POST /committee/run    -> non-2xx (no materialization)
[OK] 6  rollback flag=false    -> Stage 4f guard inactive on next call
=== ALL CHECKS PASSED — Stage 4f hard guards verified ===
```

### Regression — existing smokes

| Smoke | Result |
|---|---|
| `smoke_stage4d_gate_unit.py` (12 cases) | ALL PASS |
| `smoke_stage4d_gate_integration.py` | ALL PASS |
| `smoke_stage4e_outcome_unit.py` (13 cases) | ALL PASS |
| `smoke_stage4e_flag_integration.py` | ALL PASS |
| `smoke_stage4e_http_endpoint.py` | ALL PASS |

No Stage 4d / 4e regressions.

---

## 7. Code that still exists (and why)

| Artifact | Status | Why kept |
|---|---|---|
| `_materialize_structural_entry_committee_apply` body | **Quarantined behind hard guard** | Required for the documented rollback escape hatch. Setting `AGENTIC_PRIMARY_MATERIALIZATION_ENABLED='false'` restores legacy behavior in one switch. |
| `apply_live_trade_committee` structural-ENTRY branch | Guarded — first instruction in branch is the 4f guard | Same rollback rationale. Also still handles EXIT and the assert_live_committee_policy-permitted non-structural path. |
| `run_live_trade_committee` structural-ENTRY branch | Guarded | Same. Also still handles structural EXIT execution-only verdicts. |
| `committee_final_decision_commit_for_action` + `COMMITTEE_FINAL_DECISION` writes | Allowed | Now strictly diagnostic / hearing-replay. Used by position-health context and training. |
| `COMMITTEE_HEARING` refresh | Allowed | Diagnostic baseline. |
| Stream `committee/live-prompt` SSE endpoint | Allowed | Pure read/stream; cannot materialize. The `final` event hands off to `/committee/apply`, which is gated. |
| `evaluate_authority_gate` open-on-error path | Preserved only when `AGENTIC_PRIMARY_MATERIALIZATION_ENABLED='false'` | Maintains the original Stage 4d behavior for legacy mode rollback. When 4e/4f are on, this branch is unreachable. |
| `should_enter` in CFD-derived JD | Overridden by agentic materializer | Prevents stale C2 JD from leaking into Submit derivation. |

---

## 8. Known unrelated finding

While running the new HTTP smoke against `POST /committee/apply`, the
function-level `logger` symbol was found to be undefined in two pre-existing
lines (`live.py:9594`, no longer reachable in the current code path
post-Stage-4f-guard at line 9693, which has been fixed). These were latent
bugs that only triggered if the legacy `force_rerun` code path was hit; they
are not caused by Stage 4f and are not addressed here. Filing as a known
follow-up: replace `logger` with `_log` in the remaining pre-existing call
site at `live.py:9594` (force_rerun warning).

---

## 9. Acceptance-criteria checklist

| Criterion | Status |
|---|---|
| With `AGENTIC_PRIMARY_MATERIALIZATION_ENABLED=true`, deterministic C2 cannot materialize `LIVE_ACTIONS` from the LPA / operator flow | **PASS** (smoke 2, 5a, 5b) |
| No hidden fallback to C2 approval exists | **PASS** (audit sections 2 + 3) |
| Missing/degraded/stale/failed agentic authority fails closed | **PASS** (smoke 4a–c, section 5) |
| `COMMITTEE_FINAL_DECISION` is diagnostic only | **PASS** (section 4) |
| Submit is not enabled by deterministic approval | **PASS** — Submit derivation drops `committee_blocks_entry` contribution under flag=on; `status` advancement now exclusively via agentic materializer |
| Existing safety revalidation remains intact | **PASS** — no changes to `_run_opening_sanity_gate`, `evaluate_safety_revalidation`, IBKR bar / news guards |
| No IBKR / order placement changes | **PASS** |
| Report gives a clear yes/no verdict on remaining fallback risk | **PASS** — see section 1 |

---

## 10. Operational notes

* Current `AGENTIC_PRIMARY_MATERIALIZATION_ENABLED = 'true'` (Stage 4f effective).
* Rollback: `UPDATE MIP.APP.APP_CONFIG SET CONFIG_VALUE='false' WHERE CONFIG_KEY='AGENTIC_PRIMARY_MATERIALIZATION_ENABLED'`.
* After rollback, the Stage 4f guards become inert (verified by smoke `[6]`),
  and the deterministic C2 path resumes its pre-Stage-4e materialization role.
* UI API restart required for Stage 4f code to take effect.
