# LPA-first Committee 2.0 — implementation spec (tight scope)

**UX baseline (approved):** Live Portfolio Activity is the operational surface; Committee 2.0 is **inline** with **one primary action** that performs hearing lifecycle + LIVE materialization; **no** manual `action_id`, **no** multi-proposal picker on the default path; standalone hearing page is **optional deep review** only.

---

## 1) Scope

**In scope**

- **One canonical structural ENTRY row per symbol** in LPA pending list (server-defined selection; see §3).
- **One inline summary panel** per structural ENTRY pending row (stance, confidence, short status, last orchestration time / errors).
- **One primary orchestration control** (“Apply Committee 2.0” / final label TBD) calling **one backend orchestration** that:
  - Binds **`action_id`** and **`PROPOSAL_ID`** from the loaded `LIVE_ACTIONS` row only (client never sends alternate IDs in the default path).
  - Ensures hearing exists for that proposal (open/reuse semantics aligned with [`committee_hearing_open`](../../apps/mip_ui_api/app/routers/committee.py)).
  - Runs **refresh** (fresh evidence + persisted hearing state).
  - Runs **commit** with `action_id` set (writes `COMMITTEE_FINAL_DECISION`).
  - Runs existing **LIVE materialization** (same outcome as today’s structural `committee/run` or `committee/apply` after final decision exists — `COMMITTEE_RUN` / `COMMITTEE_VERDICT` / `LIVE_ACTIONS` updates).
- **Optional “Open full hearing”** link: navigates to `/structural-committee/:hearingId` with **query** `action_id` + `proposal_id` **for deep review only** (pre-fill / context); not required for trading.

**Out of scope (this iteration)**

- Timeline **`StlHearingLaunch`** redesign beyond copy/de-emphasis (no new operational paths there).
- Changing **EXIT** flows (execution-only path unchanged).
- Replacing **SSE** streaming for power users (may remain debug/advanced; default path is non-SSE orchestration).
- **Snowflake DDL** (reuse existing `COMMITTEE_*` APP + `MIP.LIVE` tables).

---

## 2) Backend — orchestration endpoint

**Proposed route (live router):**

- `POST /live/trades/actions/{action_id}/committee2/orchestrate`
- **Auth / policy:** Same guards as today’s committee apply/run: `assert_live_committee_policy`, structural-only, **ENTRY only** (409 if EXIT).

**Request body (minimal):**

- Optional: `{ "force_rebuild_hearing": false }` only — **no** `proposal_id` or `action_id` in body for default path (IDs taken from DB row).

**Internal sequence (single transaction or ordered steps with clear rollback semantics):**

1. Load `LIVE_ACTIONS` by `action_id`; verify **structural ENTRY** (`is_structural_live_action`, intent ENTRY).
2. Read **`PROPOSAL_ID`** from row; 409 if null / invalid for structural operation.
3. **Open or reuse hearing** for `proposal_id` (mirror `committee_hearing_open` without `force_rebuild` unless flag set): obtain `hearing_id`.
4. **`committee_hearing_refresh(hearing_id)`** logic in-process (same DB effects as today’s POST refresh).
5. **`committee_hearing_commit`** logic in-process with **`action_id`** = path param and **`proposal_id`** from row (reject if `COMMITTEE_FINAL_DECISION` already exists for another `action_id` / mismatch — define idempotent behavior: if already committed **for this action_id**, skip commit and proceed to materialization only).
6. **Materialize LIVE committee artifacts** — call shared helper extracted from current structural branch of `committee/apply` or `committee/run` (final decision → bridge → persist run/verdict/update action). **Do not** require a second user-facing “Sync” step.

**Response JSON (for inline panel):**

- `ok`, `action_id`, `proposal_id`, `hearing_id`
- Summary: `stance`, `confidence`, `blocked` (derived from materialized verdict), `recommendation`
- `reason_codes` (subset for UI)
- `committee_run_id`, `action_status` (post-update)
- `already_committed` / `idempotent_replay` flags when applicable

**Errors:** Reuse existing codes where possible (`COMMITTEE2_FINAL_DECISION_REQUIRED` should **not** appear after successful orchestrate; prefer `NO_SNAPSHOT`, `COMMITTEE2_DISABLED`, proposal mismatch, opening gate, etc.).

**Implementation note:** Prefer **internal Python functions** (refactor from [`committee.py`](../../apps/mip_ui_api/app/routers/committee.py) + [`live.py`](../../apps/mip_ui_api/app/routers/live.py)) over chained HTTP to self — one DB connection / transaction where Snowflake allows.

---

## 3) Canonical pending row — one structural proposal per symbol

**Today:** [`get_live_activity_overview`](../../apps/mip_ui_api/app/routers/live.py) collapses with `pending_by_symbol` (submission_allowed preference + timestamp).

**Tighten for structural ENTRY:**

- When multiple pending rows share a symbol and **`live_intent_kind` is STRUCTURAL** and intent is **ENTRY**, keep the **single operational** row by:
  1. Prefer row whose **`PROPOSAL_ID`** equals the **maximum `PROPOSAL_ID`** for that symbol among candidates (latest proposal id wins), **or**
  2. If product rule prefers “latest `CREATED_AT`” for LIVE_ACTIONS only, document one rule and apply consistently.

- **Other** structural ENTRY rows for that symbol: omit from primary `pending_decisions` **or** attach as `superseded_actions: [...]` on the canonical row (read-only, no second Committee 2.0 button). **Do not** show parallel “pick proposal” UI.

(Exact tie-break: pick one rule in implementation PR; `max(PROPOSAL_ID)` is the default recommendation for “latest actionable proposal.”)

---

## 4) Frontend — LPA ([`LivePortfolioActivity.jsx`](../../apps/mip_ui_web/src/pages/LivePortfolioActivity.jsx))

**Per structural ENTRY row:**

- **Inline panel** (collapsible optional): show stance, confidence, last orchestrate result, compact reason chips.
- **Primary button:** calls `POST .../committee2/orchestrate` for `d.action_id` only — **replace** separate “Sync Committee 2.0” + SSE finalize flow for ENTRY (remove or hide legacy path for structural ENTRY once orchestrate is stable).
- **Secondary link:** “Open full hearing” → `/structural-committee/${hearingId}?action_id=...&proposal_id=...` — **only** after orchestrate returns `hearing_id`, or construct from overview if backend adds `hearing_id` to pending payload when known.

**Remove from default path:**

- Manual **action_id** input on standalone hearing page **for operators coming from LPA** (hearing page may still accept query params when opened from LPA).

---

## 5) Standalone hearing page ([`StructuralCommitteeHearing.jsx`](../../apps/mip_ui_web/src/pages/StructuralCommitteeHearing.jsx))

- Remains for **deep review**: refresh, read-only compare, optional manual re-commit only if orchestration failed mid-flight (edge case).
- When opened with `?action_id=&proposal_id=`, **pre-fill** context; **Commit** on this page should remain **idempotent** with server-side binding (no user paste required if params present).

---

## 6) Validation / smoke

- **API:** Orchestrate on structural ENTRY with valid snapshot → `COMMITTEE_FINAL_DECISION.ACTION_ID` set; `COMMITTEE_RUN` completed; LPA overview shows updated committee fields.
- **API:** Second call idempotent or safe replay (document behavior).
- **API:** EXIT action → 409 from orchestrate.
- **UI:** Single button completes without navigation; full hearing opens only via optional link.

---

## 7) Deliverables checklist

| Item | Owner |
|------|--------|
| `committee2/orchestrate` + shared helpers | `mip_ui_api` |
| Pending canonical row rule | `live.py` overview |
| LPA inline panel + primary button | `mip_ui_web` |
| Deprecate structural ENTRY SSE/sync as default | `mip_ui_web` + docs |
| Unit/integration tests | `mip_ui_api/tests` |

---

## Related docs

- [`cutover_spec.md`](./cutover_spec.md) — C2.0 → LIVE verdict mapping (unchanged).
- [`structural_exit_committee_removal_spec.md`](./structural_exit_committee_removal_spec.md) — EXIT execution-only.
