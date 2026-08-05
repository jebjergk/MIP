# Brooks Intraday Lab — Phase C Bulk Persist Design

**Status:** Approved with amendments (2026-08-04). Implementation delivered; **operational enablement complete** (`BROOKS_PERSIST_MODE=bulk` in runtime).  
**Persist default (code):** `legacy` if `BROOKS_PERSIST_MODE` is unset — must not change.  
**Runtime setting applied at:** repo-root `.env` → loaded by `MIP/apps/mip_ui_api/app/config.py` (`load_dotenv`) into the mip_ui_api / Phase 9 worker process; consumed only via `app.brooks_intraday.persist_mode.get_persist_mode()` for Lab objective/pattern/context/simulation persistence.  
**Verify active mode:** startup log `Brooks Intraday Lab persist mode active=…`, `GET /research/brooks-intraday/persist-mode`, or `brooks_persist_mode` on Phase 9 status / lab `/meta` (process env, not static inference).  
**Rollback:** set `BROOKS_PERSIST_MODE=legacy` in repo-root `.env`, restart uvicorn (`app.main:app`), confirm active=`legacy`. Legacy code path retained.  
**Canonical location:** this file under `MIP/docs/brooks_intraday/` (repo-owned).

**Preserved:** paused week 3 (`2026-06-01`), `BROOKS_CONTEXT_RULESET_V0_2`, Freeze V1, all existing completed/paused attempts.  
**Benchmark target week (stored bars only):** `2026-06-22` / run `894e32f9-6add-4ba9-8564-824889090bee`.  
**Official V0.2 chain (must remain readable & unchanged):**  
- objective `e63808aa-679d-4948-aee7-d5f0ad4c6fd4`  
- pattern `f48709c1-f6c7-4c4e-99e2-acaaef0aa29b`  
- context `78b2a5d3-d7f8-40bb-b19d-9cbf1a6027a0`  
- sim `8736de83-18cc-4c19-8d83-26d0f13fb893`

---

## C0. Goal and non-goals

**Goal:** Replace per-bar MERGE persistence with chunked bulk INSERT so OBJECTIVE / PATTERN / CONTEXT stages drop from ~1560 Snowflake statements each to a small fixed batch count, without changing engine semantics (V0.2 outputs identical).

**Non-goals for Phase C:**
- No V0.3 ruleset, no Freeze V2, no week-3 resume, no bar reacquisition.
- No DELETE of preserved attempt rows (baseline / validation weeks / paused week-3 acquisition).
- No run-wide deletion of simulation trades/blocked signals.
- No change to official production `pattern_sequence_hash` merely for benchmarking.

---

## C1. Partial rows from failed attempts

**Current behavior risk:** Stages already compute then flush with per-row MERGE + periodic `commit`. A crash mid-flush leaves an `IN_PROGRESS` attempt header plus a partial child-row set.

**Phase C policy:**
1. Create attempt header first with `STATUS='IN_PROGRESS'` and `NOTES` tagging purpose (`PHASE_C_BENCHMARK` / `PHASE_C_BULK` / user note).
2. Compute **all** rows in memory (no Snowflake writes during evaluate loop).
3. Persist via chunked bulk INSERT; commit after each successful chunk.
4. On any chunk / integrity failure:
   - Do **not** call `complete_*_attempt`.
   - Mark attempt `FAILED` / `ABANDONED` with NOTES `phase_c_fail:<reason>; rows_written=N; expected=E`.
   - **Failure status commit must be reliable:** use a **fresh connection** if the writer connection is broken or unusable (never rely solely on a failed writer txn).
   - Leave partial child rows in place for diagnosis.
5. Readers select only approved / pinned attempt ids, or `STATUS='COMPLETED'` when auto-picking. Never auto-select `IN_PROGRESS`, `FAILED`, or `ABANDONED`.
6. Cleanup DELETE only for disposable `PHASE_C_BENCHMARK` attempts not in any official chain.

---

## C2. Simulation attempt isolation (additive `SIMULATION_ATTEMPT_ID`)

### Exact DDL

```sql
ALTER TABLE MIP.APP.BROOKS_INTRADAY_SIM_TRADE
  ADD COLUMN IF NOT EXISTS SIMULATION_ATTEMPT_ID VARCHAR(36);

ALTER TABLE MIP.APP.BROOKS_INTRADAY_BLOCKED_SIGNAL
  ADD COLUMN IF NOT EXISTS SIMULATION_ATTEMPT_ID VARCHAR(36);

-- Legacy blocked rows: assign sentinel so PK can include attempt id (NULLs not allowed in PK).
UPDATE MIP.APP.BROOKS_INTRADAY_BLOCKED_SIGNAL
SET SIMULATION_ATTEMPT_ID = 'LEGACY_UNSCOPED'
WHERE SIMULATION_ATTEMPT_ID IS NULL;

ALTER TABLE MIP.APP.BROOKS_INTRADAY_BLOCKED_SIGNAL DROP PRIMARY KEY;
ALTER TABLE MIP.APP.BROOKS_INTRADAY_BLOCKED_SIGNAL
  ADD PRIMARY KEY (RUN_ID, SIMULATION_ATTEMPT_ID, SYMBOL, SIGNAL_TS, CANDIDATE_ACTION);
```

`SIM_TRADE` keeps `TRADE_ID` PK; `SIMULATION_ATTEMPT_ID` is additive/nullable for historical rows (no backfill required for trades).

### New-write behaviour
- Every new `INSERT` into `SIM_TRADE` / `BLOCKED_SIGNAL` **must** set `SIMULATION_ATTEMPT_ID` to the current attempt UUID.
- Do **not** call `clear_run_simulation_artifacts(run_id)` (run-wide DELETE). Remove or gate that helper so Phase C / new sim paths never use it.
- Multiple simulation attempts may coexist for the same `RUN_ID`.

### Approved-attempt filtering
```sql
-- Preferred
WHERE RUN_ID = %s AND SIMULATION_ATTEMPT_ID = %s
```

### Legacy-null / sentinel reader fallback
When loading an **approved** simulation attempt for a run:
1. First load rows with `SIMULATION_ATTEMPT_ID = <approved_id>`.
2. If that returns zero rows (historical data written before the column), fall back to:
   - `SIM_TRADE`: `RUN_ID = %s AND SIMULATION_ATTEMPT_ID IS NULL`
   - `BLOCKED_SIGNAL`: `RUN_ID = %s AND SIMULATION_ATTEMPT_ID IN ('LEGACY_UNSCOPED')`  
   Only when the caller is resolving the run’s approved/official simulation attempt (not for arbitrary new attempts).

Trade-ID namespacing is **not** the primary design.

---

## C3. Transaction and rollback boundaries

```
[create attempt IN_PROGRESS] -- own txn, commit
[compute all rows in memory] -- no Snowflake DML; progress phase=compute
[for each chunk]:
    executemany INSERT
    commit chunk             -- progress phase=persist
[schedule-key + count integrity] -- progress phase=integrity
[if pass: complete attempt COMPLETED + sequence_hash]
[if fail: mark FAILED on fresh connection if needed; stop]
```

Logical rollback = leave FAILED attempt + new attempt_id. Never UPDATE completed historical attempts.

---

## C4. Feature flag (safe default)

`BROOKS_PERSIST_MODE` ∈ {`legacy`, `bulk`}.

- **Default during implementation and verification: `legacy`.**
- Bulk mode enabled **explicitly only after** benchmark + semantic-equivalence acceptance (env/config override).
- Legacy path retains per-row MERGE helpers unchanged.

| Table | Bulk mechanism | Chunk size |
|---|---|---|
| `BAR_OBSERVATION` | multi-row `INSERT … SELECT … FROM VALUES` (PARSE_JSON in SELECT) | 500 |
| `PATTERN_INSTANCE` | same; final instance state | 500 |
| `BAR_PATTERN_LINK` | same | 500 |
| `CONTEXT_OBSERVATION` | same | 500 |
| `SIM_TRADE` | batched `INSERT` with attempt id | 100 |
| `BLOCKED_SIGNAL` | batched `INSERT` with attempt id | 200 |

---

## C5. Expected SQL statement counts

For 1560 schedule bars: bulk path issues **no per-bar MERGE**; objective/context persist ≤ **10** DML each; pattern ≤ **15** DML (instances + links + complete).

---

## C6. Integrity checks (before COMPLETED)

### Exact schedule-key integrity (objective, context, bar-links)
Build expected key set from the replay schedule:  
`expected = {(symbol, normalize_ts(bar_timestamp_utc)) for each schedule step × symbol}`.

Load persisted keys for the attempt. Pass only if:

- `persisted == expected` (set equality both directions)
- equivalently: `persisted - expected` empty **and** `expected - persisted` empty

Row-count equality alone is **not** sufficient.

### Pattern instances
- `COUNT(*)` **equals** the in-memory computed instance count from the just-finished evaluate pass (may be **0** — zero patterns is a valid analytical result).
- Do **not** require `>= 1`.
- Orphan check: every `active_pattern_ids` entry in bar-links ⊆ persisted instance ids for the same attempt (vacuously true if both empty).

### Simulation
- `TRADE_COUNT` / `BLOCKED_SIGNAL_COUNT` match COUNT filtered by `SIMULATION_ATTEMPT_ID`.

On any failed check → mark attempt `FAILED` (fresh connection if needed); do not COMPLETED.

---

## C7. Deterministic ordering

Compute/persist order unchanged: schedule step ascending; symbols `AAPL, AMZN, JPM, MCD`.

**Production hashes (unchanged):**
- Observations: `ORDER BY BAR_TS, SYMBOL`
- Context: `ORDER BY BAR_TS, SYMBOL`
- Patterns (`pattern_sequence_hash`): content fields exclude `PATTERN_INSTANCE_ID`, but `ORDER BY START_TS, SYMBOL, PATTERN_INSTANCE_ID` — **tie-breaking by UUID means new attempts can disagree with official production hashes even when semantics match.** Do not alter this production function for benchmarks.

---

## C8. Progress phases

Expose separate progress for:
1. **compute** — bars/steps evaluated in memory
2. **persist** — bulk INSERT chunks written
3. **integrity** — schedule-key / count validation

`stage_progress` payload includes `phase` ∈ {`compute`,`persist`,`integrity`} plus completed/total/unit.

---

## C9. Semantic equivalence verification

### Pattern hash fact
Official `pattern_sequence_hash` does **not** include `PATTERN_INSTANCE_ID` in selected content columns, but **does** order by `PATTERN_INSTANCE_ID`. New attempts mint new UUIDs → production hashes are **not** a reliable cross-attempt semantic comparator when ties exist.

### Benchmark comparator (new; production hash untouched)
`pattern_semantic_sequence_hash(run_id, replay_attempt_id)`:
- Same content fields as production: `TRADING_DATE, START_TS, SYMBOL, PATTERN_FAMILY, LIFECYCLE_STATUS, RELEVANT_PRICES_JSON, LIFECYCLE_HISTORY_JSON`
- **ORDER BY** `START_TS, SYMBOL, PATTERN_FAMILY, LIFECYCLE_STATUS,` canonical JSON of prices/history — **no** `PATTERN_INSTANCE_ID`
- Optionally normalize link snapshots by stripping instance ids when comparing bar-links

Use semantic hash for Phase C benchmark equivalence. Continue writing production `pattern_sequence_hash` onto attempt headers as today.

### Other stages
- Objective / context: production sequence hashes may be compared across attempts (no attempt-specific ids in content/order).
- Row counts + exact schedule-key sets must match.
- Sampled payload equality for known bars (JPM resistance, AAPL NO_CLEAR_LONG).

---

## C10. Before/after benchmark method

Disposable attempts tagged `PHASE_C_BENCHMARK` on stored week `2026-06-22`. Record evidence JSON with statement counts, phase timings, semantic hashes, integrity results.

**Bulk enablement:** keep `BROOKS_PERSIST_MODE=legacy` until benchmark + equivalence accepted; then explicitly set `bulk`.

---

## C11. Rollback plan

| Layer | Action |
|---|---|
| Code | `BROOKS_PERSIST_MODE=legacy` (default) |
| Failed attempt | Mark FAILED on fresh connection; leave partial rows |
| Official / Freeze / V0.2 / week 3 | Never modified or deleted |
| Run state | Do not auto-point learning/Phase 9 to unapproved benchmark attempts |

---

## C12. Delivery checklist

1. Amend design (this document) — done.
2. Deploy additive `SIMULATION_ATTEMPT_ID` DDL.
3. Implement batch INSERT helpers; default persist mode **legacy**.
4. Wire bulk path + schedule-key integrity + instance-count equality + fail-on-fresh-connection.
5. Progress phases: compute / persist / integrity.
6. Semantic pattern hash helper (benchmark only).
7. Unit tests + focused smoke; explicit bulk benchmark when ready.
8. No Phase D.

---

## C13. Explicit preservations

- Paused third week remains paused.
- `BROOKS_CONTEXT_RULESET_V0_2` unchanged.
- Freeze V1 unchanged and active.
- All existing attempts retained.
