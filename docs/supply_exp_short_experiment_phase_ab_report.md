# SHORT supply-expansion experiment — Phase A & B report

**Window:** `2025-08-01` through `current_date()` (Snowflake run date **2026-04-06**).  
**Scope:** Research backfills only — no proposal, committee, execution, or production-trust changes.  
**Ingest:** IB-only (per phase plan). SHORT patterns **inactive** between runs (`IS_ACTIVE='N'`, `ENABLED=false`).

---

## Phase A — Step 1: IB adequacy audit (`SUPPLY_EXP_2026`)

**Script:** [`MIP/SQL/smoke/supply_exp_2026_ib_adequacy_audit.sql`](../SQL/smoke/supply_exp_2026_ib_adequacy_audit.sql)

**Calendar reference:** NYSE session dates from **SPY** daily **IBKR** bars.  
**Parameters (from audit output):**

| Field | Value |
|--------|--------|
| `D_PHASE_START` | 2025-08-01 |
| `D_AS_OF` (SPY last bar) | 2026-03-24 |
| `D_HISTORY_FLOOR` (60th NYSE session before phase) | 2025-05-06 |
| Expected sessions in **U** (`D_HISTORY_FLOOR` … `D_AS_OF`) | 222 |

### A. PASS / FAIL (all 16 symbols)

| SYMBOL | STATUS | FIRST_BAR | LAST_BAR | GAP |
|--------|--------|-----------|----------|-----|
| ABBV, COP, CRWD, DAL, FCX, MCD, MRK, NEE, NET, NUE, ORCL, PANW, QCOM, SBUX, SLB, TGT | **PASS** | 2025-04-03 | 2026-04-02 | none |

**All 16 symbols PASS.** No `CALENDAR_GAP`, `INSUFFICIENT_HISTORY`, or `IB_INADEQUATE` failures.

### B. Scoped disable

No failures → **no `IS_ENABLED` updates.** If a symbol had failed, the approved remediation is **`UPDATE` only** the matching `INGEST_UNIVERSE` row (`STOCK`, `1440`, `SYMBOL_COHORT = 'SUPPLY_EXP_2026'`).

---

## Phase A — Step 2: Strict SHORT backfill (1101, 1102 only)

**Script:** [`MIP/SQL/scripts/short_supply_exp_phase2_strict_only_backfill.sql`](../SQL/scripts/short_supply_exp_phase2_strict_only_backfill.sql)

- Temporarily enabled **only** `PATTERN_ID` **1101** and **1102**.
- `CALL MIP.APP.SP_BACKFILL_SHORT_MOMENTUM('2025-08-01', current_date(), 'STOCK', 1440, …, false)`.
- Restored **1101/1102** to inactive after completion.
- `SHORT_MOMENTUM_GENERATION_ENABLED` toggled inside proc and **restored** (`short_gate_restored: true`).

### Deliverables (Step 2)

| Item | Value |
|------|--------|
| **A. run_id** | `38e747c8-a4f6-4058-b0fa-9340559c7cdf` |
| **B. Total strict SHORT signals** (1101+1102, in window, STOCK 1440) | **390** |
| **C. By pattern_id** | 1101: **148**, 1102: **242** |
| **D. Distinct symbols (strict, combined)** | **59** (union of symbols with either pattern in window) |
| **E. Top symbols by strict count** (from proc summary) | COIN 17, MSFT 16, NKE 16, SOFI 15, CRM 15, PYPL 14, **ORCL** 14, BA 13, **CRWD** 12, DIS 11 |

**F. Outcomes by horizon (1101 / 1102)** — `EVAL_STATUS = 'SUCCESS'` for labeled stats:

| Pattern | H | n_outcomes | n_labeled | hit_rate | avg_return | median_return |
|---------|---|------------|-----------|----------|------------|---------------|
| 1101 | 1 | 148 | 148 | 0.446 | -0.00126 | -0.00377 |
| 1101 | 3 | 148 | 147 | 0.429 | -0.00069 | -0.00577 |
| 1101 | 5 | 148 | 143 | 0.490 | 0.00791 | -0.00070 |
| 1101 | 10 | 148 | 134 | 0.530 | 0.01209 | 0.00606 |
| 1101 | 20 | 148 | 127 | 0.417 | 0.00057 | -0.01512 |
| 1102 | 1 | 242 | 242 | 0.475 | -0.00055 | -0.00202 |
| 1102 | 3 | 242 | 242 | 0.467 | 0.00173 | -0.00250 |
| 1102 | 5 | 242 | 233 | 0.502 | 0.00807 | 0.00000 |
| 1102 | 10 | 242 | 220 | 0.495 | 0.00580 | -0.00048 |
| 1102 | 20 | 242 | 196 | 0.464 | -0.00255 | -0.01232 |

**G. Safety (Step 2 proc + spot checks)**

- `v_signal_outcomes_base_short_rows_in_range`: **0**
- `v_trusted_signals_latest_ts_short_rows`: **0**
- Post-hoc SQL: `V_SIGNAL_OUTCOMES_BASE` ⋈ `RECOMMENDATION_LOG` with `SIGNAL_DIRECTION='SHORT'` → **0** rows  
- Same for `V_TRUSTED_SIGNALS_LATEST_TS` → **0** rows  

### Before / after: universe expansion effect on **strict** SHORT

**Definition — “old universe”:** `SYMBOL` **not in** the 16 `SUPPLY_EXP_2026` tickers.  
**Definition — “expansion cohort only”:** `SYMBOL` **in** those 16.

| Pattern | Baseline (old only) n_signals | After strict backfill (old only) | Cohort-only n_signals |
|---------|------------------------------|----------------------------------|------------------------|
| **1101** | 123 | **123** (unchanged) | **25** |
| **1102** | 192 | **192** (unchanged) | **50** |

**Interpretation:** The strict backfill is **idempotent** on legacy symbols; **all incremental strict SHORT supply** from this run sits on the **16 new names** (**+75** signals total: 25 + 50). Expanded-universe **totals** become **148** (1101) and **242** (1102).

**Note:** First backfill run moved `V_SYMBOL_TRAINING_READINESS` aggregate outcomes (`sum_outcomes_n` 77132 → 77668) because new outcomes were evaluated — **row_count** stayed **77**. Second backfill left readiness sums unchanged.

---

## Phase B — Step 3: Relaxed SHORT patterns (first pass)

**Migration (deployed):** [`MIP/SQL/migrations/20260406_short_relaxed_patterns_supply_exp.sql`](../SQL/migrations/20260406_short_relaxed_patterns_supply_exp.sql)

### A. Definitions

| PATTERN_ID | NAME | DISPLAY_NAME | PATTERN_FAMILY |
|------------|------|--------------|----------------|
| **1103** | `STOCK_MOMENTUM_FAST_SHORT_RELAXED` | Momentum (Fast, SHORT, relaxed) | `MOMENTUM_SHORT_RELAXED` |
| **1104** | `STOCK_MOMENTUM_SLOW_SHORT_RELAXED` | Momentum (Slow, SHORT, relaxed) | `MOMENTUM_SHORT_RELAXED` |

### B. Parameter deltas vs strict (only `min_return` / `min_zscore`)

| Parameter | 1101 strict | **1103 relaxed** | 1102 strict | **1104 relaxed** |
|-----------|------------|-------------------|------------|------------------|
| fast_window | 20 | 20 | 30 | 30 |
| slow_window | 3 | 3 | 2 | 2 |
| lookback_days | 30 | 30 | 60 | 60 |
| **min_return** | **0.002** | **0.0015** | **0.001** | **0.0008** |
| **min_zscore** | **1.0** | **0.5** | **0.75** | **0.5** |

### C. Objects touched

- `MIP.APP.PATTERN_DEFINITION` — **insert** 1103, 1104 (inactive).
- **No** change to **`070_sp_generate_momentum_recs.sql`** (no `enforce_new_low` relaxation).
- **Strict 1101/1102** rows **unchanged** (verified in Snowflake after migration).

### D. Training Status

API already exposes `pattern_display_name` and `pattern_family` from `V_PATTERN_METADATA_UI` ([`training.py`](../apps/mip_ui_api/app/routers/training.py)). Relaxed rows use distinct **DISPLAY_NAME** / **DISPLAY_SHORT_NAME** and **`MOMENTUM_SHORT_RELAXED`** family for UI distinction when patterns are enabled for research.

---

## Phase B — Step 4: Strict + relaxed backfill

**Script:** [`MIP/SQL/scripts/short_supply_exp_phase2_strict_relaxed_backfill.sql`](../SQL/scripts/short_supply_exp_phase2_strict_relaxed_backfill.sql)

| Item | Value |
|------|--------|
| **A. run_id** | `949245db-7f53-44d2-92ca-e35534b1a651` |
| **B. Total SHORT signals (all four patterns)** | **948** |
| **C–D. By pattern** | See table below |

| PATTERN_ID | n_signals | n_symbols | n_signal_days |
|------------|-----------|-----------|---------------|
| 1101 | 148 | 51 | 68 |
| 1102 | 242 | 59 | 90 |
| 1103 | 241 | 57 | 92 |
| 1104 | 317 | 60 | 104 |

**E. Outcomes by horizon** — full table in Step 2-style metrics above (query snapshot 2026-04-06); relaxed **1103/1104** rows included in JSON run output.

**F. Safety**

- Proc: `v_signal_outcomes_base_short_rows_in_range` **0**, `v_trusted_signals_latest_ts_short_rows` **0**
- Post-hoc SQL (same as Phase A): **0** / **0**
- `readiness_unchanged`: **true** for this run

---

## Phase B — Step 5: Comparison report

### 1. Universe expansion vs strict SHORT

| Metric | 1101 old-only | 1101 expanded | 1102 old-only | 1102 expanded |
|--------|---------------|---------------|---------------|---------------|
| Total signals | 123 | **148** | 192 | **242** |
| Distinct symbols | 39 | **51** | 44 | **59** |
| Distinct signal days | 64 | **68** | 83 | **90** |

**Cohort-only increment:** +25 (1101) and +50 (1102) on the 16 new symbols.

### 2. Relaxed vs strict (expanded universe, same window)

| Pair | Strict n_signals | Relaxed n_signals | Relaxed / strict |
|------|------------------|-------------------|------------------|
| Fast 1101 vs 1103 | 148 | 241 | **1.63×** |
| Slow 1102 vs 1104 | 242 | 317 | **1.31×** |

**Quality snapshot (H=1 bar):**

| Pattern | hit_rate | avg_return |
|---------|----------|------------|
| 1101 | 0.446 | -0.00126 |
| 1103 | 0.456 | -0.00059 |
| 1102 | 0.475 | -0.00055 |
| 1104 | 0.489 | 0.00028 |

Relaxed fast shows **similar** hit rate and **less negative** mean at H=1; relaxed slow **slightly higher** hit rate and **near-flat** mean at H=1. Longer horizons **mixed** — review full horizon block before production use.

### 3. Interpretation

1. **Universe expansion:** Material for **strict** SHORT **count** on the **new** symbols (+75 strict signals); legacy strict counts **unchanged** (dedupe).
2. **Relaxed thresholds:** **Large** density gain (**+558** incremental SHORT signals vs strict-only totals in-window: 948 − 390).
3. **Quality:** At short horizons, relaxed is **not** obviously worse on hit rate; median returns remain **noisy/negative** at several horizons — **monitor** before any trust use.
4. **Research enablement:** Reasonable to keep **relaxed patterns enabled only for research backfills** (`IS_ACTIVE='Y'` when running `SP_BACKFILL_SHORT_MOMENTUM`); keep **`SHORT_MOMENTUM_GENERATION_ENABLED=false`** for daily pipeline unless deliberately testing.
5. **Disable policy:** Keep **strict 1101/1102** as baseline reference; **no need to disable** strict. Relaxed can stay **inactive by default** between experiments.

---

## Phase B — Step 6: Training Status validation (structured)

**No additional UI/API code** was required for this experiment beyond the earlier Training Status pass.

| Check | Evidence |
|-------|----------|
| Ticker primary | `formatSymbolLabel` + symbol column ([`TrainingStatus.jsx`](../apps/mip_ui_web/src/pages/TrainingStatus.jsx)). |
| Company name secondary | Alias dict + `formatSymbolLabel` (existing). |
| Strict vs relaxed | Distinct `DISPLAY_NAME` / `pattern_family` from API when patterns are active in data. |
| LONG vs SHORT | Direction badge (existing). |
| Desktop width | Horizons moved to **expanded** row + sticky header ([`TrainingStatus.css`](../apps/mip_ui_web/src/pages/TrainingStatus.css)). |

**Screenshots:** Not captured in this agent run (no browser). Open **Training Status** after temporarily enabling SHORT patterns for a smoke UI check if needed.

**Remaining issues:** If SHORT rows are sparse with patterns disabled, grid may show only LONG — expected until research patterns are enabled for generation.

---

## Artifact index

| Artifact | Path |
|----------|------|
| IB adequacy audit | [`MIP/SQL/smoke/supply_exp_2026_ib_adequacy_audit.sql`](../SQL/smoke/supply_exp_2026_ib_adequacy_audit.sql) |
| Strict-only backfill driver | [`MIP/SQL/scripts/short_supply_exp_phase2_strict_only_backfill.sql`](../SQL/scripts/short_supply_exp_phase2_strict_only_backfill.sql) |
| Strict+relaxed backfill driver | [`MIP/SQL/scripts/short_supply_exp_phase2_strict_relaxed_backfill.sql`](../SQL/scripts/short_supply_exp_phase2_strict_relaxed_backfill.sql) |
| Relaxed pattern migration | [`MIP/SQL/migrations/20260406_short_relaxed_patterns_supply_exp.sql`](../SQL/migrations/20260406_short_relaxed_patterns_supply_exp.sql) |

---

## What was deployed / executed

| Object / action | Status |
|-----------------|--------|
| `supply_exp_2026_ib_adequacy_audit.sql` | **Executed** (read-only audit) |
| `20260406_short_relaxed_patterns_supply_exp.sql` | **Deployed** to Snowflake |
| `SP_BACKFILL_SHORT_MOMENTUM` strict-only run | **Executed** (`38e747c8-…`) |
| `SP_BACKFILL_SHORT_MOMENTUM` strict+relaxed run | **Executed** (`949245db-…`) |
| `PATTERN_DEFINITION` 1101–1104 | **Inactive** after runs |

**Your action required:** Optional UI screenshot for Step 6 archives; enable **1101–1104** only inside controlled backfill scripts (as above), not for production daily generation unless you explicitly decide otherwise.
