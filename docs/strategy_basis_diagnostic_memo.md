# Strategy-basis diagnostic memo (Phase 3 — planning only)

**Status:** Diagnostic and **options for redesign** only. **No** implementation of pattern changes, thresholds, horizons, brackets, or universe tiering in this memo. **No** universe replacement.

**Scope link:** [post_diagnostic_repair_plan.md](post_diagnostic_repair_plan.md) formal review gate; [18_strategy_shape_misalignment.sql](../SQL/scripts/live_trade_diagnostic/18_strategy_shape_misalignment.sql) for empirical shape.

**Environmental rule (next month):** Do **not** evaluate the strategy as a **real-time intraday** system while IB data is delayed — see [live_market_data_path_fix_spec.md §12–§13](live_market_data_path_fix_spec.md#12-operating-stance-2026-04--delayed-data-period). Focus **execution truth** ([execution_truth_reconcile_playbook.md](execution_truth_reconcile_playbook.md)) and **structural** questions below on **daily / path / bracket philosophy** grounds.

---

## 1. Ranked strategy-basis problems (largest first)

| Rank | Problem | Why it matters |
|------|---------|----------------|
| 1 | **Path blindness between signal and outcome** | Expected return and success metrics are often **point estimates** or **linear horizon** summaries. Real P&amp;L is a **path**: drawdown, time-under-water, gap risk, and stop proximity dominate lived experience. Misalignment produces **false confidence** and wrong committee framing. |
| 2 | **Thresholds vs realized volatility** | **Success floors** and **expected-return** gates may be **too small** relative to **name-level volatility** and **holding-period noise**. Outcomes then look like “strategy failure” when they are **scale mismatch** (signal vs noise ratio). |
| 3 | **Bracket philosophy vs patience narrative** | **Tight stops** and **fixed R-multiple targets** may be **fundamentally mismatched** to **patient-hold** and **strong-name** theses. The system can **approve** a story it **cannot survive** mechanically. |
| 4 | **Pattern training basis vs live path** | Patterns may be trained or filtered on **bars, regimes, or labels** that **do not match** live **fill path**, **session**, or **liquidity**. Diagnostic SQL (e.g. `18`) can expose **shape** mismatch; redesign would address **basis**, not just thresholds. |
| 5 | **Single-tier universe stress** | A **flat universe** forces one **risk and patience** contract across names with different **microstructure** and **tail behavior**. **Tiering** (later) may reduce **one-size-fits-all** bracket errors without **replacing** the universe outright. |

---

## 2. Options catalog (no commitment — design space only)

### 2.1 Pattern redesign

- Re-derive pattern eligibility on **daily or session** features that **survive delayed data** and match **actual hold horizons**.
- Separate **pattern families** by **volatility bucket** or **liquidity tier** before live scoring.
- Add explicit **path-sensitive** labels (e.g. max adverse excursion bands) in training targets, not only terminal return.

### 2.2 Target / success-threshold redesign

- Express floors in **vol-normalized** units (e.g. multiples of realized or ATR-scaled noise) rather than raw **expected return** points.
- **Two-track gates:** minimum **edge per risk unit** and maximum **fragility** (e.g. stop distance vs typical wiggle).
- **Relaxed intraday precision** while delayed: emphasize **next-session** and **weekly** consistency checks.

### 2.3 Horizon redesign

- Replace **pure linear time-to-target** with **path checkpoints** (e.g. “if by day *t* MFE &lt; x, downgrade conviction”).
- **Session-aware** horizons (open, mid, close) instead of continuous intraday bar counts when data is delayed.
- Explicit **minimum patience window** before stop logic is allowed to dominate the narrative.

### 2.4 Bracket philosophy redesign

- **Volatility-scaled** stop/target widths by name class; **asymmetric** brackets when thesis is **skewed** (not symmetric R).
- **Protective leg** rules that **do not** contradict committee **hold** guidance (documented tension resolution).
- **Tiered** aggressiveness: smaller size + wider survival zone for **high-path-risk** names.

### 2.5 Possible future universe tiering

- **Tier A:** large, liquid — standard brackets and intraday-friendly (when data returns).
- **Tier B:** patient-hold — wider survival, **daily** governance, reduced intraday sensitivity.
- **Tier C (research):** optional **exclude** or **paper-only** without **full universe replacement**.

---

## 3. Recommended diagnostic actions (read-only)

1. Run and archive **`18_strategy_shape_misalignment.sql`** (and related phase-4 pack) **after** Track A reconcile so **closeouts** and **TIR** reflect broker truth.
2. Segment results by **symbol**, **volatility decile**, and **`PATH_APPROXIMATION_LEVEL`** (if present) — not only by win rate.
3. Document **committee vs outcome** separately from **bracket vs path** — avoid conflating **signal quality** with **execution geometry**.

---

## 4. Explicit non-goals (this phase)

- No change to **training universes** or **pattern IDs** in production.
- No new **live entry** automation tied to **1m freshness**.
- No **Parallel Worlds** **logic** redesign — consume existing surfaces; see repair plan.
