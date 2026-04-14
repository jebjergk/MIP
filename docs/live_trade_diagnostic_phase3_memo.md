# Live trade diagnostic — Phase 3 memo (DUQ101771 broker-truth loss cohort)

**Scope:** Post-reset broker-truth cohort (`10_broker_truth_duq101771_post_reset.sql`), anchored on the **8 closed FIFO losing round-trips** (see `11_duq_phase2_closed_fifo_hypotheses.sql`).  
**Phase 3 SQL:** `12`–`16` under `MIP/SQL/scripts/live_trade_diagnostic/`.  
**Constraint:** Diagnostic only — no production, threshold, strategy, UI, or IB subscription changes in this phase.

---

## 1. Hypothesis ranking (evidence strength)

| Rank | Hypothesis | Verdict | Why |
|------|------------|---------|-----|
| **1** | **H3 — Feedback loop break / outcome propagation** | **Proven (Snowflake)** | All **8** entry `ACTION_ID`s have **`TRADE_CLOSEOUT` = 0 rows**, hence **`MIP.MART.V_TRADE_INTELLIGENCE` = 0 rows** (view grain is closeout). **`ENTRY_INTEL_ACTION_LINK` = 0 rows** for these actions. **`LIVE_ORDERS`** show **no** `FILLED` / `PARTIAL_FILL` and **no** `QTY_FILLED` while broker executions exist — lifecycle is **desynchronized** from broker truth. Portfolio-level **`V_PARALLEL_WORLD_DIFF` rows exist** on exit calendar dates, but **trade-level regret / TIR linkage cannot attach** without closeout. Run: `14_closeout_propagation_pw_gap.sql`. |
| **2** | **H4 — IB / “latest 1m” freshness vs decision time** | **Proven (Snowflake, historical anchor)** | For **all 8** trades, `LIVE_ACTIONS.ONE_MIN_BAR_TS` is **~16–17 minutes behind broker `entry_ts`** for six names (**981–1007 s**), **longer for APA (~26 min)** and **WMT (~36 min)**. `EXECUTION_PRICE_SOURCE = IBKR_DIRECT_1M`. This supports the user’s **~15 minute delayed bar** concern as **material** for most entries (not casual seconds). **Caveat:** `ONE_MIN_BAR_TS` is the **bar end timestamp**, not the wall-clock time of committee completion; it is still the **stored micro-anchor** for execution/revalidation context. Run: `15_ib_data_freshness_and_delay.sql`. **Code:** `live.py` revalidation uses latest `MARKET_BARS` IB 1m (or refresh) and compares **`bar_age_sec` to `QUOTE_FRESHNESS_THRESHOLD_SEC`** (portfolio config, default **900 s**). |
| **3** | **H1 — Tiny training edge vs path volatility** | **Partially proven** | Active gate **`MIN_AVG_RETURN = 0.0005` (0.05%)** (`TRAINING_GATE_PARAMS`) — **tiny** vs realized losses and vs daily ranges. Training **H=1** `REALIZED_RETURN` on the linked `RECOMMENDATION_ID`s is often **sub-20 bps** or **negative** for several cohort recs, while **broker round-trips** are **−1.8% to −14.6%** and **next-session adverse %** is **large for energy**. Flags in `12_training_edge_vs_path_volatility.sql`. **Not** a full distributional proof across all symbols — only **cohort-linked** recs. |
| **4** | **H2 — Bracket vs path / stop fragility** | **Mixed / partial** | **6 / 8** trades: **`BRACKET_DATA_MISSING`** in `PARAM_SNAPSHOT` (no executable/committee stop% in Snowflake). **PG / WMT:** **stop% present**, exits **`STOP_AT_OR_NEAR_BRACKET`**; **WMT** stop **narrower than same-day high-low range**; **PG** stop **≈ full-day range**. **Energy trio:** **`GAP_MOVE_DOMINATED`** vs bracket story. Run: `13_bracket_construction_vs_realized_path.sql`. |

**Synthesis query (heuristic labels):** `16_phase3_synthesis_closed_trades.sql` — use memo reasoning for interpretation; SQL `CASE` order prioritizes gap/stop/stale bar.

---

## 2. User-submitted hypotheses — explicit test

| User claim | Result |
|------------|--------|
| Committee cautious after bear-pattern / regime framing | **Not proven from verdict JSON** (no `BEAR`/`REGIME` tokens in `COMMITTEE_VERDICT` for these runs). **Uniform `PROCEED_REDUCED`** + size factors — **caution**, not a written “bear frame.” Stale **`V_MARKET_REGIME`** ends **2026-03-24** — **cannot** assert April regime from that view. (Phase 2; still valid.) |
| Daily signal framing vs live intraday execution | **Supported:** all eight proposals **`INTERVAL_MINUTES = 1440`**; fills use **`IBKR_DIRECT_1M`** anchor. **Mismatch** between **signal horizon** and **micro price path** is **structural**. |
| Expected-return thresholds too small for robust live brackets | **Supported** for **gate floor** (0.05%) and **many H1 training returns** vs **multi-percent** path moves; **bracket** often **missing in persistence** for six trades. |
| Closed trades not feeding PW / regret | **Proven for trade-level path:** **no `TRADE_CLOSEOUT` → no `V_TRADE_INTELLIGENCE`.** **Do not** infer “PW empty” from missing trades — **portfolio-day PW diffs exist**, but **per-trade learning chain is broken at closeout**. |
| Latest IB 1m delayed ~15 min and used as live | **Supported on cohort:** **6/8** in **~15 min band** vs broker entry; **2/8** **worse**. |

---

## 3. Proven vs plausible vs unknown

**Proven by data (this environment)**  
- **No `TRADE_CLOSEOUT` / no TIR / no `ENTRY_INTEL_ACTION_LINK`** for the eight broker-real closes.  
- **`LIVE_ORDERS` not marked filled** despite broker executions.  
- **Persisted `ONE_MIN_BAR_TS` materially lags broker entry time** for all eight.  
- **Training gate `MIN_AVG_RETURN` is 0.05%.**  
- **PG/WMT** exits align with **computed stop**; **six** trades lack bracket % in `PARAM_SNAPSHOT`.

**Plausible but not fully proven**  
- **Stale bar alone caused** each loss (losses are **multi-causal**; **energy gap** dominates three trades).  
- **Exact** request-time lag at committee step (would need **request/audit** timestamps per call — not fully in SQL).  
- **TWS API “delayed mode”** — **not** read from Snowflake; confirm in **TWS / API connection** if required (user FYI).

**Unknown / untestable here**  
- **Sub-minute path** (1m/5m/15m) **homogeneous** series for all symbols (coverage uneven in `MARKET_BARS`).  
- **Historical IB “delayed vs live” flag** at each decision (no column found in stored actions).  
- Full **committee prompt text** at runtime (only **stored verdict / params**).

---

## 4. Script map

| File | Purpose |
|------|---------|
| `12_training_edge_vs_path_volatility.sql` | Gate params, digest sample, `RECOMMENDATION_OUTCOMES` by horizon, cohort flags vs daily/next-session/stop. |
| `13_bracket_construction_vs_realized_path.sql` | Bracket %, exit vs stop, range labels, gap vs intraday scale, recovery after exit. |
| `14_closeout_propagation_pw_gap.sql` | Lifecycle vs broker truth; PW diff existence on exit day; summary by stage. |
| `15_ib_data_freshness_and_delay.sql` | Portfolio freshness threshold; bar end vs broker entry; latest IB 1m snapshot per symbol. |
| `16_phase3_synthesis_closed_trades.sql` | Single cross-hypothesis table (heuristic dominant label). |

**Prior cohort definition:** `10_…`, `11_…`.

---

## 5. Smoke (executed in dev)

- `12` statement **6** (cohort join flags): **OK**  
- `13` statement **3**: **OK**  
- `14` statement **4** (summary): **8 × `NO_TRADE_CLOSEOUT`**  
- `15` statement **4** (per-trade lag): **OK**  
- `16` statement **3**: **OK**

---

## 6. What you should do next (optional, not done here)

- **Closeout pipeline:** emit `TRADE_CLOSEOUT` (and links) from **broker truth** or reconciled fills.  
- **Bar freshness:** enforce **`bar_age_sec` vs threshold** on **every** path that writes `ONE_MIN_BAR_*`, not only revalidation; log **request_ts** vs **bar end**.  
- **Bracket persistence:** ensure **executable/committee bracket** lands in `PARAM_SNAPSHOT` (or sibling table) for **all** live entries.  
- **TWS:** verify **market data type** (live vs delayed) on the session used for ingest.

**No action required** to reproduce diagnostics — re-run the SQL files above as `MIP_ADMIN_ROLE`.
