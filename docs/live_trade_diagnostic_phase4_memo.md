# Live trade diagnostic — Phase 4 memo (strategy shape + delayed data + feedback loop)

**Anchor:** Broker-truth post-reset cohort, DUQ101771, **8 closed FIFO losing round-trips** (definitions in `10` / `11`).  
**Phase 4 adds:** `17` (IB path, markdown), `18_strategy_shape_misalignment.sql`, `19` (closeout trace, markdown), this memo.  
**Constraint:** Diagnostic only — no production, IB subscription, TWS/Gateway, threshold, or strategy changes.

---

## 1. Ranked conclusions (proven vs plausible vs unknown)

| Priority | Issue | Strength | Summary |
|----------|--------|----------|---------|
| **P1** | **Closeout / learning loop broken at fill persistence** | **Proven** | `TRADE_CLOSEOUT` is written only from hooks that require **`LIVE_ORDERS.STATUS = FILLED`** (and qty rules) on **exit** or **protective** legs (`entry_intel_hooks.py`). Cohort shows **PENDINGSUBMIT** + **null** fills while **BROKER_SNAPSHOTS** show executions → **first break** is **MIP order/fill state**, so **closeout never runs** → **no `V_TRADE_INTELLIGENCE`** (closeout-grain). |
| **P2** | **Delayed / non-live bar context for “latest 1m”** | **Proven (path + timestamps)** | Direct refresh uses **`cursorfiles/fetch_ibkr_live_bars.py`** → **`ib_insync.IB.reqHistoricalData`** with **`keepUpToDate` not set** (historical snapshot, not streaming). **No `reqMarketDataType()`** in MIP fetch code — type follows **TWS/Gateway default + entitlements**. Revalidation without refresh reads **last `MARKET_BARS` IB 1m**. Persisted **`ONE_MIN_BAR_TS` lags broker `ENTRY_TS` by ~16–17 min (most)** — consistent with **delayed or end-of-bar historical** behavior. |
| **P3** | **Freshness gate vs observed lag** | **Proven (cohort + SQL `18` stmt 3)** | Portfolio **`QUOTE_FRESHNESS_THRESHOLD_SEC`** for `PORTFOLIO_ID = 1` is **1800** in Snowflake (not the code default **900** when the row is missing). **Bar-end to `LIVE_ACTIONS.UPDATED_AT` ~8200 s** → **`WOULD_PASS_BAR_AGE_VS_ACTION_UPDATED` = FAIL** for all eight. **Bar-end to broker entry** is **~981–2180 s** → **FAIL vs 1800 only for WMT (2180)**; **PASS** for the other seven under that single threshold. **Delay contamination risk** remains **HIGH** in `18` because **bar-age-at-update** fails everywhere (stale vs row touch) and **WMT** fails bar-vs-entry even at **1800 s**. |
| **P4** | **Strategy-shape mismatch (small edge / tight risk vs patient hold)** | **Plausible / structurally supported** | Gate **`MIN_AVG_RETURN = 0.0005`**, **daily (1440) signals**, **stops on PG/WMT ~1.8–2.7%**, **six trades without persisted bracket %**. **Energy** losses are **gap-dominated** (wider stops alone insufficient). **Large-cap** subset mixes **tight stop exits** (WMT/PG) with **small scratch / drift** — framework reads as **finite patience + path fragility**, not **thesis-resolution holding**. |

**Unknown without runtime IB session audit:** actual **`marketDataType`** byte returned by IB for your login (delayed vs live); **exact** committee request-time `now` vs bar_ts for each call (SQL only has persisted `ONE_MIN_BAR_TS` / `UPDATED_AT`).

---

## 2. User hypotheses (explicit)

| Hypothesis | Verdict |
|------------|---------|
| MIP may use delayed IB bars as live | **Supported:** historical **`reqHistoricalData`** path; **no explicit live/delay switch** in code; **stored bar end ≈15+ min before broker entry** on all eight. |
| Missing closeout / PW learning | **Proven at trade grain:** no closeout → no TIR; **portfolio-day PW diffs can still exist** — do not equate to per-trade learning. |
| Tiny-edge / tight-risk vs patient hold | **Supported as “current shape”:** training gate + **1440** proposals + **tight stops where present**; **not** proof patient-hold would have won. |
| Stop/risk mismatch for strong names | **Partial:** **WMT** stop **<** same-day range; **PG** stop **≈** full-day range; **JNJ/NEE** more **drift** than single gap; categorize per `18` SQL. |
| Linear horizon logic ignores path | **Supported at design level:** **H1 horizon returns** on recs are **not** bracket/path-aware; **gap days** violate smooth path assumption. |

---

## 3. Deliverable map

| Artifact | Role |
|----------|------|
| `MIP/docs/17_ib_api_bar_path_and_market_data_mode.md` | File/function → IB API methods; `reqMarketDataType` **not called** in app; freshness thresholds table. |
| `MIP/SQL/scripts/live_trade_diagnostic/18_strategy_shape_misalignment.sql` | Freshness gate truth test + strategy-shape / large-cap classification. |
| `MIP/docs/19_closeout_feedback_path_trace.md` | Broker → orders → closeout → TIR/PW intended flow; **first broken transition**. |
| This memo | Synthesis and ranking. |

---

## 4. Smoke

Executed **`18_strategy_shape_misalignment.sql`** via `query_snowflake.py` **`-s 3`**, **`-s 4`**, **`-s 5`** (after `USE ROLE` / `USE DATABASE` as stmts 1–2). **Passed** (8 rows each on 3–4; stmt 5 after fixing a semicolon-in-string splitter issue).

**Deployed:** none (diagnostic SQL + docs only).

**Your action:** None for diagnostics; optional IB session check of **market data type** in TWS if you need **live vs delayed** proof beyond code + timestamps.
