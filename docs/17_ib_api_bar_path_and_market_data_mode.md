# IB API bar path and market-data mode (Phase 4 diagnostic)

**Scope:** How MIP obtains the “latest” 1-minute (or fallback) bar context for **live revalidation** and related UI paths. **No production changes.**

---

## 1. File / function map (first-party)

| Step | Location | Role |
|------|----------|------|
| Revalidation entry | `MIP/apps/mip_ui_api/app/routers/live.py` — `revalidate_live_action` (~9548+) | Loads `QUOTE_FRESHNESS_THRESHOLD_SEC` (default **900**), optional `_force_refresh_latest_one_minute_bars`, else reads `MIP.MART.MARKET_BARS` IBKR 1m (then 15/60/1440 fallback). Computes `bar_age_sec = now_utc - ref_ts_utc`. |
| Forced 1m refresh | `live.py` — `_force_refresh_latest_one_minute_bars` → `_run_agent_ibkr_bar_refresh` (~3695–3482) | Subprocess: `cursorfiles/.venv/Scripts/python.exe cursorfiles/fetch_ibkr_live_bars.py` with `--interval-minutes 1`, `--window-bars 1`. |
| IB fetch implementation | `cursorfiles/fetch_ibkr_live_bars.py` — `_fetch_symbol` | **`ib_insync.IB.reqHistoricalData(...)`** with `endDateTime=""`, **no `keepUpToDate` argument** → defaults to **`False`** (one-shot historical pull, not streaming updates). |
| Mart reference (committee alignment) | `live.py` — `_fetch_ibkr_mart_reference_close` (~3702+) | Latest close from `MARKET_BARS` IBKR 1m / fallback — **Snowflake**, not live socket. |
| Duplicate entry point | `MIP/apps/mip_ui_api/app/services/ibkr_live_bars.py` | Same script path pattern for agent/runtime bar fetch (verify call sites if extending diagnostics). |

**Ingest (separate concern):** `cursorfiles/ingest_ibkr_bars.py` also uses historical bars for persistence into Snowflake; freshness of **`MARKET_BARS`** depends on ingest schedule + IB behavior, not identical to the subprocess “direct refresh” path.

---

## 2. Exact IB API methods used

| Path | API / method | Notes |
|------|----------------|-------|
| Direct live refresh | **`reqHistoricalData`** | Snapshot of recent bars; last row treated as “current” close. |
| Revalidation without refresh | **SQL** `select TS, CLOSE from MIP.MART.MARKET_BARS ...` | No `reqMktData` / `realTimeBars` on this path. |
| **`reqMarketDataType()`** | **Not called** in `fetch_ibkr_live_bars.py`, `live.py`, or ingest script (grep first-party). | Mode follows **TWS/Gateway + subscription entitlements** defaults. |
| **`reqMktData`** | Not used in the traced 1m bar paths. | — |
| **`keepUpToDate=True`** | **Not used** in `fetch_ibkr_live_bars.py`. | Updated-bar stream pattern from IBKR docs is **not** implemented here. |
| **`realTimeBars`** | Not used in traced paths. | — |

---

## 3. Live vs delayed — detectable in code?

| Question | Answer |
|----------|--------|
| Does MIP set delayed vs real-time explicitly? | **No** — no `reqMarketDataType()` in app/bar code. |
| Does MIP read IB’s “delayed” flag on bars? | **No** discriminator found on historical bar objects in this path; only **timestamps and OHLC** are used. |
| What *is* checked? | **Bar timestamp age** vs wall clock at revalidation (`bar_age_sec` vs `QUOTE_FRESHNESS_THRESHOLD_SEC`). That measures **staleness of the bar’s end timestamp relative to server clock**, not **IB market data type** (real-time vs delayed). |

**Conclusion:** The stack does **not** distinguish “live” vs “delayed” market data type; it only distinguishes **old bar end time** vs **fresh** under a configurable second threshold (and exit path bypasses stale blocking).

---

## 4. Timestamp / freshness gating before use

| Gate | Where | Behavior |
|------|-------|----------|
| `bar_age_sec > freshness_threshold_sec` | `live.py` revalidation (~9631–9640) | **Blocks** non-exit revalidation when **extended session open** and bar too old. |
| Exit revalidation | Same region (~9658–9660) | Does **not** block on stale bar; adds **`EXIT_REVALIDATION_STALE_BAR_BYPASS`**. |
| Outside session | (~9660–9661) | Stale bar allowed with reason **`REVALIDATION_STALE_BAR_OUTSIDE_SESSION_ALLOWED`**. |
| Opening snapshot / other | `live.py` (~2477+, ~2658+) | Separate `bar_age_sec` checks with config-derived max ages (e.g. `QUOTE_FRESHNESS_THRESHOLD_SEC` combined with **`max(..., 300)`** in places). |

**Stale delayed bars:** If IB returns a bar whose **end timestamp** is ~15–20 minutes behind wall clock, then at revalidation **`bar_age_sec` ≈ 900–1200+** → **fails** the **900 s** default gate for **entries** (when market open). **However:** persisted cohort rows show **`ONE_MIN_BAR_TS` lagging broker `ENTRY_TS` by ~16–17 min** while trades still executed — implying either (a) gate not applied on the path that set `ONE_MIN_BAR_TS`, (b) refresh path returned similarly delayed “latest” bar so age vs **now** was small while **bar end vs truth** was large, and/or (c) different code path at execution vs revalidation. **Phase 4 SQL** (`18`) tests **bar-end vs broker entry** vs **900 s** explicitly.

---

## 5. Freshness thresholds / config (code)

| Key / concept | Default or pattern | Source |
|---------------|-------------------|--------|
| `QUOTE_FRESHNESS_THRESHOLD_SEC` | **900** if null in DB | `live.py` revalidation `coalesce(..., 900)` |
| Portfolio row | `MIP.LIVE.LIVE_PORTFOLIO_CONFIG` | Per `PORTFOLIO_ID` |
| Opening / snapshot paths | `max(int(cfg.get("QUOTE_FRESHNESS_THRESHOLD_SEC") or 60), 300)` in places | `live.py` ~2660, ~2683 |
| `LIVE_OPENING_SNAPSHOT_MAX_AGE_SEC` | Falls back to quote threshold | `live.py` ~2359 |

---

## 6. Verdict: can delayed bars contaminate live decisions?

| Mechanism | Assessment |
|-----------|--------------|
| **Historical snapshot `reqHistoricalData`** | **Yes, plausible:** last bar may be **end-of-delayed-minute** or **non-streaming**; no type flag consumed. |
| **Age gate** | Reduces risk that a **very old** bar end timestamp is used vs **server now** at revalidation; does **not** prove bar is **exchange-time-current**. |
| **Bar end vs broker fill (cohort)** | **Proven lag** in Phase 3/4 SQL: persisted **`ONE_MIN_BAR_TS`** is often **~16–17 min before** broker **`ENTRY_TS`** — **incompatible** with treating that bar as **simultaneous with fill** unless the fill path ignores that gap. |

**Ranked:** **Path + timestamps support** “delayed or non-coincident bar anchor” **hypothesis**; **IB entitlement byte** not observed in code ( **unknown** without TWS session log).
