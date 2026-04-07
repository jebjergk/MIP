# SBUX live bracket — forensic review (Snowflake + `live.py` logic)

**Scope:** Latest (only) `MIP.LIVE.LIVE_ACTIONS` row for `SBUX`, as of query execution against Snowflake.  
**Important:** Parent and bracket legs are **`PENDINGSUBMIT`** with **`QTY_FILLED` / `AVG_FILL_PRICE` null** — there is **no broker fill** in `LIVE_ORDERS` yet. Economics below use **limit / revalidation** prices (the same inputs used to build the bracket in `execute_live_action`).

---

## 1. Identifiers

| Field | Value |
|--------|--------|
| **action_id** | `29bef288-cf20-424b-8aec-f112e9add739` |
| **portfolio_id** | 1 |
| **symbol** | SBUX |
| **action_intent** | ENTRY |
| **side** | BUY |
| **status (action)** | EXECUTION_REQUESTED |

### Broker order ids (`MIP.LIVE.LIVE_ORDERS`)

| Leg (inferred from `ORDER_TYPE` / side) | `BROKER_ORDER_ID` | `ORDER_TYPE` | `LIMIT_PRICE` |
|----------------------------------------|-------------------|--------------|---------------|
| Parent (entry) | **686115298** | LMT | 94.93 |
| Take profit | **686115299** | LMT | 95.55 |
| Stop loss | **686115300** | STP | 94.46 |

`SUBMITTED_AT` cluster: `2026-04-07T20:20:51Z`–`20:20:52Z` (approx.; stored as NTZ in Snowflake).

---

## 2. Quantity, entry, notional (limit / pre-fill)

| Metric | Value |
|--------|--------|
| **Qty ordered** | 2 |
| **Entry reference** | `REVALIDATION_PRICE` = **94.93** (matches parent `LIMIT_PRICE`; `execute_live_action` uses `REVALIDATION_PRICE` ?? `PROPOSED_PRICE`) |
| **Notional (entry × qty)** | **189.86** USD |

**Actual entry fill price / filled qty:** **Not available** — no `FILLED` / `AVG_FILL_PRICE` on the parent leg.

---

## 3. Bracket levels and implied percentages

### 3.1 From committee (`MIP.LIVE.COMMITTEE_VERDICT`, `RUN_ID` = `19079058-7ebb-4d59-a28b-7abefc29f969`)

JSON paths (via Snowflake):

- `VERDICT_JSON:verdict:joint_decision:acceptable_early_exit_target_return` → **0.0065** (fraction)
- `VERDICT_JSON:verdict:joint_decision:realistic_target_return` → **0.005**
- `VERDICT_JSON:verdict:joint_decision:stop_loss_pct` → **0.005**

`execute_live_action` **prefers early-exit target** over realistic return (see `live.py` ~9655–9659), so **effective target_return = 0.0065** (0.65%).

Stop is capped by `LIVE_PORTFOLIO_CONFIG.BUST_PCT` (**0.6**); **0.005** is within cap.

### 3.2 Prices (`execute_live_action` math, BUY)

- `tp_price = entry * (1 + target_return)` → 94.93 × 1.0065 = **95.547045** (ledger payload; IB rounded limit **95.55**)
- `sl_price = entry * (1 - stop_loss_pct)` → 94.93 × 0.995 = **94.45535** (payload; limit **94.46**)

### 3.3 Implied % from submitted limits (sanity check)

- **Target move (TP):** (95.55 / 94.93 − 1) × 100 ≈ **0.654%**
- **Stop distance:** (94.93 − 94.46) / 94.93 × 100 ≈ **0.496%**

---

## 4. Gross dollars to TP / SL (2 shares, limit prices)

| | Formula | Result |
|--|---------|--------|
| **Gross $ to TP** | (95.55 − 94.93) × 2 | **1.24** |
| **Gross $ to SL** | (94.93 − 94.46) × 2 | **0.94** |

---

## 5. Friction assumptions (current live model) and net TP dollars

From `MIP.APP.APP_CONFIG` (via `_load_live_entry_fee_params`):

- `SLIPPAGE_BPS` = 2, `FEE_BPS` = 1, `SPREAD_BPS` = 0

**Fee return floor** (same as `live.py`):

`fee_return_floor = (slippage_bps + fee_bps + spread_bps/2) / 10000` = **0.0003** (3 bps per the code’s single-pass model).

**Default env** (not overridden in APP_CONFIG rows we queried): `LIVE_MIN_NET_TP_BPS` = **5** → **0.0005** additive hurdle in viability check.

**Estimated net $ to TP** (plan-aligned: approximate expected dollar P&amp;L at TP using **notional × (target_return − fee_return_floor)**):

| | Calculation | Result |
|--|-------------|--------|
| Net return fraction (approx.) | 0.0065 − 0.0003 | 0.0062 |
| **Est. net $ at TP** | 0.0062 × 189.86 | **~1.18** |

**Note:** Real IB commissions and **round-trip** friction may be **higher** than this single `fee_return_floor`; the gate in code is **not** a full round-trip dollar model.

---

## 6. Effective R-multiple (code-consistent)

Using joint-decision fractions as in `_live_ib_entry_risk_reason_codes`:

**R = target_return / stop_loss_pct = 0.0065 / 0.005 = 1.30**

(`LIVE_MIN_R_MULTIPLE` default **1.10** → **pass**.)

---

## 7. Exact source fields for the bracket

| Element | Source |
|---------|--------|
| **target_return** | `COMMITTEE_VERDICT.VERDICT_JSON:verdict:joint_decision:acceptable_early_exit_target_return` (else `realistic_target_return`) |
| **stop_loss_pct** | `...joint_decision:stop_loss_pct`, capped by `LIVE_PORTFOLIO_CONFIG.BUST_PCT` |
| **entry_price for math** | `LIVE_ACTIONS.REVALIDATION_PRICE` (else `PROPOSED_PRICE`) |
| **Submitted TP/SL** | `BROKER_EVENT_LEDGER` `EXECUTION_SUBMIT_ATTEMPT` payload: `tp_price`, `sl_price`, `entry_price` |

Authoritative SQL-shaped extraction in router matches lines ~9638–9677 in `live.py`.

---

## 8. Freshness of 1-minute / reference data

### 8.1 Persisted on the action

| Field | Value |
|--------|--------|
| `ONE_MIN_BAR_TS` | **2026-04-07 18:04:00** |
| `ONE_MIN_BAR_CLOSE` | **94.93** (matches `REVALIDATION_PRICE`) |
| `COMMITTEE_COMPLETED_TS` | **2026-04-07 20:19:54** |
| `REVALIDATION_TS` | **2026-04-07 20:20:04** |
| First bracket `SUBMITTED_AT` | **~2026-04-07 20:20:51** |

**Age of bar vs committee:** ~**76 minutes** after bar close to committee completion (bar end 18:04 → committee 20:19). Versus **bar timestamp** as “last bar start”, the reference close is still **~2.3 hours** ahead of submission — **stale / not contemporaneous** for intraday committee context.

Using `_compute_snapshot_freshness_state` semantics from `live.py` (300s threshold for snapshots): if applied to **bar lag vs committee**, this would fall in **BLOCKED** (far beyond 4× threshold). **Note:** that helper is defined for **broker snapshot age**, not `MARKET_BARS`, but the magnitude is the same qualitative conclusion: **not fresh**.

### 8.2 `MIP.MART.MARKET_BARS` (committee context query path)

`_build_action_decision_context` reads the **latest** `MIP.MART.MARKET_BARS` row for the symbol (`INTERVAL_MINUTES = 1`) with **no age filter**.

**Observed in this environment:**

- **No rows** for `SBUX` in `MIP.MART.MARKET_BARS`.
- Table `MAX(TS)` for 1m bars ≈ **2026-03-18** (well before this trade).

So: **either** production committee relied on other paths / caches **or** the mart is incomplete in this account; **in Snowflake as queried, mart cannot validate short-horizon vol or “live” bar freshness for SBUX.**

---

## 9. Short-horizon volatility context

**Not computable** from `MIP.MART.MARKET_BARS` for SBUX in this database (no series).  
**Qualitative:** TP width is **~$0.62/share** (~**0.65%**); without σ we cannot assert a k·σ√τ floor, but dollar risk **&lt;$1** gross on SL highlights **noise/commission scale**.

---

## 10. Rule evaluation

### 10.1 Current live viability (mirrors `_live_ib_entry_risk_reason_codes` + defaults)

| Check | Result |
|-------|--------|
| TP / SL present | **Pass** |
| `target_return` > `fee_return_floor` + `min_net_tp_bps/10000` (0.0003 + 0.0005 = **0.0008**) | 0.0065 > 0.0008 → **Pass** |
| R ≥ `LIVE_MIN_R_MULTIPLE` (1.10) | 1.30 → **Pass** |

**Conclusion:** This bracket **passes** current **percentage-only** IB gates; **quantity does not enter** those checks (`live.py` comment ~1542–1543).

### 10.2 Stronger “minimum bracket realism” (illustrative candidates)

Assume **illustrative** floors often used in desk rules (tune in product): **min gross TP $5**, **min net TP $3**, **min gross risk $1 on SL**, **vol floor** (skip if no σ), **small notional** rule: if notional &lt; **$500**, require gross TP ≥ **$5** or block.

| Candidate rule | SBUX (2 sh, limits above) |
|----------------|----------------------------|
| **Min gross TP $** (e.g. ≥ $5) | **Fail** (~$1.24) |
| **Min net TP $** after modeled friction (e.g. ≥ $3) | **Fail** (~$1.18 est.) |
| **Min stop distance** in $ (e.g. gross SL ≥ $1) | **Fail** (~$0.94) |
| **Volatility-aware floor** | **No data** in mart for SBUX |
| **Small-position sanity** (low notional + tiny gross TP) | **Fail** typical gating |

---

## 11. Final conclusion

| Question | Answer |
|----------|--------|
| **Should this trade have been allowed as-is with actual TP/SL levels under *current* code?** | **Yes** — it satisfies `_live_ib_entry_risk_reason_codes` on percentages and R. |
| **Is live bracket logic too percentage-driven and blind to absolute-dollar realism for small size?** | **Yes** — with **2 shares**, **~$190** notional, gross TP **~$1.24** and gross SL **~$0.94** are economically fragile vs friction **even though** 0.65% / 0.5% / R=1.3 look “reasonable” in ratio form. |
| **Should SBUX have been blocked or only allowed with wider bracket or larger size?** | Under stronger dollar-realism rules: **blocked as sized**, or **allowed only with larger effective size and/or wider targets** so gross TP and gross risk clear sensible minima. |

### Recommendation summary

- **As-is (current rules):** **Allow** (technically consistent with code).
- **Product / risk view:** **Reject or widen / size up** if you require **minimum gross/net dollar edge** on live entries.

---

## 12. Smoke / verification

- **Queries executed:** Read-only `SELECT` against `MIP.LIVE.LIVE_ACTIONS`, `LIVE_ORDERS`, `COMMITTEE_VERDICT`, `BROKER_EVENT_LEDGER`, `APP.APP_CONFIG`, `LIVE.LIVE_PORTFOLIO_CONFIG`, `MART.MARKET_BARS` (summaries + SBUX-specific).
- **No DDL/DML deployed.**

---

*Generated from Snowflake inspection and `MIP/apps/mip_ui_api/app/routers/live.py` bracket/viability logic.*
