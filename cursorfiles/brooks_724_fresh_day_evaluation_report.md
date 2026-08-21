# Brooks Intraday Adviser — 2026-07-24 Fresh-Day Evaluation Pack

**Method:** Read-only forensic on completed sessions. ACE-Lite V1.1 / Entry Synthesis patch 2. No reruns, no tuning.

## Attempt IDs

- **CRM** `f76c5f27-b3dd-436e-8935-31520a2c1b21` (31 calls, COMPLETED)
- **MSFT** `88c67578-d99f-4518-b756-ebe7db3a94ac` (34 calls, COMPLETED)
- **NKE** `bcf1aaf0-d700-4cc4-9a1b-93d0eadbe135` (33 calls, COMPLETED)
- **NVDA** `8549c812-5f3d-494b-8d3c-7dd3d0d8e24f` (36 calls, COMPLETED)

---

## 1. Executive Scorecard

### CRM

| Field | Value |
|-------|-------|
| Executed trades | **0** |
| Gross P&L | $0.00 |
| CONSIDER_ENTRY opps | 3 (all blocked) |
| — 11:10 | NOT_ACTED_MARGINAL / MARGINAL / Q=MARGINAL / W=TRADE_WORTHY |
| — 11:20 | NOT_ACTED_MARGINAL / MARGINAL / Q=MARGINAL / W=TRADE_WORTHY |
| — 12:20 | NOT_ACTED_VALID_BUT_PASS / VALID_BUT_PASS / Q=STRONG / W=VALID_BUT_PASS |

### MSFT

| Field | Value |
|-------|-------|
| Executed trades | **1** |
| Entry | 13:15 @ 384.29 |
| Exit | 13:20 @ 383.96 |
| Gross P&L | $-0.66 |
| Est. fees ($1/order RT) | $2.00 |
| Net P&L | $-2.66 |
| Initial structural stop | 383.55 |
| Exit reason | THESIS_FAILURE / PATTERN_BASED (failed immediate follow-through; swing_break_low) |
| Trade intent | SWING |
| Entry quality | STRONG |
| Trade-worthiness | TRADE_WORTHY |

### NKE

| Field | Value |
|-------|-------|
| Executed trades | **1** |
| Entry | 14:45 @ 41.93 |
| Exit | 15:55 @ 41.6 |
| Gross P&L | $-7.59 |
| Est. fees ($1/order RT) | $2.00 |
| Net P&L | $-9.59 |
| Initial structural stop | 41.76 |
| Exit reason | THESIS_FAILURE / STRUCTURAL_BREACH (close below pullback low 41.69) |
| Trade intent | SWING |
| Entry quality | ACCEPTABLE |
| Trade-worthiness | TRADE_WORTHY |

### NVDA

| Field | Value |
|-------|-------|
| Executed trades | **0** |
| Gross P&L | $0.00 |
| CONSIDER_ENTRY opps | 5 (all blocked) |
| — 10:50 | NOT_ACTED_MARGINAL / MARGINAL / Q=MARGINAL / W=TRADE_WORTHY |
| — 10:55 | NOT_ACTED_MARGINAL / MARGINAL / Q=MARGINAL / W=TRADE_WORTHY |
| — 12:00 | NOT_ACTED_MARGINAL / MARGINAL / Q=MARGINAL / W=TRADE_WORTHY |
| — 12:05 | NOT_ACTED_MARGINAL / MARGINAL / Q=MARGINAL / W=TRADE_WORTHY |
| — 15:40 | NOT_ACTED_MARGINAL / MARGINAL / Q=MARGINAL / W=VALID_BUT_PASS |

### Aggregate (executed trades only)

| Metric | Value |
|--------|-------|
| Trades | 2 |
| Wins / Losses | 0 / 2 |
| Gross P&L | $-8.25 |
| Total est. fees | $4.00 |
| Net P&L (@ $1/order) | $-12.25 |
| Avg loser (gross) | $-4.12 |
| Payoff ratio | N/A (no winners) |
| Profit factor | 0 (no gross wins) |

*Gross performance is reported separately from $1,000-account fee economics.*

---

## 2. Decision-Time Brooks Evaluation (executed trades)

### MSFT @ 13:15 — **BROOKS_STRONG**

Coherent cycle, confirmation, location, and equation at decision time.

**MARKET CYCLE:** Intraday session is in TRADING_RANGE with neutral always-in bias. Session high is 389.03, session low is 380.65, pullback low is 383.49. Price is currently 384.29, near the middle of the range. The cycle is characterized by two-sided trading (4 bull bars, 4 bear bars in recent 8-bar window) with tight overlap and no directional breakout. This is a mature range environment where pullback-to-support entries are appropriate Brooks setups.

**ALWAYS-IN / CONTROL:** Always-in bias is NEUTRAL in both slow_context_regime and intraday_regime. This means no directional bias from higher timeframe; trade only with intraday structure. The pullback-to-support setup is valid in neutral bias because it is a reversal structure (signal bar breakout + pullback hold), not a trend-following entry. Neutral bias does not prohibit reversals; it requires stronger intraday evidence, which is present (signal bar + pullback confirmation).

**LOCATION:** Price is 384.29, located 4.74 points below session high (389.03) and 3.64 points above session low (380.65). Support cluster (381.71–382.27) is 0.11% below current price (very close). This location is ideal for pullback-to-support entry: price has pulled back from swing high (383.49) to support cluster and is holding. Room to upside is ~4.74 points to session high; room to downside (stop) is ~2.58 points to support cluster low (381.71). Risk-reward ratio is approximately 1:1.8 (favorable for Bro

**OPPOSING EVIDENCE:** Recent bar evidence shows tight overlap and alternating bull/bear bars, indicating indecision and range-bound behavior. The 13:15 bar is small (body_fraction 0.0417, range 0.24), suggesting weak follow-through after the strong 13:10 signal bar. This could indicate exhaustion or consolidation rather than strong bull continuation. However, the small bar is consistent with pullback behavior in a range; it does not invalidate the pullback structure. Daily PAA is NO_CLEAR_LONG, which is cautious cont

**TRADER'S EQUATION:** Probability: Signal bar (13:10) is a strong bull breakout with high body_fraction (0.7544) and close_location (0.9825), indicating conviction. Pullback hold (13:15) confirms reversal intent. In a trading range with support cluster hold, probability of upside continuation is moderate-to-good (pullback-to-support is a high-probability Brooks setup). Reward: Room to session high is ~4.74 points; room to measured move or next resistance is potentially higher. Risk: Stop below support cluster (381.71

**SETUP:** This is a valid Brooks BREAKOUT_PULLBACK setup (pullback-to-support after breakout of swing high). Signal bar (13:10) is a strong bull breakout above swing high (383.49) with swing_break_high flag. Pullback leg (13:15) holds above support cluster (381.71–382.27) with bullish close. The setup is mechanically sound: breakout + pullback hold = reversal confirmation. Setup quality is ACCEPTABLE (not STRONG because of small 13:15 bar and tight range context, but valid and executable).

**CONFIRMATION:** Mandatory confirmation contract is SATISFIED. PULLBACK_LEG_COMPLETED predicate is satisfied at 13:15 bar (price pulled back to support cluster 381.71–382.27 and held with bullish close 384.29). No additional confirmations are pending. Entry is ready.

**TRADE-WORTHINESS:** TRADE_WORTHY

### NKE @ 14:45 — **BROOKS_REASONABLE**

Valid Brooks structure with acceptable but non-standout quality; marginal R:R acknowledged at entry.

**MARKET CYCLE:** Intraday market is in STRONG_BULL_TREND with always-in bias LONG. Session high is 42.15, session low 40.88, pullback low 41.69. Price has broken above prior swing highs (41.85, 42.00) and is consolidating within the bull flag structure. Cycle is mature bull trend transitioning into consolidation/pullback-hold phase—typical setup for breakout-pullback entry.

**ALWAYS-IN / CONTROL:** Intraday always-in bias is LONG. Slow context regime (STRONG_BULL_TREND) and intraday regime (STRONG_BULL_TREND) both support long bias. No conflicting always-in signals. Entry is aligned with trend direction.

**LOCATION:** Price is at 41.93, above pullback low (41.69), above session open (41.19), and within the bull flag consolidation zone (41.82–41.97). Location is within the pullback-hold structure, not at an extreme or exhaustion level. Room to session high (42.15) is +0.22 (0.53% upside). Room to prior swing high (41.85) is already exceeded. Location is acceptable for entry; price is not at a major resistance or exhaustion point.

**OPPOSING EVIDENCE:** Current bar (14:45) is bearish (close 41.93, body fraction 0.33, range 0.09) and closes below the signal bar (14:40, close 41.96). This is a pullback/consolidation bar, not a follow-through bar. Mechanical summary shows only 4 bull bars in 8 bars; recent consecutive bull bars max is 2 (14:35–14:40). No new session high or swing break high on current bar. Daily PAA is NO_CLEAR_LONG, indicating daily context is cautious. However, the current bar is a normal consolidation bar within the pullback-ho

**TRADER'S EQUATION:** Probability: Pullback-hold structure in strong bull trend has moderate-to-good probability of follow-through. Signal bar (14:40) was bullish and closed above prior swing high; pullback low held and reversed upward. Structural probability is favorable. Risk: Stop loss would be placed below pullback low (41.69) or intraday swing low (41.64), approximately 0.24–0.29 risk from entry (41.93). Reward: Upside target is session high (42.15) or higher, approximately +0.22 upside (0.53% reward). Reward-to

**SETUP:** Setup is a valid Brooks breakout-pullback structure: bull flag breakout (14:35 high 41.92, 14:40 high 42.00) followed by pullback consolidation (14:30–14:45 range 41.82–41.97) with pullback low held (41.69). Signal bar (14:40) is bullish, closes above prior swing high (41.85), and establishes a bull signal. Pullback low (41.69) is held and reversed upward at bar 63 (14:40 close 41.96 > 41.69). Setup is ACCEPTABLE quality: valid structure, confirmed pullback-hold, but current bar is a consolidati

**CONFIRMATION:** Mandatory confirmation contract is satisfied: PULLBACK_LEG_COMPLETED predicate is satisfied at bar 63 (14:40 close 41.96 > pullback low 41.69). No additional confirmations are pending on this bar. Confirmation is complete and frozen. Entry is actionable on this bar (14:45).

**TRADE-WORTHINESS:** TRADE_WORTHY

---

## 3. Missed Opportunity Review

### NKE — 10:20–11:25 (MISSED_BUT_DEBATABLE)

- **Structure:** Morning bull leg from ~40.88 toward session high; two-legged breakout-pullback-follow-through.
- **Cycle/control recognized:** True
- **WATCH/ARM:** ARM_LONG 10:30, 10:50
- **CONSIDER_ENTRY:** 10:55, 11:10
- **Blocker:** quality (MARGINAL: near_session_high_extension; 10:55 also trading_range_without_textbook_structure)
- **Brooks-coherent pass:** True
- **Note:** Small absolute move; buying at session-high extension in sub-$1 range is a defensible Brooks pass despite strong mechanical setup.

### NVDA — 10:50–12:05 (LEGITIMATE_PASS)

- **Structure:** Range breakout then strong intraday bull trend to 211+.
- **Cycle/control recognized:** True
- **WATCH/ARM:** Multiple ARM cycles
- **CONSIDER_ENTRY:** 10:50, 10:55, 12:00, 12:05
- **Blocker:** quality (MARGINAL: near session high / extension entries)
- **Brooks-coherent pass:** True
- **Note:** Repeated extension-at-high CONSIDER_ENTRY blocked by quality gate; consistent selective discipline, not a missed structural entry at support.

### CRM — 11:00–12:00 (LEGITIMATE_PASS)

- **Structure:** Strong bull trend to new session highs.
- **Cycle/control recognized:** True
- **WATCH/ARM:** True
- **CONSIDER_ENTRY:** 11:10, 11:20, 12:20 (VALID_BUT_PASS)
- **Blocker:** quality MARGINAL or synthesis VALID_BUT_PASS
- **Brooks-coherent pass:** True
- **Note:** 

---

## 4. NKE 2026-07-24 Special Review

| Bar | Action | Wake |
|-----|--------|------|
| 10:30 | ARM_LONG | PRICE_ACTION_CHARACTER_CHANGE |
| 10:50 | ARM_LONG | THESIS_WATCH_CONDITION_MET |
| 10:55 | CONSIDER_ENTRY | PRICE_ACTION_CHARACTER_CHANGE |
| 11:10 | CONSIDER_ENTRY | PRICE_ACTION_CHARACTER_CHANGE |
| 11:30 | HOLD | PRICE_ACTION_CHARACTER_CHANGE |
| 11:40 | INVALIDATE | PRICE_ACTION_CHARACTER_CHANGE |
| 14:40 | ARM_LONG | PRICE_ACTION_CHARACTER_CHANGE |
| 14:45 | CONSIDER_ENTRY | CONFIRMATION_COMPLETE |

**Did the Adviser correctly recognize bull control during the morning move?**
Mostly yes — regime progressed TRADING_RANGE → INTRADAY_BULL_BREAKOUT → STRONG_BULL_TREND by 11:10; ARM and CONSIDER_ENTRY fired.

**Why were 10:55 and 11:10 not executed?**
NOT_ACTED_MARGINAL — brooks_entry_quality MARGINAL despite TRADE_WORTHY synthesis (near_session_high_extension).

**Were those passes Brooks-coherent given move size/location?**
Yes — near-session-high extension in a ~$0.60 range day; quality MARGINAL is a defensible Brooks filter.

**Was the 14:45 trade methodologically better, worse, or simply different?**
Different and modestly better methodologically: afternoon 14:45 was pullback-hold in STRONG_BULL_TREND (ACCEPTABLE, not MARGINAL), not session-high extension chase.

**ACE-Lite continuity?**
No material continuity break; 11:40 INVALIDATE on trend weakening was lifecycle-coherent; afternoon re-arm was a new setup family.

The −$7.59 outcome does not, by itself, indicate a defect.

---

## 5. Exit / Risk Management Review

- **MSFT** (13:15→13:20, gross $-0.66): **GOOD_CONTROLLED_LOSS** — Loss contained; exit on thesis failure with structural/pattern basis, not runaway hold.
- **NKE** (14:45→15:55, gross $-7.59): **GOOD_CONTROLLED_LOSS** — Loss contained; exit on thesis failure with structural/pattern basis, not runaway hold.

Positive prior behavior retained: failed opportunities did not become large losses.

---

## 6. Reproducibility / Architecture Health

**NONE (no engineering reopen warranted):**
- No loss of market-cycle continuity requiring engineering reopen.
- No PAA veto of executed entries.
- No invented probability/R thresholds blocking trades.
- No execution/persistence anomalies on completed sessions.

**OBSERVATION (monitor):**
- NKE: ADVISER_MANAGEMENT_EVIDENCE_MISMATCH on HOLD bars (narrative 'consecutive_bears' vs packet max_consecutive_bear_since_entry ≤1) — non-blocking.
- NKE exit: thesis_failure_reference_bar_not_found warning — exit still structurally supported.
- Multiple SETUP_CONTRACT_REJECTED events (unsupported predicate types) — normal contract hygiene, setups re-armed successfully.
- Quality vs synthesis split (TRADE_WORTHY + MARGINAL quality → NOT_ACTED) — by design on 7/24.
- PAA NO_CLEAR_LONG present all day; daily_bias_role=higher_timeframe_context_only — not acting as veto.
- NKE 10:55 synthesis market_cycle still TRADING_RANGE while call state INTRADAY_BULL_BREAKOUT — minor cycle label lag.

**MATERIAL_DEFECT:** None identified from 7/24 persisted calls.

---

## 7. Fresh-Day Verdict: **AMBER — CONTINUE, BUT WATCH**

| Question | Answer |
|----------|--------|
| Positive gross edge on 7/24? | No — 2 trades, 0 wins, gross −$8.25. |
| Contained failed trades? | Yes — MSFT −$0.66 in 5 minutes on thesis failure; NKE −$7.59 exited on structural breach, not open-ended hold. |
| Material Brooks misses (methodology)? | No clear MATERIAL_MISSED — NKE morning blocked on extension quality is debatable but Brooks-coherent. |
| Defect serious enough to stop validation? | No material repeated defect observed. |
| Next fresh block without system changes? | Yes — proceed to next fresh validation block; monitor extension-entry quality gate behavior. |

Machine-readable JSON: `brooks_724_fresh_day_evaluation_report.json`