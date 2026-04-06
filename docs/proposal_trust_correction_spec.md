# Proposal trust correction spec (strategy hierarchy + symbol-level enforcement)

Minimal-change direction for restoring **long momentum (patterns 2 and 3)** as the primary STOCK/1440 proposal engine, keeping **mean reversion** supporting, and controlling **bearish momentum** unless explicitly enabled. Grounded in forensic findings: global `MIN_HIT_RATE` in `V_TRUSTED_PATTERN_HORIZONS` currently excludes `MOMENTUM`/`MEAN_REVERSION` while `BEARISH_MOMENTUM` can pass; UI/classification trust uses a different path than autonomous proposals.

---

## 1. Recommended correction path (options)

### A. Adjust global `TRAINING_GATE_PARAMS` only (e.g. lower `MIN_HIT_RATE`)

- **Effect:** Widens `V_TRUSTED_PATTERN_HORIZONS` for all families.
- **Blast radius:** **High** — likely admits **MEAN_REVERSION** as a peer proposal engine alongside **MOMENTUM**, conflicting with strategy.
- **Verdict:** **Not recommended** as the sole fix.

### B. Family-aware inclusion in `V_TRUSTED_PATTERN_HORIZONS` (or proposal-only slice)

- **Effect:** e.g. separate `MIN_HIT_RATE` (or bootstrap rules) for `MOMENTUM` vs `MEAN_REVERSION` vs `BEARISH_MOMENTUM`.
- **Blast radius:** **Medium**; single choke view, scoped widening.
- **Verdict:** **Primary technical lever** together with symbol-level rules (§3).

### C. Separate proposal-originating trust from UI/classification trust

- **Effect:** Named proposal eligibility path vs training/UI trust.
- **Blast radius:** **Medium–high** initially; clearer long-term.
- **Verdict:** **Document immediately**; optional second view or thin wrapper later.

### D. Explicit role separation (config: primary vs supporting families)

- **Effect:** Autonomous merge uses **primary** families by default; supporting families feed context/ranking/committee.
- **Blast radius:** **Medium**; touches policy or SP merge inputs.
- **Verdict:** **Combine with B** for enforcement.

---

## 2. Recommended target behavior

- **MOMENTUM (2, 3):** Proposal-eligible again for **STOCK / 1440** via horizons + `V_TRUSTED_SIGNALS_LATEST_TS`; policy already allows `MOMENTUM` in `PROPOSAL_POLICY_RULE`.
- **MEAN_REVERSION:** **Not** the default autonomous proposal-originating family; use for confirmation, caution, turn context, ranking, explanation, committee.
- **BEARISH_MOMENTUM:** Proposal-eligible **only** if explicitly enabled by product (policy + optional trust slice).

---

## 3. Symbol-level enforcement requirement

The proposal architecture must preserve **symbol-focused** decisioning for **MOMENTUM** and **MEAN_REVERSION**.

- **Non-interchangeability:** Two symbols must **not** be treated as interchangeable merely because they share the same **pattern family**. Eligibility and ranking must remain anchored on **(symbol, pattern, market, interval)** evidence, not only on family-level aggregates.

- **Role of family-level trust:** Family-level trust may remain a **coarse allow/deny guardrail** that determines whether a family may participate in the autonomous proposal architecture at all. For **primary-entry** families such as **MOMENTUM**, this guardrail should act as **structural participation control**, not as the **dominant statistical exclusion mechanism**. Once the family is **enabled** for a given market/interval, **symbol-local evidence** should be the **deciding factor** for proposal origination.

- **MOMENTUM (primary entry):** Once the family is allowed at the coarse gate, **proposal-originating eligibility** for momentum should be driven **primarily** by **symbol-local trust / evidence** (for example `V_TRAINING_DIGEST_SNAPSHOT_SYMBOL`, recent outcomes, symbol-local gates in `SP_AGENT_PROPOSE_TRADES`). A **strong symbol-specific case** must **not** be discarded merely because **broader family aggregates** weakened due to **other symbols**.

- **MOMENTUM ranking:** Ranking among **eligible momentum** proposals must also remain **symbol-focused**. Family-level aggregates may inform **broad calibration**, but **final ordering** should be based on the **strength, trust, and evidence** of the **specific symbol’s** signal.

- **MEAN_REVERSION (supporting / context):** Symbol-level MR signals should remain **contextual and supportive** for the **same symbol** (turn/caution/confirmation, **ranking support**, explanation, committee context). They must **not** become **autonomous proposal drivers** through **broad family-level gating** alone. Default policy should keep MR **out of the autonomous merge** unless **explicitly** enabled by product configuration.

**Implementation implication:** Avoid fixing only a **global** family hit-rate bar without (1) keeping **symbol-local gates decisive** for **which symbols** get momentum proposals, and (2) preventing MR from gaining **autonomous origination by default** when thresholds are relaxed.

---

## 4. Decision on MR role

**Recommendation:** **Never direct proposal-originating for current design** — MR stays a **support-only context layer** (and optional future ranking/confidence modifier), not rows that drive autonomous `ORDER_PROPOSALS` unless a **separate explicit policy** enables it.

Enforce via default **`PROPOSAL_POLICY_RULE`** (e.g. `MEAN_REVERSION` not eligible for autonomous) **and/or** excluding MR from the proposal trust slice while still joining MR for diagnostics/briefs.

---

## 5. Decision on trust meanings

**Recommendation:** **Keep multiple meanings; rename and document clearly** (e.g. **Trusted (UI/training)** vs **Eligible (autonomous proposals)** vs **Symbol-local trust**). Full unification is possible later but has high blast radius.

---

## 6. Concrete implementation recommendation (first objects)

1. **`V_TRUSTED_PATTERN_HORIZONS`** ([`MIP/SQL/mart/036_mart_trusted_gate_views.sql`](../SQL/mart/036_mart_trusted_gate_views.sql)) + **`TRAINING_GATE_PARAMS`** / scoped config: **family-specific** thresholds so **`MOMENTUM`** can pass **without** automatically passing **`MEAN_REVERSION`** or **`BEARISH_MOMENTUM`** unless configured.
2. **`SP_AGENT_PROPOSE_TRADES`** ([`MIP/SQL/app/188_sp_agent_propose_trades.sql`](../SQL/app/188_sp_agent_propose_trades.sql)): ensure **symbol-local** gate remains **primary** for **who** among allowed-family momentum signals becomes a candidate, and that **final ordering** among momentum candidates stays **symbol-focused** (per §3).
3. **`PROPOSAL_POLICY_RULE`** ([`MIP/SQL/app/491_proposal_policy_pw_schema.sql`](../SQL/app/491_proposal_policy_pw_schema.sql)): default **MR** non-originating; **bearish momentum** off until explicit; new policy version if needed.

---

## 7. Validation plan

- MU-style trace: patterns 2/3 at `latest_ts` appear in `V_TRUSTED_SIGNALS_LATEST_TS` when family + symbol-local gates pass.
- Confirm patterns **2** and **3** can enter proposal candidates and produce `ORDER_PROPOSALS` over fresh runs.
- Confirm **momentum ranking** among multiple eligible symbols reflects **symbol-level** score/trust/evidence (not family-only ordering).
- Confirm **MR** does not originate autonomous proposals under default policy; remains available for context joins.
- Confirm **bearish momentum** is not silently “globally trusted then policy-blocked” without documented intent.
- Proposal counts by `PATTERN_ID` 2/3 over 7–14 days; snapshot `distinct PATTERN_TYPE` in trusted-latest and in new proposals — no unintended families.
- Smoke: [`MIP/SQL/smoke/triage_trusted_signals_pipeline.sql`](../SQL/smoke/triage_trusted_signals_pipeline.sql), [`short_momentum_foundation_smoke.sql`](../SQL/smoke/short_momentum_foundation_smoke.sql), [`14_proposal_policy_pw_smoke.sql`](../SQL/smoke/14_proposal_policy_pw_smoke.sql).

---

## 8. Required conclusion (single recommendation)

**Best minimal correction:** Implement **family-aware** horizon gating (not a global `MIN_HIT_RATE` cut alone) so **`MOMENTUM`** re-enters the proposal path for STOCK/1440, with family-level trust as **participation control** rather than the **dominant** statistical veto; pair with **default policy** that keeps **MR** and **bearish momentum** from autonomous origination unless explicitly enabled; **preserve symbol-local enforcement** as the **deciding** factor for origination and keep **momentum ranking symbol-focused** (§3). Document **two (or three) named trust concepts** for UI vs autonomous eligibility vs symbol-local.

---

## Implementation record (deployed)

| Object | Role |
|--------|------|
| `MIP.APP.AUTONOMOUS_TRUST_FAMILY_GATE` | Config table: `MOMENTUM` × STOCK/ETF/FX × 1440 with `MIN_HIT_RATE=0.52`, `MIN_AVG_RETURN=0.0005`. No rows for MR/bearish → excluded from autonomous slice. |
| `MIP.MART.V_AUTONOMOUS_PROPOSAL_TRUSTED_PATTERN_HORIZONS` | Leaderboard rows matching gate keys + thresholds. |
| `MIP.MART.V_TRUSTED_SIGNALS_LATEST_TS` | `trusted_ph` now sourced from autonomous view (not `V_TRUSTED_PATTERN_HORIZONS`). |
| `MIP.MART.V_TRUSTED_PATTERN_HORIZONS` | Unchanged global bar for training / intraday / briefs. |
| `PROPOSAL_POLICY_MANIFEST` / `PROPOSAL_POLICY_RULE` | **`2026_04_07_V2`** default: `MOMENTUM` eligible, `MEAN_REVERSION` all directions ineligible for autonomous, `BEARISH_MOMENTUM` off. |
| `SP_AGENT_PROPOSE_TRADES` | Counts autonomous horizons; fallback policy version `2026_04_07_V2`. |

**Files:** [`492_autonomous_trust_family_gate.sql`](../SQL/app/492_autonomous_trust_family_gate.sql), [`036_mart_trusted_gate_views.sql`](../SQL/mart/036_mart_trusted_gate_views.sql), [`493_proposal_policy_2026_04_07_v2.sql`](../SQL/app/493_proposal_policy_2026_04_07_v2.sql), [`188_sp_agent_propose_trades.sql`](../SQL/app/188_sp_agent_propose_trades.sql), [`15_autonomous_trust_momentum_smoke.sql`](../SQL/smoke/15_autonomous_trust_momentum_smoke.sql), grants in [`02_grants_readonly.sql`](../SQL/deploy/ux_api_user/02_grants_readonly.sql).

**Note:** Do not put unescaped `;` inside string literals in SQL files run by `query_snowflake.py` statement splitter (use commas in descriptions).
