# Autonomy Contracts (Action Surface)

This document defines the canonical contract for autonomous decisioning in MIP.
All agents, sims, briefs, and dashboards **must** treat
`MIP.APP.V_SIGNALS_ELIGIBLE_TODAY` as the official action surface.

## Canonical view: `MIP.APP.V_SIGNALS_ELIGIBLE_TODAY`

This view is the source-of-truth for eligibility decisions. Downstream systems
should read eligibility, trust labeling, and gating reasons **only** from this
view.

### Columns + semantics

| Column | Type | Semantics |
| --- | --- | --- |
| `RUN_ID` | string | Deterministic run identifier for a daily signal batch. Derived from recommendation log metadata; used for downstream joins. |
| `RECOMMENDATION_ID` | number | Unique recommendation identifier from `MIP.APP.RECOMMENDATION_LOG`. |
| `TS` | timestamp_ntz | Signal timestamp (event time). |
| `SYMBOL` | string | Asset identifier. |
| `MARKET_TYPE` | string | Market category (e.g., `STOCK`, `FX`). |
| `INTERVAL_MINUTES` | number | Bar interval for the signal. |
| `PATTERN_ID` | number | Pattern identifier tied to the recommendation. |
| `SCORE` | number | Model score for the recommendation. |
| `DETAILS` | variant | Raw recommendation metadata for audit/trace. |
| `TRUST_LABEL` | string | Trust status derived from policy (e.g., `TRUSTED`, `WATCH`, `BLOCK`). |
| `RECOMMENDED_ACTION` | string | Policy action associated with the trust label (`ENABLE`/`DISABLE`). |
| `IS_ELIGIBLE` | boolean | Final eligibility decision (`true` if trusted + enabled). |
| `GATING_REASON` | variant | Policy-derived rationale for eligibility or rejection. |

### Allowed consumer views (MART)

Downstream consumers should use these MART-layer views (no direct APP table
access):

* `MIP.MART.V_SIGNALS_TODAY` — dashboard-friendly wrapper for current eligible signals.
* `MIP.MART.V_PORTFOLIO_SIGNALS` — portfolio simulation input view (trusted signals only).

### Join keys

The canonical join keys for cross-system linkage are:

```
SYMBOL, MARKET_TYPE, INTERVAL_MINUTES, TS, PATTERN_ID
```

When correlating proposals, trades, or analyses back to eligibility decisions,
**all** join keys above must match.

### Lookahead-safe requirement

Consumers must treat `V_SIGNALS_ELIGIBLE_TODAY` as **lookahead-safe**:

* No downstream process should use future information (e.g., future bars or
  outcomes) to alter eligibility for the same day.
* Eligibility decisions are **locked to the signal timestamp** and the policy
  applied at that time.
* Dashboards and agents may filter or rank eligible signals, but **must not**
  override `IS_ELIGIBLE`.

