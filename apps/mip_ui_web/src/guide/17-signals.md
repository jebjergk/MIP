# 17. Signals Explorer

Browse raw signal data — every detection the system has made. Use this page to see exactly which signals were generated today, their trust labels, and whether they're eligible for trading.

## Filters

You can filter signals by symbol, market type, pattern, horizon, pipeline run ID, timestamp, and trust label. Filters can be set via URL parameters (often linked from other pages) or manually adjusted on this page.

## Signals Table — Every Column Explained

| Column | What it means | Example |
|--------|---------------|---------|
| **Symbol** | Which asset generated this signal | EUR/USD |
| **Market** | Asset class | FX |
| **Pattern** | Which pattern definition detected this signal | 2 |
| **Score** | The signal strength — typically the observed return at the moment of detection. Higher = stronger detection. | 0.0035 (meaning +0.35%) |
| **Trust** | Current trust label for this symbol/pattern combination. TRUSTED (green) = can generate proposals. WATCH (orange) = monitoring. UNTRUSTED (red) = not eligible. | TRUSTED |
| **Action** | The recommended action — typically BUY or SELL | BUY |
| **Eligible** | Whether this signal can become a trade proposal. ✓ = eligible (trusted pattern, risk gate allows). If not eligible, shows the gating reason (e.g., "NOT_TRUSTED", "Z_SCORE_BELOW_THRESHOLD"). | ✓ or "Z_SCORE_BELOW_THRESHOLD" |
| **Signal Time** | When the signal was generated | 2026-02-07 14:30 |

## Example: Reading the Signals Table

You see a row: AUD/USD | FX | Pattern 2 | Score 0.0035 | TRUSTED | BUY | ✓ | 2026-02-07

This means: Today, the FX_MOMENTUM_DAILY pattern detected a +0.35% move in AUD/USD. The pattern is TRUSTED (it has a proven track record). The signal IS eligible to become a trade proposal (✓). If the risk gate allows and the portfolio has capacity, this could result in a BUY order for AUD/USD.

## Fallback Banner

If no signals match your filters for the current run, the system automatically tries a broader search (fallback). A yellow banner appears explaining what happened and offering actions: "Clear all filters," "Use latest run," or "Back to Cockpit."
