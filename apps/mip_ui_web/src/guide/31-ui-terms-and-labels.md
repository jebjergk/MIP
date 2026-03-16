# 31. UI Terms and Labels Reference

This section defines common labels shown in MIP pages so Ask MIP can explain them consistently.

## Symbol Tracker terms

- **Thesis**: current validity of the trade idea (`THESIS_INTACT`, `WEAKENING`, `INVALIDATED`).
- **Open R**: current open reward-to-risk multiple from entry relative to stop distance.
- **Expected move reached**: realized move divided by trained expected move.
- **Distance to TP**: side-aware distance from current price to take-profit.
- **Distance to SL**: side-aware safety buffer from current price to stop-loss.
- **Bars since entry**: number of chart bars elapsed since entry timestamp.
- **Vol regime**: comparison of live volatility to trained volatility context.

## Decision/committee terms

- **Committee stance**: synthesized high-level posture (`THESIS_INTACT`, `WATCH_CLOSELY`, `ESCALATE`).
- **Committee confidence**: confidence tier (`LOW`, `MEDIUM`, `HIGH`).
- **Reason tags**: standardized reason codes explaining why a stance was produced.
- **Actions to consider**: ordered, non-binding action suggestions based on current evidence.

## Live workflow terms

- **Live Portfolio Link**: configuration/control state for live-linked paper workflow.
- **Live Portfolio Activity**: event stream of validation/execution lifecycle changes.
- **Learning Ledger**: decision-to-outcome trace used for post-trade learning and accountability.

## Runs and freshness terms

- **Run status**: execution status for pipeline runs (`SUCCESS`, `FAILED`, `RUNNING`).
- **Freshness**: recency indicator of data/snapshot relative to current time.

## Interpretation guidance

- Labels are diagnostics, not guarantees.
- Metrics should be interpreted with route context and current mode (daily/intraday).
- For live values, users should verify directly in the corresponding page table/panel.
