# 1. The Big Picture

MIP is an automated market intelligence platform. It watches markets every day, detects interesting price movements, evaluates whether those patterns historically lead to profitable outcomes, and — only when confident — proposes trades.

**MIP does not predict tomorrow's price.** Instead, it asks: "When this type of price action happened in the past, what usually followed?" It builds evidence over time and only acts when the evidence is strong enough.

## The MIP Pipeline at a Glance

```
Market Data → Signal Detection → Training → Trust Decision → Trade Proposals
```

1. **Market Data** — Fresh price bars arrive daily
2. **Signal Detection** — Patterns scan for notable moves
3. **Training** — Evaluate past signals, build evidence
4. **Trust Decision** — Enough evidence? Earn trust status
5. **Trade Proposals** — Only trusted patterns can trade

## Real-World Analogy

Imagine you're hiring a weather forecaster. You wouldn't trust someone on day one. You'd watch them make predictions over weeks. After 40+ predictions, if they were right 75% of the time, you'd start to trust them. That's exactly how MIP works — it watches its own pattern detections, checks what actually happened, and only trusts patterns with a proven track record.
