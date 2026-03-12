# 1. The Big Picture

MIP is an automated market intelligence platform.

It does three core jobs:
- notice repeatable market behaviors,
- test whether they worked historically,
- act only when evidence is strong enough.

**MIP does not guess tomorrow's exact price.**  
It asks: "When this setup happened before, what usually happened next?"

## The MIP Pipeline at a Glance

```
Market Data -> Signal Detection -> Outcome Evaluation -> Trust Decision -> Trade Proposals
```

1. **Market Data** — fresh bars arrive.
2. **Signal Detection** — patterns detect setups.
3. **Outcome Evaluation** — old signals are scored against what actually happened.
4. **Trust Decision** — evidence is weighted into readiness.
5. **Trade Proposals** — only eligible setups move toward execution.

MIP runs **two pipelines**:
- **Daily pipeline** for multi-day behavior.
- **Intraday pipeline** for shorter-horizon behavior.

Both use the same principle: **learn first, then trade**.

## Real-World Analogy

Think of MIP like hiring a weather forecaster:

- Day 1: you do not trust them yet.
- After many forecasts: you compare forecast vs reality.
- If they stay reliable over many observations: you start trusting them.

That is exactly how MIP treats each pattern/symbol pair.
