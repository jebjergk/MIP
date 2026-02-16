# 7. What Is Hit Rate?

Hit rate answers: **"When this pattern said 'go,' how often did the price actually move favorably?"**

## How It's Calculated

```
Hit Rate = HIT_COUNT ÷ SUCCESS_COUNT
```

- **SUCCESS_COUNT** — The number of outcomes that were successfully evaluated — the system was able to check what happened after the signal fired. For example, if 40 signals have been generated and all 40 have a known outcome at the 5-bar horizon, SUCCESS_COUNT = 40.

- **HIT_COUNT** — Of those, how many had the price move above the minimum threshold in the right direction. If 30 out of 40 outcomes showed a positive return exceeding the pattern's minimum, HIT_COUNT = 30.

## Detailed Example

AUD/USD has been tracked for 3 months. In that time, the FX_MOMENTUM_DAILY pattern fired 40 times. The system evaluated what happened 5 bars (1 week) after each signal:

- Signal #1: AUD went up +0.5% → **HIT** (above threshold)
- Signal #2: AUD went down -0.2% → **MISS**
- Signal #3: AUD went up +1.1% → **HIT**
- ... and so on for all 40 signals ...

Final count: 30 hits out of 40 total evaluations.
**Hit Rate = 30 / 40 = 0.75 (75%)**

The system requires at least 55%. At 75%, AUD/USD passes this gate comfortably — meaning 3 out of 4 times the pattern fired, the price moved favorably.
