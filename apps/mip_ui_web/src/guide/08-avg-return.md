# 8. What Is Avg Return?

Avg return answers: **"On average, how much money did this pattern make (or lose) per signal?"**

While hit rate measures *accuracy* (how often), avg return measures *profitability* (how much). Both are needed — a pattern could be right 60% of the time but still lose money if the losses are bigger than the wins.

## How It's Calculated

```
Avg Return = sum of all realized_return values ÷ number of evaluated outcomes
```

## Detailed Example

AUD/USD generated 40 signals. Here are the 5-bar returns for 8 of them:

| Signal | 5-bar return | Hit? |
|--------|-------------|------|
| #1 | +1.20% | Yes |
| #2 | +0.50% | Yes |
| #3 | -0.30% | No |
| #4 | +0.80% | Yes |
| #5 | +2.10% | Yes |
| #6 | -0.50% | No |
| #7 | +0.30% | Yes |
| #8 | +1.50% | Yes |

Average across all 40 outcomes: **+0.81%** per signal.

A value of 0.0081 (0.81%) is well above the 0.0005 (0.05%) threshold. Even though some signals lost money (-0.30%, -0.50%), the average is solidly positive.
