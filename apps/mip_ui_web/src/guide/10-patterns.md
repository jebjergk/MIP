# 10. What Are Patterns?

A **pattern** is a named signal strategy with specific parameters. Each pattern defines what the system looks for in market data. MIP can run multiple patterns simultaneously, each targeting different market types and time scales.

## Pattern Examples

| Pattern | Market | fast_window | slow_window | What It Actually Requires | Min Return | Min Z-Score |
|---------|--------|-------------|-------------|---------------------------|------------|-------------|
| **FX_MOMENTUM_DAILY** | FX | 10 | 20 | Price above 10-bar MA *and* 20-bar MA (traditional crossover) | 0.1% | 0 (any) |
| **STOCK_MOMENTUM_FAST** | STOCK | 5 | 1 | 1 prior green day + 5-day high breakout + z-score ≥ 1.0 | 0.2% | 1.0 |
| **ETF_MOMENTUM_DAILY** | ETF | 20 | 3 | 3 consecutive green days + 20-day high breakout + z-score ≥ 1.0 | 0.2% | 1.0 |

**FX_MOMENTUM_DAILY** uses the traditional moving-average approach (less selective: z-score = 0, any positive move above 0.1% fires).

**STOCK_MOMENTUM_FAST** requires a 5-day high breakout with at least 1 prior green day — achievable and fires regularly.

**ETF_MOMENTUM_DAILY** is paradoxically the *most demanding* pattern: it requires 3 consecutive green days AND a 20-day high breakout AND z-score ≥ 1.0. This is extremely selective, which is why ETF signals fire less frequently despite targeting less volatile instruments.

Each symbol is evaluated *per pattern*. So AAPL might be CONFIDENT under STOCK_MOMENTUM_FAST but INSUFFICIENT under a different pattern. Trust is also earned per pattern — the system judges each strategy independently.

> **Patterns don't change automatically.** The AI narratives in the Cockpit describe pattern behavior but never change pattern parameters. Only the system operator can modify pattern definitions. The AI is strictly observational — it reports, it doesn't act.
