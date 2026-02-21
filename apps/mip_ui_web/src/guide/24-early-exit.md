# 24. Intraday Early Exit

## The Problem: Winning Trades That Give Back Their Gains

Imagine you buy a stock in the morning because MIP detected a strong momentum signal. By lunchtime, the stock has already hit your target return — mission accomplished. But MIP's plan says "hold for 5 bars," so it holds. By the end of the day, the stock reverses and the gains evaporate. You had the win in hand and gave it back.

This is **giveback risk** — one of the most frustrating (and common) phenomena in systematic trading. The early-exit layer is MIP's answer.

## How It Works: A Two-Stage Safety Net

The early-exit system doesn't just grab profits the moment they appear. That would be like leaving a party the instant you're having fun — you'd miss the best parts. Instead, it uses a **two-stage decision**:

### Stage A: "Has the payoff arrived?"

Every time new 15-minute bars come in, MIP checks each open daily position:

- What's the **current return** since entry?
- What's the **target return** for this position's pattern?
- Has the return reached or exceeded the target?

If yes, the position becomes an **early-exit candidate**. Think of it like a traffic light turning yellow — attention required, but no action yet.

### Stage B: "Is the move reversing?"

Being a candidate doesn't trigger an exit. MIP now watches for **evidence of reversal**:

- **Giveback percentage** — Has the position given back 40% or more of its peak return? (Like reaching 100 meters up a hill, then sliding 40 meters back down.)
- **No new highs** — Have the last 3 consecutive bars all failed to make a new high? (The climb has stalled.)
- **Quick payoff bonus** — If the target was hit within 60 minutes, MIP gets more cautious (uses a 25% giveback threshold instead of 40%), because very fast moves are more likely to reverse.

Only when **both stages** pass does MIP signal an early exit.

## The Three Modes

The early-exit system has a safety ladder:

| Mode | What Happens | When to Use |
|------|-------------|-------------|
| **SHADOW** | Evaluates positions and logs what *would* happen, but changes nothing | First 2-4 weeks (prove it works) |
| **PAPER** | Actually closes positions in the simulation | After SHADOW shows net benefit |
| **ACTIVE** | Live execution (future) | Only after Paper mode proves stable |

Right now, the system runs in **SHADOW mode** by default. You get full visibility into what the system *would* do, without any risk to your portfolio.

## What Gets Logged

Every evaluation is recorded with full audit trail:

- **bar_close_ts** — which 15-minute bar the decision was based on
- **decision_ts** — when MIP made the decision (prevents "time travel")
- **Payoff metrics** — target return, current return, MFE (max favorable excursion)
- **Giveback metrics** — peak return, current return, giveback percentage
- **Gate results** — which checks passed and failed
- **Fee-adjusted P&L** — what the early exit would actually produce after slippage and fees
- **Hold-to-end comparison** — the delta between exiting early and holding to horizon

## Configuration

All thresholds are tunable via `APP_CONFIG`:

| Parameter | Default | What It Controls |
|-----------|---------|-----------------|
| `EARLY_EXIT_PAYOFF_MULTIPLIER` | 1.0 | How much buffer above target (1.0 = exact, 1.2 = 20% buffer) |
| `EARLY_EXIT_GIVEBACK_PCT` | 0.40 | What fraction of peak must be given back to trigger |
| `EARLY_EXIT_NO_NEW_HIGH_BARS` | 3 | How many bars with no new high confirms the stall |
| `EARLY_EXIT_QUICK_PAYOFF_MINS` | 60 | Threshold for "quick payoff" (stricter giveback) |
| `EARLY_EXIT_QUICK_GIVEBACK_PCT` | 0.25 | Giveback threshold for quick payoffs |

## Pipeline Integration

The early-exit evaluation runs as **Step 4** of the intraday pipeline, right after outcome evaluation:

```
Ingest 15m bars → Generate signals → Evaluate outcomes → Evaluate early exits → Log run
```

It only runs when `EARLY_EXIT_ENABLED` is `true`. If intraday bars are missing for a position, that position is silently skipped — no harm done.

## Why Not Just "Exit When Target Hit"?

The naive approach — exit the moment your target is reached — sounds appealing but systematically cuts winners short. Many of MIP's best trades are continuation moves where the initial target is just the start. Exiting immediately would lock in the minimum gain while missing the full move.

The two-stage approach captures the **best of both worlds**: it lets winners run when momentum continues, but protects gains when the move is clearly fading.
