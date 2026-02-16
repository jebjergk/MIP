# 9. From Trust to Trading

Even after a pattern earns TRUSTED status, several more gates must be passed before an actual trade happens:

```
TRUSTED Signal → Risk Gate → Capacity Check → Proposal → Trade Executed
```

1. **TRUSTED Signal** — Pattern passed all trust gates
2. **Risk Gate** — Is the portfolio safe? Is entry allowed?
3. **Capacity Check** — Does the portfolio have open slots?
4. **Proposal** — Order proposed & validated
5. **Trade Executed** — Position opened in portfolio

The **proposal funnel** shows this narrowing effect. If you see many signals but few trades, it's usually because:

- **Most patterns are still in WATCH status** — they haven't earned trust yet, so their signals can't become proposals.
- **The portfolio is fully saturated** — all position slots are in use. New trades can't open until existing ones close.
- **The risk gate is in CAUTION or STOPPED mode** — the portfolio's drawdown exceeded a safety threshold, blocking new entries.
- **Duplicate position** — the portfolio already holds this symbol, so a new entry is rejected.

## Example: A Typical Day's Funnel

Today's pipeline generated **15 signals** across all symbols. Of those, **3** came from TRUSTED patterns (the rest are still training). Of those 3, **2** passed the risk gate (the portfolio's gate is SAFE). Of those 2, **1** passed the capacity check (1 slot was available). Result: **1 trade executed** out of 15 signals. That's normal.
