# Intraday v2 Natural Key Contract (Phase 0)

## Bars Contract (15m source of truth)

- Table: `MIP.MART.MARKET_BARS`
- Scope: `INTERVAL_MINUTES = 15`
- Natural key columns:
  - `MARKET_TYPE` (`string`)
  - `SYMBOL` (`string`)
  - `INTERVAL_MINUTES` (`number`)
  - `TS` (`timestamp_ntz`)
- Contract rule:
  - One row per `(MARKET_TYPE, SYMBOL, INTERVAL_MINUTES, TS)`.
  - No duplicate keys are allowed.

## Current Legacy Intraday Signal Contract

- Table: `MIP.APP.RECOMMENDATION_LOG`
- Scope: intraday rows where `INTERVAL_MINUTES = 15`
- Current detector dedupe key:
  - `PATTERN_ID` (`number`)
  - `SYMBOL` (`string`)
  - `MARKET_TYPE` (`string`)
  - `INTERVAL_MINUTES` (`number`)
  - `TS` (`timestamp_ntz`)
- Contract rule:
  - One signal per key above.
  - Current detector procedures enforce this with `NOT EXISTS` checks.

## Intraday v2 Signal Contract (Target)

- Table: `MIP.APP.INTRA_SIGNALS`
- Natural key columns:
  - `PATTERN_ID`
  - `MARKET_TYPE`
  - `SYMBOL`
  - `INTERVAL_MINUTES`
  - `SIGNAL_TS`
  - `SIGNAL_SIDE`
- Deterministic identity:
  - `SIGNAL_NK_HASH = SHA2(PATTERN_ID|MARKET_TYPE|SYMBOL|INTERVAL_MINUTES|SIGNAL_TS|SIGNAL_SIDE)`
  - `SIGNAL_NK_HASH` is unique and is the stable lookup key for outcomes.

## Outcome Linkage Contract (Target)

- Table: `MIP.APP.INTRA_OUTCOMES`
- Contract rule:
  - Outcomes are written only after resolving `SIGNAL_ID` from `INTRA_SIGNALS` via `SIGNAL_NK_HASH`.
  - Idempotency is verified on `(SIGNAL_NK_HASH, HORIZON_BARS)` across reruns.
