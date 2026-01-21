# KPI Layer (MIP.MART)

## Overview
This layer provides KPI-ready views for portfolio runs, attribution, and signal quality. All objects are views in `MIP.MART` and are designed for daily interval data (`INTERVAL_MINUTES=1440`).

## Portfolio KPIs
### `MIP.MART.V_PORTFOLIO_RUN_KPIS`
**Grain:** `(portfolio_id, run_id)`

**Key fields**
- `from_ts`, `to_ts`, `trading_days`: min/max daily timestamps and count of daily rows.
- `starting_cash`: from `MIP.APP.PORTFOLIO.STARTING_CASH`.
- `final_equity`: `MAX_BY(total_equity, ts)`.
- `total_return`: `(final_equity / starting_cash) - 1` (NULL-safe).
- `max_drawdown`, `peak_equity`, `min_equity` from daily series.
- `daily_volatility`, `avg_daily_return` from `daily_return`.
- `win_days`, `loss_days` and average win/loss PnL from `daily_pnl`.
- `avg_open_positions` and `time_in_market` (share of days with open positions).
- `drawdown_stop_ts`: first timestamp where `drawdown >= drawdown_stop_pct`.

**Drawdown threshold**
- Uses `PORTFOLIO_PROFILE.DRAWDOWN_STOP_PCT` when present, otherwise defaults to `0.10`.

### `MIP.MART.V_PORTFOLIO_RUN_EVENTS`
**Grain:** `(portfolio_id, run_id)`

**Key fields**
- `drawdown_stop_ts`: first drawdown breach (same threshold as above).
- `first_flat_no_positions_ts`: first timestamp after stop with `open_positions=0`.
- `stop_reason`: currently `DRAWDOWN_STOP` when a stop exists.

## Attribution KPIs
### `MIP.MART.V_PORTFOLIO_ATTRIBUTION`
**Grain:** `(portfolio_id, run_id, market_type, symbol)`

**Key fields**
- `total_realized_pnl`: sum of `realized_pnl` from SELL trades.
- `roundtrips`: count of SELL trades.
- `avg_pnl_per_trade`, `win_rate`.
- `contribution_pct`: contribution versus total realized PnL for the run.

### `MIP.MART.V_PORTFOLIO_ATTRIBUTION_BY_PATTERN`
**Grain:** `(portfolio_id, run_id, pattern_id, market_type, horizon_bars)`

**Mapping assumptions**
- Maps SELL trades to positions by the most recent `entry_ts` at or before the trade timestamp.
- Maps positions to recommendations by exact match on `(symbol, market_type, interval_minutes, entry_ts = recommendation.ts)`.
- When multiple horizons exist for a recommendation, the smallest `horizon_bars` is selected to avoid fan-out.

## Signal/Outcome KPIs
### `MIP.MART.V_SIGNAL_OUTCOME_KPIS`
**Grain:** `(pattern_id, market_type, interval_minutes, horizon_bars)`

**Key fields**
- `n_total`, `n_success`, `coverage_rate`.
- Return distribution stats from matured (`eval_status='SUCCESS'`) outcomes.
- `hit_rate`, `avg_win`, `avg_loss`, `score_return_corr`.
- Readiness dates: oldest/newest not-ready entry and latest matured entry.

## Score Calibration
### `MIP.MART.V_SCORE_CALIBRATION`
**Grain:** `(pattern_id, market_type, interval_minutes, horizon_bars, score_decile)`

**Key fields**
- Deciles from `NTILE(10)` on score (matured outcomes only).
- Sample size, average/median return, hit rate, score min/max.

### `MIP.MART.V_SIGNALS_WITH_EXPECTED_RETURN`
**Grain:** `recommendation_id`

**Key fields**
- Score decile for each recommendation.
- `expected_return` sourced from the calibration table’s decile average return.

## Intended UI Mapping (later)
- **Portfolio dashboard:** `V_PORTFOLIO_RUN_KPIS`, `V_PORTFOLIO_RUN_EVENTS`.
- **Attribution view:** `V_PORTFOLIO_ATTRIBUTION` and `V_PORTFOLIO_ATTRIBUTION_BY_PATTERN`.
- **Signal quality & calibration:** `V_SIGNAL_OUTCOME_KPIS`, `V_SCORE_CALIBRATION`, `V_SIGNALS_WITH_EXPECTED_RETURN`.
