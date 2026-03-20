# 16. Performance Dashboard

Route: `/performance-dashboard`

The Performance Dashboard is the quickest way to compare how portfolios are doing over time.

## What you can verify here

- Return trends by portfolio.
- Relative performance between strategies.
- Drawdown and consistency at a glance.
- Whether recent gains are broad-based or concentrated.

## KPIs and summary cards

The top of the page shows key portfolio metrics:
- **Total Equity**: Current total portfolio value.
- **Total Return (%)**: Overall return since inception or selected period.
- **Max Drawdown (%)**: The largest peak-to-trough decline observed. Lower absolute values mean more consistent risk management.
- **Sharpe Ratio**: Risk-adjusted return measure. Higher is better; above 1.0 is generally considered good.
- **Win Rate (%)**: Percentage of closed trades that were profitable.
- **Expectancy**: Average expected gain per trade, combining win rate and average win/loss size.

## Charts and visualizations

- **Equity Curve**: Line chart showing total equity over time. An upward-sloping curve indicates growing portfolio value.
- **Monthly Cost Trend**: Bar chart showing transaction/execution costs by month.
- **Decision Quality Trend**: Line chart with two series — expectancy (average gain per trade) and percent positive (win rate). Tracks whether trading quality is improving or declining over time.
- **Selectivity Trend**: Bar chart showing how selective the committee is being — ratio of approved to total proposals.
- **Decision Funnel**: Horizontal bar chart showing how many proposals flow through each stage (proposed → approved → executed → profitable).
- **Committee Effectiveness**: Summary of how well committee decisions translate to positive outcomes.
- **Parallel Worlds**: Counterfactual analysis showing what would have happened under different rule settings.
- **Target Realism Analysis**: Bar chart comparing expected move targets vs actual realized moves.
- **Cost Attribution**: Breakdown of where trading costs come from (slippage, commissions, spread, etc.).
- **Learning-to-Decision Ledger**: How training pipeline outputs influence trade decisions.

## Typical workflow

1. Start with period filters (for example: 1M, 3M, YTD).
2. Check equity curve for overall trend direction.
3. Compare top and bottom performers.
4. Check drawdown behavior next to returns.
5. Review decision quality trend for improving/declining edge.
6. Open supporting pages (Runs, AI Agent Decisions, Training Status) for root cause.

## When to use this page

- Daily performance check across multiple portfolios.
- Weekly review for strategy comparison.
- Before changing risk settings, to validate stability vs. return.
