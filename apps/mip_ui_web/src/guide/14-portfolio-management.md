# 14. Portfolio Management

Create and configure portfolios, manage risk profiles, deposit/withdraw cash, view lifecycle history, and generate AI-powered portfolio stories. This is the operational hub where you set up and maintain your portfolios.

> **Pipeline Lock:** When the daily pipeline is actively running, **all editing is disabled** on this page. A yellow warning banner appears at the top: "Pipeline is currently running — editing is disabled until the run completes." This prevents changes from interfering with an active simulation. Buttons automatically re-enable once the pipeline finishes (the page polls every 15 seconds). You can still browse data and read-only tabs while waiting.

## Tabs

The page is organized into four tabs:

| Tab | Purpose |
|-----|---------|
| **Portfolios** | Create/edit portfolios, deposit/withdraw cash, attach risk profiles |
| **Profiles** | Create/edit risk profiles with position limits, drawdown stops, and crystallization settings |
| **Lifecycle Timeline** | Visual history of every lifecycle event (charts + timeline) for a selected portfolio |
| **Portfolio Story** | AI-generated narrative summarizing the portfolio's complete journey |

## Tab 1: Portfolios

Shows a table of all portfolios with key metrics. Each row displays the portfolio's ID, name, assigned risk profile, starting cash, final equity, total return, and status.

### Actions on Each Portfolio Row

- **Edit** — Update the portfolio name, currency, or notes. **Starting cash cannot be changed after creation** — use the Cash button for deposits/withdrawals instead.

- **Cash** — Opens the **Cash Event** dialog where you register a deposit or withdrawal.
  - **Deposit:** Adds money to the portfolio. Increases cash and equity by the deposited amount.
  - **Withdraw:** Removes money from the portfolio. You can only withdraw up to the current cash balance.
  - **Important:** Your lifetime P&L tracking stays intact. The system adjusts the cost basis so gains/losses are always calculated correctly — a deposit doesn't count as "profit" and a withdrawal doesn't count as a "loss."

- **Profile** — Attach a different risk profile to the portfolio. **Warning:** Changing the profile ends the current episode and starts a new one. Episode results are preserved in the lifecycle history.

### "+ Create Portfolio" Button

Opens a dialog to create a new portfolio. You'll set:

| Field | What it means | Example |
|-------|---------------|---------|
| **Name** | A descriptive name for the portfolio | Main FX Portfolio |
| **Currency** | Base currency (USD, EUR, or GBP) | USD |
| **Starting Cash** | Initial capital — cannot be changed later | $100,000 |
| **Risk Profile** | Which profile's rules to apply (position limits, drawdown stops, crystallization) | MODERATE_RISK |
| **Notes** | Optional description | "FX-focused momentum strategy" |

### Example: Depositing Cash

Your portfolio "Main FX" has $87,000 in cash and $15,000 in open positions (equity = $102,000). You click **Cash → Deposit → $10,000**. After the deposit:
- Cash: $87,000 + $10,000 = **$97,000**
- Equity: $102,000 + $10,000 = **$112,000**
- P&L stays the same — the deposit is a cost basis adjustment, not a profit
- A **DEPOSIT** event is recorded in the lifecycle timeline

## Tab 2: Profiles

Risk profiles are reusable templates that define how a portfolio should behave. You can create as many profiles as you need and attach them to any portfolio. The table shows each profile's settings and how many portfolios are currently using it.

### Profile Settings Explained

| Setting | What it controls | Example |
|---------|------------------|---------|
| **Max Positions** | Maximum number of holdings at once | 10 |
| **Max Position %** | Maximum size of any single position as a % of cash | 8% |
| **Bust Equity %** | If equity drops below this % of starting cash, the portfolio is "bust" | 50% |
| **Bust Action** | What happens at bust: Allow Exits Only, Liquidate Next Bar, or Liquidate Immediate | Allow Exits Only |
| **Drawdown Stop %** | Maximum peak-to-trough decline before entries are blocked | 15% |

### Crystallization Settings

Crystallization is the process of **locking in gains** when a profit target is reached. When triggered, the current episode ends, profits are recorded, and a new episode begins.

| Setting | What it does | Example |
|---------|-------------|---------|
| **Enabled** | Turn crystallization on or off | On |
| **Profit Target %** | The return that triggers crystallization | 10% |
| **Mode** | **Withdraw Profits:** Gains are withdrawn from the portfolio. New episode starts with original capital. **Rebase (compound):** Gains stay in the portfolio. New episode starts with the higher equity as the new cost basis. | Withdraw Profits |
| **Cooldown Days** | Minimum days between crystallization events | 30 |
| **Max Episode Days** | Force a new episode after this many days even without hitting the profit target | 90 |
| **Take Profit On** | Check the target at End of Day or Intraday | End of Day |

### Example: Crystallization in Action

Your profile has a **10% profit target** in **Withdraw Profits** mode. The portfolio started with $100,000. After 6 weeks, equity reaches $110,500 (+10.5%).

1. The pipeline detects the profit target is hit (+10.5% > 10%)
2. $10,500 in profits is withdrawn and recorded as a payout
3. The current episode (Episode 1) ends with status "CRYSTALLIZED"
4. A new episode (Episode 2) starts with $100,000 as the cost basis
5. The lifecycle timeline records both the CRYSTALLIZE and EPISODE_START events

If **Rebase** mode were used instead, the $10,500 would stay in the portfolio and Episode 2 would start with $110,500 as the new cost basis. This is compounding — subsequent profit targets are measured against the higher base.

## Tab 3: Lifecycle Timeline

A visual history of every meaningful event in a portfolio's life. Select a portfolio from the dropdown to view its history.

### Charts (4 panels)

- **Lifetime Equity** — A line chart showing equity over time across all lifecycle events. Each dot marks a recorded event (deposit, withdrawal, crystallization, etc.).
- **Cumulative Lifetime P&L** — An area chart showing the running total of profit/loss over time. Green = above zero (profitable), red area appears if it dips below zero.
- **Cash Flow Events** — A bar chart showing money in (green bars for CREATE and DEPOSIT) and money out (red bars for WITHDRAW and CRYSTALLIZE). Helps you see the pattern of cash flows over time.
- **Cash vs Equity** — Two lines: orange for cash on hand, blue for total equity. The gap between them represents the value tied up in open positions.

### Event Timeline (below the charts)

A vertical timeline listing every lifecycle event in chronological order. Each entry shows:

| Element | What it shows | Example |
|---------|---------------|---------|
| **Event Type** | What happened (color-coded dot) | DEPOSIT, CRYSTALLIZE, EPISODE_START |
| **Timestamp** | When it happened | Feb 7, 2026, 14:30 |
| **Amount** | Money involved (if applicable) | +$10,000 or -$5,200 |
| **Snapshots** | Cash, Equity, and Lifetime P&L at that moment | Cash: $97,000 / Equity: $112,000 / P&L: $2,000 |
| **Notes** | Optional notes recorded with the event | "Quarterly deposit" |

### Lifecycle Event Types

| Event Type | When it happens | What it records |
|------------|-----------------|-----------------|
| **CREATE** | Portfolio is first created | Initial cash, starting equity |
| **DEPOSIT** | You add cash to the portfolio | Deposit amount, new cash/equity balances |
| **WITHDRAW** | You remove cash from the portfolio | Withdrawal amount, new balances |
| **CRYSTALLIZE** | Profit target hit, gains locked in | Payout amount, mode (withdraw or rebase) |
| **PROFILE_CHANGE** | Risk profile is changed | Old and new profile references |
| **EPISODE_START** | A new episode begins | New episode ID, starting equity |
| **EPISODE_END** | An episode ends | Final equity, end reason |
| **BUST** | Portfolio hits bust threshold | Equity at bust |

## Tab 4: Portfolio Story

An AI-generated narrative that tells the complete story of a portfolio — from creation through every deposit, withdrawal, crystallization event, and market performance period. Think of it as a "biography" for your portfolio.

- **Headline** — A one-line summary of the portfolio's journey. Example: "Main FX Portfolio: 3 episodes, $5,200 in payouts, currently active with +2.4% return."
- **Narrative** — Multiple paragraphs explaining the full story — how the portfolio started, what happened in each episode, how it responded to market conditions, and where it stands now.
- **Key Moments** — A bulleted list of the most significant events. Example: "Episode 1 crystallized at +12.3% after 45 days" or "Deposit of $10,000 on Feb 7 increased the capital base."
- **Outlook** — Forward-looking commentary based on current state. Example: "With the risk gate in SAFE mode and 4 of 10 position slots available, the portfolio is well-positioned for new opportunities."

> **Auto-generation:** The story is generated automatically the first time you visit the tab for a portfolio. You can manually regenerate it by clicking the **Regenerate** button. Generation uses Snowflake Cortex AI and typically takes 10–20 seconds.
