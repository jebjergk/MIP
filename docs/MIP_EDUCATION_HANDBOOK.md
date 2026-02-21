# MIP Education Handbook

**A complete guide to the Market Intelligence Platform -- written for everyone, no trading experience required.**

---

## Table of Contents

1. [What is MIP?](#chapter-1-what-is-mip)
2. [The Building Blocks -- Understanding Markets](#chapter-2-the-building-blocks----understanding-markets)
3. [How MIP Watches the Market](#chapter-3-how-mip-watches-the-market)
4. [Patterns and Signals -- How MIP Spots Opportunities](#chapter-4-patterns-and-signals----how-mip-spots-opportunities)
5. [Did the Signal Work? -- Outcomes, Horizons, and Training](#chapter-5-did-the-signal-work----outcomes-horizons-and-training)
6. [Trust -- How MIP Decides What to Act On](#chapter-6-trust----how-mip-decides-what-to-act-on)
7. [Portfolios -- Simulated Investing](#chapter-7-portfolios----simulated-investing)
8. [The Daily Pipeline and Advanced Features](#chapter-8-the-daily-pipeline-and-advanced-features)
9. [Using the MIP Dashboard](#chapter-9-using-the-mip-dashboard)
- [Appendix A: Glossary](#appendix-a-glossary)
- [Appendix B: Frequently Asked Questions](#appendix-b-frequently-asked-questions)

---

## Chapter 1: What is MIP?

### The Short Version

MIP stands for **Market Intelligence Platform**. It is a system that watches financial markets every day, looks for patterns in how prices are moving, and then tests whether acting on those patterns would have made money -- all without spending a single real dollar.

Think of MIP as a **flight simulator for investing**. Just as a pilot practices flying in a simulator before stepping into a real cockpit, MIP lets you practice and observe investment strategies in a safe, simulated environment. No real money is at risk. Ever.

### What MIP Does

MIP performs five major jobs, automatically, every day:

1. **Collects market data.** MIP downloads the latest prices for stocks, ETFs, and currencies from a financial data provider.
2. **Looks for patterns.** It runs a set of rules against that price data to spot potentially interesting opportunities -- moments where historical behavior suggests a price might keep going up.
3. **Grades its own work.** After making a prediction, MIP goes back later to check: did the price actually go up? By how much? It keeps a scorecard for every prediction it has ever made.
4. **Simulates portfolios.** MIP manages virtual accounts with virtual money. Based on its best predictions, it "buys" and "sells" assets on paper, tracking performance as if the trades were real.
5. **Reports the results.** Each day, MIP produces summaries -- called morning briefs -- that describe what it found, what it traded, and how things are going.

### What MIP is NOT

It is important to be clear about what MIP does not do:

- **MIP is not a broker.** It does not connect to any stock exchange or financial institution. It cannot place real orders.
- **MIP does not use real money.** Every trade is simulated. The "cash" in a MIP portfolio is virtual.
- **MIP is not real-time.** It processes data once per day (or at set intervals for intraday mode). It is not watching prices tick by tick.
- **MIP does not guarantee profits.** It is an analytical and educational tool. The patterns it finds may or may not work in the future.

### Who Is MIP For?

MIP is for anyone who wants to understand how systematic, data-driven investing works. Whether you are a curious beginner who has never bought a stock, or someone exploring quantitative strategies, MIP gives you a transparent window into the process: how market data becomes signals, how signals are evaluated, and how portfolios are managed with disciplined risk rules.

---

## Chapter 2: The Building Blocks -- Understanding Markets

Before diving into how MIP works, let's cover the basic concepts about financial markets that MIP deals with. If you already know what stocks and price charts are, feel free to skip ahead -- but these explanations are here so that nothing later in the handbook feels like a mystery.

### What Are Stocks, ETFs, and Currencies?

MIP watches three types of financial assets:

**Stocks** are tiny ownership slices of a company. When you "buy a share of Apple," you own a small piece of Apple Inc. The price of that share goes up and down every day based on what buyers and sellers think the company is worth. Examples: AAPL (Apple), MSFT (Microsoft), TSLA (Tesla).

**ETFs (Exchange-Traded Funds)** are bundles of assets packaged together and traded like a single stock. Instead of buying 500 individual stocks, you can buy one share of an ETF like SPY, which tracks the 500 largest U.S. companies. ETFs let you invest in broad themes (technology, gold, international markets) without picking individual companies.

**Currencies (FX -- Foreign Exchange)** represent the exchange rate between two countries' money. For example, EUR/USD tells you how many U.S. dollars one Euro is worth. Currency prices move based on economic conditions, interest rates, and global events. In MIP, these are labeled with the market type "FX."

### What Is a Price Bar?

Imagine you are watching the price of Apple stock throughout a single trading day. The price moves constantly -- it might open at \$150 in the morning, swing up to \$153 during the afternoon, dip down to \$149 at one point, and close at \$151 when the market shuts.

A **price bar** captures that entire day's activity in just five numbers:

| Number | Name | What It Means |
|--------|------|---------------|
| **O** | Open | The price at the very start of the period (\$150) |
| **H** | High | The highest price reached during the period (\$153) |
| **L** | Low | The lowest price reached during the period (\$149) |
| **C** | Close | The price at the very end of the period (\$151) |
| **V** | Volume | How many shares were traded during the period (e.g., 50 million) |

Together, these five numbers are called an **OHLCV bar** (pronounced "oh-aitch-ell-see-vee"). Each bar is a compact summary of everything that happened to a price during a specific time window.

### Time Intervals: Daily vs. Intraday

A price bar can represent different lengths of time:

- A **daily bar** covers one full trading day (from market open to market close). In MIP, this is identified by `INTERVAL_MINUTES = 1440`, which is 24 hours expressed in minutes. Daily bars are MIP's primary mode of operation.
- An **intraday bar** covers a shorter period within a single day -- for example, 15 minutes. MIP can also work with intraday bars for faster-paced analysis.

Most of MIP's core features -- pattern detection, training, portfolio simulation -- operate on daily bars. Think of it as MIP taking one "snapshot" of each asset at the end of every trading day.

### What Is a "Return"?

A **return** is simply how much an asset's price changed, expressed as a percentage.

**Example:** You note that Apple's stock closed at \$100 yesterday and \$103 today. The return is:

> (103 - 100) / 100 = 0.03 = **3% return**

A positive return means the price went up. A negative return means it went down. Returns are the fundamental building block MIP uses to detect patterns -- it is not looking at raw prices, but at *how much prices are changing* from one period to the next.

### What Does a Trading Day Look Like?

In the U.S., the stock market is open from 9:30 AM to 4:00 PM Eastern Time, Monday through Friday (excluding holidays). At the end of each trading day, every stock, ETF, and currency pair has a final "closing price." That closing price becomes the basis for MIP's daily calculations.

MIP's daily pipeline typically runs once per day (for example, at 7:00 AM European time), processing the previous day's closing data.

---

## Chapter 3: How MIP Watches the Market

Now that you understand the building blocks, let's look at how MIP collects and organizes market data.

### Data Ingestion: Downloading Prices

Every day, MIP connects to a financial data provider called **AlphaVantage** and downloads the latest price bars for every asset it is configured to track. This process is called **ingestion** -- MIP is "ingesting" (taking in) raw market data.

The downloaded data includes the OHLCV numbers (open, high, low, close, volume) for each asset and each time interval. MIP stores this data in a database table called `MARKET_BARS`, which becomes the foundation for everything else the system does.

Think of ingestion as MIP's "morning newspaper delivery." Each day, fresh data arrives, and MIP's analysis begins.

### The Universe: What MIP Tracks

MIP does not track every asset in the world -- it watches a specific, configured list called the **ingest universe**. This universe defines:

- **Which symbols** to track (e.g., AAPL, MSFT, SPY, EUR/USD)
- **What type** each symbol is (STOCK, ETF, or FX)
- **What time interval** to use (e.g., daily bars at 1440 minutes)

You can add or remove symbols from the universe. If a symbol is not in the universe, MIP simply does not watch it.

### Market Types

MIP organizes assets into three market types, which matters because each type may behave differently:

| Market Type | What It Includes | Example Symbols |
|-------------|------------------|-----------------|
| **STOCK** | Individual company shares | AAPL, MSFT, TSLA |
| **ETF** | Exchange-traded funds (bundles of assets) | SPY, QQQ, GLD |
| **FX** | Currency pairs | EUR/USD, GBP/USD |

The system tracks these types separately because the rules for spotting patterns in stocks might differ from the rules for currencies. For example, stocks tend to be more volatile (prices swing more) than major currency pairs.

### Calculating Returns

After ingesting fresh price data, MIP calculates **returns** for every asset. These returns tell MIP how much each asset's price moved compared to the previous period.

For daily bars, this means comparing today's closing price to yesterday's closing price. MIP computes:

- **Simple return**: The straightforward percentage change -- `(today's close - yesterday's close) / yesterday's close`
- **Log return**: A mathematically convenient version used in some calculations -- `ln(today's close / yesterday's close)`

For most purposes, you can think of both as "how much did the price change today?" The returns are stored in a view called `MARKET_RETURNS` and feed directly into MIP's pattern detection.

---

## Chapter 4: Patterns and Signals -- How MIP Spots Opportunities

This is where MIP gets interesting. After collecting data and calculating returns, MIP runs a set of rules to look for assets that might be worth buying. This chapter explains how.

### What Is a Pattern?

A **pattern** is a set of rules that examines recent price behavior and decides: "Something noteworthy is happening with this asset right now."

Think of it like a **weather forecast rule**. A meteorologist might say: "If barometric pressure drops rapidly, humidity rises above 80%, and wind shifts from the southwest, then rain is likely within 24 hours." That is a pattern -- a combination of observable conditions that, historically, has preceded a specific outcome.

In MIP, a pattern works the same way, but instead of weather data, it looks at price data. Instead of predicting rain, it is flagging assets whose recent price behavior suggests continued upward movement.

Each pattern is defined by:

- A **name** (e.g., "STOCK_MOMENTUM_FAST")
- A set of **parameters** that control how sensitive or strict the rules are
- A **status**: active (currently being used) or inactive (turned off because it did not perform well enough)

### The Momentum Pattern: MIP's Primary Strategy

MIP's main pattern type is called **momentum**. The core idea behind momentum is simple and intuitive:

> *"Assets that have been going up recently tend to keep going up for a little while longer."*

This is one of the most well-studied phenomena in finance. It does not always work, but historically, it has worked often enough to be useful. Here is how MIP's momentum pattern evaluates an asset:

**Step 1: Look at recent returns.** The pattern examines how the asset's price has moved over recent days. It uses two time windows:

- **Fast window** (e.g., 3 days): A short, recent lookback. "What happened in the last few days?"
- **Slow window** (e.g., 20 days): A longer lookback. "What has the broader trend been?"

**Step 2: Check minimum strength.** The asset's recent return must be above a minimum threshold (e.g., at least 0.2%). This filters out tiny, meaningless price movements. If a stock went up only 0.01%, that is noise, not a signal.

**Step 3: Check consistency.** The pattern wants to see that the asset has been positive *consistently*, not just on one lucky day. It counts how many of the recent days had positive returns and requires a minimum number (based on the slow window).

**Step 4: Check if price is at a recent high.** The current closing price should be at or above the highest close in the fast window. This confirms the upward trend is still active, not fading.

**Step 5 (optional): Check how unusual the move is.** MIP can also compute a **z-score**, which measures how unusual the current return is compared to the asset's normal behavior. A z-score of 2 means "this move is about twice as large as typical." A minimum z-score filter ensures MIP only flags genuinely notable movements, not routine fluctuations.

If an asset passes all these checks, the pattern fires, and MIP records a signal.

### What Is a Signal?

A **signal** (also called a **recommendation** in MIP's database) is a record that says:

> "Pattern X detected an opportunity in asset Y at time Z, with strength score S."

For example: "The STOCK_MOMENTUM_FAST pattern detected a signal for AAPL on February 19, 2026, with a score of 0.035 (3.5% return)."

Each signal is stored in MIP's `RECOMMENDATION_LOG` and includes:

- **Which pattern** generated it
- **Which asset** (symbol) it applies to
- **When** the signal was generated (timestamp)
- **How strong** the signal is (score -- typically the asset's return)
- **Additional details** about why the pattern fired

### Understanding the Score

The **score** is a number that indicates how strong the signal is. For momentum signals, the score is usually the asset's recent return. A score of 0.05 means the asset had a 5% return -- a stronger signal than one with a score of 0.01 (1% return).

Higher scores mean the pattern detected a more pronounced price movement. When MIP later decides which signals to act on, it prefers higher-scoring signals.

### How Signals Are Generated

Signals are generated automatically as part of MIP's daily pipeline. Each day, after fresh market data is downloaded and returns are calculated, MIP runs its active pattern definitions against all assets in its universe. Any asset that meets all the pattern's criteria at that point in time gets a signal recorded.

On a typical day, MIP might generate anywhere from zero to dozens of signals, depending on market conditions. In a quiet market, few assets will meet the criteria. In a strongly trending market, many might.

---

## Chapter 5: Did the Signal Work? -- Outcomes, Horizons, and Training

Generating signals is only half the story. The other half -- arguably the more important half -- is checking whether those signals actually led to profitable outcomes. This is how MIP learns and improves over time.

### The Core Question

Every time MIP records a signal ("AAPL looks interesting today"), a clock starts ticking. The question becomes:

> "If we had bought AAPL at the moment of this signal, would we have made money?"

MIP does not just ask this once. It checks back at multiple points in the future to build a complete picture of how the signal played out.

### Horizons: Checking Back at Multiple Time Points

A **horizon** is the number of future days (or bars) that MIP waits before checking the result of a signal.

MIP evaluates each signal at five different horizons:

| Horizon | Meaning | Analogy |
|---------|---------|---------|
| **1 day** | What happened the very next day? | "Was the weather forecast right for tomorrow?" |
| **3 days** | What happened after three days? | "Was the three-day forecast accurate?" |
| **5 days** | What happened after one trading week? | "Was the weekly outlook correct?" |
| **10 days** | What happened after two trading weeks? | "Was the two-week prediction right?" |
| **20 days** | What happened after one trading month? | "Was the monthly outlook accurate?" |

Why check at multiple horizons? Because a signal might be wrong tomorrow but right over the next week. Or it might work great for three days but reverse after that. Checking multiple horizons reveals the *shape* of the opportunity -- how long the effect lasts.

### Outcomes: The Scorecard

For each signal at each horizon, MIP records an **outcome**. An outcome answers the question: "What actually happened?" It contains:

- **Entry price**: The price of the asset when the signal was generated (the price you would have "bought" at)
- **Exit price**: The price of the asset at the horizon point (the price you would have "sold" at)
- **Realized return**: The percentage gain or loss. For example, if you "entered" at \$100 and "exited" at \$104, your realized return is +4%
- **Hit flag**: Did the signal meet the minimum return threshold? (more on this next)
- **Evaluation status**: Was there enough future data to compute this outcome? (Sometimes a signal is too recent and not enough future bars exist yet)

### Hit Rate: The Batting Average

The **hit flag** is a simple yes-or-no answer: did this signal produce at least the minimum required return?

By default, the threshold is 0% -- meaning any positive return counts as a "hit." If the realized return at a given horizon is 0% or higher, the hit flag is TRUE. If the price went down, it is FALSE.

The **hit rate** is the percentage of signals that were hits. It is MIP's batting average.

**Example:** Imagine a pattern generated 100 signals over the past few months. At the 5-day horizon, 58 of those signals resulted in positive returns. The hit rate is 58/100 = **58%**. That means the pattern was right about 58% of the time when checked five days later.

A hit rate above 50% means the pattern is right more often than it is wrong. MIP requires a minimum hit rate (typically 55%) before it trusts a pattern.

### Strict Lookahead: No Cheating

One critical rule MIP follows is **strict lookahead prevention**. When evaluating a signal, MIP only uses price data that came *after* the signal timestamp. It never peeks at data from the same time or earlier.

Why does this matter? Without this rule, you could accidentally create patterns that appear to predict the future but are really just using information that was already available. This is a common trap in financial analysis called "lookahead bias," and it produces results that look amazing on paper but fail completely in practice.

MIP's strict rule ensures that every evaluation honestly represents what would have happened if you had acted on the signal in real time.

### Training: How MIP Learns

**Training** in MIP is the process of reviewing how patterns have performed and deciding which ones deserve to keep running. It is not the same as training a machine learning model with neural networks -- it is more like a **performance review for each pattern**.

Here is how it works:

#### Backtesting

A **backtest** replays historical data to evaluate a pattern's track record. MIP looks at every signal a pattern has generated, checks the outcomes at each horizon, and computes summary statistics:

- **Trade count**: How many signals did this pattern generate?
- **Hit count**: How many were hits (positive returns)?
- **Hit rate**: What percentage were hits?
- **Average return**: What was the average profit or loss per signal?
- **Cumulative return**: If you had followed every signal, what would your total return be?

#### The Learning Cycle

MIP's full learning process works in a loop:

1. **Generate signals** from market data using the pattern's rules
2. **Evaluate outcomes** -- go back and check whether past signals worked
3. **Run a backtest** -- aggregate performance across all signals
4. **Update the pattern** -- refresh the pattern's performance metrics and decide whether to keep it active

#### Minimum Thresholds: The Bar for Active Patterns

After a backtest, MIP decides whether each pattern should remain active using three minimum thresholds:

| Threshold | Default Value | What It Means |
|-----------|--------------|---------------|
| **Minimum trades** | 30 | The pattern must have generated at least 30 signals. Fewer than that is not enough data to be confident. |
| **Minimum hit rate** | 55% | At least 55% of those signals must have been hits. Below that, the pattern is wrong too often. |
| **Minimum cumulative return** | 0% | The overall result of following the pattern must be positive. Even a high hit rate is not enough if the losses on misses outweigh the gains on hits. |

If a pattern meets all three thresholds, it stays **active** (IS_ACTIVE = 'Y'). If it fails any one of them, it is **deactivated** (IS_ACTIVE = 'N') and MIP stops using it for new signals until conditions improve.

This is MIP's self-correcting mechanism. Patterns that stop working get automatically shelved.

### Maturity Stages: How Much Does MIP Know?

Not all patterns and symbol combinations have the same amount of data. MIP tracks how "mature" each combination is using four stages:

| Stage | Signal Count | What It Means |
|-------|-------------|---------------|
| **INSUFFICIENT** | Fewer than 25 | Not enough data to draw any conclusions. Too early to tell. |
| **WARMING_UP** | 25 to 49 | Starting to accumulate data, but still not enough to be confident. |
| **LEARNING** | 50 to 74 | A reasonable amount of data. Trends are becoming visible, but more would be better. |
| **CONFIDENT** | 75 or more | Plenty of data. MIP can make informed judgments about this pattern's reliability. |

Maturity is tracked per combination of pattern, symbol, and market type. For example, MIP might be "CONFIDENT" about momentum patterns for AAPL (lots of data) but still "WARMING_UP" for a recently added currency pair.

### Coverage: Data Completeness

**Coverage** answers the question: "Of all the signals we generated, how many have we been able to fully evaluate?"

Some signals are too recent -- there has not been enough time to check the 20-day horizon yet. Coverage is the fraction of signals that have complete outcome data.

**Example:** A pattern generated 100 signals. 85 of them are old enough that MIP has checked all horizons and recorded outcomes. Coverage is 85/100 = **85%**.

High coverage (above 80%) means MIP has a solid picture of how the pattern is performing. Low coverage might mean the pattern is very new, or there are data gaps.

---

## Chapter 6: Trust -- How MIP Decides What to Act On

MIP generates signals every day, but it does not act on all of them. Before a signal influences a portfolio, it must earn MIP's **trust**. This chapter explains how trust works and why it matters.

### The Trust Problem

Imagine you have two weather forecasters:

- **Forecaster A** has been working for 10 years, has made thousands of predictions, and is right 62% of the time.
- **Forecaster B** just started last week, has made 4 predictions, and has been right on all 4.

Which one do you trust more? Most people would say Forecaster A. Even though Forecaster B has a perfect track record, it is based on too little data. Four predictions could easily be luck.

MIP faces the same problem with its patterns. Some have extensive track records; others are new and unproven. The trust system ensures MIP only relies on patterns with enough evidence behind them.

### Trust Levels

MIP classifies every pattern into one of three trust levels:

| Trust Level | Meaning | Action |
|-------------|---------|--------|
| **TRUSTED** | This pattern has a proven track record with enough data, a high enough hit rate, and positive returns. | MIP will use its signals for portfolio decisions and trade proposals. |
| **WATCH** | This pattern shows some promise but does not yet meet all trust criteria. It might need more data or slightly better performance. | MIP monitors it but does not act on its signals. |
| **UNTRUSTED** | This pattern has not demonstrated reliable performance, or there is not enough data to judge. | MIP ignores its signals for trading purposes. |

### What Makes a Pattern Trusted?

MIP uses a **training gate** -- a set of minimum requirements that a pattern must pass before its signals are trusted. The gate checks three things:

| Requirement | Threshold | What It Means |
|-------------|-----------|---------------|
| **Enough signals** | At least 40 evaluated signals | The pattern must have a meaningful sample size. A few lucky wins are not enough. |
| **High enough hit rate** | At least 55% | More than half the signals must have been correct. |
| **Positive average return** | At least 0.05% | On average, following the signal must produce a positive result, even if small. |

A pattern must pass all three requirements to be classified as TRUSTED for a given market type and time interval.

### Bootstrap Mode: Giving New Patterns a Chance

There is a special allowance for brand-new patterns called **bootstrap mode**. When a pattern has between 5 and 39 evaluated signals, MIP may allow it with "LOW" confidence. This gives promising new patterns a chance to contribute to portfolios while they build up their track records, but flags them as less certain.

Once a pattern reaches 40+ signals, it graduates to regular confidence evaluation.

### The Signal Policy: A Second Layer

In addition to the training gate, MIP has a **signal policy** that considers:

- **Coverage rate**: At least 80% of the pattern's signals must have been fully evaluated (enough time has passed to check outcomes).
- **Performance**: The average return or median return must be positive.

Based on these criteria, the policy assigns labels (TRUSTED, WATCH, UNTRUSTED) and recommended actions (ENABLE, MONITOR, DISABLE).

If neither the training gate nor the policy can classify a signal (for example, a brand-new pattern with no history), MIP falls back to the **signal score**:

- Score of 0.7 or higher: classified as TRUSTED
- Score between 0.4 and 0.7: classified as WATCH
- Score below 0.4: classified as UNTRUSTED

### Why Trust Matters

Trust is the gatekeeper between "MIP noticed something" and "MIP acts on it." Only TRUSTED signals feed into:

- **Portfolio simulation**: deciding which assets to "buy" in virtual portfolios
- **Trade proposals**: the agent's daily suggestions for new positions
- **Morning briefs**: the daily summary of actionable opportunities

This prevents MIP from making portfolio decisions based on unproven or unreliable patterns. It is the system's way of saying: "I will not put money (even virtual money) behind something unless the evidence supports it."

---

## Chapter 7: Portfolios -- Simulated Investing

This chapter covers MIP's portfolio system -- how it uses trusted signals to simulate actual investing, complete with cash management, position tracking, and risk controls.

### What Is a Portfolio?

In everyday language, a portfolio is a collection of investments. In MIP, a **portfolio** is a virtual account that simulates investing with a set amount of starting cash.

Each portfolio has:

- A **name** (e.g., "Conservative Growth")
- **Starting cash** (e.g., \$100,000 in virtual money)
- A **risk profile** that controls how aggressively or conservatively MIP trades
- A **status** (ACTIVE, BUST, or STOPPED)
- Performance metrics that update every time the simulation runs

You can create multiple portfolios, each with different risk settings, to see how the same market signals play out under different strategies.

### Portfolio Profiles: The Risk Rulebook

Every portfolio is linked to a **portfolio profile** -- a reusable set of rules that define how much risk the portfolio is allowed to take. Think of it as the "personality" of the portfolio.

Here are the key settings in a profile:

#### Max Positions

How many different assets the portfolio can hold at the same time. A limit of 5 means MIP can buy up to 5 different stocks/ETFs/currencies simultaneously. This forces diversification -- the portfolio cannot put everything into one asset.

#### Max Position Size

The maximum percentage of the portfolio's cash that can go into a single asset. If this is set to 5%, and the portfolio has \$100,000 in cash, no single purchase can exceed \$5,000.

This prevents the portfolio from being too concentrated. Even if MIP finds an incredibly strong signal, it will not bet the farm on it.

#### Drawdown Stop

The **drawdown stop** is one of the most important risk controls. Drawdown measures how far the portfolio has fallen from its highest point (its "peak").

**Example:** Your portfolio reached a peak value of \$110,000, and it is now worth \$99,000. Your drawdown is:

> (\$110,000 - \$99,000) / \$110,000 = **10%**

If the profile's drawdown stop is set to 10%, MIP will stop making new purchases when drawdown reaches that level. It can still sell existing positions (to limit further losses), but it will not buy anything new until conditions improve. This is like pulling back from the table when you are on a losing streak.

#### Bust Threshold

If the portfolio drops to an extreme level -- say, below 60% of its starting cash -- the profile declares the portfolio "bust." This triggers emergency rules defined by the **bust action**:

- **ALLOW_EXITS_ONLY**: Stop all new purchases, only allow selling existing positions
- **LIQUIDATE_NEXT_BAR**: Sell everything at the next available price
- **LIQUIDATE_IMMEDIATE**: Sell everything immediately

Bust is the emergency brake. It prevents a bad strategy from grinding the portfolio all the way to zero.

#### Example Profiles

MIP comes with three pre-built profiles that illustrate different risk appetites:

| Profile | Max Positions | Max Per Position | Bust Level | Drawdown Stop | Style |
|---------|:---:|:---:|:---:|:---:|-------|
| **PRIVATE_SAVINGS** | 5 | 5% | 60% | 10% | Conservative. Small positions, tight stops. For careful investing. |
| **LOW_RISK** | 8 | 8% | 50% | 15% | Moderate. More positions, wider stops. A balanced approach. |
| **HIGH_RISK** | 15 | 15% | 35% | 30% | Aggressive. Many positions, large sizes, wide tolerance for losses. |

### How Portfolio Simulation Works

Every day, MIP's simulation engine replays the portfolio through time, bar by bar (day by day). Here is what happens on each simulated day:

#### Step 1: Sell Positions That Have Reached Their Holding Period

Each position has a predetermined holding period based on the signal's evaluation horizon. When that period is up, MIP sells the position at the current market price. The cash from the sale goes back into the portfolio.

#### Step 2: Calculate Current Portfolio Value

MIP adds up two things:

- **Cash**: Money not currently invested in any position
- **Market value of positions**: For each asset the portfolio holds, MIP multiplies the number of shares by the current market price

The sum is the portfolio's **equity** -- its total value at that moment.

#### Step 3: Check Risk Rules

Before considering any new purchases, MIP checks the risk rules:

- Has the portfolio hit the **bust threshold**? If so, apply the bust action.
- Has the **drawdown** exceeded the stop level? If so, block new entries.
- Is the portfolio in a **cooldown period** (after locking in profits)? If so, wait.

If any of these conditions are true, MIP skips buying for the day.

#### Step 4: Buy Assets Based on Trusted Signals

If the portfolio is cleared to trade, MIP looks at today's trusted signals and:

1. Filters out any assets the portfolio already owns (no doubling up)
2. Ranks the remaining signals by score (strongest first)
3. Buys the top signals, up to the remaining capacity (max positions minus current positions)
4. Sizes each purchase according to the max position size rule

For example, if the portfolio holds 3 positions and the max is 5, MIP can buy up to 2 new positions. It picks the 2 strongest signals and invests up to 5% of available cash in each.

MIP also accounts for **slippage** (the small price difference between when you decide to trade and when the trade actually happens) and **fees** (transaction costs), making the simulation more realistic.

#### Step 5: Record Everything

For each day, MIP records:

- **Trades**: Every buy and sell, with price, quantity, and fees
- **Positions**: Every asset currently held, with entry price and cost basis
- **Daily snapshot**: Cash, equity, open positions, daily return, drawdown

This creates a complete, auditable history of the portfolio's simulated life.

### Key Portfolio Metrics

After simulation, MIP calculates and displays several important performance metrics:

| Metric | What It Means | Example |
|--------|--------------|---------|
| **Starting Cash** | The initial virtual money in the portfolio | \$100,000 |
| **Final Equity** | The current total value (cash + positions) | \$108,500 |
| **Total Return** | The overall percentage gain or loss | +8.5% |
| **Max Drawdown** | The largest peak-to-trough decline at any point | -6.2% |
| **Win Days** | Number of days the portfolio gained value | 47 |
| **Loss Days** | Number of days the portfolio lost value | 38 |

**Max drawdown** deserves special attention. Even a profitable portfolio will have bad periods. Max drawdown tells you the worst slump the portfolio experienced. A max drawdown of 6.2% means that at some point, the portfolio dropped 6.2% from its highest value before recovering. This helps you understand the "pain" involved in the strategy, not just the final result.

### Episodes: Chapters in a Portfolio's Life

A portfolio's life is divided into **episodes**. An episode is a continuous period of trading that starts fresh -- either when the portfolio is first created or after a significant event like hitting a profit target or a risk stop.

Each episode has its own starting equity and its own performance metrics. When MIP locks in profits (crystallization) or when a risk rule triggers a reset, the current episode ends and a new one begins.

This is like chapters in a book. Each chapter has its own story, and you can evaluate performance per chapter or across the whole book.

### Crystallization: Locking In Profits

**Crystallization** is an optional feature where MIP locks in gains when a portfolio reaches a profit target.

**Example:** You set a profit target of 15%. The portfolio starts an episode with \$100,000 and grows to \$115,000 (a 15% gain). Crystallization triggers:

1. The current episode ends
2. The \$15,000 profit is "paid out" (recorded as realized gains)
3. A new episode starts with the base amount
4. A **cooldown period** begins (e.g., 5 days) during which no new trades are made

Crystallization acts as a "take profits and reset" mechanism. It prevents a profitable portfolio from giving back all its gains in a sudden reversal.

---

## Chapter 8: The Daily Pipeline and Advanced Features

Everything MIP does comes together in the **daily pipeline** -- an automated sequence of steps that runs every day. This chapter walks through the pipeline and then covers several advanced features.

### The Daily Pipeline: Step by Step

Each day (typically in the early morning), MIP runs its pipeline. Think of it as an assembly line where raw data enters one end and finished reports come out the other.

```
Market Data --> Returns --> Signals --> Evaluation --> Simulation --> Proposals --> Execution --> Morning Brief
```

Here is each step:

#### Step 1: Ingest Market Data

MIP downloads the latest price bars from AlphaVantage for every symbol in its universe. New data is merged into the `MARKET_BARS` table, with duplicate checks to ensure data integrity.

#### Step 2: Calculate Returns

MIP refreshes returns -- computing how much each asset's price changed since the previous bar. These returns feed into pattern detection.

#### Step 3: Generate Signals

MIP runs each active pattern definition against the fresh market data. Any asset that meets a pattern's criteria receives a new signal in the `RECOMMENDATION_LOG`.

#### Step 4: Evaluate Past Signals

MIP looks back at previously generated signals and checks their outcomes. For each signal where enough future bars now exist, MIP calculates the realized return and hit flag at each horizon. This step builds the historical scorecard that training and trust rely on.

#### Step 5: Simulate Portfolios

For each active portfolio, MIP runs the full simulation -- selling positions whose holding period is up, checking risk rules, buying new positions from trusted signals, and recording daily snapshots.

#### Step 6: Propose Trades

An automated agent reviews each portfolio's current state and proposes new trades based on:

- Trusted signals from today's data
- Available capacity (how many more positions the portfolio can hold)
- A mix of asset types (e.g., 60% stocks, 40% FX)

Proposals are recorded with status "PROPOSED."

#### Step 7: Validate and Execute

Each proposal goes through validation:

- Does the underlying signal still qualify?
- Does the position size comply with the profile's rules?
- Are entries currently allowed (risk gate not blocking)?
- Is there already a position in this symbol?

Proposals that pass all checks are **approved** and **executed** as paper trades -- written into the portfolio's trade and position tables. Failed proposals are **rejected** with a reason.

#### Step 8: Generate Morning Briefs

Finally, MIP assembles a **morning brief** for each portfolio. This is a comprehensive daily summary that includes:

- **Signals**: Which trusted signals were found today, along with watch items and changes from yesterday
- **Risk status**: Current drawdown, whether entries are blocked, and any risk warnings
- **Portfolio KPIs**: Total return, daily return, volatility, and how these compare to the previous run
- **Exposure**: Current cash, total equity, number of open positions
- **Proposals**: How many were proposed, approved, rejected, and executed -- with reasons for rejections
- **Attribution**: Realized profit/loss broken down by market type, including top contributors and detractors

The morning brief is like a daily executive summary: everything you need to know about each portfolio in one place.

### Trade Proposals: The Agent's Recommendations

The **trade proposal** system is MIP's decision-making layer. Here is how it works:

1. The agent looks at today's trusted signals from `V_TRUSTED_SIGNALS_LATEST_TS`
2. It checks the portfolio's current state: how many positions are open, is the risk gate clear, is there a cooldown?
3. It selects the best signals (highest scores), avoiding symbols already held
4. It creates proposals with a target weight (e.g., 5% of the portfolio)

Each proposal includes the signal details, the rationale, and a snapshot of the supporting data. This creates a traceable record of *why* a trade was suggested.

### Validation and Execution: The Safety Net

Before any proposal becomes a paper trade, it must pass a series of safety checks:

- **Risk gate**: If the portfolio's entries are blocked (drawdown stop, bust, or cooldown), all buy proposals are rejected
- **Signal eligibility**: The underlying signal must still be valid and eligible
- **Position limits**: The portfolio cannot exceed its maximum number of positions
- **Size limits**: No single position can exceed the profile's maximum position percentage
- **No duplicates**: Cannot buy something already in the portfolio

Only proposals that pass every check are executed. The execution applies realistic slippage and fees, then creates the trade and position records.

This two-step process (propose, then validate) creates a clear audit trail. You can always see what was proposed, what was approved or rejected, and why.

### Risk Gates: Traffic Lights for Trading

MIP uses a **risk gate** system that works like traffic lights:

| Gate Status | Color | What It Means |
|-------------|-------|---------------|
| **Normal** | Green | Everything is fine. The portfolio can buy and sell freely. |
| **Caution** | Yellow | Drawdown is approaching the stop level. Trading continues, but MIP is watching closely. |
| **Defensive** | Red | Drawdown has hit the stop level, or the portfolio has bust. No new purchases allowed -- only selling existing positions. |

The risk gate is checked before every trade decision. When the gate turns "defensive," MIP enters a protective mode where it can only reduce exposure (sell), never increase it (buy). This continues until the portfolio recovers or all positions are closed.

### Parallel Worlds: "What If?" Scenarios

**Parallel worlds** are one of MIP's most interesting features. They answer the question: "What would have happened if we had done things differently?"

For each portfolio and each day, MIP can run alternative scenarios alongside the actual results:

| Scenario Type | What It Tests | Example |
|---------------|---------------|---------|
| **THRESHOLD** | What if the signal filters were stricter or looser? | "What if we required a higher minimum return before buying?" |
| **SIZING** | What if positions were larger or smaller? | "What if we invested 75% of the normal amount per position?" |
| **TIMING** | What if we waited before acting? | "What if we delayed entry by one day after each signal?" |
| **BASELINE** | What if we did nothing at all? | "What if we just held cash and made zero trades?" |

Each scenario runs with modified parameters but uses the same market data. The results are stored separately and can be compared to the actual portfolio performance.

The **baseline "do nothing"** scenario is particularly valuable. It answers the fundamental question: "Is MIP's trading actually adding value, or would we have been better off sitting on our hands?" If the actual portfolio consistently outperforms the do-nothing baseline, that is evidence the strategy is working.

Parallel worlds also support **regret analysis** -- comparing what you did to what you could have done. If a threshold scenario outperformed your actual results, that suggests you might want to adjust your thresholds.

### Paper Trading vs. Real Trading

It is worth emphasizing once more: everything in MIP is **paper trading**. This means:

- All cash is virtual. Starting with "\$100,000" means starting with 100,000 virtual dollars.
- All trades are simulated. No orders go to any exchange or broker.
- All positions are tracked in MIP's database, not in any brokerage account.
- Performance, risk, and attribution are all computed from these simulated records.

The advantage of paper trading is that you can test strategies, learn from mistakes, and understand market dynamics without any financial risk. The disadvantage is that paper results may not perfectly match real-world results due to factors like market impact (real orders can move prices) and execution differences.

---

## Chapter 9: Using the MIP Dashboard

MIP comes with a web-based dashboard that lets you monitor everything the system does. This chapter provides a tour of each screen.

### Home

**What it is:** Your starting point. A quick health check for the entire system.

**What you see:**

- **Last Pipeline Run**: When MIP last processed data (e.g., "2 hours ago"). If this is old, something may need attention.
- **New Evaluations**: How many signal outcomes were calculated since the last run.
- **Latest Digest**: When the most recent summary was generated.
- **Quick Action Cards**: Shortcuts to Portfolios, Cockpit, Training Status, and Suggestions.

**When to use it:** Check this first to make sure MIP is running normally.

### Cockpit

**What it is:** The daily command center. This is the most information-rich screen and where you will likely spend the most time.

**What you see:**

- **System Overview**: A global digest of what happened across all portfolios and patterns.
- **Portfolio Intelligence**: Per-portfolio summaries, including signal activity and risk status.
- **Global Training Digest**: How MIP's learning is progressing across all patterns.
- **Today's Signal Candidates**: Signals generated today that might lead to trades.
- **Upcoming Symbols**: Assets approaching maturity or showing emerging trends.
- **Data Freshness**: Badges indicating whether data is "Fresh" (recently updated) or "Stale" (possibly outdated).

**When to use it:** Every day, as your primary "what is happening?" view.

### Portfolio Pages

**What they are:** Detailed views of individual portfolio performance.

**The list view** shows all portfolios side by side:

| Column | What It Means |
|--------|---------------|
| Gate | Risk status (is trading allowed?) |
| Health | Overall portfolio health indicator |
| Equity | Current total value |
| Paid Out | Profits that have been crystallized |
| Active Episode | Which trading chapter is current |
| Status | ACTIVE, BUST, or STOPPED |

**The detail view** for a single portfolio shows:

- **Header metrics**: Starting cash, final equity, total return, max drawdown, win/loss days
- **Equity chart**: A line chart showing the portfolio's value over time
- **Drawdown chart**: How far the portfolio has fallen from its peak at each point
- **Trades per day**: A bar chart of trading activity
- **Risk regime timeline**: When the portfolio was in Normal, Caution, or Defensive mode
- **Cash and exposure breakdown**: How much is in cash vs. invested
- **Open positions**: Current holdings with entry prices and current values
- **Trade history**: Every buy and sell, with prices and profit/loss
- **Risk gate status**: Current gate state and reason

### Training Status

**What it is:** A view into how MIP's patterns are learning and maturing.

**What you see:**

- **Per symbol/pattern**: Market type, symbol, pattern name, interval
- **Maturity**: Stage (INSUFFICIENT, WARMING_UP, LEARNING, CONFIDENT) and score (0-100)
- **Sample size**: How many signals have been generated
- **Coverage**: What fraction of signals have been evaluated
- **Horizons covered**: Which of the 5 horizons (1, 3, 5, 10, 20 day) have data
- **Average returns per horizon**: Average realized return at each time point (H1, H3, H5, H10, H20)

**When to use it:** To understand how confident MIP is in each pattern/symbol combination, and to identify which ones are approaching trust thresholds.

### Suggestions

**What it is:** A ranked list of the most promising signal opportunities.

MIP ranks opportunities in two tiers:

- **Strong Candidates**: Patterns with 10 or more recommendations and solid performance metrics
- **Early Signals**: Patterns with 3 to 9 recommendations that show promise but need more data

Each suggestion shows:

- **Rank**: Overall priority
- **Symbol and Pattern**: Which asset and which pattern
- **Suggestion Score**: A composite score based on maturity (60% weight), mean return (20%), and hit rate (20%)
- **Maturity stage and score**: How much data is behind this suggestion
- **What History Suggests**: Plain-language summary of the pattern's track record
- **Horizon strip**: Performance breakdown at each horizon

Click on any suggestion to see the full evidence drawer with detailed outcome data.

### Signals

**What it is:** A searchable list of all detected signals with filtering options.

Each signal shows the symbol, market type, pattern, score, trust level, recommended action, eligibility status, and signal timestamp. You can filter by:

- Symbol name
- Market type (STOCK, ETF, FX)
- Pattern name
- Trust level (TRUSTED, WATCH, UNTRUSTED)
- Run ID or date

**When to use it:** To browse individual signals and understand why MIP flagged specific assets.

### Market Timeline

**What it is:** A per-symbol deep dive that shows the complete history of MIP's interaction with an asset.

**What you see:**

- **Symbol cards**: Each tracked asset with counts of signals (S), proposals (P), and trades (T), plus trust badges
- **Expanded view**: For a selected symbol:
  - Price chart (line or candlestick)
  - Event overlays showing when signals were generated, proposals were made, and trades were executed
  - Decision narrative explaining MIP's reasoning
  - Trust summary for that symbol's patterns
  - Signal chains linking related signals over time

**When to use it:** To understand MIP's complete history with a specific asset -- every signal, every trade, and how they relate.

### Audit Viewer (Runs)

**What it is:** A log of every pipeline run with detailed step-by-step breakdowns.

**What you see:**

- **Run list**: Each pipeline run with status (SUCCESS, FAIL, RUNNING), duration, and timestamp
- **Run detail**: For a selected run:
  - Summary cards with key statistics
  - Run narrative (what happened in plain language)
  - Error panel (if anything went wrong)
  - Step timeline showing each pipeline step in order
  - Step details with execution times and row counts

**When to use it:** To diagnose pipeline issues or understand what happened during a specific run.

### Portfolio Management

**What it is:** The administrative screen for creating and configuring portfolios and risk profiles.

**What you can do:**

- **Create new portfolios**: Set a name, starting cash, and risk profile
- **Edit existing portfolios**: Adjust cash (deposits/withdrawals), change the assigned profile
- **Manage risk profiles**: Create or modify risk profiles with custom settings (max positions, position size, drawdown stop, bust threshold)
- **View lifecycle timeline**: See the portfolio's complete history of events (creation, deposits, withdrawals, crystallizations, episodes)
- **Read the portfolio story**: An AI-generated narrative biography of the portfolio's journey

A **pipeline lock** prevents edits while the daily pipeline is running, ensuring data consistency.

### Parallel Worlds

**What it is:** The "what if" analysis screen.

**What you see:**

- Scenario definitions and parameters
- Side-by-side comparison of actual vs. counterfactual results
- Regret analysis (what you could have gained or avoided)
- Equity curve overlays comparing different scenarios
- Confidence intervals and sensitivity analysis

**When to use it:** To evaluate whether your current strategy is optimal, or whether alternative approaches might yield better results.

---

## Chapter 10: The Early-Exit Layer and Decision Console

### The Problem: Catching the Fish, Then Throwing It Back

Imagine you go fishing and catch a beautiful trout. You put it in your bucket. Then, because your plan says "fish for four more hours," you keep the line in the water. A wave rolls by, tips the bucket, and the trout flops back into the lake. You had the win and lost it because you followed the plan too rigidly.

This is **giveback risk** in trading. MIP's daily positions have a planned holding period -- say, 5 bars (about a week). But sometimes, the position achieves its expected return within hours. If MIP holds mechanically until the planned exit, the market can reverse and erase those gains. The early-exit layer is MIP's solution: a system that watches your open positions in real time using 15-minute bars and can recognize when it is time to take the win and walk away.

### How the Two-Stage Policy Works

The early-exit system does not simply grab profits the instant a target is reached. That would be like leaving a party the moment you start having fun -- you would miss the best moments. Instead, it uses a **two-stage decision**, similar to how a lifeguard operates:

**Stage A: "Has the swimmer reached safety?"** (Payoff Detection)

Every time new 15-minute price bars arrive, MIP checks each open daily position:
- What is the current return since entry?
- What was the expected return for this position's pattern?
- Has the current return met or exceeded the target?

If yes, the position becomes an **early-exit candidate**. The flag goes up, but no whistle blows yet.

**Stage B: "Is the swimmer drifting back out?"** (Giveback Confirmation)

Being a candidate does not trigger an exit. MIP now watches for evidence of reversal:

- **Giveback from peak**: Has the position given back 40% or more of its best return? Imagine climbing 100 meters up a hill, then sliding 40 meters back down -- that is significant backsliding.
- **No new highs**: Have the last 3 consecutive 15-minute bars all failed to set a new high? The climb has stalled.
- **Quick payoff rule**: If the target was hit within 60 minutes of entry, MIP applies a stricter threshold (25% giveback instead of 40%), because very fast moves are statistically more likely to snap back.

Only when **both** stages confirm does MIP signal an early exit.

### Why Not Just Exit When Target Is Hit?

Consider this analogy: you are driving to a destination 100 miles away. After 80 miles, you see a sign saying "Your destination is near." Would you immediately pull over and walk the rest? Of course not -- you might be on a highway and the remaining 20 miles could take 15 minutes.

Similarly, many of MIP's best trades are **continuation moves** where hitting the initial target is just the first act. Exiting at the minimum target would systematically cut winners short while doing nothing to protect against reversals. The two-stage approach lets winners run when momentum continues, but steps in when the move is clearly fading.

### The Safety Ladder: Three Execution Modes

MIP rolls out the early-exit feature through three modes, like a pilot qualifying in a new aircraft:

1. **SHADOW Mode** (simulator flights): The system evaluates every position and logs what it *would* do, but changes nothing. Your portfolio is completely unaffected. This is the proving ground where you accumulate evidence that the system adds value.

2. **PAPER Mode** (supervised solo flights): The system actually closes positions in the simulation when an exit triggers. Original hold-to-horizon results are preserved as a baseline for comparison. You can see exactly what changed.

3. **ACTIVE Mode** (certified flights): Full execution. Only enabled after Paper mode demonstrates stable, positive results over multiple weeks.

The system starts in Shadow mode by default. Upgrading requires a deliberate configuration change -- it never auto-promotes itself.

### What Gets Recorded

Every evaluation is logged with two critical timestamps:
- **bar_close_ts** -- which 15-minute bar the decision was based on (the data)
- **decision_ts** -- when MIP actually made the decision (the action)

This separation is essential. It proves that MIP never uses future information ("time travel") in its decisions. An auditor can verify that every decision was made using only data available at the time.

Beyond timestamps, each log entry records: target return, current return, peak return (MFE), giveback percentage, gate results (pass/fail for each check), fee-adjusted P&L, and the delta versus holding to the planned horizon.

### The Decision Console

The Decision Console is MIP's real-time command center -- think of it as a flight control tower for your positions. It has three modes:

**Open Positions** shows every position with a color-coded stage badge:
- 🟢 **On Track** -- target not yet reached, monitoring normally
- 🟡 **Candidate** -- target reached, watching for reversal
- 🟡 **Watching** -- significant giveback detected
- 🔴 **Exit Triggered** -- both stages passed, exit signal fired

**Live Decisions** is a rolling feed of decision events that updates automatically via Server-Sent Events (SSE) -- no manual refresh needed. Each event appears as a "story card" with severity coloring, a concise summary, and key metrics. You can click any event to see the full gate trace.

**History** replays past events for any date, so you can review what happened after market close.

The **Position Inspector** (right panel) shows two powerful views:
- **Decision Diff**: "If we exit now, we lock in X. If we hold, we expect Y." This comparison makes the system feel like a smart advisor, not just a logger.
- **Gate Trace Timeline**: A vertical timeline of every evaluation, with pass/fail pills for each check and expandable advanced JSON for full audit detail.

### How the Early-Exit Layer Fits Into the Pipeline

The early-exit evaluation runs as Step 4 of the existing intraday pipeline:

```
Ingest 15m bars → Generate signals → Evaluate outcomes → Evaluate early exits → Log run
```

It piggybacks on the same hourly pipeline that already ingests intraday data. If intraday bars are missing for any symbol, that position is silently skipped -- no crashes, no errors, no disruption to anything else.

### Cost Awareness

The Decision Console polls Snowflake every 30 minutes (not continuously) to keep data warehouse costs minimal. Since the intraday pipeline runs hourly, this polling frequency ensures you see fresh data within one cycle while keeping the warehouse idle most of the time.

---

## Appendix A: Glossary

A plain-language reference for every term used in MIP, organized alphabetically.

**Backtest**
A replay of historical data to evaluate how a pattern would have performed in the past. MIP uses backtests to decide which patterns should stay active.

**Bar (OHLCV)**
A summary of price activity during a single time period, containing five numbers: Open (starting price), High (highest price), Low (lowest price), Close (ending price), and Volume (number of shares or contracts traded). One bar = one time period = one snapshot.

**Bootstrap Mode**
An early-stage allowance where MIP can provisionally trust a pattern with as few as 5 evaluated signals (instead of the normal 40). Marked with "LOW" confidence until more data accumulates.

**Bust**
When a portfolio's total value drops below a critical threshold (defined by the profile's bust equity percentage). Triggers emergency protective actions like stopping all new purchases or liquidating all positions.

**Cost Basis**
The average price paid for a position, including any fees. Used to calculate profit or loss when the position is sold.

**Coverage**
The fraction of a pattern's signals that have been fully evaluated (enough time has passed to check all horizons). High coverage means MIP has a complete picture; low coverage means many signals are still too recent to judge.

**Crystallization**
The process of locking in profits when a portfolio reaches a defined profit target. Ends the current episode, records the gains, and starts a new episode after a cooldown period.

**Decision Console**
A live UI page that shows open daily positions, their real-time decision lifecycle (monitoring, candidate detection, exit triggers), and a rolling event feed. Functions as both a system log and a decision trace explorer. Includes a Position Inspector with gate trace timeline and Decision Diff comparison.

**Decision Diff**
A comparison view in the Decision Console showing what happens if a position is exited now versus held to its planned horizon. Displays current return, expected return, P&L delta, and bars remaining.

**Decision Event**
A structured record emitted whenever the early-exit system evaluates or transitions a position through a gate. Contains timestamps, metrics, gate results, reason codes, and links to advanced JSON detail.

**Drawdown**
How far a portfolio has fallen from its highest-ever value (its peak), expressed as a percentage. A drawdown of 8% means the portfolio is currently 8% below its best point.

**Drawdown Stop**
A risk rule that blocks new purchases when drawdown exceeds a defined threshold. If the stop is set to 10%, MIP will not buy anything new once the portfolio has dropped 10% from its peak.

**Early Exit**
An execution optimization that can close daily positions before their planned horizon when intraday price action indicates the payoff has been achieved and giveback risk is high. Uses a two-stage policy: payoff detection followed by reversal confirmation.

**Entry Gate**
The mechanism that controls whether a portfolio is allowed to make new purchases. The gate can be "open" (buying allowed) or "closed" (buying blocked) based on risk conditions.

**Episode**
A chapter in a portfolio's life. Each episode has its own starting point, performance metrics, and ending condition. A new episode starts after crystallization, a manual reset, or a risk event.

**Equity**
The total value of a portfolio: cash on hand plus the current market value of all positions. If you have \$50,000 in cash and hold stocks currently worth \$60,000, your equity is \$110,000.

**ETF (Exchange-Traded Fund)**
A bundle of assets (like stocks or bonds) packaged together and traded as a single security on an exchange. Lets you invest in a broad category with one purchase.

**Evaluation Status**
Indicates whether an outcome could be fully calculated. "SUCCESS" means enough future data existed. "INSUFFICIENT_FUTURE_DATA" means the signal is too recent to evaluate at that horizon.

**FX (Foreign Exchange)**
The market for trading currencies. FX prices represent exchange rates between two currencies (e.g., EUR/USD = how many dollars one euro is worth).

**Gate Trace**
A timeline of every decision gate evaluated for a position over time. Each node shows the gate name, pass/fail result, key metrics, and timestamps. Accessible in the Decision Console's Position Inspector.

**Giveback Risk**
The risk that a position which has already reached its target return will reverse and lose those gains before the planned exit. The early-exit system monitors for this by tracking the peak return and the subsequent price decay.

**Hit Flag**
A yes/no indicator for whether a signal's realized return met the minimum threshold (default: 0%). TRUE means the signal made money (or at least broke even); FALSE means it lost money.

**Hit Rate**
The percentage of a pattern's signals that were "hits" (met the minimum return threshold). A hit rate of 58% means 58 out of every 100 signals were correct. MIP requires at least 55% for trust.

**Horizon**
The number of future bars (days) after a signal at which MIP evaluates the outcome. MIP uses horizons of 1, 3, 5, 10, and 20 days to understand both short-term and longer-term signal accuracy.

**Ingestion**
The process of downloading and storing fresh market data from an external provider (AlphaVantage). MIP ingests data daily as the first step of its pipeline.

**Intraday**
Refers to time periods shorter than one full trading day. Intraday bars (e.g., 15-minute bars) capture more granular price movements within a single day.

**Learning Cycle**
MIP's full feedback loop: generate signals, evaluate outcomes, run a backtest, and update pattern status. This is how MIP continuously improves its pattern selection.

**Maturity**
How much data MIP has accumulated for a specific pattern/symbol combination. Ranges from INSUFFICIENT (fewer than 25 signals) through WARMING_UP, LEARNING, to CONFIDENT (75+ signals).

**Max Favorable Excursion (MFE)**
The highest unrealized return a position achieves before it is closed. MFE measures how much profit was "on the table" at the best point -- useful for evaluating whether exits are well-timed.

**Max Drawdown**
The largest peak-to-trough decline a portfolio experienced at any point during its history. Represents the worst-case scenario the strategy went through.

**Momentum**
A trading concept (and MIP's primary pattern type) based on the observation that assets with recent strong performance tend to continue performing well in the near term.

**Morning Brief**
A daily JSON summary produced for each portfolio at the end of the pipeline. Contains signals, risk status, portfolio KPIs, exposure details, proposal outcomes, and attribution data.

**OHLCV**
See "Bar (OHLCV)."

**Outcome**
The measured result of a signal at a specific horizon. Contains the entry price, exit price, realized return, and hit flag. Answers the question: "Did this signal work?"

**Paper Trading**
Simulated trading that uses virtual money instead of real money. All trades in MIP are paper trades -- no actual financial transactions occur.

**Parallel Worlds**
Counterfactual scenarios that ask "what would have happened if we used different rules?" MIP can test alternative thresholds, position sizes, timing, or a do-nothing baseline alongside actual results.

**Pattern**
A defined set of rules that examines price data and flags potentially interesting opportunities. Each pattern has parameters (like lookback windows and minimum thresholds) and a performance history.

**Pipeline**
The automated sequence of steps MIP runs each day: ingest data, calculate returns, generate signals, evaluate outcomes, simulate portfolios, propose trades, execute trades, and generate briefs.

**Pipeline Lock**
A safety mechanism that prevents portfolio edits while the daily pipeline is running, ensuring data consistency.

**Portfolio**
A virtual investment account with starting cash, a risk profile, and a history of simulated trades and positions.

**Position**
A holding in a specific asset. If MIP "buys" 100 shares of AAPL at \$150, that is one position with a cost basis of \$15,000.

**Proposal**
A suggested trade generated by MIP's agent. Proposals are created based on trusted signals and portfolio capacity, then validated against risk rules before execution.

**Realized Return**
The actual percentage gain or loss from a trade or signal, measured from the entry price to the exit price. "Realized" means it has actually happened (as opposed to an unrealized gain on a position you still hold).

**Recommendation**
See "Signal." In MIP's database, signals are stored as "recommendations" in the RECOMMENDATION_LOG table.

**Return**
The percentage change in an asset's price over a period. A return of +3% means the price went up 3%. A return of -2% means it went down 2%.

**Risk Gate**
The system that controls trading permissions for a portfolio. Can be in Normal (green -- trade freely), Caution (yellow -- approaching limits), or Defensive (red -- no new buying) mode.

**Risk Profile**
A reusable template of risk rules (max positions, position sizes, drawdown stops, bust thresholds) that can be assigned to one or more portfolios.

**Run ID**
A unique identifier (UUID) assigned to each pipeline execution. Used to trace every action back to the specific run that caused it.

**Score**
A number indicating how strong a signal is. For momentum signals, the score is typically the asset's recent return. Higher scores indicate stronger price movements.

**Server-Sent Events (SSE)**
A web protocol where the server pushes updates to the browser over a persistent connection. The Decision Console uses SSE to deliver new decision events without manual page refresh.

**Shadow Mode**
An early-exit execution mode where the system evaluates positions and logs hypothetical exit decisions, but does not actually close any positions. Used for proof-of-concept during the initial rollout period (typically 2--4 weeks).

**Signal**
A detected trading opportunity. When a pattern's rules are met for a specific asset at a specific time, MIP records a signal. Signals are the raw material that portfolios act on (after trust filtering).

**Simulation**
The process of replaying market data through a portfolio with defined rules, as if the trades were actually happening. MIP's simulation accounts for position sizing, risk limits, slippage, and fees.

**Slippage**
The small difference between the expected price of a trade and the actual execution price. In real markets, slippage happens because prices move slightly between when you decide to trade and when the trade is filled. MIP simulates slippage to make results more realistic.

**Spread**
The difference between the price at which you can buy an asset (the "ask") and the price at which you can sell it (the "bid"). MIP can simulate spread costs for more realistic portfolio performance.

**Strict Lookahead**
MIP's rule that signal evaluation can only use data from *after* the signal was generated, never data from the same time or earlier. Prevents artificially inflated performance results.

**Trade**
A record of a simulated buy or sell. Each trade includes the symbol, side (BUY or SELL), price, quantity, fees, and resulting cash balance.

**Trust**
A classification of how reliable a pattern's signals are, based on historical performance. Trust levels are TRUSTED (proven), WATCH (promising but unproven), and UNTRUSTED (unreliable or insufficient data).

**Universe**
The configured set of symbols that MIP tracks. Only assets in the universe receive daily data downloads and pattern analysis.

**Z-Score**
A statistical measure of how unusual a value is compared to normal. A z-score of 0 means "perfectly average." A z-score of 2 means "about twice as far from average as typical." MIP uses z-scores to identify price movements that are notably stronger than usual for that asset.

---

## Appendix B: Frequently Asked Questions

### Is MIP trading with real money?

**No.** MIP is a paper trading system. All money is virtual, all trades are simulated, and no real financial transactions occur. MIP has no connection to any broker or exchange. The numbers in MIP represent what *would* have happened if you had made those trades, but no actual money is at risk.

### Can MIP lose my money?

**No.** Because MIP uses only virtual money, there is nothing real to lose. A portfolio going "bust" in MIP means the simulated value dropped below a threshold -- it is a learning experience, not a financial loss.

### How accurate are the signals?

It depends on the pattern, the asset, and market conditions. That is exactly what MIP's training and trust systems measure. A pattern with a 60% hit rate is "right" about 60% of the time at a given horizon. Some patterns perform better with certain assets or in certain market environments. MIP's trust system ensures that only patterns with demonstrated accuracy are used for portfolio decisions.

### What happens when a pattern stops working?

MIP's training system catches this automatically. During backtesting, if a pattern's hit rate drops below the minimum threshold (55%), or its cumulative return turns negative, MIP deactivates the pattern. It stops generating new signals from that pattern until performance improves. This is one of MIP's most important self-correcting mechanisms.

### Why does MIP need so many signals before trusting a pattern?

**Statistical significance.** If you flip a coin 5 times and get heads 4 times (80%), you would not conclude the coin is rigged. That is too small a sample -- it could easily be luck. But if you flip it 1,000 times and get heads 550 times (55%), that is much more convincing.

The same principle applies to signals. A pattern that has been right 3 out of 4 times could easily be lucky. But one that has been right 55 out of 100 times is demonstrating a genuine, if modest, edge. MIP's requirement of 40+ signals (or 30+ for general training) ensures decisions are based on meaningful evidence, not coincidence.

### What is the difference between a signal and a trade?

A **signal** is a detection: "This asset meets the pattern's criteria right now." A **trade** is an action: "Buy (or sell) this asset in a portfolio." Many signals are generated, but only a fraction become trades. Signals must first earn trust, then survive portfolio capacity limits and risk gate checks, and finally pass validation before becoming paper trades.

### Can I create my own patterns?

MIP's pattern system is configured through pattern definitions with adjustable parameters (lookback windows, minimum returns, z-score thresholds). New pattern definitions can be added to the system, and MIP will automatically begin generating signals, evaluating outcomes, and building a track record for them.

### What does "drawdown" really mean in practical terms?

Imagine your portfolio grew from \$100,000 to \$120,000 -- a new peak. Then it dropped to \$108,000. Your drawdown at that point is (\$120,000 - \$108,000) / \$120,000 = 10%. It does not matter that you are still up \$8,000 from the start. Drawdown measures the decline from the *best* point, not the starting point. It captures the emotional and financial reality of watching gains evaporate, even if you are still technically profitable overall.

### How does MIP handle market holidays and weekends?

Markets are closed on weekends and holidays, so no new bars are produced on those days. MIP's simulation is aware of this -- when it needs a price for a day when the market was closed, it carries forward the last available closing price. The pipeline only processes days where market data actually exists.

### What is the difference between the simulation and the proposal/execution system?

The **simulation** replays the entire portfolio history from start to finish, recalculating all trades based on signals and rules. The **proposal/execution** system operates on a single day: it looks at today's trusted signals, proposes trades for the current moment, validates them against current risk rules, and executes them. The simulation gives you the "big picture replay"; the proposal system gives you the "what should we do right now" decision.

### Can different portfolios act on the same signal?

Yes. If MIP generates a trusted signal for AAPL, multiple portfolios can each buy AAPL based on that signal. Each portfolio applies its own risk rules, position sizing, and capacity limits independently. The same signal might lead to a trade in one portfolio but not another (because the other portfolio is full, or its risk gate is blocking entries).

---

### Does the early-exit system change my portfolio automatically?

**Not by default.** The early-exit system starts in Shadow mode, which means it evaluates every position and logs what it *would* do, but takes no action. Your portfolio is completely unaffected. To enable actual early exits, you must explicitly switch to Paper mode (applies to simulated positions) or Active mode (future real execution). The mode is controlled by the `EARLY_EXIT_MODE` configuration setting.

### How much does the Decision Console cost to run?

Very little. The console polls Snowflake every 30 minutes (not continuously), so the data warehouse spends most of its time idle. The underlying intraday pipeline runs once per hour during market hours. There is no continuous warehouse usage from the Decision Console.

### Can I see why an early exit was triggered or not triggered?

Yes. The Decision Console's Position Inspector shows a full gate trace timeline for every position. Each evaluation node shows which gates passed, which failed, the exact metrics used, and timestamps. You can also expand the "Advanced" section on any node to see the raw JSON with every detail.

---

*This handbook is a living document. As MIP evolves with new features and capabilities, this guide will be updated to reflect them.*
