-- Seed comprehensive trading/market glossary terms for Ask MIP
-- These cover common market, trading, and portfolio concepts users may ask about.

-- P&L (Profit and Loss)
MERGE INTO MIP.APP.GLOSSARY_TERM t
USING (SELECT 'p&l' AS TERM_KEY) s ON t.TERM_KEY = s.TERM_KEY
WHEN NOT MATCHED THEN INSERT (
  TERM_KEY, DISPLAY_TERM, ALIASES, CATEGORY,
  DEFINITION_SHORT, MIP_SPECIFIC_MEANING, GENERAL_MARKET_MEANING,
  EXAMPLE_IN_MIP, SOURCE_TYPE, SOURCE_REF,
  IS_APPROVED, REVIEW_STATUS, CREATED_AT, UPDATED_AT
) VALUES (
  'p&l', 'P&L', '["PnL","profit and loss","profit & loss","pnl"]', 'trading',
  'Profit and Loss — the financial gain or loss from trading activity.',
  'In MIP, P&L is shown as both Unrealized P&L (open positions) and Realized P&L (closed trades). Unrealized P&L appears on the Live Portfolio Activity page as a KPI card and sparkline chart. Realized P&L appears in the Execution History section when trades are closed.',
  'P&L measures the difference between revenue from trades and costs. Unrealized P&L is the paper gain/loss on open positions; Realized P&L is locked in when positions are closed.',
  'On Live Portfolio Activity, the Unrealized P&L card shows the aggregate paper gain/loss across all open positions with a trend sparkline.',
  'SEED', 'ask_mip_trading_seed', TRUE, 'approved', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);

-- Unrealized P&L
MERGE INTO MIP.APP.GLOSSARY_TERM t
USING (SELECT 'unrealized p&l' AS TERM_KEY) s ON t.TERM_KEY = s.TERM_KEY
WHEN NOT MATCHED THEN INSERT (
  TERM_KEY, DISPLAY_TERM, ALIASES, CATEGORY,
  DEFINITION_SHORT, MIP_SPECIFIC_MEANING, GENERAL_MARKET_MEANING,
  EXAMPLE_IN_MIP, SOURCE_TYPE, SOURCE_REF,
  IS_APPROVED, REVIEW_STATUS, CREATED_AT, UPDATED_AT
) VALUES (
  'unrealized p&l', 'Unrealized P&L', '["unrealised P&L","unrealized pnl","unrealised pnl","paper profit","paper loss","open P&L","unrealized profit","unrealized loss"]', 'trading',
  'The theoretical profit or loss on positions that are still open — not yet closed or sold.',
  'In MIP, Unrealized P&L is displayed on the Live Portfolio Activity page as a prominent KPI card and trend sparkline. It shows the aggregate paper gain/loss of all currently open positions. Each individual position also shows its own unrealized P&L in the Open Positions table.',
  'Unrealized P&L is calculated as (current market value - cost basis) for each open position. It changes with every price tick. Gains are not locked in until the position is closed.',
  'The Live Portfolio Activity page shows a sparkline chart labeled "Unrealized P&L" tracking the trend over recent snapshots, plus a signed dollar value showing the current total.',
  'SEED', 'ask_mip_trading_seed', TRUE, 'approved', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);

-- Realized P&L
MERGE INTO MIP.APP.GLOSSARY_TERM t
USING (SELECT 'realized p&l' AS TERM_KEY) s ON t.TERM_KEY = s.TERM_KEY
WHEN NOT MATCHED THEN INSERT (
  TERM_KEY, DISPLAY_TERM, ALIASES, CATEGORY,
  DEFINITION_SHORT, MIP_SPECIFIC_MEANING, GENERAL_MARKET_MEANING,
  EXAMPLE_IN_MIP, SOURCE_TYPE, SOURCE_REF,
  IS_APPROVED, REVIEW_STATUS, CREATED_AT, UPDATED_AT
) VALUES (
  'realized p&l', 'Realized P&L', '["realised P&L","realized pnl","realised pnl","closed P&L","locked in profit"]', 'trading',
  'Actual profit or loss from positions that have been closed.',
  'In MIP, Realized P&L appears in the Execution History section of Live Portfolio Activity. A tilde (~) prefix indicates the value is estimated rather than confirmed.',
  'Realized P&L is the actual gain/loss recorded when a position is closed. Unlike unrealized P&L, it cannot change after the trade is completed.',
  'In the Execution History table on Live Portfolio Activity, each closed trade shows its realized P&L. Green means profitable, red means a loss.',
  'SEED', 'ask_mip_trading_seed', TRUE, 'approved', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);

-- Equity
MERGE INTO MIP.APP.GLOSSARY_TERM t
USING (SELECT 'equity' AS TERM_KEY) s ON t.TERM_KEY = s.TERM_KEY
WHEN NOT MATCHED THEN INSERT (
  TERM_KEY, DISPLAY_TERM, ALIASES, CATEGORY,
  DEFINITION_SHORT, MIP_SPECIFIC_MEANING, GENERAL_MARKET_MEANING,
  EXAMPLE_IN_MIP, SOURCE_TYPE, SOURCE_REF,
  IS_APPROVED, REVIEW_STATUS, CREATED_AT, UPDATED_AT
) VALUES (
  'equity', 'Equity', '["total equity","portfolio equity","account equity","equity curve"]', 'trading',
  'Total value of a portfolio account, including cash and all positions at current market prices.',
  'In MIP, equity is shown on the Performance Dashboard as an Equity Curve chart and on the Cockpit as part of portfolio KPIs. It represents the total account value tracked from the broker (IBKR).',
  'Portfolio equity = cash + market value of all positions. An equity curve plots this value over time.',
  'The Performance Dashboard shows an Equity Curve line chart tracking total equity over time for each portfolio.',
  'SEED', 'ask_mip_trading_seed', TRUE, 'approved', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);

-- Drawdown
MERGE INTO MIP.APP.GLOSSARY_TERM t
USING (SELECT 'drawdown' AS TERM_KEY) s ON t.TERM_KEY = s.TERM_KEY
WHEN MATCHED THEN UPDATE SET
  DEFINITION_SHORT = 'The percentage decline from a portfolio''s peak equity to its lowest point before a new high.',
  MIP_SPECIFIC_MEANING = 'In MIP, drawdown is tracked on the Performance Dashboard and Cockpit. Max Drawdown is shown as a KPI. Episode drawdown is used for risk gate decisions — if drawdown exceeds the threshold, the risk gate moves from SAFE to CAUTION or STOPPED.',
  GENERAL_MARKET_MEANING = 'Drawdown measures how much a portfolio has fallen from its highest value. A -5% drawdown means the portfolio is 5% below its all-time high. Lower drawdowns indicate more consistent risk management.',
  EXAMPLE_IN_MIP = 'On the Cockpit, you might see "Episode drawdown at 4.2% (threshold: 5.0%)" warning that the portfolio is approaching its risk limit.',
  UPDATED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
  TERM_KEY, DISPLAY_TERM, ALIASES, CATEGORY,
  DEFINITION_SHORT, MIP_SPECIFIC_MEANING, GENERAL_MARKET_MEANING,
  EXAMPLE_IN_MIP, SOURCE_TYPE, SOURCE_REF,
  IS_APPROVED, REVIEW_STATUS, CREATED_AT, UPDATED_AT
) VALUES (
  'drawdown', 'Drawdown', '["max drawdown","maximum drawdown","DD","peak to trough"]', 'trading',
  'The percentage decline from a portfolio''s peak equity to its lowest point before a new high.',
  'In MIP, drawdown is tracked on the Performance Dashboard and Cockpit. Max Drawdown is shown as a KPI. Episode drawdown is used for risk gate decisions — if drawdown exceeds the threshold, the risk gate moves from SAFE to CAUTION or STOPPED.',
  'Drawdown measures how much a portfolio has fallen from its highest value. A -5% drawdown means the portfolio is 5% below its all-time high. Lower drawdowns indicate more consistent risk management.',
  'On the Cockpit, you might see "Episode drawdown at 4.2% (threshold: 5.0%)" warning that the portfolio is approaching its risk limit.',
  'SEED', 'ask_mip_trading_seed', TRUE, 'approved', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);

-- Slippage
MERGE INTO MIP.APP.GLOSSARY_TERM t
USING (SELECT 'slippage' AS TERM_KEY) s ON t.TERM_KEY = s.TERM_KEY
WHEN NOT MATCHED THEN INSERT (
  TERM_KEY, DISPLAY_TERM, ALIASES, CATEGORY,
  DEFINITION_SHORT, MIP_SPECIFIC_MEANING, GENERAL_MARKET_MEANING,
  EXAMPLE_IN_MIP, SOURCE_TYPE, SOURCE_REF,
  IS_APPROVED, REVIEW_STATUS, CREATED_AT, UPDATED_AT
) VALUES (
  'slippage', 'Slippage', '["slip","execution slippage","fill slippage"]', 'trading',
  'The difference between the expected price of a trade and the actual fill price.',
  'In MIP, slippage is factored into cost attribution on the Performance Dashboard. It represents execution quality loss between intended and actual fill price.',
  'Slippage occurs because market prices can move between the time a trade is decided and when it is actually filled. Higher slippage means worse execution quality. It is especially relevant for larger orders or less liquid markets.',
  'The Cost Attribution section on Performance Dashboard breaks down costs including slippage.',
  'SEED', 'ask_mip_trading_seed', TRUE, 'approved', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);

-- Exposure
MERGE INTO MIP.APP.GLOSSARY_TERM t
USING (SELECT 'exposure' AS TERM_KEY) s ON t.TERM_KEY = s.TERM_KEY
WHEN NOT MATCHED THEN INSERT (
  TERM_KEY, DISPLAY_TERM, ALIASES, CATEGORY,
  DEFINITION_SHORT, MIP_SPECIFIC_MEANING, GENERAL_MARKET_MEANING,
  EXAMPLE_IN_MIP, SOURCE_TYPE, SOURCE_REF,
  IS_APPROVED, REVIEW_STATUS, CREATED_AT, UPDATED_AT
) VALUES (
  'exposure', 'Exposure', '["market exposure","portfolio exposure","gross exposure","net exposure"]', 'trading',
  'The total amount of capital at risk in the market through open positions.',
  'In MIP, exposure is tracked through position sizing and risk overlay controls. The system monitors how much capital is deployed versus cash reserves.',
  'Gross exposure is the total absolute value of all positions (longs + shorts). Net exposure is longs minus shorts. High exposure means more capital at risk; low exposure means more cash is sitting idle.',
  'MIP tracks exposure to ensure portfolios stay within risk limits defined by the risk overlay.',
  'SEED', 'ask_mip_trading_seed', TRUE, 'approved', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);

-- Take Profit (TP)
MERGE INTO MIP.APP.GLOSSARY_TERM t
USING (SELECT 'take profit' AS TERM_KEY) s ON t.TERM_KEY = s.TERM_KEY
WHEN NOT MATCHED THEN INSERT (
  TERM_KEY, DISPLAY_TERM, ALIASES, CATEGORY,
  DEFINITION_SHORT, MIP_SPECIFIC_MEANING, GENERAL_MARKET_MEANING,
  EXAMPLE_IN_MIP, SOURCE_TYPE, SOURCE_REF,
  IS_APPROVED, REVIEW_STATUS, CREATED_AT, UPDATED_AT
) VALUES (
  'take profit', 'Take Profit', '["TP","target price","profit target","take-profit"]', 'trading',
  'A preset price level at which a position is automatically closed to lock in gains.',
  'In MIP, take-profit orders are part of bracket protection. The Symbol Tracker shows Distance to TP, and Live Portfolio Activity shows protection status (PROTECTED_FULL means both TP and SL are armed at the broker).',
  'A take-profit order automatically sells (or covers) a position when the price reaches a target level, locking in the profit without manual intervention.',
  'On Symbol Tracker, "Distance to TP" shows how far the current price is from the take-profit target.',
  'SEED', 'ask_mip_trading_seed', TRUE, 'approved', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);

-- Stop Loss (SL)
MERGE INTO MIP.APP.GLOSSARY_TERM t
USING (SELECT 'stop loss' AS TERM_KEY) s ON t.TERM_KEY = s.TERM_KEY
WHEN NOT MATCHED THEN INSERT (
  TERM_KEY, DISPLAY_TERM, ALIASES, CATEGORY,
  DEFINITION_SHORT, MIP_SPECIFIC_MEANING, GENERAL_MARKET_MEANING,
  EXAMPLE_IN_MIP, SOURCE_TYPE, SOURCE_REF,
  IS_APPROVED, REVIEW_STATUS, CREATED_AT, UPDATED_AT
) VALUES (
  'stop loss', 'Stop Loss', '["SL","stop","stop-loss","stoploss","protective stop"]', 'trading',
  'A preset price level at which a position is automatically closed to limit losses.',
  'In MIP, stop-loss orders are part of bracket protection. The Symbol Tracker shows Distance to SL (remaining safety buffer). PROTECTED_FULL means both TP and SL are active at the broker; UNPROTECTED means neither exists.',
  'A stop-loss order automatically exits a position when the price moves against you past a certain level, preventing larger losses.',
  'On Symbol Tracker, "Distance to SL" shows the remaining safety buffer. On Live Portfolio Activity, the protection status column shows whether SL orders are armed.',
  'SEED', 'ask_mip_trading_seed', TRUE, 'approved', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);

-- Risk Gate
MERGE INTO MIP.APP.GLOSSARY_TERM t
USING (SELECT 'risk gate' AS TERM_KEY) s ON t.TERM_KEY = s.TERM_KEY
WHEN NOT MATCHED THEN INSERT (
  TERM_KEY, DISPLAY_TERM, ALIASES, CATEGORY,
  DEFINITION_SHORT, MIP_SPECIFIC_MEANING, GENERAL_MARKET_MEANING,
  EXAMPLE_IN_MIP, SOURCE_TYPE, SOURCE_REF,
  IS_APPROVED, REVIEW_STATUS, CREATED_AT, UPDATED_AT
) VALUES (
  'risk gate', 'Risk Gate', '["gate","risk overlay","safety gate","gate status"]', 'mip',
  'A safety mechanism that controls whether new trade entries are allowed based on current risk levels.',
  'In MIP, the risk gate has three states: SAFE (green, new entries allowed), CAUTION (yellow, approaching limit), and STOPPED (red, no new entries). The gate status is driven by episode drawdown thresholds and is visible on the Cockpit portfolio cards.',
  'Risk gates are portfolio-level controls that automatically restrict trading when risk metrics exceed predefined thresholds.',
  'On the Cockpit, each portfolio card shows a Gate Badge: SAFE (green), CAUTION (yellow), or STOPPED (red).',
  'SEED', 'ask_mip_trading_seed', TRUE, 'approved', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);

-- Sharpe Ratio
MERGE INTO MIP.APP.GLOSSARY_TERM t
USING (SELECT 'sharpe ratio' AS TERM_KEY) s ON t.TERM_KEY = s.TERM_KEY
WHEN NOT MATCHED THEN INSERT (
  TERM_KEY, DISPLAY_TERM, ALIASES, CATEGORY,
  DEFINITION_SHORT, MIP_SPECIFIC_MEANING, GENERAL_MARKET_MEANING,
  EXAMPLE_IN_MIP, SOURCE_TYPE, SOURCE_REF,
  IS_APPROVED, REVIEW_STATUS, CREATED_AT, UPDATED_AT
) VALUES (
  'sharpe ratio', 'Sharpe Ratio', '["sharpe","risk-adjusted return","risk adjusted return"]', 'trading',
  'A measure of risk-adjusted return — how much return you get per unit of risk taken.',
  'In MIP, the Sharpe Ratio may appear on the Performance Dashboard as a KPI for comparing portfolio quality across strategies.',
  'Sharpe Ratio = (Portfolio Return - Risk-Free Rate) / Standard Deviation of Returns. A ratio above 1.0 is generally good; above 2.0 is excellent. Higher values mean better return for the risk taken.',
  'Compare Sharpe Ratios across portfolios on the Performance Dashboard to identify which strategies deliver the best risk-adjusted returns.',
  'SEED', 'ask_mip_trading_seed', TRUE, 'approved', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);

-- Win Rate
MERGE INTO MIP.APP.GLOSSARY_TERM t
USING (SELECT 'win rate' AS TERM_KEY) s ON t.TERM_KEY = s.TERM_KEY
WHEN NOT MATCHED THEN INSERT (
  TERM_KEY, DISPLAY_TERM, ALIASES, CATEGORY,
  DEFINITION_SHORT, MIP_SPECIFIC_MEANING, GENERAL_MARKET_MEANING,
  EXAMPLE_IN_MIP, SOURCE_TYPE, SOURCE_REF,
  IS_APPROVED, REVIEW_STATUS, CREATED_AT, UPDATED_AT
) VALUES (
  'win rate', 'Win Rate', '["hit rate","win percentage","batting average","success rate"]', 'trading',
  'The percentage of trades that were profitable out of all closed trades.',
  'In MIP, win rate (also called hit rate) is tracked per symbol/pattern on Training Status and as a Decision Quality metric on the Performance Dashboard. It is one factor in determining trust-readiness.',
  'Win rate alone does not determine profitability. A strategy can have a low win rate but still be profitable if winners are much larger than losers (high reward-to-risk ratio).',
  'On Training Status, check the hit rate alongside average returns to assess whether a pattern is genuinely reliable.',
  'SEED', 'ask_mip_trading_seed', TRUE, 'approved', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);

-- Expectancy
MERGE INTO MIP.APP.GLOSSARY_TERM t
USING (SELECT 'expectancy' AS TERM_KEY) s ON t.TERM_KEY = s.TERM_KEY
WHEN NOT MATCHED THEN INSERT (
  TERM_KEY, DISPLAY_TERM, ALIASES, CATEGORY,
  DEFINITION_SHORT, MIP_SPECIFIC_MEANING, GENERAL_MARKET_MEANING,
  EXAMPLE_IN_MIP, SOURCE_TYPE, SOURCE_REF,
  IS_APPROVED, REVIEW_STATUS, CREATED_AT, UPDATED_AT
) VALUES (
  'expectancy', 'Expectancy', '["expected value","edge","trading edge","expected gain"]', 'trading',
  'The average amount you expect to gain (or lose) per trade, combining win rate and average win/loss size.',
  'In MIP, expectancy is shown on the Performance Dashboard in the Decision Quality Trend chart. Positive expectancy means the strategy has a statistical edge.',
  'Expectancy = (Win Rate × Average Win) - (Loss Rate × Average Loss). Positive expectancy means the strategy is expected to be profitable over many trades.',
  'The Decision Quality Trend chart on Performance Dashboard tracks expectancy over time — an upward trend means improving edge.',
  'SEED', 'ask_mip_trading_seed', TRUE, 'approved', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);

-- Volatility
MERGE INTO MIP.APP.GLOSSARY_TERM t
USING (SELECT 'volatility' AS TERM_KEY) s ON t.TERM_KEY = s.TERM_KEY
WHEN NOT MATCHED THEN INSERT (
  TERM_KEY, DISPLAY_TERM, ALIASES, CATEGORY,
  DEFINITION_SHORT, MIP_SPECIFIC_MEANING, GENERAL_MARKET_MEANING,
  EXAMPLE_IN_MIP, SOURCE_TYPE, SOURCE_REF,
  IS_APPROVED, REVIEW_STATUS, CREATED_AT, UPDATED_AT
) VALUES (
  'volatility', 'Volatility', '["vol","price volatility","market volatility","implied vol"]', 'trading',
  'A measure of how much and how quickly prices move. Higher volatility means larger price swings.',
  'In MIP, the Vol Regime indicator on Symbol Tracker compares live volatility to the trained volatility context. Volatility expansion is a factor in trade evaluation and risk management.',
  'Volatility is typically measured as the standard deviation of returns. High volatility means more risk but also more opportunity. Low volatility means smaller moves and calmer markets.',
  'On Symbol Tracker, the Vol Regime column shows whether current volatility is normal, elevated, or compressed relative to what the model was trained on.',
  'SEED', 'ask_mip_trading_seed', TRUE, 'approved', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);

-- Maturity Score
MERGE INTO MIP.APP.GLOSSARY_TERM t
USING (SELECT 'maturity' AS TERM_KEY) s ON t.TERM_KEY = s.TERM_KEY
WHEN NOT MATCHED THEN INSERT (
  TERM_KEY, DISPLAY_TERM, ALIASES, CATEGORY,
  DEFINITION_SHORT, MIP_SPECIFIC_MEANING, GENERAL_MARKET_MEANING,
  EXAMPLE_IN_MIP, SOURCE_TYPE, SOURCE_REF,
  IS_APPROVED, REVIEW_STATUS, CREATED_AT, UPDATED_AT
) VALUES (
  'maturity', 'Maturity Score', '["maturity score","learning stage","maturity stage","training maturity"]', 'mip',
  'A quality score (0-100) showing how complete and reliable the evidence is for a pattern/symbol.',
  'In MIP, maturity progresses through stages: INSUFFICIENT → WARMING_UP → LEARNING → CONFIDENT. Higher maturity means more evidence. A symbol must reach CONFIDENT before it can be trusted for trading proposals.',
  'N/A — maturity score is a MIP-specific concept for measuring statistical readiness of a trading pattern.',
  'On Training Status, maturity appears as a score and stage badge. On the Cockpit, Signal Candidates show maturity scores with progress bars.',
  'SEED', 'ask_mip_trading_seed', TRUE, 'approved', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);

-- Position / Trade Position
MERGE INTO MIP.APP.GLOSSARY_TERM t
USING (SELECT 'position' AS TERM_KEY) s ON t.TERM_KEY = s.TERM_KEY
WHEN NOT MATCHED THEN INSERT (
  TERM_KEY, DISPLAY_TERM, ALIASES, CATEGORY,
  DEFINITION_SHORT, MIP_SPECIFIC_MEANING, GENERAL_MARKET_MEANING,
  EXAMPLE_IN_MIP, SOURCE_TYPE, SOURCE_REF,
  IS_APPROVED, REVIEW_STATUS, CREATED_AT, UPDATED_AT
) VALUES (
  'position', 'Position', '["open position","trade position","holding"]', 'trading',
  'An active holding in a financial instrument — either long (bought) or short (sold short).',
  'In MIP, positions are shown in the Open Positions section of Live Portfolio Activity with symbol, side, quantity, cost, market value, and unrealized P&L.',
  'A position represents your stake in a market. A long position profits when prices rise; a short position profits when prices fall.',
  'The Open Positions table on Live Portfolio Activity lists all current holdings with their P&L and protection status.',
  'SEED', 'ask_mip_trading_seed', TRUE, 'approved', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);

-- Long / Short
MERGE INTO MIP.APP.GLOSSARY_TERM t
USING (SELECT 'long' AS TERM_KEY) s ON t.TERM_KEY = s.TERM_KEY
WHEN NOT MATCHED THEN INSERT (
  TERM_KEY, DISPLAY_TERM, ALIASES, CATEGORY,
  DEFINITION_SHORT, MIP_SPECIFIC_MEANING, GENERAL_MARKET_MEANING,
  EXAMPLE_IN_MIP, SOURCE_TYPE, SOURCE_REF,
  IS_APPROVED, REVIEW_STATUS, CREATED_AT, UPDATED_AT
) VALUES (
  'long', 'Long', '["long position","buy","going long","bullish position"]', 'trading',
  'A position that profits when the price goes up. You buy an asset expecting its value to increase.',
  'In MIP, long positions appear with side = LONG/BUY in the positions and order tables on Live Portfolio Activity.',
  'Going long means buying an asset. You profit if the price rises above your purchase price and lose if it falls below.',
  'On Live Portfolio Activity, the Side column shows BUY (for long entries) with a green chip.',
  'SEED', 'ask_mip_trading_seed', TRUE, 'approved', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);

MERGE INTO MIP.APP.GLOSSARY_TERM t
USING (SELECT 'short' AS TERM_KEY) s ON t.TERM_KEY = s.TERM_KEY
WHEN NOT MATCHED THEN INSERT (
  TERM_KEY, DISPLAY_TERM, ALIASES, CATEGORY,
  DEFINITION_SHORT, MIP_SPECIFIC_MEANING, GENERAL_MARKET_MEANING,
  EXAMPLE_IN_MIP, SOURCE_TYPE, SOURCE_REF,
  IS_APPROVED, REVIEW_STATUS, CREATED_AT, UPDATED_AT
) VALUES (
  'short', 'Short', '["short position","sell short","going short","shorting","bearish position"]', 'trading',
  'A position that profits when the price goes down. You sell an asset you do not own, expecting to buy it back cheaper.',
  'In MIP, short positions appear with side = SHORT/SELL in the positions and order tables on Live Portfolio Activity.',
  'Short selling means selling borrowed shares and buying them back later. You profit if the price falls below your sell price.',
  'On Live Portfolio Activity, the Side column shows SELL (for short entries) with a red chip.',
  'SEED', 'ask_mip_trading_seed', TRUE, 'approved', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);

-- Spread
MERGE INTO MIP.APP.GLOSSARY_TERM t
USING (SELECT 'spread' AS TERM_KEY) s ON t.TERM_KEY = s.TERM_KEY
WHEN NOT MATCHED THEN INSERT (
  TERM_KEY, DISPLAY_TERM, ALIASES, CATEGORY,
  DEFINITION_SHORT, MIP_SPECIFIC_MEANING, GENERAL_MARKET_MEANING,
  EXAMPLE_IN_MIP, SOURCE_TYPE, SOURCE_REF,
  IS_APPROVED, REVIEW_STATUS, CREATED_AT, UPDATED_AT
) VALUES (
  'spread', 'Spread', '["bid-ask spread","bid ask","spread cost"]', 'trading',
  'The difference between the buy price (ask) and sell price (bid) of an asset.',
  'In MIP, spread is part of execution costs tracked in the Cost Attribution section of the Performance Dashboard.',
  'The spread is an implicit cost of trading. Tighter spreads (smaller difference) mean lower costs. Wider spreads are common in less liquid markets or during volatile periods.',
  'Check the Cost Attribution chart on the Performance Dashboard to see how much spread costs contribute to total trading costs.',
  'SEED', 'ask_mip_trading_seed', TRUE, 'approved', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);

-- Broker / IBKR
MERGE INTO MIP.APP.GLOSSARY_TERM t
USING (SELECT 'ibkr' AS TERM_KEY) s ON t.TERM_KEY = s.TERM_KEY
WHEN NOT MATCHED THEN INSERT (
  TERM_KEY, DISPLAY_TERM, ALIASES, CATEGORY,
  DEFINITION_SHORT, MIP_SPECIFIC_MEANING, GENERAL_MARKET_MEANING,
  EXAMPLE_IN_MIP, SOURCE_TYPE, SOURCE_REF,
  IS_APPROVED, REVIEW_STATUS, CREATED_AT, UPDATED_AT
) VALUES (
  'ibkr', 'IBKR (Interactive Brokers)', '["Interactive Brokers","IB","broker","brokerage"]', 'mip',
  'Interactive Brokers — the brokerage platform MIP connects to for live/paper trading execution.',
  'MIP integrates with IBKR through Live Portfolio Configuration. The IBKR account ID is configured per portfolio, and MIP syncs positions, orders, and snapshots from IBKR. Accounts starting with DU are paper trading accounts.',
  'Interactive Brokers is a major electronic brokerage firm providing trading access to stocks, options, futures, forex, and more.',
  'On Live Portfolio Config, you enter your IBKR Account ID (e.g., DU12345 for paper). The Refresh From IB button syncs the latest data.',
  'SEED', 'ask_mip_trading_seed', TRUE, 'approved', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);

-- Drift
MERGE INTO MIP.APP.GLOSSARY_TERM t
USING (SELECT 'drift' AS TERM_KEY) s ON t.TERM_KEY = s.TERM_KEY
WHEN NOT MATCHED THEN INSERT (
  TERM_KEY, DISPLAY_TERM, ALIASES, CATEGORY,
  DEFINITION_SHORT, MIP_SPECIFIC_MEANING, GENERAL_MARKET_MEANING,
  EXAMPLE_IN_MIP, SOURCE_TYPE, SOURCE_REF,
  IS_APPROVED, REVIEW_STATUS, CREATED_AT, UPDATED_AT
) VALUES (
  'drift', 'Drift', '["portfolio drift","drift state","execution drift"]', 'mip',
  'How much the actual portfolio state has deviated from its expected/intended state.',
  'In MIP, drift is shown as a KPI on Live Portfolio Activity with states: ALIGNED (no drift), MINOR_DRIFT (small deviation), SIGNIFICANT_DRIFT (large deviation). Significant drift may require reconciliation before trading.',
  'Drift occurs when actual positions differ from what the system expects — due to manual trades, partial fills, corporate actions, or sync issues.',
  'On Live Portfolio Activity, the Drift KPI card shows the current drift state. If SIGNIFICANT_DRIFT appears, use Refresh From IB to reconcile.',
  'SEED', 'ask_mip_trading_seed', TRUE, 'approved', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);

-- GTC (Good Till Cancelled)
MERGE INTO MIP.APP.GLOSSARY_TERM t
USING (SELECT 'gtc' AS TERM_KEY) s ON t.TERM_KEY = s.TERM_KEY
WHEN NOT MATCHED THEN INSERT (
  TERM_KEY, DISPLAY_TERM, ALIASES, CATEGORY,
  DEFINITION_SHORT, MIP_SPECIFIC_MEANING, GENERAL_MARKET_MEANING,
  EXAMPLE_IN_MIP, SOURCE_TYPE, SOURCE_REF,
  IS_APPROVED, REVIEW_STATUS, CREATED_AT, UPDATED_AT
) VALUES (
  'gtc', 'GTC (Good Till Cancelled)', '["good till cancelled","good til canceled","GTC order"]', 'trading',
  'An order type that remains active until it is either filled or explicitly cancelled.',
  'In MIP, bracket orders (take-profit and stop-loss) are typically placed as GTC orders at the broker so they remain active until the target price is hit.',
  'Unlike day orders that expire at market close, GTC orders persist across trading sessions. They are commonly used for stop-loss and take-profit orders.',
  'Protection orders on Live Portfolio Activity are placed as GTC at the broker — they stay active until the price target is reached or the order is manually cancelled.',
  'SEED', 'ask_mip_trading_seed', TRUE, 'approved', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);

-- Sparkline
MERGE INTO MIP.APP.GLOSSARY_TERM t
USING (SELECT 'sparkline' AS TERM_KEY) s ON t.TERM_KEY = s.TERM_KEY
WHEN NOT MATCHED THEN INSERT (
  TERM_KEY, DISPLAY_TERM, ALIASES, CATEGORY,
  DEFINITION_SHORT, MIP_SPECIFIC_MEANING, GENERAL_MARKET_MEANING,
  EXAMPLE_IN_MIP, SOURCE_TYPE, SOURCE_REF,
  IS_APPROVED, REVIEW_STATUS, CREATED_AT, UPDATED_AT
) VALUES (
  'sparkline', 'Sparkline', '["mini chart","trend line","spark chart","mini sparkline"]', 'ui',
  'A small, inline chart that shows a trend at a glance without axes or labels.',
  'In MIP, sparklines appear on Live Portfolio Activity as mini trend charts next to KPI values (NAV trend, Unrealized P&L trend, position count trend). They give you a quick visual of whether a metric is trending up or down.',
  'Sparklines are small, word-sized graphics embedded inline to show the general shape of variation over time.',
  'On Live Portfolio Activity, sparklines next to the NAV and Unrealized P&L cards show the recent trend direction.',
  'SEED', 'ask_mip_trading_seed', TRUE, 'approved', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);

-- Snapshot
MERGE INTO MIP.APP.GLOSSARY_TERM t
USING (SELECT 'snapshot' AS TERM_KEY) s ON t.TERM_KEY = s.TERM_KEY
WHEN NOT MATCHED THEN INSERT (
  TERM_KEY, DISPLAY_TERM, ALIASES, CATEGORY,
  DEFINITION_SHORT, MIP_SPECIFIC_MEANING, GENERAL_MARKET_MEANING,
  EXAMPLE_IN_MIP, SOURCE_TYPE, SOURCE_REF,
  IS_APPROVED, REVIEW_STATUS, CREATED_AT, UPDATED_AT
) VALUES (
  'snapshot', 'Snapshot', '["account snapshot","portfolio snapshot","data snapshot"]', 'mip',
  'A point-in-time capture of portfolio state (positions, values, NAV) synced from the broker.',
  'In MIP, snapshots are periodically pulled from IBKR. Snapshot trends on Live Portfolio Activity show how KPIs (NAV, unrealized P&L) have changed across recent captures. Snapshot freshness indicates how recently the data was refreshed.',
  'A snapshot is a frozen view of data at a specific moment. In portfolio management, it captures all positions, values, and metrics at that instant.',
  'Snapshot Trends on Live Portfolio Activity show NAV and unrealized P&L across the last several broker syncs.',
  'SEED', 'ask_mip_trading_seed', TRUE, 'approved', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);

-- Committee
MERGE INTO MIP.APP.GLOSSARY_TERM t
USING (SELECT 'committee' AS TERM_KEY) s ON t.TERM_KEY = s.TERM_KEY
WHEN NOT MATCHED THEN INSERT (
  TERM_KEY, DISPLAY_TERM, ALIASES, CATEGORY,
  DEFINITION_SHORT, MIP_SPECIFIC_MEANING, GENERAL_MARKET_MEANING,
  EXAMPLE_IN_MIP, SOURCE_TYPE, SOURCE_REF,
  IS_APPROVED, REVIEW_STATUS, CREATED_AT, UPDATED_AT
) VALUES (
  'committee', 'Committee', '["AI committee","committee view","committee verdict","trade committee","committee decision"]', 'mip',
  'The AI-driven decision body that evaluates proposed trades and issues verdicts (approve/reject/maybe) with explanations.',
  'In MIP, the committee is a multi-agent AI system that reviews each trade proposal. It considers pattern evidence, risk context, market conditions, and news intelligence. Committee decisions are visible on the AI Agent Decisions page with verdicts, summaries, and reason codes.',
  'An investment committee traditionally refers to a group of people who review and approve/reject investment decisions. In MIP, this is automated through AI agents.',
  'On AI Agent Decisions, each row shows the committee verdict (APPROVED/REJECTED), summary explanation, and reason codes for the decision.',
  'SEED', 'ask_mip_trading_seed', TRUE, 'approved', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);

-- Signal
MERGE INTO MIP.APP.GLOSSARY_TERM t
USING (SELECT 'signal' AS TERM_KEY) s ON t.TERM_KEY = s.TERM_KEY
WHEN NOT MATCHED THEN INSERT (
  TERM_KEY, DISPLAY_TERM, ALIASES, CATEGORY,
  DEFINITION_SHORT, MIP_SPECIFIC_MEANING, GENERAL_MARKET_MEANING,
  EXAMPLE_IN_MIP, SOURCE_TYPE, SOURCE_REF,
  IS_APPROVED, REVIEW_STATUS, CREATED_AT, UPDATED_AT
) VALUES (
  'signal', 'Signal', '["trade signal","trading signal","signal candidate","signal detection"]', 'mip',
  'A detection by a pattern that interesting price action occurred — not a trade, just evidence.',
  'In MIP, signals are detected by patterns scanning market data. Signal candidates appear on the Cockpit with maturity scores. Only signals from trusted patterns can progress to proposals and committee review.',
  'In trading, a signal is an indication that conditions are met for a potential trade. It may come from technical analysis, fundamental data, or algorithmic detection.',
  'On the Cockpit, Signal Candidates shows today''s strongest detections ranked by maturity, with explanations of why each qualifies.',
  'SEED', 'ask_mip_trading_seed', TRUE, 'approved', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);
