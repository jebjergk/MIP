-- Fix glossary entries that failed due to semicolons in values

-- P&L
MERGE INTO MIP.APP.GLOSSARY_TERM t
USING (SELECT 'p&l' AS TERM_KEY) s ON t.TERM_KEY = s.TERM_KEY
WHEN NOT MATCHED THEN INSERT (
  TERM_KEY, DISPLAY_TERM, ALIASES, CATEGORY,
  DEFINITION_SHORT, MIP_SPECIFIC_MEANING, GENERAL_MARKET_MEANING,
  EXAMPLE_IN_MIP, SOURCE_TYPE, SOURCE_REF,
  IS_APPROVED, REVIEW_STATUS, CREATED_AT, UPDATED_AT
) VALUES (
  'p&l', 'P&L', '["PnL","profit and loss","profit & loss","pnl"]', 'trading',
  'Profit and Loss, the financial gain or loss from trading activity.',
  'In MIP, P&L is shown as both Unrealized P&L (open positions) and Realized P&L (closed trades). Unrealized P&L appears on the Live Portfolio Activity page as a KPI card and sparkline chart. Realized P&L appears in the Execution History section when trades are closed.',
  'P&L measures the difference between revenue from trades and costs. Unrealized P&L is the paper gain or loss on open positions. Realized P&L is locked in when positions are closed.',
  'On Live Portfolio Activity, the Unrealized P&L card shows the aggregate paper gain or loss across all open positions with a trend sparkline.',
  'SEED', 'ask_mip_trading_seed', TRUE, 'approved', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
)
;

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
  'Gross exposure is the total absolute value of all positions (longs plus shorts). Net exposure is longs minus shorts. High exposure means more capital at risk and low exposure means more cash is idle.',
  'MIP tracks exposure to ensure portfolios stay within risk limits defined by the risk overlay.',
  'SEED', 'ask_mip_trading_seed', TRUE, 'approved', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
)
;

-- Stop Loss
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
  'In MIP, stop-loss orders are part of bracket protection. The Symbol Tracker shows Distance to SL (remaining safety buffer). PROTECTED_FULL means both TP and SL are active at the broker. UNPROTECTED means neither exists.',
  'A stop-loss order automatically exits a position when the price moves against you past a certain level, preventing larger losses.',
  'On Symbol Tracker, Distance to SL shows the remaining safety buffer. On Live Portfolio Activity, the protection status column shows whether SL orders are armed.',
  'SEED', 'ask_mip_trading_seed', TRUE, 'approved', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
)
;

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
  'A measure of risk-adjusted return, how much return you get per unit of risk taken.',
  'In MIP, the Sharpe Ratio may appear on the Performance Dashboard as a KPI for comparing portfolio quality across strategies.',
  'Sharpe Ratio equals (Portfolio Return minus Risk-Free Rate) divided by Standard Deviation of Returns. Above 1.0 is generally considered good, above 2.0 is excellent. Higher values mean better return for the risk taken.',
  'Compare Sharpe Ratios across portfolios on the Performance Dashboard to identify which strategies deliver the best risk-adjusted returns.',
  'SEED', 'ask_mip_trading_seed', TRUE, 'approved', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
)
;

-- Position
MERGE INTO MIP.APP.GLOSSARY_TERM t
USING (SELECT 'position' AS TERM_KEY) s ON t.TERM_KEY = s.TERM_KEY
WHEN NOT MATCHED THEN INSERT (
  TERM_KEY, DISPLAY_TERM, ALIASES, CATEGORY,
  DEFINITION_SHORT, MIP_SPECIFIC_MEANING, GENERAL_MARKET_MEANING,
  EXAMPLE_IN_MIP, SOURCE_TYPE, SOURCE_REF,
  IS_APPROVED, REVIEW_STATUS, CREATED_AT, UPDATED_AT
) VALUES (
  'position', 'Position', '["open position","trade position","holding"]', 'trading',
  'An active holding in a financial instrument, either long (bought) or short (sold short).',
  'In MIP, positions are shown in the Open Positions section of Live Portfolio Activity with symbol, side, quantity, cost, market value, and unrealized P&L.',
  'A position represents your stake in a market. A long position profits when prices rise and a short position profits when prices fall.',
  'The Open Positions table on Live Portfolio Activity lists all current holdings with their P&L and protection status.',
  'SEED', 'ask_mip_trading_seed', TRUE, 'approved', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
)
;

-- Slippage (re-insert since it showed 0 rows, may have had existing empty entry)
MERGE INTO MIP.APP.GLOSSARY_TERM t
USING (SELECT 'slippage' AS TERM_KEY) s ON t.TERM_KEY = s.TERM_KEY
WHEN MATCHED THEN UPDATE SET
  DISPLAY_TERM = 'Slippage',
  ALIASES = '["slip","execution slippage","fill slippage"]',
  CATEGORY = 'trading',
  DEFINITION_SHORT = 'The difference between the expected price of a trade and the actual fill price.',
  MIP_SPECIFIC_MEANING = 'In MIP, slippage is factored into cost attribution on the Performance Dashboard. It represents execution quality loss between intended and actual fill price.',
  GENERAL_MARKET_MEANING = 'Slippage occurs because market prices can move between the time a trade is decided and when it is actually filled. Higher slippage means worse execution quality.',
  EXAMPLE_IN_MIP = 'The Cost Attribution section on Performance Dashboard breaks down costs including slippage.',
  IS_APPROVED = TRUE,
  REVIEW_STATUS = 'approved',
  UPDATED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
  TERM_KEY, DISPLAY_TERM, ALIASES, CATEGORY,
  DEFINITION_SHORT, MIP_SPECIFIC_MEANING, GENERAL_MARKET_MEANING,
  EXAMPLE_IN_MIP, SOURCE_TYPE, SOURCE_REF,
  IS_APPROVED, REVIEW_STATUS, CREATED_AT, UPDATED_AT
) VALUES (
  'slippage', 'Slippage', '["slip","execution slippage","fill slippage"]', 'trading',
  'The difference between the expected price of a trade and the actual fill price.',
  'In MIP, slippage is factored into cost attribution on the Performance Dashboard. It represents execution quality loss between intended and actual fill price.',
  'Slippage occurs because market prices can move between the time a trade is decided and when it is actually filled. Higher slippage means worse execution quality.',
  'The Cost Attribution section on Performance Dashboard breaks down costs including slippage.',
  'SEED', 'ask_mip_trading_seed', TRUE, 'approved', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
)
;
