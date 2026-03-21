use role MIP_ADMIN_ROLE;
use database MIP;

MERGE INTO MIP.APP.PATTERN_DEFINITION tgt
USING (
    SELECT column1 AS NAME, column2 AS DESCRIPTION, column3 AS PATTERN_TYPE,
           parse_json(column4) AS PARAMS_JSON
    FROM VALUES
        ('BEARISH_STOCK_DAILY',
         'Bearish momentum for stocks: detects sustained downtrends for DEFENSIVE use (suppress bullish entries, trigger early exits). NOT for short positions.',
         'BEARISH_MOMENTUM',
         '{"fast_window": 3, "min_drop_pct": 0.01, "min_negative_lag_count": 2, "min_zscore": 1.0, "lookback_days": 30, "market_type": "STOCK", "interval_minutes": 1440}'),
        ('BEARISH_ETF_DAILY',
         'Bearish momentum for ETFs: detects sustained downtrends for DEFENSIVE use (suppress bullish entries, trigger early exits). NOT for short positions.',
         'BEARISH_MOMENTUM',
         '{"fast_window": 3, "min_drop_pct": 0.008, "min_negative_lag_count": 2, "min_zscore": 0.8, "lookback_days": 30, "market_type": "ETF", "interval_minutes": 1440}'),
        ('BEARISH_FX_DAILY',
         'Bearish momentum for FX: detects sustained downtrends for DEFENSIVE use (suppress bullish entries, trigger early exits). NOT for short positions.',
         'BEARISH_MOMENTUM',
         '{"fast_window": 3, "min_drop_pct": 0.005, "min_negative_lag_count": 2, "min_zscore": 0.8, "lookback_days": 30, "market_type": "FX", "interval_minutes": 1440}')
) AS src
ON tgt.NAME = src.NAME
WHEN MATCHED THEN UPDATE SET
    DESCRIPTION = src.DESCRIPTION,
    PATTERN_TYPE = src.PATTERN_TYPE,
    PARAMS_JSON = src.PARAMS_JSON,
    IS_ACTIVE = 'Y',
    ENABLED = true
WHEN NOT MATCHED THEN INSERT (NAME, DESCRIPTION, PATTERN_TYPE, PARAMS_JSON, IS_ACTIVE, ENABLED)
    VALUES (src.NAME, src.DESCRIPTION, src.PATTERN_TYPE, src.PARAMS_JSON, 'Y', true);
