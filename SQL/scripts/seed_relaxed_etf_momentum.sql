use role MIP_ADMIN_ROLE;
use database MIP;

MERGE INTO MIP.APP.PATTERN_DEFINITION tgt
USING (
    SELECT column1 AS NAME, column2 AS DESCRIPTION, column3 AS PATTERN_TYPE,
           parse_json(column4) AS PARAMS_JSON
    FROM VALUES
        ('ETF_MOMENTUM_RELAXED',
         'Relaxed daily momentum for ETFs: lower return threshold (0.3%) and z-score (0.5) to catch subtle but real trends in low-volatility index ETFs',
         'MOMENTUM',
         '{"fast_window": 3, "slow_window": 0, "min_return": 0.003, "min_zscore": 0.5, "lookback_days": 90, "market_type": "ETF", "interval_minutes": 1440}')
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
