use role MIP_ADMIN_ROLE;
use database MIP;

MERGE INTO MIP.APP.PATTERN_DEFINITION tgt
USING (
    SELECT column1 AS NAME, column2 AS DESCRIPTION, column3 AS PATTERN_TYPE,
           parse_json(column4) AS PARAMS_JSON
    FROM VALUES
        ('MEANREV_STOCK_DAILY',
         'Daily mean-reversion for stocks: buys oversold bounces when price deviates >1.5% from 20-day VWAP proxy',
         'MEAN_REVERSION',
         '{"anchor_window": 20, "deviation_threshold_pct": 0.015, "min_bars_for_anchor": 10, "direction": "BOTH", "market_type": "STOCK", "interval_minutes": 1440}'),
        ('MEANREV_ETF_DAILY',
         'Daily mean-reversion for ETFs: buys oversold bounces when price deviates >1.2% from 20-day VWAP proxy',
         'MEAN_REVERSION',
         '{"anchor_window": 20, "deviation_threshold_pct": 0.012, "min_bars_for_anchor": 10, "direction": "BOTH", "market_type": "ETF", "interval_minutes": 1440}'),
        ('MEANREV_FX_DAILY',
         'Daily mean-reversion for FX: buys oversold bounces when price deviates >0.8% from 20-day VWAP proxy',
         'MEAN_REVERSION',
         '{"anchor_window": 20, "deviation_threshold_pct": 0.008, "min_bars_for_anchor": 10, "direction": "BOTH", "market_type": "FX", "interval_minutes": 1440}')
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
