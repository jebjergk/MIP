use role MIP_ADMIN_ROLE;
use database MIP;

-- Expand universe with 10 new symbols covering sector gaps:
-- STOCK: V (fintech), CRM (cloud/SaaS), PLTR (defense/AI), COST (consumer staples),
--        LLY (pharma/biotech), UBER (transportation/tech), COIN (crypto proxy)
-- ETF:   GLD (gold/commodity), TLT (treasury bonds)
-- FX:    EUR/GBP (European cross)

MERGE INTO MIP.APP.INGEST_UNIVERSE tgt
USING (
    SELECT column1 AS SYMBOL, column2 AS MARKET_TYPE, column3 AS INTERVAL_MINUTES,
           column4 AS IS_ENABLED, column5 AS PRIORITY, column6 AS NOTES
    FROM VALUES
        ('V',       'STOCK', 1440, true, 100, 'Visa - fintech leader, highly liquid'),
        ('CRM',     'STOCK', 1440, true, 100, 'Salesforce - cloud/SaaS bellwether'),
        ('PLTR',    'STOCK', 1440, true, 120, 'Palantir - defense/AI, volatile mean-reversion candidate'),
        ('COST',    'STOCK', 1440, true, 100, 'Costco - consumer staples, low-vol momentum candidate'),
        ('LLY',     'STOCK', 1440, true, 100, 'Eli Lilly - pharma/biotech, strong trend stock'),
        ('UBER',    'STOCK', 1440, true, 100, 'Uber - transportation/tech, liquid'),
        ('COIN',    'STOCK', 1440, true,  50, 'Coinbase - crypto proxy, high volatility'),
        ('GLD',     'ETF',   1440, true, 115, 'Gold ETF - commodity exposure, mean-reverts well'),
        ('TLT',     'ETF',   1440, true, 115, 'Treasury Bond ETF - rate-sensitive, mean-reversion candidate'),
        ('EUR/GBP', 'FX',    1440, true, 110, 'Euro/British Pound cross - European pair, adds FX diversity')
) AS src
ON tgt.SYMBOL = src.SYMBOL
    AND tgt.MARKET_TYPE = src.MARKET_TYPE
    AND tgt.INTERVAL_MINUTES = src.INTERVAL_MINUTES
WHEN MATCHED THEN UPDATE SET
    IS_ENABLED = src.IS_ENABLED,
    PRIORITY = src.PRIORITY,
    NOTES = src.NOTES
WHEN NOT MATCHED THEN INSERT (SYMBOL, MARKET_TYPE, INTERVAL_MINUTES, IS_ENABLED, PRIORITY, NOTES)
    VALUES (src.SYMBOL, src.MARKET_TYPE, src.INTERVAL_MINUTES, src.IS_ENABLED, src.PRIORITY, src.NOTES);
