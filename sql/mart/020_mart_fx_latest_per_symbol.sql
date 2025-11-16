-- Utility view exposing the most recent FX tick per symbol for higher-level signal logic
-- and UI pages to consume consistently.

CREATE OR REPLACE VIEW MIP.MART.FX_LATEST_PER_SYMBOL AS
WITH ranked_ticks AS (
    SELECT
        as_of_ts,
        symbol,
        bid,
        ask,
        mid,
        spread,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY as_of_ts DESC) AS rn
    FROM MIP.MART.FX_TICKS_BASE
)
SELECT
    as_of_ts,
    symbol,
    bid,
    ask,
    mid,
    spread
FROM ranked_ticks
WHERE rn = 1;
