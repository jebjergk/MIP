-- Source table: TRADERMADE_CURRENCY_EXCHANGE_RATES.PUBLIC.TICK_DATA_SAMPLE
-- Purpose: Provide a cleaned, generic FX tick representation for MIP that standardizes timestamps
--          and enriches every record with derived MID and SPREAD measures for downstream marts.

CREATE OR REPLACE VIEW MIP.MART.FX_TICKS_BASE AS
SELECT
    TO_TIMESTAMP_NTZ(t.tick_time)                          AS as_of_ts,
    t.symbol                                               AS symbol,
    t.bid                                                  AS bid,
    t.ask                                                  AS ask,
    (t.bid + t.ask) / 2                                    AS mid,
    (t.ask - t.bid)                                        AS spread
FROM TRADERMADE_CURRENCY_EXCHANGE_RATES.PUBLIC.TICK_DATA_SAMPLE t;

COMMENT ON COLUMN MIP.MART.FX_TICKS_BASE.AS_OF_TS IS 'Standardized timestamp (ntz) sourced from TICK_TIME in the TraderMade sample feed.';
COMMENT ON COLUMN MIP.MART.FX_TICKS_BASE.SYMBOL  IS 'Currency pair symbol provided by TraderMade (e.g., EUR/USD).';
COMMENT ON COLUMN MIP.MART.FX_TICKS_BASE.BID     IS 'Raw bid quote from the TraderMade sample feed.';
COMMENT ON COLUMN MIP.MART.FX_TICKS_BASE.ASK     IS 'Raw ask quote from the TraderMade sample feed.';
COMMENT ON COLUMN MIP.MART.FX_TICKS_BASE.MID     IS 'Mid-market rate derived as (BID + ASK) / 2.';
COMMENT ON COLUMN MIP.MART.FX_TICKS_BASE.SPREAD  IS 'Bid/ask spread derived as ASK - BID.';
