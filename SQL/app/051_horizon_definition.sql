-- /sql/app/051_horizon_definition.sql
-- Purpose: Unified horizon metadata for daily and intraday evaluation
-- Supports BAR (n-th future bar), DAY (alias for daily BAR), and SESSION (end-of-day close)

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;

CREATE TABLE IF NOT EXISTS MIP.APP.HORIZON_DEFINITION (
    HORIZON_ID        INT            NOT NULL,
    HORIZON_TYPE      VARCHAR(20)    NOT NULL,   -- 'BAR', 'DAY', 'SESSION'
    HORIZON_LENGTH    INT            NOT NULL,   -- bars/days forward (-1 for SESSION)
    RESOLUTION        VARCHAR(20)    NOT NULL,   -- 'DAILY', 'INTRADAY'
    INTERVAL_MINUTES  INT            NOT NULL,   -- 1440 for daily, 15 for intraday
    DISPLAY_LABEL     VARCHAR(50)    NOT NULL,   -- human-readable label
    DISPLAY_SHORT     VARCHAR(10)    NOT NULL,   -- column key: 'H1', 'H4', 'EOD'
    DESCRIPTION       VARCHAR(200),
    IS_ACTIVE         BOOLEAN        DEFAULT TRUE,
    CREATED_AT        TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (HORIZON_ID)
);

-- Daily horizons (functionally identical to the previous hardcoded set)
MERGE INTO MIP.APP.HORIZON_DEFINITION t
USING (
    SELECT column1::INT AS HORIZON_ID,
           column2::VARCHAR AS HORIZON_TYPE,
           column3::INT AS HORIZON_LENGTH,
           column4::VARCHAR AS RESOLUTION,
           column5::INT AS INTERVAL_MINUTES,
           column6::VARCHAR AS DISPLAY_LABEL,
           column7::VARCHAR AS DISPLAY_SHORT,
           column8::VARCHAR AS DESCRIPTION
    FROM VALUES
        (1,  'DAY', 1,  'DAILY',    1440, '1 day',   'H1',  '1-day forward return'),
        (2,  'DAY', 3,  'DAILY',    1440, '3 days',  'H3',  '3-day forward return'),
        (3,  'DAY', 5,  'DAILY',    1440, '5 days',  'H5',  '5-day forward return'),
        (4,  'DAY', 10, 'DAILY',    1440, '10 days', 'H10', '10-day forward return'),
        (5,  'DAY', 20, 'DAILY',    1440, '20 days', 'H20', '20-day forward return'),
        (101, 'BAR',     1,  'INTRADAY', 15, '+1 bar (15m)',   'H1',  'Immediate signal validation'),
        (102, 'BAR',     4,  'INTRADAY', 15, '+4 bars (~1hr)', 'H4',  '~1 hour continuation'),
        (103, 'BAR',     8,  'INTRADAY', 15, '+8 bars (~2hr)', 'H8',  '~2 hour persistence'),
        (104, 'SESSION', -1, 'INTRADAY', 15, 'EOD close',      'EOD', 'End-of-day session impact')
) s ON t.HORIZON_ID = s.HORIZON_ID
WHEN MATCHED THEN UPDATE SET
    t.HORIZON_TYPE     = s.HORIZON_TYPE,
    t.HORIZON_LENGTH   = s.HORIZON_LENGTH,
    t.RESOLUTION       = s.RESOLUTION,
    t.INTERVAL_MINUTES = s.INTERVAL_MINUTES,
    t.DISPLAY_LABEL    = s.DISPLAY_LABEL,
    t.DISPLAY_SHORT    = s.DISPLAY_SHORT,
    t.DESCRIPTION      = s.DESCRIPTION,
    t.IS_ACTIVE        = TRUE
WHEN NOT MATCHED THEN INSERT
    (HORIZON_ID, HORIZON_TYPE, HORIZON_LENGTH, RESOLUTION, INTERVAL_MINUTES,
     DISPLAY_LABEL, DISPLAY_SHORT, DESCRIPTION, IS_ACTIVE)
VALUES
    (s.HORIZON_ID, s.HORIZON_TYPE, s.HORIZON_LENGTH, s.RESOLUTION, s.INTERVAL_MINUTES,
     s.DISPLAY_LABEL, s.DISPLAY_SHORT, s.DESCRIPTION, TRUE);
