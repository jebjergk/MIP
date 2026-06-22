-- 20260622_us_equity_holidays.sql
-- Purpose: Seed table of observed US equity (NYSE) full-day exchange holidays.
-- Used by the agentic-opportunity-search staleness gate to determine the
-- latest expected completed US equity trading session. FULL_DAY_CLOSE=TRUE
-- for all standard holidays. Add partial-session rows with FALSE if needed.
-- Coverage: 2025-2027 observed NYSE holidays. Observed = date market is closed
-- (weekend holidays shift to nearest weekday per NYSE rule).
-- Maintenance: add future years before Jan 1 of that year.

USE ROLE MIP_ADMIN_ROLE;
USE DATABASE MIP;
USE SCHEMA APP;

CREATE TABLE IF NOT EXISTS MIP.APP.US_EQUITY_HOLIDAYS (
    HOLIDAY_DATE    DATE         NOT NULL,
    HOLIDAY_NAME    VARCHAR(100) NOT NULL,
    EXCHANGE        VARCHAR(10)  NOT NULL DEFAULT 'NYSE',
    FULL_DAY_CLOSE  BOOLEAN      NOT NULL DEFAULT TRUE,
    NOTES           VARCHAR(500),
    CONSTRAINT PK_US_EQUITY_HOLIDAYS PRIMARY KEY (HOLIDAY_DATE, EXCHANGE)
);

-- 2025 observed NYSE holidays
INSERT INTO MIP.APP.US_EQUITY_HOLIDAYS (HOLIDAY_DATE, HOLIDAY_NAME, EXCHANGE, FULL_DAY_CLOSE, NOTES)
SELECT d, n, 'NYSE', TRUE, nt FROM VALUES
    ('2025-01-01'::DATE, 'New Year''s Day',              'Jan 1 falls on Wednesday'),
    ('2025-01-20'::DATE, 'Martin Luther King Jr. Day',   'Third Monday of January'),
    ('2025-02-17'::DATE, 'Presidents'' Day',             'Third Monday of February'),
    ('2025-04-18'::DATE, 'Good Friday',                  '2 days before Easter Sunday'),
    ('2025-05-26'::DATE, 'Memorial Day',                 'Last Monday of May'),
    ('2025-06-19'::DATE, 'Juneteenth',                   'Jun 19 falls on Thursday'),
    ('2025-07-04'::DATE, 'Independence Day',             'Jul 4 falls on Friday'),
    ('2025-09-01'::DATE, 'Labor Day',                    'First Monday of September'),
    ('2025-11-27'::DATE, 'Thanksgiving',                 'Fourth Thursday of November'),
    ('2025-12-25'::DATE, 'Christmas',                    'Dec 25 falls on Thursday')
    AS v(d, n, nt)
WHERE NOT EXISTS (SELECT 1 FROM MIP.APP.US_EQUITY_HOLIDAYS h WHERE h.HOLIDAY_DATE = v.d AND h.EXCHANGE = 'NYSE');

-- 2026 observed NYSE holidays
INSERT INTO MIP.APP.US_EQUITY_HOLIDAYS (HOLIDAY_DATE, HOLIDAY_NAME, EXCHANGE, FULL_DAY_CLOSE, NOTES)
SELECT d, n, 'NYSE', TRUE, nt FROM VALUES
    ('2026-01-01'::DATE, 'New Year''s Day',              'Jan 1 falls on Thursday'),
    ('2026-01-19'::DATE, 'Martin Luther King Jr. Day',   'Third Monday of January'),
    ('2026-02-16'::DATE, 'Presidents'' Day',             'Third Monday of February'),
    ('2026-04-03'::DATE, 'Good Friday',                  '2 days before Easter Sunday'),
    ('2026-05-25'::DATE, 'Memorial Day',                 'Last Monday of May'),
    ('2026-06-19'::DATE, 'Juneteenth',                   'Jun 19 falls on Friday'),
    ('2026-07-03'::DATE, 'Independence Day Observed',    'Jul 4 is Saturday - observed Friday Jul 3'),
    ('2026-09-07'::DATE, 'Labor Day',                    'First Monday of September'),
    ('2026-11-26'::DATE, 'Thanksgiving',                 'Fourth Thursday of November'),
    ('2026-12-25'::DATE, 'Christmas',                    'Dec 25 falls on Friday')
    AS v(d, n, nt)
WHERE NOT EXISTS (SELECT 1 FROM MIP.APP.US_EQUITY_HOLIDAYS h WHERE h.HOLIDAY_DATE = v.d AND h.EXCHANGE = 'NYSE');

-- 2027 observed NYSE holidays
INSERT INTO MIP.APP.US_EQUITY_HOLIDAYS (HOLIDAY_DATE, HOLIDAY_NAME, EXCHANGE, FULL_DAY_CLOSE, NOTES)
SELECT d, n, 'NYSE', TRUE, nt FROM VALUES
    ('2027-01-01'::DATE, 'New Year''s Day',              'Jan 1 falls on Friday'),
    ('2027-01-18'::DATE, 'Martin Luther King Jr. Day',   'Third Monday of January'),
    ('2027-02-15'::DATE, 'Presidents'' Day',             'Third Monday of February'),
    ('2027-03-26'::DATE, 'Good Friday',                  '2 days before Easter Sunday'),
    ('2027-05-31'::DATE, 'Memorial Day',                 'Last Monday of May'),
    ('2027-06-18'::DATE, 'Juneteenth Observed',          'Jun 19 is Saturday - observed Friday Jun 18'),
    ('2027-07-05'::DATE, 'Independence Day Observed',    'Jul 4 is Sunday - observed Monday Jul 5'),
    ('2027-09-06'::DATE, 'Labor Day',                    'First Monday of September'),
    ('2027-11-25'::DATE, 'Thanksgiving',                 'Fourth Thursday of November'),
    ('2027-12-24'::DATE, 'Christmas Observed',           'Dec 25 is Saturday - observed Friday Dec 24')
    AS v(d, n, nt)
WHERE NOT EXISTS (SELECT 1 FROM MIP.APP.US_EQUITY_HOLIDAYS h WHERE h.HOLIDAY_DATE = v.d AND h.EXCHANGE = 'NYSE');

GRANT SELECT ON TABLE MIP.APP.US_EQUITY_HOLIDAYS TO ROLE MIP_ADMIN_ROLE;
GRANT SELECT ON TABLE MIP.APP.US_EQUITY_HOLIDAYS TO ROLE MIP_UI_API_ROLE;
