-- restart_portfolio_episode.sql
-- Purpose: Manual restart with a new portfolio id (new row in MIP.APP.PORTFOLIO).
-- This creates a new portfolio; positions, trades, and daily snapshots are keyed by
-- PORTFOLIO_ID, so the new portfolio starts with no child rows in
-- PORTFOLIO_POSITIONS, PORTFOLIO_TRADES, or PORTFOLIO_DAILY.
-- Columns match MIP/SQL/app/160_app_portfolio_tables.sql exactly.

use role MIP_ADMIN_ROLE;
use database MIP;

-- Required: PROFILE_ID, NAME, BASE_CURRENCY, STARTING_CASH.
-- Optional: STATUS, NOTES. Other columns (LAST_SIMULATION_RUN_ID, etc.) default to null.
insert into MIP.APP.PORTFOLIO (
    PROFILE_ID,
    NAME,
    BASE_CURRENCY,
    STARTING_CASH,
    STATUS,
    NOTES
)
select
    (select PROFILE_ID from MIP.APP.PORTFOLIO_PROFILE where NAME = 'PRIVATE_SAVINGS' limit 1),
    'RESTART_EPISODE_' || to_varchar(current_timestamp(), 'YYYYMMDDHH24MISS'),
    'USD',
    100000,
    'ACTIVE',
    'Created by restart_portfolio_episode.sql';

-- Optional: cleanup old test portfolio (run only if you intend to remove a specific test portfolio).
-- Uncomment and set :old_portfolio_name or :old_portfolio_id as needed.
-- delete from MIP.APP.PORTFOLIO_TRADES where PORTFOLIO_ID = :old_portfolio_id;
-- delete from MIP.APP.PORTFOLIO_POSITIONS where PORTFOLIO_ID = :old_portfolio_id;
-- delete from MIP.APP.PORTFOLIO_DAILY where PORTFOLIO_ID = :old_portfolio_id;
-- delete from MIP.APP.PORTFOLIO where PORTFOLIO_ID = :old_portfolio_id;
