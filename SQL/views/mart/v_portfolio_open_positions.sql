-- v_portfolio_open_positions.sql
-- Purpose: Compatibility wrapper around canonical open positions view

use role MIP_ADMIN_ROLE;
use database MIP;

create or replace view MIP.MART.V_PORTFOLIO_OPEN_POSITIONS as
select *
from MIP.MART.V_PORTFOLIO_OPEN_POSITIONS_CANONICAL;
