-- bootstrap_portfolio_episodes.sql
-- Purpose: Create one ACTIVE episode per existing portfolio so KPIs/risk are episode-scoped from "now".
-- Run after deploying 168_portfolio_episode.sql (table + view + procedure).
-- Safe to run idempotently: only inserts when a portfolio has no ACTIVE episode.

use role MIP_ADMIN_ROLE;
use database MIP;

insert into MIP.APP.PORTFOLIO_EPISODE (
    PORTFOLIO_ID,
    PROFILE_ID,
    START_TS,
    END_TS,
    STATUS,
    END_REASON
)
select
    p.PORTFOLIO_ID,
    p.PROFILE_ID,
    current_timestamp() as START_TS,
    null,
    'ACTIVE',
    null
from MIP.APP.PORTFOLIO p
where p.STATUS = 'ACTIVE'
  and p.PROFILE_ID is not null
  and not exists (
      select 1
      from MIP.APP.PORTFOLIO_EPISODE e
      where e.PORTFOLIO_ID = p.PORTFOLIO_ID
        and e.STATUS = 'ACTIVE'
  );
