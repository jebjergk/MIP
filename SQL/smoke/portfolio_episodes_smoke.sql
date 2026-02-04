-- portfolio_episodes_smoke.sql
-- Purpose: Smoke checks for portfolio episodes (one active per portfolio, KPIs reset with new episode, API shape).
-- Prereqs: 168_portfolio_episode.sql deployed; bootstrap_portfolio_episodes.sql run.

use role MIP_ADMIN_ROLE;
use database MIP;

-- 1) Exactly one ACTIVE episode per portfolio
select PORTFOLIO_ID, count(*) as active_count
  from MIP.APP.PORTFOLIO_EPISODE
 where STATUS = 'ACTIVE'
 group by PORTFOLIO_ID
 having count(*) <> 1;
-- Expect: 0 rows (no portfolio may have <> 1 active episode).

-- 2) All ACTIVE portfolios have an active episode
select p.PORTFOLIO_ID
  from MIP.APP.PORTFOLIO p
 left join MIP.APP.V_PORTFOLIO_ACTIVE_EPISODE e
   on e.PORTFOLIO_ID = p.PORTFOLIO_ID
 where p.STATUS = 'ACTIVE'
   and e.EPISODE_ID is null;
-- Expect: 0 rows (every active portfolio has an active episode).

-- 3) V_PORTFOLIO_ACTIVE_EPISODE returns one row per portfolio with active episode
select * from MIP.APP.V_PORTFOLIO_ACTIVE_EPISODE;

-- 4) After starting a new episode, KPIs should be scoped to new window (manual check: call SP_START_PORTFOLIO_EPISODE then re-run pipeline; drawdown/win/loss should reset).
-- Optional: set portfolio_id and run to test procedure.
-- set portfolio_id = 1;
-- set profile_id = (select PROFILE_ID from MIP.APP.PORTFOLIO where PORTFOLIO_ID = $portfolio_id);
-- call MIP.APP.SP_START_PORTFOLIO_EPISODE($portfolio_id, $profile_id, 'MANUAL_RESET');

-- 5) API: GET /portfolios/{id}/snapshot should include active_episode: { episode_id, profile_id, start_ts, status }.
--    GET /portfolios/{id}/episodes should return list of episodes (most recent first) with episode_id, profile_id, start_ts, end_ts, end_reason, status, total_return, max_drawdown, win_days, loss_days, trades_count.
--    Verify via UI or curl.
