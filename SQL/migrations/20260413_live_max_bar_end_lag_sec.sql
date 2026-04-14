-- Add optional cap on bar-end age for entry revalidation (Track B).
-- Effective entry stale threshold = min(QUOTE_FRESHNESS_THRESHOLD_SEC, MAX_BAR_END_LAG_SEC) when MAX_BAR_END_LAG_SEC is set.

use role MIP_ADMIN_ROLE;
use database MIP;

alter table MIP.LIVE.LIVE_PORTFOLIO_CONFIG
add column if not exists MAX_BAR_END_LAG_SEC number;

comment on column MIP.LIVE.LIVE_PORTFOLIO_CONFIG.MAX_BAR_END_LAG_SEC is
  'Optional min with QUOTE_FRESHNESS_THRESHOLD_SEC for entry bar-age gate. Null uses quote threshold only.';
