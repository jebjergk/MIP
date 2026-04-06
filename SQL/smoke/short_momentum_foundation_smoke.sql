-- Short Momentum Foundation — post-deploy smoke (read-only)
-- Expect: LONG-safe views return rows; research views compile; no SHORT in trusted latest TS when gate off.

use role MIP_ADMIN_ROLE;
use database MIP;

select 'V_SIGNAL_OUTCOMES_BASE' as v, count(*) as n from MIP.MART.V_SIGNAL_OUTCOMES_BASE;
select 'V_SIGNAL_OUTCOMES_BASE_RESEARCH' as v, count(*) as n from MIP.MART.V_SIGNAL_OUTCOMES_BASE_RESEARCH;
select 'V_TRAINING_LEADERBOARD' as v, count(*) as n from MIP.MART.V_TRAINING_LEADERBOARD;
select 'V_TRUSTED_SIGNALS_LATEST_TS' as v, count(*) as n from MIP.MART.V_TRUSTED_SIGNALS_LATEST_TS;

select 'short_in_trusted_latest' as check_name,
       count(*) as n
from MIP.MART.V_TRUSTED_SIGNALS_LATEST_TS t
join MIP.APP.RECOMMENDATION_LOG r on r.RECOMMENDATION_ID = t.RECOMMENDATION_ID
where r.SIGNAL_DIRECTION = 'SHORT';

select 'rec_log_short_count' as check_name, count(*) as n
from MIP.APP.RECOMMENDATION_LOG
where SIGNAL_DIRECTION = 'SHORT';

-- Research backfill (optional): requires active MOMENTUM_SHORT patterns + SHORT_MOMENTUM_GENERATION_ENABLED.
-- Dry-run (no writes): batches_completed only.
-- call MIP.APP.SP_BACKFILL_SHORT_MOMENTUM('2025-08-01'::date, current_date()::date, 'STOCK', 1440, null, null, null, 7, true);
