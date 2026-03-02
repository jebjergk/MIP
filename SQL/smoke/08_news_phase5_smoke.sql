-- 08_news_phase5_smoke.sql
-- Purpose: Phase 5 smoke for decision console/proposal news context.

use role MIP_ADMIN_ROLE;
use database MIP;

set phase5_portfolio_id = (
    select min(PORTFOLIO_ID)
    from MIP.APP.PORTFOLIO
    where STATUS = 'ACTIVE'
);

set phase5_run_id = (
    select 'NEWS_PHASE5_SMOKE_' || to_char(current_timestamp(), 'YYYYMMDDHH24MISS')
);

call MIP.APP.SP_AGENT_PROPOSE_TRADES($phase5_portfolio_id, $phase5_run_id, null);

-- 1) Proposal payload sample with decision-time news fields.
select
    PROPOSAL_ID,
    PORTFOLIO_ID,
    SYMBOL,
    MARKET_TYPE,
    SIGNAL_TS,
    SOURCE_SIGNALS:news_enabled::boolean as NEWS_ENABLED,
    SOURCE_SIGNALS:news_display_only::boolean as NEWS_DISPLAY_ONLY,
    SOURCE_SIGNALS:news_influence_enabled::boolean as NEWS_INFLUENCE_ENABLED,
    SOURCE_SIGNALS:base_score::float as BASE_SCORE,
    SOURCE_SIGNALS:final_score::float as FINAL_SCORE,
    SOURCE_SIGNALS:news_score_adj::float as NEWS_SCORE_ADJ,
    SOURCE_SIGNALS:news_block_new_entry::boolean as NEWS_BLOCK_NEW_ENTRY,
    SOURCE_SIGNALS:news_snapshot_age_minutes::number as NEWS_SNAPSHOT_AGE_MINUTES,
    SOURCE_SIGNALS:news_is_stale::boolean as NEWS_IS_STALE,
    SOURCE_SIGNALS:news_context:news_context_badge::string as NEWS_CONTEXT_BADGE,
    SOURCE_SIGNALS:news_context:news_count::number as NEWS_COUNT,
    SOURCE_SIGNALS:news_context:snapshot_ts::string as NEWS_SNAPSHOT_TS
from MIP.AGENT_OUT.ORDER_PROPOSALS
where PORTFOLIO_ID = $phase5_portfolio_id
  and STATUS = 'PROPOSED'
order by PROPOSED_AT desc, PROPOSAL_ID desc
limit 50;

-- 2) Rationale payload sample (display-only metadata).
select
    PROPOSAL_ID,
    SYMBOL,
    RATIONALE:strategy::string as STRATEGY,
    RATIONALE:base_score::float as BASE_SCORE,
    RATIONALE:final_score::float as FINAL_SCORE,
    RATIONALE:news_score_adj::float as NEWS_SCORE_ADJ,
    RATIONALE:news_block_new_entry::boolean as NEWS_BLOCK_NEW_ENTRY,
    RATIONALE:news_reasons as NEWS_REASONS,
    RATIONALE:news_snapshot_age_minutes::number as NEWS_SNAPSHOT_AGE_MINUTES,
    RATIONALE:news_is_stale::boolean as NEWS_IS_STALE
from MIP.AGENT_OUT.ORDER_PROPOSALS
where PORTFOLIO_ID = $phase5_portfolio_id
  and STATUS = 'PROPOSED'
order by PROPOSED_AT desc, PROPOSAL_ID desc
limit 50;

-- 3) Presence summary.
select
    count(*) as PROPOSED_ROWS_SCOPED,
    coalesce(count_if(SOURCE_SIGNALS:news_context is not null), 0) as WITH_NEWS_CONTEXT,
    coalesce(count_if(SOURCE_SIGNALS:news_is_stale::boolean = true), 0) as STALE_COUNT,
    coalesce(count_if(abs(SOURCE_SIGNALS:news_score_adj::float) > 0), 0) as WITH_NEWS_ADJ,
    coalesce(count_if(SOURCE_SIGNALS:news_block_new_entry::boolean = true), 0) as BLOCKED_BY_NEWS
from MIP.AGENT_OUT.ORDER_PROPOSALS
where PORTFOLIO_ID = $phase5_portfolio_id
  and STATUS = 'PROPOSED';
