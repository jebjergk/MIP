use role MIP_ADMIN_ROLE;
use database MIP;

select 'LEDGER_NEWS_INFLUENCE_EVENTS_30D' as check_name, count(*) as cnt
from MIP.AGENT_OUT.LEARNING_DECISION_LEDGER
where EVENT_TS >= dateadd(day, -30, current_timestamp())
  and (
    INFLUENCE_DELTA:news_context_state is not null
    or INFLUENCE_DELTA:news_event_shock_flag is not null
    or OUTCOME_STATE:news_context_snapshot is not null
    or OUTCOME_STATE:news_monitoring is not null
  );

select
    'LEDGER_NEWS_ACTION_CHAIN_SAMPLE' as check_name,
    LIVE_ACTION_ID,
    count(*) as chain_event_count,
    min(EVENT_TS) as first_ts,
    max(EVENT_TS) as last_ts
from MIP.AGENT_OUT.LEARNING_DECISION_LEDGER
where EVENT_TS >= dateadd(day, -30, current_timestamp())
  and LIVE_ACTION_ID is not null
  and (
    INFLUENCE_DELTA:news_context_state is not null
    or OUTCOME_STATE:news_context_snapshot is not null
  )
group by LIVE_ACTION_ID
order by chain_event_count desc, last_ts desc
limit 10;

